//go:build integration

package engine

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/replication"
)

// liveGate is the real replication gate over the estate's replica (db3), checked every
// second with no cache, so a stopped applier holds the next delete chunk.
func liveGate(t *testing.T) (*replication.Gate, func(stmt string)) {
	t.Helper()
	replica := openServer(t, "TEST_REPLICA", "", nil)
	port, _ := strconv.Atoi(os.Getenv("TEST_REPLICA_PORT"))
	g, err := replication.New(config.ReplicationConfig{
		Enabled: true, SecondsBehindSourceWithin: 30, CheckInterval: 1, CacheTTL: 0,
		Servers: []config.ReplicationServerConfig{{
			Host: os.Getenv("TEST_REPLICA_HOST"), Port: port,
			User: os.Getenv("TEST_REPLICA_USER"), Password: os.Getenv("TEST_REPLICA_PASSWORD"),
		}},
	}, []*sql.DB{replica}, quietLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	exec := func(stmt string) { execAll(t, replica, stmt) }
	// Registered before any stop, so the applier runs again whatever the test does.
	t.Cleanup(func() { _, _ = replica.Exec("START REPLICA SQL_THREAD") })
	return g, exec
}

// TestMySQLLiveReplicationGate (M7-3, G19): the replication gate holds every delete chunk
// while the replica's applier is stopped, and the same run finishes once it restarts.
// Pair (R56): applier running → the run deletes the old orders without holding; applier
// stopped → after several check intervals the run has not returned and every old order is
// still in the source; restarted → the same run finishes and they are gone. A run held at
// the gate is cancelled with context.Canceled and deletes nothing. Archive and purge.
func TestMySQLLiveReplicationGate(t *testing.T) {
	cs := "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge} {
		t.Run(string(typ), func(t *testing.T) {
			setup := func(t *testing.T, name string) (env, Config, func(string)) {
				e := newEnv(t, name)
				if typ == jobspec.Purge {
					execAll(t, e.src, orderTables(cs)...)
				} else {
					e.both(t, orderTables(cs)...)
				}
				seedOrders(t, e, 3)
				gate, exec := liveGate(t)
				cfg := e.config(t, job(t, typ, orderJob))
				cfg.Lag = gate
				return e, cfg, exec
			}
			oldOrders := "SELECT COUNT(*) FROM orders WHERE id <> 'new'"

			t.Run("applier running", func(t *testing.T) {
				e, cfg, _ := setup(t, "lagok")
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				start := time.Now()
				if _, err := Run(ctx, cfg); err != nil {
					t.Fatalf("Run() = %v", err)
				}
				if got := count(t, e.src, oldOrders); got != 0 {
					t.Errorf("old orders left in the source = %d, want 0", got)
				}
				if d := time.Since(start); d > 20*time.Second {
					t.Errorf("a healthy replica held the run for %s", d)
				}
			})

			t.Run("applier stopped, then restarted", func(t *testing.T) {
				e, cfg, exec := setup(t, "laghold")
				exec("STOP REPLICA SQL_THREAD")
				ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
				defer cancel()
				done := make(chan error, 1)
				go func() {
					_, err := Run(ctx, cfg)
					done <- err
				}()
				select {
				case err := <-done:
					t.Fatalf("Run() returned %v while the applier was stopped, want it held at the gate", err)
				case <-time.After(4 * time.Second): // four check intervals
				}
				if got := count(t, e.src, oldOrders); got != 3 {
					t.Errorf("old orders in the source while held = %d, want 3: no delete chunk may pass the gate", got)
				}
				exec("START REPLICA SQL_THREAD")
				if err := <-done; err != nil {
					t.Fatalf("Run() after the applier restarted = %v", err)
				}
				if got := count(t, e.src, oldOrders); got != 0 {
					t.Errorf("old orders left in the source = %d, want 0", got)
				}
			})

			t.Run("cancelled while held", func(t *testing.T) {
				e, cfg, exec := setup(t, "lagcancel")
				exec("STOP REPLICA SQL_THREAD")
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error, 1)
				go func() {
					_, err := Run(ctx, cfg)
					done <- err
				}()
				time.Sleep(3 * time.Second)
				cancel()
				select {
				case err := <-done:
					if !errors.Is(err, context.Canceled) {
						t.Errorf("Run() cancelled at the gate = %v, want context.Canceled", err)
					}
				case <-time.After(30 * time.Second):
					t.Fatal("Run() did not return after its context was cancelled")
				}
				if got := count(t, e.src, oldOrders); got != 3 {
					t.Errorf("old orders in the source = %d, want 3", got)
				}
			})
		})
	}
}
