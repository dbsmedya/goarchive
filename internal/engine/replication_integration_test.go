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

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
	"github.com/dbsmedya/goarchive/v3/internal/replication"
)

// liveGate is the real replication gate over the estate's replica (db3), checked every
// second with no cache, so a stopped or lagging applier holds the next delete chunk. within
// is the lag it tolerates, in seconds.
func liveGate(t *testing.T, within int) (*replication.Gate, func(stmt string)) {
	t.Helper()
	replica := openServer(t, "TEST_REPLICA", "", nil)
	port, _ := strconv.Atoi(os.Getenv("TEST_REPLICA_PORT"))
	g, err := replication.New(config.ReplicationConfig{
		Enabled: true, SecondsBehindSourceWithin: within, CheckInterval: 1, CacheTTL: 0,
		Servers: []config.ReplicationServerConfig{{
			Host: os.Getenv("TEST_REPLICA_HOST"), Port: port,
			User: os.Getenv("TEST_REPLICA_USER"), Password: os.Getenv("TEST_REPLICA_PASSWORD"),
		}},
	}, []*sql.DB{replica}, gateLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	exec := func(stmt string) { execAll(t, replica, stmt) }
	// Registered before any stop, so the applier runs again whatever the test does.
	t.Cleanup(func() { _, _ = replica.Exec("START REPLICA SQL_THREAD") })
	return g, exec
}

// gateLogger writes the gate's warnings, among them every hold with its cause, to the test
// log, so a hold the test did not cause names its reason (M7c-2: the first full run held
// "applier running" for 60 s, and quietLogger had dropped why).
func gateLogger(t *testing.T) *logger.Logger {
	return &logger.Logger{SugaredLogger: zaptest.NewLogger(t, zaptest.Level(zapcore.WarnLevel)).Sugar()}
}

// replicaLag is the default channel's Seconds_Behind_Source as the library reads it (the
// gate's own reader), -1 when the server reports NULL.
func replicaLag(t *testing.T, replica *sql.DB) int64 {
	t.Helper()
	channels, err := dbsrepl.NewInspector(replica).ReplicaStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range channels {
		if c.ChannelName == "" {
			if !c.SecondsBehindSource.Valid {
				return -1
			}
			return c.SecondsBehindSource.Int64
		}
	}
	t.Fatalf("replica status has no default channel: %+v", channels)
	return 0
}

// TestMySQLLiveReplicationGate (M7-3, G19): the replication gate holds every delete chunk
// while the replica's applier is stopped, and the same run finishes once it restarts.
// Pair (R56): applier running → the run deletes the old orders without holding; applier
// stopped → after several check intervals the run has not returned and every old order is
// still in the source; restarted → the same run finishes and they are gone. A run held at
// the gate is cancelled with context.Canceled and deletes nothing. Archive and purge.
// M7c-2 (G19's SOURCE_DELAY variant, probe PR-5): with the applier running but delayed by
// SOURCE_DELAY, the lag of the test's own seed writes passes the gate's tolerance: the run
// is held with every old order in the source, and finishes on its own once the delayed
// events apply, with no restart.
func TestMySQLLiveReplicationGate(t *testing.T) {
	cs := "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge} {
		t.Run(string(typ), func(t *testing.T) {
			setup := func(t *testing.T, name string, within int) (env, Config, func(string)) {
				e := newEnv(t, name)
				if typ == jobspec.Purge {
					execAll(t, e.src, orderTables(cs)...)
				} else {
					e.both(t, orderTables(cs)...)
				}
				seedOrders(t, e, 3)
				gate, exec := liveGate(t, within)
				cfg := e.config(t, job(t, typ, orderJob))
				cfg.Lag = gate
				return e, cfg, exec
			}
			oldOrders := "SELECT COUNT(*) FROM orders WHERE id <> 'new'"

			t.Run("applier running", func(t *testing.T) {
				e, cfg, _ := setup(t, "lagok", 30)
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
				e, cfg, exec := setup(t, "laghold", 30)
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

			t.Run("applier lagging, running", func(t *testing.T) {
				replica := openServer(t, "TEST_REPLICA", "", nil)
				// Registered before the delay is set, so the replica returns to no delay
				// whatever the test does; it runs after the scratch schemas are dropped.
				t.Cleanup(func() {
					for _, stmt := range []string{"STOP REPLICA SQL_THREAD", "CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 0", "START REPLICA SQL_THREAD"} {
						_, _ = replica.Exec(stmt)
					}
				})
				execAll(t, replica, "STOP REPLICA SQL_THREAD", "CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 8", "START REPLICA SQL_THREAD")
				e, cfg, _ := setup(t, "laglag", 1)
				cfg.Processing.BatchSize, cfg.Processing.BatchDeleteSize = 1000, 1000
				deadline := time.Now().Add(15 * time.Second)
				for replicaLag(t, replica) < 2 {
					if time.Now().After(deadline) {
						t.Fatal("the replica's lag never passed 1 s with SOURCE_DELAY = 8 after the seed writes")
					}
					time.Sleep(200 * time.Millisecond)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
				defer cancel()
				done := make(chan error, 1)
				go func() {
					_, err := Run(ctx, cfg)
					done <- err
				}()
				select {
				case err := <-done:
					t.Fatalf("Run() returned %v while the replica lagged, want it held at the gate", err)
				case <-time.After(3 * time.Second):
				}
				if got := count(t, e.src, oldOrders); got != 3 {
					t.Errorf("old orders in the source while lagging = %d, want 3: no delete chunk may pass the gate", got)
				}
				if err := <-done; err != nil {
					t.Fatalf("Run() once the delayed events applied = %v", err)
				}
				if got := count(t, e.src, oldOrders); got != 0 {
					t.Errorf("old orders left in the source = %d, want 0", got)
				}
			})

			t.Run("cancelled while held", func(t *testing.T) {
				e, cfg, exec := setup(t, "lagcancel", 30)
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
