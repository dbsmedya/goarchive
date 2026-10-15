//go:build integration

package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/lock"
)

// LAG-1 (beta.2 PR 2): a replication hold of any length, and a first signal during one.

// killingGate stands in for a hold that outlives the source sessions' wait_timeout, which
// the DSN pins to at least 8 h: a test cannot wait that long, and lowering the pin would
// change product code to prove behaviour. On its first wait it kills every session whose
// default schema is the test's source schema, except the run's table-locks session, waits
// until the server has closed them (the driver's liveness check sees a close only once the
// FIN has arrived, Fable F2), then passes. Later waits pass at once.
//
// The process list is read from performance_schema directly: dbsgomysql has no session-list
// fact, a process list is not one of the facts the library owns (replica status, grants,
// schema), and the consumer-policy guard exempts _test.go files (Fable F3). The kill also
// closes the harness's idle sessions on e.src, which survive it as the run does; no helper
// may pin a *sql.Conn of e.src across the run.
type killingGate struct {
	admin  *sql.DB // no default schema, so its own sessions are not in the schema's list
	schema string  // the test's source schema: the default schema of every session the run opens on e.src
	lock   string  // one of the run's table-lock names; its holder is the table-locks session
	killed atomic.Int64
}

func newKillingGate(t *testing.T, e env, root string) *killingGate {
	t.Helper()
	return &killingGate{admin: openServer(t, "TEST_SOURCE", "", nil), schema: e.srcSchema, lock: lock.TableLockName(e.srcSchema, root)}
}

func (g *killingGate) WaitForLag(ctx context.Context, _ <-chan struct{}) error {
	if g.killed.Load() > 0 {
		return nil
	}
	holder, err := lockHolder(ctx, g.admin, g.lock)
	if err != nil {
		return err
	}
	if holder == 0 {
		return fmt.Errorf("killingGate: no session holds %s", g.lock)
	}
	ids, err := g.sessions(ctx, holder)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return errors.New("killingGate: no source session of the run to kill; the test would prove nothing")
	}
	for _, id := range ids {
		// 1094 (unknown thread id): the session closed on its own since the list was read.
		if _, err := g.admin.ExecContext(ctx, fmt.Sprintf("KILL %d", id)); err != nil && !isMySQLError(err, 1094) {
			return fmt.Errorf("killingGate: KILL %d: %w", id, err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		left, err := g.sessions(ctx, holder)
		if err != nil {
			return err
		}
		if len(left) == 0 {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("killingGate: sessions %v still open 10 s after KILL", left)
		}
		time.Sleep(50 * time.Millisecond)
	}
	g.killed.Store(int64(len(ids)))
	return nil
}

// sessions lists the sessions whose default schema is the source schema, but the holder's.
func (g *killingGate) sessions(ctx context.Context, holder int64) ([]int64, error) {
	rows, err := g.admin.QueryContext(ctx,
		"SELECT PROCESSLIST_ID FROM performance_schema.threads WHERE PROCESSLIST_DB = ? AND PROCESSLIST_ID <> ? AND PROCESSLIST_ID <> CONNECTION_ID()",
		g.schema, holder)
	if err != nil {
		return nil, fmt.Errorf("killingGate: the session list: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// TestMySQLLongHold (LAG-1 §3.1, §5 test 1): a hold that outlives the source sessions no
// longer fails the run. Each gate site's first wait kills the run's idle source sessions:
// before the fix the next delete chunk ran on the batch's (or the recovery page's) pinned,
// killed session and the run failed, its roots logged and its rows in the source; after it,
// each chunk takes a live session after its wait and the run completes. The rows cover the
// loop's delete chunks for archive and purge (purge's first wait is before logDeleting), and a
// recovery replay of each, left by a SIGKILL at a crash point. The source pool is held to
// the run's minimum of 2, so a read session kept past the delete phase would hang the first
// chunk's checkout (Fable F1).
func TestMySQLLongHold(t *testing.T) {
	cs := "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
	for _, tc := range []struct {
		name      string
		typ       jobspec.Type
		crash     point // "" for none; else a first run is killed here, leaving work for recovery
		nth       int
		recovered int64
	}{
		{"archive", jobspec.Archive, "", 0, 0},
		{"purge", jobspec.Purge, "", 0, 0},
		{"archive recovery", jobspec.Archive, atCommitted, 1, 2},
		{"purge recovery", jobspec.Purge, atLogged, 1, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, "longhold")
			if tc.typ == jobspec.Purge {
				execAll(t, e.src, orderTables(cs)...)
			} else {
				e.both(t, orderTables(cs)...)
			}
			seedOrders(t, e, 3)
			if tc.crash != "" {
				e.killAt(t, tc.typ, orderJob, tc.crash, tc.nth)
			}
			e.src.SetMaxOpenConns(minSourceConnections)
			gate := newKillingGate(t, e, "orders")
			cfg := e.config(t, job(t, tc.typ, orderJob))
			cfg.Lag = gate
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			rep, err := e.runContext(t, ctx, cfg)
			if gate.killed.Load() == 0 {
				t.Fatalf("the gate killed no session (Run() = %v): the test proved nothing", err)
			}
			if err != nil {
				t.Fatalf("Run() after a hold that outlived its source sessions = %v, want it to finish: no session may be held idle across a gate wait", err)
			}
			if rep.Recovered != tc.recovered {
				t.Errorf("recovered = %d, want %d", rep.Recovered, tc.recovered)
			}
			if got := count(t, e.src, "SELECT COUNT(*) FROM orders WHERE id <> 'new'"); got != 0 {
				t.Errorf("old orders left in the source = %d, want 0", got)
			}
			if got := fmt.Sprint(statuses(t, e.dst, e.dstSchema)); got != "map[2:3]" {
				t.Errorf("log = %s, want 3 completed", got)
			}
		})
	}
}

// TestMySQLStopDuringHold (LAG-1 §3.2, §5 tests 3–4): a first signal during a replication
// hold stops the run at once, with no error and Stopped set; nothing passes the gate, the
// held batch's roots stay logged (a purge held before it logs leaves nothing), and a second
// run with the replica healthy recovers them and completes. Before the fix the run stayed
// held until the replica recovered or a second signal cancelled it. The real gate over db3,
// its applier stopped, as TestMySQLLiveReplicationGate's "cancelled while held".
func TestMySQLStopDuringHold(t *testing.T) {
	cs := "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
	oldOrders := "SELECT COUNT(*) FROM orders WHERE id <> 'new'"
	for _, tc := range []struct {
		name      string
		typ       jobspec.Type
		crash     point
		nth       int
		left      string // the log after the stop, as fmt.Sprint(statuses)
		recovered int64  // the second run's recovery
	}{
		{"archive, before a delete chunk", jobspec.Archive, "", 0, "map[1:2]", 2},
		{"purge, before logging deleting", jobspec.Purge, "", 0, "map[]", 0},
		{"archive recovery, before a delete chunk", jobspec.Archive, atCommitted, 1, "map[1:2]", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, "holdstop")
			if tc.typ == jobspec.Purge {
				execAll(t, e.src, orderTables(cs)...)
			} else {
				e.both(t, orderTables(cs)...)
			}
			seedOrders(t, e, 3)
			if tc.crash != "" {
				e.killAt(t, tc.typ, orderJob, tc.crash, tc.nth)
			}
			gate, exec := liveGate(t, 30)
			exec("STOP REPLICA SQL_THREAD")
			stop := make(chan struct{})
			cfg := e.config(t, job(t, tc.typ, orderJob))
			cfg.Lag, cfg.Stop = gate, stop
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			type result struct {
				rep *Report
				err error
			}
			done := make(chan result, 1)
			go func() {
				rep, err := e.runContext(t, ctx, cfg)
				done <- result{rep, err}
			}()
			select {
			case r := <-done:
				t.Fatalf("Run() = %+v, %v before the stop, want it held at the gate", r.rep, r.err)
			case <-time.After(3 * time.Second): // three check intervals
			}
			close(stop)
			var r result
			select {
			case r = <-done:
			case <-time.After(10 * time.Second): // Docker's default grace, the number the docs cite
				cancel()
				<-done
				t.Fatal("Run() still held 10 s after a first signal: a stop must end a replication hold")
			}
			if r.err != nil || r.rep == nil || !r.rep.Stopped {
				t.Fatalf("Run() stopped during a hold = %+v, %v, want Stopped and no error", r.rep, r.err)
			}
			if got := count(t, e.src, oldOrders); got != 3 {
				t.Errorf("old orders after the stop = %d, want 3: nothing passes the gate", got)
			}
			if got := fmt.Sprint(statuses(t, e.dst, e.dstSchema)); got != tc.left {
				t.Errorf("log after the stop = %s, want %s", got, tc.left)
			}

			exec("START REPLICA SQL_THREAD")
			again := e.config(t, job(t, tc.typ, orderJob))
			again.Lag = gate
			ctx2, cancel2 := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel2()
			rep, err := e.runContext(t, ctx2, again)
			if err != nil {
				t.Fatalf("second Run() = %v", err)
			}
			if rep.Recovered != tc.recovered {
				t.Errorf("second run recovered %d, want %d", rep.Recovered, tc.recovered)
			}
			if got := count(t, e.src, oldOrders); got != 0 {
				t.Errorf("old orders after the second run = %d, want 0", got)
			}
			if got := fmt.Sprint(statuses(t, e.dst, e.dstSchema)); got != "map[2:3]" {
				t.Errorf("log after the second run = %s, want 3 completed", got)
			}
		})
	}
}

// TestMySQLStopNeverCutsAPass (LAG-1 §3.2, §5 test 5): a stop never cuts a healthy pass
// short. With the replica healthy and no cache, every delete chunk probes; a stop after the
// first chunk lets the batch finish, and the run then stops at the batch boundary. It passes
// before the fix and must still pass after it.
func TestMySQLStopNeverCutsAPass(t *testing.T) {
	cs := "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge} {
		t.Run(string(typ), func(t *testing.T) {
			e := newEnv(t, "holdpass")
			if typ == jobspec.Purge {
				execAll(t, e.src, orderTables(cs)...)
			} else {
				e.both(t, orderTables(cs)...)
			}
			seedOrders(t, e, 3)
			gate, _ := liveGate(t, 30)
			stop := make(chan struct{})
			var once sync.Once
			cfg := e.config(t, job(t, typ, orderJob))
			cfg.Lag, cfg.Stop = gate, stop
			cfg.at = func(p point) {
				if p == atDeleted {
					once.Do(func() { close(stop) })
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			rep, err := e.runContext(t, ctx, cfg)
			if err != nil || rep == nil || !rep.Stopped || rep.Roots != 2 {
				t.Fatalf("Run() stopped mid-batch with a healthy replica = %+v, %v, want batch 1 finished (2 roots) and Stopped", rep, err)
			}
			if got := count(t, e.src, "SELECT COUNT(*) FROM orders WHERE id <> 'new'"); got != 1 {
				t.Errorf("old orders left = %d, want 1: batch 1 deleted both its roots", got)
			}
			if got := fmt.Sprint(statuses(t, e.dst, e.dstSchema)); got != "map[2:2]" {
				t.Errorf("log = %s, want batch 1's 2 roots completed", got)
			}
		})
	}
}

// isMySQLError reports whether err is MySQL error number.
func isMySQLError(err error, number uint16) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == number
}
