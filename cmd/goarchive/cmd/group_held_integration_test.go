//go:build integration && !windows

package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/database"
)

// Stopping a held run (spec §4.2, rev 5.1; §8; GR C4). A copy-only run whose source is a newly elected primary
// is held by the server while the primary applies its backlog (M5's state, built below); the run's first
// phase of stop does not end the statement, and only a signal's second phase cancels it. These tests PIN what
// the shutdown code does today: nothing in it changes. They do not assert which statement is held: after C2 a
// copy-only run stops at the plugin-state read, after C7's pin at the table inspection.

const (
	heldConsistencyVar = "group_replication_consistency"
	heldFenced         = "BEFORE_ON_PRIMARY_FAILOVER"
)

// openMember opens a pool on a group member as root; eventual raises its sessions to EVENTUAL, so that a read of
// the state is never held by the state.
func openMember(t *testing.T, prefix, schema string, eventual bool) *sql.DB {
	t.Helper()
	s := serverFor(t, prefix)
	cfg := config.DatabaseConfig{Host: s.host, Port: s.port, User: s.user, Password: s.pass, Database: schema, TLS: "disable"}
	var globals []database.Global
	if eventual {
		globals = append(globals, database.Global{Name: heldConsistencyVar, Value: "'EVENTUAL'"})
	}
	db, err := database.OpenPool(t.Context(), &cfg, true, globals...)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(0)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// newlyElectedPrimary builds M5's server state and registers its undoing: gr2's applier is held by a table lock,
// a write on gr1 gives it a backlog, and the group is switched to gr2, which is PRIMARY and read-only until the
// backlog is applied. gr2's own consistency is set to the fenced value, whatever the series defaults to, and
// restored. The fixture's schemas must exist before it is called: a read-only primary takes no more.
func newlyElectedPrimary(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	gr1, gr2, gr3 := openMember(t, "TEST_SOURCE", "", true), openMember(t, "TEST_GROUP_MEMBER2", "", true), openMember(t, "TEST_GROUP_MEMBER3", "", true)
	str := func(db *sql.DB, q string, args ...any) string {
		var s string
		if err := db.QueryRowContext(ctx, q, args...).Scan(&s); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return s
	}
	view := func() string {
		rows, err := gr3.QueryContext(ctx, "SELECT member_host, member_role, member_state FROM performance_schema.replication_group_members ORDER BY 1")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = rows.Close() }()
		var parts []string
		for rows.Next() {
			var h, r, s string
			if err := rows.Scan(&h, &r, &s); err != nil {
				t.Fatal(err)
			}
			parts = append(parts, h+" "+r+" "+s)
		}
		return strings.Join(parts, "|")
	}
	const canonical = "gr1 PRIMARY ONLINE|gr2 SECONDARY ONLINE|gr3 SECONDARY ONLINE"
	if v := view(); v != canonical {
		t.Fatalf("the group is %q, want %q: the state starts from the estate's own", v, canonical)
	}
	uuid1, uuid2 := str(gr1, "SELECT @@server_uuid"), str(gr2, "SELECT @@server_uuid")
	blocker := fmt.Sprintf("ga3c4_blocker_%d", os.Getpid())
	mustExec(t, gr1, "DROP DATABASE IF EXISTS `"+blocker+"`", "CREATE DATABASE `"+blocker+"`",
		"CREATE TABLE `"+blocker+"`.blocker (id INT NOT NULL PRIMARY KEY, v INT NOT NULL) ENGINE=InnoDB", "INSERT INTO `"+blocker+"`.blocker VALUES (1, 0)")
	for _, m := range []*sql.DB{gr2, gr3} {
		var rc sql.NullInt64
		if err := m.QueryRowContext(ctx, "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 60)", str(gr1, "SELECT @@GLOBAL.gtid_executed")).Scan(&rc); err != nil || rc.Int64 != 0 {
			t.Fatalf("a member did not apply the primary's transactions: %v (%v)", rc, err)
		}
	}
	original := str(gr2, "SELECT @@GLOBAL."+heldConsistencyVar)
	mustExec(t, gr2, "SET GLOBAL "+heldConsistencyVar+" = '"+heldFenced+"'")

	lock, err := openMember(t, "TEST_GROUP_MEMBER2", blocker, true).Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lock.ExecContext(ctx, "LOCK TABLES blocker READ"); err != nil {
		t.Fatal(err)
	}
	released := false
	switched := make(chan error, 1)
	t.Cleanup(func() {
		if !released {
			released = true
			if _, err := lock.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
				t.Errorf("unlock the blocker: %v", err)
			}
			_ = lock.Close()
		}
		select {
		case err := <-switched:
			if err != nil {
				t.Errorf("the switch to gr2: %v", err)
			}
		case <-time.After(60 * time.Second):
			t.Errorf("the switch to gr2 did not complete after the lock was released")
		}
		if strings.Contains(view(), "gr2 PRIMARY") { // a UDF takes a constant, never a placeholder
			if _, err := gr2.ExecContext(ctx, "SELECT group_replication_set_as_primary('"+uuid1+"')"); err != nil {
				t.Errorf("switch the primary back to gr1: %v", err)
			}
		}
		for deadline := time.Now().Add(90 * time.Second); view() != canonical; time.Sleep(500 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Errorf("the group is %q after switching back, want %q", view(), canonical)
				break
			}
		}
		mustExec(t, gr1, "DROP DATABASE IF EXISTS `"+blocker+"`")
		if _, err := gr2.ExecContext(ctx, "SET GLOBAL "+heldConsistencyVar+" = '"+original+"'"); err != nil {
			t.Errorf("restore %s on gr2: %v", heldConsistencyVar, err)
		}
		if got := str(gr2, "SELECT @@GLOBAL."+heldConsistencyVar); got != original {
			t.Errorf("%s on gr2 is %s after the restore, want %s", heldConsistencyVar, got, original)
		}
	})

	mustExec(t, gr1, "UPDATE `"+blocker+"`.blocker SET v = v + 1 WHERE id = 1")
	go func() {
		_, err := gr1.ExecContext(ctx, "SELECT group_replication_set_as_primary('"+uuid2+"')")
		switched <- err
	}()
	until := func(what string, cond func() bool) {
		for deadline := time.Now().Add(60 * time.Second); !cond(); time.Sleep(100 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
		}
	}
	until("gr2 to be the PRIMARY", func() bool { return strings.Contains(view(), "gr2 PRIMARY") })
	until("gr2 to be read-only", func() bool { return str(gr2, "SELECT @@GLOBAL.super_read_only") == "1" })
}

// heldCopyOnly is a copy-only run whose source is the group's newly elected primary, started and held: the
// fixture's source is the group's primary, and its config is pointed at gr2 once the state exists. It returns
// the run after its socket serves and the run has gone on for two seconds without ending.
func heldCopyOnly(t *testing.T, name string) *liveRun {
	t.Helper()
	if os.Getenv("CI") == "true" {
		t.Skip("a held run is stopped with real signals, which the CI environment does not take")
	}
	requireGroupMemberSource(t)
	f, dir := controlRunFixture(t, name, "processing:\n  batch_size: 10\n  batch_delete_size: 100\n")
	setLogging(t, f, "logging:\n  level: warn\n  format: json\n  output: stderr\n") // the stop's two lines are warnings
	newlyElectedPrimary(t)
	setConfig(t, f, fmt.Sprintf("port: %d", serverFor(t, "TEST_SOURCE").port), fmt.Sprintf("port: %d", serverFor(t, "TEST_GROUP_MEMBER2").port))
	r := startLive(t, filepath.Join(dir, "copy_orders.sock"), "copy-only", "-c", f.config, "--job", "copy_orders")
	r.status() // the socket serves: it binds before the run connects
	time.Sleep(2 * time.Second)
	if r.exited(0) {
		t.Fatalf("the run ended instead of being held:\n%s", r.errb.String())
	}
	return r
}

// stillHeld asserts the run has not ended after d: its statement is held.
func (r *liveRun) stillHeld(d time.Duration, why string) {
	r.t.Helper()
	if r.exited(d) {
		r.t.Fatalf("%s: the run ended, want its statement still held:\n%s", why, r.errb.String())
	}
}

// waitLog waits for text in the run's stderr.
func (r *liveRun) waitLog(text string) {
	r.t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !strings.Contains(r.errb.String(), text); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			r.t.Fatalf("no %q in the run's log after 10 s:\n%s", text, r.errb.String())
		}
	}
}

// TestMySQLStoppingAHeldRunWithNoStopRequested is spec §8's first row: a first signal is logged as a stop
// request and the statement stays held; a second signal cancels the run, exit 1.
func TestMySQLStoppingAHeldRunWithNoStopRequested(t *testing.T) {
	r := heldCopyOnly(t, "grheld1")
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	r.waitLog("Stop requested")
	r.stillHeld(3*time.Second, "after a first signal")
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	r.waitLog("Cancelling the run now")
	if code := r.finish(); code != 1 {
		t.Errorf("exit %d after the second signal, want 1:\n%s", code, r.errb.String())
	}
}

// TestMySQLStoppingAHeldRunAfterTheSocketsStop is spec §8's second row: the socket's `stop` is accepted and the
// statement stays held; a second `stop` answers "already stopping" and changes nothing; one signal then cancels
// the run, exit 1.
func TestMySQLStoppingAHeldRunAfterTheSocketsStop(t *testing.T) {
	r := heldCopyOnly(t, "grheld2")
	if reply := r.ask("stop"); !strings.HasPrefix(reply, "OK\nstop requested") {
		t.Fatalf("stop = %q, want it accepted", reply)
	}
	r.waitLog("Stop requested")
	r.stillHeld(3*time.Second, "after the socket's stop")
	if reply := r.ask("stop"); !strings.HasPrefix(reply, "OK\nalready stopping") {
		t.Errorf("a second stop = %q, want \"already stopping\"", reply)
	}
	r.stillHeld(2*time.Second, "after a second stop")
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	r.waitLog("Cancelling the run now")
	if code := r.finish(); code != 1 {
		t.Errorf("exit %d after one signal, want 1:\n%s", code, r.errb.String())
	}
}
