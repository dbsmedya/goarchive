//go:build integration && !windows

package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// The live group gate (spec §8; GR C6), in the server state M3 selects: a read lock on a secondary holds its applier, so
// the writes of the primary queue there and stay until the lock goes. The gate watches that secondary as a `type:
// group` server: the run holds with the member named, status-json shows its queue, and the run resumes when the queue
// drains. It needs a group estate, and skips through requireGroupMemberSource where the source is no member, as every
// group-only test does, so the matrix rows and the plain estate skip it.

// groupGateFixture is an order fixture whose replication block lists the estate's second member as a group server,
// bounded at applier 5, with the control socket on and a check interval of one second.
func groupGateFixture(t *testing.T, name string) (fixture, string) {
	t.Helper()
	return groupGateFixtureWith(t, name, "processing:\n  batch_size: 10\n  batch_delete_size: 100\n", 5)
}

// groupGateFixtureWith is groupGateFixture with the processing block and the configured applier bound the test chooses.
func groupGateFixtureWith(t *testing.T, name, processing string, applierBound int) (fixture, string) {
	t.Helper()
	requireGroupMemberSource(t)
	f, dir := controlRunFixture(t, name, processing)
	m2 := serverFor(t, "TEST_GROUP_MEMBER2")
	appendConfig(t, f, fmt.Sprintf("replication:\n  enabled: true\n  check_interval: 1\n  cache_ttl: 0\n  group_applier_queue_within: %d\n"+
		"  group_certifier_queue_within: 1000\n  servers:\n    - host: %s\n      port: %d\n      user: %s\n      password: %q\n      tls: disable\n      type: group\n",
		applierBound, m2.host, m2.port, m2.user, m2.pass))
	return f, dir
}

// holdSecondaryApplier makes the second member's applier queue: it locks a table on that member for reading and then
// writes the same table on the primary, which the member can only apply once the lock is gone. It returns the function
// that releases the lock, which the test's cleanup calls too, and checks that the lock is gone and the queue empty.
func holdSecondaryApplier(t *testing.T, writes int) (release func()) {
	t.Helper()
	ctx := context.Background()
	schema := fmt.Sprintf("ga3c6_blocker_%d", os.Getpid())
	primary, secondary := serverFor(t, "TEST_SOURCE").open(t, ""), serverFor(t, "TEST_GROUP_MEMBER2").open(t, "")
	mustExec(t, primary, "DROP DATABASE IF EXISTS `"+schema+"`", "CREATE DATABASE `"+schema+"`",
		"CREATE TABLE `"+schema+"`.blocker (id INT NOT NULL PRIMARY KEY, v INT NOT NULL) ENGINE=InnoDB", "INSERT INTO `"+schema+"`.blocker VALUES (1, 0)")
	t.Cleanup(func() { _, _ = primary.ExecContext(ctx, "DROP DATABASE IF EXISTS `"+schema+"`") })
	// the schema reaches the second member before it is locked
	var gtids string
	if err := primary.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&gtids); err != nil {
		t.Fatal(err)
	}
	var rc sql.NullInt64
	if err := secondary.QueryRowContext(ctx, "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 60)", gtids).Scan(&rc); err != nil || rc.Int64 != 0 {
		t.Fatalf("the second member did not apply the primary's transactions: %v (%v)", rc, err)
	}
	conn, err := secondary.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "LOCK TABLES `"+schema+"`.blocker READ"); err != nil {
		t.Fatal(err)
	}
	released := false
	release = func() {
		if released {
			return
		}
		released = true
		if _, err := conn.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
			t.Errorf("unlock the second member: %v", err)
		}
		_ = conn.Close()
		// the queue drains: the member has applied what the primary executed
		if err := primary.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&gtids); err != nil {
			t.Errorf("read the primary's executed set: %v", err)
			return
		}
		if err := secondary.QueryRowContext(ctx, "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 60)", gtids).Scan(&rc); err != nil || rc.Int64 != 0 {
			t.Errorf("the second member did not drain its queue after the lock was released: %v (%v)", rc, err)
		}
	}
	t.Cleanup(release)
	for i := 0; i < writes; i++ {
		mustExec(t, primary, fmt.Sprintf("UPDATE `%s`.blocker SET v = %d WHERE id = 1", schema, i+1))
	}
	return release
}

// TestMySQLALiveGroupGateHoldsOnASecondarysQueue: with M3's state the run holds before its first delete with the member
// named; status-json says so, with the member's type, its queue over the bound and no lag; releasing the lock drains the
// queue and the run goes on to complete.
func TestMySQLALiveGroupGateHoldsOnASecondarysQueue(t *testing.T) {
	f, dir := groupGateFixture(t, "livegroup")
	release := holdSecondaryApplier(t, 40)
	sock := filepath.Join(dir, "archive_orders.sock")
	r := startLive(t, sock, "archive", "-c", f.config, "--job", "archive_orders")
	st := r.waitFor("a hold on the second member", 60*time.Second, func(st map[string]any) bool { return st["state"] == "held" })

	hold := st["hold"].(map[string]any)["servers"].([]any)
	if len(hold) != 1 {
		t.Fatalf("hold.servers = %v, want the second member alone", hold)
	}
	m2 := serverFor(t, "TEST_GROUP_MEMBER2")
	member := hold[0].(map[string]any)
	if member["server"] != fmt.Sprintf("%s:%d", m2.host, m2.port) || member["type"] != "group" || member["cause"] != "unhealthy" || member["lag_seconds"] != nil {
		t.Errorf("hold.servers[0] = %v, want the second member as an unhealthy group server with no lag", member)
	}
	if q, ok := member["applier_queue"].(float64); !ok || q <= 5 {
		t.Errorf("hold.servers[0].applier_queue = %v, want a queue over the bound of 5", member["applier_queue"])
	}
	if d, _ := member["detail"].(string); !strings.Contains(d, "GROUP_REPLICATION_APPLIER_QUEUE_WITHIN") {
		t.Errorf("the hold's detail = %q, want the library's applier-queue finding", d)
	}
	repl := st["replication"].(map[string]any)
	if repl["group_applier_queue_within"] != 5.0 || repl["group_certifier_queue_within"] != 1000.0 {
		t.Errorf("replication bounds = %v and %v, want the configured 5 and 1000", repl["group_applier_queue_within"], repl["group_certifier_queue_within"])
	}
	if got := scalar(t, f.src, oldOrders); got != 3 {
		t.Errorf("old orders in the source while held = %d, want 3: nothing is deleted under a hold", got)
	}

	release()
	if code := r.finish(); code != 0 {
		t.Fatalf("exit %d after the queue drained, stderr:\n%s", code, r.errb.String())
	}
	if got := scalar(t, f.src, oldOrders); got != 0 {
		t.Errorf("old orders left in the source = %d, want 0", got)
	}
}

// TestMySQLALiveGroupBoundChangeHoldsTheNextDecision (spec §8, GR C7s): with a secondary's applier queue held above 0 (M3's
// state) and a configured bound well above it, the run passes the gate for its first batch, a real pass whose verdict names
// the bound it was judged by. group_applier_queue_within lowered over the socket to a value below the queue makes the next
// gate decision hold, naming the member and the new bound, with nothing deleted under the hold. Restoring the configured
// value lets the run go on to complete. No raise above the configuration is needed, so no round is waited for.
func TestMySQLALiveGroupBoundChangeHoldsTheNextDecision(t *testing.T) {
	const configured, lowered = 200, 20
	// two batches of two roots a sleep apart: the setter lands between them
	f, dir := groupGateFixtureWith(t, "livebound", "processing:\n  batch_size: 2\n  batch_delete_size: 2\n  sleep_seconds: 2\n", configured)
	release := holdSecondaryApplier(t, 40)
	t.Cleanup(release)
	m2 := serverFor(t, "TEST_GROUP_MEMBER2")
	member := fmt.Sprintf("%s:%d", m2.host, m2.port)
	sock := filepath.Join(dir, "archive_orders.sock")
	r := startLive(t, sock, "archive", "-c", f.config, "--job", "archive_orders")

	// a real pass: the first batch's deletes went through the gate with the queue held under the bound
	st := r.waitFor("the first batch", 60*time.Second, func(st map[string]any) bool { return counter(st, "batches") >= 1 })
	if got := scalar(t, f.src, oldOrders); got != 1 {
		t.Errorf("old orders in the source after the first batch = %d, want 1: the first batch passed the gate and deleted its two roots", got)
	}
	pass := st["replication"].(map[string]any)["servers"].([]any)[0].(map[string]any)
	if pass["server"] != member || pass["healthy"] != true || pass["applier_queue_within"] != float64(configured) {
		t.Errorf("the verdict of the pass = %v, want %s healthy and judged by the configured bound %d", pass, member, configured)
	}
	if q, ok := pass["applier_queue"].(float64); !ok || q <= lowered || q > configured {
		t.Errorf("the verdict's applier_queue = %v, want a held queue between the lowered bound %d and the configured %d", pass["applier_queue"], lowered, configured)
	}

	// lowered below the queue: the next decision holds, naming the member and the new bound
	if got, want := r.ask(fmt.Sprintf("group_applier_queue_within=%d", lowered)), fmt.Sprintf("OK\ngroup_applier_queue_within: %d -> %d, from the next gate decision\n", configured, lowered); got != want {
		t.Fatalf("lowering = %q, want %q", got, want)
	}
	st = r.waitFor("a hold at the lowered bound", 60*time.Second, func(st map[string]any) bool { return st["state"] == "held" })
	held := st["hold"].(map[string]any)["servers"].([]any)[0].(map[string]any)
	detail, _ := held["detail"].(string)
	if held["server"] != member || held["type"] != "group" || held["cause"] != "unhealthy" || !regexp.MustCompile(`above the bound 20([^0-9]|$)`).MatchString(detail) {
		t.Errorf("the hold = %v, want the member named, unhealthy, above the bound %d", held, lowered)
	}
	if held["applier_queue_within"] != float64(lowered) {
		t.Errorf("the held verdict's applier_queue_within = %v, want the new bound %d", held["applier_queue_within"], lowered)
	}
	repl := st["replication"].(map[string]any)
	setting := st["settings"].(map[string]any)["group_applier_queue_within"].(map[string]any)
	if repl["group_applier_queue_within"] != float64(lowered) || setting["value"] != float64(lowered) || setting["configured"] != float64(configured) {
		t.Errorf("the bound in force = %v, settings %v, want %d against the configured %d", repl["group_applier_queue_within"], setting, lowered, configured)
	}
	if got := scalar(t, f.src, oldOrders); got != 1 {
		t.Errorf("old orders in the source while held = %d, want 1: nothing is deleted under a hold", got)
	}

	// restored: the run goes on, with its queue still held under the configured bound
	if got, want := r.ask(fmt.Sprintf("group_applier_queue_within=%d", configured)), fmt.Sprintf("OK\ngroup_applier_queue_within: %d -> %d, from the next gate decision\n", lowered, configured); got != want {
		t.Fatalf("restoring = %q, want %q", got, want)
	}
	if code := r.finish(); code != 0 {
		t.Fatalf("exit %d after the bound was restored, stderr:\n%s", code, r.errb.String())
	}
	if got := scalar(t, f.src, oldOrders); got != 0 {
		t.Errorf("old orders left in the source = %d, want 0", got)
	}
	if errs := r.errb.String(); strings.Contains(errs, "WARNING: group_applier_queue_within raised") {
		t.Errorf("stderr carries a raise warning for a lowering and a restore:\n%s", errs)
	}
}
