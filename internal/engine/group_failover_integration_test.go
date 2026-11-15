//go:build integration

package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
)

// M5's server state, for the read-only checks (spec §4.5, §8; GR C4): a newly elected primary that still has a
// backlog to apply. The member gr2's applier is held by a table lock, a write on the old primary gives it a
// backlog, and the group is switched to gr2, which becomes PRIMARY and stays read-only until the backlog is
// applied. A statement a fence holds waits there; the checks are judged on the first read, which a fence lets
// through, and refuse at once. The state is the server's, built and undone by the test.

const (
	consistencyVar = "group_replication_consistency"
	fenced         = "BEFORE_ON_PRIMARY_FAILOVER"
)

// eventual is a pool parameter that raises a session to EVENTUAL (database.Global's value is SQL text).
var eventual = map[string]string{consistencyVar: "'EVENTUAL'"}

// failover is the state's handles.
type failover struct {
	gr1, gr2probe, gr3 *sql.DB // as root, EVENTUAL sessions: reads of the state are never held by it
}

// waitUntilTrue polls cond every 100 ms until it holds, and fails the test after within.
func waitUntilTrue(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", within, what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// view is the group as gr3 sees it: "gr1 PRIMARY ONLINE|gr2 SECONDARY ONLINE|...".
func (f failover) view(t *testing.T) string {
	t.Helper()
	rows, err := f.gr3.QueryContext(context.Background(), "SELECT member_host, member_role, member_state FROM performance_schema.replication_group_members ORDER BY 1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var parts []string
	for rows.Next() {
		var host, role, state string
		if err := rows.Scan(&host, &role, &state); err != nil {
			t.Fatal(err)
		}
		parts = append(parts, host+" "+role+" "+state)
	}
	return strings.Join(parts, "|")
}

const canonicalView = "gr1 PRIMARY ONLINE|gr2 SECONDARY ONLINE|gr3 SECONDARY ONLINE"

// newlyElectedPrimary builds the state and registers its undoing. Every fixture the tests need must exist before
// it is called: a read-only primary takes no more schemas. It returns when gr2 is PRIMARY and read-only with its
// backlog held.
func newlyElectedPrimary(t *testing.T) failover {
	t.Helper()
	f := failover{
		gr1:      openServer(t, "TEST_SOURCE", "", eventual),
		gr2probe: openServer(t, "TEST_GROUP_MEMBER2", "", eventual),
		gr3:      openServer(t, "TEST_GROUP_MEMBER3", "", eventual),
	}
	if v := f.view(t); v != canonicalView {
		t.Fatalf("the group is %q, want %q: the state starts from the estate's own", v, canonicalView)
	}
	ctx := context.Background()
	scalarStr := func(db *sql.DB, q string, args ...any) string {
		var s string
		if err := db.QueryRowContext(ctx, q, args...).Scan(&s); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return s
	}
	uuid2 := scalarStr(f.gr2probe, "SELECT @@server_uuid")
	uuid1 := scalarStr(f.gr1, "SELECT @@server_uuid")

	// the blocker: a table whose row gr2's applier needs, under a lock gr2 holds
	blocker := fmt.Sprintf("ga3c4_blocker_%d", os.Getpid())
	execAll(t, f.gr1, "DROP DATABASE IF EXISTS `"+blocker+"`", "CREATE DATABASE `"+blocker+"`",
		"CREATE TABLE `"+blocker+"`.blocker (id INT NOT NULL PRIMARY KEY, v INT NOT NULL) ENGINE=InnoDB", "INSERT INTO `"+blocker+"`.blocker VALUES (1, 0)")
	waitReplicated := func(db *sql.DB) {
		var rc sql.NullInt64
		if err := db.QueryRowContext(ctx, "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 60)", scalarStr(f.gr1, "SELECT @@GLOBAL.gtid_executed")).Scan(&rc); err != nil || rc.Int64 != 0 {
			t.Fatalf("a member did not apply the primary's transactions: %v (%v)", rc, err)
		}
	}
	waitReplicated(f.gr2probe)
	waitReplicated(f.gr3)

	// gr2's own consistency, the server's setting that fences a session: the test sets it, whatever the series
	// defaults to, and restores it
	original := scalarStr(f.gr2probe, "SELECT @@GLOBAL."+consistencyVar)
	execAll(t, f.gr2probe, "SET GLOBAL "+consistencyVar+" = '"+fenced+"'")

	lock, err := openServer(t, "TEST_GROUP_MEMBER2", blocker, eventual).Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lock.ExecContext(ctx, "LOCK TABLES blocker READ"); err != nil {
		t.Fatal(err)
	}
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		if _, err := lock.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
			t.Errorf("unlock the blocker: %v", err)
		}
		_ = lock.Close()
	}

	switched := make(chan error, 1)
	t.Cleanup(func() {
		release() // the backlog applies, the switch completes, gr2 is writable
		select {
		case err := <-switched:
			if err != nil {
				t.Errorf("the switch to gr2: %v", err)
			}
		case <-time.After(60 * time.Second):
			t.Errorf("the switch to gr2 did not complete after the lock was released")
		}
		// the switch back, when gr2 became the primary (a UDF takes a constant, never a placeholder); the schema
		// is dropped on whichever member is the primary by then
		if strings.Contains(f.view(t), "gr2 PRIMARY") {
			if _, err := f.gr2probe.ExecContext(ctx, "SELECT group_replication_set_as_primary('"+uuid1+"')"); err != nil {
				t.Errorf("switch the primary back to gr1: %v", err)
			}
		}
		for deadline := time.Now().Add(90 * time.Second); f.view(t) != canonicalView; time.Sleep(500 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Errorf("the group is %q after switching back, want %q", f.view(t), canonicalView)
				break
			}
		}
		execAll(t, f.gr1, "DROP DATABASE IF EXISTS `"+blocker+"`")
		if _, err := f.gr2probe.ExecContext(ctx, "SET GLOBAL "+consistencyVar+" = '"+original+"'"); err != nil {
			t.Errorf("restore %s on gr2: %v", consistencyVar, err)
		}
		if got := scalarStr(f.gr2probe, "SELECT @@GLOBAL."+consistencyVar); got != original {
			t.Errorf("%s on gr2 is %s after the restore, want %s", consistencyVar, got, original)
		}
	})

	// the backlog: one transaction gr2 cannot apply, then the switch, which returns only when it is applied
	execAll(t, f.gr1, "UPDATE `"+blocker+"`.blocker SET v = v + 1 WHERE id = 1")
	go func() {
		_, err := f.gr1.ExecContext(ctx, "SELECT group_replication_set_as_primary('"+uuid2+"')")
		switched <- err
	}()
	waitUntilTrue(t, 60*time.Second, "gr2 to be the PRIMARY", func() bool { return strings.Contains(f.view(t), "gr2 PRIMARY") })
	waitUntilTrue(t, 10*time.Second, "gr2 to be read-only", func() bool {
		return scalarStr(f.gr2probe, "SELECT @@GLOBAL.super_read_only") == "1"
	})
	t.Logf("the state is built: %s (gr2 super_read_only=%s, consistency=%s)", f.view(t),
		scalarStr(f.gr2probe, "SELECT @@GLOBAL.super_read_only"), scalarStr(f.gr2probe, "SELECT @@GLOBAL."+consistencyVar))
	return f
}

// pendingWork runs the job once against its source, with every DELETE on the source refused by a trigger, so that
// it ends with its roots logged as copied (archive) or deleting (purge) and nothing deleted; the trigger is then
// dropped. It returns the env, the config for the next run, and the status the roots hold.
func pendingWork(t *testing.T, name string, typ jobspec.Type) (env, Config, tracking.Status) {
	t.Helper()
	e := newEnv(t, name+string(typ))
	stuck := tracking.StatusCopied
	if typ == jobspec.Purge {
		execAll(t, e.src, numsDDL)
		stuck = tracking.StatusDeleting
	} else {
		e.both(t, numsDDL)
	}
	seedNums(t, e.src, 20)
	allow := stopDeletes(t, e.src, "nums")
	cfg := e.config(t, job(t, typ, numsJob))
	cfg.Processing, cfg.ForceTriggers = config.ProcessingConfig{BatchSize: 20, BatchDeleteSize: 20}, true
	if _, err := e.run(t, cfg); err == nil || !strings.Contains(err.Error(), "stopped by the test") {
		t.Fatalf("the first run = %v, want it to end at the refused DELETE", err)
	}
	if got := statuses(t, e.dst, e.dstSchema); got[int(stuck)] != 20 || len(got) != 1 {
		t.Fatalf("the log after the interrupted run = %v, want 20 roots as %s", got, stuck)
	}
	allow()
	cfg.ForceTriggers = false
	return e, cfg, stuck
}

// TestMySQLANewlyElectedPrimaryIsRefusedAtOnce is spec §8's *G9 on a newly elected primary* and the group half
// of *read-only destination*: with M5's server state, an archive and a purge whose source is the new primary are
// refused by SOURCE_READ_ONLY_CHECK at once, not held, on a session the server fences and on an EVENTUAL one,
// under a skip flag, and for an account without the view's grant; nothing is replayed or marked completed. A job
// whose destination is the new primary is refused by DEST_READ_ONLY_CHECK at once.
func TestMySQLANewlyElectedPrimaryIsRefusedAtOnce(t *testing.T) {
	requireGroupMemberSource(t)

	// every fixture first: a read-only primary takes no more schemas
	archive, archiveCfg, archiveStuck := pendingWork(t, "grro", jobspec.Archive)
	purge, purgeCfg, purgeStuck := pendingWork(t, "grro", jobspec.Purge)
	dstRole := newEnvOn(t, "grrodst", "TEST_DEST", "TEST_SOURCE")
	dstRole.both(t, numsDDL)
	seedNums(t, dstRole.src, 20)
	admin := openServer(t, "TEST_SOURCE", "", eventual)
	const user, pass = "ga3c4_nogrant", "ga3m5-nogrant-pw"
	account := "'" + user + "'@'%'"
	execAll(t, admin, "DROP USER IF EXISTS "+account, "CREATE USER "+account+" IDENTIFIED BY '"+pass+"'",
		"GRANT SELECT, DELETE, TRIGGER ON `"+archive.srcSchema+"`.* TO "+account, "GRANT PROCESS ON *.* TO "+account, "GRANT SELECT ON `mysql`.* TO "+account)
	t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+account) })

	newlyElectedPrimary(t)

	ctx := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), 40*time.Second)
	}
	// refusedAtOnce runs cfg and asserts it ends in 'check' quickly: a held statement would take the whole timeout.
	refusedAtOnce := func(t *testing.T, cfg Config, check string, wantMessage string) {
		t.Helper()
		c, cancel := ctx()
		defer cancel()
		start := time.Now()
		_, err := Run(c, cfg)
		var pe *preflight.Error
		if !errors.As(err, &pe) || pe.Check != check {
			t.Fatalf("Run() after %v = %v, want %s", time.Since(start).Round(time.Millisecond), err, check)
		}
		if took := time.Since(start); took > 15*time.Second {
			t.Errorf("the refusal took %v: it was held, not refused at once", took)
		}
		if !strings.Contains(pe.Message, wantMessage) {
			t.Errorf("the refusal does not say %q:\n%s", wantMessage, pe.Message)
		}
	}
	// onGr2 is a pool on the new primary as its role requires: the server's own fenced setting, or an EVENTUAL session
	onGr2 := func(schema string, session bool) *sql.DB {
		params := map[string]string(nil)
		if session {
			params = eventual
		}
		return openServer(t, "TEST_GROUP_MEMBER2", schema, params)
	}
	untouched := func(t *testing.T, e env, stuck tracking.Status, destRows bool) {
		t.Helper()
		if got := statuses(t, e.dst, e.dstSchema); got[int(stuck)] != 20 || len(got) != 1 {
			t.Errorf("the log = %v, want 20 roots still %s: none replayed or marked completed", got, stuck)
		}
		if n := count(t, e.src, "SELECT COUNT(*) FROM nums"); n != 20 {
			t.Errorf("source rows = %d, want 20 untouched", n)
		}
		if destRows {
			if n := count(t, e.dst, "SELECT COUNT(*) FROM nums"); n != 20 {
				t.Errorf("destination rows = %d, want the 20 copied before", n)
			}
		}
	}
	const general = "a newly elected primary until it has applied its backlog"

	for _, tc := range []struct {
		name    string
		e       env
		cfg     Config
		stuck   tracking.Status
		session bool // an EVENTUAL session instead of the server's fenced one
		skip    bool
		account bool // an account without the view's grant
	}{
		{"an archive on the server's fenced session", archive, archiveCfg, archiveStuck, false, false, false},
		{"an archive on an EVENTUAL session", archive, archiveCfg, archiveStuck, true, false, false},
		{"a purge on the server's fenced session", purge, purgeCfg, purgeStuck, false, false, false},
		{"a purge on an EVENTUAL session", purge, purgeCfg, purgeStuck, true, false, false},
		{"an archive under a skip flag", archive, archiveCfg, archiveStuck, false, true, false},
		{"a purge under a skip flag", purge, purgeCfg, purgeStuck, false, true, false},
		{"an archive by an account without the view's grant", archive, archiveCfg, archiveStuck, false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			cfg.Source = onGr2(tc.e.srcSchema, tc.session)
			if tc.account {
				cfg.Source = openAccount(t, "TEST_GROUP_MEMBER2", tc.e.srcSchema, user, pass)
			}
			cfg.SkipPolicy = tc.skip
			refusedAtOnce(t, cfg, "SOURCE_READ_ONLY_CHECK", general)
			untouched(t, tc.e, tc.stuck, tc.stuck == tracking.StatusCopied)
		})
	}

	// the destination is the new primary: every job type is refused by G13, with the source a plain server
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
		t.Run("a "+string(typ)+" job whose destination is the new primary", func(t *testing.T) {
			cfg := dstRole.config(t, job(t, typ, numsJob))
			cfg.Destination = onGr2(dstRole.dstSchema, false)
			refusedAtOnce(t, cfg, "DEST_READ_ONLY_CHECK", general)
			if n := count(t, dstRole.src, "SELECT COUNT(*) FROM nums"); n != 20 {
				t.Errorf("source rows = %d, want 20 untouched", n)
			}
		})
	}
}

// TestMySQLAReadOnlySecondaryIsRefusedForBeingReadOnlyWithoutTheGrant is spec §8's precedence on an ordinary
// secondary, which needs no backlog: an archive by an account without the view's grant is refused by
// SOURCE_READ_ONLY_CHECK, and a copy-only job by the same account by the permission check. The secondary is
// read-only, so no other statement of the refused job is asked of it.
func TestMySQLAReadOnlySecondaryIsRefusedForBeingReadOnlyWithoutTheGrant(t *testing.T) {
	requireGroupMemberSource(t)
	e := newEnv(t, "grrosecondary")
	e.both(t, numsDDL)
	seedNums(t, e.src, 20)
	admin := openServer(t, "TEST_SOURCE", "", eventual)
	const user, pass = "ga3c4_nogrant2", "ga3m5-nogrant2-pw"
	account := "'" + user + "'@'%'"
	execAll(t, admin, "DROP USER IF EXISTS "+account, "CREATE USER "+account+" IDENTIFIED BY '"+pass+"'",
		"GRANT SELECT, DELETE, TRIGGER ON `"+e.srcSchema+"`.* TO "+account, "GRANT PROCESS ON *.* TO "+account, "GRANT SELECT ON `mysql`.* TO "+account)
	t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+account) })
	// gr3 is a secondary: wait for it to hold the schema and the account
	var rc sql.NullInt64
	var gtids string
	if err := admin.QueryRowContext(context.Background(), "SELECT @@GLOBAL.gtid_executed").Scan(&gtids); err != nil {
		t.Fatal(err)
	}
	gr3 := openServer(t, "TEST_GROUP_MEMBER3", "", eventual)
	if err := gr3.QueryRowContext(context.Background(), "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 60)", gtids).Scan(&rc); err != nil || rc.Int64 != 0 {
		t.Fatalf("gr3 did not apply the primary's transactions: %v (%v)", rc, err)
	}

	for _, tc := range []struct {
		typ   jobspec.Type
		check string
	}{
		{jobspec.Archive, "SOURCE_READ_ONLY_CHECK"},
		{jobspec.Purge, "SOURCE_READ_ONLY_CHECK"},
		{jobspec.CopyOnly, "SOURCE_GROUP_VIEW_PERMISSION_CHECK"},
	} {
		t.Run(string(tc.typ), func(t *testing.T) {
			cfg := e.config(t, job(t, tc.typ, numsJob))
			cfg.Source = openAccount(t, "TEST_GROUP_MEMBER3", e.srcSchema, user, pass)
			_, err := Run(context.Background(), cfg)
			var pe *preflight.Error
			if !errors.As(err, &pe) || pe.Check != tc.check {
				t.Fatalf("Run() = %v, want %s", err, tc.check)
			}
			if strings.Contains(pe.Message, "performance_schema") && tc.check == "SOURCE_READ_ONLY_CHECK" {
				t.Errorf("the read-only refusal names the grant:\n%s", pe.Message)
			}
		})
	}
}

// setReadOnly turns read_only on for a server, as a SET GLOBAL, and restores both variables to the values the
// server had when the test ends, reading them back: a server left read-only breaks every later test.
func setReadOnly(t *testing.T, prefix string) {
	t.Helper()
	db := openServer(t, prefix, "", nil)
	var ro, sro int
	if err := db.QueryRowContext(context.Background(), "SELECT @@GLOBAL.read_only, @@GLOBAL.super_read_only").Scan(&ro, &sro); err != nil {
		t.Fatal(err)
	}
	execAll(t, db, "SET GLOBAL read_only = 1")
	t.Cleanup(func() {
		for _, stmt := range []string{fmt.Sprintf("SET GLOBAL super_read_only = %d", sro), fmt.Sprintf("SET GLOBAL read_only = %d", ro)} {
			if _, err := db.ExecContext(context.Background(), stmt); err != nil {
				t.Errorf("%s: restore %q: %v", prefix, stmt, err)
			}
		}
		var gotRO, gotSRO int
		if err := db.QueryRowContext(context.Background(), "SELECT @@GLOBAL.read_only, @@GLOBAL.super_read_only").Scan(&gotRO, &gotSRO); err != nil || gotRO != ro || gotSRO != sro {
			t.Errorf("%s: read_only/super_read_only are %d/%d (%v) after the restore, want %d/%d", prefix, gotRO, gotSRO, err, ro, sro)
		}
	})
}

// TestMySQLAReadOnlySourceIsRefusedBeforeRecovery is spec §8's *read-only source*, plain half (the estate's
// replica is a plain server on both estates): an archive and a purge whose log holds roots as copied or deleting
// are refused by SOURCE_READ_ONLY_CHECK when the source they now name is read-only, before recovery reads or
// replays anything: no root changes status, no row is deleted. Under a skip flag too.
func TestMySQLAReadOnlySourceIsRefusedBeforeRecovery(t *testing.T) {
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge} {
		for _, skip := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/skip=%v", typ, skip), func(t *testing.T) {
				e, cfg, stuck := pendingWork(t, "roreplay", typ)
				// the replica must hold the job's schema (the engine reads the source's identity first, on a session
				// that names it), then it is made read-only
				var gtids string
				if err := e.src.QueryRowContext(context.Background(), "SELECT @@GLOBAL.gtid_executed").Scan(&gtids); err != nil {
					t.Fatal(err)
				}
				replica := openServer(t, "TEST_REPLICA", "", nil)
				var rc sql.NullInt64
				if err := replica.QueryRowContext(context.Background(), "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 60)", gtids).Scan(&rc); err != nil || rc.Int64 != 0 {
					t.Fatalf("the replica did not apply the source's transactions: %v (%v)", rc, err)
				}
				setReadOnly(t, "TEST_REPLICA")
				cfg.Source = openServer(t, "TEST_REPLICA", e.srcSchema, nil)
				cfg.SkipPolicy = skip
				_, err := Run(context.Background(), cfg)
				var pe *preflight.Error
				if !errors.As(err, &pe) || pe.Check != "SOURCE_READ_ONLY_CHECK" {
					t.Fatalf("Run() = %v, want SOURCE_READ_ONLY_CHECK", err)
				}
				if got := statuses(t, e.dst, e.dstSchema); got[int(stuck)] != 20 || len(got) != 1 {
					t.Errorf("the log = %v, want 20 roots still %s: none replayed or completed", got, stuck)
				}
				if n := count(t, e.src, "SELECT COUNT(*) FROM nums"); n != 20 {
					t.Errorf("source rows = %d, want 20 untouched", n)
				}
			})
		}
	}
}
