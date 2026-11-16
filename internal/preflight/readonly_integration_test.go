//go:build integration

package preflight

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// The two read-only checks against real servers (spec §4.5, §8; GR C4), on both estates: the destination server
// and the estate's replica are plain servers on each. A test that makes a server read-only restores it when it
// ends and reads the restore back, because a server left read-only breaks every later test.

// makeReadOnly turns one of the two variables on for a server, as a SET GLOBAL, and restores both variables to
// the values the server had when the test ends, reading them back. mode is "read_only" or "super_read_only"; the
// second implies the first, as MySQL has it.
func makeReadOnly(t *testing.T, prefix, mode string) {
	t.Helper()
	db := openServer(t, prefix, "")
	var ro, sro int
	if err := db.QueryRowContext(context.Background(), "SELECT @@GLOBAL.read_only, @@GLOBAL.super_read_only").Scan(&ro, &sro); err != nil {
		t.Fatal(err)
	}
	execAll(t, db, "SET GLOBAL "+mode+" = 1")
	t.Cleanup(func() {
		// super_read_only first: it holds read_only on while it is on.
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

// onlyStatement is what a refused read-only source was asked: the library's configuration read first, and then, for the
// cause (GR C8), only the plugin-state reads of the group facts, which a fence does not hold. Nothing that reads a table of
// the job comes before the refusal.
func onlyStatement(t *testing.T, log *statementLog, what string) {
	t.Helper()
	got := log.all()
	if len(got) == 0 || !strings.Contains(got[0], "@@GLOBAL.read_only, @@GLOBAL.super_read_only") {
		t.Errorf("%s sent %q, want its configuration read first", what, got)
		return
	}
	for _, statement := range got[1:] {
		if !strings.Contains(statement, "group_replication") && !strings.Contains(statement, "PLUGINS") {
			t.Errorf("%s sent %q before its refusal, want only the plugin-state reads the cause needs", what, statement)
		}
	}
}

// errantGTIDs are the transactions the replica has executed that its source has not: the ones made on the
// replica itself. A statement of a test on the replica is one, and it breaks replication from then on. The
// replica's set is read first, so the source's, read after it, holds everything the replica could have applied.
func errantGTIDs(t *testing.T, replica, source *sql.DB) string {
	t.Helper()
	var onReplica, onSource, errant string
	ctx := context.Background()
	if err := replica.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&onReplica); err != nil {
		t.Fatal(err)
	}
	if err := source.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&onSource); err != nil {
		t.Fatal(err)
	}
	if err := source.QueryRowContext(ctx, "SELECT GTID_SUBTRACT(?, ?)", onReplica, onSource).Scan(&errant); err != nil {
		t.Fatal(err)
	}
	return errant
}

// replicatedAccount creates, on the source, an account that may do what preflight asks of a destination (the
// grants of "with CREATE it passes" in permissions_integration_test.go) on schema, and waits until the replica has
// it. The statements run on the source and reach the replica by replication: none is ever sent to the replica.
// It returns the account's name and password and drops the account, on the source, when the test ends.
func replicatedAccount(t *testing.T, admin, replica *sql.DB, suffix, schema string) (user, pass string) {
	t.Helper()
	user = acctName(suffix)
	pass = fmt.Sprintf("ga7p-%d-%d", os.Getpid(), time.Now().UnixNano())
	account := "'" + user + "'@'%'"
	execAll(t, admin, "DROP USER IF EXISTS "+account, "CREATE USER "+account+" IDENTIFIED BY '"+pass+"'",
		"GRANT CREATE, SELECT, INSERT, UPDATE, TRIGGER ON `"+schema+"`.* TO "+account)
	t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+account) })
	waitReplica(t, admin, replica)
	return user, pass
}

// TestMySQLADestinationThatIsReadOnlyRefusesEveryJobType is spec §8's *read-only destination*, plain half, on the
// estate's replica: every job type against a destination whose super_read_only is on is refused before any other
// destination statement, also under a skip flag; with read_only alone on, the outcome is today's, preflight passes.
//
// The destination is the replica because no test changes a global of db1 or db2, which every package's tests write
// to. The job's schema is created on the source and reaches the replica by replication; the account that
// preflight runs as there has no SUPER and no CONNECTION_ADMIN, is created on the source too, and the test checks
// that the replica holds no transaction of its own afterwards. Nothing but preflight's reads reaches the replica.
func TestMySQLADestinationThatIsReadOnlyRefusesEveryJobType(t *testing.T) {
	replica := openServer(t, "TEST_REPLICA", "") // the replica is this test's, whole, before anything else is made
	admin := openServer(t, "TEST_SOURCE", "")
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
		t.Run(string(typ), func(t *testing.T) {
			const cs = "CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"
			s := scratch(t, "TEST_SOURCE", "rodst"+string(typ))
			execAll(t, s, orderTables(cs)...)
			schema := schemaOf(t, s)
			user, pass := replicatedAccount(t, admin, replica, "rodst"+string(typ), schema)
			errantBefore := errantGTIDs(t, replica, admin)
			spec := job(t, typ, orderJob)
			cfg := func(dst *sql.DB) Config {
				return Config{Source: s, SourceDatabase: schema, Destination: dst, DestinationDatabase: schema, JobSchema: schema,
					Spec: spec, Verification: jobspec.VerifyCount, Processing: defaultProcessing}
			}

			t.Run("super_read_only", func(t *testing.T) {
				var dstLog statementLog
				c := cfg(recorded(t, "TEST_REPLICA", schema, &dstLog))
				makeReadOnly(t, "TEST_REPLICA", "super_read_only")
				_, err := Run(context.Background(), c)
				e := refusedWith(t, err, "DEST_READ_ONLY_CHECK")
				// turning super_read_only on turns read_only on with it, and the message names both
				if !strings.Contains(e.Message, "read_only and super_read_only are ON") || !strings.Contains(e.Message, "the destination is read-only") {
					t.Errorf("the refusal does not say which variables are on:\n%s", e.Message)
				}
				onlyStatement(t, &dstLog, "the destination")

				// with the policy checks skipped it is still refused
				c.SkipPolicy = true
				_, err = Run(context.Background(), c)
				_ = refusedWith(t, err, "DEST_READ_ONLY_CHECK")
			})
			t.Run("read_only alone", func(t *testing.T) {
				c := cfg(acctOpen(t, "TEST_REPLICA", schema, user, pass))
				makeReadOnly(t, "TEST_REPLICA", "read_only")
				if _, err := Run(context.Background(), c); err != nil {
					t.Errorf("Run() with read_only alone on the destination = %v, want today's outcome: success", err)
				}
			})
			if got := errantGTIDs(t, replica, admin); got != errantBefore {
				t.Errorf("the replica holds transactions of its own after the test: %q, before %q", got, errantBefore)
			}
		})
	}
}

// waitReplica waits for the estate's replica to apply what the source executed.
func waitReplica(t *testing.T, source *sql.DB, replica *sql.DB) {
	t.Helper()
	var gtids string
	if err := source.QueryRowContext(context.Background(), "SELECT @@GLOBAL.gtid_executed").Scan(&gtids); err != nil {
		t.Fatal(err)
	}
	var rc sql.NullInt64
	if err := replica.QueryRowContext(context.Background(), "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 60)", gtids).Scan(&rc); err != nil || rc.Int64 != 0 {
		t.Fatalf("the replica did not apply the source's transactions: %v (%v)", rc, err)
	}
}

// TestMySQLAReadOnlyReplicaAsSourceRefusesAnArchiveAndAPurge is spec §8's *read-only source*, plain half: the
// estate's replica as a source, with either variable on, refuses an archive and a purge before anything is
// read from it, and a copy-only job there passes. The estate's replica is writable as it is left (M10); the test
// turns the variables on and restores them.
func TestMySQLAReadOnlyReplicaAsSourceRefusesAnArchiveAndAPurge(t *testing.T) {
	s := scratch(t, "TEST_SOURCE", "rosrc")
	execAll(t, s, orderTables("CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")...)
	d := scratch(t, "TEST_DEST", "rosrcdst")
	execAll(t, d, orderTables("CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")...)
	srcSchema := schemaOf(t, s)
	waitReplica(t, s, openServer(t, "TEST_REPLICA", ""))
	for _, mode := range []string{"read_only", "super_read_only"} {
		t.Run(mode, func(t *testing.T) {
			makeReadOnly(t, "TEST_REPLICA", mode)
			for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge} {
				var srcLog statementLog
				replica := recorded(t, "TEST_REPLICA", srcSchema, &srcLog)
				_, err := Run(context.Background(), Config{Source: replica, SourceDatabase: srcSchema, Destination: d, DestinationDatabase: schemaOf(t, d),
					JobSchema: schemaOf(t, d), Spec: job(t, typ, orderJob), Verification: jobspec.VerifyCount, Processing: defaultProcessing})
				e := refusedWith(t, err, "SOURCE_READ_ONLY_CHECK")
				if !strings.Contains(e.Message, mode+" is ON") && !strings.Contains(e.Message, mode+" are ON") {
					t.Errorf("%s: the refusal does not name %s:\n%s", typ, mode, e.Message)
				}
				onlyStatement(t, &srcLog, "the replica")
			}
			// a copy-only job reads the replica and runs
			if _, err := runOn(t, openServer(t, "TEST_REPLICA", srcSchema), d, job(t, jobspec.CopyOnly, orderJob)); err != nil {
				t.Errorf("copy-only against the read-only replica = %v, want success", err)
			}
		})
	}
}
