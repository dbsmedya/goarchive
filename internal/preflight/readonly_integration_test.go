//go:build integration

package preflight

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

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

// onlyStatement is the one statement a log should hold: the library's configuration read.
func onlyStatement(t *testing.T, log *statementLog, what string) {
	t.Helper()
	got := log.all()
	if len(got) != 1 || !strings.Contains(got[0], "@@GLOBAL.read_only, @@GLOBAL.super_read_only") {
		t.Errorf("%s sent %q, want only its configuration read: the refusal comes before anything else", what, got)
	}
}

// TestMySQLADestinationThatIsReadOnlyRefusesEveryJobType is spec §8's *read-only destination*, plain half: every
// job type against a destination whose super_read_only is on is refused before any other destination statement;
// with read_only alone on, the outcome is today's (root may write, so preflight passes).
func TestMySQLADestinationThatIsReadOnlyRefusesEveryJobType(t *testing.T) {
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
		t.Run(string(typ), func(t *testing.T) {
			s, d, spec := orderPair(t, "rodst"+string(typ), typ)
			var srcLog, dstLog statementLog
			src, dst := recorded(t, "TEST_SOURCE", schemaOf(t, s), &srcLog), recorded(t, "TEST_DEST", schemaOf(t, d), &dstLog)
			cfg := Config{Source: src, SourceDatabase: schemaOf(t, s), Destination: dst, DestinationDatabase: schemaOf(t, d), JobSchema: schemaOf(t, d),
				Spec: spec, Verification: jobspec.VerifyCount, Processing: defaultProcessing}

			makeReadOnly(t, "TEST_DEST", "super_read_only")
			_, err := Run(context.Background(), cfg)
			e := refusedWith(t, err, "DEST_READ_ONLY_CHECK")
			// turning super_read_only on turns read_only on with it, and the message names both
			if !strings.Contains(e.Message, "read_only and super_read_only are ON") || !strings.Contains(e.Message, "the destination is read-only") {
				t.Errorf("the refusal does not say which variables are on:\n%s", e.Message)
			}
			onlyStatement(t, &dstLog, "the destination")

			// with the policy checks skipped it is still refused
			cfg.SkipPolicy = true
			_, err = Run(context.Background(), cfg)
			_ = refusedWith(t, err, "DEST_READ_ONLY_CHECK")
		})
		t.Run(string(typ)+"/read_only alone", func(t *testing.T) {
			s, d, spec := orderPair(t, "rodstro"+string(typ), typ)
			makeReadOnly(t, "TEST_DEST", "read_only")
			if _, err := runOn(t, s, d, spec); err != nil {
				t.Errorf("Run() with read_only alone on the destination = %v, want today's outcome: success", err)
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
