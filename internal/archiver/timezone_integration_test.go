//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/dbsmedya/goarchive/internal/archiver/testsupport"
	"github.com/dbsmedya/goarchive/internal/config"
)

// Issue #16. MySQL converts TIMESTAMP between UTC and the SESSION time zone on
// every read and write. GoArchive copies rows through Go as wall-clock values,
// so unless both sessions share one zone the stored instant shifts by the
// difference — and SHA256 verification cannot see it, because each side hashes
// its own session-local rendering. The estate runs the destination in +03:00
// (tests/docker_files/my.cnf.d/db2.cnf) precisely so this test has teeth.
//
// Every assertion is on UNIX_TIMESTAMP(), which reads the stored instant
// independently of any session zone. Rows are seeded by epoch for the same
// reason: FROM_UNIXTIME round-trips through one session and lands on the
// instant it was given.

const tzScenarioJob = "tz_instant_roundtrip"

// tzScenarioRows maps id -> expected UNIX_TIMESTAMP(ts), rendered as MySQL
// renders a TIMESTAMP(6). Row 1 carries fractional seconds. Rows 2 and 3 are
// the two instants that BOTH render as 02:30:00 in Europe/Berlin on
// 2025-10-26 — the DST fall-back overlap hour. A wall-clock copy FROM a
// Berlin source session sends "02:30:00" twice, and the destination can only
// store one instant for both.
var tzScenarioRows = map[int64]string{
	1: "1767225600.123456", // 2026-01-01 00:00:00.123456 UTC
	2: "1761438600.000000", // 2025-10-26 00:30:00 UTC = 02:30 CEST
	3: "1761442200.000000", // 2025-10-26 01:30:00 UTC = 02:30 CET
}

func tzScenarioTables(t *testing.T, ctx context.Context, dbs ...*sql.DB) {
	t.Helper()
	const ddl = "CREATE TABLE tz_events (" +
		"id BIGINT NOT NULL PRIMARY KEY, " +
		"ts TIMESTAMP(6) NULL, " +
		"dt DATETIME(6) NULL" +
		") ENGINE=InnoDB"
	for _, db := range dbs {
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS tz_events"); err != nil {
			t.Fatalf("drop tz_events: %v", err)
		}
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			t.Fatalf("create tz_events: %v", err)
		}
	}
	t.Cleanup(func() {
		for _, db := range dbs {
			_, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS tz_events")
		}
	})
}

func seedTZScenario(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `INSERT INTO tz_events (id, ts, dt) VALUES
		(1, FROM_UNIXTIME(1767225600.123456), '2026-01-01 00:00:00.123456'),
		(2, FROM_UNIXTIME(1761438600),        '2025-10-26 00:30:00'),
		(3, FROM_UNIXTIME(1761442200),        '2025-10-26 01:30:00')`); err != nil {
		t.Fatalf("seed tz_events: %v", err)
	}
}

func readTZInstants(t *testing.T, ctx context.Context, db *sql.DB) map[int64]string {
	t.Helper()
	rows, err := db.QueryContext(ctx, "SELECT id, UNIX_TIMESTAMP(ts) FROM tz_events ORDER BY id")
	if err != nil {
		t.Fatalf("read tz_events: %v", err)
	}
	defer func() { _ = rows.Close() }()
	got := map[int64]string{}
	for rows.Next() {
		var id int64
		var unix string
		if err := rows.Scan(&id, &unix); err != nil {
			t.Fatalf("scan tz_events: %v", err)
		}
		got[id] = unix
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate tz_events: %v", err)
	}
	return got
}

func assertTZInstants(t *testing.T, label string, got map[int64]string) {
	t.Helper()
	if len(got) != len(tzScenarioRows) {
		t.Fatalf("%s: expected %d rows, got %d: %v", label, len(tzScenarioRows), len(got), got)
	}
	for id, want := range tzScenarioRows {
		if got[id] != want {
			t.Errorf("%s: row %d UNIX_TIMESTAMP(ts) = %s, want %s — the stored instant changed", label, id, got[id], want)
		}
	}
}

func readGlobalTimeZone(t *testing.T, ctx context.Context, db *sql.DB) string {
	t.Helper()
	var zone string
	if err := db.QueryRowContext(ctx, "SELECT @@global.time_zone").Scan(&zone); err != nil {
		t.Fatalf("read @@global.time_zone: %v", err)
	}
	return zone
}

func TestSessionTimeZone_TimestampInstantSurvivesArchive_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	setup, ctx := SetupIntegrationTest(t)
	t.Cleanup(setup.Close)
	sourceDB, destDB := resumeScenarioDBs(t, setup)

	// Only meaningful across a zone boundary: refuse to pass vacuously.
	if zone := readGlobalTimeZone(t, ctx, destDB); zone == "SYSTEM" || zone == "+00:00" || zone == "UTC" {
		t.Fatalf("destination @@global.time_zone is %q; db2 must run in a non-UTC zone (tests/docker_files/my.cnf.d/db2.cnf) for this test to prove anything", zone)
	}

	// Leg 1: the estate as it is — UTC source, +03:00 destination.
	// Leg 2: the SOURCE in a DST zone. Rows 2 and 3 both render as
	// 2025-10-26 02:30:00 there, so a wall-clock copy sends the same value
	// twice and the destination can only store one instant for both.
	zones := []struct {
		name   string
		db     *sql.DB // server whose global zone the leg changes; nil = none
		global string
	}{
		{name: "fixed-offset-destination"},
		{name: "dst-overlap-source", db: sourceDB, global: "Europe/Berlin"},
	}
	for _, zone := range zones {
		t.Run(zone.name, func(t *testing.T) {
			if zone.db != nil {
				previous := readGlobalTimeZone(t, ctx, zone.db)
				// Constants under test control; SET does not take a placeholder.
				if _, err := zone.db.ExecContext(ctx, fmt.Sprintf("SET GLOBAL time_zone = '%s'", zone.global)); err != nil {
					t.Fatalf("SET GLOBAL time_zone = %q: %v", zone.global, err)
				}
				t.Cleanup(func() {
					// The estate is a singleton: a silent restore failure would
					// leave the source in Berlin for every later test.
					if _, err := zone.db.ExecContext(context.Background(), fmt.Sprintf("SET GLOBAL time_zone = '%s'", previous)); err != nil {
						t.Errorf("restore @@global.time_zone to %q: %v", previous, err)
					}
					if got := readGlobalTimeZone(t, context.Background(), zone.db); got != previous {
						t.Errorf("@@global.time_zone is %q after cleanup, want %q", got, previous)
					}
				})
			}
			tzScenarioTables(t, ctx, sourceDB, destDB)
			seedTZScenario(t, ctx, sourceDB)
			assertTZInstants(t, "source before archive", readTZInstants(t, ctx, sourceDB))

			// A fresh Manager AFTER any SET GLOBAL: only new sessions inherit it.
			dbManager, cfg := resumeScenarioDBManager(t, setup, "sha256", 100)
			testsupport.CleanupArchiverState(t, destDB, tzScenarioJob)
			jobCfg := &config.JobConfig{RootTable: "tz_events", PrimaryKey: "id", Where: "id > 0"}

			orch, err := NewOrchestrator(cfg, tzScenarioJob, jobCfg, dbManager)
			if err != nil {
				t.Fatalf("NewOrchestrator: %v", err)
			}
			if err := orch.Initialize(); err != nil {
				t.Fatalf("Initialize: %v", err)
			}
			result, err := orch.Execute(ctx, nil)
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if !result.Success {
				t.Fatalf("expected success, got errors: %v", result.Errors)
			}
			if result.RecordsDeleted != int64(len(tzScenarioRows)) {
				t.Fatalf("expected %d source rows deleted, got %d", len(tzScenarioRows), result.RecordsDeleted)
			}

			assertTZInstants(t, "destination after archive", readTZInstants(t, ctx, destDB))
			if remaining := readTZInstants(t, ctx, sourceDB); len(remaining) != 0 {
				t.Fatalf("source still holds %d rows after archive", len(remaining))
			}
		})
	}
}
