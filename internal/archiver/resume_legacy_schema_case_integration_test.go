//go:build integration

package archiver

import (
	"context"
	"testing"
)

// TestLegacyTrackingSchemaResolvesNameCaseExactly_Integration pins that
// checkLegacySchema's migration to validations.Inspector.Tables PRESERVES exact-name
// table resolution — it does not change it.
//
// information_schema.TABLES.TABLE_NAME collates utf8mb3_bin (case-sensitive), verified
// read-only on this project's MySQL 8.4.10 servers: against the existing sakila.film
// table, TABLE_NAME = 'film' returns 1 row while TABLE_NAME = 'FILM' and 'Film' each
// return 0. So 1.8's table_name = 'archiver_job' lookup was already case-exact on the
// supported lower_case_table_names = 0 configuration, and Inspector.Tables resolves the
// same way. A mixed-case Archiver_Job is, and always was, a distinct object from
// archiver_job — this is not a deviation from 1.8, it is behaviour this migration must
// not disturb.
func TestLegacyTrackingSchemaResolvesNameCaseExactly_Integration(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	destDB, ok := setup.GetDB("destination")
	if !ok {
		t.Fatal("destination database not found in integration setup")
	}

	var destDBName string
	for _, dbCfg := range setup.Config.Databases {
		if dbCfg.Name == "destination" {
			destDBName = dbCfg.Database
		}
	}
	if destDBName == "" {
		t.Fatal("could not resolve destination DB name")
	}

	// The premise (archiver_job and Archiver_Job are distinct objects) only holds when
	// the server does not fold table names at creation. Skip explicitly, never pass
	// silently, when it does.
	var lowerCaseTableNames int
	if err := destDB.QueryRowContext(ctx, "SELECT @@lower_case_table_names").Scan(&lowerCaseTableNames); err != nil {
		t.Fatalf("query @@lower_case_table_names: %v", err)
	}
	if lowerCaseTableNames != 0 {
		t.Skipf("server folds table names at creation (@@lower_case_table_names=%d); "+
			"archiver_job and Archiver_Job are not distinct objects here, so exact-name "+
			"resolution is not exercised", lowerCaseTableNames)
	}

	// Defensive pre-cleanup of prior-test residue.
	_, _ = destDB.ExecContext(ctx, "DROP TABLE IF EXISTS `archiver_job`")
	_, _ = destDB.ExecContext(ctx, "DROP TABLE IF EXISTS `Archiver_Job`")

	// Seed ONLY the mixed-case object.
	if _, err := destDB.ExecContext(ctx,
		"CREATE TABLE `Archiver_Job` (job_name VARCHAR(255) PRIMARY KEY, root_table VARCHAR(255))"); err != nil {
		t.Fatalf("seed mixed-case table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = destDB.ExecContext(context.Background(), "DROP TABLE IF EXISTS `archiver_job`")
		_, _ = destDB.ExecContext(context.Background(), "DROP TABLE IF EXISTS `Archiver_Job`")
	})

	rm, err := NewResumeManager(destDB, nil, destDBName)
	if err != nil {
		t.Fatalf("NewResumeManager: %v", err)
	}

	// Archiver_Job is a different object and must not be mistaken for the legacy
	// tracking table. 1.8 behaves the same way — this pins behaviour, it does not
	// record a change.
	if err := rm.InitializeTables(ctx); err != nil {
		t.Fatalf("InitializeTables must succeed when only a differently-cased object "+
			"exists (exact-name resolution must be preserved), got: %v", err)
	}

	// Prove the two spellings are distinct objects by reading names back and comparing
	// in Go, rather than issuing two "WHERE TABLE_NAME = ?" probes: on a platform where
	// that predicate is case-insensitive, both probes would match the same row and the
	// test would pass vacuously. Test files are outside the Step 6 consumer-policy
	// guard: this assertion is about MySQL's own case behaviour, not a goarchive fact.
	rows, err := destDB.QueryContext(ctx,
		"SELECT TABLE_NAME FROM information_schema.TABLES "+
			"WHERE TABLE_SCHEMA = ? AND LOWER(TABLE_NAME) = 'archiver_job'", destDBName)
	if err != nil {
		t.Fatalf("query table names: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table name: %v", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate table names: %v", err)
	}

	hasExact := func(want string) bool {
		for _, n := range names {
			if n == want {
				return true
			}
		}
		return false
	}
	if !hasExact("archiver_job") {
		t.Fatalf("expected an exact-spelling archiver_job table, found names: %v", names)
	}
	if !hasExact("Archiver_Job") {
		t.Fatalf("expected the mixed-case Archiver_Job to remain untouched, found names: %v", names)
	}
}
