//go:build integration

package archiver

import (
	"context"
	"strings"
	"testing"
)

func TestMetaVersion_FreshSchemaIsStamped_Integration(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	t.Cleanup(setup.Close)

	destDB, ok := setup.GetDB("destination")
	if !ok {
		t.Fatal("destination database not found in integration setup")
	}
	destDBName := destinationSchemaName(t, setup)

	_, _ = destDB.ExecContext(ctx, "DROP TABLE IF EXISTS `goarchive_meta`")
	t.Cleanup(func() {
		_, _ = destDB.ExecContext(context.Background(), "DROP TABLE IF EXISTS `goarchive_meta`")
	})

	rm, err := NewResumeManager(destDB, nil, destDBName)
	if err != nil {
		t.Fatalf("NewResumeManager: %v", err)
	}
	if err := rm.InitializeTables(ctx); err != nil {
		t.Fatalf("InitializeTables on a fresh schema must succeed: %v", err)
	}

	var got string
	if err := destDB.QueryRowContext(ctx,
		"SELECT schema_version FROM `goarchive_meta` WHERE id = 1").Scan(&got); err != nil {
		t.Fatalf("read schema_version: %v", err)
	}
	if got != trackingSchemaVersion {
		t.Fatalf("expected schema_version %q, got %q", trackingSchemaVersion, got)
	}
}

func TestMetaVersion_PopulatedPreMarkerSchemaIsRefused_Integration(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	t.Cleanup(setup.Close)

	destDB, ok := setup.GetDB("destination")
	if !ok {
		t.Fatal("destination database not found in integration setup")
	}
	destDBName := destinationSchemaName(t, setup)

	_, _ = destDB.ExecContext(ctx, "DROP TABLE IF EXISTS `goarchive_meta`")
	_, _ = destDB.ExecContext(ctx, "DROP TABLE IF EXISTS `archiver_job`")
	if _, err := destDB.ExecContext(ctx, `CREATE TABLE archiver_job (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		job_name VARCHAR(255) NOT NULL,
		root_table VARCHAR(255) NOT NULL,
		job_type VARCHAR(32) NOT NULL DEFAULT 'archive',
		last_processed_root_pk_id VARCHAR(255) DEFAULT NULL,
		job_status TINYINT NOT NULL DEFAULT 0,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		last_heartbeat_at DATETIME NULL,
		UNIQUE KEY uk_job_name (job_name)
	) ENGINE=InnoDB`); err != nil {
		t.Fatalf("seed pre-marker archiver_job: %v", err)
	}
	if _, err := destDB.ExecContext(ctx,
		"INSERT INTO archiver_job (job_name, root_table, last_processed_root_pk_id) VALUES (?, ?, ?)",
		"premarker_job", "customers", "4242"); err != nil {
		t.Fatalf("seed job row: %v", err)
	}
	t.Cleanup(func() {
		_, _ = destDB.ExecContext(context.Background(), "DROP TABLE IF EXISTS `goarchive_meta`")
		_, _ = destDB.ExecContext(context.Background(), "DROP TABLE IF EXISTS `archiver_job`")
	})

	rm, err := NewResumeManager(destDB, nil, destDBName)
	if err != nil {
		t.Fatalf("NewResumeManager: %v", err)
	}
	err = rm.InitializeTables(ctx)
	if err == nil {
		t.Fatal("populated tracking tables with no marker must be refused, not stamped")
	}
	if !strings.Contains(err.Error(), "no schema_version marker") || !strings.Contains(err.Error(), "README_JOBS_SCHEMA") {
		t.Fatalf("refusal must say why and point at the procedures: %v", err)
	}

	var markers int
	if err := destDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM `goarchive_meta`").Scan(&markers); err != nil {
		t.Fatalf("count markers: %v", err)
	}
	if markers != 0 {
		t.Fatalf("a refused schema must not be stamped; found %d marker rows", markers)
	}

	var checkpoint string
	if err := destDB.QueryRowContext(ctx,
		"SELECT last_processed_root_pk_id FROM archiver_job WHERE job_name = ?",
		"premarker_job").Scan(&checkpoint); err != nil {
		t.Fatalf("read preserved checkpoint: %v", err)
	}
	if checkpoint != "4242" {
		t.Fatalf("a refusal must not disturb existing job state; checkpoint is %q", checkpoint)
	}
}

func TestMetaVersion_NewerRevisionIsRefused_Integration(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	t.Cleanup(setup.Close)

	destDB, ok := setup.GetDB("destination")
	if !ok {
		t.Fatal("destination database not found in integration setup")
	}
	destDBName := destinationSchemaName(t, setup)

	_, _ = destDB.ExecContext(ctx, "DROP TABLE IF EXISTS `goarchive_meta`")
	if _, err := destDB.ExecContext(ctx, `CREATE TABLE goarchive_meta (
		id TINYINT NOT NULL PRIMARY KEY,
		schema_version VARCHAR(16) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	) ENGINE=InnoDB`); err != nil {
		t.Fatalf("seed marker table: %v", err)
	}
	if _, err := destDB.ExecContext(ctx,
		"INSERT INTO goarchive_meta (id, schema_version) VALUES (1, ?)", "99.0"); err != nil {
		t.Fatalf("seed newer revision: %v", err)
	}
	t.Cleanup(func() {
		_, _ = destDB.ExecContext(context.Background(), "DROP TABLE IF EXISTS `goarchive_meta`")
		_, _ = destDB.ExecContext(context.Background(), "DROP TABLE IF EXISTS `archiver_job`")
	})

	rm, err := NewResumeManager(destDB, nil, destDBName)
	if err != nil {
		t.Fatalf("NewResumeManager: %v", err)
	}
	err = rm.InitializeTables(ctx)
	if err == nil {
		t.Fatal("expected refusal against an unrecognized schema_version")
	}
	if !strings.Contains(err.Error(), "99.0") {
		t.Fatalf("refusal must name the revision it found: %v", err)
	}
}

// The 2.0 -> 2.2 procedure documented in docs/README_JOBS_SCHEMA.md, executed
// verbatim: the refusal, the two statements, and the run that follows.
func TestMetaVersion_OlderRevisionIsRefusedUntilRemedy_Integration(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	t.Cleanup(setup.Close)

	destDB, ok := setup.GetDB("destination")
	if !ok {
		t.Fatal("destination database not found in integration setup")
	}
	destDBName := destinationSchemaName(t, setup)

	_, _ = destDB.ExecContext(ctx, "DROP TABLE IF EXISTS `goarchive_meta`")
	_, _ = destDB.ExecContext(ctx, "DROP TABLE IF EXISTS `archiver_job`")
	if _, err := destDB.ExecContext(ctx, `CREATE TABLE archiver_job (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		job_name VARCHAR(255) NOT NULL,
		root_table VARCHAR(255) NOT NULL,
		job_type VARCHAR(32) NOT NULL DEFAULT 'archive',
		last_processed_root_pk_id VARCHAR(255) DEFAULT NULL,
		job_status TINYINT NOT NULL DEFAULT 0,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		last_heartbeat_at DATETIME NULL,
		UNIQUE KEY uk_job_name (job_name)
	) ENGINE=InnoDB`); err != nil {
		t.Fatalf("seed 2.0-era archiver_job: %v", err)
	}
	// A heartbeat written by a 2.1 binary: local wall-clock of unknown zone.
	if _, err := destDB.ExecContext(ctx,
		"INSERT INTO archiver_job (job_name, root_table, last_processed_root_pk_id, last_heartbeat_at) VALUES (?, ?, ?, NOW())",
		"old_job", "customers", "4242"); err != nil {
		t.Fatalf("seed job row: %v", err)
	}
	if _, err := destDB.ExecContext(ctx, `CREATE TABLE goarchive_meta (
		id TINYINT NOT NULL PRIMARY KEY,
		schema_version VARCHAR(16) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	) ENGINE=InnoDB`); err != nil {
		t.Fatalf("seed marker table: %v", err)
	}
	if _, err := destDB.ExecContext(ctx,
		"INSERT INTO goarchive_meta (id, schema_version) VALUES (1, ?)", "2.0"); err != nil {
		t.Fatalf("seed 2.0 marker: %v", err)
	}
	t.Cleanup(func() {
		_, _ = destDB.ExecContext(context.Background(), "DROP TABLE IF EXISTS `goarchive_meta`")
		_, _ = destDB.ExecContext(context.Background(), "DROP TABLE IF EXISTS `archiver_job`")
	})

	rm, err := NewResumeManager(destDB, nil, destDBName)
	if err != nil {
		t.Fatalf("NewResumeManager: %v", err)
	}
	err = rm.InitializeTables(ctx)
	if err == nil {
		t.Fatal("schema_version 2.0 must be refused")
	}
	if !strings.Contains(err.Error(), `"2.0"`) || !strings.Contains(err.Error(), trackingSchemaVersion) {
		t.Fatalf("refusal must name both revisions: %v", err)
	}

	// The documented procedure, verbatim.
	if _, err := destDB.ExecContext(ctx, "UPDATE archiver_job SET last_heartbeat_at = NULL"); err != nil {
		t.Fatalf("remedy step 1: %v", err)
	}
	if _, err := destDB.ExecContext(ctx, "UPDATE goarchive_meta SET schema_version = ? WHERE id = 1", trackingSchemaVersion); err != nil {
		t.Fatalf("remedy step 2: %v", err)
	}

	if err := rm.InitializeTables(ctx); err != nil {
		t.Fatalf("after the remedy the schema must be accepted: %v", err)
	}
	var got string
	if err := destDB.QueryRowContext(ctx, "SELECT schema_version FROM `goarchive_meta` WHERE id = 1").Scan(&got); err != nil {
		t.Fatalf("read schema_version: %v", err)
	}
	if got != trackingSchemaVersion {
		t.Fatalf("expected schema_version %q, got %q", trackingSchemaVersion, got)
	}
	var voided bool
	if err := destDB.QueryRowContext(ctx, "SELECT last_heartbeat_at IS NULL FROM archiver_job WHERE job_name = 'old_job'").Scan(&voided); err != nil {
		t.Fatalf("read heartbeat: %v", err)
	}
	if !voided {
		t.Fatal("the remedy voids pre-UTC heartbeats; it was not applied")
	}
}

func destinationSchemaName(t *testing.T, setup *IntegrationTestSetup) string {
	t.Helper()
	for _, dbCfg := range setup.Config.Databases {
		if dbCfg.Name == "destination" {
			return dbCfg.Database
		}
	}
	t.Fatal("could not resolve destination DB name")
	return ""
}
