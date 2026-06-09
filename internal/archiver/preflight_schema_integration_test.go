//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
)

// ============================================================================
// Destination Schema Compatibility Integration Tests
//
// Verifies the relaxed DEST_SCHEMA_COMPATIBILITY_CHECK against real MySQL
// information_schema metadata: the destination may drop secondary indexes,
// auto_increment, and column defaults, but must keep the primary key and may
// not add constraints the source lacks.
// ============================================================================

const schemaRelaxTable = "schema_relax_logs"

func setupSchemaCompatibilityChecker(t *testing.T, setup *IntegrationTestSetup) (*PreflightChecker, *sql.DB, *sql.DB) {
	t.Helper()

	sourceDB, ok := setup.GetDB("source")
	if !ok {
		t.Fatal("source database not found in integration setup")
	}
	destDB, ok := setup.GetDB("destination")
	if !ok {
		t.Fatal("destination database not found in integration setup")
	}

	var sourceDBName, destDBName string
	for _, dbCfg := range setup.Config.Databases {
		switch dbCfg.Name {
		case "source":
			sourceDBName = dbCfg.Database
		case "destination":
			destDBName = dbCfg.Database
		}
	}

	g := graph.NewGraph(schemaRelaxTable, "id")
	checker, err := NewPreflightChecker(sourceDB, sourceDBName, g, logger.NewDefault())
	if err != nil {
		t.Fatalf("failed to create preflight checker: %v", err)
	}
	if err := checker.ConfigureDestination(destDB, destDBName); err != nil {
		t.Fatalf("failed to configure destination: %v", err)
	}

	return checker, sourceDB, destDB
}

func dropSchemaRelaxTables(ctx context.Context, sourceDB, destDB *sql.DB) {
	_, _ = sourceDB.ExecContext(ctx, "DROP TABLE IF EXISTS "+schemaRelaxTable)
	_, _ = destDB.ExecContext(ctx, "DROP TABLE IF EXISTS "+schemaRelaxTable)
}

// TestIntegrationSchemaCompatibility_RelaxedDestination creates a source table
// with a secondary index, auto_increment, and an ON UPDATE timestamp default,
// and a destination table stripped of all three. Preflight must pass.
func TestIntegrationSchemaCompatibility_RelaxedDestination(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	checker, sourceDB, destDB := setupSchemaCompatibilityChecker(t, setup)
	dropSchemaRelaxTables(ctx, sourceDB, destDB)
	defer dropSchemaRelaxTables(ctx, sourceDB, destDB)

	if _, err := sourceDB.ExecContext(ctx, `
		CREATE TABLE `+schemaRelaxTable+` (
			id bigint NOT NULL AUTO_INCREMENT,
			aiErrorId bigint NULL,
			message varchar(255) NOT NULL,
			last_update timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY (id),
			KEY idx_aiErrorId (aiErrorId)
		) ENGINE=InnoDB`); err != nil {
		t.Fatalf("failed to create source table: %v", err)
	}

	// Destination: no secondary index, no auto_increment, no default/on-update,
	// and NOT NULL relaxed on message.
	if _, err := destDB.ExecContext(ctx, `
		CREATE TABLE `+schemaRelaxTable+` (
			id bigint NOT NULL,
			aiErrorId bigint NULL,
			message varchar(255) NULL,
			last_update timestamp NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB`); err != nil {
		t.Fatalf("failed to create destination table: %v", err)
	}

	if err := checker.ValidateDestinationSchemaCompatibility(ctx, []string{schemaRelaxTable}); err != nil {
		t.Fatalf("expected relaxed destination schema to pass, got: %v", err)
	}
}

// TestIntegrationSchemaCompatibility_StricterDestinationRejected verifies the
// guards: a destination missing the primary key, or adding a unique index the
// source lacks, must fail preflight.
func TestIntegrationSchemaCompatibility_StricterDestinationRejected(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	checker, sourceDB, destDB := setupSchemaCompatibilityChecker(t, setup)
	dropSchemaRelaxTables(ctx, sourceDB, destDB)
	defer dropSchemaRelaxTables(ctx, sourceDB, destDB)

	if _, err := sourceDB.ExecContext(ctx, `
		CREATE TABLE `+schemaRelaxTable+` (
			id bigint NOT NULL AUTO_INCREMENT,
			email varchar(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB`); err != nil {
		t.Fatalf("failed to create source table: %v", err)
	}

	t.Run("missing destination primary key", func(t *testing.T) {
		if _, err := destDB.ExecContext(ctx, `
			CREATE TABLE `+schemaRelaxTable+` (
				id bigint NOT NULL,
				email varchar(255) NOT NULL
			) ENGINE=InnoDB`); err != nil {
			t.Fatalf("failed to create destination table: %v", err)
		}
		defer func() { _, _ = destDB.ExecContext(ctx, "DROP TABLE IF EXISTS "+schemaRelaxTable) }()

		err := checker.ValidateDestinationSchemaCompatibility(ctx, []string{schemaRelaxTable})
		if err == nil {
			t.Fatal("expected primary key mismatch error, got nil")
		}
		if !strings.Contains(err.Error(), "primary key mismatch") {
			t.Fatalf("expected primary key mismatch reason, got: %v", err)
		}
	})

	t.Run("destination-only unique index", func(t *testing.T) {
		if _, err := destDB.ExecContext(ctx, `
			CREATE TABLE `+schemaRelaxTable+` (
				id bigint NOT NULL,
				email varchar(255) NOT NULL,
				PRIMARY KEY (id),
				UNIQUE KEY uniq_email (email)
			) ENGINE=InnoDB`); err != nil {
			t.Fatalf("failed to create destination table: %v", err)
		}
		defer func() { _, _ = destDB.ExecContext(ctx, "DROP TABLE IF EXISTS "+schemaRelaxTable) }()

		err := checker.ValidateDestinationSchemaCompatibility(ctx, []string{schemaRelaxTable})
		if err == nil {
			t.Fatal("expected unique index error, got nil")
		}
		if !strings.Contains(err.Error(), "unique index") {
			t.Fatalf("expected unique index reason, got: %v", err)
		}
	})
}

// TestIntegrationSchemaCompatibility_CharsetMismatch verifies real
// information_schema charset metadata drives the check: latin1 destination
// column fails under (default) count verification, passes with a warning
// under sha256.
func TestIntegrationSchemaCompatibility_CharsetMismatch(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	checker, sourceDB, destDB := setupSchemaCompatibilityChecker(t, setup)
	dropSchemaRelaxTables(ctx, sourceDB, destDB)
	defer dropSchemaRelaxTables(ctx, sourceDB, destDB)

	if _, err := sourceDB.ExecContext(ctx, `
		CREATE TABLE `+schemaRelaxTable+` (
			id bigint NOT NULL,
			message varchar(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`); err != nil {
		t.Fatalf("failed to create source table: %v", err)
	}
	if _, err := destDB.ExecContext(ctx, `
		CREATE TABLE `+schemaRelaxTable+` (
			id bigint NOT NULL,
			message varchar(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=latin1`); err != nil {
		t.Fatalf("failed to create destination table: %v", err)
	}

	err := checker.ValidateDestinationSchemaCompatibility(ctx, []string{schemaRelaxTable})
	if err == nil {
		t.Fatal("expected charset mismatch error under count verification, got nil")
	}
	if !strings.Contains(err.Error(), "character set mismatch") {
		t.Fatalf("expected character set mismatch reason, got: %v", err)
	}

	checker.SetVerification(config.VerificationConfig{Method: "sha256"})
	if err := checker.ValidateDestinationSchemaCompatibility(ctx, []string{schemaRelaxTable}); err != nil {
		t.Fatalf("expected charset mismatch to pass under sha256 verification, got: %v", err)
	}
}
