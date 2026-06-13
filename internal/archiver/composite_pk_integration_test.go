//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	_ "github.com/go-sql-driver/mysql"
)

// ============================================================================
// Composite Primary Key Integration Tests (review P1-1)
//
// GoArchive identifies, copies, verifies, and DELETES rows by a single PK
// column (WHERE pk IN (...)). A composite (multi-column) PRIMARY KEY would make
// that filter over-match and risk deleting rows outside the archived set, so
// preflight must reject it. These tests exercise the real check against MySQL
// information_schema metadata rather than sqlmock.
// ============================================================================

const compositePKTable = "composite_pk_acct"
const singlePKTable = "single_pk_acct"

func compositePKChecker(t *testing.T, setup *IntegrationTestSetup) (*PreflightChecker, *sql.DB) {
	t.Helper()

	sourceDB, ok := setup.GetDB("source")
	if !ok {
		t.Fatal("source database not found in integration setup")
	}
	var sourceDBName string
	for _, dbCfg := range setup.Config.Databases {
		if dbCfg.Name == "source" {
			sourceDBName = dbCfg.Database
		}
	}

	g := graph.NewGraph(compositePKTable, "user")
	checker, err := NewPreflightChecker(sourceDB, sourceDBName, g, logger.NewDefault())
	if err != nil {
		t.Fatalf("failed to create preflight checker: %v", err)
	}
	return checker, sourceDB
}

func dropCompositePKTables(ctx context.Context, sourceDB *sql.DB) {
	_, _ = sourceDB.ExecContext(ctx, "DROP TABLE IF EXISTS "+compositePKTable)
	_, _ = sourceDB.ExecContext(ctx, "DROP TABLE IF EXISTS "+singlePKTable)
}

// TestIntegrationCompositePK_Rejected creates a source table with a two-column
// PRIMARY KEY (user, host) and asserts that ValidateSingleColumnPrimaryKey
// rejects it with COMPOSITE_PK_CHECK, naming the offending table.
func TestIntegrationCompositePK_Rejected(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	checker, sourceDB := compositePKChecker(t, setup)
	dropCompositePKTables(ctx, sourceDB)
	defer dropCompositePKTables(ctx, sourceDB)

	if _, err := sourceDB.ExecContext(ctx, `
		CREATE TABLE `+compositePKTable+` (
			user varchar(64) NOT NULL,
			host varchar(64) NOT NULL,
			created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (user, host)
		) ENGINE=InnoDB`); err != nil {
		t.Fatalf("failed to create composite-PK source table: %v", err)
	}

	err := checker.ValidateSingleColumnPrimaryKey(ctx, []string{compositePKTable})
	if err == nil {
		t.Fatal("expected composite PRIMARY KEY to be rejected, got nil")
	}
	var pfErr *PreflightError
	if pe, ok := err.(*PreflightError); !ok {
		t.Fatalf("expected *PreflightError, got %T: %v", err, err)
	} else {
		pfErr = pe
	}
	if pfErr.Check != "COMPOSITE_PK_CHECK" {
		t.Fatalf("expected COMPOSITE_PK_CHECK, got %q", pfErr.Check)
	}
	if !strings.Contains(err.Error(), compositePKTable) {
		t.Fatalf("expected error to name the offending table %q, got: %v", compositePKTable, err)
	}
	t.Logf("composite PK correctly rejected: %v", err)
}

// TestIntegrationCompositePK_DetectedAsChildTable proves the check covers every
// participating table, not just the root: a composite-PK table appearing only
// as a child in the table set is still rejected.
func TestIntegrationCompositePK_DetectedAsChildTable(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	checker, sourceDB := compositePKChecker(t, setup)
	dropCompositePKTables(ctx, sourceDB)
	defer dropCompositePKTables(ctx, sourceDB)

	// single_pk_acct = a valid single-column PK root; composite_pk_acct = a
	// child with a two-column PK that must still be caught.
	if _, err := sourceDB.ExecContext(ctx, `
		CREATE TABLE `+singlePKTable+` (
			id bigint NOT NULL AUTO_INCREMENT,
			name varchar(64) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB`); err != nil {
		t.Fatalf("failed to create single-PK table: %v", err)
	}
	if _, err := sourceDB.ExecContext(ctx, `
		CREATE TABLE `+compositePKTable+` (
			user varchar(64) NOT NULL,
			host varchar(64) NOT NULL,
			PRIMARY KEY (user, host)
		) ENGINE=InnoDB`); err != nil {
		t.Fatalf("failed to create composite-PK child table: %v", err)
	}

	// Order the single-PK table first to prove the scan does not stop early.
	err := checker.ValidateSingleColumnPrimaryKey(ctx, []string{singlePKTable, compositePKTable})
	if err == nil {
		t.Fatal("expected composite-PK child table to be rejected, got nil")
	}
	if !strings.Contains(err.Error(), compositePKTable) {
		t.Fatalf("expected error to name the composite child %q, got: %v", compositePKTable, err)
	}
}

// TestIntegrationCompositePK_SingleColumnPasses is the negative control: a
// genuine single-column PRIMARY KEY passes the check.
func TestIntegrationCompositePK_SingleColumnPasses(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	checker, sourceDB := compositePKChecker(t, setup)
	dropCompositePKTables(ctx, sourceDB)
	defer dropCompositePKTables(ctx, sourceDB)

	if _, err := sourceDB.ExecContext(ctx, `
		CREATE TABLE `+singlePKTable+` (
			id bigint NOT NULL AUTO_INCREMENT,
			name varchar(64) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB`); err != nil {
		t.Fatalf("failed to create single-PK table: %v", err)
	}

	if err := checker.ValidateSingleColumnPrimaryKey(ctx, []string{singlePKTable}); err != nil {
		t.Fatalf("expected single-column PK to pass, got: %v", err)
	}
}
