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
// Primary-key column case-sensitivity integration tests.
//
// MySQL's information_schema.COLUMNS collates COLUMN_NAME case-insensitively
// (utf8mb3_tolower_ci), so a naive `WHERE COLUMN_NAME = 'LOG_ID'` lookup treats
// "log_id", "LOG_ID" and "Log_Id" as the same column. ValidatePrimaryKeyColumns
// must NOT accept that: the configured primary_key has to match the real column
// name exactly, including letter case. These tests exercise the real check
// against live MySQL metadata (sqlmock cannot reproduce the collation).
// ============================================================================

const pkCaseTable = "pk_case_events"

func pkCaseChecker(t *testing.T, setup *IntegrationTestSetup, configuredPK string) (*PreflightChecker, *sql.DB) {
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

	g := graph.NewGraph(pkCaseTable, configuredPK)
	checker, err := NewPreflightChecker(sourceDB, sourceDBName, g, logger.NewDefault())
	if err != nil {
		t.Fatalf("failed to create preflight checker: %v", err)
	}
	return checker, sourceDB
}

func dropPKCaseTable(ctx context.Context, sourceDB *sql.DB) {
	_, _ = sourceDB.ExecContext(ctx, "DROP TABLE IF EXISTS "+pkCaseTable)
}

// createPKCaseTable creates the table with a lowercase "log_id" PRIMARY KEY.
func createPKCaseTable(t *testing.T, ctx context.Context, sourceDB *sql.DB) {
	t.Helper()
	if _, err := sourceDB.ExecContext(ctx, `
		CREATE TABLE `+pkCaseTable+` (
			log_id bigint NOT NULL AUTO_INCREMENT,
			payload varchar(64) NOT NULL,
			PRIMARY KEY (log_id)
		) ENGINE=InnoDB`); err != nil {
		t.Fatalf("failed to create pk-case source table: %v", err)
	}
}

// TestIntegrationPKColumnCase_Rejected proves a primary_key that differs from
// the real column only in letter case ("LOG_ID" vs "log_id") is rejected with
// the dedicated PK_COLUMN_CASE_CHECK, naming both spellings, and does NOT slip
// through as "column exists" thanks to MySQL's case-insensitive collation.
func TestIntegrationPKColumnCase_Rejected(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	checker, sourceDB := pkCaseChecker(t, setup, "LOG_ID")
	dropPKCaseTable(ctx, sourceDB)
	defer dropPKCaseTable(ctx, sourceDB)
	createPKCaseTable(t, ctx, sourceDB)

	err := checker.ValidatePrimaryKeyColumns(ctx, []string{pkCaseTable})
	if err == nil {
		t.Fatal("expected case-mismatched primary_key to be rejected, got nil")
	}
	pfErr, ok := err.(*PreflightError)
	if !ok {
		t.Fatalf("expected *PreflightError, got %T: %v", err, err)
	}
	if pfErr.Check != "PK_COLUMN_CASE_CHECK" {
		t.Fatalf("expected PK_COLUMN_CASE_CHECK, got %q (%v)", pfErr.Check, err)
	}
	if !strings.Contains(err.Error(), "LOG_ID") || !strings.Contains(err.Error(), "log_id") {
		t.Fatalf("expected error to name both configured and actual column, got: %v", err)
	}
	if strings.Contains(err.Error(), "over-match") {
		t.Fatalf("case-mismatch error should not use the data-loss over-match wording, got: %v", err)
	}
}

// TestIntegrationPKColumnCase_MixedCaseRejected covers a mixed-case spelling
// ("Log_Id") to confirm equality is never allowed for any casing variant.
func TestIntegrationPKColumnCase_MixedCaseRejected(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	checker, sourceDB := pkCaseChecker(t, setup, "Log_Id")
	dropPKCaseTable(ctx, sourceDB)
	defer dropPKCaseTable(ctx, sourceDB)
	createPKCaseTable(t, ctx, sourceDB)

	err := checker.ValidatePrimaryKeyColumns(ctx, []string{pkCaseTable})
	if err == nil {
		t.Fatal("expected mixed-case primary_key to be rejected, got nil")
	}
	if pfErr, ok := err.(*PreflightError); !ok || pfErr.Check != "PK_COLUMN_CASE_CHECK" {
		t.Fatalf("expected PK_COLUMN_CASE_CHECK, got: %v", err)
	}
}

// TestIntegrationPKColumnCase_ExactMatchPasses is the negative control: the
// exact column name (matching case) passes the check.
func TestIntegrationPKColumnCase_ExactMatchPasses(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	checker, sourceDB := pkCaseChecker(t, setup, "log_id")
	dropPKCaseTable(ctx, sourceDB)
	defer dropPKCaseTable(ctx, sourceDB)
	createPKCaseTable(t, ctx, sourceDB)

	if err := checker.ValidatePrimaryKeyColumns(ctx, []string{pkCaseTable}); err != nil {
		t.Fatalf("expected exact-case primary_key to pass, got: %v", err)
	}
}

// TestIntegrationPKColumn_MissingRejected proves a primary_key that matches no
// column even case-insensitively is reported as not-found (PK_COLUMN_CHECK),
// distinct from a mere casing mismatch.
func TestIntegrationPKColumn_MissingRejected(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	checker, sourceDB := pkCaseChecker(t, setup, "sinan_id")
	dropPKCaseTable(ctx, sourceDB)
	defer dropPKCaseTable(ctx, sourceDB)
	createPKCaseTable(t, ctx, sourceDB)

	err := checker.ValidatePrimaryKeyColumns(ctx, []string{pkCaseTable})
	if err == nil {
		t.Fatal("expected non-existent primary_key to be rejected, got nil")
	}
	pfErr, ok := err.(*PreflightError)
	if !ok {
		t.Fatalf("expected *PreflightError, got %T: %v", err, err)
	}
	if pfErr.Check != "PK_COLUMN_CHECK" {
		t.Fatalf("expected PK_COLUMN_CHECK, got %q (%v)", pfErr.Check, err)
	}
}
