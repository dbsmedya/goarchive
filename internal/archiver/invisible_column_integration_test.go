//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	_ "github.com/go-sql-driver/mysql"
)

// ============================================================================
// Invisible-column preflight integration tests (issue #23).
//
// GoArchive copies rows with SELECT *, which MySQL excludes INVISIBLE columns
// from, so an invisible column's stored value is never copied nor hashed, yet
// the source row is deleted — silent data loss. Until explicit-column support
// exists, ValidateNoInvisibleColumns must reject any participating table that
// has an invisible column. sqlmock cannot reproduce this: the check keys off the
// real information_schema.COLUMNS.EXTRA token, which only live MySQL populates.
// ============================================================================

const invTable = "inv_records"

func invMakeChecker(t *testing.T, setup *IntegrationTestSetup) (*PreflightChecker, *sql.DB) {
	t.Helper()
	srcDB, ok := setup.GetDB("source")
	if !ok {
		t.Fatal("source database not found in integration setup")
	}
	var srcSchema string
	for _, c := range setup.Config.Databases {
		if c.Name == "source" {
			srcSchema = c.Database
		}
	}
	if srcSchema == "" {
		t.Fatal("could not resolve source schema name")
	}
	g := graph.NewGraph(invTable, "id")
	checker, err := NewPreflightChecker(srcDB, srcSchema, g, logger.NewDefault())
	if err != nil {
		t.Fatalf("new preflight checker: %v", err)
	}
	return checker, srcDB
}

func dropInvTable(ctx context.Context, db *sql.DB) {
	_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+invTable)
}

// assertInvisibleRejected runs the check and asserts INVISIBLE_COLUMN_CHECK
// naming the given column.
func assertInvisibleRejected(t *testing.T, ctx context.Context, checker *PreflightChecker, wantCol string) {
	t.Helper()
	err := checker.ValidateNoInvisibleColumns(ctx, []string{invTable})
	if err == nil {
		t.Fatal("expected INVISIBLE_COLUMN_CHECK for a participating invisible column, got nil — " +
			"SELECT * would drop its value before the source row is deleted")
	}
	var pfErr *PreflightError
	if !errors.As(err, &pfErr) {
		t.Fatalf("expected *PreflightError, got %T: %v", err, err)
	}
	if pfErr.Check != "INVISIBLE_COLUMN_CHECK" {
		t.Fatalf("expected INVISIBLE_COLUMN_CHECK, got %q: %v", pfErr.Check, err)
	}
	if !strings.Contains(err.Error(), invTable+"."+wantCol) {
		t.Fatalf("expected error to name the offending column %s.%s, got: %v", invTable, wantCol, err)
	}
}

// TestIntegrationInvisibleColumn_Rejected covers both a plain invisible non-PK
// column (with a nondefault value, the classic silent-loss case) and a generated
// invisible column, plus a clean negative control.
func TestIntegrationInvisibleColumn_Rejected(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	checker, srcDB := invMakeChecker(t, setup)
	dropInvTable(ctx, srcDB)
	defer dropInvTable(ctx, srcDB)

	// Plain invisible non-PK column holding a nondefault value.
	t.Run("plain_invisible_column", func(t *testing.T) {
		dropInvTable(ctx, srcDB)
		if _, err := srcDB.ExecContext(ctx,
			"CREATE TABLE "+invTable+" (id BIGINT PRIMARY KEY, "+
				"payload VARCHAR(255) INVISIBLE DEFAULT 'default-value') ENGINE=InnoDB"); err != nil {
			t.Fatalf("create table: %v", err)
		}
		if _, err := srcDB.ExecContext(ctx,
			"INSERT INTO "+invTable+" (id, payload) VALUES (1, 'original-secret')"); err != nil {
			t.Fatalf("insert: %v", err)
		}
		assertInvisibleRejected(t, ctx, checker, "payload")
	})

	// Generated invisible column (EXTRA = "STORED GENERATED INVISIBLE").
	t.Run("generated_invisible_column", func(t *testing.T) {
		dropInvTable(ctx, srcDB)
		if _, err := srcDB.ExecContext(ctx,
			"CREATE TABLE "+invTable+" (id BIGINT PRIMARY KEY, "+
				"doubled BIGINT AS (id * 2) STORED INVISIBLE) ENGINE=InnoDB"); err != nil {
			t.Fatalf("create table: %v", err)
		}
		assertInvisibleRejected(t, ctx, checker, "doubled")
	})

	// Invisible PRIMARY KEY (the GIPK-shaped variant): MySQL marks it
	// EXTRA='INVISIBLE' just like any other invisible column, so it is caught.
	t.Run("invisible_primary_key", func(t *testing.T) {
		dropInvTable(ctx, srcDB)
		if _, err := srcDB.ExecContext(ctx,
			"CREATE TABLE "+invTable+" (id BIGINT INVISIBLE PRIMARY KEY, "+
				"val INT) ENGINE=InnoDB"); err != nil {
			t.Fatalf("create table: %v", err)
		}
		assertInvisibleRejected(t, ctx, checker, "id")
	})

	// Negative control: only visible columns -> passes.
	t.Run("no_invisible_columns_passes", func(t *testing.T) {
		dropInvTable(ctx, srcDB)
		if _, err := srcDB.ExecContext(ctx,
			"CREATE TABLE "+invTable+" (id BIGINT PRIMARY KEY, "+
				"payload VARCHAR(255) DEFAULT 'x') ENGINE=InnoDB"); err != nil {
			t.Fatalf("create table: %v", err)
		}
		if err := checker.ValidateNoInvisibleColumns(ctx, []string{invTable}); err != nil {
			t.Fatalf("expected a table with only visible columns to pass, got: %v", err)
		}
	})
}

// TestIntegrationInvisibleColumn_RunWithProfile proves the check is actually
// wired into the preflight pipeline — not just callable in isolation — so a
// future refactor of RunWithProfile cannot silently unwire the safety property.
// It runs the SourceOnly profile (the purge path, no destination needed); the
// invisible check sits ahead of every profile branch, so passing here proves it
// runs for all commands (archive/purge/copy-only/dry-run/validate) before any
// delete.
func TestIntegrationInvisibleColumn_RunWithProfile(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	checker, srcDB := invMakeChecker(t, setup)
	dropInvTable(ctx, srcDB)
	defer dropInvTable(ctx, srcDB)

	if _, err := srcDB.ExecContext(ctx,
		"CREATE TABLE "+invTable+" (id BIGINT PRIMARY KEY, "+
			"payload VARCHAR(255) INVISIBLE DEFAULT 'default-value') ENGINE=InnoDB"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := srcDB.ExecContext(ctx,
		"INSERT INTO "+invTable+" (id, payload) VALUES (1, 'original-secret')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	err := checker.RunWithProfile(ctx, PreflightProfileSourceOnly, false, false)
	if err == nil {
		t.Fatal("expected RunWithProfile to fail on the participating invisible column, got nil — " +
			"the check is not wired into the preflight pipeline")
	}
	var pfErr *PreflightError
	if !errors.As(err, &pfErr) {
		t.Fatalf("expected *PreflightError, got %T: %v", err, err)
	}
	if pfErr.Check != "INVISIBLE_COLUMN_CHECK" {
		t.Fatalf("expected RunWithProfile to fail with INVISIBLE_COLUMN_CHECK, got %q: %v", pfErr.Check, err)
	}
}
