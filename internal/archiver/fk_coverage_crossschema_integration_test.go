//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	_ "github.com/go-sql-driver/mysql"
)

// ============================================================================
// Cross-schema incoming foreign-key coverage integration tests (issue #15).
//
// FK_COVERAGE_CHECK is supposed to reject every foreign key that references an
// in-graph source table but is itself outside the configured graph, because a
// source DELETE can then cascade (or SET NULL) into rows GoArchive never copied
// or verified.
//
// The regression: getForeignKeys anchors its metadata query on the *child*
// table's schema (`kcu.TABLE_SCHEMA = sourceDBName`), so a constraint DEFINED IN
// ANOTHER SCHEMA that references an in-graph table is never seen, and coverage
// silently passes. MySQL permits a table in one database to reference a table in
// another, and ON DELETE CASCADE will then delete matching child rows.
//
// sqlmock cannot reproduce this — it needs real information_schema metadata for a
// genuine cross-schema constraint, so these live against MySQL.
// ============================================================================

const (
	xschemaParentTable = "xschema_orders"       // lives in the source schema (in-graph)
	xschemaChildTable  = "xschema_order_events" // lives in a DIFFERENT schema (out-of-graph)
	xschemaChildSchema = "goarchive_xschema_audit"
)

// xschemaSourceName resolves the configured source schema (database) name.
func xschemaSourceName(setup *IntegrationTestSetup) string {
	for _, dbCfg := range setup.Config.Databases {
		if dbCfg.Name == "source" {
			return dbCfg.Database
		}
	}
	return ""
}

// dropCrossSchemaFK removes all objects the test creates, child-first so the FK
// never blocks the parent drop.
func dropCrossSchemaFK(ctx context.Context, db *sql.DB, srcSchema string) {
	_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", xschemaChildSchema, xschemaChildTable))
	_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", srcSchema, xschemaParentTable))
	_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", xschemaChildSchema))
}

// setupCrossSchemaFK creates an in-graph parent table in the source schema and a
// child table in a separate schema whose FK references the parent with the given
// ON DELETE rule.
func setupCrossSchemaFK(t *testing.T, ctx context.Context, db *sql.DB, srcSchema, deleteRule string) {
	t.Helper()
	dropCrossSchemaFK(ctx, db, srcSchema)

	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE `%s`.`%s` (id BIGINT NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB",
		srcSchema, xschemaParentTable)); err != nil {
		t.Fatalf("failed to create in-graph parent %s.%s: %v", srcSchema, xschemaParentTable, err)
	}

	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		"CREATE DATABASE IF NOT EXISTS `%s`", xschemaChildSchema)); err != nil {
		t.Fatalf("failed to create out-of-graph schema %s: %v", xschemaChildSchema, err)
	}

	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE `%s`.`%s` (id BIGINT NOT NULL, order_id BIGINT NULL, PRIMARY KEY (id), "+
			"CONSTRAINT fk_xschema_event_order FOREIGN KEY (order_id) "+
			"REFERENCES `%s`.`%s`(id) ON DELETE %s) ENGINE=InnoDB",
		xschemaChildSchema, xschemaChildTable, srcSchema, xschemaParentTable, deleteRule)); err != nil {
		t.Fatalf("failed to create cross-schema child %s.%s (ON DELETE %s): %v",
			xschemaChildSchema, xschemaChildTable, deleteRule, err)
	}
}

// TestIntegrationFKCoverage_CrossSchemaIncoming_Rejected proves that an incoming
// foreign key from a table OUTSIDE the source schema is a fatal FK_COVERAGE_CHECK
// error for every ON DELETE rule. The cross-schema child cannot be represented in
// the graph (config identifiers forbid `schema.table`), so any such constraint
// must stop the run before any copy/delete happens.
func TestIntegrationFKCoverage_CrossSchemaIncoming_Rejected(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	sourceDB, ok := setup.GetDB("source")
	if !ok {
		t.Fatal("source database not found in integration setup")
	}
	srcSchema := xschemaSourceName(setup)
	if srcSchema == "" {
		t.Fatal("could not resolve source schema name from integration config")
	}
	if srcSchema == xschemaChildSchema {
		t.Fatalf("source schema %q must differ from the out-of-graph schema", srcSchema)
	}

	// ON DELETE CASCADE / SET NULL silently mutate uncopied rows; RESTRICT and
	// NO ACTION fail safe at delete time but must still be flagged — coverage is
	// rule-independent, so every ON DELETE rule is exercised.
	for _, deleteRule := range []string{"CASCADE", "SET NULL", "RESTRICT", "NO ACTION"} {
		deleteRule := deleteRule
		t.Run(strings.ReplaceAll(deleteRule, " ", "_"), func(t *testing.T) {
			setupCrossSchemaFK(t, ctx, sourceDB, srcSchema, deleteRule)
			defer dropCrossSchemaFK(ctx, sourceDB, srcSchema)

			g := graph.NewGraph(xschemaParentTable, "id")
			checker, err := NewPreflightChecker(sourceDB, srcSchema, g, logger.NewDefault())
			if err != nil {
				t.Fatalf("failed to create preflight checker: %v", err)
			}

			err = checker.ValidateForeignKeyCoverage(ctx)
			if err == nil {
				t.Fatalf("ON DELETE %s: expected FK_COVERAGE_CHECK to reject the cross-schema "+
					"incoming FK, got nil — an external cascade would delete uncopied rows", deleteRule)
			}
			pfErr, ok := err.(*PreflightError)
			if !ok {
				t.Fatalf("ON DELETE %s: expected *PreflightError, got %T: %v", deleteRule, err, err)
			}
			if pfErr.Check != "FK_COVERAGE_CHECK" {
				t.Fatalf("ON DELETE %s: expected FK_COVERAGE_CHECK, got %q (%v)", deleteRule, pfErr.Check, err)
			}
			if !strings.Contains(err.Error(), xschemaChildTable) {
				t.Fatalf("ON DELETE %s: expected error to name the offending child table %q, got: %v",
					deleteRule, xschemaChildTable, err)
			}
		})
	}
}
