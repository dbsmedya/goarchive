//go:build integration

package preflight

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
)

// These tests run the foreign-key coverage, destination trigger and cascade checks against
// MySQL. Each pairs a schema with one fact (a constraint, a trigger, an ON DELETE rule)
// against the same schema without it.

// schCS is the charset and collation of every character column of the order fixture, the
// server default, so source and destination never differ by it.
const schCS = "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"

// schTypes are the three job types, in the order the tests run them.
var schTypes = []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly}

// schRun runs preflight with count verification and an observed logger, after edit (if
// any) changes the config, and returns the warnings it logged in order.
func schRun(t *testing.T, src, dst *sql.DB, spec *jobspec.Spec, edit func(*Config)) (*Result, []string, error) {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	cfg := Config{
		Source: src, SourceDatabase: schemaOf(t, src),
		Destination: dst, DestinationDatabase: schemaOf(t, dst), JobSchema: schemaOf(t, dst),
		Spec: spec, Verification: jobspec.VerifyCount,
		Logger: &logger.Logger{SugaredLogger: zap.New(core).Sugar()},
	}
	if edit != nil {
		edit(&cfg)
	}
	res, err := Run(context.Background(), cfg)
	var warnings []string
	for _, e := range logs.All() {
		if e.Level == zapcore.WarnLevel {
			warnings = append(warnings, e.Message)
		}
	}
	return res, warnings, err
}

// schPasses runs the job of each type and fails the test on any error.
func schPasses(t *testing.T, src, dst *sql.DB, body string, types ...jobspec.Type) {
	t.Helper()
	for _, typ := range types {
		if _, _, err := schRun(t, src, dst, job(t, typ, body), nil); err != nil {
			t.Errorf("%s: Run() = %v, want success", typ, err)
		}
	}
}

// TestMySQLCrossSchemaForeignKeyCoverage (M7-3, G7): a table in another schema of the
// source server that references a job table is refused for every job type and every ON
// DELETE rule, because a cross-schema child can never join the job.
//
// Pair: audit in a second schema with a foreign key into orders (CASCADE, SET NULL,
// RESTRICT, NO ACTION) or into order_lines → FK_COVERAGE_CHECK for archive, purge and
// copy-only, naming <schema>.audit and its rule under the referenced table; the same audit
// table without the constraint → all three pass. A job table that references a table in
// the other schema (the reverse direction) passes too.
func TestMySQLCrossSchemaForeignKeyCoverage(t *testing.T) {
	src := scratch(t, "TEST_SOURCE", "schxs")
	execAll(t, src, orderTables(schCS)...)
	// Created after src, so dropped before it: a schema whose tables reference another
	// cannot be dropped first (Error 3730).
	ext := scratch(t, "TEST_SOURCE", "schxsext")
	dst := scratch(t, "TEST_DEST", "schxsdst")
	execAll(t, dst, orderTables(schCS)...)
	srcName, extName := schemaOf(t, src), schemaOf(t, ext)

	toOrders := func(rule string) string {
		return fmt.Sprintf("CREATE TABLE audit (id INT NOT NULL PRIMARY KEY, order_id CHAR(36) %s, "+
			"CONSTRAINT fk_audit FOREIGN KEY (order_id) REFERENCES `%s`.orders (id) ON DELETE %s) ENGINE=InnoDB",
			schCS, srcName, rule)
	}
	for _, tc := range []struct{ name, ddl, want string }{
		{"cascade", toOrders("CASCADE"), "  - orders is referenced by: [" + extName + ".audit (ON DELETE CASCADE)]"},
		{"set null", toOrders("SET NULL"), "  - orders is referenced by: [" + extName + ".audit (ON DELETE SET NULL)]"},
		{"restrict", toOrders("RESTRICT"), "  - orders is referenced by: [" + extName + ".audit (ON DELETE RESTRICT)]"},
		{"no action", toOrders("NO ACTION"), "  - orders is referenced by: [" + extName + ".audit (ON DELETE NO ACTION)]"},
		{"into a child", fmt.Sprintf("CREATE TABLE audit (id INT NOT NULL PRIMARY KEY, order_id CHAR(36) %s, line_no SMALLINT, "+
			"CONSTRAINT fk_audit_line FOREIGN KEY (order_id, line_no) REFERENCES `%s`.order_lines (order_id, line_no) "+
			"ON DELETE RESTRICT) ENGINE=InnoDB", schCS, srcName),
			"  - order_lines is referenced by: [" + extName + ".audit (ON DELETE RESTRICT)]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			execAll(t, ext, tc.ddl)
			t.Cleanup(func() { execAll(t, ext, "DROP TABLE audit") })
			for _, typ := range schTypes {
				_, _, err := schRun(t, src, dst, job(t, typ, orderJob), nil)
				e := refusedWith(t, err, "FK_COVERAGE_CHECK")
				if want := "Foreign key constraints not covered by relations (fatal for any ON DELETE rule):\n" + tc.want; e.Message != want || len(e.Tables) != 0 {
					t.Errorf("%s: error = %q, tables %v; want %q", typ, e.Message, e.Tables, want)
				}
			}
		})
	}

	t.Run("without the constraint", func(t *testing.T) {
		execAll(t, ext, fmt.Sprintf("CREATE TABLE audit (id INT NOT NULL PRIMARY KEY, order_id CHAR(36) %s, KEY ix_order (order_id)) ENGINE=InnoDB", schCS))
		t.Cleanup(func() { execAll(t, ext, "DROP TABLE audit") })
		schPasses(t, src, dst, orderJob, schTypes...)
	})

	t.Run("a job table references the other schema", func(t *testing.T) {
		execAll(t, ext, "CREATE TABLE regions (id INT NOT NULL PRIMARY KEY) ENGINE=InnoDB")
		execAll(t, src, fmt.Sprintf("ALTER TABLE orders ADD COLUMN region_id INT, "+
			"ADD CONSTRAINT fk_orders_region FOREIGN KEY (region_id) REFERENCES `%s`.regions (id)", extName))
		execAll(t, dst, "ALTER TABLE orders ADD COLUMN region_id INT")
		t.Cleanup(func() {
			execAll(t, src, "ALTER TABLE orders DROP FOREIGN KEY fk_orders_region, DROP COLUMN region_id")
			execAll(t, dst, "ALTER TABLE orders DROP COLUMN region_id")
			execAll(t, ext, "DROP TABLE regions")
		})
		schPasses(t, src, dst, orderJob, schTypes...)
	})
}
