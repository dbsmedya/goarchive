//go:build integration

package preflight

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
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

// TestMySQLInternalForeignKeyCoverage (M7-3, G9): a foreign key declared between two of
// the job's tables must be one of its relations, with the same foreign_key columns.
//
// Pair: line_notes declares fk_notes_line to order_lines while the job lists line_notes
// under orders → INTERNAL_FK_COVERAGE "[no relation: …]"; the constraint dropped → passes.
// Pair: child declares fk_child_parent on pid while its relation's foreign_key is alt →
// INTERNAL_FK_COVERAGE "[the relation's foreign_key is (alt)]"; the constraint dropped →
// passes. Each for archive, purge and copy-only.
func TestMySQLInternalForeignKeyCoverage(t *testing.T) {
	const hint = "\n\nHint: nest each child under its parent, with foreign_key and references matching the constraint."
	const prefix = "Foreign keys between the job's tables must each be a configured relation with the same foreign_key columns:\n"

	t.Run("no relation", func(t *testing.T) {
		src := scratch(t, "TEST_SOURCE", "schifk")
		execAll(t, src, orderTables(schCS)...)
		dst := scratch(t, "TEST_DEST", "schifkdst")
		execAll(t, dst, orderTables(schCS)...)
		const siblings = `
    root_table: orders
    primary_key: id
    where: "1=1"
    relations:
      - table: order_lines
        primary_key: [order_id, line_no]
        foreign_key: order_id
      - table: line_notes
        primary_key: note_id
        foreign_key: order_id
`
		want := prefix + "  - line_notes(order_id, line_no) -> order_lines(order_id, line_no) (constraint fk_notes_line) " +
			"[no relation: line_notes is not configured under order_lines]" + hint
		for _, typ := range schTypes {
			_, _, err := schRun(t, src, dst, job(t, typ, siblings), nil)
			if e := refusedWith(t, err, "INTERNAL_FK_COVERAGE"); e.Message != want {
				t.Errorf("%s: message = %q, want %q", typ, e.Message, want)
			}
		}
		execAll(t, src, "ALTER TABLE line_notes DROP FOREIGN KEY fk_notes_line")
		schPasses(t, src, dst, siblings, schTypes...)
	})

	t.Run("other foreign_key column", func(t *testing.T) {
		tables := []string{
			"CREATE TABLE parent (id INT NOT NULL PRIMARY KEY) ENGINE=InnoDB",
			"CREATE TABLE child (id INT NOT NULL PRIMARY KEY, pid INT, alt INT, KEY ix_pid (pid), KEY ix_alt (alt), " +
				"CONSTRAINT fk_child_parent FOREIGN KEY (pid) REFERENCES parent (id)) ENGINE=InnoDB",
		}
		src := scratch(t, "TEST_SOURCE", "schifkcol")
		execAll(t, src, tables...)
		dst := scratch(t, "TEST_DEST", "schifkcoldst")
		execAll(t, dst, tables...)
		const body = `
    root_table: parent
    primary_key: id
    where: "1=1"
    relations:
      - {table: child, primary_key: id, foreign_key: alt}
`
		want := prefix + "  - child(pid) -> parent(id) (constraint fk_child_parent) [the relation's foreign_key is (alt)]" + hint
		for _, typ := range schTypes {
			_, _, err := schRun(t, src, dst, job(t, typ, body), nil)
			if e := refusedWith(t, err, "INTERNAL_FK_COVERAGE"); e.Message != want {
				t.Errorf("%s: message = %q, want %q", typ, e.Message, want)
			}
		}
		execAll(t, src, "ALTER TABLE child DROP FOREIGN KEY fk_child_parent")
		schPasses(t, src, dst, body, schTypes...)
	})
}

// TestMySQLDestinationInsertTriggers (M7-3, G10): a destination INSERT trigger refuses
// every job type that copies, whatever its timing, and --force-triggers does not lift it.
//
// Pair: a BEFORE INSERT trigger on orders and an AFTER INSERT trigger on line_notes in the
// destination → DEST_INSERT_TRIGGER_CHECK naming orders(trg_orders_bi) and
// line_notes(trg_notes_ai) for archive and copy-only, with and without ForceTriggers, and
// purge passes; the triggers dropped → archive and copy-only pass.
func TestMySQLDestinationInsertTriggers(t *testing.T) {
	src := scratch(t, "TEST_SOURCE", "schtrg")
	execAll(t, src, orderTables(schCS)...)
	dst := scratch(t, "TEST_DEST", "schtrgdst")
	execAll(t, dst, orderTables(schCS)...)
	execAll(t, dst,
		"CREATE TRIGGER trg_orders_bi BEFORE INSERT ON orders FOR EACH ROW SET NEW.code = UPPER(NEW.code)",
		"CREATE TRIGGER trg_notes_ai AFTER INSERT ON line_notes FOR EACH ROW SET @ga3m3_notes = NEW.note_id")

	want := []string{"line_notes(trg_notes_ai)", "orders(trg_orders_bi)"}
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.CopyOnly} {
		for _, force := range []bool{false, true} {
			_, _, err := schRun(t, src, dst, job(t, typ, orderJob), func(c *Config) { c.ForceTriggers = force })
			e := refusedWith(t, err, "DEST_INSERT_TRIGGER_CHECK")
			if got := slices.Sorted(slices.Values(e.Tables)); !slices.Equal(got, want) {
				t.Errorf("%s, ForceTriggers %v: tables = %v, want %v", typ, force, e.Tables, want)
			}
		}
	}
	schPasses(t, src, dst, orderJob, jobspec.Purge)

	execAll(t, dst, "DROP TRIGGER trg_orders_bi", "DROP TRIGGER trg_notes_ai")
	schPasses(t, src, dst, orderJob, jobspec.Archive, jobspec.CopyOnly)
}

// TestMySQLCascadeAdvisory (M7-3, G11): an ON DELETE CASCADE between two job tables is
// legitimate, so archive and purge pass with one advisory naming the edge once, although
// the constraint is both incoming and outgoing; copy-only deletes nothing and does not
// look.
//
// Pair: fk_lines_order ON DELETE CASCADE → archive and purge pass and log exactly the two
// cascade warnings, the first naming <src>.order_lines(order_id)-><src>.orders(id) once,
// and copy-only logs none; the same constraint without ON DELETE CASCADE → archive and
// purge log no warning.
func TestMySQLCascadeAdvisory(t *testing.T) {
	cascade := orderTables(schCS)
	cascade[1] = strings.Replace(cascade[1], "REFERENCES orders (id)", "REFERENCES orders (id) ON DELETE CASCADE", 1)
	src := scratch(t, "TEST_SOURCE", "schcasc")
	execAll(t, src, cascade...)
	plain := scratch(t, "TEST_SOURCE", "schnocasc")
	execAll(t, plain, orderTables(schCS)...)
	dst := scratch(t, "TEST_DEST", "schcascdst")
	execAll(t, dst, orderTables(schCS)...)

	s := schemaOf(t, src)
	want := []string{
		fmt.Sprintf("ON DELETE CASCADE rules detected (1): [%s.order_lines(order_id)->%s.orders(id)]", s, s),
		"CASCADE rules may cause automatic deletion of related records. Verify this is intended behavior.",
	}
	for _, tc := range []struct {
		typ  jobspec.Type
		src  *sql.DB
		want []string
	}{
		{jobspec.Archive, src, want},
		{jobspec.Purge, src, want},
		{jobspec.CopyOnly, src, nil},
		{jobspec.Archive, plain, nil},
		{jobspec.Purge, plain, nil},
	} {
		_, warnings, err := schRun(t, tc.src, dst, job(t, tc.typ, orderJob), nil)
		if err != nil {
			t.Fatalf("%s on %s: Run() = %v, want success", tc.typ, schemaOf(t, tc.src), err)
		}
		if !slices.Equal(warnings, tc.want) {
			t.Errorf("%s on %s: warnings = %q, want %q", tc.typ, schemaOf(t, tc.src), warnings, tc.want)
		}
	}
}
