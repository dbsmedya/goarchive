//go:build integration

package preflight

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// These tests judge destination and source structure from the library's real facts. The
// schema-compatibility policy parses the library's SpecDiff A and B strings (types,
// "true"/"false"), so a real-facts witness guards that format against a library release.

// schSwap returns ddl with old replaced by new, once, and fails the test when old is not
// in ddl, so a variant can never silently equal the original.
func schSwap(t *testing.T, ddl, old, new string) string {
	t.Helper()
	if !strings.Contains(ddl, old) {
		t.Fatalf("%q is not in %q", old, ddl)
	}
	return strings.Replace(ddl, old, new, 1)
}

// schRecreate drops the named tables of db, in order, and runs ddl.
func schRecreate(t *testing.T, db *sql.DB, drop []string, ddl ...string) {
	t.Helper()
	for _, table := range drop {
		execAll(t, db, "DROP TABLE IF EXISTS "+table)
	}
	execAll(t, db, ddl...)
}

// schCompatT and schCompatU are the source tables of the schema-compatibility job; the
// destination copies drop the foreign key, which the copy never needs.
const (
	schCompatT = "CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT, a INT NOT NULL DEFAULT 0, " +
		"b VARCHAR(20) " + schCS + " NULL, " +
		"ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, " +
		"z INT(5) UNSIGNED ZEROFILL NULL, f TINYINT(1) NULL, PRIMARY KEY (id), KEY ix_a (a)) ENGINE=InnoDB"
	schCompatU = "CREATE TABLE u (id INT NOT NULL, t_id INT NOT NULL, n INT NULL, m INT NULL, " +
		"PRIMARY KEY (id), KEY ix_t (t_id)) ENGINE=InnoDB"
	schCompatJob = `
    root_table: t
    primary_key: id
    where: "1=1"
    relations:
      - {table: u, primary_key: id, foreign_key: t_id}
`
)

// TestMySQLSchemaCompatibilityFromRealFacts (M7-3, G12): DEST_SCHEMA_COMPATIBILITY_CHECK
// lets the destination be looser than the source, never stricter, judged from the
// library's real SpecDiff strings.
//
// Pair: each fatal variant of the destination (a type change, tinyint(1) to smallint, a
// destination-only NOT NULL, a generated column, a missing column, a column order change,
// two bad tables, a table with two bad columns) → DEST_SCHEMA_COMPATIBILITY_CHECK naming
// exactly its entries; the identical destination, that one difference undone → passes with
// no advisory. Each looser variant (secondary index, AUTO_INCREMENT, DEFAULT, ON UPDATE
// dropped, NULL relaxed, tinyint(1) to tinyint, a ZEROFILL width) passes with no advisory,
// and a case-only column or key name passes with exactly one advisory.
func TestMySQLSchemaCompatibilityFromRealFacts(t *testing.T) {
	src := scratch(t, "TEST_SOURCE", "schcompat")
	execAll(t, src, schCompatT,
		schSwap(t, schCompatU, "KEY ix_t (t_id))", "KEY ix_t (t_id), CONSTRAINT fk_u_t FOREIGN KEY (t_id) REFERENCES t (id))"))
	dst := scratch(t, "TEST_DEST", "schcompatdst")

	tSwap := func(old, new string) string { return schSwap(t, schCompatT, old, new) }
	uSwap := func(old, new string) string { return schSwap(t, schCompatU, old, new) }
	typeA := tSwap("a INT NOT NULL", "a BIGINT NOT NULL")
	const caseOnly = "name differs only in letter case (source=%s destination=%s); MySQL resolves column " +
		"names case-insensitively, so the copy and verification are unaffected"
	for _, tc := range []struct {
		name, t, u string
		fatal      []string // DEST_SCHEMA_COMPATIBILITY_CHECK's tables; nil passes
		warnings   []string
	}{
		{name: "identical"},

		{name: "type change", t: typeA,
			fatal: []string{"t(column a: column type mismatch; source=int, destination=bigint)"}},
		{name: "tinyint(1) to smallint", t: tSwap("f TINYINT(1)", "f SMALLINT"),
			fatal: []string{"t(column f: column type mismatch; source=tinyint(1), destination=smallint)"}},
		{name: "destination-only NOT NULL", t: tSwap("b VARCHAR(20) "+schCS+" NULL", "b VARCHAR(20) "+schCS+" NOT NULL"),
			fatal: []string{"t(column b: destination is NOT NULL but source allows NULL; source=true, destination=false)"}},
		{name: "generated column", t: tSwap("b VARCHAR(20) "+schCS+" NULL", "b VARCHAR(20) "+schCS+" GENERATED ALWAYS AS (CONCAT('x', a)) VIRTUAL"),
			fatal: []string{"t(column b: destination column is generated (virtual); the copy inserts a value for every " +
				"column and MySQL rejects it with error 3105)"}},
		{name: "missing column", t: tSwap(", f TINYINT(1) NULL", ""),
			fatal: []string{"t(column f: column missing on one side; source=tinyint(1), destination=)"}},
		{name: "column order", u: uSwap("n INT NULL, m INT NULL", "m INT NULL, n INT NULL"),
			fatal: []string{"u(column n: column order mismatch; source=3, destination=4)"}},
		{name: "two bad tables", t: typeA, u: uSwap("n INT NULL", "n BIGINT NULL"),
			fatal: []string{"t(column a: column type mismatch; source=int, destination=bigint)",
				"u(column n: column type mismatch; source=int, destination=bigint)"}},
		{name: "two bad columns", u: uSwap("n INT NULL, m INT NULL", "n BIGINT NULL, m BIGINT NULL"),
			fatal: []string{"u(column n: column type mismatch; source=int, destination=bigint)"}},

		{name: "secondary index dropped", t: tSwap(", KEY ix_a (a)", "")},
		{name: "auto_increment dropped", t: tSwap("id INT NOT NULL AUTO_INCREMENT", "id INT NOT NULL")},
		{name: "default dropped", t: tSwap("a INT NOT NULL DEFAULT 0", "a INT NOT NULL")},
		{name: "on update dropped", t: tSwap(" ON UPDATE CURRENT_TIMESTAMP", "")},
		{name: "null relaxed", t: tSwap("a INT NOT NULL", "a INT NULL")},
		{name: "tinyint(1) to tinyint", t: tSwap("f TINYINT(1)", "f TINYINT")},
		{name: "zerofill width", t: tSwap("z INT(5) UNSIGNED ZEROFILL", "z INT(10) UNSIGNED ZEROFILL")},
		{name: "case-only column", t: tSwap("b VARCHAR(20)", "B VARCHAR(20)"),
			warnings: []string{"Table t column b: " + fmt.Sprintf(caseOnly, "b", "B")}},
		{name: "case-only key", u: schSwap(t, uSwap("id INT NOT NULL", "ID INT NOT NULL"), "PRIMARY KEY (id)", "PRIMARY KEY (ID)"),
			warnings: []string{"Table u column id: " + fmt.Sprintf(caseOnly, "id", "ID")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tt, uu := tc.t, tc.u
			if tt == "" {
				tt = schCompatT
			}
			if uu == "" {
				uu = schCompatU
			}
			schRecreate(t, dst, []string{"u", "t"}, tt, uu)
			_, warnings, err := schRun(t, src, dst, job(t, jobspec.Archive, schCompatJob), nil)
			if tc.fatal == nil {
				if err != nil {
					t.Fatalf("Run() = %v, want success", err)
				}
			} else if e := refusedWith(t, err, "DEST_SCHEMA_COMPATIBILITY_CHECK"); !slices.Equal(e.Tables, tc.fatal) ||
				e.Message != "Source and destination schemas are incompatible" {
				t.Errorf("error = %q, tables %q; want tables %q", e.Message, e.Tables, tc.fatal)
			}
			if !slices.Equal(warnings, tc.warnings) {
				t.Errorf("warnings = %q, want %q", warnings, tc.warnings)
			}
		})
	}
}

// schNoEquivalent is the D3 refusal of a destination unique index on table t.
func schNoEquivalent(index string) string {
	return fmt.Sprintf("t(destination unique index %q has no equivalent on the source; rows the source holds "+
		"as distinct would collide and fail the copy. Drop the destination unique index, or add an equivalent "+
		"one to the source)", index)
}

// TestMySQLDestinationUniqueIndexRule (M7-3, G13): deviation D3. A destination unique
// index must enforce a uniqueness predicate the source enforces too, compared by parts
// (column or expression, prefix, collation), never by name or part order.
//
// Pair: each destination-only unique predicate (a single column, a subset of a source
// composite, a new composite, a prefix of a source column, another expression, the source
// index under a case-insensitive collation) → DEST_SCHEMA_COMPATIBILITY_CHECK naming that
// index; the identical destination, the index undone → passes. A source functional index
// matched by the destination's is refused once another column differs in collation, and
// passes when it does not. Renamed or reordered equivalents and source-only unique indexes
// pass.
func TestMySQLDestinationUniqueIndexRule(t *testing.T) {
	const base = "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, " +
		"email VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, a INT, b INT, c INT, note VARCHAR(20) " + schCS +
		", UNIQUE KEY uk_email (email), UNIQUE KEY uk_ab (a, b), UNIQUE KEY uk_lc ((lower(email)))) ENGINE=InnoDB"
	src := scratch(t, "TEST_SOURCE", "schuniq")
	execAll(t, src, base)
	dst := scratch(t, "TEST_DEST", "schuniqdst")
	const body = "\n    root_table: t\n    primary_key: id\n    where: \"1=1\"\n"

	swap := func(old, new string) string { return schSwap(t, base, old, new) }
	add := func(index string) string { return swap(") ENGINE=InnoDB", ", "+index+") ENGINE=InnoDB") }
	ciEmail := swap("email VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin", "email VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
	for _, tc := range []struct{ name, ddl, fatal string }{
		{"identical", base, ""},
		{"renamed and reordered", swap("UNIQUE KEY uk_email (email), UNIQUE KEY uk_ab (a, b), UNIQUE KEY uk_lc ((lower(email)))",
			"UNIQUE KEY ux_mail (email), UNIQUE KEY ux_ba (b, a), UNIQUE KEY ux_lc ((lower(email)))"), ""},
		{"source-only unique indexes", swap(", UNIQUE KEY uk_email (email), UNIQUE KEY uk_ab (a, b), UNIQUE KEY uk_lc ((lower(email)))", ""), ""},

		{"single", add("UNIQUE KEY uk_c (c)"), schNoEquivalent("uk_c")},
		{"overlapping", add("UNIQUE KEY uk_a (a)"), schNoEquivalent("uk_a")},
		{"composite", add("UNIQUE KEY uk_bc (b, c)"), schNoEquivalent("uk_bc")},
		{"prefix", swap("UNIQUE KEY uk_email (email)", "UNIQUE KEY uk_email (email(10))"), schNoEquivalent("uk_email")},
		{"functional", add("UNIQUE KEY uk_uc ((upper(email)))"), schNoEquivalent("uk_uc")},
		// uk_lc is left out: its expression would be judged by the functional rule instead.
		{"collation", schSwap(t, ciEmail, ", UNIQUE KEY uk_lc ((lower(email)))", ""), schNoEquivalent("uk_email")},
		{"functional with a collation difference", swap("note VARCHAR(20) "+schCS, "note VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"),
			"t(destination functional unique index \"uk_lc\" matches a source expression textually, but a column " +
				"differs in charset or collation, so GoArchive cannot prove the expression's equality semantics " +
				"match. Align the column charsets and collations, or drop the destination index)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schRecreate(t, dst, []string{"t"}, tc.ddl)
			_, _, err := schRun(t, src, dst, job(t, jobspec.Archive, body), nil)
			if tc.fatal == "" {
				if err != nil {
					t.Fatalf("Run() = %v, want success", err)
				}
				return
			}
			if e := refusedWith(t, err, "DEST_SCHEMA_COMPATIBILITY_CHECK"); !slices.Equal(e.Tables, []string{tc.fatal}) {
				t.Errorf("tables = %q, want [%q]", e.Tables, tc.fatal)
			}
		})
	}
}

// schTwoTables is the orders fixture without line_notes, and schTwoJob its job.
var schTwoTables = orderTables(schCS)[:2]

const schTwoJob = `
    root_table: orders
    primary_key: id
    where: "1=1"
    relations:
      - table: order_lines
        primary_key: [order_id, line_no]
        foreign_key: order_id
`

// schViewTables is schTwoTables with order_lines a view over a base table.
func schViewTables(t *testing.T) []string {
	return []string{schTwoTables[0],
		schSwap(t, schSwap(t, schTwoTables[1], "CREATE TABLE order_lines", "CREATE TABLE order_lines_data"),
			"CONSTRAINT fk_lines_order", "CONSTRAINT fk_lines_data_order"),
		"CREATE VIEW order_lines AS SELECT * FROM order_lines_data"}
}

// TestMySQLViewsAreRefused (M7-3, G14): only base tables take part, on either side.
//
// Pair: order_lines a view in the source → TABLE_EXISTENCE_CHECK naming exactly
// order_lines(VIEW) for archive, purge and copy-only; a base table → all three pass.
// Pair: order_lines a view in the destination → DEST_TABLE_EXISTENCE_CHECK naming exactly
// order_lines(VIEW) for archive and copy-only, and purge passes; a base table → archive and
// copy-only pass.
func TestMySQLViewsAreRefused(t *testing.T) {
	srcView := scratch(t, "TEST_SOURCE", "schview")
	execAll(t, srcView, schViewTables(t)...)
	src := scratch(t, "TEST_SOURCE", "schbase")
	execAll(t, src, schTwoTables...)
	dstView := scratch(t, "TEST_DEST", "schviewdst")
	execAll(t, dstView, schViewTables(t)...)
	dst := scratch(t, "TEST_DEST", "schbasedst")
	execAll(t, dst, schTwoTables...)

	for _, typ := range schTypes {
		_, _, err := schRun(t, srcView, dst, job(t, typ, schTwoJob), nil)
		if e := refusedWith(t, err, "TABLE_EXISTENCE_CHECK"); !slices.Equal(e.Tables, []string{"order_lines(VIEW)"}) ||
			!strings.HasPrefix(e.Message, "Only base tables can be archived.") {
			t.Errorf("%s: error = %q, tables %q", typ, e.Message, e.Tables)
		}
	}
	schPasses(t, src, dst, schTwoJob, schTypes...)

	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.CopyOnly} {
		_, _, err := schRun(t, src, dstView, job(t, typ, schTwoJob), nil)
		if e := refusedWith(t, err, "DEST_TABLE_EXISTENCE_CHECK"); !slices.Equal(e.Tables, []string{"order_lines(VIEW)"}) ||
			!strings.HasPrefix(e.Message, "Only base tables can receive archived rows.") {
			t.Errorf("%s: error = %q, tables %q", typ, e.Message, e.Tables)
		}
	}
	schPasses(t, src, dstView, schTwoJob, jobspec.Purge)
}
