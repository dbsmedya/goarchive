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
