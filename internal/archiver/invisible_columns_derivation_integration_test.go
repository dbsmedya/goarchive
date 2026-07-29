//go:build integration

package archiver

import (
	"reflect"
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
)

// TestIntegrationInvisibleColumnsDerivationMatchesLibrary pins, against a real MySQL
// server, the equivalence the P1 correction (phase 018 review) relies on: deriving
// invisible-column facts from Inspector.Columns produces EXACTLY what the library's own
// Inspector.InvisibleColumns method returns — same tables, same columns, same order —
// so preflight_facts.go's invisibleColumns can stop issuing a second query for a fact
// Columns already answers.
//
// This is a permanent gate, not a one-off check: the equivalence was justified by
// reading library source (parseColumnExtra's case-insensitive "INVISIBLE" substring
// check on EXTRA vs. the SQL EXTRA LIKE '%INVISIBLE%' predicate, both in request/
// ORDINAL_POSITION order), which holds against v0.6.0 today but nothing else in the
// repository would notice if a future library version changed either side.
//
// Deliberately does NOT call preflightRun.invisibleColumns or
// ValidateNoInvisibleColumns: the point is to pin the LIBRARY-LEVEL equivalence the
// derivation rests on, independent of goarchive's own wiring, so this keeps working —
// and keeps testing the right thing — as goarchive's surrounding code changes.
func TestIntegrationInvisibleColumnsDerivationMatchesLibrary(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	// clean_tbl: no invisible columns at all. Control proving a clean table is
	// ABSENT from both results, not present with an empty Columns slice.
	f.ExecSource(t, ctx,
		"CREATE TABLE clean_tbl (id BIGINT PRIMARY KEY, name VARCHAR(64)) ENGINE=InnoDB")

	// plain_tbl: a single plain INVISIBLE column (EXTRA = "INVISIBLE").
	f.ExecSource(t, ctx,
		"CREATE TABLE plain_tbl (id BIGINT PRIMARY KEY, secret VARCHAR(64) INVISIBLE) ENGINE=InnoDB")

	// generated_tbl: STORED GENERATED INVISIBLE — the second EXTRA form, and the one
	// most likely to diverge if either implementation changes.
	f.ExecSource(t, ctx,
		"CREATE TABLE generated_tbl (id BIGINT PRIMARY KEY, doubled BIGINT AS (id * 2) STORED INVISIBLE) ENGINE=InnoDB")

	// multi_tbl: several invisible columns, deliberately NOT in alphabetical order
	// (zeta before alpha), so an implementation that silently sorted would be caught —
	// pins column ORDER within a table.
	f.ExecSource(t, ctx,
		"CREATE TABLE multi_tbl ("+
			"id BIGINT PRIMARY KEY, "+
			"zeta VARCHAR(8) INVISIBLE, "+
			"alpha VARCHAR(8) INVISIBLE, "+
			"visible_col VARCHAR(8)"+
			") ENGINE=InnoDB")

	// Table request order deliberately does not match creation order, to pin table
	// ordering independent of DDL order.
	tables := []string{"multi_tbl", "clean_tbl", "generated_tbl", "plain_tbl"}

	inspector := validations.NewInspector(f.SourceDB, f.SourceSchema)

	cols, err := inspector.Columns(ctx, tables)
	if err != nil {
		t.Fatalf("Inspector.Columns: %v", err)
	}

	// Local re-derivation, independent of preflight_facts.go's invisibleColumns, so a
	// change to THAT function cannot mask a real library-level divergence.
	var derived []validations.InvisibleColumns
	for _, tc := range cols {
		var invisibleCols []string
		for _, c := range tc.Columns {
			if c.Invisible {
				invisibleCols = append(invisibleCols, c.Name)
			}
		}
		if len(invisibleCols) > 0 {
			derived = append(derived, validations.InvisibleColumns{Table: tc.Table, Columns: invisibleCols})
		}
	}

	want, err := inspector.InvisibleColumns(ctx, tables)
	if err != nil {
		t.Fatalf("Inspector.InvisibleColumns: %v", err)
	}

	if !reflect.DeepEqual(derived, want) {
		t.Fatalf("derivation diverges from the library's own InvisibleColumns:\n derived=%+v\n library=%+v",
			derived, want)
	}

	// Sanity checks so a coincidental pair of matching bugs on both sides cannot mask
	// a real defect: exactly the three dirty tables, clean_tbl genuinely absent, and
	// multi_tbl's columns in ordinal (not alphabetical) order.
	if len(want) != 3 {
		t.Fatalf("expected exactly 3 tables with invisible columns (plain_tbl, generated_tbl, multi_tbl), got %d: %+v",
			len(want), want)
	}
	for _, ic := range want {
		if ic.Table == "clean_tbl" {
			t.Fatalf("clean_tbl must be absent from InvisibleColumns' result, got: %+v", ic)
		}
		if ic.Table == "multi_tbl" {
			if len(ic.Columns) != 2 || ic.Columns[0] != "zeta" || ic.Columns[1] != "alpha" {
				t.Fatalf("multi_tbl columns = %v, want [zeta alpha] in ordinal order", ic.Columns)
			}
		}
	}
}
