package archiver

import (
	"strings"
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
)

// specCol is a terse ColumnSpec builder for the tests below.
func specCol(ordinal int, name, colType string) validations.ColumnSpec {
	return validations.ColumnSpec{
		Name: name, Ordinal: ordinal, Type: colType, NormalizedType: colType,
		Generated: validations.GeneratedNone,
	}
}

// pairOf builds a specPair with indexes captured on both sides and a PRIMARY index on
// the first column, which is the shape every fixture below needs to get past the
// invariants phase 029 adds.
func pairOf(a, b []validations.ColumnSpec) specPair {
	primary := validations.IndexSpec{
		Name: "PRIMARY", Unique: true, Type: "BTREE", Visible: true,
		Parts: []validations.IndexPart{{Column: "id"}},
	}
	return specPair{
		Table: "orders",
		A: validations.TableSpec{Schema: "srcdb", Table: "orders", Columns: a,
			Indexes: []validations.IndexSpec{primary}, Captured: validations.SectionIndexes},
		B: validations.TableSpec{Schema: "dstdb", Table: "orders", Columns: b,
			Indexes: []validations.IndexSpec{primary}, Captured: validations.SectionIndexes},
	}
}

// TestSchemaCompatIdenticalPasses is the negative control.
func TestSchemaCompatIdenticalPasses(t *testing.T) {
	cols := []validations.ColumnSpec{specCol(1, "id", "bigint"), specCol(2, "note", "varchar(64)")}
	v, err := evaluateSchemaCompatibility(pairOf(cols, cols), true)
	if err != nil {
		t.Fatalf("evaluateSchemaCompatibility: %v", err)
	}
	if v.Fatal != "" || len(v.Warnings) != 0 {
		t.Fatalf("verdict = %+v, want clean", v)
	}
}

// TestSchemaCompatColumnAbsentIsFatal covers the ColumnAbsent row.
func TestSchemaCompatColumnAbsentIsFatal(t *testing.T) {
	a := []validations.ColumnSpec{specCol(1, "id", "bigint"), specCol(2, "note", "varchar(64)")}
	b := []validations.ColumnSpec{specCol(1, "id", "bigint")}
	v, err := evaluateSchemaCompatibility(pairOf(a, b), true)
	if err != nil {
		t.Fatalf("evaluateSchemaCompatibility: %v", err)
	}
	if v.Fatal == "" {
		t.Fatal("a destination column absent from the copy target must be fatal")
	}
}

// TestSchemaCompatTypeMismatchIsFatal covers the ColumnTypeMismatch row.
func TestSchemaCompatTypeMismatchIsFatal(t *testing.T) {
	a := []validations.ColumnSpec{specCol(1, "id", "bigint"), specCol(2, "amount", "decimal(10,2)")}
	b := []validations.ColumnSpec{specCol(1, "id", "bigint"), specCol(2, "amount", "decimal(12,2)")}
	v, _ := evaluateSchemaCompatibility(pairOf(a, b), true)
	if v.Fatal == "" {
		t.Fatal("differing column types must be fatal")
	}
}

// TestSchemaCompatOrdinaryDisplayWidthEmitsNoDiff proves the library absorbs ordinary
// integer display widths BEFORE GoArchive sees anything: NormalizedType is set the way a
// real Inspector would set it (width already stripped), so DiffSpecs emits no
// ColumnTypeMismatch at all and goarchiveTypesCompatible is never reached.
//
// This is the honest model. An earlier revision forced NormalizedType to differ, which
// made the evaluator's override look load-bearing for every width — a state the library
// cannot produce.
func TestSchemaCompatOrdinaryDisplayWidthEmitsNoDiff(t *testing.T) {
	for _, tc := range []struct{ aType, bType, normalized string }{
		{"bigint(20)", "bigint", "bigint"},
		{"int(11)", "int", "int"},
		{"int(10) unsigned", "int unsigned", "int unsigned"},
	} {
		t.Run(tc.aType+"_vs_"+tc.bType, func(t *testing.T) {
			ca, cb := specCol(2, "n", tc.aType), specCol(2, "n", tc.bType)
			ca.NormalizedType, cb.NormalizedType = tc.normalized, tc.normalized

			a := []validations.ColumnSpec{specCol(1, "id", "bigint"), ca}
			b := []validations.ColumnSpec{specCol(1, "id", "bigint"), cb}

			if diffs := validations.DiffSpecs(pairOf(a, b).A, pairOf(a, b).B); len(diffs) != 0 {
				t.Fatalf("the library should absorb this width difference entirely, got %v", diffs)
			}
			v, err := evaluateSchemaCompatibility(pairOf(a, b), true)
			if err != nil {
				t.Fatalf("evaluateSchemaCompatibility: %v", err)
			}
			if v.Fatal != "" {
				t.Fatalf("must not be fatal, got: %s", v.Fatal)
			}
		})
	}
}

// TestSchemaCompatTinyint1IsAcceptedByPolicy is the ONE live case for
// goarchiveTypesCompatible. The library deliberately preserves tinyint(1) (BOOLEAN is an
// alias for it), so NormalizedType really does differ here and DiffSpecs really does emit
// ColumnTypeMismatch — GoArchive's policy override is what accepts it, as spec §3.3
// requires and as 1.8 behaved.
func TestSchemaCompatTinyint1IsAcceptedByPolicy(t *testing.T) {
	ca, cb := specCol(2, "flag", "tinyint(1)"), specCol(2, "flag", "tinyint")
	ca.NormalizedType, cb.NormalizedType = "tinyint(1)", "tinyint" // as the library produces

	a := []validations.ColumnSpec{specCol(1, "id", "bigint"), ca}
	b := []validations.ColumnSpec{specCol(1, "id", "bigint"), cb}

	diffs := validations.DiffSpecs(pairOf(a, b).A, pairOf(a, b).B)
	var sawType bool
	for _, d := range diffs {
		if d.Kind == validations.ColumnTypeMismatch {
			sawType = true
		}
	}
	if !sawType {
		t.Fatal("the library must still EMIT ColumnTypeMismatch for tinyint(1) vs tinyint; " +
			"if this fails the library changed and the override may now be dead code")
	}

	v, err := evaluateSchemaCompatibility(pairOf(a, b), true)
	if err != nil {
		t.Fatalf("evaluateSchemaCompatibility: %v", err)
	}
	if v.Fatal != "" {
		t.Fatalf("tinyint(1) vs tinyint must be accepted (spec §3.3), got fatal: %s", v.Fatal)
	}
}

// TestSchemaCompatUnsignedIsNotIgnored guards the other half: unsigned changes the value
// range, so it must stay fatal. NormalizedType is set as the library really produces it —
// "int unsigned" vs "int" — not forced.
func TestSchemaCompatUnsignedIsNotIgnored(t *testing.T) {
	ca, cb := specCol(2, "n", "int(10) unsigned"), specCol(2, "n", "int")
	ca.NormalizedType, cb.NormalizedType = "int unsigned", "int"

	a := []validations.ColumnSpec{specCol(1, "id", "bigint"), ca}
	b := []validations.ColumnSpec{specCol(1, "id", "bigint"), cb}
	v, _ := evaluateSchemaCompatibility(pairOf(a, b), true)
	if v.Fatal == "" {
		t.Fatal("int unsigned vs int must stay fatal — the value range differs")
	}
}

// TestSchemaCompatColumnOrderMismatchIsFatal covers the ColumnOrderMismatch row. The copy
// builds its INSERT column list from the source order, so a reordered destination would
// write values into the wrong columns.
func TestSchemaCompatColumnOrderMismatchIsFatal(t *testing.T) {
	a := []validations.ColumnSpec{specCol(1, "id", "bigint"), specCol(2, "x", "int"), specCol(3, "y", "int")}
	b := []validations.ColumnSpec{specCol(1, "id", "bigint"), specCol(2, "y", "int"), specCol(3, "x", "int")}
	v, err := evaluateSchemaCompatibility(pairOf(a, b), true)
	if err != nil {
		t.Fatalf("evaluateSchemaCompatibility: %v", err)
	}
	if v.Fatal == "" {
		t.Fatal("reordered destination columns must be fatal")
	}
}

// TestSchemaCompatNullabilityIsDirectional covers the ColumnNullabilityMismatch row.
func TestSchemaCompatNullabilityIsDirectional(t *testing.T) {
	t.Run("destination_stricter_is_fatal", func(t *testing.T) {
		ca := specCol(2, "note", "varchar(64)")
		ca.Nullable = true
		cb := specCol(2, "note", "varchar(64)")
		cb.Nullable = false
		a := []validations.ColumnSpec{specCol(1, "id", "bigint"), ca}
		b := []validations.ColumnSpec{specCol(1, "id", "bigint"), cb}
		v, _ := evaluateSchemaCompatibility(pairOf(a, b), true)
		if v.Fatal == "" {
			t.Fatal("source-nullable into destination-NOT NULL must be fatal")
		}
	})
	t.Run("destination_looser_passes", func(t *testing.T) {
		ca := specCol(2, "note", "varchar(64)")
		ca.Nullable = false
		cb := specCol(2, "note", "varchar(64)")
		cb.Nullable = true
		a := []validations.ColumnSpec{specCol(1, "id", "bigint"), ca}
		b := []validations.ColumnSpec{specCol(1, "id", "bigint"), cb}
		v, _ := evaluateSchemaCompatibility(pairOf(a, b), true)
		if v.Fatal != "" {
			t.Fatalf("relaxing NOT NULL is allowed, got: %s", v.Fatal)
		}
	})
}

// TestSchemaCompatCharsetFatalityFollowsVerification covers the ColumnCharsetMismatch row.
func TestSchemaCompatCharsetFatalityFollowsVerification(t *testing.T) {
	ca := specCol(2, "note", "varchar(64)")
	ca.Charset, ca.Collation = "utf8mb4", "utf8mb4_bin"
	cb := specCol(2, "note", "varchar(64)")
	cb.Charset, cb.Collation = "latin1", "latin1_swedish_ci"
	a := []validations.ColumnSpec{specCol(1, "id", "bigint"), ca}
	b := []validations.ColumnSpec{specCol(1, "id", "bigint"), cb}

	strict, _ := evaluateSchemaCompatibility(pairOf(a, b), true)
	if strict.Fatal == "" {
		t.Fatal("charset mismatch must be fatal under count verification / --skip-verify")
	}
	advisory, _ := evaluateSchemaCompatibility(pairOf(a, b), false)
	if advisory.Fatal != "" {
		t.Fatalf("charset mismatch must be advisory under a sha256 verification, got: %s", advisory.Fatal)
	}
	if len(advisory.Warnings) != 1 || !strings.Contains(advisory.Warnings[0], "charset differs") {
		t.Fatalf("expected exactly one charset advisory, got %v", advisory.Warnings)
	}
}

// TestSchemaCompatCollationWarningSuppressedByCharset covers the suppression rule: at
// most one advisory per column, charset first.
func TestSchemaCompatCollationWarningSuppressedByCharset(t *testing.T) {
	ca := specCol(2, "note", "varchar(64)")
	ca.Charset, ca.Collation = "utf8mb4", "utf8mb4_bin"
	cb := specCol(2, "note", "varchar(64)")
	cb.Charset, cb.Collation = "latin1", "latin1_swedish_ci"
	a := []validations.ColumnSpec{specCol(1, "id", "bigint"), ca}
	b := []validations.ColumnSpec{specCol(1, "id", "bigint"), cb}

	v, _ := evaluateSchemaCompatibility(pairOf(a, b), false)
	for _, w := range v.Warnings {
		if strings.Contains(w, "collation differs") {
			t.Fatalf("collation advisory must be suppressed when charset already warned: %v", v.Warnings)
		}
	}
}

// TestSchemaCompatCollationWarnsAlone covers the other half: same charset, different
// collation, one advisory.
func TestSchemaCompatCollationWarnsAlone(t *testing.T) {
	ca := specCol(2, "note", "varchar(64)")
	ca.Charset, ca.Collation = "utf8mb4", "utf8mb4_bin"
	cb := specCol(2, "note", "varchar(64)")
	cb.Charset, cb.Collation = "utf8mb4", "utf8mb4_0900_ai_ci"
	a := []validations.ColumnSpec{specCol(1, "id", "bigint"), ca}
	b := []validations.ColumnSpec{specCol(1, "id", "bigint"), cb}

	v, _ := evaluateSchemaCompatibility(pairOf(a, b), true)
	if v.Fatal != "" {
		t.Fatalf("collation-only difference must not be fatal here, got: %s", v.Fatal)
	}
	if len(v.Warnings) != 1 || !strings.Contains(v.Warnings[0], "collation differs") {
		t.Fatalf("expected exactly one collation advisory, got %v", v.Warnings)
	}
}

// TestSchemaCompatLooserDestinationAllowed covers the allowed rows: dropped defaults,
// auto_increment and ON UPDATE.
func TestSchemaCompatLooserDestinationAllowed(t *testing.T) {
	def := "0"
	ca := specCol(2, "n", "int")
	ca.Default, ca.AutoIncrement, ca.OnUpdate, ca.DefaultIsExpression = &def, true, true, true
	cb := specCol(2, "n", "int")

	a := []validations.ColumnSpec{specCol(1, "id", "bigint"), ca}
	b := []validations.ColumnSpec{specCol(1, "id", "bigint"), cb}
	v, err := evaluateSchemaCompatibility(pairOf(a, b), true)
	if err != nil {
		t.Fatalf("evaluateSchemaCompatibility: %v", err)
	}
	if v.Fatal != "" {
		t.Fatalf("dropping defaults/auto_increment/ON UPDATE is allowed, got: %s", v.Fatal)
	}
}

// TestSchemaCompatUnknownDiffKindFailsClosed enforces the spec's fail-closed rule for a
// diff kind published after this design.
func TestSchemaCompatUnknownDiffKindFailsClosed(t *testing.T) {
	_, err := disposeDiff(validations.SpecDiff{Kind: validations.SpecDiffKind(250), Column: "x"}, true, nil)
	if err == nil {
		t.Fatal("an unrecognised SpecDiffKind must abort preflight")
	}
	if !strings.Contains(err.Error(), "250") {
		t.Fatalf("the abort must name the unrecognised kind: %v", err)
	}
}

// TestSchemaCompatIndexUnconfirmedAbortsPreflight covers the inspection-integrity guard
// through a REAL DiffSpecs call (not a hand-built SpecDiff): breaking the symmetry
// WithIndexes() is supposed to guarantee on both sides makes DiffSpecs itself emit
// IndexUnconfirmed, and evaluateSchemaCompatibility must abort rather than silently treat
// the pair as compatible. This is also the unit case mutants 5/6 (dropping WithIndexes()
// from one side of tableSpecs) would reach if the fetch-level guard in
// TestPreflightRunMemoizesTableSpecs were ever weakened.
func TestSchemaCompatIndexUnconfirmedAbortsPreflight(t *testing.T) {
	cols := []validations.ColumnSpec{specCol(1, "id", "bigint")}
	pair := pairOf(cols, cols)
	pair.B.Captured = 0 // simulate the destination side never having captured indexes

	_, err := evaluateSchemaCompatibility(pair, true)
	if err == nil {
		t.Fatal("an asymmetric index-capture must abort preflight, not silently pass")
	}
	if !strings.Contains(err.Error(), "IndexUnconfirmed") {
		t.Fatalf("abort must name IndexUnconfirmed, got: %v", err)
	}
}
