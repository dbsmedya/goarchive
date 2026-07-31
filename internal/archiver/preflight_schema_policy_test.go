package archiver

import (
	"errors"
	"fmt"
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

// TestSchemaCompatUncapturedIndexesAbortBeforeDiffSpecs is the evaluator-level proof that
// the Captured guard runs FIRST.
//
// Before phase 029 this pair reached DiffSpecs, which emitted IndexUnconfirmed, and
// disposeDiff aborted on it. Now checkAbsoluteInvariants rejects the pair before DiffSpecs
// is called at all. The negative assertion is the load-bearing one: if the invariant were
// moved after the diff loop — or removed — the pair would still abort, but with
// IndexUnconfirmed in the message, and only that assertion would notice.
func TestSchemaCompatUncapturedIndexesAbortBeforeDiffSpecs(t *testing.T) {
	cols := []validations.ColumnSpec{specCol(1, "id", "bigint")}
	pair := pairOf(cols, cols)
	pair.B.Captured = 0 // simulate the destination side never having captured indexes

	_, err := evaluateSchemaCompatibility(pair, true)
	if err == nil {
		t.Fatal("an asymmetric index-capture must abort preflight, not silently pass")
	}
	if !strings.Contains(err.Error(), "did not capture its index section") {
		t.Fatalf("abort must come from the Captured guard, got: %v", err)
	}
	if !strings.Contains(err.Error(), "destination") {
		t.Fatalf("abort must name the offending side, got: %v", err)
	}
	if strings.Contains(err.Error(), "IndexUnconfirmed") {
		t.Fatalf("the guard must run BEFORE DiffSpecs; this abort came from the diff loop: %v", err)
	}
}

// pairWithIndexes builds a specPair with explicit index sections on both sides.
func pairWithIndexes(
	aCols, bCols []validations.ColumnSpec,
	aIdx, bIdx []validations.IndexSpec,
) specPair {
	return specPair{
		Table: "orders",
		A: validations.TableSpec{Schema: "srcdb", Table: "orders", Columns: aCols,
			Indexes: aIdx, Captured: validations.SectionIndexes},
		B: validations.TableSpec{Schema: "dstdb", Table: "orders", Columns: bCols,
			Indexes: bIdx, Captured: validations.SectionIndexes},
	}
}

func primaryOn(parts ...validations.IndexPart) validations.IndexSpec {
	return validations.IndexSpec{Name: "PRIMARY", Unique: true, Type: "BTREE", Visible: true, Parts: parts}
}

// TestAbsoluteInvariantDestinationGeneratedIsFatal covers the first rule, including the
// case the spec calls out explicitly: the source column is IDENTICALLY generated and it
// is still fatal, because the copy inserts explicit values for every column and MySQL
// rejects them with Error 3105 even under INSERT IGNORE.
func TestAbsoluteInvariantDestinationGeneratedIsFatal(t *testing.T) {
	for _, kind := range []validations.GeneratedKind{validations.GeneratedVirtual, validations.GeneratedStored} {
		t.Run(kind.String(), func(t *testing.T) {
			gen := specCol(2, "doubled", "int")
			gen.Generated, gen.GenerationExpr = kind, "(`amount` * 2)"

			cols := []validations.ColumnSpec{specCol(1, "id", "bigint"), gen}
			pair := pairWithIndexes(cols, cols,
				[]validations.IndexSpec{primaryOn(validations.IndexPart{Column: "id"})},
				[]validations.IndexSpec{primaryOn(validations.IndexPart{Column: "id"})})

			reason, err := checkAbsoluteInvariants(pair)
			if err != nil {
				t.Fatalf("checkAbsoluteInvariants: %v", err)
			}
			if reason == "" {
				t.Fatal("a generated DESTINATION column must be fatal even when the source matches")
			}
			if !strings.Contains(reason, "3105") {
				t.Fatalf("the reason must name MySQL Error 3105: %q", reason)
			}
		})
	}
}

// TestAbsoluteInvariantUnknownGeneratedAborts pins inspection integrity: GeneratedUnknown
// is the ZERO value of GeneratedKind, so it means "the fact was never populated", not
// "plain column". Accepting it would silently treat an unpopulated spec as safe.
func TestAbsoluteInvariantUnknownGeneratedAborts(t *testing.T) {
	unknown := specCol(2, "amount", "int")
	unknown.Generated = validations.GeneratedUnknown
	pair := pairWithIndexes(
		[]validations.ColumnSpec{specCol(1, "id", "bigint"), unknown},
		[]validations.ColumnSpec{specCol(1, "id", "bigint"), unknown},
		[]validations.IndexSpec{primaryOn(validations.IndexPart{Column: "id"})},
		[]validations.IndexSpec{primaryOn(validations.IndexPart{Column: "id"})})

	if _, err := checkAbsoluteInvariants(pair); err == nil {
		t.Fatal("GeneratedUnknown must abort as an unpopulated fact")
	}
}

// TestAbsoluteInvariantSourceOnlyGeneratedIsAllowed covers the other direction: a
// source-generated column writing into a plain destination column is fine, because
// SELECT materialises the value.
func TestAbsoluteInvariantSourceOnlyGeneratedIsAllowed(t *testing.T) {
	gen := specCol(2, "doubled", "int")
	gen.Generated, gen.GenerationExpr = validations.GeneratedStored, "(`amount` * 2)"
	plain := specCol(2, "doubled", "int")

	pair := pairWithIndexes(
		[]validations.ColumnSpec{specCol(1, "id", "bigint"), gen},
		[]validations.ColumnSpec{specCol(1, "id", "bigint"), plain},
		[]validations.IndexSpec{primaryOn(validations.IndexPart{Column: "id"})},
		[]validations.IndexSpec{primaryOn(validations.IndexPart{Column: "id"})})

	reason, err := checkAbsoluteInvariants(pair)
	if err != nil {
		t.Fatalf("checkAbsoluteInvariants: %v", err)
	}
	if reason != "" {
		t.Fatalf("source-only generated column is allowed, got: %s", reason)
	}
}

// TestAbsoluteInvariantPrimaryKeyMustMatch covers the second rule in all four ways it
// can be violated.
func TestAbsoluteInvariantPrimaryKeyMustMatch(t *testing.T) {
	cols := []validations.ColumnSpec{specCol(1, "id", "bigint"), specCol(2, "tenant_id", "int")}

	cases := []struct {
		name    string
		aIdx    []validations.IndexSpec
		bIdx    []validations.IndexSpec
		wantBad bool
	}{
		{
			name:    "identical",
			aIdx:    []validations.IndexSpec{primaryOn(validations.IndexPart{Column: "id"})},
			bIdx:    []validations.IndexSpec{primaryOn(validations.IndexPart{Column: "id"})},
			wantBad: false,
		},
		{
			name:    "destination_missing_primary",
			aIdx:    []validations.IndexSpec{primaryOn(validations.IndexPart{Column: "id"})},
			bIdx:    nil,
			wantBad: true,
		},
		{
			name:    "different_column",
			aIdx:    []validations.IndexSpec{primaryOn(validations.IndexPart{Column: "id"})},
			bIdx:    []validations.IndexSpec{primaryOn(validations.IndexPart{Column: "tenant_id"})},
			wantBad: true,
		},
		{
			name: "different_order",
			aIdx: []validations.IndexSpec{primaryOn(
				validations.IndexPart{Column: "id"}, validations.IndexPart{Column: "tenant_id"})},
			bIdx: []validations.IndexSpec{primaryOn(
				validations.IndexPart{Column: "tenant_id"}, validations.IndexPart{Column: "id"})},
			wantBad: true,
		},
		{
			name:    "different_prefix",
			aIdx:    []validations.IndexSpec{primaryOn(validations.IndexPart{Column: "id"})},
			bIdx:    []validations.IndexSpec{primaryOn(validations.IndexPart{Column: "id", SubPart: 10})},
			wantBad: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reason, err := checkAbsoluteInvariants(pairWithIndexes(cols, cols, tc.aIdx, tc.bIdx))
			if err != nil {
				t.Fatalf("checkAbsoluteInvariants: %v", err)
			}
			if tc.wantBad && reason == "" {
				t.Fatal("expected a fatal PRIMARY key mismatch")
			}
			if !tc.wantBad && reason != "" {
				t.Fatalf("expected clean, got: %s", reason)
			}
		})
	}
}

// TestAbsoluteInvariantPrimaryKeyIgnoresDescending is the fourth review's precision:
// direction does not affect INSERT IGNORE idempotency, and the 1.8 COLUMN_KEY policy
// never observed it, so comparing it would introduce a NEW failure.
func TestAbsoluteInvariantPrimaryKeyIgnoresDescending(t *testing.T) {
	cols := []validations.ColumnSpec{specCol(1, "id", "bigint")}
	aIdx := []validations.IndexSpec{primaryOn(validations.IndexPart{Column: "id"})}
	bIdx := []validations.IndexSpec{primaryOn(validations.IndexPart{Column: "id", Descending: true})}

	reason, err := checkAbsoluteInvariants(pairWithIndexes(cols, cols, aIdx, bIdx))
	if err != nil {
		t.Fatalf("checkAbsoluteInvariants: %v", err)
	}
	if reason != "" {
		t.Fatalf("PRIMARY key direction must be ignored, got: %s", reason)
	}
}

// TestAbsoluteInvariantUncapturedIndexesAborts covers the third rule: a spec that did not
// capture its index section makes the whole evaluation untrustworthy, and that is an
// engine error, not a schema verdict.
func TestAbsoluteInvariantUncapturedIndexesAborts(t *testing.T) {
	cols := []validations.ColumnSpec{specCol(1, "id", "bigint")}
	pair := specPair{
		Table: "orders",
		A: validations.TableSpec{Schema: "srcdb", Table: "orders", Columns: cols,
			Indexes:  []validations.IndexSpec{primaryOn(validations.IndexPart{Column: "id"})},
			Captured: validations.SectionIndexes},
		B: validations.TableSpec{Schema: "dstdb", Table: "orders", Columns: cols}, // Captured == 0
	}
	_, err := checkAbsoluteInvariants(pair)
	if err == nil {
		t.Fatal("an uncaptured index section must abort preflight")
	}
	if !strings.Contains(err.Error(), "destination") {
		t.Fatalf("the abort must name the offending side: %v", err)
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatal("an inspection-integrity failure must not be a *PreflightError")
	}
}

// TestAbsoluteInvariantBothSidesUncapturedNamesSourceFirst pins two things at once.
//
// FIRST, the hole this rule exists to close: when NEITHER side captured indexes, the
// library's diffIndexes returns nil rather than IndexUnconfirmed (sectionAgreement's
// SideUnknown arm, spec_diff.go:339-347), so a pair like this is SILENTLY judged
// compatible without the Captured guard. Nothing else in the suite covers that case.
//
// SECOND, attribution is deterministic: the sides are checked source-first from an
// ordered slice, never a map range, so the reported side cannot vary between runs.
func TestAbsoluteInvariantBothSidesUncapturedNamesSourceFirst(t *testing.T) {
	cols := []validations.ColumnSpec{specCol(1, "id", "bigint")}
	pair := specPair{
		Table: "orders",
		A:     validations.TableSpec{Schema: "srcdb", Table: "orders", Columns: cols},
		B:     validations.TableSpec{Schema: "dstdb", Table: "orders", Columns: cols},
	}
	for i := 0; i < 20; i++ {
		_, err := checkAbsoluteInvariants(pair)
		if err == nil {
			t.Fatal("both sides uncaptured must abort; DiffSpecs alone reports nothing here")
		}
		if !strings.Contains(err.Error(), "source") {
			t.Fatalf("attribution must be deterministic and source-first, got: %v", err)
		}
	}
}

// TestPrimaryPartSignatureIsTotalAndCollisionResistant pins primaryPartSignature's
// defensive contract directly, because no PRIMARY-key fixture can reach the expression
// branch: MySQL does not permit functional primary key parts.
//
// The branch is kept anyway, and this is why: mapping every expression part to the empty
// identity would make two DIFFERENT expressions compare EQUAL — a silent false all-clear
// in the one place the code is least able to check itself. "Impossible today" is not a
// reason to make the function non-total.
func TestPrimaryPartSignatureIsTotalAndCollisionResistant(t *testing.T) {
	t.Run("different_expressions_do_not_collide", func(t *testing.T) {
		lower := primaryPartSignature(validations.IndexPart{Expression: "lower(`email`)"})
		upper := primaryPartSignature(validations.IndexPart{Expression: "upper(`email`)"})
		if lower == upper {
			t.Fatalf("two different expressions collided on %q", lower)
		}
	})
	// The namespace collision is only reachable when the column's own text can imitate the
	// expression tag. Comparing Column:"email" against Expression:"email" does NOT test
	// this — it passes against a bare column identity, because "email" and "expr:email"
	// differ anyway. The real case is a column literally NAMED "expr:email", which MySQL
	// permits inside a backtick-quoted identifier. That is what forces BOTH branches to be
	// tagged rather than just the expression one.
	t.Run("a_column_named_like_an_expression_tag_does_not_collide", func(t *testing.T) {
		col := primaryPartSignature(validations.IndexPart{Column: "expr:email"})
		expr := primaryPartSignature(validations.IndexPart{Expression: "email"})
		if col == expr {
			t.Fatalf("a column named %q collided with the expression %q on %s", "expr:email", "email", col)
		}
	})
	t.Run("expression_prefix_length_participates", func(t *testing.T) {
		whole := primaryPartSignature(validations.IndexPart{Expression: "lower(`email`)"})
		prefixed := primaryPartSignature(validations.IndexPart{Expression: "lower(`email`)", SubPart: 10})
		if whole == prefixed {
			t.Fatalf("prefix length dropped out of the signature: %q", whole)
		}
	})
	t.Run("column_prefix_length_participates", func(t *testing.T) {
		whole := primaryPartSignature(validations.IndexPart{Column: "email"})
		prefixed := primaryPartSignature(validations.IndexPart{Column: "email", SubPart: 10})
		if whole == prefixed {
			t.Fatalf("prefix length dropped out of the signature: %q", whole)
		}
	})
	t.Run("descending_is_deliberately_ignored", func(t *testing.T) {
		asc := primaryPartSignature(validations.IndexPart{Column: "id"})
		desc := primaryPartSignature(validations.IndexPart{Column: "id", Descending: true})
		if asc != desc {
			t.Fatalf("direction must not participate: %q vs %q", asc, desc)
		}
	})
}

// TestSchemaCompatInvariantsRunBeforeDiffs is the ONLY unit test that can detect the
// invariants not being wired into evaluateSchemaCompatibility. Every
// TestAbsoluteInvariant* test above calls checkAbsoluteInvariants directly and would
// still pass with the call site deleted.
//
// The fixture is deliberately one DiffSpecs reports as clean: source and destination are
// IDENTICAL, both carrying the same generated column, so there is no
// ColumnGeneratedMismatch to dispose of — and disposeDiff ignores that kind anyway. A
// fatal verdict here can only come from the invariant.
func TestSchemaCompatInvariantsRunBeforeDiffs(t *testing.T) {
	gen := specCol(2, "doubled", "int")
	gen.Generated, gen.GenerationExpr = validations.GeneratedStored, "(`amount` * 2)"
	cols := []validations.ColumnSpec{specCol(1, "id", "bigint"), gen}

	v, err := evaluateSchemaCompatibility(pairOf(cols, cols), true)
	if err != nil {
		t.Fatalf("evaluateSchemaCompatibility: %v", err)
	}
	if v.Fatal == "" {
		t.Fatal("checkAbsoluteInvariants is not wired into evaluateSchemaCompatibility")
	}
	if !strings.Contains(v.Fatal, "3105") {
		t.Fatalf("expected the destination-generated reason, got: %s", v.Fatal)
	}
}

// TestDisposeDiffIndexUnconfirmedFailsClosed keeps the defensive enum arm covered.
//
// After phase 029 that arm is UNREACHABLE through evaluateSchemaCompatibility — the
// Captured guard rejects a one-sided capture before DiffSpecs runs — so it can only be
// exercised by calling disposeDiff directly. Without this test the arm would ship
// untested, and a later reader could delete it into the fail-closed default with a
// misleading message.
func TestDisposeDiffIndexUnconfirmedFailsClosed(t *testing.T) {
	_, err := disposeDiff(
		validations.SpecDiff{Kind: validations.IndexUnconfirmed, Side: validations.SideB}, true, nil)
	if err == nil {
		t.Fatal("IndexUnconfirmed must abort preflight")
	}
	if !strings.Contains(err.Error(), "IndexUnconfirmed") {
		t.Fatalf("the abort must name the diff kind: %v", err)
	}
}

func uniqueIdx(name string, parts ...validations.IndexPart) validations.IndexSpec {
	return validations.IndexSpec{Name: name, Unique: true, Type: "BTREE", Visible: true, Parts: parts}
}

func strCol(ordinal int, name, charset, collation string) validations.ColumnSpec {
	c := specCol(ordinal, name, "varchar(64)")
	c.Charset, c.Collation = charset, collation
	return c
}

// d3Pair builds a table whose source and destination differ only in their unique indexes.
func d3Pair(cols []validations.ColumnSpec, aIdx, bIdx []validations.IndexSpec) specPair {
	primary := primaryOn(validations.IndexPart{Column: "id"})
	return pairWithIndexes(cols, cols,
		append([]validations.IndexSpec{primary}, aIdx...),
		append([]validations.IndexSpec{primary}, bIdx...))
}

// GAP 1 — TestD3CompositeDestinationOnlyIsFatal.
// UNIQUE(a,b) on the destination only. COLUMN_KEY reported MUL for `a`, so 1.8 passed it.
func TestD3CompositeDestinationOnlyIsFatal(t *testing.T) {
	cols := []validations.ColumnSpec{
		specCol(1, "id", "bigint"), specCol(2, "a", "int"), specCol(3, "b", "int")}
	pair := d3Pair(cols, nil, []validations.IndexSpec{
		uniqueIdx("uq_ab", validations.IndexPart{Column: "a"}, validations.IndexPart{Column: "b"})})

	if reason := checkDestinationUniqueness(pair, nil); reason == "" {
		t.Fatal("a destination-only composite UNIQUE must be fatal (INSERT IGNORE silently skips)")
	}
}

// GAP 2 — TestD3PrefixDifferenceIsFatal.
// Source UNIQUE(email), destination UNIQUE(email(10)). Both reported UNI, so 1.8 passed it.
func TestD3PrefixDifferenceIsFatal(t *testing.T) {
	cols := []validations.ColumnSpec{
		specCol(1, "id", "bigint"), strCol(2, "email", "utf8mb4", "utf8mb4_bin")}
	pair := d3Pair(cols,
		[]validations.IndexSpec{uniqueIdx("uq_email", validations.IndexPart{Column: "email"})},
		[]validations.IndexSpec{uniqueIdx("uq_email", validations.IndexPart{Column: "email", SubPart: 10})})

	if reason := checkDestinationUniqueness(pair, nil); reason == "" {
		t.Fatal("a prefix difference changes the uniqueness predicate and must be fatal")
	}
}

// GAP 3 — TestD3FunctionalDestinationOnlyIsFatal.
// Destination-only UNIQUE((lower(email))). No column part, so COLUMN_KEY stayed empty.
func TestD3FunctionalDestinationOnlyIsFatal(t *testing.T) {
	cols := []validations.ColumnSpec{
		specCol(1, "id", "bigint"), strCol(2, "email", "utf8mb4", "utf8mb4_bin")}
	pair := d3Pair(cols, nil, []validations.IndexSpec{
		uniqueIdx("uq_lower", validations.IndexPart{Expression: "lower(`email`)"})})

	if reason := checkDestinationUniqueness(pair, nil); reason == "" {
		t.Fatal("a destination-only functional UNIQUE must be fatal")
	}
}

// GAP 4 — TestD3CollationDifferenceIsFatalRegardlessOfVerification.
// Same UNIQUE(email) topology, looser destination collation. 1.8 warned only.
func TestD3CollationDifferenceIsFatalRegardlessOfVerification(t *testing.T) {
	aCols := []validations.ColumnSpec{
		specCol(1, "id", "bigint"), strCol(2, "email", "utf8mb4", "utf8mb4_bin")}
	bCols := []validations.ColumnSpec{
		specCol(1, "id", "bigint"), strCol(2, "email", "utf8mb4", "utf8mb4_0900_ai_ci")}

	primary := primaryOn(validations.IndexPart{Column: "id"})
	idx := uniqueIdx("uq_email", validations.IndexPart{Column: "email"})
	pair := pairWithIndexes(aCols, bCols,
		[]validations.IndexSpec{primary, idx}, []validations.IndexSpec{primary, idx})

	if reason := checkDestinationUniqueness(pair, nil); reason == "" {
		t.Fatal("a collation-loosened destination UNIQUE collides rows the source held " +
			"distinct and must be fatal, regardless of verification mode")
	}
}

// GUARD 1 — TestD3RenamedEquivalentIndexPasses. The index NAME is ignored.
func TestD3RenamedEquivalentIndexPasses(t *testing.T) {
	cols := []validations.ColumnSpec{
		specCol(1, "id", "bigint"), strCol(2, "email", "utf8mb4", "utf8mb4_bin")}
	pair := d3Pair(cols,
		[]validations.IndexSpec{uniqueIdx("uq_email_src", validations.IndexPart{Column: "email"})},
		[]validations.IndexSpec{uniqueIdx("uq_email_dst", validations.IndexPart{Column: "email"})})

	if reason := checkDestinationUniqueness(pair, nil); reason != "" {
		t.Fatalf("renaming an equivalent index must not false-fail, got: %s", reason)
	}
}

// GUARD 2 — TestD3PartOrderIsIgnored. UNIQUE(a,b) and UNIQUE(b,a) enforce the same
// row-uniqueness predicate and are equivalent by declared intent.
func TestD3PartOrderIsIgnored(t *testing.T) {
	cols := []validations.ColumnSpec{
		specCol(1, "id", "bigint"), specCol(2, "a", "int"), specCol(3, "b", "int")}
	pair := d3Pair(cols,
		[]validations.IndexSpec{uniqueIdx("uq1",
			validations.IndexPart{Column: "a"}, validations.IndexPart{Column: "b"})},
		[]validations.IndexSpec{uniqueIdx("uq2",
			validations.IndexPart{Column: "b"}, validations.IndexPart{Column: "a"})})

	if reason := checkDestinationUniqueness(pair, nil); reason != "" {
		t.Fatalf("part order must be ignored, got: %s", reason)
	}
}

// GUARD 3 — TestD3DescendingIsIgnored. Direction does not change the predicate.
func TestD3DescendingIsIgnored(t *testing.T) {
	cols := []validations.ColumnSpec{
		specCol(1, "id", "bigint"), strCol(2, "email", "utf8mb4", "utf8mb4_bin")}
	pair := d3Pair(cols,
		[]validations.IndexSpec{uniqueIdx("uq", validations.IndexPart{Column: "email"})},
		[]validations.IndexSpec{uniqueIdx("uq", validations.IndexPart{Column: "email", Descending: true})})

	if reason := checkDestinationUniqueness(pair, nil); reason != "" {
		t.Fatalf("index direction must be ignored, got: %s", reason)
	}
}

// GUARD 4 — TestD3SourceOnlyUniqueIsAllowed. A looser destination is always fine.
func TestD3SourceOnlyUniqueIsAllowed(t *testing.T) {
	cols := []validations.ColumnSpec{
		specCol(1, "id", "bigint"), strCol(2, "email", "utf8mb4", "utf8mb4_bin")}
	pair := d3Pair(cols,
		[]validations.IndexSpec{uniqueIdx("uq_email", validations.IndexPart{Column: "email"})},
		nil)

	if reason := checkDestinationUniqueness(pair, nil); reason != "" {
		t.Fatalf("a source-only unique index is allowed, got: %s", reason)
	}
}

// GUARD 5 — TestD3PrimaryIsNotJudgedHere. PRIMARY is a unique index, but its equality is
// the absolute invariant's job (phase 029); judging it twice would produce two different
// messages for one defect.
func TestD3PrimaryIsNotJudgedHere(t *testing.T) {
	cols := []validations.ColumnSpec{specCol(1, "id", "bigint"), specCol(2, "alt", "bigint")}
	pair := pairWithIndexes(cols, cols,
		[]validations.IndexSpec{primaryOn(validations.IndexPart{Column: "id"})},
		[]validations.IndexSpec{primaryOn(validations.IndexPart{Column: "alt"})})

	if reason := checkDestinationUniqueness(pair, nil); reason != "" {
		t.Fatalf("PRIMARY is judged by checkAbsoluteInvariants, not here; got: %s", reason)
	}
}

// FUNCTIONAL — TestD3FunctionalMatchAllowedOnlyInACleanColumnEnvironment.
// IndexPart cannot expose an expression's RESULT collation, so an identical expression
// text is accepted only when the table has no charset/collation diff on any column.
func TestD3FunctionalMatchAllowedOnlyInACleanColumnEnvironment(t *testing.T) {
	cols := []validations.ColumnSpec{
		specCol(1, "id", "bigint"), strCol(2, "email", "utf8mb4", "utf8mb4_bin")}
	idx := uniqueIdx("uq_lower", validations.IndexPart{Expression: "lower(`email`)"})
	pair := d3Pair(cols, []validations.IndexSpec{idx}, []validations.IndexSpec{idx})

	t.Run("clean_environment_passes", func(t *testing.T) {
		if reason := checkDestinationUniqueness(pair, nil); reason != "" {
			t.Fatalf("identical expression text in a diff-free table is accepted, got: %s", reason)
		}
	})

	t.Run("collation_diff_anywhere_makes_it_fatal", func(t *testing.T) {
		diffs := []validations.SpecDiff{
			{Kind: validations.ColumnCollationMismatch, Column: "unrelated", A: "x", B: "y"},
		}
		if reason := checkDestinationUniqueness(pair, diffs); reason == "" {
			t.Fatal("a charset/collation diff anywhere in the table removes the only available " +
				"proof that the expression's equality semantics match; it must be fatal")
		}
	})
}

// KIND — TestD3ColumnAndExpressionPartsDoNotCollide.
//
// A part's kind is carried by uniquePart.Expression. Without it, a column part and an
// expression part of the same text compare equal, and a destination UNIQUE(x) would be
// silently accepted as equivalent to a source UNIQUE((x)). The two enforce different
// predicates: one indexes the stored value, the other the expression's result.
//
// The column here is deliberately an INT, so columnCollation returns "" and Identity is
// the same text on both sides — the kind flag is the ONLY field that can separate them.
//
// (Phase 029 shipped the same class of defect in primaryPartSignature, and PR #59's review
// found a worse one here: both are recorded in the plan. This test predates neither fix and
// guards the property directly.)
func TestD3ColumnAndExpressionPartsDoNotCollide(t *testing.T) {
	cols := []validations.ColumnSpec{specCol(1, "id", "bigint"), specCol(2, "x", "int")}
	pair := d3Pair(cols,
		[]validations.IndexSpec{uniqueIdx("uq_expr", validations.IndexPart{Expression: "x"})},
		[]validations.IndexSpec{uniqueIdx("uq_col", validations.IndexPart{Column: "x"})})

	if reason := checkDestinationUniqueness(pair, nil); reason == "" {
		t.Fatal("a destination COLUMN unique must not match a source EXPRESSION unique of the " +
			"same text; they enforce different predicates")
	}
}

// WIRING — TestSchemaCompatD3IsWiredIntoTheEvaluator.
//
// Every TestD3* test above calls checkDestinationUniqueness DIRECTLY and therefore cannot
// detect the call site being deleted from evaluateSchemaCompatibility. This is the only
// unit test that can. (Phase 029 needed the identical companion for its invariants.)
func TestSchemaCompatD3IsWiredIntoTheEvaluator(t *testing.T) {
	cols := []validations.ColumnSpec{
		specCol(1, "id", "bigint"), strCol(2, "email", "utf8mb4", "utf8mb4_bin")}
	pair := d3Pair(cols, nil,
		[]validations.IndexSpec{uniqueIdx("uq_email", validations.IndexPart{Column: "email"})})

	v, err := evaluateSchemaCompatibility(pair, true)
	if err != nil {
		t.Fatalf("evaluateSchemaCompatibility: %v", err)
	}
	if v.Fatal == "" {
		t.Fatal("checkDestinationUniqueness is not wired into evaluateSchemaCompatibility")
	}
	if !strings.Contains(v.Fatal, "unique index") {
		t.Fatalf("expected the D3 reason, got: %s", v.Fatal)
	}
}

// ORDERING — TestSchemaCompatD3PrecedesColumnDispositions.
//
// Step 3 places D3 BEFORE the column-disposition loop deliberately: a destination unique
// index the source lacks is a data-loss hazard, and its message is more actionable than a
// column-level one on the same table. Nothing else pins that ordering — with D3 moved
// after the loop this table still fails, just with the type-mismatch message.
func TestSchemaCompatD3PrecedesColumnDispositions(t *testing.T) {
	aCols := []validations.ColumnSpec{
		specCol(1, "id", "bigint"), strCol(2, "email", "utf8mb4", "utf8mb4_bin")}
	bCols := []validations.ColumnSpec{
		specCol(1, "id", "bigint"), strCol(2, "email", "utf8mb4", "utf8mb4_bin")}
	bCols[1].Type, bCols[1].NormalizedType = "varchar(128)", "varchar(128)" // also a type mismatch

	pair := d3Pair(aCols, nil,
		[]validations.IndexSpec{uniqueIdx("uq_email", validations.IndexPart{Column: "email"})})
	pair.B.Columns = bCols

	v, err := evaluateSchemaCompatibility(pair, true)
	if err != nil {
		t.Fatalf("evaluateSchemaCompatibility: %v", err)
	}
	if !strings.Contains(v.Fatal, "unique index") {
		t.Fatalf("D3 must be reported ahead of the column disposition, got: %s", v.Fatal)
	}
}

// TestD3SignatureCannotBeForgedByAnIdentifier is a REGRESSION test for a fail-open hole
// found in external review of PR #59.
//
// The first implementation rendered each part as "col|<name>|<prefix>|<collation>" and
// joined parts with "&&". MySQL permits any character inside a backtick-quoted identifier,
// so a single column literally named `a|0|&&col|b` renders exactly what a two-part
// signature over (a, b) renders:
//
//	source      UNIQUE(a, b)              -> "col|a|0|" + "&&" + "col|b|0|"
//	destination UNIQUE(`a|0|&&col|b`)     -> "col|"     + "a|0|&&col|b" + "|0|"
//
// Byte-identical — so the destination's SINGLE-column unique was accepted as equivalent to
// the source's COMPOSITE unique. It is not: a single-column unique is STRICTER, and
// INSERT IGNORE would silently skip rows the source holds as distinct. That is the precise
// failure D3 exists to prevent, so an ambiguous encoding fails open on D3's whole purpose.
//
// Signatures are compared as typed values now; there is no encoding to forge.
func TestD3SignatureCannotBeForgedByAnIdentifier(t *testing.T) {
	const forged = "a|0|&&col|b"
	cols := []validations.ColumnSpec{
		specCol(1, "id", "bigint"),
		specCol(2, "a", "int"),
		specCol(3, "b", "int"),
		specCol(4, forged, "int"),
	}
	pair := d3Pair(cols,
		[]validations.IndexSpec{uniqueIdx("uq_ab",
			validations.IndexPart{Column: "a"}, validations.IndexPart{Column: "b"})},
		[]validations.IndexSpec{uniqueIdx("uq_forged", validations.IndexPart{Column: forged})})

	if reason := checkDestinationUniqueness(pair, nil); reason == "" {
		t.Fatalf("a destination UNIQUE(%q) must NOT match the source's composite UNIQUE(a, b); "+
			"the signature encoding was forged", forged)
	}
}

// allPublishedDiffKinds is every SpecDiffKind this build has classified in disposeDiff.
// SpecDiffUnknown is deliberately absent: it is the zero value and must hit the
// fail-closed default. TestDisposeDiffFailsClosedOnUnclassifiedKinds asserts that.
var allPublishedDiffKinds = []validations.SpecDiffKind{
	validations.EngineMismatch,
	validations.CharsetMismatch,
	validations.CollationMismatch,
	validations.CommentMismatch,
	validations.CommentUnconfirmed,

	validations.ColumnAbsent,
	validations.ColumnTypeMismatch,
	validations.ColumnNullabilityMismatch,
	validations.ColumnCharsetMismatch,
	validations.ColumnCollationMismatch,
	validations.ColumnDefaultMismatch,
	validations.ColumnOrderMismatch,
	validations.ColumnVisibilityMismatch,
	validations.ColumnGeneratedMismatch,
	validations.ColumnGenerationExprMismatch,
	validations.ColumnAutoIncrementMismatch,
	validations.ColumnOnUpdateMismatch,

	validations.IndexUnconfirmed,
	validations.IndexAbsent,
	validations.IndexPartsMismatch,
	validations.IndexUniquenessMismatch,
	validations.IndexTypeMismatch,
	validations.IndexVisibilityMismatch,

	validations.ConstraintUnconfirmed,
	validations.ConstraintAbsent,
	validations.ConstraintKindMismatch,
	validations.CheckClauseMismatch,
	validations.CheckEnforcementMismatch,
	validations.ForeignKeyColumnsMismatch,
	validations.ForeignKeyReferenceMismatch,
	validations.ForeignKeyRuleMismatch,
}

// TestDisposeDiffClassifiesEveryPublishedKind proves goarchive has an explicit case for
// every SpecDiffKind the pinned library publishes.
//
// "Classified" means disposeDiff does NOT return the PREFLIGHT_UNKNOWN_DIFF error. Some
// kinds are classified AS an error (the constraint/comment family, which cannot occur
// because those sections are not captured, and IndexUnconfirmed, which is an
// inspection-integrity failure) — that is a decision, not a gap.
//
// The kill this test owns is narrow and specific: deleting a `case` label so a published
// kind falls through to `default`. It deliberately does NOT check dispositions; the
// per-kind policy tests own those.
func TestDisposeDiffClassifiesEveryPublishedKind(t *testing.T) {
	if got, want := len(allPublishedDiffKinds), 31; got != want {
		t.Fatalf("the pinned library publishes %d classified kinds, the list has %d; "+
			"reconcile against spec_diff.go rather than trimming the list", want, got)
	}
	for _, kind := range allPublishedDiffKinds {
		t.Run(fmt.Sprintf("kind_%d", kind), func(t *testing.T) {
			_, err := disposeDiff(
				validations.SpecDiff{Kind: kind, Column: "c", Index: "i", A: "a", B: "b"},
				true, map[string]bool{})
			if err != nil && strings.Contains(err.Error(), "PREFLIGHT_UNKNOWN_DIFF") {
				t.Fatalf("SpecDiffKind %d is unclassified; add an explicit case to disposeDiff", kind)
			}
		})
	}
}

// TestDisposeDiffFailsClosedOnUnclassifiedKinds is the POSITIVE CONTROL for the guard
// above, and the only test in the repository that reaches disposeDiff's `default` arm.
//
// Two things are broken without it, and neither turns any other test red:
//
//  1. Mutating `default:` to `return nil, nil` — the exact fail-open this phase exists to
//     prevent — leaves the whole suite green, because every kind the guard iterates has an
//     explicit case and so never enters `default`.
//  2. The guard's sole kill mechanism is a substring match on the production error text.
//     Reword that message and the guard silently matches nothing, forever, with nothing
//     going red. This test pins the same substring from the other direction, so the two
//     cannot drift apart unnoticed.
//
// This is NOT the enum-boundary probing rejected in Ambiguities #2. It infers no iota
// range and asserts nothing about where the enum ends; it asserts that a value this build
// has not classified is refused. SpecDiffUnknown covers the zero value a zero-initialised
// struct produces; 200 covers a kind from a future library minor.
func TestDisposeDiffFailsClosedOnUnclassifiedKinds(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind validations.SpecDiffKind
	}{
		{"zero_value", validations.SpecDiffUnknown},
		{"future_library_kind", validations.SpecDiffKind(200)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := disposeDiff(
				validations.SpecDiff{Kind: tc.kind, Column: "c", Index: "i", A: "a", B: "b"},
				true, map[string]bool{})
			if err == nil {
				t.Fatalf("SpecDiffKind %d must abort preflight, got disposition %+v and nil error; "+
					"disposeDiff's default arm has been made fail-open", tc.kind, got)
			}
			if !strings.Contains(err.Error(), "PREFLIGHT_UNKNOWN_DIFF") {
				t.Fatalf("expected the PREFLIGHT_UNKNOWN_DIFF error, got: %v", err)
			}
		})
	}
}
