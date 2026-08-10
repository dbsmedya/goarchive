package archiver

import (
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
)

// TestDBSGOMySQLDiffSpecsDefaultAbsenceOrientation pins the exported semantic fix
// adopted in dbsgomysql v0.8.1. A nil default and an empty-string default both render as an empty
// SpecDiff value, so Side is the only typed fact that preserves which spec has no
// default. GoArchive does not model the library's SQL or JSON wire format here.
func TestDBSGOMySQLDiffSpecsDefaultAbsenceOrientation(t *testing.T) {
	empty := ""

	tests := []struct {
		name     string
		a, b     *string
		wantSide validations.DiffSide
	}{
		{name: "source has no default", a: nil, b: &empty, wantSide: validations.SideA},
		{name: "destination has no default", a: &empty, b: nil, wantSide: validations.SideB},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a := specCol(2, "note", "varchar(64)")
			b := specCol(2, "note", "varchar(64)")
			a.Default = test.a
			b.Default = test.b

			pair := pairOf(
				[]validations.ColumnSpec{specCol(1, "id", "bigint"), a},
				[]validations.ColumnSpec{specCol(1, "id", "bigint"), b},
			)
			diffs := validations.DiffSpecs(pair.A, pair.B)
			if len(diffs) != 1 || diffs[0].Kind != validations.ColumnDefaultMismatch {
				t.Fatalf("DiffSpecs returned %+v, want one ColumnDefaultMismatch", diffs)
			}
			if diffs[0].Side != test.wantSide {
				t.Fatalf("ColumnDefaultMismatch.Side = %v, want %v", diffs[0].Side, test.wantSide)
			}
		})
	}
}

// TestDBSGOMySQLDiffSpecsAbsentDefaultsIgnoreExpressionFlag pins the other v0.8.1
// correction: DefaultIsExpression qualifies a supplied default's text and cannot make
// two absent defaults differ.
func TestDBSGOMySQLDiffSpecsAbsentDefaultsIgnoreExpressionFlag(t *testing.T) {
	a := specCol(2, "created_at", "timestamp")
	b := specCol(2, "created_at", "timestamp")
	b.DefaultIsExpression = true

	pair := pairOf(
		[]validations.ColumnSpec{specCol(1, "id", "bigint"), a},
		[]validations.ColumnSpec{specCol(1, "id", "bigint"), b},
	)
	if diffs := validations.DiffSpecs(pair.A, pair.B); len(diffs) != 0 {
		t.Fatalf("DiffSpecs returned %+v, want no differences for two absent defaults", diffs)
	}
}

// TestSchemaCompatDefaultAbsenceOrientationsAllowed proves the GoArchive policy on the
// corrected typed facts. Copy statements provide explicit values, so neither a missing
// default nor an empty-string default makes one side stricter for archival compatibility.
func TestSchemaCompatDefaultAbsenceOrientationsAllowed(t *testing.T) {
	empty := ""

	tests := []struct {
		name string
		a, b *string
	}{
		{name: "source has no default", a: nil, b: &empty},
		{name: "destination has no default", a: &empty, b: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a := specCol(2, "note", "varchar(64)")
			b := specCol(2, "note", "varchar(64)")
			a.Default = test.a
			b.Default = test.b

			verdict, err := evaluateSchemaCompatibility(pairOf(
				[]validations.ColumnSpec{specCol(1, "id", "bigint"), a},
				[]validations.ColumnSpec{specCol(1, "id", "bigint"), b},
			), true)
			if err != nil {
				t.Fatalf("evaluateSchemaCompatibility: %v", err)
			}
			if verdict.Fatal != "" || len(verdict.Warnings) != 0 {
				t.Fatalf("verdict = %+v, want compatible without warnings", verdict)
			}
		})
	}
}
