package key

import (
	"errors"
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
)

func TestColumnFromSpecAllowedTypes(t *testing.T) {
	cases := []struct {
		typ, charset, collation string
		want                    Column
	}{
		{"int", "", "", Column{Family: FamilySigned}},
		{"bigint unsigned", "", "", Column{Family: FamilyUnsigned}},
		{"tinyint(1)", "", "", Column{Family: FamilySigned}},
		{"smallint unsigned zerofill", "", "", Column{Family: FamilyUnsigned}},
		{"decimal(30,10) unsigned", "", "", Column{Family: FamilyDecimal, Precision: 30, Scale: 10}},
		{"char(36)", "utf8mb4", "utf8mb4_0900_ai_ci", Column{Family: FamilyString, Length: 36}},
		{"varchar(255)", "latin1", "latin1_swedish_ci", Column{Family: FamilyString, Length: 255}},
		{"binary(16)", "", "", Column{Family: FamilyBinary, Length: 16}},
		{"varbinary(64)", "", "", Column{Family: FamilyBinary, Length: 64}},
		{"date", "", "", Column{Family: FamilyDate}},
		{"datetime(3)", "", "", Column{Family: FamilyDateTime, Fsp: 3}},
		{"datetime", "", "", Column{Family: FamilyDateTime}},
		{"timestamp(6)", "", "", Column{Family: FamilyTimestamp, Fsp: 6}},
		{"time(2)", "", "", Column{Family: FamilyTime, Fsp: 2}},
		{"year", "", "", Column{Family: FamilyYear}},
	}
	for _, tc := range cases {
		t.Run(tc.typ, func(t *testing.T) {
			got, err := ColumnFromSpec("t", validations.ColumnSpec{Name: "k", Type: tc.typ, NormalizedType: tc.typ,
				Charset: tc.charset, Collation: tc.collation})
			if err != nil {
				t.Fatalf("ColumnFromSpec(%q): %v", tc.typ, err)
			}
			if got.Family != tc.want.Family || got.Precision != tc.want.Precision || got.Scale != tc.want.Scale ||
				got.Fsp != tc.want.Fsp || got.Length != tc.want.Length {
				t.Fatalf("ColumnFromSpec(%q) = %+v, want %+v", tc.typ, got, tc.want)
			}
		})
	}
}

func TestColumnFromSpecRefusedTypes(t *testing.T) {
	for _, typ := range []string{"float", "double", "real", "enum('a','b')", "set('x','y')", "bit(1)", "json", "text", "blob", "geometry", "char(10)"} {
		t.Run(typ, func(t *testing.T) {
			// char(10) without charset/collation stands for a malformed fact and must be refused too.
			_, err := ColumnFromSpec("t", validations.ColumnSpec{Name: "k", Type: typ, NormalizedType: typ})
			var unsupported *UnsupportedError
			if !errors.As(err, &unsupported) {
				t.Fatalf("ColumnFromSpec(%q) error = %v, want *UnsupportedError", typ, err)
			}
		})
	}
}
