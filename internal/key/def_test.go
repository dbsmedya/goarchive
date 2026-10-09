package key

import (
	"errors"
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
)

func orderLinesSpec(subPart int) (validations.PKInfo, validations.TableSpec) {
	pk := validations.PKInfo{Table: "order_lines", Kind: validations.PKComposite, Columns: []string{"order_id", "line_no"}}
	spec := validations.TableSpec{
		Table: "order_lines",
		Columns: []validations.ColumnSpec{
			// Table ordinal order differs from key order on purpose: the key order wins.
			{Name: "line_no", Ordinal: 1, Type: "varchar(20)", NormalizedType: "varchar(20)", Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"},
			{Name: "order_id", Ordinal: 2, Type: "bigint unsigned", NormalizedType: "bigint unsigned"},
			{Name: "qty", Ordinal: 3, Type: "int", NormalizedType: "int"},
		},
		Indexes: []validations.IndexSpec{{Name: "PRIMARY", Unique: true, Parts: []validations.IndexPart{
			{Column: "order_id"}, {Column: "line_no", SubPart: subPart},
		}}},
		Captured: validations.SectionIndexes,
	}
	return pk, spec
}

func TestFromFactsCompositeKeyInKeyOrder(t *testing.T) {
	pk, spec := orderLinesSpec(0)
	def, err := FromFacts(pk, spec)
	if err != nil {
		t.Fatalf("FromFacts: %v", err)
	}
	if got := def.Names(); len(got) != 2 || got[0] != "order_id" || got[1] != "line_no" {
		t.Fatalf("Names() = %v, want [order_id line_no]", got)
	}
	if def.Columns[0].Family != FamilyUnsigned || def.Columns[1].Family != FamilyString {
		t.Fatalf("families = %s, %s", def.Columns[0].Family, def.Columns[1].Family)
	}
	if def.Columns[1].Collation != "utf8mb4_0900_ai_ci" {
		t.Fatalf("collation = %q", def.Columns[1].Collation)
	}
}

func TestFromFactsRefusesPrefixKeyPart(t *testing.T) {
	pk, spec := orderLinesSpec(10)
	_, err := FromFacts(pk, spec)
	var unsupported *UnsupportedError
	if !errors.As(err, &unsupported) || unsupported.Column != "line_no" {
		t.Fatalf("error = %v, want *UnsupportedError on line_no", err)
	}
}

func TestFromFactsRefusesMissingPrimaryKey(t *testing.T) {
	_, spec := orderLinesSpec(0)
	_, err := FromFacts(validations.PKInfo{Table: "order_lines", Kind: validations.PKNone}, spec)
	var none *NoPrimaryKeyError
	if !errors.As(err, &none) {
		t.Fatalf("error = %v, want *NoPrimaryKeyError", err)
	}
}

func TestFromFactsRequiresIndexes(t *testing.T) {
	pk, spec := orderLinesSpec(0)
	spec.Captured = 0
	if _, err := FromFacts(pk, spec); err == nil {
		t.Fatal("FromFacts accepted a TableSpec captured without indexes")
	}
}
