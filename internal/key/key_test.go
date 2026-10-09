package key

import (
	"testing"
)

func testDef() Def {
	return Def{Table: "order_lines", Columns: []Column{
		{Name: "order_id", Family: FamilyUnsigned},
		{Name: "line_no", Family: FamilyString, Charset: "utf8mb4", Collation: "utf8mb4_bin"},
	}}
}

func TestEncodeIsInjective(t *testing.T) {
	pairs := [][2]Key{
		{{[]byte("ab"), []byte("")}, {[]byte("a"), []byte("b")}},
		{{int64(1)}, {uint64(1)}},
		{{"1.50"}, {[]byte("1.50")}},
		{{[]byte("abc")}, {[]byte("ABC")}},
		{{[]byte("a")}, {[]byte("a ")}},
	}
	for _, p := range pairs {
		if Encode(p[0]) == Encode(p[1]) {
			t.Fatalf("Encode(%v) == Encode(%v)", p[0], p[1])
		}
	}
}

func TestSetKeepsFirstInsertionOrderAndDeduplicates(t *testing.T) {
	s := NewSet()
	a := Key{uint64(42), []byte("A-7")}
	b := Key{uint64(42), []byte("B-1")}
	if !s.Add(a) || !s.Add(b) || s.Add(Key{uint64(42), []byte("A-7")}) {
		t.Fatal("Add returned the wrong novelty")
	}
	if s.Len() != 2 || string(s.Keys()[0][1].([]byte)) != "A-7" || !s.Has(b) {
		t.Fatalf("set = %v", s.Keys())
	}
}

func TestValidateChecksArityAndTypes(t *testing.T) {
	d := testDef()
	if err := d.Validate(Key{uint64(1), []byte("x")}); err != nil {
		t.Fatalf("Validate(valid): %v", err)
	}
	if err := d.Validate(Key{int64(1), []byte("x")}); err == nil {
		t.Fatal("Validate accepted int64 for an unsigned column")
	}
	if err := d.Validate(Key{uint64(1)}); err == nil {
		t.Fatal("Validate accepted a short key")
	}
}

func TestDecodeKeepsExactValues(t *testing.T) {
	d := Def{Table: "t", Columns: []Column{
		{Name: "u", Family: FamilyUnsigned},
		{Name: "d", Family: FamilyDecimal, Precision: 30, Scale: 10},
		{Name: "s", Family: FamilyString},
		{Name: "ts", Family: FamilyTimestamp, Fsp: 6},
	}}
	raw := [][]byte{[]byte("18446744073709551615"), []byte("1.0000000002"), []byte("a "), []byte("2024-03-31 01:00:00.000000")}
	k, err := d.Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	raw[2][0] = 'X' // Decode must have copied the bytes
	if k[0].(uint64) != 18446744073709551615 || k[1].(string) != "1.0000000002" ||
		string(k[2].([]byte)) != "a " || k[3].(string) != "2024-03-31 01:00:00.000000" {
		t.Fatalf("Decode = %#v", k)
	}
	if _, err := d.Decode([][]byte{[]byte("1"), []byte("1"), []byte("a"), []byte("0000-00-00 00:00:00")}); err == nil {
		t.Fatal("Decode accepted a zero temporal identity")
	}
	if _, err := d.Decode([][]byte{nil, []byte("1"), []byte("a"), []byte("2024-01-01 00:00:00")}); err == nil {
		t.Fatal("Decode accepted a NULL key column")
	}
}

func TestDecodeKeepsEmptyKeysNonNil(t *testing.T) {
	d := Def{Table: "t", Columns: []Column{{Name: "s", Family: FamilyString}, {Name: "b", Family: FamilyBinary}}}
	k, err := d.Decode([][]byte{{}, {}})
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	for i, v := range k {
		if b := v.([]byte); b == nil || len(b) != 0 {
			t.Fatalf("value %d = %#v, want a non-nil empty slice (a nil []byte binds as NULL)", i, b)
		}
	}
	if Encode(Key{[]byte{}}) == Encode(Key{[]byte("a")}) {
		t.Fatal("empty key encodes like a non-empty one")
	}
}
