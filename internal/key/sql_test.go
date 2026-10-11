package key

import (
	"fmt"
	"reflect"
	"testing"
)

func TestPlaceholderPerFamily(t *testing.T) {
	cases := map[string]Column{
		"?":                         {Family: FamilySigned},
		"CAST(? AS DECIMAL(30,10))": {Family: FamilyDecimal, Precision: 30, Scale: 10},
		"CAST(? AS DATE)":           {Family: FamilyDate},
		"CAST(? AS DATETIME(3))":    {Family: FamilyDateTime, Fsp: 3},
		"CAST(? AS DATETIME(6))":    {Family: FamilyTimestamp, Fsp: 6},
		"CAST(? AS YEAR)":           {Family: FamilyYear},
	}
	for want, c := range cases {
		if got := Placeholder(c); got != want {
			t.Errorf("Placeholder(%s) = %q, want %q", c.Family, got, want)
		}
	}
	if got := Placeholder(Column{Family: FamilyString}); got != "?" {
		t.Errorf("Placeholder(string) = %q", got)
	}
	// R46, docs/README_COMPAT.md: TIME binds through CHAR, the one form exact on 8.0, 8.4
	// and 9.7.
	for _, fsp := range []int{0, 2, 6} {
		want := fmt.Sprintf("CAST(CAST(? AS CHAR) AS TIME(%d))", fsp)
		if got := Placeholder(Column{Family: FamilyTime, Fsp: fsp}); got != want {
			t.Errorf("Placeholder(time(%d)) = %q, want %q", fsp, got, want)
		}
	}
}

func TestInSingleAndComposite(t *testing.T) {
	d := testDef()
	if got, want := In([]string{"order_id"}, d.Columns[:1], 3), "`order_id` IN (?,?,?)"; got != want {
		t.Errorf("In(single) = %q, want %q", got, want)
	}
	if got, want := In(d.Names(), d.Columns, 2), "(`order_id`,`line_no`) IN ((?,?),(?,?))"; got != want {
		t.Errorf("In(composite) = %q, want %q", got, want)
	}
	// A child's FK columns bound as the parent's key columns, in references order (spec §7.3).
	dec := []Column{{Family: FamilyDecimal, Precision: 20}, {Family: FamilyDateTime, Fsp: 3}}
	if got, want := In([]string{"parent_b", "parent_a"}, dec, 1), "(`parent_b`,`parent_a`) IN ((CAST(? AS DECIMAL(20,0)),CAST(? AS DATETIME(3))))"; got != want {
		t.Errorf("In(child) = %q, want %q", got, want)
	}
}

func TestAfterExpandsTheDisjunction(t *testing.T) {
	one := Def{Columns: []Column{{Name: "id", Family: FamilySigned}}}
	if got, want := After(one), "(`id` > ?)"; got != want {
		t.Errorf("After(1) = %q, want %q", got, want)
	}
	three := Def{Columns: []Column{{Name: "a", Family: FamilySigned}, {Name: "b", Family: FamilyString}, {Name: "c", Family: FamilyDate}}}
	want := "(`a` > ? OR (`a` = ? AND `b` > ?) OR (`a` = ? AND `b` = ? AND `c` > CAST(? AS DATE)))"
	if got := After(three); got != want {
		t.Errorf("After(3) = %q, want %q", got, want)
	}
	k := Key{int64(1), []byte("x"), "2024-01-01"}
	wantArgs := []any{int64(1), int64(1), []byte("x"), int64(1), []byte("x"), "2024-01-01"}
	if got := AfterArgs(k); !reflect.DeepEqual(got, wantArgs) {
		t.Errorf("AfterArgs = %#v, want %#v", got, wantArgs)
	}
}

func TestProjectionReadsTemporalAsText(t *testing.T) {
	d := Def{Columns: []Column{{Name: "id", Family: FamilySigned}, {Name: "at", Family: FamilyTimestamp, Fsp: 6}}}
	if got, want := Projection(d), "`id`, CAST(`at` AS CHAR)"; got != want {
		t.Errorf("Projection = %q, want %q", got, want)
	}
	if got, want := OrderBy(d), "`id`, `at`"; got != want {
		t.Errorf("OrderBy = %q, want %q", got, want)
	}
}

func TestArgsFlattensInKeyOrder(t *testing.T) {
	got := Args([]Key{{uint64(1), []byte("a")}, {uint64(2), []byte("b")}})
	want := []any{uint64(1), []byte("a"), uint64(2), []byte("b")}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Args = %#v", got)
	}
}

// TestIdentifiersAreQuoted: every key-SQL shape quotes each column and doubles an embedded
// backtick. Pair (M1-D2, R56): a reserved word and a name holding a backtick → quoted, the
// backtick doubled, in In, OrderBy, Projection and After; the same names without the
// backtick → quoted once, nothing doubled.
func TestIdentifiersAreQuoted(t *testing.T) {
	for _, tc := range []struct {
		name, second, quoted string
	}{
		{"a backtick in the name", "we`ird", "`we``ird`"},
		{"no backtick", "weird", "`weird`"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := Def{Columns: []Column{{Name: "order", Family: FamilySigned}, {Name: tc.second, Family: FamilyDate}}}
			q := tc.quoted
			for _, c := range []struct{ shape, got, want string }{
				{"In", In(d.Names(), d.Columns, 1), "(`order`," + q + ") IN ((?," + Placeholder(d.Columns[1]) + "))"},
				{"OrderBy", OrderBy(d), "`order`, " + q},
				{"Projection", Projection(d), "`order`, CAST(" + q + " AS CHAR)"},
				{"After", After(d), "(`order` > ? OR (`order` = ? AND " + q + " > " + Placeholder(d.Columns[1]) + "))"},
			} {
				if c.got != c.want {
					t.Errorf("%s = %q, want %q", c.shape, c.got, c.want)
				}
			}
		})
	}
}
