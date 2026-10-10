package engine

import (
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
	"github.com/dbsmedya/goarchive/v3/internal/types"
)

func intCol(name string) key.Column {
	return key.Column{Name: name, Type: "int", Family: key.FamilySigned}
}

func decCol(name string) key.Column {
	return key.Column{Name: name, Type: "decimal(30,10)", Family: key.FamilyDecimal, Precision: 30, Scale: 10}
}

// parentAB is a parent keyed (a INT, b DECIMAL(30,10)).
func parentAB() key.Def { return key.Def{Table: "p", Columns: []key.Column{intCol("a"), decCol("b")}} }

func TestFetchSQL(t *testing.T) {
	d := parentAB()
	first := fetchSQL(d, "created < '2020-01-01' -- old", false)
	want := "SELECT `a`, `b` FROM `p` WHERE (created < '2020-01-01' -- old\n) ORDER BY `a`, `b` LIMIT ?"
	if first != want {
		t.Errorf("first fetch:\n got %q\nwant %q", first, want)
	}
	next := fetchSQL(d, "1=1", true)
	want = "SELECT `a`, `b` FROM `p` WHERE (1=1\n) AND (`a` > ? OR (`a` = ? AND `b` > CAST(? AS DECIMAL(30,10)))) ORDER BY `a`, `b` LIMIT ?"
	if next != want {
		t.Errorf("continued fetch:\n got %q\nwant %q", next, want)
	}
}

// TestDiscoveryProjectsIntoReferencesOrder: spec §7.3's example. foreign_key [pb, pa] pairs
// with references [b, a], so each placeholder is cast like the parent column it receives,
// and parent (a=1, b=2) binds as (2, 1).
func TestDiscoveryProjectsIntoReferencesOrder(t *testing.T) {
	spec := &jobspec.Spec{RootTable: "p", PrimaryKey: jobspec.Columns{"a", "b"}, Relations: []jobspec.Relation{
		{Table: "c", PrimaryKey: jobspec.Columns{"id"}, ForeignKey: jobspec.Columns{"pb", "pa"}, References: jobspec.Columns{"b", "a"}},
	}}
	n := jobspec.NewTree(spec).Node("c")
	parent := parentAB()
	idx, err := n.Projection(parent.Names())
	if err != nil {
		t.Fatal(err)
	}
	child := key.Def{Table: "c", Columns: []key.Column{intCol("id")}}
	got := discoverySQL(child, n, jobspec.ProjectColumns(parent.Columns, idx), 2)
	want := "SELECT `id` FROM `c` WHERE (`pb`,`pa`) IN ((CAST(? AS DECIMAL(30,10)),?),(CAST(? AS DECIMAL(30,10)),?))"
	if got != want {
		t.Errorf("discovery:\n got %q\nwant %q", got, want)
	}
	if k := jobspec.Project(key.Key{int64(1), "2.0000000000"}, idx); k[0] != "2.0000000000" || k[1] != int64(1) {
		t.Errorf("projected parent (1, 2) = %v, want (2, 1)", k)
	}
}

func meta(names ...string) types.ColumnMetadata {
	return types.ColumnMetadata{Names: names, Temporal: map[string]types.TemporalKind{"at": types.TemporalDateTime}}
}

func TestKeyAddressedStatements(t *testing.T) {
	d := parentAB()
	m := meta("a", "b", "at", "note")
	read, err := readSQL(d, m, 1)
	if err != nil {
		t.Fatal(err)
	}
	hashed, err := hashSQL(d, m, 1)
	if err != nil {
		t.Fatal(err)
	}
	in := "(`a`,`b`) IN ((?,CAST(? AS DECIMAL(30,10))))"
	for name, tc := range map[string]struct{ got, want string }{
		// the key projection leads, for the identity check; every column follows, temporal as text
		"copy read": {read, "SELECT `a`, `b`, `a`, `b`, CAST(`at` AS CHAR) AS `at`, `note` FROM `p` WHERE " + in},
		"count":     {countSQL(d, 1), "SELECT COUNT(*) FROM `p` WHERE " + in},
		"sha256":    {hashed, "SELECT `a`, `b`, CAST(`at` AS CHAR) AS `at`, `note` FROM `p` WHERE " + in + " ORDER BY `a`, `b`"},
		"delete":    {deleteSQL(d, 1), "DELETE FROM `p` WHERE " + in},
		"present":   {presentSQL(d, 1), "SELECT `a`, `b` FROM `p` WHERE " + in},
		"insert":    {insertSQL("p", []string{"a", "b"}, 2), "INSERT INTO `p` (`a`, `b`) VALUES (?, ?), (?, ?)"},
	} {
		if tc.got != tc.want {
			t.Errorf("%s:\n got %q\nwant %q", name, tc.got, tc.want)
		}
	}
	if _, err := readSQL(d, types.ColumnMetadata{}, 1); err == nil {
		t.Error("readSQL without column metadata passed")
	}
	if strings.Contains(insertSQL("p", []string{"a"}, 1), "IGNORE") {
		t.Error("INSERT IGNORE does not exist in 3.0")
	}
}
