package jobspec

import (
	"slices"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/key"
)

func tableNames(nodes []*Node) string {
	names := make([]string, len(nodes))
	for i, n := range nodes {
		names[i] = n.Table
	}
	return strings.Join(names, ",")
}

// TestTreeOrders: copy order puts each parent before its children, in config order;
// delete order puts each child before its parent.
func TestTreeOrders(t *testing.T) {
	d, err := parse(t, `
jobs:
  j:
    job_type: archive
    root_table: a
    primary_key: id
    where: "1=1"
    relations:
      - table: b
        primary_key: id
        foreign_key: a_id
        dependency_type: "1-1"
        relations:
          - table: d
            primary_key: id
            foreign_key: b_id
      - table: c
        primary_key: id
        foreign_key: a_id
`)
	if err != nil {
		t.Fatal(err)
	}
	tree := NewTree(d.Jobs["j"])
	if got := tableNames(tree.CopyOrder()); got != "a,b,d,c" {
		t.Errorf("CopyOrder = %s, want a,b,d,c", got)
	}
	if got := tableNames(tree.DeleteOrder()); got != "c,d,b,a" {
		t.Errorf("DeleteOrder = %s, want c,d,b,a", got)
	}
	b, c := tree.Node("b"), tree.Node("c")
	if b.Parent != tree.Root || b.DependencyType != "1-1" || c.DependencyType != "1-N" || tree.Node("d").Parent != b {
		t.Fatalf("edges: b=%+v c=%+v", b, c)
	}
	if !slices.Equal(b.References, []string{"id"}) || tree.Root.ForeignKey != nil || tree.Node("x") != nil {
		t.Fatal("node fields")
	}
	// The tree holds copies: changing the spec afterwards does not change it.
	d.Jobs["j"].Relations[0].ForeignKey[0] = "changed"
	if b.ForeignKey[0] != "a_id" {
		t.Fatal("the tree aliases the spec's column lists")
	}
}

// TestProjectionBindsReferencesOrder is spec §7.3's example: parent key (a, b),
// foreign_key [parent_b, parent_a], references [b, a]. Parent (a=1, b=2) binds as (2, 1).
// Binding the key-ordered (1, 2) would select the children of parent (a=2, b=1).
func TestProjectionBindsReferencesOrder(t *testing.T) {
	d, err := parse(t, `
jobs:
  j:
    job_type: archive
    root_table: parent
    primary_key: [a, b]
    where: "1=1"
    relations:
      - table: child
        primary_key: id
        foreign_key: [parent_b, parent_a]
        references: [b, a]
`)
	if err != nil {
		t.Fatal(err)
	}
	child := NewTree(d.Jobs["j"]).Node("child")
	parentDef := key.Def{Table: "parent", Columns: []key.Column{
		{Name: "a", Family: key.FamilySigned},
		{Name: "b", Family: key.FamilyString, Length: 10, Charset: "utf8mb4", Collation: "utf8mb4_bin"},
	}}
	idx, err := child.Projection(parentDef.Names())
	if err != nil {
		t.Fatal(err)
	}
	got := Project(key.Key{int64(1), []byte("2")}, idx)
	if key.Encode(got) != key.Encode(key.Key{[]byte("2"), int64(1)}) {
		t.Fatalf("Project = %v, want (\"2\", 1)", got)
	}
	bind := ProjectColumns(parentDef.Columns, idx)
	if bind[0].Name != "b" || bind[1].Name != "a" {
		t.Fatalf("ProjectColumns = %v", bind)
	}
	sql := key.In(child.ForeignKey, bind, 1)
	if sql != "(`parent_b`,`parent_a`) IN ((?,?))" {
		t.Fatalf("In = %s", sql)
	}
}

// TestProjectionRefusesAKeyThatDiffersFromTheDocumentedOne: MySQL's key is authoritative;
// a references list that is not a permutation of it fails (preflight refuses it first).
func TestProjectionRefusesAKeyThatDiffersFromTheDocumentedOne(t *testing.T) {
	d, err := parse(t, `
jobs:
  j:
    job_type: archive
    root_table: parent
    primary_key: [a, b]
    where: "1=1"
    relations:
      - table: child
        primary_key: id
        foreign_key: [p_a, p_b]
        references: [a, b]
`)
	if err != nil {
		t.Fatal(err)
	}
	tree := NewTree(d.Jobs["j"])
	child := tree.Node("child")
	for _, real := range [][]string{{"a"}, {"a", "c"}, {"a", "b", "c"}, {"A", "b"}} {
		if _, err := child.Projection(real); err == nil || !strings.Contains(err.Error(), "child references (a, b)") {
			t.Errorf("Projection(%v) = %v", real, err)
		}
	}
	if _, err := tree.Root.Projection([]string{"a", "b"}); err == nil {
		t.Error("the root has no parent to project from")
	}
}
