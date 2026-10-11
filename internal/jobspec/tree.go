package jobspec

import (
	"fmt"
	"slices"
	"strings"

	"github.com/dbsmedya/goarchive/v3/internal/key"
)

// Node is one table of a job's tree. Its edge to Parent is the column pairing
// (ForeignKey[i], References[i]): ForeignKey columns are in this table, References columns
// are the parent's key columns (spec §7.3).
type Node struct {
	Table          string
	PrimaryKey     []string // documented; SQL uses the key MySQL reports (R31)
	ForeignKey     []string // nil for the root
	References     []string // nil for the root
	DependencyType string   // "1-1" or "1-N"; documentation, "1-N" when omitted
	Parent         *Node
	Children       []*Node
}

// Tree is a job's tables. Each table appears once (validateRelation), so the relations
// form a tree and no cycle can arise.
type Tree struct {
	Root  *Node
	nodes []*Node // parent-first, depth-first, in config order
}

// NewTree builds the tree of a job returned by Load, whose references are filled in.
func NewTree(s *Spec) *Tree {
	root := &Node{Table: s.RootTable, PrimaryKey: slices.Clone(s.PrimaryKey)}
	t := &Tree{Root: root, nodes: []*Node{root}}
	t.add(root, s.Relations)
	return t
}

func (t *Tree) add(parent *Node, rels []Relation) {
	for _, r := range rels {
		dep := r.DependencyType
		if dep == "" {
			dep = "1-N"
		}
		n := &Node{
			Table:          r.Table,
			PrimaryKey:     slices.Clone(r.PrimaryKey),
			ForeignKey:     slices.Clone(r.ForeignKey),
			References:     slices.Clone(r.References),
			DependencyType: dep,
			Parent:         parent,
		}
		parent.Children = append(parent.Children, n)
		t.nodes = append(t.nodes, n)
		t.add(n, r.Relations)
	}
}

// CopyOrder returns every table with each parent before its children: the order of
// discovery and copy (spec §7.3).
func (t *Tree) CopyOrder() []*Node {
	return slices.Clone(t.nodes)
}

// DeleteOrder returns every table with each child before its parent.
func (t *Tree) DeleteOrder() []*Node {
	out := slices.Clone(t.nodes)
	slices.Reverse(out)
	return out
}

// Node returns the named table's node, or nil.
func (t *Tree) Node(table string) *Node {
	for _, n := range t.nodes {
		if n.Table == table {
			return n
		}
	}
	return nil
}

// Projection maps a parent key tuple onto this node's ForeignKey columns (spec §7.3).
// parentKey is the parent's PRIMARY KEY columns in key order, as MySQL reports them
// (key.Def.Names()). Position i of the result is the index in parentKey of References[i],
// so Project(k, idx)[i] is the parent's value of column References[i], which pairs with
// ForeignKey[i].
//
// It fails when References is not a permutation of parentKey: the documented key and
// MySQL's differ, which preflight refuses first (R31).
func (n *Node) Projection(parentKey []string) ([]int, error) {
	if n.Parent == nil {
		return nil, fmt.Errorf("jobspec: %s is the root table and has no parent", n.Table)
	}
	if len(n.References) != len(parentKey) {
		return nil, fmt.Errorf("jobspec: %s references (%s) but the key of %s is (%s)",
			n.Table, strings.Join(n.References, ", "), n.Parent.Table, strings.Join(parentKey, ", "))
	}
	idx := make([]int, len(n.References))
	used := make([]bool, len(parentKey))
	for i, ref := range n.References {
		j := slices.Index(parentKey, ref)
		if j < 0 || used[j] {
			return nil, fmt.Errorf("jobspec: %s references (%s) but the key of %s is (%s)",
				n.Table, strings.Join(n.References, ", "), n.Parent.Table, strings.Join(parentKey, ", "))
		}
		idx[i], used[j] = j, true
	}
	return idx, nil
}

// Project returns the parent key k reordered by a Projection result.
func Project(k key.Key, idx []int) key.Key {
	out := make(key.Key, len(idx))
	for i, j := range idx {
		out[i] = k[j]
	}
	return out
}

// ProjectColumns reorders the parent key's columns the same way, for key.In's bind
// columns: the placeholder of foreign_key[i] is cast like the parent column it receives.
func ProjectColumns(cols []key.Column, idx []int) []key.Column {
	out := make([]key.Column, len(idx))
	for i, j := range idx {
		out[i] = cols[j]
	}
	return out
}
