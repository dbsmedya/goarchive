package preflight

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
)

// checkRelationKeys checks each relation's column pairs (foreign_key[i], references[i])
// against the schema (spec §8.3). Each foreign_key column must exist in the child, spelled
// exactly, and pair with the parent key column it references:
//   - two integer types of the same signedness, of any width;
//   - two DECIMAL types, of any precision and scale;
//   - two character types with the same charset and collation;
//   - BINARY(n) with BINARY(n) of the same length, or VARBINARY with VARBINARY;
//   - the same temporal type, with the same fractional-seconds precision.
//
// references names the parent's key columns: jobspec checked it is a permutation of the
// documented primary_key, and checkPrimaryKeyColumns that the documented key is MySQL's.
// A nullable foreign_key column is allowed: a child row with a NULL in any foreign_key
// column matches no parent key, so it is neither discovered nor held by MySQL's
// constraint check (MATCH SIMPLE).
func checkRelationKeys(ctx context.Context, f *facts) error {
	specs, err := f.sourceSpecs(ctx)
	if err != nil {
		return inspectionError("relation_keys", err)
	}
	var issues []string
	for _, n := range f.tree.CopyOrder() {
		if n.Parent == nil {
			continue
		}
		parent := f.keys[n.Parent.Table]
		for i, fkCol := range n.ForeignKey {
			issues = append(issues, judgePair(n, fkCol, n.References[i], specs[n.Table], parent)...)
		}
	}
	if len(issues) > 0 {
		return &Error{Check: "RELATION_KEY_CHECK",
			Message: "Each foreign_key column must exist in its table and pair with the parent key column it " +
				"references: two integer types of the same signedness, two DECIMAL types, two character " +
				"types with the same charset and collation, BINARY(n) with BINARY(n), VARBINARY with " +
				"VARBINARY, or the same temporal type",
			Tables: issues}
	}
	return nil
}

func judgePair(n *jobspec.Node, fkCol, refCol string, child validations.TableSpec, parent key.Def) []string {
	spec, found := columnSpec(child, fkCol)
	if !found {
		for _, c := range child.Columns {
			if asciiFoldEqual(c.Name, fkCol) {
				return []string{fmt.Sprintf("%s.%s(the column is named %s; write it exactly)", n.Table, fkCol, c.Name)}
			}
		}
		return []string{fmt.Sprintf("%s.%s(no such column)", n.Table, fkCol)}
	}
	var ref key.Column
	for _, c := range parent.Columns {
		if c.Name == refCol {
			ref = c
		}
	}
	if ref.Name == "" {
		return []string{fmt.Sprintf("%s.%s(references %s.%s, which is not in its PRIMARY KEY %s)",
			n.Table, fkCol, n.Parent.Table, refCol, columnList(parent.Names()))}
	}
	col, err := key.ColumnFromSpec(n.Table, spec)
	var ue *key.UnsupportedError
	switch {
	case errors.As(err, &ue):
		return []string{fmt.Sprintf("%s.%s (%s) -> %s.%s (%s): %s", n.Table, fkCol, spec.Type, n.Parent.Table, refCol, ref.Type, ue.Reason)}
	case err != nil:
		return []string{fmt.Sprintf("%s.%s: %v", n.Table, fkCol, err)}
	}
	if reason := pairMismatch(col, ref); reason != "" {
		return []string{fmt.Sprintf("%s.%s (%s) -> %s.%s (%s): %s", n.Table, fkCol, col.Type, n.Parent.Table, refCol, ref.Type, reason)}
	}
	return nil
}

// pairMismatch returns why child column c cannot hold values of parent key column p, or "".
func pairMismatch(c, p key.Column) string {
	integer := func(f key.Family) bool { return f == key.FamilySigned || f == key.FamilyUnsigned }
	switch {
	case integer(c.Family) && integer(p.Family) && c.Family != p.Family:
		return "integer signedness differs"
	case c.Family != p.Family:
		return "the types do not pair"
	}
	switch c.Family {
	case key.FamilyString:
		if c.Charset != p.Charset || c.Collation != p.Collation {
			return fmt.Sprintf("charset and collation differ (%s %s, %s %s)", c.Charset, c.Collation, p.Charset, p.Collation)
		}
	case key.FamilyBinary:
		cb, pb := baseType(c.Type), baseType(p.Type)
		switch {
		case cb != pb:
			return "BINARY and VARBINARY do not pair: BINARY pads with 0x00 and compares the padding"
		case cb == "binary" && c.Length != p.Length:
			return "BINARY lengths differ"
		}
	case key.FamilyDateTime, key.FamilyTimestamp, key.FamilyTime:
		if c.Fsp != p.Fsp {
			return "fractional-seconds precision differs"
		}
	}
	return ""
}

// baseType is a type's name without its arguments: "varbinary(16)" is "varbinary".
func baseType(t string) string {
	base, _, _ := strings.Cut(t, "(")
	return strings.TrimSpace(base)
}

func columnSpec(t validations.TableSpec, name string) (validations.ColumnSpec, bool) {
	for _, c := range t.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return validations.ColumnSpec{}, false
}

// checkForeignKeyIndexes needs, for each relation, an index on the child whose leading
// columns are exactly the foreign_key columns, in any order (spec §8.3): discovery reads
// the child by foreign_key, and without such an index every batch scans the table. A
// declared constraint without its supporting index, which MySQL never allows, is refused
// too.
func checkForeignKeyIndexes(ctx context.Context, f *facts) error {
	specs, err := f.sourceSpecs(ctx)
	if err != nil {
		return inspectionError("fk_index", err)
	}
	var missing []string
	for _, n := range f.tree.CopyOrder() {
		if n.Parent != nil && !hasLeadingIndex(specs[n.Table], n.ForeignKey) {
			missing = append(missing, n.Table+columnList(n.ForeignKey))
		}
	}
	declared, err := f.foreignKeys(ctx, fkOutgoing)
	if err != nil {
		return inspectionError("fk_index", err)
	}
	for _, fd := range validations.CheckFKIndexed(declared.Keys) {
		if fd.Check != validations.IDFKIndexed {
			return unexpectedFinding("fk_index", fd)
		}
		fk, ok := fd.Facts.(validations.ForeignKey)
		if !ok {
			return unexpectedFacts("fk_index", fd, "validations.ForeignKey")
		}
		missing = append(missing, fk.ChildTable+columnList(fk.ChildColumns))
	}
	if len(missing) > 0 {
		return &Error{Check: "FK_INDEX_CHECK",
			Message: "Each foreign_key needs an index on its table whose leading columns are exactly the " +
				"foreign_key columns; without one, discovery scans the whole table on every batch. Add one " +
				"with: CREATE INDEX idx_fk ON <table> (<foreign_key columns>)",
			Tables: missing}
	}
	return nil
}

// hasLeadingIndex reports whether some visible index of t starts with exactly the columns
// cols, in any order, each whole: a prefix or functional part does not count, and neither
// does an INVISIBLE index, which the optimizer does not use.
func hasLeadingIndex(t validations.TableSpec, cols []string) bool {
	for _, idx := range t.Indexes {
		if !idx.Visible || len(idx.Parts) < len(cols) {
			continue
		}
		var lead []string
		for _, p := range idx.Parts[:len(cols)] {
			if p.Column == "" || p.SubPart != 0 {
				break
			}
			lead = append(lead, p.Column)
		}
		if len(lead) == len(cols) && sameSet(lead, cols) {
			return true
		}
	}
	return false
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	x, y := slices.Clone(a), slices.Clone(b)
	slices.Sort(x)
	slices.Sort(y)
	return slices.Equal(x, y)
}

// checkInternalForeignKeys reconciles every foreign key declared between two of the
// job's tables with the configured relations.
//
// INTERNAL_FK_COVERAGE: the constraint must be a relation, child under parent, with the
// same foreign_key columns, and must reference the parent's key. Otherwise the delete
// fails with error 1451, or ON DELETE CASCADE removes child rows that were never copied. A
// child that declares two foreign keys to the same parent cannot meet this, because a
// table appears once in a job (R43). A constraint on a parent's unique key cannot either,
// because references lists the parent's key (R11 defers that to 3.1).
//
// FK_PAIRING_CHECK: when the constraint's child columns are the relation's foreign_key,
// its pairing must equal the configured one, compared as a set of (child column, parent
// column) pairs (spec §8.3). A swapped composite pairing would select another parent's
// children.
//
// A self-referencing constraint (employees.manager_id -> employees.id) is not a relation
// and is skipped, as in 2.x.
func checkInternalForeignKeys(ctx context.Context, f *facts) error {
	within, err := f.foreignKeys(ctx, fkWithin)
	if err != nil {
		return inspectionError("internal_fk_coverage", err)
	}
	var coverage, pairing []string
	for _, fk := range within.Keys {
		if fk.ChildTable == fk.ParentTable {
			continue
		}
		decl := fmt.Sprintf("%s%s -> %s%s (constraint %s)", fk.ChildTable, columnList(fk.ChildColumns),
			fk.ParentTable, columnList(fk.ParentColumns), fk.ConstraintName)
		n := f.tree.Node(fk.ChildTable)
		switch {
		case n == nil || n.Parent == nil || n.Parent.Table != fk.ParentTable:
			coverage = append(coverage, fmt.Sprintf("  - %s [no relation: %s is not configured under %s]",
				decl, fk.ChildTable, fk.ParentTable))
		case !sameSet(n.ForeignKey, fk.ChildColumns):
			coverage = append(coverage, fmt.Sprintf("  - %s [the relation's foreign_key is %s]", decl, columnList(n.ForeignKey)))
		case !sameSet(fk.ParentColumns, f.keys[fk.ParentTable].Names()):
			// references lists the parent's key, so no configuration can pair with this.
			coverage = append(coverage, fmt.Sprintf("  - %s [it references %s, which is not the PRIMARY KEY of %s %s; "+
				"a foreign key to a unique key is not supported in 3.0]",
				decl, columnList(fk.ParentColumns), fk.ParentTable, columnList(f.keys[fk.ParentTable].Names())))
		case !samePairs(n.ForeignKey, n.References, fk.ChildColumns, fk.ParentColumns):
			pairing = append(pairing, fmt.Sprintf("  - %s [configured: %s]", decl, pairs(n.ForeignKey, n.References)))
		}
	}
	if len(coverage) > 0 {
		sort.Strings(coverage)
		return &Error{Check: "INTERNAL_FK_COVERAGE", Message: fmt.Sprintf(
			"Foreign keys between the job's tables must each be a configured relation with the same "+
				"foreign_key columns:\n%s\n\nHint: nest each child under its parent, with foreign_key and "+
				"references matching the constraint.", strings.Join(coverage, "\n"))}
	}
	if len(pairing) > 0 {
		sort.Strings(pairing)
		return &Error{Check: "FK_PAIRING_CHECK", Message: fmt.Sprintf(
			"A relation pairs foreign_key with references differently from its declared foreign key; "+
				"list references in the order that pairs with foreign_key:\n%s", strings.Join(pairing, "\n"))}
	}
	return nil
}

// samePairs compares two pairings as sets of (child, parent) column pairs.
func samePairs(childA, parentA, childB, parentB []string) bool {
	if len(childA) != len(childB) {
		return false
	}
	want := make(map[string]string, len(childA))
	for i, c := range childA {
		want[c] = parentA[i]
	}
	for i, c := range childB {
		if p, ok := want[c]; !ok || p != parentB[i] {
			return false
		}
	}
	return true
}

func pairs(child, parent []string) string {
	out := make([]string, len(child))
	for i := range child {
		out[i] = child[i] + "=" + parent[i]
	}
	return strings.Join(out, ", ")
}
