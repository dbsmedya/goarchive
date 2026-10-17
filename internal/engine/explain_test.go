package engine

import (
	"strings"
	"testing"
)

// TestFetchNotes (FSN-1): a filesort in the fetch's select 1 is the root fetch's own sort,
// whatever table its row names (a subquery's table the optimizer joins first, a materialized
// <subquery2>, a self-reference). A filesort in another select is that subquery's own sort,
// named as such. The plan shapes follow PRC-1's records and the FSN-1 probe.
func TestFetchNotes(t *testing.T) {
	row := func(id, selectType, table, extra string) ExplainRow {
		return ExplainRow{ID: id, SelectType: selectType, Table: table, Extra: extra}
	}
	const own = "The root fetch on rental sorts its matching rows on every batch (Using filesort)."
	const sub = "The root fetch's statement also sorts in its DEPENDENT SUBQUERY step on payment (Using filesort)"
	for _, tc := range []struct {
		name string
		rows []ExplainRow
		want []string // a substring of each note, in order; nil: no notes
	}{
		{"the root's own row sorts", []ExplainRow{row("1", "SIMPLE", "rental", "Using where; Using filesort")}, []string{own}},
		{"a semijoin's table first in select 1 carries the sort", []ExplainRow{
			row("1", "SIMPLE", "customer", "Using where; Using temporary; Using filesort"),
			row("1", "SIMPLE", "rental", "Using where")}, []string{own}},
		{"a materialized subquery first carries the sort", []ExplainRow{
			row("1", "SIMPLE", "<subquery2>", "Using temporary; Using filesort"),
			row("1", "SIMPLE", "rental", "Using where"),
			row("2", "MATERIALIZED", "payment", "Using where")}, []string{own}},
		{"a self-reference, the sort on the first of two rows named like the root", []ExplainRow{
			row("1", "SIMPLE", "rental", "Using temporary; Using filesort"),
			row("1", "SIMPLE", "rental", "Using where; FirstMatch")}, []string{own}},
		{"only a dependent subquery sorts", []ExplainRow{
			row("1", "PRIMARY", "rental", "Using where"),
			row("2", "DEPENDENT SUBQUERY", "payment", "Using where; Using filesort")}, []string{sub}},
		{"both sort: the root's note first", []ExplainRow{
			row("2", "DEPENDENT SUBQUERY", "payment", "Using where; Using filesort"),
			row("1", "PRIMARY", "rental", "Using where; Using filesort")}, []string{own, sub}},
		{"nothing sorts", []ExplainRow{row("1", "SIMPLE", "rental", "Using where")}, nil},
		{"a union result row is never the root's", []ExplainRow{
			row("1", "PRIMARY", "rental", "Using where"),
			row("", "UNION RESULT", "<union2,3>", "Using temporary; Using filesort")},
			[]string{"also sorts in its UNION RESULT step on <union2,3>"}},
	} {
		got := fetchNotes("rental", tc.rows)
		if len(got) != len(tc.want) {
			t.Errorf("%s: %d notes, want %d: %q", tc.name, len(got), len(tc.want), got)
			continue
		}
		for i, w := range tc.want {
			if !strings.Contains(got[i], w) {
				t.Errorf("%s: note %d = %q, want it to contain %q", tc.name, i, got[i], w)
			}
		}
	}
}
