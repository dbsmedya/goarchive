package engine

import (
	"strings"
	"testing"
)

// planRow is one EXPLAIN FORMAT=TRADITIONAL row as estimateRoots reads it.
func planRow(id, table, rows, filtered, extra string) ExplainRow {
	return ExplainRow{ID: id, Table: table, Rows: rows, Filtered: filtered, Extra: extra}
}

// TestEstimateRoots (PRC-1, spec rev 3 §3.2): --progress's total is the optimizer's estimate of
// the count statement's root rows: the product over select 1's rows in plan order, a row after
// the root's capped at 1. The tables follow the plan shapes probed on db1 (Percona 8.4.11, sakila)
// and on MySQL 8.0.40, 8.4, 9.7 and Percona 8.0, 9.7, plus the edge cases spec §3.2 names.
func TestEstimateRoots(t *testing.T) {
	for _, tc := range []struct {
		name string
		plan []ExplainRow
		want int64
	}{
		{"one root row (dependent-subquery dedup)", []ExplainRow{
			planRow("1", "rental", "15420", "100.00", "Using where; Using index"),
			planRow("2", "r2", "25", "100.00", "Using index")}, 15420},
		{"filtered", []ExplainRow{planRow("1", "rental", "15420", "10.00", "Using where")}, 1542},
		{"a table before the root (IN semijoin)", []ExplainRow{
			planRow("1", "customer", "326", "10.00", "Using where"),
			planRow("1", "rental", "25", "100.00", "Using index")}, 815},
		{"a materialized scan before the root (EXISTS)", []ExplainRow{
			planRow("1", "<subquery2>", "", "100.00", "Using where"),
			planRow("1", "rental", "1", "100.00", "Using index"),
			planRow("2", "p", "16500", "33.33", "Using where")}, 5499},
		{"a table after the root (FirstMatch)", []ExplainRow{
			planRow("1", "rental", "1156", "100.00", "Using where; Using index"),
			planRow("1", "p", "1", "33.33", "Using where; FirstMatch(rental)")}, 385},
		{"a table after the root never multiplies", []ExplainRow{
			planRow("1", "rental", "1156", "100.00", "Using where"),
			planRow("1", "p", "4", "100.00", "Using where; FirstMatch(rental)")}, 1156},
		{"a materialized lookup after the root", []ExplainRow{
			planRow("1", "rental", "1156", "100.00", "Using where"),
			planRow("1", "<subquery2>", "1", "100.00", ""),
			planRow("2", "p", "16500", "33.33", "Using where")}, 1156},
		{"a derived table inside a materialized subquery", []ExplainRow{
			planRow("1", "<subquery2>", "", "100.00", ""),
			planRow("1", "rental", "26", "100.00", "Using index"),
			planRow("2", "<derived3>", "50", "100.00", ""),
			planRow("3", "customer", "600", "10.00", "Using where")}, 1300},
		{"the table name in another case", []ExplainRow{planRow("1", "RENTAL", "38", "100.00", "")}, 38},
		{"no filtered column", []ExplainRow{planRow("1", "rental", "38", "", "")}, 38},
		{"rounded at the end", []ExplainRow{planRow("1", "rental", "3", "33.33", "")}, 1},
		{"a positive estimate is never 0", []ExplainRow{planRow("1", "rental", "1", "10.00", "")}, 1},
		// filtered prints with two decimals, so 0.00 can be a positive selectivity: only a
		// table-less plan proves the range empty (Codex MR review R1).
		{"a filtered printed as 0.00 on the root", []ExplainRow{planRow("1", "rental", "1000000", "0.00", "Using where")}, 1},
		{"a filtered printed as 0.00 inside a materialized scan", []ExplainRow{
			planRow("1", "<subquery2>", "", "100.00", "Using where"),
			planRow("1", "rental", "1", "100.00", "Using index"),
			planRow("2", "p", "16500", "0.00", "Using where")}, 1},
		{"a filtered printed as 0.00 after the root", []ExplainRow{
			planRow("1", "rental", "1156", "100.00", "Using where"),
			planRow("1", "p", "1", "0.00", "Using where; FirstMatch(rental)")}, 1},
		{"Impossible WHERE", []ExplainRow{planRow("1", "", "", "", "Impossible WHERE")}, 0},
		{"Impossible WHERE after const tables", []ExplainRow{planRow("1", "", "", "", "Impossible WHERE noticed after reading const tables")}, 0},
		{"no matching row in a const table", []ExplainRow{planRow("1", "", "", "", "no matching row in const table")}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := estimateRoots(tc.plan, "rental")
			if err != nil || got != tc.want {
				t.Errorf("estimateRoots = %d, %v; want %d", got, err, tc.want)
			}
		})
	}

	// A plan estimateRoots does not understand is an error naming the table and the Extra, never
	// a confident 0 (Sonnet F5).
	for _, tc := range []struct {
		name string
		plan []ExplainRow
		says string
	}{
		{"no root row", []ExplainRow{planRow("1", "customer", "10", "100.00", "Using where")}, "no row for rental"},
		{"Select tables optimized away", []ExplainRow{planRow("1", "", "", "", "Select tables optimized away")}, "Select tables optimized away"},
		{"rows that do not parse", []ExplainRow{planRow("1", "rental", "many", "100.00", "")}, "rows"},
		{"a NULL rows on the root", []ExplainRow{planRow("1", "rental", "", "100.00", "")}, "rows"},
		{"a materialized scan with no select", []ExplainRow{
			planRow("1", "<subquery3>", "", "100.00", ""),
			planRow("1", "rental", "1", "100.00", "")}, "<subquery3>"},
		{"an empty plan", nil, "no row for rental"},
		// The error names the rejected shape's Extra (Codex MR review R2).
		{"no root row names the plan's Extra", []ExplainRow{planRow("1", "<derived2>", "50", "100.00", "Using temporary")}, "Using temporary"},
		{"rows that do not parse name the row's Extra", []ExplainRow{planRow("1", "rental", "many", "100.00", "Using filesort")}, "Using filesort"},
		{"a materialized scan with no select names its Extra", []ExplainRow{
			planRow("1", "<subquery3>", "", "100.00", "Start temporary"),
			planRow("1", "rental", "1", "100.00", "")}, "Start temporary"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := estimateRoots(tc.plan, "rental"); err == nil || !strings.Contains(err.Error(), tc.says) {
				t.Errorf("estimateRoots = %d, %v; want an error naming %q", got, err, tc.says)
			}
		})
	}
}
