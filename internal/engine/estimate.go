package engine

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// emptyPlans are the Extra texts of a plan whose table is NULL because MySQL proved the range
// empty while planning (PRC-1, spec rev 3 §3.2). Zero limit cannot occur under the
// sql_select_limit pin; Select tables optimized away needs an engine with an exact count, which
// InnoDB is not, so it is an error like any plan estimateRoots does not understand.
var emptyPlans = map[string]bool{
	"Impossible WHERE": true,
	"Impossible WHERE noticed after reading const tables": true,
	"no matching row in const table":                      true,
}

// estimateRoots reads --progress's total from EXPLAIN FORMAT=TRADITIONAL of rangeCountSQL: the
// optimizer's estimate of the where range's root rows, never a count (PRC-1). The count's FROM
// names the root alone, so every other table in select 1 came from a subquery predicate: one
// read before the root multiplies by its rows × filtered (rows is per combination of the tables
// before it), one read after the root passes at most one match per root row and so counts at
// most 1. A materialized subquery printed with rows NULL contributes its own select's output.
func estimateRoots(plan []ExplainRow, table string) (int64, error) {
	if len(plan) > 0 && plan[0].Table == "" {
		// No table: MySQL decided while planning. Only a proven-empty range is a number.
		if emptyPlans[plan[0].Extra] {
			return 0, nil
		}
		return 0, fmt.Errorf("the plan for %s has no table (Extra %q)", table, plan[0].Extra)
	}
	est, root := 1.0, false
	for _, row := range plan {
		if row.ID != "1" {
			continue
		}
		n, err := planOutput(plan, row)
		if err != nil {
			return 0, err
		}
		if root {
			n = math.Min(1, n)
		}
		est *= n
		if !root && strings.EqualFold(row.Table, table) {
			root = true
		}
	}
	if !root {
		return 0, fmt.Errorf("the plan has no row for %s (select 1: %s)", table, selectOne(plan))
	}
	if est < 0.5 {
		// Only a table-less plan proves the range empty: filtered prints with two decimals, so
		// 0.00 can be a positive selectivity, and a small estimate never rounds to the 0 reserved
		// for a proven-empty range (Fable plan review F2, Codex MR review R1).
		return 1, nil
	}
	return int64(math.Round(est)), nil
}

// planOutput is one plan row's rows × filtered / 100, or, for a materialized subquery printed
// with rows NULL, the output of its select.
func planOutput(plan []ExplainRow, row ExplainRow) (float64, error) {
	if n, ok := strings.CutPrefix(row.Table, "<subquery"); ok && row.Rows == "" {
		id := strings.TrimSuffix(n, ">")
		out, found := 1.0, false
		for _, r := range plan {
			if r.ID != id {
				continue
			}
			f, err := rowFactor(r)
			if err != nil {
				return 0, err
			}
			out *= f
			found = true
		}
		if !found {
			return 0, fmt.Errorf("the plan has no select %s for %s (Extra %q)", id, row.Table, row.Extra)
		}
		return out, nil
	}
	return rowFactor(row)
}

// rowFactor is rows × filtered / 100; an empty filtered is 100.
func rowFactor(row ExplainRow) (float64, error) {
	rows, err := strconv.ParseFloat(row.Rows, 64)
	if err != nil {
		return 0, fmt.Errorf("the plan's rows for %s is %q (Extra %q)", row.Table, row.Rows, row.Extra)
	}
	filtered := 100.0
	if row.Filtered != "" {
		if filtered, err = strconv.ParseFloat(row.Filtered, 64); err != nil {
			return 0, fmt.Errorf("the plan's filtered for %s is %q (Extra %q)", row.Table, row.Filtered, row.Extra)
		}
	}
	return rows * filtered / 100, nil
}

// selectOne names select 1's rows with their Extra, for an error about a plan estimateRoots does
// not understand (Codex MR review R2).
func selectOne(plan []ExplainRow) string {
	var rows []string
	for _, row := range plan {
		if row.ID == "1" {
			rows = append(rows, fmt.Sprintf("%s (Extra %q)", row.Table, row.Extra))
		}
	}
	if len(rows) == 0 {
		return "no rows"
	}
	return strings.Join(rows, ", ")
}
