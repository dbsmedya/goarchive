package key

import (
	"fmt"
	"strings"

	"github.com/dbsmedya/dbsgomysql/pkg/sqlutil"
)

// Placeholder returns the bound-parameter expression for one value of c (spec §4.2 rule 3).
// Values are always prepared-statement parameters, never inline literals (R22).
func Placeholder(c Column) string {
	switch c.Family {
	case FamilyDecimal:
		return fmt.Sprintf("CAST(? AS DECIMAL(%d,%d))", c.Precision, c.Scale)
	case FamilyDate:
		return "CAST(? AS DATE)"
	case FamilyDateTime, FamilyTimestamp:
		// CAST has no TIMESTAMP target; DATETIME(n) compares exactly under the UTC pin.
		return fmt.Sprintf("CAST(? AS DATETIME(%d))", c.Fsp)
	case FamilyTime:
		// MySQL compatibility issue, docs/README_COMPAT.md ("TIME key values bound as
		// prepared parameters"): CAST(? AS TIME(n)) mis-converts some bound strings on 8.0,
		// 8.4 and 9.7, and a bare ? in a multi-key IN matches nothing on 8.0 and 8.4. The
		// cast through CHAR converts the text as a literal is converted, which is exact on
		// every tested version (R46). Do not simplify it without re-running that page's probe.
		return fmt.Sprintf("CAST(CAST(? AS CHAR) AS TIME(%d))", c.Fsp)
	case FamilyYear:
		return "CAST(? AS YEAR)"
	default:
		return "?"
	}
}

// Projection is the select list that reads d's key columns for Scan: temporal columns are
// read as SQL text, everything else as-is.
func Projection(d Def) string {
	parts := make([]string, len(d.Columns))
	for i, c := range d.Columns {
		q := sqlutil.QuoteIdentifier(c.Name)
		if c.Family.temporal() {
			parts[i] = "CAST(" + q + " AS CHAR)"
		} else {
			parts[i] = q
		}
	}
	return strings.Join(parts, ", ")
}

// OrderBy is the ORDER BY list for d, in key order.
func OrderBy(d Def) string {
	parts := make([]string, len(d.Columns))
	for i, c := range d.Columns {
		parts[i] = sqlutil.QuoteIdentifier(c.Name)
	}
	return strings.Join(parts, ", ")
}

// In renders a membership predicate for n key tuples. cols are the compared columns — the
// key's own, or a child's foreign-key columns in config order — and bind gives the column
// each position is bound as. One column renders `a` IN (?,…); several render
// (`a`,`b`) IN ((?,?),…). It panics on mismatched lengths or n < 1, which are programming
// errors.
func In(cols []string, bind []Column, n int) string {
	if len(cols) == 0 || len(cols) != len(bind) || n < 1 {
		panic(fmt.Sprintf("key.In: %d columns, %d bind columns, %d tuples", len(cols), len(bind), n))
	}
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = sqlutil.QuoteIdentifier(c)
	}
	phs := make([]string, len(bind))
	for i, c := range bind {
		phs[i] = Placeholder(c)
	}
	var b strings.Builder
	if len(cols) == 1 {
		b.WriteString(quoted[0])
		b.WriteString(" IN (")
		for i := 0; i < n; i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(phs[0])
		}
		b.WriteByte(')')
		return b.String()
	}
	tuple := "(" + strings.Join(phs, ",") + ")"
	b.WriteString("(" + strings.Join(quoted, ",") + ") IN (")
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(tuple)
	}
	b.WriteByte(')')
	return b.String()
}

// InDisplay renders In's predicate for a report (dry-run, spec §3.8): the same columns, order
// and bind shape, with label in place of the placeholder list. A shape other than bare
// placeholders (a typed cast) is shown once, after the label, so the report says what each
// tuple is bound as without printing a key value.
func InDisplay(cols []string, bind []Column, label string) string {
	if len(cols) == 0 || len(cols) != len(bind) {
		panic(fmt.Sprintf("key.InDisplay: %d columns, %d bind columns", len(cols), len(bind)))
	}
	quoted := make([]string, len(cols))
	phs := make([]string, len(bind))
	plain := true
	for i := range cols {
		quoted[i] = sqlutil.QuoteIdentifier(cols[i])
		phs[i] = Placeholder(bind[i])
		plain = plain && phs[i] == "?"
	}
	lhs, shape := quoted[0], phs[0]
	if len(cols) > 1 {
		lhs, shape = "("+strings.Join(quoted, ",")+")", "("+strings.Join(phs, ",")+")"
	}
	inner := "<" + label + ">"
	if !plain {
		inner = "<" + label + ", each " + shape + ">"
	}
	return lhs + " IN (" + inner + ")"
}

// Args flattens keys into bound arguments, key by key, column by column.
func Args(keys []Key) []any {
	if len(keys) == 0 {
		return nil
	}
	out := make([]any, 0, len(keys)*len(keys[0]))
	for _, k := range keys {
		out = append(out, k...)
	}
	return out
}

// After renders the "rows after the last key" predicate for d as the expanded disjunction
// (spec §4.3; probe P1 showed (a,b) > (?,?) gets no range access):
//
//	(`a` > ? OR (`a` = ? AND `b` > ?) OR (`a` = ? AND `b` = ? AND `c` > ?))
//
// AfterArgs returns its arguments for a given last key.
func After(d Def) string {
	terms := make([]string, len(d.Columns))
	for i := range d.Columns {
		conds := make([]string, 0, i+1)
		for j := 0; j < i; j++ {
			conds = append(conds, sqlutil.QuoteIdentifier(d.Columns[j].Name)+" = "+Placeholder(d.Columns[j]))
		}
		conds = append(conds, sqlutil.QuoteIdentifier(d.Columns[i].Name)+" > "+Placeholder(d.Columns[i]))
		if len(conds) == 1 {
			terms[i] = conds[0]
		} else {
			terms[i] = "(" + strings.Join(conds, " AND ") + ")"
		}
	}
	return "(" + strings.Join(terms, " OR ") + ")"
}

// AfterArgs returns the arguments of After(d) for last key k, in placeholder order.
func AfterArgs(k Key) []any {
	out := make([]any, 0, len(k)*(len(k)+1)/2)
	for i := range k {
		out = append(out, k[:i+1]...)
	}
	return out
}
