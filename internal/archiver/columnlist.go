package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/dbsmedya/dbsgomysql/pkg/sqlutil"
	"github.com/dbsmedya/dbsgomysql/pkg/validations"
)

// columnListsFromFacts projects library column facts to per-table ordered name
// lists. Every requested table must be present with at least one column —
// anything else fails closed before any data moves.
func columnListsFromFacts(tables []string, facts []validations.TableColumns) (map[string][]string, error) {
	byTable := make(map[string][]string, len(facts))
	for _, fact := range facts {
		names := make([]string, 0, len(fact.Columns))
		for _, col := range fact.Columns {
			names = append(names, col.Name)
		}
		byTable[fact.Table] = names
	}
	for _, table := range tables {
		if len(byTable[table]) == 0 {
			return nil, fmt.Errorf("no column facts for table %q: cannot build explicit column lists", table)
		}
	}
	return byTable, nil
}

// sourceColumnLists fetches every graph table's full column list once, at run
// startup, via the library inspector. Every column is included — invisible,
// generated, auto-increment. No filtering: the insert list must equal the
// select list, and destination generated columns are already fatal in
// preflight (issue #23 spec).
func sourceColumnLists(ctx context.Context, db *sql.DB, schema string, tables []string) (map[string][]string, error) {
	facts, err := validations.NewInspector(db, schema).Columns(ctx, tables)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch source column lists: %w", err)
	}
	return columnListsFromFacts(tables, facts)
}

// quotedColumnList renders `c1`, `c2`, ... for interpolation into SQL.
func quotedColumnList(columns []string) string {
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = sqlutil.QuoteIdentifier(col)
	}
	return strings.Join(quoted, ", ")
}
