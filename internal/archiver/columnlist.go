package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/dbsmedya/dbsgomysql/pkg/sqlutil"
	"github.com/dbsmedya/dbsgomysql/pkg/validations"
	"github.com/dbsmedya/goarchive/internal/types"
)

// columnMetadataFromFacts projects library column facts to per-table ordered name
// lists. Every requested table must be present with at least one column —
// anything else fails closed before any data moves.
func columnMetadataFromFacts(tables []string, facts []validations.TableColumns) (map[string]types.ColumnMetadata, error) {
	byTable := make(map[string]types.ColumnMetadata, len(facts))
	for _, fact := range facts {
		meta := types.ColumnMetadata{Names: make([]string, 0, len(fact.Columns)), Temporal: map[string]types.TemporalKind{}}
		for _, col := range fact.Columns {
			if col.Name == "" || col.DataType == "" {
				return nil, &types.TemporalReadContractError{Table: fact.Table, Operation: "metadata", Detail: "incomplete column facts"}
			}
			meta.Names = append(meta.Names, col.Name)
			switch strings.ToLower(col.DataType) {
			case "date":
				meta.Temporal[col.Name] = types.TemporalDate
			case "datetime":
				meta.Temporal[col.Name] = types.TemporalDateTime
			case "timestamp":
				meta.Temporal[col.Name] = types.TemporalTimestamp
			}
		}
		byTable[fact.Table] = meta
	}
	for _, table := range tables {
		if len(byTable[table].Names) == 0 {
			return nil, &types.TemporalReadContractError{Table: table, Operation: "metadata", Detail: "missing column facts"}
		}
	}
	return byTable, nil
}

// sourceColumnMetadata fetches every graph table's full column list once, at run
// startup, via the library inspector. Every column is included — invisible,
// generated, auto-increment. No filtering: the insert list must equal the
// select list, and destination generated columns are already fatal in
// preflight (issue #23 spec).
func sourceColumnMetadata(ctx context.Context, db *sql.DB, schema string, tables []string) (map[string]types.ColumnMetadata, error) {
	facts, err := validations.NewInspector(db, schema).Columns(ctx, tables)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch source column lists: %w", err)
	}
	return columnMetadataFromFacts(tables, facts)
}

// quotedColumnList renders `c1`, `c2`, ... for interpolation into SQL.
func quotedColumnList(columns []string) string {
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = sqlutil.QuoteIdentifier(col)
	}
	return strings.Join(quoted, ", ")
}
