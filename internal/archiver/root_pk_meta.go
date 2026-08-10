package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
	"github.com/dbsmedya/goarchive/internal/graph"
)

type rootPKMetaLoader func(context.Context, *sql.DB, string, *graph.Graph) error

// isIntegerRootPKType reports whether dataType (information_schema.COLUMNS.DATA_TYPE,
// e.g. "bigint") is one of GoArchive's supported integer root primary-key types.
//
// This stays goarchive policy over a library fact rather than a library call:
// validations.ColumnInfo carries DataType and Unsigned but no IsInteger, and the
// library's own integer classifier is unexported. loadRootPKMeta is this helper's only
// remaining consumer (phase 018), which is why it lives here rather than in preflight.go.
func isIntegerRootPKType(dataType string) bool {
	switch strings.ToLower(dataType) {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint":
		return true
	default:
		return false
	}
}

// applyRootPKMeta evaluates dbsgomysql's detached column facts and records the
// configured root column's type on the graph. Keeping this policy independent of
// Inspector.Columns lets unit tests exercise GoArchive's exact/case-fold selection,
// supported-type decision, and unsigned propagation without reproducing the
// library's information_schema query.
func applyRootPKMeta(g *graph.Graph, facts []validations.TableColumns) error {
	rootTable := g.Root
	rootPK := g.GetPK(rootTable)

	var columns []validations.ColumnInfo
	for _, fact := range facts {
		if fact.Table == rootTable {
			columns = fact.Columns
			break
		}
	}

	var col *validations.ColumnInfo
	for i := range columns {
		if columns[i].Name == rootPK {
			col = &columns[i]
			break
		}
	}
	if col == nil {
		for i := range columns {
			if asciiFoldEqual(columns[i].Name, rootPK) {
				col = &columns[i]
				break
			}
		}
	}
	if col == nil {
		return fmt.Errorf("loadRootPKMeta: column %q not found in table %q", rootPK, rootTable)
	}

	if !isIntegerRootPKType(col.DataType) {
		return fmt.Errorf("ROOT_PK_TYPE_UNSUPPORTED: root table %q has primary key %q of type %q. GoArchive Community edition only supports integer root primary keys (TINYINT through BIGINT)", rootTable, rootPK, col.DataType)
	}
	g.SetRootPKMeta(strings.ToLower(col.DataType), col.Unsigned)
	return nil
}

// loadRootPKMeta records the root primary key column's MySQL data type and signedness
// for the batch pipeline's checkpoint arithmetic. It runs for commands that skip
// preflight, so it must validate the CONFIGURED column — not the table's actual PRIMARY
// KEY, which nothing here has proven is the same column.
//
// The column is matched in TWO passes: first exact Name == configured primary_key, and
// only if none matches, an ASCII case-fold match. This preserves 1.8's
// `COLUMN_NAME = ?` lookup, which collated case-insensitively
// (utf8mb3_tolower_ci) — a configured "log_id" found the server's "LOG_ID" and the skip
// path accepted it. Normal preflight still rejects that config at position 2 with
// PK_COLUMN_CASE_CHECK; this path only preserves what 1.8 did when the operator opted
// out of that check with --skip-validate-preflight.
func loadRootPKMeta(ctx context.Context, sourceDB *sql.DB, sourceDBName string, g *graph.Graph) error {
	if sourceDB == nil {
		return fmt.Errorf("source database is nil")
	}
	if g == nil {
		return fmt.Errorf("graph is nil")
	}
	if sourceDBName == "" {
		return fmt.Errorf("source database name is required")
	}
	rootTable := g.Root

	inspector := validations.NewInspector(sourceDB, sourceDBName)
	facts, err := inspector.Columns(ctx, []string{rootTable})
	if err != nil {
		return fmt.Errorf("loadRootPKMeta: %w", err)
	}
	return applyRootPKMeta(g, facts)
}
