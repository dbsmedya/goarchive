package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/dbsmedya/goarchive/internal/graph"
)

func loadRootPKMeta(ctx context.Context, sourceDB *sql.DB, g *graph.Graph) error {
	if sourceDB == nil {
		return fmt.Errorf("source database is nil")
	}
	if g == nil {
		return fmt.Errorf("graph is nil")
	}
	rootTable := g.Root
	rootPK := g.GetPK(rootTable)
	const query = `
		SELECT DATA_TYPE, COLUMN_TYPE
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = ?
		  AND COLUMN_NAME = ?
	`
	var dataType, columnType string
	if err := sourceDB.QueryRowContext(ctx, query, rootTable, rootPK).Scan(&dataType, &columnType); err != nil {
		return fmt.Errorf("loadRootPKMeta: %w", err)
	}
	g.SetRootPKMeta(strings.ToLower(dataType), strings.Contains(strings.ToLower(columnType), "unsigned"))
	return nil
}
