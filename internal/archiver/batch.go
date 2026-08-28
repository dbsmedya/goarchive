// Package archiver provides core archiving functionality for GoArchive.
package archiver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dbsmedya/dbsgomysql/pkg/sqlutil"
)

// RootIDFetcher handles fetching batches of root table primary keys.
// It supports checkpoint-based resumption and respects configurable batch sizes.
//
// GA-P3-F1-T1: Root ID Fetcher
type RootIDFetcher struct {
	db         *sql.DB
	rootTable  string
	pkColumn   string
	criteria   string
	batchSize  int
	checkpoint interface{} // Last processed integer PK value; nil means no lower bound.
}

// NewRootIDFetcher creates a new RootIDFetcher for the specified root table.
//
// Parameters:
//   - db: Source database connection
//   - rootTable: Name of the root table to fetch IDs from
//   - pkColumn: Name of the primary key column
//   - criteria: WHERE clause criteria (can be empty for "all rows")
//   - batchSize: Number of IDs to fetch per batch
//   - checkpoint: Last processed PK value for resumption (nil to start from beginning)
func NewRootIDFetcher(db *sql.DB, rootTable, pkColumn, criteria string, batchSize int, checkpoint interface{}) *RootIDFetcher {
	return &RootIDFetcher{
		db:         db,
		rootTable:  rootTable,
		pkColumn:   pkColumn,
		criteria:   criteria,
		batchSize:  batchSize,
		checkpoint: normalizeCheckpoint(checkpoint),
	}
}

// predicate returns the WHERE body shared by FetchNextBatch and
// CountRemaining so the progress estimate cannot drift from what the fetch
// will select. It reads the fetcher's current checkpoint.
func (f *RootIDFetcher) predicate() (string, []interface{}) {
	where := f.criteria
	if where == "" {
		where = "1=1"
	}
	clause := fmt.Sprintf("(%s)", where)
	var args []interface{}
	if f.checkpoint != nil {
		clause += fmt.Sprintf(" AND %s > ?", sqlutil.QuoteIdentifier(f.pkColumn))
		args = append(args, f.checkpoint)
	}
	return clause, args
}

// FetchNextBatch retrieves the next batch of root IDs matching the criteria.
//
// The query respects the checkpoint by selecting only PKs greater than the last
// processed value, ensuring progress can resume after interruption.
//
// Returns:
//   - []interface{}: Slice of primary key values (empty if no more rows)
//   - error: Database error, if any
//
// GA-P3-F1-T1: Fetches root PKs with checkpoint support
// GA-P3-F1-T2: Respects batch_size configuration
func (f *RootIDFetcher) FetchNextBatch(ctx context.Context) ([]interface{}, error) {
	clause, args := f.predicate()
	query := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s ORDER BY %s ASC LIMIT ?",
		sqlutil.QuoteIdentifier(f.pkColumn),
		sqlutil.QuoteIdentifier(f.rootTable),
		clause,
		sqlutil.QuoteIdentifier(f.pkColumn),
	)
	args = append(args, f.batchSize)

	rows, err := f.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch root IDs from %s: %w", f.rootTable, err)
	}
	defer func() { _ = rows.Close() }()

	var ids []interface{}
	for rows.Next() {
		var id interface{}
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan root ID from %s: %w", f.rootTable, err)
		}

		// MySQL driver returns int64 for integers, []byte for strings/blobs
		// Convert []byte to string for consistency
		if b, ok := id.([]byte); ok {
			id = string(b)
		}

		ids = append(ids, id)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating root IDs from %s: %w", f.rootTable, err)
	}

	return ids, nil
}

// CountRemaining counts the root rows the forward scan would currently
// select. It is a point-in-time estimate used only for --progress display.
func (f *RootIDFetcher) CountRemaining(ctx context.Context) (int64, error) {
	clause, args := f.predicate()
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s",
		sqlutil.QuoteIdentifier(f.rootTable), clause)
	var n int64
	if err := f.db.QueryRowContext(ctx, query, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("failed to count remaining root rows in %s: %w", f.rootTable, err)
	}
	return n, nil
}

// UpdateCheckpoint updates the last processed PK value.
// This should be called after successfully processing a batch to enable resumption.
func (f *RootIDFetcher) UpdateCheckpoint(lastID interface{}) {
	f.checkpoint = normalizeCheckpoint(lastID)
}

func normalizeCheckpoint(checkpoint interface{}) interface{} {
	if checkpoint == "" {
		return nil
	}
	return checkpoint
}
