// Package archiver provides the copy phase implementation for GoArchive.
package archiver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/dbsmedya/goarchive/internal/sqlutil"
)

// CopyStats contains statistics about the copy operation.
// GA-P3-F3-T8: Copy stats logging
type CopyStats struct {
	TablesCopied  int           // Number of tables processed
	RowsCopied    int64         // Total rows copied across all tables
	Duration      time.Duration // Time taken for copy operation
	TablesSkipped int           // Tables with no rows to copy
	RowsPerTable  map[string]int64
	InsertErrors  int // Number of rows that failed to insert (should be 0 with INSERT IGNORE)
}

// CopyPhase manages the transactional copy of discovered records from source to destination.
//
// GA-P3-F3-T1: Destination transaction
// GA-P3-F3-T2: FK checks configuration
// GA-P3-F3-T3: Copy root table
// GA-P3-F3-T4: Copy child tables
// GA-P3-F3-T5: Use INSERT IGNORE for idempotency
// GA-P3-F3-T6: Commit transaction
// GA-P3-F3-T7: Rollback on error
// GA-P3-F3-T8: Copy stats logging
type CopyPhase struct {
	sourceDB     *sql.DB
	destDB       *sql.DB
	graph        *graph.Graph
	safetyCfg    config.SafetyConfig
	logger       *logger.Logger
	strictInsert bool
	batchSize    int // fetch+insert chunk size; 0 => defaultCopyBatchSize
}

const defaultCopyBatchSize = 200
const mysqlErrDuplicateEntry = 1062

// ErrDestinationDuplicate is returned when strict INSERT sees a destination duplicate.
type ErrDestinationDuplicate struct {
	Table         string
	ConflictingPK string
	RawMySQLError string
}

func (e *ErrDestinationDuplicate) Error() string {
	return fmt.Sprintf(
		"Archive aborted: destination already contains a row in table %q with primary key %q (MySQL: %s).\n\n"+
			"This is unsafe under verification.method: count, which only checks row counts and cannot prove pre-existing destination rows match the source. Additional conflicting rows may exist beyond the one MySQL reported above.\n\n"+
			"To resolve:\n"+
			"  1. Switch this job to verification.method: sha256 (recommended), OR\n"+
			"  2. Manually remove the conflicting destination rows and re-run.\n\n"+
			"Source data has NOT been deleted.",
		e.Table, e.ConflictingPK, e.RawMySQLError)
}

// NewCopyPhase creates a new copy phase coordinator.
func NewCopyPhase(
	sourceDB *sql.DB,
	destDB *sql.DB,
	g *graph.Graph,
	safetyCfg config.SafetyConfig,
	log *logger.Logger,
) (*CopyPhase, error) {
	if sourceDB == nil {
		return nil, fmt.Errorf("source database is nil")
	}
	if destDB == nil {
		return nil, fmt.Errorf("destination database is nil")
	}
	if g == nil {
		return nil, fmt.Errorf("graph is nil")
	}
	if log == nil {
		log = logger.NewDefault()
	}

	return &CopyPhase{
		sourceDB:  sourceDB,
		destDB:    destDB,
		graph:     g,
		safetyCfg: safetyCfg,
		logger:    log,
	}, nil
}

// SetStrictInsert switches copy to plain INSERT. Used for destructive count verification.
func (cp *CopyPhase) SetStrictInsert(strict bool) {
	cp.strictInsert = strict
}

// SetBatchSize sets the fetch+insert chunk size for the copy phase. Values <= 0
// are ignored. When never set, defaultCopyBatchSize is used.
func (cp *CopyPhase) SetBatchSize(n int) {
	if n > 0 {
		cp.batchSize = n
	}
}

// effectiveBatchSize returns the configured chunk size or the default.
func (cp *CopyPhase) effectiveBatchSize() int {
	if cp.batchSize > 0 {
		return cp.batchSize
	}
	return defaultCopyBatchSize
}

// Copy executes the copy phase for the given discovered record set.
// It copies all tables in dependency order within a single destination transaction.
//
// When DisableForeignKeyChecks is true, the copy runs on a dedicated *sql.Conn
// so the SET FOREIGN_KEY_CHECKS=0 session variable cannot leak back into the
// connection pool. The variable is explicitly reset before the connection is
// returned to the pool, regardless of whether the transaction committed or
// rolled back. SET is not transactional in MySQL, so explicit reset is required.
//
// GA-P3-F3-T1: Uses destination transaction for atomicity
// GA-P3-F3-T6: Commits on success
// GA-P3-F3-T7: Rolls back on error
// GA-P3-F3-T8: Returns copy statistics
func (cp *CopyPhase) Copy(ctx context.Context, recordSet *RecordSet) (*CopyStats, error) {
	startTime := time.Now()

	stats := &CopyStats{
		RowsPerTable: make(map[string]int64),
	}

	// Loud warning when FK checks are disabled. This is an advanced option that
	// can mask referential-integrity bugs in the copy order; operators should
	// see it in every run's log output.
	if cp.safetyCfg.DisableForeignKeyChecks {
		cp.logger.Warn("SAFETY: FOREIGN_KEY_CHECKS is DISABLED for this copy phase. " +
			"Destination inserts will not validate FK constraints. " +
			"Use only when you have verified the copy order and accept the risk.")
	}

	// Checkout a dedicated connection so any session state (FOREIGN_KEY_CHECKS)
	// is contained to this conn and cannot leak back to the pool.
	conn, err := cp.destDB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get destination connection: %w", err)
	}
	fkReset := false
	defer func() {
		// Reset FK checks before returning the connection to the pool, even if
		// the transaction rolled back — SET is not transactional in MySQL.
		if !fkReset && cp.safetyCfg.DisableForeignKeyChecks {
			if _, resetErr := conn.ExecContext(context.Background(),
				"SET FOREIGN_KEY_CHECKS = 1"); resetErr != nil {
				cp.logger.Errorf("Failed to reset FOREIGN_KEY_CHECKS on destination connection: %v", resetErr)
			}
		}
		if closeErr := conn.Close(); closeErr != nil {
			cp.logger.Warnf("Failed to close destination connection: %v", closeErr)
		}
	}()

	// GA-P3-F3-T1: Begin destination transaction on the dedicated connection
	cp.logger.Debug("Starting destination transaction")
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin destination transaction: %w", err)
	}

	// GA-P3-F3-T7: Ensure rollback on error
	defer func() {
		if tx != nil {
			// Transaction not yet committed - rollback
			cp.logger.Warn("Rolling back destination transaction due to error or panic")
			if rbErr := tx.Rollback(); rbErr != nil {
				cp.logger.Errorf("Failed to rollback transaction: %v", rbErr)
			}
		}
	}()

	// GA-P3-F3-T2: Configure foreign key checks on the dedicated connection.
	// Using tx.ExecContext vs conn.ExecContext both land on the same connection,
	// but SET is not rolled back by tx, so location does not matter for safety.
	if err := cp.setForeignKeyChecks(ctx, tx, cp.safetyCfg.DisableForeignKeyChecks); err != nil {
		return nil, fmt.Errorf("failed to configure FK checks: %w", err)
	}

	// Get copy order from dependency graph (parent tables first)
	copyOrder, err := cp.graph.CopyOrder()
	if err != nil {
		return nil, fmt.Errorf("failed to get copy order: %w", err)
	}

	cp.logger.Infof("Starting copy phase for %d tables in dependency order", len(copyOrder))

	// Copy tables in order: root table first, then children
	for _, table := range copyOrder {
		// Check context cancellation
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("copy interrupted: %w", err)
		}

		pks, exists := recordSet.Records[table]
		if !exists || len(pks) == 0 {
			// Table has no records to copy (not discovered or empty)
			cp.logger.Debugf("Skipping table %q (no records to copy)", table)
			stats.TablesSkipped++
			continue
		}

		// GA-P3-F3-T3 and GA-P3-F3-T4: Copy table (root or child)
		rowsCopied, err := cp.copyTable(ctx, tx, table, pks)
		if err != nil {
			return nil, fmt.Errorf("failed to copy table %s: %w", table, err)
		}

		stats.TablesCopied++
		stats.RowsCopied += rowsCopied
		stats.RowsPerTable[table] = rowsCopied

		cp.logger.Debugf("Copied %d rows from table %q", rowsCopied, table)
	}

	// Re-enable FK checks before commit so the reset is part of the same
	// session's linear statement stream and cannot be interleaved with any
	// later use of the connection. Belt-and-suspenders with the defer.
	if cp.safetyCfg.DisableForeignKeyChecks {
		if _, err := tx.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 1"); err != nil {
			return nil, fmt.Errorf("failed to reset FOREIGN_KEY_CHECKS before commit: %w", err)
		}
		fkReset = true
	}

	// GA-P3-F3-T6: Commit transaction on success
	cp.logger.Debug("Committing destination transaction")
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit destination transaction: %w", err)
	}

	// Mark transaction as committed (prevent defer rollback)
	tx = nil

	// GA-P3-F3-T8: Populate final statistics
	stats.Duration = time.Since(startTime)

	cp.logger.Infof("Copy phase complete: %d tables, %d rows, duration: %s",
		stats.TablesCopied,
		stats.RowsCopied,
		stats.Duration,
	)

	return stats, nil
}

// copyTable copies all specified records for one table, in batchSize-sized
// chunks. Each chunk is one SELECT (fetch) followed by one INSERT, all inside
// the caller's single destination transaction tx.
//
// GA-P3-F3-T5: Uses INSERT IGNORE for idempotent inserts (unless strictInsert)
func (cp *CopyPhase) copyTable(ctx context.Context, tx *sql.Tx, table string, pks []interface{}) (int64, error) {
	if len(pks) == 0 {
		return 0, nil
	}

	chunk := cp.effectiveBatchSize()
	var rowsCopied int64

	for start := 0; start < len(pks); start += chunk {
		if err := ctx.Err(); err != nil {
			return rowsCopied, fmt.Errorf("copy interrupted: %w", err)
		}
		end := start + chunk
		if end > len(pks) {
			end = len(pks)
		}
		copied, err := cp.copyChunk(ctx, tx, table, pks[start:end])
		if err != nil {
			return rowsCopied, err
		}
		rowsCopied += copied
	}
	return rowsCopied, nil
}

// copyChunk fetches one chunk of rows from source and inserts them into dest
// within tx as a single INSERT statement.
func (cp *CopyPhase) copyChunk(ctx context.Context, tx *sql.Tx, table string, pks []interface{}) (int64, error) {
	pkColumn := cp.graph.GetPK(table)

	placeholders := make([]string, len(pks))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	selectQuery := fmt.Sprintf(
		"SELECT * FROM %s WHERE %s IN (%s)",
		sqlutil.QuoteIdentifier(table),
		sqlutil.QuoteIdentifier(pkColumn),
		strings.Join(placeholders, ", "),
	)

	rows, err := cp.sourceDB.QueryContext(ctx, selectQuery, pks...)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch rows from source for %s: %w", table, err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			cp.logger.Warnf("Failed to close rows: %v", cerr)
		}
	}()

	columns, err := rows.Columns()
	if err != nil {
		return 0, fmt.Errorf("failed to get columns for %s: %w", table, err)
	}

	batchValues := make([]interface{}, 0, len(columns)*len(pks))
	rowsInBatch := 0
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return 0, fmt.Errorf("failed to scan row for %s: %w", table, err)
		}
		batchValues = append(batchValues, values...)
		rowsInBatch++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("error iterating rows for %s: %w", table, err)
	}
	if rowsInBatch == 0 {
		return 0, nil
	}

	insertQuery := cp.buildInsertIgnoreBatchQuery(table, columns, rowsInBatch)
	if cp.strictInsert {
		insertQuery = cp.buildInsertBatchQuery(table, columns, rowsInBatch)
	}
	result, err := tx.ExecContext(ctx, insertQuery, batchValues...)
	if err != nil {
		if cp.strictInsert {
			var mysqlErr *mysql.MySQLError
			if errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDuplicateEntry {
				return 0, &ErrDestinationDuplicate{
					Table:         table,
					ConflictingPK: extractDuplicatePK(mysqlErr.Message),
					RawMySQLError: mysqlErr.Message,
				}
			}
		}
		return 0, fmt.Errorf("failed to insert batch into %s: %w", table, err)
	}
	affected, _ := result.RowsAffected()
	return affected, nil
}

// buildInsertIgnoreQuery constructs an INSERT IGNORE statement for the given table and columns.
//
// GA-P3-F3-T5: INSERT IGNORE syntax for idempotent inserts
// Example: INSERT IGNORE INTO users (id, name, email) VALUES (?, ?, ?)
func (cp *CopyPhase) buildInsertIgnoreQuery(table string, columns []string) string {
	return cp.buildInsertIgnoreBatchQuery(table, columns, 1)
}

func (cp *CopyPhase) buildInsertIgnoreBatchQuery(table string, columns []string, rowCount int) string {
	// Column list: (`col1`, `col2`, `col3`)
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = sqlutil.QuoteIdentifier(col)
	}
	columnList := strings.Join(quotedColumns, ", ")

	// Placeholders: (?, ?, ?)
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	placeholderList := fmt.Sprintf("(%s)", strings.Join(placeholders, ", "))
	valueTuples := make([]string, rowCount)
	for i := 0; i < rowCount; i++ {
		valueTuples[i] = placeholderList
	}

	// GA-P3-F3-T5: INSERT IGNORE ensures idempotency
	return fmt.Sprintf(
		"INSERT IGNORE INTO %s (%s) VALUES %s",
		sqlutil.QuoteIdentifier(table),
		columnList,
		strings.Join(valueTuples, ", "),
	)
}

func (cp *CopyPhase) buildInsertBatchQuery(table string, columns []string, rowCount int) string {
	query := cp.buildInsertIgnoreBatchQuery(table, columns, rowCount)
	return strings.Replace(query, "INSERT IGNORE INTO", "INSERT INTO", 1)
}

func extractDuplicatePK(mysqlMsg string) string {
	first := strings.IndexByte(mysqlMsg, '\'')
	if first == -1 {
		return mysqlMsg
	}
	last := strings.IndexByte(mysqlMsg[first+1:], '\'')
	if last == -1 {
		return mysqlMsg
	}
	return mysqlMsg[first+1 : first+1+last]
}

// setForeignKeyChecks configures FOREIGN_KEY_CHECKS for the transaction.
//
// GA-P3-F3-T2: FK checks configuration
// Setting to 0 disables FK checks during inserts, allowing out-of-order inserts
// Setting to 1 (default) enforces FK constraints
func (cp *CopyPhase) setForeignKeyChecks(ctx context.Context, tx *sql.Tx, disable bool) error {
	value := 1
	if disable {
		value = 0
		cp.logger.Debug("Disabling FOREIGN_KEY_CHECKS for destination transaction")
	} else {
		cp.logger.Debug("FOREIGN_KEY_CHECKS enabled (default)")
	}

	query := fmt.Sprintf("SET FOREIGN_KEY_CHECKS = %d", value)
	if _, err := tx.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to set FOREIGN_KEY_CHECKS: %w", err)
	}

	return nil
}

// GetGraph returns the dependency graph used by this copy phase.
func (cp *CopyPhase) GetGraph() *graph.Graph {
	return cp.graph
}

// SetLogger sets a custom logger for the copy phase.
func (cp *CopyPhase) SetLogger(log *logger.Logger) {
	cp.logger = log
}
