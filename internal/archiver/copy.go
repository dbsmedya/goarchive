// Package archiver provides the copy phase implementation for GoArchive.
package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/dbsmedya/goarchive/internal/sqlutil"
)

// CopyStats contains statistics about the copy operation.
// GA-P3-F3-T8: Copy stats logging
type CopyStats struct {
	TablesCopied   int           // Number of tables processed
	RowsCopied     int64         // Total rows copied across all tables
	Duration       time.Duration // Time taken for copy operation
	TablesSkipped  int           // Tables with no rows to copy
	RowsPerTable   map[string]int64
	InsertErrors   int // Number of rows that failed to insert (should be 0 with INSERT IGNORE)
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
	sourceDB      *sql.DB
	destDB        *sql.DB
	graph         *graph.Graph
	safetyCfg     config.SafetyConfig
	logger        *logger.Logger
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

	return &CopyPhase{
		sourceDB:  sourceDB,
		destDB:    destDB,
		graph:     g,
		safetyCfg: safetyCfg,
		logger:    log,
	}, nil
}

// Copy executes the copy phase for the given discovered record set.
// It copies all tables in dependency order within a single destination transaction.
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

	// GA-P3-F3-T1: Begin destination transaction
	cp.logger.Debug("Starting destination transaction")
	tx, err := cp.destDB.BeginTx(ctx, nil)
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

	// GA-P3-F3-T2: Configure foreign key checks
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

// copyTable copies all specified records from source to destination for a single table.
//
// GA-P3-F3-T5: Uses INSERT IGNORE for idempotent inserts
func (cp *CopyPhase) copyTable(ctx context.Context, tx *sql.Tx, table string, pks []interface{}) (int64, error) {
	if len(pks) == 0 {
		return 0, nil
	}

	// Step 1: Fetch full rows from source database
	rows, columns, err := cp.fetchRows(ctx, table, pks)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch rows from source: %w", err)
	}
	defer rows.Close()

	// Step 2: Prepare INSERT IGNORE statement for destination
	// GA-P3-F3-T5: INSERT IGNORE makes the operation idempotent
	// If a row already exists (duplicate PK), it's silently skipped
	insertQuery := cp.buildInsertIgnoreQuery(table, columns)

	stmt, err := tx.PrepareContext(ctx, insertQuery)
	if err != nil {
		return 0, fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	// Step 3: Insert rows one by one
	var rowsCopied int64
	for rows.Next() {
		// Check context cancellation
		if err := ctx.Err(); err != nil {
			return rowsCopied, fmt.Errorf("copy interrupted: %w", err)
		}

		// Scan row values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return rowsCopied, fmt.Errorf("failed to scan row: %w", err)
		}

		// Insert into destination
		// GA-P3-F3-T5: INSERT IGNORE will skip duplicates without error
		result, err := stmt.ExecContext(ctx, values...)
		if err != nil {
			return rowsCopied, fmt.Errorf("failed to insert row: %w", err)
		}

		// Check if row was actually inserted (0 if duplicate)
		affected, _ := result.RowsAffected()
		rowsCopied += affected
	}

	if err := rows.Err(); err != nil {
		return rowsCopied, fmt.Errorf("error iterating rows: %w", err)
	}

	return rowsCopied, nil
}

// fetchRows retrieves full rows from source database for the given primary keys.
// Returns the result set, column names, and any error.
func (cp *CopyPhase) fetchRows(ctx context.Context, table string, pks []interface{}) (*sql.Rows, []string, error) {
	if len(pks) == 0 {
		return nil, nil, fmt.Errorf("no PKs provided")
	}

	// GA-P3-F3-T9: Get PK column from graph (supports configurable PKs for all tables)
	pkColumn := cp.graph.GetPK(table)

	placeholders := make([]string, len(pks))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf(
		"SELECT * FROM %s WHERE %s IN (%s)",
		sqlutil.QuoteIdentifier(table),
		sqlutil.QuoteIdentifier(pkColumn),
		strings.Join(placeholders, ", "),
	)

	rows, err := cp.sourceDB.QueryContext(ctx, query, pks...)
	if err != nil {
		return nil, nil, fmt.Errorf("query failed: %w", err)
	}

	// Get column names from result set
	columns, err := rows.Columns()
	if err != nil {
		rows.Close()
		return nil, nil, fmt.Errorf("failed to get column names: %w", err)
	}

	return rows, columns, nil
}

// buildInsertIgnoreQuery constructs an INSERT IGNORE statement for the given table and columns.
//
// GA-P3-F3-T5: INSERT IGNORE syntax for idempotent inserts
// Example: INSERT IGNORE INTO users (id, name, email) VALUES (?, ?, ?)
func (cp *CopyPhase) buildInsertIgnoreQuery(table string, columns []string) string {
	// Column list: (col1, col2, col3)
	columnList := strings.Join(columns, ", ")

	// Placeholders: (?, ?, ?)
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	placeholderList := strings.Join(placeholders, ", ")

	// GA-P3-F3-T5: INSERT IGNORE ensures idempotency
	return fmt.Sprintf(
		"INSERT IGNORE INTO %s (%s) VALUES (%s)",
		sqlutil.QuoteIdentifier(table),
		columnList,
		placeholderList,
	)
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
