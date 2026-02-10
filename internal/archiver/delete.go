// Package archiver provides the delete phase implementation for GoArchive.
package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/dbsmedya/goarchive/internal/sqlutil"
)

// DeleteStats contains statistics about the delete operation.
//
// GA-P4-F2-T5: Delete progress logging
type DeleteStats struct {
	TablesProcessed int           // Number of tables processed
	RowsDeleted     int64         // Total rows deleted across all tables
	Duration        time.Duration // Time taken for delete operation
	TablesSkipped   int           // Tables with no rows to delete
	RowsPerTable    map[string]int64
}

// DeletePhase handles deletion of archived records from the source database.
//
// GA-P4-F2-T1: Delete order processing (reverse topological order)
// GA-P4-F2-T2: Batch delete size handling
// GA-P4-F2-T3: PK-based deletes
// GA-P4-F2-T4: Delete without transaction (auto-commit for safety)
// GA-P4-F2-T5: Delete progress logging
// GA-P4-F2-T6: Idempotent deletes (no error on re-delete)
type DeletePhase struct {
	db        *sql.DB
	graph     *graph.Graph
	batchSize int // GA-P4-F2-T2: Batch delete size
	logger    *logger.Logger
}

// NewDeletePhase creates a new delete phase coordinator.
func NewDeletePhase(db *sql.DB, g *graph.Graph, batchSize int, log *logger.Logger) (*DeletePhase, error) {
	if db == nil {
		return nil, fmt.Errorf("database is nil")
	}
	if g == nil {
		return nil, fmt.Errorf("graph is nil")
	}
	if log == nil {
		log = logger.NewDefault()
	}

	// GA-P4-F2-T2: Default batch size
	if batchSize <= 0 {
		batchSize = 500 // Smaller than copy batch for safety
	}

	return &DeletePhase{
		db:        db,
		graph:     g,
		batchSize: batchSize,
		logger:    log,
	}, nil
}

// Delete executes the delete phase for the given record set.
// It deletes all tables in reverse dependency order (children first, then parents).
//
// GA-P4-F2-T1: Processes tables in reverse topological order
// GA-P4-F2-T4: Uses auto-commit (no transaction) to avoid long locks
// GA-P4-F2-T5: Returns delete statistics
func (dp *DeletePhase) Delete(ctx context.Context, recordSet *RecordSet) (*DeleteStats, error) {
	startTime := time.Now()

	stats := &DeleteStats{
		RowsPerTable: make(map[string]int64),
	}

	// GA-P4-F2-T1: Get delete order (reverse topological - children first)
	deleteOrder, err := dp.graph.DeleteOrder()
	if err != nil {
		return nil, fmt.Errorf("failed to get delete order: %w", err)
	}

	dp.logger.Infof("Starting delete phase for %d tables in reverse dependency order", len(deleteOrder))

	// GA-P4-F2-T1: Delete tables in reverse order (children → parents)
	for _, table := range deleteOrder {
		// Check context cancellation
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("delete interrupted: %w", err)
		}

		pks, exists := recordSet.Records[table]
		if !exists || len(pks) == 0 {
			// Table has no records to delete
			dp.logger.Debugf("Skipping table %q (no records to delete)", table)
			stats.TablesSkipped++
			continue
		}

		// GA-P4-F2-T3: Delete table using primary keys
		rowsDeleted, err := dp.deleteTable(ctx, table, pks)
		if err != nil {
			return nil, fmt.Errorf("failed to delete from table %s: %w", table, err)
		}

		stats.TablesProcessed++
		stats.RowsDeleted += rowsDeleted
		stats.RowsPerTable[table] = rowsDeleted

		// GA-P4-F2-T5: Delete progress logging
		dp.logger.Infof("Deleted %d rows from table %q", rowsDeleted, table)
	}

	// Populate final statistics
	stats.Duration = time.Since(startTime)

	dp.logger.Infof("Delete phase complete: %d tables, %d rows deleted, duration: %s",
		stats.TablesProcessed,
		stats.RowsDeleted,
		stats.Duration,
	)

	return stats, nil
}

// deleteTable deletes all specified records from a single table in batches.
//
// GA-P4-F2-T2: Batch delete size handling
// GA-P4-F2-T3: PK-based deletes
// GA-P4-F2-T4: Delete without transaction (auto-commit for each batch)
// GA-P4-F2-T6: Idempotent deletes (no error if already deleted)
func (dp *DeletePhase) deleteTable(ctx context.Context, table string, pks []interface{}) (int64, error) {
	if len(pks) == 0 {
		return 0, nil
	}

	// GA-P3-F3-T9: Get PK column from graph (supports configurable PKs for all tables)
	pkColumn := dp.graph.GetPK(table)

	var totalDeleted int64

	// GA-P4-F2-T2: Process in batches to avoid large IN clauses
	totalBatches := (len(pks) + dp.batchSize - 1) / dp.batchSize

	for batchNum := 0; batchNum < totalBatches; batchNum++ {
		// Check context cancellation
		if err := ctx.Err(); err != nil {
			return totalDeleted, fmt.Errorf("delete interrupted: %w", err)
		}

		start := batchNum * dp.batchSize
		end := start + dp.batchSize
		if end > len(pks) {
			end = len(pks)
		}
		batchPKs := pks[start:end]

		// GA-P4-F2-T3: Execute PK-based delete
		// GA-P4-F2-T4: No transaction - each DELETE is auto-committed
		rowsDeleted, err := dp.executeDelete(ctx, table, pkColumn, batchPKs)
		if err != nil {
			return totalDeleted, fmt.Errorf("batch %d/%d failed: %w", batchNum+1, totalBatches, err)
		}

		totalDeleted += rowsDeleted

		// GA-P4-F2-T5: Log batch progress
		if totalBatches > 1 {
			dp.logger.Debugf("Deleted %d rows from %s (batch %d/%d)",
				rowsDeleted, table, batchNum+1, totalBatches)
		}
	}

	return totalDeleted, nil
}

// executeDelete executes a single DELETE statement for a batch of PKs.
//
// GA-P4-F2-T3: PK-based delete using IN clause
// GA-P4-F2-T4: Auto-commit (no explicit transaction)
// GA-P4-F2-T6: Idempotent (no error if 0 rows deleted)
func (dp *DeletePhase) executeDelete(ctx context.Context, table, pkColumn string, pks []interface{}) (int64, error) {
	if len(pks) == 0 {
		return 0, nil
	}

	// Build DELETE query with IN clause
	placeholders := make([]string, len(pks))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	// GA-P4-F2-T3: PK-based DELETE
	query := fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)",
		sqlutil.QuoteIdentifier(table),
		sqlutil.QuoteIdentifier(pkColumn),
		strings.Join(placeholders, ","),
	)

	// GA-P4-F2-T4: Execute without transaction (auto-commit)
	result, err := dp.db.ExecContext(ctx, query, pks...)
	if err != nil {
		return 0, fmt.Errorf("delete failed: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	// GA-P4-F2-T6: Idempotent - warn if 0 rows deleted (may have been deleted already)
	if rowsAffected == 0 {
		dp.logger.Debugf("No rows deleted from %s for %d PKs (may have been deleted already)", table, len(pks))
	} else if rowsAffected < int64(len(pks)) {
		dp.logger.Warnf("Partial delete from %s: %d/%d rows deleted", table, rowsAffected, len(pks))
	}

	return rowsAffected, nil
}

// SetBatchSize sets the batch size for delete operations.
//
// GA-P4-F2-T2: Batch delete size configuration
func (dp *DeletePhase) SetBatchSize(size int) {
	if size > 0 {
		dp.batchSize = size
	}
}

// GetBatchSize returns the current batch size.
func (dp *DeletePhase) GetBatchSize() int {
	return dp.batchSize
}

// GetGraph returns the dependency graph used by this delete phase.
func (dp *DeletePhase) GetGraph() *graph.Graph {
	return dp.graph
}

// SetLogger sets a custom logger for the delete phase.
func (dp *DeletePhase) SetLogger(log *logger.Logger) {
	dp.logger = log
}
