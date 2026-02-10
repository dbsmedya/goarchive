// Package archiver provides core archiving functionality for GoArchive.
package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/dbsmedya/goarchive/internal/sqlutil"
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
	checkpoint interface{} // Last processed PK value (int64, string, etc.)
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
		checkpoint: checkpoint,
	}
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
	// Build WHERE clause with criteria
	whereClause := f.criteria
	if whereClause == "" {
		whereClause = "1=1"
	}

	// Query format: SELECT pk FROM table WHERE criteria AND pk > checkpoint ORDER BY pk ASC LIMIT batch_size
	// This ensures:
	// 1. Only rows matching criteria are selected
	// 2. Resume from checkpoint (pk > last_processed)
	// 3. Deterministic ordering (pk ASC)
	// 4. Controlled batch size
	query := fmt.Sprintf(
		"SELECT %s FROM %s WHERE (%s) AND %s > ? ORDER BY %s ASC LIMIT ?",
		sqlutil.QuoteIdentifier(f.pkColumn),
		sqlutil.QuoteIdentifier(f.rootTable),
		whereClause,
		sqlutil.QuoteIdentifier(f.pkColumn),
		sqlutil.QuoteIdentifier(f.pkColumn),
	)

	// Use checkpoint value (default to 0 for int types if nil)
	startVal := f.checkpoint
	if startVal == nil {
		startVal = 0
	}

	rows, err := f.db.QueryContext(ctx, query, startVal, f.batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch root IDs from %s: %w", f.rootTable, err)
	}
	defer rows.Close()

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

// UpdateCheckpoint updates the last processed PK value.
// This should be called after successfully processing a batch to enable resumption.
func (f *RootIDFetcher) UpdateCheckpoint(lastID interface{}) {
	f.checkpoint = lastID
}

// GetCheckpoint returns the current checkpoint value.
func (f *RootIDFetcher) GetCheckpoint() interface{} {
	return f.checkpoint
}

// BatchProcessor orchestrates the batch processing loop for archiving.
// It coordinates fetching root IDs, sleeping between batches, logging progress,
// and graceful shutdown.
//
// GA-P3-F1-T2: Batch size handling
// GA-P3-F1-T3: Sleep interval
// GA-P3-F1-T4: Empty batch handling
// GA-P3-F1-T5: Progress logging
// GA-P3-F1-T6: Graceful shutdown
type BatchProcessor struct {
	fetcher        *RootIDFetcher
	processingCfg  config.ProcessingConfig
	logger         *logger.Logger
	jobName        string
	batchCount     int
	totalProcessed int
}

// NewBatchProcessor creates a new BatchProcessor for coordinating batch operations.
//
// Parameters:
//   - fetcher: Configured RootIDFetcher for retrieving batches
//   - processingCfg: Processing configuration (batch size, sleep interval)
//   - logger: Logger for progress reporting
//   - jobName: Name of the job for logging context
func NewBatchProcessor(
	fetcher *RootIDFetcher,
	processingCfg config.ProcessingConfig,
	logger *logger.Logger,
	jobName string,
) *BatchProcessor {
	return &BatchProcessor{
		fetcher:       fetcher,
		processingCfg: processingCfg,
		logger:        logger.WithJob(jobName),
		jobName:       jobName,
	}
}

// ProcessBatch processes a single batch by fetching root IDs and invoking the provided handler.
//
// The handler function receives the batch of IDs and should perform the actual
// archiving work (discovery, copy, verify, delete). This allows the BatchProcessor
// to remain agnostic to the specific archiving logic.
//
// Returns:
//   - bool: true if batch was processed, false if no more rows (empty batch)
//   - error: Any error from fetching or processing
//
// GA-P3-F1-T4: Handles empty batches by returning false
// GA-P3-F1-T5: Logs progress with batch number and count
func (bp *BatchProcessor) ProcessBatch(ctx context.Context, handler func(context.Context, []interface{}) error) (bool, error) {
	// Check context before fetching
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("context cancelled before fetching batch: %w", err)
	}

	// Fetch next batch of root IDs
	rootIDs, err := bp.fetcher.FetchNextBatch(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to fetch batch: %w", err)
	}

	// GA-P3-F1-T4: Empty batch handling - exit gracefully
	if len(rootIDs) == 0 {
		bp.logger.Info("No more rows to process, batch loop complete")
		return false, nil
	}

	bp.batchCount++
	batchLogger := bp.logger.WithBatch(bp.batchCount)

	// GA-P3-F1-T5: Progress logging
	batchLogger.Infof("Processing batch %d with %d root IDs (checkpoint: %v)",
		bp.batchCount, len(rootIDs), bp.fetcher.GetCheckpoint())

	// Invoke handler to process this batch
	if err := handler(ctx, rootIDs); err != nil {
		return false, fmt.Errorf("batch %d processing failed: %w", bp.batchCount, err)
	}

	// Update checkpoint to last ID in batch for resumption
	lastID := rootIDs[len(rootIDs)-1]
	bp.fetcher.UpdateCheckpoint(lastID)
	bp.totalProcessed += len(rootIDs)

	batchLogger.Infof("Batch %d complete: processed %d IDs, total processed: %d",
		bp.batchCount, len(rootIDs), bp.totalProcessed)

	return true, nil
}

// Run executes the batch processing loop until completion or cancellation.
//
// The loop:
// 1. Fetches a batch of root IDs
// 2. Processes the batch via the handler function
// 3. Sleeps for the configured interval
// 4. Repeats until no more rows or context is cancelled
//
// Parameters:
//   - ctx: Context for cancellation (graceful shutdown)
//   - handler: Function to process each batch of root IDs
//
// Returns:
//   - error: Processing error or context cancellation
//
// GA-P3-F1-T3: Sleep interval between batches
// GA-P3-F1-T4: Exits when no more batches
// GA-P3-F1-T5: Logs start and completion
// GA-P3-F1-T6: Graceful shutdown on context cancellation
func (bp *BatchProcessor) Run(ctx context.Context, handler func(context.Context, []interface{}) error) error {
	bp.logger.Infof("Starting batch processing loop for job %q", bp.jobName)

	startTime := time.Now()

	for {
		// GA-P3-F1-T6: Check for graceful shutdown
		select {
		case <-ctx.Done():
			bp.logger.Warnf("Batch processing interrupted: %v (processed %d batches, %d total IDs)",
				ctx.Err(), bp.batchCount, bp.totalProcessed)
			return ctx.Err()
		default:
			// Continue processing
		}

		// Process next batch
		hasMore, err := bp.ProcessBatch(ctx, handler)
		if err != nil {
			return fmt.Errorf("batch processing error: %w", err)
		}

		// GA-P3-F1-T4: Exit when no more batches
		if !hasMore {
			elapsed := time.Since(startTime)
			bp.logger.Infof("Batch processing complete for job %q: %d batches, %d total IDs, duration: %s",
				bp.jobName, bp.batchCount, bp.totalProcessed, elapsed)
			return nil
		}

		// GA-P3-F1-T3: Sleep interval between batches to reduce load
		if bp.processingCfg.SleepSeconds > 0 {
			sleepDuration := time.Duration(bp.processingCfg.SleepSeconds * float64(time.Second))

			bp.logger.Debugf("Sleeping for %v before next batch", sleepDuration)

			// Use context-aware sleep for immediate cancellation
			select {
			case <-ctx.Done():
				bp.logger.Warnf("Batch processing interrupted during sleep: %v", ctx.Err())
				return ctx.Err()
			case <-time.After(sleepDuration):
				// Sleep complete, continue to next batch
			}
		}
	}
}

// GetStats returns current processing statistics.
func (bp *BatchProcessor) GetStats() (batchCount, totalProcessed int) {
	return bp.batchCount, bp.totalProcessed
}
