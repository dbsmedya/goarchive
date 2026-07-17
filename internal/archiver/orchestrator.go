// Package archiver provides the core archive orchestration logic for GoArchive.
package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/dbsmedya/goarchive/internal/types"
	"github.com/dbsmedya/goarchive/internal/verifier"
)

// ArchiveResult contains statistics and status of archive operation.
type ArchiveResult struct {
	JobName            string
	StartedAt          time.Time
	CompletedAt        time.Time
	Duration           time.Duration
	TablesCopied       int
	TablesDeleted      int
	RecordsCopied      int64
	RecordsDeleted     int64
	TablesVerified     int
	RecordsVerified    int64
	VerificationMethod string
	Errors             []error
	Success            bool
}

// CheckpointCallback is called after each root PK is processed for crash recovery.
type CheckpointCallback func(rootPK interface{}, status string) error

// BatchStats holds per-batch processing totals returned by processBatch so the
// caller can aggregate run-level result fields.
type BatchStats struct {
	RootsProcessed  int
	RecordsCopied   int64
	RecordsDeleted  int64
	TablesVerified  int
	RecordsVerified int64
}

// batchMode selects how a batch is recovered/processed.
type batchMode int

const (
	// batchFull: Discover -> Copy -> Verify -> MarkBatchCopied -> Delete -> Complete.
	batchFull batchMode = iota
	// batchDeleteOnly: Discover -> Delete -> Complete (skip Copy/Verify). Used only
	// by replay of 'copied' batches (already copied+verified; source may be partially
	// deleted, so re-verify would be invalid).
	batchDeleteOnly
)

type lagWaiter interface {
	WaitForLag(context.Context) error
}

type lagMonitorFactory func(*sql.DB, config.SafetyConfig, *logger.Logger) (lagWaiter, error)

// ArchiveOrchestrator coordinates the archive operation using the dependency graph
// to determine the correct order for copying and deleting records.
type ArchiveOrchestrator struct {
	config          *config.Config
	jobConfig       *config.JobConfig
	jobName         string
	dbManager       *database.Manager
	graph           *graph.Graph
	logger          *logger.Logger
	copyOrder       []string
	deleteOrder     []string
	initialized     bool
	processingCfg   config.ProcessingConfig   // Effective processing config (job-specific or global)
	verificationCfg config.VerificationConfig // Effective verification config (job-specific or global)
	lagFactory      lagMonitorFactory
	force           bool
	staleAtStartup  bool
	stopCh          <-chan struct{} // cooperative graceful-stop signal (nil = disabled)
}

// NewOrchestrator creates a new archive orchestrator with the given configuration
// and database manager. The orchestrator must be initialized with Initialize()
// before use.
func NewOrchestrator(cfg *config.Config, jobName string, jobCfg *config.JobConfig, dbManager *database.Manager) (*ArchiveOrchestrator, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is nil")
	}
	if jobCfg == nil {
		return nil, fmt.Errorf("job config is nil")
	}
	if dbManager == nil {
		return nil, fmt.Errorf("database manager is nil")
	}

	// Use default logger if none provided
	log := logger.NewDefault()

	// Get effective configs (job-specific or global fallback)
	processingCfg := jobCfg.GetJobProcessing(cfg.Processing)
	verificationCfg := jobCfg.GetJobVerification(cfg.Verification)

	return &ArchiveOrchestrator{
		config:          cfg,
		jobName:         jobName,
		jobConfig:       jobCfg,
		dbManager:       dbManager,
		logger:          log,
		processingCfg:   processingCfg,
		verificationCfg: verificationCfg,
		lagFactory: func(db *sql.DB, safety config.SafetyConfig, log *logger.Logger) (lagWaiter, error) {
			lm, err := NewLagMonitor(db, safety, log)
			if err != nil {
				return nil, err
			}
			lm.channel = cfg.Replica.ReplicationChannel
			return lm, nil
		},
	}, nil
}

// Initialize builds the dependency graph from the job configuration and
// computes the copy and delete orders. It validates that the graph has no cycles.
// This method must be called before Execute().
func (o *ArchiveOrchestrator) Initialize() error {
	if o.initialized {
		return nil
	}

	o.logger.Infow("Initializing archive orchestrator",
		"job", o.jobName,
		"root_table", o.jobConfig.RootTable,
	)

	// Build dependency graph from job configuration
	builder := graph.NewBuilder(o.jobConfig)
	g, err := builder.Build()
	if err != nil {
		return fmt.Errorf("failed to build dependency graph: %w", err)
	}

	o.graph = g

	// Validate graph has no cycles
	if err := o.ValidateGraph(); err != nil {
		return err
	}

	// Compute copy order (parent-first, topological sort)
	copyOrder, err := o.graph.CopyOrder()
	if err != nil {
		return fmt.Errorf("failed to compute copy order: %w", err)
	}
	o.copyOrder = copyOrder

	// Compute delete order (child-first, reverse topological sort)
	deleteOrder, err := o.graph.DeleteOrder()
	if err != nil {
		return fmt.Errorf("failed to compute delete order: %w", err)
	}
	o.deleteOrder = deleteOrder

	o.initialized = true

	o.logger.Infow("Orchestrator initialized successfully",
		"tables", len(o.copyOrder),
		"copy_order", o.copyOrder,
		"delete_order", o.deleteOrder,
	)

	return nil
}

// ValidateGraph checks if the dependency graph contains any cycles.
// Returns an error if a cycle is detected, nil otherwise.
func (o *ArchiveOrchestrator) ValidateGraph() error {
	if o.graph == nil {
		return fmt.Errorf("graph not built")
	}

	if o.graph.HasCycle() {
		cycleInfo := o.graph.DetectIncompleteProcessing()
		if cycleInfo != nil {
			o.logger.Errorw("Cycle detected in dependency graph",
				"total_nodes", cycleInfo.TotalNodes,
				"processed_nodes", cycleInfo.ProcessedNodes,
				"unprocessed_nodes", cycleInfo.UnprocessedNodes,
			)
			return fmt.Errorf("cycle detected in dependency graph: %d nodes in cycle",
				len(cycleInfo.UnprocessedNodes))
		}
		return fmt.Errorf("cycle detected in dependency graph")
	}

	return nil
}

// Execute runs the archive operation. It processes records in batches,
// copying them to the destination and then deleting from the source.
// The checkpoint callback is invoked after each root PK is processed.
func (o *ArchiveOrchestrator) Execute(ctx context.Context, checkpoint CheckpointCallback) (result *ArchiveResult, err error) {
	if !o.initialized {
		return nil, fmt.Errorf("orchestrator not initialized")
	}

	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}

	result = &ArchiveResult{
		JobName:            o.jobName,
		StartedAt:          time.Now(),
		VerificationMethod: o.verificationCfg.EffectiveMethod(),
		Errors:             make([]error, 0),
		Success:            false,
	}
	fail := func(format string, args ...interface{}) (*ArchiveResult, error) {
		err := fmt.Errorf(format, args...)
		result.Errors = append(result.Errors, err)
		return result, err
	}

	o.logger.Infow("Starting archive execution",
		"job", o.jobName,
		"batch_size", o.processingCfg.BatchSize,
		"batch_delete_size", o.processingCfg.BatchDeleteSize,
		"sleep_seconds", o.processingCfg.SleepSeconds,
		"verification_method", o.verificationCfg.EffectiveMethod(),
		"skip_verification", o.verificationCfg.SkipVerification,
	)
	if o.verificationCfg.SkipVerification {
		o.logger.Warn(skipVerificationBanner)
	}

	startup, err := beginJobStartup(ctx, o.dbManager.Destination, o.logger, o.jobName, o.jobConfig.RootTable, JobTypeArchive, "archive", o.force, o.config.Destination.EffectiveJobSchema())
	if err != nil {
		return fail("%w", err)
	}
	defer func() {
		if r := recover(); r != nil {
			startup.cleanup(fmt.Errorf("panic during archive: %v", r))
			panic(r)
		}
		startup.cleanup(err)
	}()
	resumeMgr := startup.resumeMgr
	jobState := startup.jobState
	o.staleAtStartup = startup.staleAtStartup
	ctx = startup.runCtx
	if err := loadRootPKMeta(ctx, o.dbManager.Source, o.graph); err != nil {
		return fail("failed to load root PK metadata: %w", err)
	}

	// Check if resuming
	shouldResume, err := resumeMgr.ShouldResume(ctx, o.jobName)
	if err != nil {
		return fail("failed to check resume: %w", err)
	}

	if shouldResume {
		o.logger.Infow("Resuming interrupted job",
			"job", o.jobName,
			"checkpoint", jobState.LastProcessedRootPKID,
		)
	}

	// Initialize lag monitor if enabled
	var lagMonitor lagWaiter
	if o.config.Replica.Enabled {
		replica := o.dbManager.Replica
		if replica == nil {
			return fail("replication monitoring enabled but no replica connection")
		}
		lagMonitor, err = o.lagFactory(replica, o.config.Safety, o.logger)
		if err != nil {
			return fail("failed to create lag monitor: %w", err)
		}
	}

	// Create component instances
	rootPKColumn := o.graph.GetPK(o.jobConfig.RootTable)
	fetcher := NewRootIDFetcher(
		o.dbManager.Source,
		o.jobConfig.RootTable,
		rootPKColumn,
		o.jobConfig.Where,
		o.processingCfg.BatchSize,
		jobState.LastProcessedRootPKID,
	)

	discovery, err := NewRecordDiscovery(o.graph, o.dbManager.Source, o.processingCfg.BatchSize)
	if err != nil {
		return fail("failed to create record discovery: %w", err)
	}
	discovery.SetLogger(o.logger)

	copyPhase, err := NewCopyPhase(
		o.dbManager.Source,
		o.dbManager.Destination,
		o.graph,
		o.config.Safety,
		o.logger,
	)
	if err != nil {
		return fail("failed to create copy phase: %w", err)
	}
	effectiveVerificationMethod := o.verificationCfg.EffectiveMethod()
	// Decide whether INSERT IGNORE is safe. INSERT IGNORE silently skips a row
	// whose key already exists on the destination; if that skip went undetected
	// the source row could then be deleted without a faithful copy. We force a
	// plain (strict) INSERT — which aborts the copy on any duplicate — whenever
	// the post-copy safety net is weak: count verification, verification skipped
	// (review P0-1), or a destination secondary UNIQUE index (review P1-2).
	destUniqueIdx, err := destinationSecondaryUniqueIndexes(ctx, o.dbManager.Destination,
		o.config.Destination.Database, o.graph.AllNodes())
	if err != nil {
		return fail("failed to inspect destination unique indexes: %w", err)
	}
	strictInsert := shouldUseStrictInsert(effectiveVerificationMethod, o.verificationCfg.SkipVerification, len(destUniqueIdx) > 0)
	if strictInsert && effectiveVerificationMethod != "count" {
		reason := "verification skipped (no post-copy check before delete)"
		if len(destUniqueIdx) > 0 {
			reason = "destination secondary unique index present: " + strings.Join(destUniqueIdx, ", ")
		}
		o.logger.Warnw("Forcing strict INSERT (INSERT IGNORE disabled): a silently-skipped duplicate could be deleted from source without a faithful copy",
			"reason", reason)
	}
	copyPhase.SetStrictInsert(strictInsert)
	copyPhase.SetBatchSize(o.processingCfg.BatchSize)

	dataVerifier, err := verifier.NewVerifier(
		o.dbManager.Source,
		o.dbManager.Destination,
		o.graph,
		verifier.VerificationMethod(effectiveVerificationMethod),
		o.logger,
	)
	if err != nil {
		return fail("failed to create verifier: %w", err)
	}
	dataVerifier.SetChunkSize(o.processingCfg.BatchSize)

	deletePhase, err := NewDeletePhase(
		o.dbManager.Source,
		o.graph,
		o.processingCfg.BatchDeleteSize,
		o.logger,
	)
	if err != nil {
		return fail("failed to create delete phase: %w", err)
	}
	// Throttle deletes (between batch_delete_size chunks) to limit binlog/replication lag.
	deletePhase.SetSleepSeconds(o.processingCfg.DeleteSleepSeconds)

	resumeMgr.SetChunkSize(o.processingCfg.BatchSize)

	if shouldResume {
		if err := o.resumePending(ctx, resumeMgr,
			discovery, copyPhase, dataVerifier, deletePhase, fetcher, lagMonitor, checkpoint, result); err != nil {
			return fail("resume failed: %w", err)
		}
	}

	// Batch processing loop
	batchNum := 0
	totalProcessed := int64(0)

	for {
		select {
		case <-ctx.Done():
			o.logger.Warn("Context cancelled - stopping after current batch")
			return fail("%w", ctx.Err())
		default:
		}

		// Cooperative graceful stop (first Ctrl-C): the previous batch has already
		// completed (copy+verify+delete+CompleteBatch) before we reach here, so we
		// stop at this boundary leaving NO non-terminal rows behind.
		if stopRequested(o.stopCh) {
			o.logger.Warn("Graceful stop requested - stopping at batch boundary (run again to resume)")
			break
		}

		// Fetch next batch of root IDs
		rootIDs, err := fetcher.FetchNextBatch(ctx)
		if err != nil {
			return fail("failed to fetch root IDs: %w", err)
		}

		// Empty batch = job complete
		if len(rootIDs) == 0 {
			o.logger.Info("No more root IDs to process - job complete")
			break
		}

		// Operator pause switch: pause at the batch boundary BEFORE logging any
		// pending entries, so a paused job leaves NO rows in 'pending'. By the time
		// we reach here the previous batch has fully completed (copy+verify+delete+
		// CompleteBatch). A finished job exits via the empty-fetch check above
		// rather than pausing forever.
		if err := newSentinelGate(o.processingCfg.SentinelFile, o.logger).wait(ctx, o.stopCh); err != nil {
			return fail("%w", err)
		}
		// A Ctrl-C during the sentinel pause ends the pause (wait returns nil);
		// honor it here before starting the next batch.
		if stopRequested(o.stopCh) {
			o.logger.Warn("Graceful stop requested - stopping at batch boundary (run again to resume)")
			break
		}

		batchNum++
		o.logger.Infow("Processing batch",
			"batch", batchNum,
			"root_ids", len(rootIDs),
		)

		// Log all batch PKs as pending before per-PK processing for crash recovery.
		if err := resumeMgr.LogBatchPending(ctx, o.jobName, rootIDs); err != nil {
			return fail("failed to log pending batch entries: %w", err)
		}

		if lagMonitor != nil {
			if err := lagMonitor.WaitForLag(ctx); err != nil {
				return fail("lag monitor error: %w", err)
			}
		}
		batchStats, err := o.processBatch(ctx, rootIDs, batchFull, true /* advanceCheckpoint */, checkpoint,
			discovery, copyPhase, dataVerifier, deletePhase, fetcher, resumeMgr, lagMonitor)
		if err != nil {
			return fail("processBatch failed: %w", err)
		}
		result.RecordsCopied += batchStats.RecordsCopied
		result.RecordsDeleted += batchStats.RecordsDeleted
		result.TablesVerified += batchStats.TablesVerified
		result.RecordsVerified += batchStats.RecordsVerified
		totalProcessed += int64(batchStats.RootsProcessed)

		// Sleep between batches (skipped early on a cooperative stop; the loop-top
		// stopRequested check then breaks).
		if o.processingCfg.SleepSeconds > 0 {
			sleepDuration := time.Duration(o.processingCfg.SleepSeconds * float64(time.Second))
			if err := interruptibleSleep(ctx, o.stopCh, sleepDuration); err != nil {
				o.logger.Warn("Context cancelled during batch sleep")
				return fail("%w", err)
			}
		}
	}

	// Finalize result
	result.Success = len(result.Errors) == 0
	result.CompletedAt = time.Now()
	result.Duration = result.CompletedAt.Sub(result.StartedAt)
	result.TablesCopied = len(o.copyOrder)
	result.TablesDeleted = len(o.deleteOrder)

	o.logger.Infow("Archive execution completed",
		"duration", result.Duration,
		"success", result.Success,
		"records_copied", result.RecordsCopied,
		"records_deleted", result.RecordsDeleted,
		"tables_verified", result.TablesVerified,
		"records_verified", result.RecordsVerified,
	)

	return result, nil
}

// processBatch runs a whole batch of root PKs through the pipeline, then performs
// the atomic T3 bookkeeping (CompleteBatch). In batchFull it also records the
// durable 'copied' marker after a successful copy+verify (MarkBatchCopied).
// advanceCheckpoint advances the checkpoint to the numeric max PK (main loop
// only; both replay paths pass false). The checkpoint callback, when non-nil, is
// invoked once per root with "completed" after T3 commits.
//
// On any error, the batch's PKs are left in their current non-terminal status
// (pending or copied) — NEVER MarkFailed — so status-aware replay recovers them.
func (o *ArchiveOrchestrator) processBatch(
	ctx context.Context,
	rootIDs []interface{},
	mode batchMode,
	advanceCheckpoint bool,
	checkpoint CheckpointCallback,
	discovery *RecordDiscovery,
	copyPhase *CopyPhase,
	dataVerifier *verifier.Verifier,
	deletePhase *DeletePhase,
	fetcher *RootIDFetcher,
	resumeMgr *ResumeManager,
	lagMonitor lagWaiter,
) (*BatchStats, error) {
	stats := &BatchStats{}
	if len(rootIDs) == 0 {
		return stats, nil
	}

	discovered, err := discovery.Discover(ctx, rootIDs)
	if err != nil {
		return stats, fmt.Errorf("discovery failed: %w", err)
	}
	recordSet := convertRecordSet(discovered)

	if mode == batchFull {
		copyStats, err := copyPhase.Copy(ctx, recordSet)
		if err != nil {
			return stats, fmt.Errorf("copy failed: %w", err)
		}
		stats.RecordsCopied = copyStats.RowsCopied

		if !o.verificationCfg.SkipVerification {
			verifyStats, err := dataVerifier.Verify(ctx, discovered)
			if err != nil {
				return stats, fmt.Errorf("verification failed: %w", err)
			}
			if verifyStats != nil {
				stats.TablesVerified += verifyStats.TablesVerified
				stats.RecordsVerified += verifyStats.TotalRows
			}
		}

		// T1.5: durable "copy+verify succeeded, safe to delete" marker.
		if err := resumeMgr.MarkBatchCopied(ctx, o.jobName, rootIDs); err != nil {
			return stats, fmt.Errorf("mark batch copied failed: %w", err)
		}
	}

	// Re-check replication lag immediately before the binlog-heavy delete phase
	// (issue #2). The pre-batch check above can be stale by now: copy+verify may
	// have taken many seconds, during which lag can climb back above threshold.
	// Fires in BOTH batchFull and batchDeleteOnly (delete-only replay) modes.
	if lagMonitor != nil {
		if err := lagMonitor.WaitForLag(ctx); err != nil {
			return stats, fmt.Errorf("lag monitor error before delete: %w", err)
		}
	}

	deleteStats, err := deletePhase.Delete(ctx, recordSet)
	if err != nil {
		return stats, fmt.Errorf("delete failed: %w", err)
	}
	stats.RecordsDeleted = deleteStats.RowsDeleted

	// T3: atomic completion (+ optional checkpoint). rootIDs come from a numeric
	// ORDER BY pkColumn ASC on the main loop, so the last element is the max PK.
	var checkpointPK interface{}
	if advanceCheckpoint {
		checkpointPK = rootIDs[len(rootIDs)-1]
	}
	if err := resumeMgr.CompleteBatch(ctx, o.jobName, rootIDs, checkpointPK); err != nil {
		return stats, fmt.Errorf("batch completion bookkeeping failed: %w", err)
	}
	if advanceCheckpoint {
		fetcher.UpdateCheckpoint(checkpointPK)
	}

	if checkpoint != nil {
		for _, rootID := range rootIDs {
			if err := checkpoint(rootID, "completed"); err != nil {
				o.logger.Warnw("Checkpoint callback failed", "error", err)
			}
		}
	}

	stats.RootsProcessed = len(rootIDs)
	return stats, nil
}

// resumePending recovers any non-terminal batches left by a prior run, in the
// correct order: 'copied' (delete-only) first, then 'pending' (full pipeline).
func (o *ArchiveOrchestrator) resumePending(
	ctx context.Context,
	resumeMgr *ResumeManager,
	discovery *RecordDiscovery,
	copyPhase *CopyPhase,
	dataVerifier *verifier.Verifier,
	deletePhase *DeletePhase,
	fetcher *RootIDFetcher,
	lagMonitor lagWaiter,
	checkpoint CheckpointCallback,
	result *ArchiveResult,
) error {
	copied, err := resumeMgr.GetRootPKsByStatus(ctx, o.jobName, LogStatusCopied)
	if err != nil {
		return fmt.Errorf("failed to get copied PKs: %w", err)
	}
	pending, err := resumeMgr.GetPendingPKs(ctx, o.jobName)
	if err != nil {
		return fmt.Errorf("failed to get pending PKs: %w", err)
	}
	if len(copied) == 0 && len(pending) == 0 {
		return nil
	}

	// count-mode cannot safely re-derive ANY non-terminal rows.
	if o.verificationCfg.EffectiveMethod() == "count" {
		total := len(copied) + len(pending)
		preview := append(append([]string{}, copied...), pending...)
		if len(preview) > 10 {
			preview = preview[:10]
		}
		return fmt.Errorf(
			"job %q has %d non-terminal root PKs (copied/pending) from a prior interrupted run, and is configured with verification.method: count.\n\n"+
				"Resuming a count-mode job is unsafe - pre-existing destination rows cannot be verified equal to source.\n\n"+
				"To recover, choose one:\n"+
				"  1. Switch this job to verification.method: sha256 in config and re-run (recommended).\n"+
				"  2. Manually inspect destination rows for these PKs, delete any that don't match source, then clear the entries:\n"+
				"       UPDATE %s SET log_status=2 WHERE log_status IN (0,1);\n"+
				"     and re-run.\n\n"+
				"PKs (first 10): %v",
			o.jobName, total, resumeMgr.LogTableName(), preview)
	}

	// Strict INSERT (forced by --skip-verify or a destination secondary unique
	// index — review P0-1/P1-2/003) cannot re-copy 'pending' rows: a crash between
	// the copy commit and MarkBatchCopied leaves those rows already committed on
	// the destination, so a strict re-INSERT aborts on duplicate and the job would
	// self-block on every resume. 'copied' rows are safe (delete-only, no re-copy),
	// so we refuse only when there are pending rows and let a copied-only resume
	// proceed on the next run once the operator clears the pending entries.
	if copyPhase.StrictInsert() && len(pending) > 0 {
		preview := pending
		if len(preview) > 10 {
			preview = preview[:10]
		}
		return fmt.Errorf(
			"job %q has %d 'pending' root PKs from a prior interrupted run and uses strict INSERT "+
				"(forced by --skip-verify or a destination secondary unique index), so they cannot be "+
				"safely re-copied (their destination rows may already be committed, and a strict INSERT "+
				"aborts on duplicate).\n\n"+
				"To recover, choose one:\n"+
				"  1. Delete the destination rows already written for these pending PKs, then re-run "+
				"(the strict re-copy then inserts cleanly).\n"+
				"  2. If you have confirmed the destination rows match source, mark them copied so they "+
				"resume as delete-only:\n"+
				"       UPDATE %s SET log_status=1 WHERE log_status=0;\n"+
				"     and re-run.\n\n"+
				"Pending PKs (first 10): %v",
			o.jobName, len(pending), resumeMgr.LogTableName(), preview)
	}

	// Phase A: finish copied batches (already verified; delete-only).
	if err := o.recoverChunks(ctx, copied, batchDeleteOnly, resumeMgr,
		discovery, copyPhase, dataVerifier, deletePhase, fetcher, lagMonitor, checkpoint, result); err != nil {
		return fmt.Errorf("copied recovery failed: %w", err)
	}
	// Phase B: finish pending batches (source intact; full pipeline).
	if err := o.recoverChunks(ctx, pending, batchFull, resumeMgr,
		discovery, copyPhase, dataVerifier, deletePhase, fetcher, lagMonitor, checkpoint, result); err != nil {
		return fmt.Errorf("pending recovery failed: %w", err)
	}
	return nil
}

// recoverChunks numerically sorts a status set, chunks it by batch_size, and
// runs each chunk through processBatch in the given mode (advanceCheckpoint=false).
func (o *ArchiveOrchestrator) recoverChunks(
	ctx context.Context,
	rawPKs []string,
	mode batchMode,
	resumeMgr *ResumeManager,
	discovery *RecordDiscovery,
	copyPhase *CopyPhase,
	dataVerifier *verifier.Verifier,
	deletePhase *DeletePhase,
	fetcher *RootIDFetcher,
	lagMonitor lagWaiter,
	checkpoint CheckpointCallback,
	result *ArchiveResult,
) error {
	if len(rawPKs) == 0 {
		return nil
	}
	dataType, unsigned, ok := o.graph.GetRootPKMeta()
	if !ok {
		return fmt.Errorf("root PK metadata not loaded")
	}
	sortPendingPKsNumeric(rawPKs, unsigned)
	o.logger.Infow("Recovering non-terminal PKs from prior run",
		"job", o.jobName, "count", len(rawPKs), "mode", mode)

	batchSize := o.processingCfg.BatchSize
	if batchSize <= 0 {
		batchSize = 1
	}
	for start := 0; start < len(rawPKs); start += batchSize {
		// Cooperative graceful stop: each started recovery chunk runs to completion
		// (processBatch is terminal), so stopping at this boundary leaves earlier
		// chunks recovered and the rest in their prior-run status — safe to resume.
		if stopRequested(o.stopCh) {
			o.logger.Warn("Graceful stop requested - stopping recovery at chunk boundary (run again to resume)")
			return nil
		}
		end := start + batchSize
		if end > len(rawPKs) {
			end = len(rawPKs)
		}
		typed := make([]interface{}, 0, end-start)
		for _, raw := range rawPKs[start:end] {
			pk, err := types.ConvertRootPK(raw, dataType, unsigned)
			if err != nil {
				return fmt.Errorf("convert PK %q: %w", raw, err)
			}
			typed = append(typed, pk)
		}
		// Operator pause switch: pause before processing the next recovery chunk so
		// each started chunk runs to completion first. Rows from earlier chunks not
		// yet recovered remain in their prior-run status during the pause — that is
		// pre-existing state, not created by the pause.
		if err := newSentinelGate(o.processingCfg.SentinelFile, o.logger).wait(ctx, o.stopCh); err != nil {
			return err
		}
		if stopRequested(o.stopCh) {
			o.logger.Warn("Graceful stop requested - stopping recovery at chunk boundary (run again to resume)")
			return nil
		}
		if lagMonitor != nil {
			if err := lagMonitor.WaitForLag(ctx); err != nil {
				return fmt.Errorf("lag monitor error: %w", err)
			}
		}
		batchStats, err := o.processBatch(ctx, typed, mode, false /* advanceCheckpoint */, checkpoint,
			discovery, copyPhase, dataVerifier, deletePhase, fetcher, resumeMgr, lagMonitor)
		if err != nil {
			return fmt.Errorf("recovery processBatch failed: %w", err)
		}
		result.RecordsCopied += batchStats.RecordsCopied
		result.RecordsDeleted += batchStats.RecordsDeleted
		result.TablesVerified += batchStats.TablesVerified
		result.RecordsVerified += batchStats.RecordsVerified
	}
	return nil
}

// sortPendingPKsNumeric sorts string PKs by their numeric value in place.
func sortPendingPKsNumeric(pending []string, unsigned bool) {
	sort.Slice(pending, func(i, j int) bool {
		if unsigned {
			a, _ := strconv.ParseUint(pending[i], 10, 64)
			b, _ := strconv.ParseUint(pending[j], 10, 64)
			return a < b
		}
		a, _ := strconv.ParseInt(pending[i], 10, 64)
		b, _ := strconv.ParseInt(pending[j], 10, 64)
		return a < b
	})
}

// pendingReplayPKs returns the job's 'pending' root PKs in numeric replay
// order, plus the root-PK metadata needed to convert them back to driver
// values. The log table stores PKs as VARCHAR and GetRootPKsByStatus is
// unordered; without the numeric sort "10" would replay before "9". Both
// copy-only and purge replay consume this helper (issue #9); the archive
// orchestrator's recovery path sorts equivalently in recoverChunks.
func pendingReplayPKs(ctx context.Context, resumeMgr *ResumeManager, jobName string, g *graph.Graph) ([]string, string, bool, error) {
	pending, err := resumeMgr.GetPendingPKs(ctx, jobName)
	if err != nil {
		return nil, "", false, err
	}
	if len(pending) == 0 {
		return nil, "", false, nil
	}
	dataType, unsigned, ok := g.GetRootPKMeta()
	if !ok {
		return nil, "", false, fmt.Errorf("root PK metadata not loaded")
	}
	sortPendingPKsNumeric(pending, unsigned)
	return pending, dataType, unsigned, nil
}

// SetForce controls heartbeat-aware advisory lock bypass.
func (o *ArchiveOrchestrator) SetForce(force bool) {
	o.force = force
}

// SetStopChannel wires the cooperative graceful-stop signal. When the channel
// closes (first Ctrl-C), the batch loop finishes the in-flight batch and stops at
// the next boundary. A nil channel disables cooperative stop.
func (o *ArchiveOrchestrator) SetStopChannel(stop <-chan struct{}) {
	o.stopCh = stop
}

// SetLogger sets a custom logger for the orchestrator. Call before
// Initialize/Execute so all phases inherit it.
func (o *ArchiveOrchestrator) SetLogger(log *logger.Logger) {
	o.logger = log
}

// convertRecordSet converts types.RecordSet to archiver.RecordSet
func convertRecordSet(ts *types.RecordSet) *RecordSet {
	return &RecordSet{
		RootPKs: ts.RootPKs,
		Records: ts.Records,
	}
}
