// Package archiver provides the core archive orchestration logic for GoArchive.
package archiver

import (
	"context"
	"fmt"
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
	JobName        string
	StartedAt      time.Time
	CompletedAt    time.Time
	Duration       time.Duration
	TablesCopied   int
	TablesDeleted  int
	RecordsCopied  int64
	RecordsDeleted int64
	Errors         []error
	Success        bool
}

// CheckpointCallback is called after each root PK is processed for crash recovery.
type CheckpointCallback func(rootPK interface{}, status string) error

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

// GetCopyOrder returns the table order for copying (parent tables first).
// Returns an error if the orchestrator has not been initialized.
func (o *ArchiveOrchestrator) GetCopyOrder() ([]string, error) {
	if !o.initialized {
		return nil, fmt.Errorf("orchestrator not initialized")
	}
	return o.copyOrder, nil
}

// GetDeleteOrder returns the table order for deletion (child tables first).
// Returns an error if the orchestrator has not been initialized.
func (o *ArchiveOrchestrator) GetDeleteOrder() ([]string, error) {
	if !o.initialized {
		return nil, fmt.Errorf("orchestrator not initialized")
	}
	return o.deleteOrder, nil
}

// Execute runs the archive operation. It processes records in batches,
// copying them to the destination and then deleting from the source.
// The checkpoint callback is invoked after each root PK is processed.
func (o *ArchiveOrchestrator) Execute(ctx context.Context, checkpoint CheckpointCallback) (*ArchiveResult, error) {
	if !o.initialized {
		return nil, fmt.Errorf("orchestrator not initialized")
	}

	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}

	result := &ArchiveResult{
		JobName:   o.jobName,
		StartedAt: time.Now(),
		Errors:    make([]error, 0),
		Success:   false,
	}

	o.logger.Infow("Starting archive execution",
		"job", o.jobName,
		"batch_size", o.processingCfg.BatchSize,
		"batch_delete_size", o.processingCfg.BatchDeleteSize,
		"sleep_seconds", o.processingCfg.SleepSeconds,
		"verification_method", o.verificationCfg.Method,
		"skip_verification", o.verificationCfg.SkipVerification,
	)

	// Initialize resume system
	resumeMgr, err := NewResumeManager(o.dbManager.Destination, o.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create resume manager: %w", err)
	}
	if err := resumeMgr.InitializeTables(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize resume tables: %w", err)
	}

	// Get or create job (auto-detects resume)
	jobState, err := resumeMgr.GetOrCreateJob(ctx, o.jobName, o.jobConfig.RootTable)
	if err != nil {
		return nil, fmt.Errorf("failed to get/create job: %w", err)
	}

	// Check if resuming
	shouldResume, err := resumeMgr.ShouldResume(ctx, o.jobName)
	if err != nil {
		return nil, fmt.Errorf("failed to check resume: %w", err)
	}

	if shouldResume {
		o.logger.Infow("Resuming interrupted job",
			"job", o.jobName,
			"checkpoint", jobState.LastProcessedRootPKID,
		)
	}

	// Initialize lag monitor if enabled
	var lagMonitor *LagMonitor
	if o.config.Replica.Enabled {
		replica := o.dbManager.Replica
		if replica == nil {
			return nil, fmt.Errorf("replication monitoring enabled but no replica connection")
		}
		lagMonitor, err = NewLagMonitor(replica, o.config.Safety, o.logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create lag monitor: %w", err)
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
		return nil, fmt.Errorf("failed to create record discovery: %w", err)
	}

	copyPhase, err := NewCopyPhase(
		o.dbManager.Source,
		o.dbManager.Destination,
		o.graph,
		o.config.Safety,
		o.logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create copy phase: %w", err)
	}

	dataVerifier, err := verifier.NewVerifier(
		o.dbManager.Source,
		o.dbManager.Destination,
		o.graph,
		verifier.VerificationMethod(o.verificationCfg.Method),
		o.logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create verifier: %w", err)
	}

	deletePhase, err := NewDeletePhase(
		o.dbManager.Source,
		o.graph,
		o.processingCfg.BatchDeleteSize,
		o.logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create delete phase: %w", err)
	}

	// Batch processing loop
	batchNum := 0
	totalProcessed := int64(0)

	for {
		select {
		case <-ctx.Done():
			o.logger.Warn("Context cancelled - stopping after current batch")
			return result, ctx.Err()
		default:
		}

		// Check replication lag before batch
		if lagMonitor != nil {
			if err := lagMonitor.WaitForLag(ctx); err != nil {
				return nil, fmt.Errorf("lag monitor error: %w", err)
			}
		}

		// Fetch next batch of root IDs
		rootIDs, err := fetcher.FetchNextBatch(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch root IDs: %w", err)
		}

		// Empty batch = job complete
		if len(rootIDs) == 0 {
			o.logger.Info("No more root IDs to process - job complete")
			break
		}

		batchNum++
		o.logger.Infow("Processing batch",
			"batch", batchNum,
			"root_ids", len(rootIDs),
		)

		// Process each root ID in batch
		for _, rootID := range rootIDs {
			rootPKID := toInt64(rootID)

			// Discovery phase (BFS)
			discovered, err := discovery.Discover(ctx, []interface{}{rootID})
			if err != nil {
				resumeMgr.MarkFailed(ctx, o.jobName, rootPKID, err.Error())
				return nil, fmt.Errorf("discovery failed: %w", err)
			}

			// Convert types.RecordSet to archiver.RecordSet for copy/delete
			recordSet := convertRecordSet(discovered)

			// Copy phase (with transaction)
			copyStats, err := copyPhase.Copy(ctx, recordSet)
			if err != nil {
				resumeMgr.MarkFailed(ctx, o.jobName, rootPKID, err.Error())
				return nil, fmt.Errorf("copy failed: %w", err)
			}

			// Verify phase
			if !o.verificationCfg.SkipVerification {
				_, err := dataVerifier.Verify(ctx, discovered)
				if err != nil {
					resumeMgr.MarkFailed(ctx, o.jobName, rootPKID, err.Error())
					return nil, fmt.Errorf("verification failed: %w", err)
				}
			}

			// Delete phase
			deleteStats, err := deletePhase.Delete(ctx, recordSet)
			if err != nil {
				resumeMgr.MarkFailed(ctx, o.jobName, rootPKID, err.Error())
				return nil, fmt.Errorf("delete failed: %w", err)
			}

			// Update checkpoint
			if err := resumeMgr.UpdateCheckpoint(ctx, o.jobName, rootPKID); err != nil {
				return nil, fmt.Errorf("checkpoint update failed: %w", err)
			}

			// Mark as completed
			if err := resumeMgr.MarkCompleted(ctx, o.jobName, rootPKID); err != nil {
				return nil, fmt.Errorf("failed to mark completed: %w", err)
			}

			// Update stats
			result.RecordsCopied += copyStats.RowsCopied
			result.RecordsDeleted += deleteStats.RowsDeleted
			totalProcessed++

			// Call checkpoint callback if provided
			if checkpoint != nil {
				if err := checkpoint(rootID, "completed"); err != nil {
					o.logger.Warnw("Checkpoint callback failed", "error", err)
				}
			}
		}

		// Sleep between batches
		if o.processingCfg.SleepSeconds > 0 {
			time.Sleep(time.Duration(o.processingCfg.SleepSeconds * float64(time.Second)))
		}
	}

	// Finalize result
	result.Success = len(result.Errors) == 0
	result.CompletedAt = time.Now()
	result.Duration = result.CompletedAt.Sub(result.StartedAt)
	result.TablesCopied = len(o.copyOrder)
	result.TablesDeleted = len(o.deleteOrder)

	// Mark job as idle
	if err := resumeMgr.UpdateJobStatus(ctx, o.jobName, JobStatusIdle); err != nil {
		o.logger.Warnw("Failed to set job status to idle", "error", err)
	}

	o.logger.Infow("Archive execution completed",
		"duration", result.Duration,
		"success", result.Success,
		"records_copied", result.RecordsCopied,
		"records_deleted", result.RecordsDeleted,
	)

	return result, nil
}

// IsInitialized returns true if the orchestrator has been initialized.
func (o *ArchiveOrchestrator) IsInitialized() bool {
	return o.initialized
}

// GetGraph returns the dependency graph. Returns nil if not initialized.
func (o *ArchiveOrchestrator) GetGraph() *graph.Graph {
	return o.graph
}

// GetJobConfig returns the job configuration.
func (o *ArchiveOrchestrator) GetJobConfig() *config.JobConfig {
	return o.jobConfig
}

// GetConfig returns the global configuration.
func (o *ArchiveOrchestrator) GetConfig() *config.Config {
	return o.config
}

// GetJobName returns the job name.
func (o *ArchiveOrchestrator) GetJobName() string {
	return o.jobName
}

// GetProcessingConfig returns the effective processing configuration.
func (o *ArchiveOrchestrator) GetProcessingConfig() config.ProcessingConfig {
	return o.processingCfg
}

// GetVerificationConfig returns the effective verification configuration.
func (o *ArchiveOrchestrator) GetVerificationConfig() config.VerificationConfig {
	return o.verificationCfg
}

// UpdateProcessingConfig updates the effective processing configuration.
// This should be called after applying CLI overrides.
func (o *ArchiveOrchestrator) UpdateProcessingConfig(cfg config.ProcessingConfig) {
	o.processingCfg = cfg
}

// convertRecordSet converts types.RecordSet to archiver.RecordSet
func convertRecordSet(ts *types.RecordSet) *RecordSet {
	return &RecordSet{
		RootPKs: ts.RootPKs,
		Records: ts.Records,
	}
}

// toInt64 converts an interface{} to int64
// Deprecated: Use types.ToInt64 from internal/types instead.
func toInt64(v interface{}) int64 {
	return types.ToInt64(v)
}
