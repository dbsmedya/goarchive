package archiver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/lock"
	"github.com/dbsmedya/goarchive/internal/logger"
)

// PurgeResult contains statistics and status of purge operation.
type PurgeResult struct {
	JobName          string
	StartedAt        time.Time
	CompletedAt      time.Time
	Duration         time.Duration
	BatchesProcessed int
	RecordsDeleted   int64
	Success          bool
}

// PurgeOrchestrator coordinates purge operation using dependency graph.
type PurgeOrchestrator struct {
	config        *config.Config
	jobConfig     *config.JobConfig
	jobName       string
	dbManager     *database.Manager
	graph         *graph.Graph
	logger        *logger.Logger
	initialized   bool
	processingCfg config.ProcessingConfig
	skipLock      bool
}

// NewPurgeOrchestrator creates a new purge orchestrator.
func NewPurgeOrchestrator(cfg *config.Config, jobName string, jobCfg *config.JobConfig, dbManager *database.Manager) (*PurgeOrchestrator, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is nil")
	}
	if jobCfg == nil {
		return nil, fmt.Errorf("job config is nil")
	}
	if dbManager == nil {
		return nil, fmt.Errorf("database manager is nil")
	}

	return &PurgeOrchestrator{
		config:        cfg,
		jobConfig:     jobCfg,
		jobName:       jobName,
		dbManager:     dbManager,
		logger:        logger.NewDefault(),
		processingCfg: jobCfg.GetJobProcessing(cfg.Processing),
	}, nil
}

// Initialize builds and validates dependency graph.
func (o *PurgeOrchestrator) Initialize() error {
	if o.initialized {
		return nil
	}

	builder := graph.NewBuilder(o.jobConfig)
	g, err := builder.Build()
	if err != nil {
		return fmt.Errorf("failed to build dependency graph: %w", err)
	}
	if g.HasCycle() {
		return fmt.Errorf("dependency cycle detected in graph")
	}

	o.graph = g
	o.initialized = true
	return nil
}

// Execute runs purge flow (discover + delete only).
func (o *PurgeOrchestrator) Execute(ctx context.Context) (*PurgeResult, error) {
	if !o.initialized {
		return nil, fmt.Errorf("orchestrator not initialized")
	}
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}

	if !o.skipLock {
		// Advisory lock is held on Destination so it serializes against Archive and
		// Copy-only for the same job name across orchestrators.
		jobLock := lock.NewJobLock(o.dbManager.Destination, o.jobName)
		if err := jobLock.AcquireOrFail(ctx); err != nil {
			if errors.Is(err, lock.ErrLockTimeout) {
				return nil, fmt.Errorf("job %q is already running on another instance", o.jobName)
			}
			return nil, fmt.Errorf("failed to acquire job lock: %w", err)
		}
		defer func() { _, _ = jobLock.ReleaseLock(context.Background()) }()
	}

	result := &PurgeResult{
		JobName:   o.jobName,
		StartedAt: time.Now(),
		Success:   false,
	}

	// Resume tables live on Destination across all orchestrators (archive, purge,
	// copy-only) so a single metadata location covers cross-command state.
	resumeMgr, err := NewResumeManager(o.dbManager.Destination, o.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create resume manager: %w", err)
	}
	if err := resumeMgr.InitializeTables(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize resume tables: %w", err)
	}
	jobState, err := resumeMgr.GetOrCreateJobWithType(ctx, o.jobName, o.jobConfig.RootTable, JobTypePurge)
	if err != nil {
		return nil, fmt.Errorf("failed to get/create job: %w", err)
	}
	if err := resumeMgr.UpdateJobStatus(ctx, o.jobName, JobStatusRunning); err != nil {
		return nil, fmt.Errorf("failed to set job running status: %w", err)
	}
	defer func() {
		_ = resumeMgr.UpdateJobStatus(context.Background(), o.jobName, JobStatusIdle)
	}()

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
	deletePhase, err := NewDeletePhase(o.dbManager.Source, o.graph, o.processingCfg.BatchDeleteSize, o.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create delete phase: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}

		rootIDs, err := fetcher.FetchNextBatch(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch root IDs: %w", err)
		}
		if len(rootIDs) == 0 {
			break
		}

		result.BatchesProcessed++
		if err := resumeMgr.LogBatchPending(ctx, o.jobName, rootIDs); err != nil {
			return nil, fmt.Errorf("failed to log pending batch entries: %w", err)
		}

		for _, rootID := range rootIDs {
			discovered, err := discovery.Discover(ctx, []interface{}{rootID})
			if err != nil {
				_ = resumeMgr.MarkFailed(ctx, o.jobName, rootID, err.Error())
				return nil, fmt.Errorf("discovery failed: %w", err)
			}

			deleteStats, err := deletePhase.Delete(ctx, convertRecordSet(discovered))
			if err != nil {
				_ = resumeMgr.MarkFailed(ctx, o.jobName, rootID, err.Error())
				return nil, fmt.Errorf("delete failed: %w", err)
			}

			if err := resumeMgr.UpdateCheckpoint(ctx, o.jobName, rootID); err != nil {
				return nil, fmt.Errorf("checkpoint update failed: %w", err)
			}
			fetcher.UpdateCheckpoint(rootID)
			if err := resumeMgr.MarkCompleted(ctx, o.jobName, rootID); err != nil {
				return nil, fmt.Errorf("failed to mark completed: %w", err)
			}

			result.RecordsDeleted += deleteStats.RowsDeleted
		}

		if o.processingCfg.SleepSeconds > 0 {
			sleepDuration := time.Duration(o.processingCfg.SleepSeconds * float64(time.Second))
			select {
			case <-ctx.Done():
				return result, ctx.Err()
			case <-time.After(sleepDuration):
			}
		}
	}

	result.CompletedAt = time.Now()
	result.Duration = result.CompletedAt.Sub(result.StartedAt)
	result.Success = true
	return result, nil
}

// SetSkipLock toggles advisory lock acquisition during Execute.
func (o *PurgeOrchestrator) SetSkipLock(skip bool) {
	o.skipLock = skip
}
