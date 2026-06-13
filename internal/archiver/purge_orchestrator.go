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
	config         *config.Config
	jobConfig      *config.JobConfig
	jobName        string
	dbManager      *database.Manager
	graph          *graph.Graph
	logger         *logger.Logger
	initialized    bool
	processingCfg  config.ProcessingConfig
	force          bool
	staleAtStartup bool
	stopCh         <-chan struct{} // cooperative graceful-stop signal (nil = disabled)
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
func (o *PurgeOrchestrator) Execute(ctx context.Context) (result *PurgeResult, err error) {
	if !o.initialized {
		return nil, fmt.Errorf("orchestrator not initialized")
	}
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}

	result = &PurgeResult{
		JobName:   o.jobName,
		StartedAt: time.Now(),
		Success:   false,
	}

	startup, err := beginJobStartup(ctx, o.dbManager.Destination, o.logger, o.jobName, o.jobConfig.RootTable, JobTypePurge, "purge", o.force, o.config.Destination.EffectiveJobSchema())
	if err != nil {
		return nil, err
	}
	defer func() {
		if r := recover(); r != nil {
			startup.cleanup(fmt.Errorf("panic during purge: %v", r))
			panic(r)
		}
		startup.cleanup(err)
	}()
	resumeMgr := startup.resumeMgr
	jobState := startup.jobState
	o.staleAtStartup = startup.staleAtStartup
	ctx = startup.runCtx
	if err := loadRootPKMeta(ctx, o.dbManager.Source, o.graph); err != nil {
		return nil, fmt.Errorf("failed to load root PK metadata: %w", err)
	}
	shouldResume, err := resumeMgr.ShouldResume(ctx, o.jobName)
	if err != nil {
		return nil, fmt.Errorf("failed to check resume: %w", err)
	}

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
	discovery.SetLogger(o.logger)
	deletePhase, err := NewDeletePhase(o.dbManager.Source, o.graph, o.processingCfg.BatchDeleteSize, o.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create delete phase: %w", err)
	}
	// Throttle deletes (between batch_delete_size chunks) to limit binlog/replication lag.
	deletePhase.SetSleepSeconds(o.processingCfg.DeleteSleepSeconds)

	if shouldResume {
		if err := o.replayPendingPKs(ctx, resumeMgr, discovery, deletePhase, fetcher); err != nil {
			return nil, fmt.Errorf("pending replay failed: %w", err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}

		// Cooperative graceful stop: the previous batch's roots are all 'completed'
		// before we reach here, so stopping at this boundary leaves no pending rows.
		if stopRequested(o.stopCh) {
			o.logger.Warn("Graceful stop requested - stopping at batch boundary (run again to resume)")
			break
		}

		rootIDs, err := fetcher.FetchNextBatch(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch root IDs: %w", err)
		}
		if len(rootIDs) == 0 {
			break
		}

		// Operator pause switch: block before processing this batch while the
		// sentinel file exists.
		if err := newSentinelGate(o.processingCfg.SentinelFile, o.logger).wait(ctx, o.stopCh); err != nil {
			return nil, err
		}
		if stopRequested(o.stopCh) {
			o.logger.Warn("Graceful stop requested - stopping at batch boundary (run again to resume)")
			break
		}

		result.BatchesProcessed++
		if err := resumeMgr.LogBatchPending(ctx, o.jobName, rootIDs); err != nil {
			return nil, fmt.Errorf("failed to log pending batch entries: %w", err)
		}

		for _, rootID := range rootIDs {
			deleted, err := o.processPurgeRoot(ctx, rootID, discovery, deletePhase, fetcher, resumeMgr)
			if err != nil {
				return nil, err
			}
			result.RecordsDeleted += deleted
		}

		if o.processingCfg.SleepSeconds > 0 {
			sleepDuration := time.Duration(o.processingCfg.SleepSeconds * float64(time.Second))
			if err := interruptibleSleep(ctx, o.stopCh, sleepDuration); err != nil {
				return result, err
			}
		}
	}

	result.CompletedAt = time.Now()
	result.Duration = result.CompletedAt.Sub(result.StartedAt)
	result.Success = true
	return result, nil
}

func (o *PurgeOrchestrator) replayPendingPKs(ctx context.Context, resumeMgr *ResumeManager, discovery *RecordDiscovery, deletePhase *DeletePhase, fetcher *RootIDFetcher) error {
	pending, err := resumeMgr.GetPendingPKs(ctx, o.jobName)
	if err != nil {
		return err
	}
	for _, rawPK := range pending {
		// Cooperative graceful stop: each replayed root reaches 'completed' before
		// we loop, so stopping here leaves the remainder in 'pending' to resume.
		if stopRequested(o.stopCh) {
			o.logger.Warn("Graceful stop requested - stopping replay at boundary (run again to resume)")
			return nil
		}
		// Operator pause switch also applies during resume/recovery.
		if err := newSentinelGate(o.processingCfg.SentinelFile, o.logger).wait(ctx, o.stopCh); err != nil {
			return err
		}
		if stopRequested(o.stopCh) {
			o.logger.Warn("Graceful stop requested - stopping replay at boundary (run again to resume)")
			return nil
		}
		dataType, unsigned, ok := o.graph.GetRootPKMeta()
		if !ok {
			return fmt.Errorf("root PK metadata not loaded")
		}
		typedPK, err := types.ConvertRootPK(rawPK, dataType, unsigned)
		if err != nil {
			return err
		}
		if _, err := o.processPurgeRoot(ctx, typedPK, discovery, deletePhase, fetcher, resumeMgr); err != nil {
			return fmt.Errorf("replay failed for pk=%s: %w", rawPK, err)
		}
	}
	return nil
}

func (o *PurgeOrchestrator) processPurgeRoot(ctx context.Context, rootID interface{}, discovery *RecordDiscovery, deletePhase *DeletePhase, fetcher *RootIDFetcher, resumeMgr *ResumeManager) (int64, error) {
	discovered, err := discovery.Discover(ctx, []interface{}{rootID})
	if err != nil {
		markFailedUnlessCanceled(ctx, resumeMgr, o.logger, o.jobName, rootID, err)
		return 0, fmt.Errorf("discovery failed: %w", err)
	}
	deleteStats, err := deletePhase.Delete(ctx, convertRecordSet(discovered))
	if err != nil {
		markFailedUnlessCanceled(ctx, resumeMgr, o.logger, o.jobName, rootID, err)
		return 0, fmt.Errorf("delete failed: %w", err)
	}
	if err := resumeMgr.UpdateCheckpoint(ctx, o.jobName, rootID); err != nil {
		return 0, fmt.Errorf("checkpoint update failed: %w", err)
	}
	fetcher.UpdateCheckpoint(rootID)
	if err := resumeMgr.MarkCompleted(ctx, o.jobName, rootID); err != nil {
		return 0, fmt.Errorf("failed to mark completed: %w", err)
	}
	return deleteStats.RowsDeleted, nil
}

// SetForce controls heartbeat-aware advisory lock bypass.
func (o *PurgeOrchestrator) SetForce(force bool) {
	o.force = force
}

// SetStopChannel wires the cooperative graceful-stop signal. When the channel
// closes (first Ctrl-C), the loop finishes the in-flight batch and stops at the
// next boundary. A nil channel disables cooperative stop.
func (o *PurgeOrchestrator) SetStopChannel(stop <-chan struct{}) {
	o.stopCh = stop
}

// SetLogger sets a custom logger for the orchestrator. Call before
// Initialize/Execute so all phases inherit it.
func (o *PurgeOrchestrator) SetLogger(log *logger.Logger) {
	o.logger = log
}
