package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/dbsmedya/goarchive/internal/replication"
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
	config           *config.Config
	jobConfig        *config.JobConfig
	jobName          string
	dbManager        *database.Manager
	graph            *graph.Graph
	logger           *logger.Logger
	initialized      bool
	processingCfg    config.ProcessingConfig
	lagFactory       lagMonitorFactory
	force            bool
	staleAtStartup   bool
	stopCh           <-chan struct{} // cooperative graceful-stop signal (nil = disabled)
	progressInterval time.Duration
	progressOut      io.Writer
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
		lagFactory: func(dbs []*sql.DB, rc config.ReplicationConfig, log *logger.Logger) (lagWaiter, error) {
			return replication.New(rc, dbs, log)
		},
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

	// Initialize the replication gate if enabled. This runs BEFORE
	// beginJobStartup so an enabled replication block with an empty fleet is
	// rejected before any advisory lock is acquired or any archiver_job state
	// is created — the invariant is fail-fast, not fail-after-side-effects.
	var lagMonitor lagWaiter
	if o.config.Replication.Enabled {
		if len(o.dbManager.Replicas) == 0 {
			return nil, fmt.Errorf("replication monitoring enabled but no replica connections")
		}
		lagMonitor, err = o.lagFactory(o.dbManager.Replicas, o.config.Replication, o.logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create replication gate: %w", err)
		}
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
	if err := loadRootPKMeta(ctx, o.dbManager.Source, o.config.Source.Database, o.graph); err != nil {
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

	// Honor processing.batch_size for resume bookkeeping chunking (issue #8,
	// Problem 2). Must run before recovery and the batch loop.
	o.applyResumeChunkSizing(resumeMgr)

	pipeline := &batchPipeline{
		jobName:       o.jobName,
		mainMode:      batchDeleteOnly,
		graph:         o.graph,
		logger:        o.logger,
		stopCh:        o.stopCh,
		processingCfg: o.processingCfg,
		discovery:     discovery,
		deletePhase:   deletePhase,
		resumeMgr:     resumeMgr,
		fetcher:       fetcher,
		lagMonitor:    lagMonitor,
	}

	if shouldResume {
		agg, err := pipeline.recover(ctx, nil)
		result.RecordsDeleted += agg.RecordsDeleted
		if err != nil {
			// Return result, not nil: agg is folded in above so partial recovery
			// totals survive a mid-resume failure (matches archive/copy-only).
			return result, fmt.Errorf("resume failed: %w", err)
		}
	}

	progressOut := o.progressOut
	if progressOut == nil {
		progressOut = os.Stdout
	}
	tracker, stopProgress, perr := startProgress(ctx, o.progressInterval, fetcher,
		batchDeleteOnly, 0, result.RecordsDeleted, progressOut)
	if perr != nil {
		return result, perr
	}
	defer stopProgress()

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
			tracker.MarkComplete()
			break
		}

		// Operator pause switch: block before processing this batch while the
		// sentinel file exists.
		if err := newSentinelGate(o.processingCfg.SentinelFile, o.logger).withNotifier(tracker).wait(ctx, o.stopCh); err != nil {
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

		if lagMonitor != nil {
			if err := lagMonitor.WaitForLag(ctx); err != nil {
				return nil, fmt.Errorf("lag monitor error: %w", err)
			}
		}
		batchStats, err := pipeline.processBatch(ctx, rootIDs, batchDeleteOnly,
			rootIDs[len(rootIDs)-1], nil)
		if err != nil {
			return nil, fmt.Errorf("processBatch failed: %w", err)
		}
		result.RecordsDeleted += batchStats.RecordsDeleted
		tracker.RecordBatch(*batchStats)

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

// applyResumeChunkSizing wires processing.batch_size into purge's
// resume-bookkeeping chunk size. Purge has no copy/verify phase — discovery and
// delete already receive batch_size / batch_delete_size directly — but the resume
// bookkeeping would otherwise pin at the 1000 default, ignoring batch_size.
// Mirrors archive/copy-only so batch_size is honored consistently (issue #8,
// Problem 2).
func (o *PurgeOrchestrator) applyResumeChunkSizing(resumeMgr *ResumeManager) {
	resumeMgr.SetChunkSize(o.processingCfg.BatchSize)
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

// SetProgressInterval enables periodic --progress reporting. Zero disables it.
func (o *PurgeOrchestrator) SetProgressInterval(d time.Duration) {
	o.progressInterval = d
}

// SetLogger sets a custom logger for the orchestrator. Call before
// Initialize/Execute so all phases inherit it.
func (o *PurgeOrchestrator) SetLogger(log *logger.Logger) {
	o.logger = log
}
