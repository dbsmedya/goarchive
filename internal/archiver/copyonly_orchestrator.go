package archiver

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/dbsmedya/goarchive/internal/sqlutil"
	"github.com/dbsmedya/goarchive/internal/types"
	"github.com/dbsmedya/goarchive/internal/verifier"
)

// CopyOnlyResult contains statistics and status of copy-only operation.
type CopyOnlyResult struct {
	JobName            string
	StartedAt          time.Time
	CompletedAt        time.Time
	Duration           time.Duration
	TablesCopied       int
	RecordsCopied      int64
	TablesVerified     int
	RecordsVerified    int64
	VerificationMethod string
	Errors             []error
	Success            bool
}

// CopyOnlyOrchestrator coordinates copy-only operation using dependency graph.
type CopyOnlyOrchestrator struct {
	config          *config.Config
	jobConfig       *config.JobConfig
	jobName         string
	dbManager       *database.Manager
	graph           *graph.Graph
	logger          *logger.Logger
	copyOrder       []string
	initialized     bool
	processingCfg   config.ProcessingConfig
	verificationCfg config.VerificationConfig
	promptReader    io.Reader
	staleAtStartup  bool
	stopCh          <-chan struct{} // cooperative graceful-stop signal (nil = disabled)
}

// NewCopyOnlyOrchestrator creates a new copy-only orchestrator.
func NewCopyOnlyOrchestrator(cfg *config.Config, jobName string, jobCfg *config.JobConfig, dbManager *database.Manager) (*CopyOnlyOrchestrator, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is nil")
	}
	if jobCfg == nil {
		return nil, fmt.Errorf("job config is nil")
	}
	if dbManager == nil {
		return nil, fmt.Errorf("database manager is nil")
	}

	log := logger.NewDefault()
	processingCfg := jobCfg.GetJobProcessing(cfg.Processing)
	verificationCfg := jobCfg.GetJobVerification(cfg.Verification)

	return &CopyOnlyOrchestrator{
		config:          cfg,
		jobName:         jobName,
		jobConfig:       jobCfg,
		dbManager:       dbManager,
		logger:          log,
		processingCfg:   processingCfg,
		verificationCfg: verificationCfg,
		promptReader:    os.Stdin,
	}, nil
}

// SetLogger sets a custom logger for the orchestrator. Call before
// Initialize/Execute so all phases inherit it.
func (o *CopyOnlyOrchestrator) SetLogger(log *logger.Logger) {
	o.logger = log
}

// SetStopChannel wires the cooperative graceful-stop signal. When the channel
// closes (first Ctrl-C), the loop finishes the in-flight batch and stops at the
// next boundary. A nil channel disables cooperative stop.
func (o *CopyOnlyOrchestrator) SetStopChannel(stop <-chan struct{}) {
	o.stopCh = stop
}

// Initialize builds dependency graph and computes copy order.
func (o *CopyOnlyOrchestrator) Initialize() error {
	if o.initialized {
		return nil
	}

	builder := graph.NewBuilder(o.jobConfig)
	g, err := builder.Build()
	if err != nil {
		return fmt.Errorf("failed to build dependency graph: %w", err)
	}
	o.graph = g

	if o.graph.HasCycle() {
		return fmt.Errorf("cycle detected in dependency graph")
	}

	copyOrder, err := o.graph.CopyOrder()
	if err != nil {
		return fmt.Errorf("failed to compute copy order: %w", err)
	}
	o.copyOrder = copyOrder
	o.initialized = true

	return nil
}

// Execute runs copy-only operation with copy and optional verify phases.
func (o *CopyOnlyOrchestrator) Execute(ctx context.Context, force bool) (result *CopyOnlyResult, err error) {
	if !o.initialized {
		return nil, fmt.Errorf("orchestrator not initialized")
	}
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}

	result = &CopyOnlyResult{
		JobName:            o.jobName,
		StartedAt:          time.Now(),
		VerificationMethod: o.verificationCfg.EffectiveMethod(),
		Errors:             make([]error, 0),
		Success:            false,
	}
	fail := func(format string, args ...interface{}) (*CopyOnlyResult, error) {
		err := fmt.Errorf(format, args...)
		result.Errors = append(result.Errors, err)
		return result, err
	}

	startup, err := beginJobStartup(ctx, o.dbManager.Destination, o.logger, o.jobName, o.jobConfig.RootTable, JobTypeCopyOnly, "copy-only", force, o.config.Destination.EffectiveJobSchema())
	if err != nil {
		return fail("%w", err)
	}
	defer func() {
		if r := recover(); r != nil {
			startup.cleanup(fmt.Errorf("panic during copy-only: %v", r))
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
	shouldResume, err := resumeMgr.ShouldResume(ctx, o.jobName)
	if err != nil {
		return fail("failed to check resume: %w", err)
	}
	if err := o.displayInfoOrPrompt(force); err != nil {
		return fail("%w", err)
	}
	if o.verificationCfg.SkipVerification {
		o.logger.Warn(copyOnlySkipVerificationNote)
	}
	if !force {
		if err := o.checkDestinationEmpty(ctx); err != nil {
			return fail("preflight check failed: %w", err)
		}
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

	// Decide whether INSERT IGNORE is safe, mirroring the archive orchestrator
	// (review P0-1/P1-2/#1). copy-only never deletes source, but a silently-skipped
	// INSERT IGNORE still produces an INCOMPLETE copy the operator believes is
	// faithful — and a later archive/purge of those source rows would then delete
	// data that was never truly copied. Force strict INSERT (abort on duplicate)
	// when the post-copy safety net is weak: count verification, verification
	// skipped, or a destination secondary UNIQUE index.
	effectiveMethod := o.verificationCfg.EffectiveMethod()
	destUniqueIdx, err := destinationSecondaryUniqueIndexes(ctx, o.dbManager.Destination,
		o.config.Destination.Database, o.graph.AllNodes())
	if err != nil {
		return fail("failed to inspect destination unique indexes: %w", err)
	}
	strictInsert := shouldUseStrictInsert(effectiveMethod, o.verificationCfg.SkipVerification, len(destUniqueIdx) > 0)
	if strictInsert && effectiveMethod != "count" {
		reason := "verification skipped (a silently-skipped row would leave an incomplete copy)"
		if len(destUniqueIdx) > 0 {
			reason = "destination secondary unique index present: " + strings.Join(destUniqueIdx, ", ")
		}
		o.logger.Warnw("Forcing strict INSERT (INSERT IGNORE disabled): a silently-skipped duplicate would leave an incomplete copy", "reason", reason)
	}
	copyPhase.SetStrictInsert(strictInsert)

	dataVerifier, err := verifier.NewVerifier(
		o.dbManager.Source,
		o.dbManager.Destination,
		o.graph,
		verifier.VerificationMethod(o.verificationCfg.EffectiveMethod()),
		o.logger,
	)
	if err != nil {
		return fail("failed to create verifier: %w", err)
	}

	// Honor processing.batch_size for copy/verify/resume chunking, not just the
	// root fetch (issue #8, Problem 2). Must run before replay and the batch loop.
	o.applyChunkSizing(copyPhase, dataVerifier, resumeMgr)

	if shouldResume {
		if err := o.replayPendingPKs(ctx, resumeMgr, discovery, copyPhase, dataVerifier, fetcher, result); err != nil {
			return fail("pending replay failed: %w", err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return fail("%w", ctx.Err())
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
			return fail("failed to fetch root IDs: %w", err)
		}
		if len(rootIDs) == 0 {
			break
		}

		// Operator pause switch: block before processing this batch while the
		// sentinel file exists.
		if err := newSentinelGate(o.processingCfg.SentinelFile, o.logger).wait(ctx, o.stopCh); err != nil {
			return fail("%w", err)
		}
		if stopRequested(o.stopCh) {
			o.logger.Warn("Graceful stop requested - stopping at batch boundary (run again to resume)")
			break
		}

		if err := resumeMgr.LogBatchPending(ctx, o.jobName, rootIDs); err != nil {
			return fail("failed to log pending batch entries: %w", err)
		}

		for _, rootID := range rootIDs {
			copied, err := o.processCopyOnlyRoot(ctx, rootID, discovery, copyPhase, dataVerifier, fetcher, resumeMgr, result)
			if err != nil {
				return fail("%w", err)
			}
			result.RecordsCopied += copied
		}

		if o.processingCfg.SleepSeconds > 0 {
			sleepDuration := time.Duration(o.processingCfg.SleepSeconds * float64(time.Second))
			if err := interruptibleSleep(ctx, o.stopCh, sleepDuration); err != nil {
				return fail("%w", err)
			}
		}
	}

	result.Success = len(result.Errors) == 0
	result.CompletedAt = time.Now()
	result.Duration = result.CompletedAt.Sub(result.StartedAt)
	result.TablesCopied = len(o.copyOrder)

	return result, nil
}

func (o *CopyOnlyOrchestrator) replayPendingPKs(ctx context.Context, resumeMgr *ResumeManager, discovery *RecordDiscovery, copyPhase *CopyPhase, dataVerifier *verifier.Verifier, fetcher *RootIDFetcher, result *CopyOnlyResult) error {
	pending, dataType, unsigned, err := pendingReplayPKs(ctx, resumeMgr, o.jobName, o.graph)
	if err != nil {
		return err
	}
	if len(pending) == 0 {
		return nil
	}
	// Strict INSERT (forced by count verification, --skip-verify, or a destination
	// secondary unique index — review P0-1/P1-2/003) cannot re-copy 'pending' rows:
	// their destination copy may already be committed, so a strict re-INSERT aborts
	// on duplicate and the job would self-block on every resume. Refuse with
	// recovery guidance rather than loop forever.
	if copyPhase.StrictInsert() {
		preview := pending
		if len(preview) > 10 {
			preview = preview[:10]
		}
		return fmt.Errorf("job %q has %d 'pending' root PKs from a prior interrupted run and uses strict INSERT "+
			"(forced by verification.method: count, --skip-verify, or a destination secondary unique index), so they "+
			"cannot be safely re-copied (their destination rows may already be committed, and a strict INSERT aborts "+
			"on duplicate).\n\n"+
			"To recover, choose one:\n"+
			"  1. Delete the destination rows already written for these pending PKs, then re-run.\n"+
			"  2. If using --skip-verify, drop it (and use verification.method: sha256) so replay uses idempotent "+
			"INSERT IGNORE, then re-run.\n\n"+
			"Pending PKs (first 10): %v", o.jobName, len(pending), preview)
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
		typedPK, err := types.ConvertRootPK(rawPK, dataType, unsigned)
		if err != nil {
			return err
		}
		copied, err := o.processCopyOnlyRoot(ctx, typedPK, discovery, copyPhase, dataVerifier, fetcher, resumeMgr, result)
		if err != nil {
			return fmt.Errorf("replay failed for pk=%s: %w", rawPK, err)
		}
		result.RecordsCopied += copied
	}
	return nil
}

// verifyChunkSizer is the narrow slice of *verifier.Verifier that
// applyChunkSizing needs. It exists so the batch-size wiring test can observe
// the handoff without Verifier carrying an otherwise-unused exported getter.
type verifyChunkSizer interface {
	SetChunkSize(size int)
}

// applyChunkSizing wires the effective processing.batch_size into copy-only's
// copy, verify, and resume-bookkeeping chunk sizes, mirroring the archive
// orchestrator (orchestrator.go). Without it copy-only silently pins copy chunks
// at defaultCopyBatchSize (200) and verify/resume chunks at 1000, ignoring the
// operator's batch_size (issue #8, Problem 2).
func (o *CopyOnlyOrchestrator) applyChunkSizing(copyPhase *CopyPhase, dataVerifier verifyChunkSizer, resumeMgr *ResumeManager) {
	copyPhase.SetBatchSize(o.processingCfg.BatchSize)
	dataVerifier.SetChunkSize(o.processingCfg.BatchSize)
	resumeMgr.SetChunkSize(o.processingCfg.BatchSize)
}

func (o *CopyOnlyOrchestrator) processCopyOnlyRoot(ctx context.Context, rootID interface{}, discovery *RecordDiscovery, copyPhase *CopyPhase, dataVerifier *verifier.Verifier, fetcher *RootIDFetcher, resumeMgr *ResumeManager, result *CopyOnlyResult) (int64, error) {
	discovered, err := discovery.Discover(ctx, []interface{}{rootID})
	if err != nil {
		markFailedUnlessCanceled(ctx, resumeMgr, o.logger, o.jobName, rootID, err)
		return 0, fmt.Errorf("discovery failed: %w", err)
	}
	copyStats, err := copyPhase.Copy(ctx, convertRecordSet(discovered))
	if err != nil {
		markFailedUnlessCanceled(ctx, resumeMgr, o.logger, o.jobName, rootID, err)
		return 0, fmt.Errorf("copy failed: %w", err)
	}
	if !o.verificationCfg.SkipVerification {
		verifyStats, err := dataVerifier.Verify(ctx, discovered)
		if err != nil {
			markFailedUnlessCanceled(ctx, resumeMgr, o.logger, o.jobName, rootID, err)
			return 0, fmt.Errorf("verification failed: %w", err)
		}
		if verifyStats != nil {
			result.TablesVerified += verifyStats.TablesVerified
			result.RecordsVerified += verifyStats.TotalRows
		}
	}
	if err := resumeMgr.UpdateCheckpoint(ctx, o.jobName, rootID); err != nil {
		return 0, fmt.Errorf("checkpoint update failed: %w", err)
	}
	fetcher.UpdateCheckpoint(rootID)
	if err := resumeMgr.MarkCompleted(ctx, o.jobName, rootID); err != nil {
		return 0, fmt.Errorf("failed to mark completed: %w", err)
	}
	return copyStats.RowsCopied, nil
}

func (o *CopyOnlyOrchestrator) displayInfoOrPrompt(force bool) error {
	if !force {
		return nil
	}

	fmt.Println("\n⚠️  FORCE MODE - WILL PROCEED WITHOUT SAFETY CHECKS:")
	fmt.Println("  • Skipping destination duplicate verification")
	fmt.Println("  • Copying data directly to destination")
	fmt.Println("  • May overwrite existing data")
	fmt.Print("\nDo you want to continue? [y/N]: ")

	reader := bufio.NewReader(o.promptReader)
	response, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("failed to read user input: %w", err)
	}
	response = strings.ToLower(strings.TrimSpace(response))
	if response != "y" && response != "yes" {
		return fmt.Errorf("operation cancelled by user")
	}
	return nil
}

// checkDestinationEmpty verifies destination tables in copy order do not contain data.
func (o *CopyOnlyOrchestrator) checkDestinationEmpty(ctx context.Context) error {
	for _, table := range o.copyOrder {
		query := fmt.Sprintf("SELECT 1 FROM %s LIMIT 1", sqlutil.QuoteIdentifier(table))
		var dummy int
		err := o.dbManager.Destination.QueryRowContext(ctx, query).Scan(&dummy)
		switch {
		case err == nil:
			return fmt.Errorf("destination table %q already contains data", table)
		case errors.Is(err, sql.ErrNoRows):
			continue
		default:
			return fmt.Errorf("failed to check destination table %s: %w", table, err)
		}
	}
	return nil
}
