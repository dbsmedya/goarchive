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
	"github.com/dbsmedya/goarchive/internal/lock"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/dbsmedya/goarchive/internal/sqlutil"
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
func (o *CopyOnlyOrchestrator) Execute(ctx context.Context, force bool) (*CopyOnlyResult, error) {
	if !o.initialized {
		return nil, fmt.Errorf("orchestrator not initialized")
	}
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}

	result := &CopyOnlyResult{
		JobName:            o.jobName,
		StartedAt:          time.Now(),
		VerificationMethod: o.verificationCfg.Method,
		Errors:             make([]error, 0),
		Success:            false,
	}
	fail := func(format string, args ...interface{}) (*CopyOnlyResult, error) {
		err := fmt.Errorf(format, args...)
		result.Errors = append(result.Errors, err)
		return result, err
	}

	resumeMgr, err := NewResumeManager(o.dbManager.Destination, o.logger)
	if err != nil {
		return fail("failed to create resume manager: %w", err)
	}
	if err := resumeMgr.InitializeTables(ctx); err != nil {
		return fail("failed to initialize resume tables: %w", err)
	}
	if err := o.checkConcurrentJobs(ctx); err != nil {
		return fail("concurrent job check failed: %w", err)
	}
	jobState, err := resumeMgr.GetOrCreateJobWithType(ctx, o.jobName, o.jobConfig.RootTable, JobTypeCopyOnly)
	if err != nil {
		return fail("failed to get/create job: %w", err)
	}
	if err := resumeMgr.UpdateJobStatus(ctx, o.jobName, JobStatusRunning); err != nil {
		return fail("failed to set job running status: %w", err)
	}
	jobLock := lock.NewJobLock(o.dbManager.Destination, o.jobName)
	if err := jobLock.AcquireOrFail(ctx); err != nil {
		if errors.Is(err, lock.ErrLockTimeout) {
			return fail("job %q is already running on destination", o.jobName)
		}
		return fail("failed to acquire destination lock: %w", err)
	}
	defer func() { _, _ = jobLock.ReleaseLock(context.Background()) }()
	if err := o.displayInfoOrPrompt(force); err != nil {
		return fail("%w", err)
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

	dataVerifier, err := verifier.NewVerifier(
		o.dbManager.Source,
		o.dbManager.Destination,
		o.graph,
		verifier.VerificationMethod(o.verificationCfg.Method),
		o.logger,
	)
	if err != nil {
		return fail("failed to create verifier: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return fail("%w", ctx.Err())
		default:
		}

		rootIDs, err := fetcher.FetchNextBatch(ctx)
		if err != nil {
			return fail("failed to fetch root IDs: %w", err)
		}
		if len(rootIDs) == 0 {
			break
		}

		if err := resumeMgr.LogBatchPending(ctx, o.jobName, rootIDs); err != nil {
			return fail("failed to log pending batch entries: %w", err)
		}

		for _, rootID := range rootIDs {
			discovered, err := discovery.Discover(ctx, []interface{}{rootID})
			if err != nil {
				_ = resumeMgr.MarkFailed(ctx, o.jobName, rootID, err.Error())
				return fail("discovery failed: %w", err)
			}

			copyStats, err := copyPhase.Copy(ctx, convertRecordSet(discovered))
			if err != nil {
				_ = resumeMgr.MarkFailed(ctx, o.jobName, rootID, err.Error())
				return fail("copy failed: %w", err)
			}

			if !o.verificationCfg.SkipVerification {
				verifyStats, err := dataVerifier.Verify(ctx, discovered)
				if err != nil {
					_ = resumeMgr.MarkFailed(ctx, o.jobName, rootID, err.Error())
					return fail("verification failed: %w", err)
				}
				if verifyStats != nil {
					result.TablesVerified += verifyStats.TablesVerified
					result.RecordsVerified += verifyStats.TotalRows
				}
			}

			if err := resumeMgr.UpdateCheckpoint(ctx, o.jobName, rootID); err != nil {
				return fail("checkpoint update failed: %w", err)
			}
			fetcher.UpdateCheckpoint(rootID)
			if err := resumeMgr.MarkCompleted(ctx, o.jobName, rootID); err != nil {
				return fail("failed to mark completed: %w", err)
			}

			result.RecordsCopied += copyStats.RowsCopied
		}

		if o.processingCfg.SleepSeconds > 0 {
			sleepDuration := time.Duration(o.processingCfg.SleepSeconds * float64(time.Second))
			select {
			case <-ctx.Done():
				return fail("%w", ctx.Err())
			case <-time.After(sleepDuration):
			}
		}
	}

	if err := resumeMgr.UpdateJobStatus(ctx, o.jobName, JobStatusIdle); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("failed to set job status to idle: %w", err))
	}
	result.Success = len(result.Errors) == 0
	result.CompletedAt = time.Now()
	result.Duration = result.CompletedAt.Sub(result.StartedAt)
	result.TablesCopied = len(o.copyOrder)

	return result, nil
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

// checkConcurrentJobs blocks copy-only when another job is running on the same root table.
func (o *CopyOnlyOrchestrator) checkConcurrentJobs(ctx context.Context) error {
	const query = `
		SELECT job_name FROM archiver_job
		WHERE root_table = ?
		AND job_status = ?
		AND job_name != ?
	`
	rows, err := o.dbManager.Destination.QueryContext(ctx, query, o.jobConfig.RootTable, JobStatusRunning, o.jobName)
	if err != nil {
		return fmt.Errorf("failed to query concurrent jobs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var conflicts []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("failed to scan concurrent job: %w", err)
		}
		conflicts = append(conflicts, name)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate concurrent jobs: %w", err)
	}
	if len(conflicts) > 0 {
		return fmt.Errorf("cannot run copy-only: job(s) already running on root_table %q: %v", o.jobConfig.RootTable, conflicts)
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

// GetCopyOrder returns table copy order.
func (o *CopyOnlyOrchestrator) GetCopyOrder() ([]string, error) {
	if !o.initialized {
		return nil, fmt.Errorf("orchestrator not initialized")
	}
	return o.copyOrder, nil
}
