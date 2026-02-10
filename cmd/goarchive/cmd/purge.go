package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dbsmedya/goarchive/internal/archiver"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/lock"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/dbsmedya/goarchive/internal/types"
	"github.com/spf13/cobra"
)

var (
	purgeJob   string
	purgeForce bool
)

var purgeCmd = &cobra.Command{
	Use:   "purge",
	Short: "Delete data from source without archiving",
	Long: `Purge deletes matching records from source database without copying
to a destination. Use this when you don't need to preserve the data.

The purge process:
  1. Discover all related records using BFS traversal
  2. Delete from source in dependency order (child-first)

WARNING: This permanently deletes data. Use --dry-run first to verify.

Example:
  goarchive purge --config archiver.yaml --job purge_old_logs`,
	RunE: runPurge,
}

func init() {
	purgeCmd.Flags().StringVarP(&purgeJob, "job", "j", "",
		"Job name from configuration file (required)")
	purgeCmd.MarkFlagRequired("job")

	purgeCmd.Flags().BoolVar(&purgeForce, "force", false,
		"Force execution even if job lock cannot be acquired (use with caution)")

	rootCmd.AddCommand(purgeCmd)
}

func runPurge(cmd *cobra.Command, args []string) error {
	configFile := GetConfigFile()

	// Load configuration
	cfg, err := config.Load(configFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Get job config first
	jobCfgValue, exists := cfg.Jobs[purgeJob]
	if !exists {
		return fmt.Errorf("job '%s' not found in configuration", purgeJob)
	}
	jobCfg := &jobCfgValue

	// Apply CLI overrides
	overrides := GetCLIOverrides()
	cfg.ApplyOverrides(overrides.LogLevel, overrides.LogFormat,
		overrides.BatchSize, overrides.BatchDeleteSize,
		overrides.SleepSeconds, overrides.SkipVerify)

	// Get effective processing config for this job
	processingCfg := jobCfg.GetJobProcessing(cfg.Processing)

	// Initialize logger
	log, err := logger.New(&cfg.Logging)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}

	log.Infow("Starting purge operation (delete only, no copy)",
		"job", purgeJob,
		"config", configFile,
	)

	// Create database manager (source only, no destination needed)
	dbManager := database.NewManager(cfg)

	// Setup context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to source database only (purge doesn't need destination)
	if err := dbManager.ConnectSource(ctx); err != nil {
		return fmt.Errorf("failed to connect to source database: %w", err)
	}
	defer dbManager.Close()

	// Test source connection
	if err := dbManager.Ping(ctx); err != nil {
		return fmt.Errorf("database connection failed: %w", err)
	}

	// Acquire advisory lock to prevent concurrent job execution
	if !purgeForce {
		jobLock := lock.NewJobLock(dbManager.Source, purgeJob)
		if err := jobLock.AcquireOrFail(ctx); err != nil {
			if errors.Is(err, lock.ErrLockTimeout) {
				return fmt.Errorf("job '%s' is already running on another instance (use --force to override)", purgeJob)
			}
			return fmt.Errorf("failed to acquire job lock: %w", err)
		}
		defer jobLock.ReleaseLock(context.Background())
		log.Infow("Acquired advisory lock for job", "job", purgeJob)
	} else {
		log.Warnw("Skipping advisory lock acquisition (--force flag used)", "job", purgeJob)
	}

	// Build graph
	builder := graph.NewBuilder(jobCfg)
	g, err := builder.Build()
	if err != nil {
		return fmt.Errorf("failed to build dependency graph: %w", err)
	}

	if g.HasCycle() {
		return fmt.Errorf("dependency cycle detected in graph")
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Warn("Received shutdown signal - completing current batch...")
		cancel()
	}()

	// Initialize resume manager
	resumeMgr, err := archiver.NewResumeManager(dbManager.Source, log)
	if err != nil {
		return fmt.Errorf("failed to create resume manager: %w", err)
	}
	if err := resumeMgr.InitializeTables(ctx); err != nil {
		return fmt.Errorf("failed to initialize resume tables: %w", err)
	}

	// Get or create job
	jobState, err := resumeMgr.GetOrCreateJob(ctx, purgeJob, jobCfg.RootTable)
	if err != nil {
		return fmt.Errorf("failed to get/create job: %w", err)
	}

	// Create components (discovery and delete only)
	rootPKColumn := g.GetPK(jobCfg.RootTable)
	fetcher := archiver.NewRootIDFetcher(
		dbManager.Source,
		jobCfg.RootTable,
		rootPKColumn,
		jobCfg.Where,
		processingCfg.BatchSize,
		jobState.LastProcessedRootPKID,
	)

	discovery, err := archiver.NewRecordDiscovery(g, dbManager.Source, processingCfg.BatchSize)
	if err != nil {
		return fmt.Errorf("failed to create record discovery: %w", err)
	}

	deletePhase, err := archiver.NewDeletePhase(
		dbManager.Source,
		g,
		processingCfg.BatchDeleteSize,
		log,
	)
	if err != nil {
		return fmt.Errorf("failed to create delete phase: %w", err)
	}

	// Purge loop (discovery + delete, no copy/verify)
	batchNum := 0
	totalDeleted := int64(0)
	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			log.Info("Purge operation cancelled")
			return nil
		default:
		}

		// Fetch next batch
		rootIDs, err := fetcher.FetchNextBatch(ctx)
		if err != nil {
			return fmt.Errorf("failed to fetch root IDs: %w", err)
		}

		if len(rootIDs) == 0 {
			log.Info("No more root IDs to process - purge complete")
			break
		}

		batchNum++
		log.Infow("Processing purge batch",
			"batch", batchNum,
			"root_ids", len(rootIDs),
		)

		// Process each root ID
		for _, rootID := range rootIDs {
			rootPKID := types.ToInt64(rootID)

			// Discovery phase (BFS)
			discovered, err := discovery.Discover(ctx, []interface{}{rootID})
			if err != nil {
				resumeMgr.MarkFailed(ctx, purgeJob, rootPKID, err.Error())
				return fmt.Errorf("discovery failed: %w", err)
			}

			// Convert to archiver.RecordSet
			recordSet := &archiver.RecordSet{
				RootPKs: discovered.RootPKs,
				Records: discovered.Records,
			}

			// Delete phase (NO COPY, NO VERIFY)
			deleteStats, err := deletePhase.Delete(ctx, recordSet)
			if err != nil {
				resumeMgr.MarkFailed(ctx, purgeJob, rootPKID, err.Error())
				return fmt.Errorf("delete failed: %w", err)
			}

			// Update checkpoint
			if err := resumeMgr.UpdateCheckpoint(ctx, purgeJob, rootPKID); err != nil {
				return fmt.Errorf("checkpoint update failed: %w", err)
			}

			// Mark as completed
			if err := resumeMgr.MarkCompleted(ctx, purgeJob, rootPKID); err != nil {
				return fmt.Errorf("failed to mark completed: %w", err)
			}

			totalDeleted += deleteStats.RowsDeleted
		}

		// Sleep between batches
		if processingCfg.SleepSeconds > 0 {
			time.Sleep(time.Duration(processingCfg.SleepSeconds * float64(time.Second)))
		}
	}

	duration := time.Since(startTime)

	// Display results
	fmt.Printf("\n=== Purge Complete ===\n")
	fmt.Printf("Job: %s\n", purgeJob)
	fmt.Printf("Duration: %s\n", duration)
	fmt.Printf("Batches processed: %d\n", batchNum)
	fmt.Printf("Records deleted: %d\n", totalDeleted)
	fmt.Println("\nℹ️  No data was copied (purge mode)")

	return nil
}
