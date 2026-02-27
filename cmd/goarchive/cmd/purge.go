package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/dbsmedya/goarchive/internal/archiver"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/dbsmedya/goarchive/internal/logger"
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
	_ = purgeCmd.MarkFlagRequired("job") // Config-time error, cannot fail

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
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	// Initialize logger
	log, err := logger.New(&cfg.Logging)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
	defer syncLogger(log)

	log.Infow("Starting purge operation (delete only, no copy)",
		"job", purgeJob,
		"config", configFile,
	)

	// Create database manager
	dbManager := database.NewManager(cfg)

	// Setup context with signal handling
	ctx := database.SetupSignalHandlerWithSecondSignal(
		func(_ os.Signal) {
			log.Warn("Received shutdown signal - completing current batch...")
		},
		func(_ os.Signal) {
			log.Error("Received second shutdown signal - forcing immediate exit")
			syncLogger(log)
			os.Exit(130)
		},
	)

	// Connect to databases (destination needed for cross-command concurrency checks)
	if err := dbManager.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to databases: %w", err)
	}
	defer func() {
		if err := dbManager.Close(); err != nil {
			log.Errorf("Failed to close database connections: %v", err)
		}
	}()

	// Test connections
	if err := dbManager.Ping(ctx); err != nil {
		return fmt.Errorf("database connection failed: %w", err)
	}
	if err := checkConcurrentJobsByRootTable(ctx, dbManager.Destination, jobCfg.RootTable, purgeJob, "purge"); err != nil {
		return fmt.Errorf("concurrent job check failed: %w", err)
	}

	orch, err := archiver.NewPurgeOrchestrator(cfg, purgeJob, jobCfg, dbManager)
	if err != nil {
		return fmt.Errorf("failed to create purge orchestrator: %w", err)
	}
	if err := orch.Initialize(); err != nil {
		return fmt.Errorf("purge orchestrator initialization failed: %w", err)
	}
	orch.SetSkipLock(purgeForce)
	if purgeForce {
		log.Warnw("Skipping advisory lock acquisition in orchestrator (--force flag used)", "job", purgeJob)
	}
	result, err := orch.Execute(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			log.Warn("Purge operation cancelled by user")
			return fmt.Errorf("purge operation cancelled: %w", err)
		}
		return fmt.Errorf("purge operation failed: %w", err)
	}

	// Display results
	fmt.Printf("\n=== Purge Complete ===\n")
	fmt.Printf("Job: %s\n", result.JobName)
	fmt.Printf("Duration: %s\n", result.Duration)
	fmt.Printf("Batches processed: %d\n", result.BatchesProcessed)
	fmt.Printf("Records deleted: %d\n", result.RecordsDeleted)
	fmt.Println("\nℹ️  No data was copied (purge mode)")

	return nil
}
