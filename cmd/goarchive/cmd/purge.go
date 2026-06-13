package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/dbsmedya/goarchive/internal/archiver"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/spf13/cobra"
)

var (
	purgeJob                   string
	purgeForce                 bool
	purgeSkipValidatePreflight bool
	purgeForceTriggers         bool
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
		"Refresh a stale heartbeat takeover only. Because purge is destructive, --force CANNOT proceed while the advisory GET_LOCK is still held by another connection (a held lock cannot be safely stolen): verify the prior process is dead and its MySQL session has closed, then retry. Cannot bypass: a live heartbeating job, the same-root concurrency check, or preflight checks.")
	purgeCmd.Flags().BoolVar(&purgeSkipValidatePreflight, "skip-validate-preflight", false,
		"Skip preflight checks before this run (DANGEROUS - see docs)")
	purgeCmd.Flags().BoolVar(&purgeForceTriggers, "force-triggers", false,
		"Proceed despite DELETE triggers detected by preflight")

	rootCmd.AddCommand(purgeCmd)
}

func runPurge(cmd *cobra.Command, args []string) error {
	configFile := GetConfigFile()

	// Load configuration
	cfg, err := config.Load(configFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Apply CLI overrides
	overrides := GetCLIOverrides()
	cfg.ApplyOverrides(overrides.LogLevel, overrides.LogFormat, overrides.SkipVerify)
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	// Get job config after applying overrides so CLI flags (e.g. --skip-verify) are visible.
	jobCfgValue, exists := cfg.Jobs[purgeJob]
	if !exists {
		return fmt.Errorf("job '%s' not found in configuration", purgeJob)
	}
	jobCfg := &jobCfgValue

	// Initialize logger
	log, err := newJobLogger(cfg, jobCfg, purgeJob)
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

	// Setup context with two-phase graceful shutdown. First Ctrl-C finishes the
	// in-flight batch and stops at the boundary (no pending rows); second Ctrl-C
	// cancels the work context, unwinding cleanly so the advisory lock is released
	// (replay recovers whatever is left). A third Ctrl-C hard-terminates.
	ctx, stopCh := database.SetupGracefulShutdown(
		func(_ os.Signal) {
			log.Warn("Received shutdown signal - finishing current batch, then stopping (Ctrl-C again to abort now)...")
		},
		func(_ os.Signal) {
			log.Error("Received second shutdown signal - aborting in-flight work")
			syncLogger(log)
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
	if err := runRuntimePreflight(ctx, cfg, jobCfg, dbManager, log, "purge", config.VerificationConfig{},
		archiver.PreflightProfileSourceOnly, purgeForceTriggers, purgeSkipValidatePreflight); err != nil {
		return err
	}

	orch, err := archiver.NewPurgeOrchestrator(cfg, purgeJob, jobCfg, dbManager)
	if err != nil {
		return fmt.Errorf("failed to create purge orchestrator: %w", err)
	}
	orch.SetLogger(log)
	if err := orch.Initialize(); err != nil {
		return fmt.Errorf("purge orchestrator initialization failed: %w", err)
	}
	orch.SetForce(purgeForce)
	orch.SetStopChannel(stopCh)
	result, err := orch.Execute(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			log.Warn("Purge operation cancelled by user")
			return fmt.Errorf("purge operation cancelled: %w", err)
		}
		return fmt.Errorf("purge operation failed: %w", err)
	}

	// Log the structured summary (reaches file outputs), then print for the console
	log.Infow("Purge complete",
		"duration", result.Duration,
		"batches_processed", result.BatchesProcessed,
		"records_deleted", result.RecordsDeleted,
	)

	// Display results
	fmt.Printf("\n=== Purge Complete ===\n")
	fmt.Printf("Job: %s\n", result.JobName)
	fmt.Printf("Duration: %s\n", result.Duration)
	fmt.Printf("Batches processed: %d\n", result.BatchesProcessed)
	fmt.Printf("Records deleted: %d\n", result.RecordsDeleted)
	fmt.Println("\nℹ️  No data was copied (purge mode)")

	return nil
}
