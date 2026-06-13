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
	archiveJob                   string
	archiveForce                 bool
	archiveSkipValidatePreflight bool
	archiveForceTriggers         bool
)

var archiveCmd = &cobra.Command{
	Use:   "archive",
	Short: "Archive data from source to destination database",
	Long: `Archive copies matching records from source to destination database,
verifies the copy, then deletes from source.

The archive process follows these steps:
  1. Discover all related records using BFS traversal
  2. Copy records to destination in dependency order (parent-first)
  3. Verify copy integrity (count or SHA256)
  4. Delete from source in reverse order (child-first)

Example:
  goarchive archive --config archiver.yaml --job archive_old_orders`,
	RunE: runArchive,
}

func init() {
	archiveCmd.Flags().StringVarP(&archiveJob, "job", "j", "",
		"Job name from configuration file (required)")
	_ = archiveCmd.MarkFlagRequired("job") // Config-time error, cannot fail

	archiveCmd.Flags().BoolVar(&archiveForce, "force", false,
		"Refresh a stale heartbeat takeover only. Because archive is destructive, --force CANNOT proceed while the advisory GET_LOCK is still held by another connection (a held lock cannot be safely stolen): verify the prior process is dead and its MySQL session has closed, then retry. Cannot bypass: a live heartbeating job, the same-root concurrency check, or preflight checks.")
	archiveCmd.Flags().BoolVar(&archiveSkipValidatePreflight, "skip-validate-preflight", false,
		"Skip preflight checks before this run (DANGEROUS - see docs)")
	archiveCmd.Flags().BoolVar(&archiveForceTriggers, "force-triggers", false,
		"Proceed despite DELETE triggers detected by preflight")

	rootCmd.AddCommand(archiveCmd)
}

func runArchive(cmd *cobra.Command, args []string) error {
	configFile := GetConfigFile()

	// Load configuration
	cfg, err := config.Load(configFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Apply CLI overrides (to global config for logging, and get effective processing config)
	overrides := GetCLIOverrides()
	cfg.ApplyOverrides(overrides.LogLevel, overrides.LogFormat, overrides.SkipVerify)
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	// Get job config after applying overrides so CLI flags (e.g. --skip-verify) are visible.
	jobCfgValue, exists := cfg.Jobs[archiveJob]
	if !exists {
		return fmt.Errorf("job '%s' not found in configuration", archiveJob)
	}
	// Use pointer to job config
	jobCfg := &jobCfgValue

	// Initialize logger (per-job logging config, CLI flags win)
	log, err := newJobLogger(cfg, jobCfg, archiveJob)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
	defer syncLogger(log)

	log.Infow("Starting archive operation",
		"job", archiveJob,
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

	// Connect to databases
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
	if err := runRuntimePreflight(ctx, cfg, jobCfg, dbManager, log, "archive", jobCfg.GetJobVerification(cfg.Verification),
		archiver.PreflightProfileFull, archiveForceTriggers, archiveSkipValidatePreflight); err != nil {
		return err
	}

	// Create orchestrator
	orch, err := archiver.NewOrchestrator(cfg, archiveJob, jobCfg, dbManager)
	if err != nil {
		return fmt.Errorf("failed to create orchestrator: %w", err)
	}
	orch.SetLogger(log)

	// Initialize (build graph, validate)
	if err := orch.Initialize(); err != nil {
		return fmt.Errorf("orchestrator initialization failed: %w", err)
	}
	orch.SetForce(archiveForce)

	// Execute archive operation
	result, err := orch.Execute(ctx, nil)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			log.Warn("Archive operation cancelled by user")
			return fmt.Errorf("archive operation cancelled: %w", err)
		}
		return fmt.Errorf("archive operation failed: %w", err)
	}

	// Log the structured summary (reaches file outputs), then print for the console
	log.Infow("Archive complete",
		"duration", result.Duration,
		"tables_copied", result.TablesCopied,
		"tables_deleted", result.TablesDeleted,
		"records_copied", result.RecordsCopied,
		"records_deleted", result.RecordsDeleted,
		"success", result.Success,
		"errors", len(result.Errors),
	)

	// Display results
	fmt.Printf("\n=== Archive Complete ===\n")
	fmt.Printf("Job: %s\n", result.JobName)
	fmt.Printf("Duration: %s\n", result.Duration)
	fmt.Printf("Tables Copied: %d\n", result.TablesCopied)
	fmt.Printf("Tables Deleted: %d\n", result.TablesDeleted)
	fmt.Printf("Records Copied: %d\n", result.RecordsCopied)
	fmt.Printf("Records Deleted: %d\n", result.RecordsDeleted)
	fmt.Printf("Success: %v\n", result.Success)

	if len(result.Errors) > 0 {
		fmt.Printf("\nErrors:\n")
		for _, e := range result.Errors {
			fmt.Printf("  - %v\n", e)
		}
		return fmt.Errorf("archive completed with errors")
	}

	return nil
}
