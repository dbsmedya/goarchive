package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/dbsmedya/goarchive/internal/archiver"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/dbsmedya/goarchive/internal/lock"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/spf13/cobra"
)

var (
	archiveJob   string
	archiveForce bool
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
	archiveCmd.MarkFlagRequired("job")

	archiveCmd.Flags().BoolVar(&archiveForce, "force", false,
		"Force execution even if job lock cannot be acquired (use with caution)")

	rootCmd.AddCommand(archiveCmd)
}

func runArchive(cmd *cobra.Command, args []string) error {
	configFile := GetConfigFile()

	// Load configuration
	cfg, err := config.Load(configFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Get job config first (before applying overrides)
	jobCfgValue, exists := cfg.Jobs[archiveJob]
	if !exists {
		return fmt.Errorf("job '%s' not found in configuration", archiveJob)
	}
	// Use pointer to job config
	jobCfg := &jobCfgValue

	// Apply CLI overrides (to global config for logging, and get effective processing config)
	overrides := GetCLIOverrides()
	cfg.ApplyOverrides(overrides.LogLevel, overrides.LogFormat,
		overrides.BatchSize, overrides.BatchDeleteSize,
		overrides.SleepSeconds, overrides.SkipVerify)

	// Initialize logger
	log, err := logger.New(&cfg.Logging)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}

	log.Infow("Starting archive operation",
		"job", archiveJob,
		"config", configFile,
	)

	// Create database manager
	dbManager := database.NewManager(cfg)

	// Setup context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to databases
	if err := dbManager.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to databases: %w", err)
	}
	defer dbManager.Close()

	// Test connections
	if err := dbManager.Ping(ctx); err != nil {
		return fmt.Errorf("database connection failed: %w", err)
	}

	// Acquire advisory lock to prevent concurrent job execution
	if !archiveForce {
		jobLock := lock.NewJobLock(dbManager.Source, archiveJob)
		if err := jobLock.AcquireOrFail(ctx); err != nil {
			if errors.Is(err, lock.ErrLockTimeout) {
				return fmt.Errorf("job '%s' is already running on another instance (use --force to override)", archiveJob)
			}
			return fmt.Errorf("failed to acquire job lock: %w", err)
		}
		defer jobLock.ReleaseLock(context.Background())
		log.Infow("Acquired advisory lock for job", "job", archiveJob)
	} else {
		log.Warnw("Skipping advisory lock acquisition (--force flag used)", "job", archiveJob)
	}

	// Create orchestrator
	orch, err := archiver.NewOrchestrator(cfg, archiveJob, jobCfg, dbManager)
	if err != nil {
		return fmt.Errorf("failed to create orchestrator: %w", err)
	}

	// Initialize (build graph, validate)
	if err := orch.Initialize(); err != nil {
		return fmt.Errorf("orchestrator initialization failed: %w", err)
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Warn("Received shutdown signal - completing current batch...")
		cancel()
	}()

	// Execute archive operation
	result, err := orch.Execute(ctx, nil)
	if err != nil {
		if err == context.Canceled {
			log.Warn("Archive operation cancelled by user")
			return nil
		}
		return fmt.Errorf("archive operation failed: %w", err)
	}

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
