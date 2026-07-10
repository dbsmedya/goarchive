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
	copyOnlyJob                   string
	copyOnlyForce                 bool
	copyOnlySkipValidatePreflight bool
)

var copyOnlyCmd = &cobra.Command{
	Use:   "copy-only",
	Short: "Copy data to destination without deleting from source",
	Long: `Copy-only copies matching records from source to destination database,
verifies the copied data, and never deletes from source.

Default mode runs non-interactively and logs:
  INFO: Source data will NOT be deleted

Force mode (--force) bypasses duplicate preflight checks.`,
	RunE: runCopyOnly,
}

func init() {
	copyOnlyCmd.Flags().StringVarP(&copyOnlyJob, "job", "j", "",
		"Job name from configuration file (required)")
	_ = copyOnlyCmd.MarkFlagRequired("job")
	copyOnlyCmd.Flags().BoolVar(&copyOnlyForce, "force", false,
		"Proceed past advisory lock contention only when the lock holder's heartbeat is stale (indicating a crashed prior instance). Also bypasses destination duplicate preflight checks after confirmation.")
	copyOnlyCmd.Flags().BoolVar(&copyOnlySkipValidatePreflight, "skip-validate-preflight", false,
		"Skip preflight checks before this run (DANGEROUS - see docs)")
	rootCmd.AddCommand(copyOnlyCmd)
}

func runCopyOnly(cmd *cobra.Command, args []string) error {
	cfg, err := config.Load(GetConfigFile())
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	overrides := GetCLIOverrides()
	cfg.ApplyOverrides(overrides.LogLevel, overrides.LogFormat, overrides.SkipVerify)
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	// Get job config after applying overrides so CLI flags (e.g. --skip-verify) are visible.
	jobCfgValue, exists := cfg.Jobs[copyOnlyJob]
	if !exists {
		return fmt.Errorf("job %q not found in configuration", copyOnlyJob)
	}
	jobCfg := &jobCfgValue

	log, err := newJobLogger(cfg, jobCfg, copyOnlyJob)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
	defer syncLogger(log)

	log.Info("Source data will NOT be deleted")

	// Two-phase graceful shutdown: first Ctrl-C finishes the in-flight batch and
	// stops at the boundary (no pending rows); second Ctrl-C cancels the work
	// context, unwinding cleanly so the advisory lock is released. A third Ctrl-C
	// hard-terminates.
	ctx, stopCh := database.SetupGracefulShutdown(
		func(_ os.Signal) {
			log.Warn("Received shutdown signal - finishing current batch, then stopping (Ctrl-C again to abort now)...")
		},
		func(_ os.Signal) {
			log.Error("Received second shutdown signal - aborting in-flight work")
			syncLogger(log)
		},
	)

	dbManager := database.NewManager(cfg)
	if err := dbManager.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to databases: %w", err)
	}
	defer func() {
		if err := dbManager.Close(); err != nil {
			log.Errorf("Failed to close database connections: %v", err)
		}
	}()
	if err := dbManager.Ping(ctx); err != nil {
		return fmt.Errorf("database connection failed: %w", err)
	}
	if err := runRuntimePreflight(ctx, cfg, jobCfg, dbManager, log, "copy-only", jobCfg.GetJobVerification(cfg.Verification),
		archiver.PreflightProfileNonDestructive, false, false, copyOnlySkipValidatePreflight); err != nil {
		return err
	}

	orch, err := archiver.NewCopyOnlyOrchestrator(cfg, copyOnlyJob, jobCfg, dbManager)
	if err != nil {
		return fmt.Errorf("failed to create copy-only orchestrator: %w", err)
	}
	orch.SetLogger(log)
	if err := orch.Initialize(); err != nil {
		return fmt.Errorf("copy-only orchestrator initialization failed: %w", err)
	}
	orch.SetStopChannel(stopCh)

	result, err := orch.Execute(ctx, copyOnlyForce)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return fmt.Errorf("copy-only operation cancelled: %w", err)
		}
		return fmt.Errorf("copy-only operation failed: %w", err)
	}

	// Log the structured summary (reaches file outputs), then print for the console
	log.Infow("Copy-only complete",
		"duration", result.Duration,
		"tables_copied", result.TablesCopied,
		"records_copied", result.RecordsCopied,
		"success", result.Success,
		"errors", len(result.Errors),
	)

	fmt.Printf("\n=== Copy-Only Complete ===\n")
	fmt.Printf("Job: %s\n", result.JobName)
	fmt.Printf("Duration: %s\n", result.Duration)
	fmt.Printf("Tables Copied: %d\n", result.TablesCopied)
	fmt.Printf("Records Copied: %d\n", result.RecordsCopied)
	fmt.Printf("Success: %v\n", result.Success)
	if len(result.Errors) > 0 {
		fmt.Printf("\nErrors:\n")
		for _, e := range result.Errors {
			fmt.Printf("  - %v\n", e)
		}
		return fmt.Errorf("copy-only completed with errors")
	}

	return nil
}
