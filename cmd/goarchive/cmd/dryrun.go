package cmd

import (
	"context"
	"fmt"

	"github.com/dbsmedya/goarchive/internal/archiver"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/spf13/cobra"
)

var dryrunJob string

var dryrunCmd = &cobra.Command{
	Use:   "dry-run",
	Short: "Simulate archive execution without making changes",
	Long: `Dry-run simulates the archive process and reports what would happen
without making any actual changes to the databases.

The dry-run:
  - Runs non-destructive preflight checks (schema, charset, grants)
  - Shows the job WHERE clause and estimated row counts (root and children,
    filtered through the relation chain)
  - Shows the number of batches that would be processed
  - Validates batch_size against destination payload limits (rolled back)

Recommended operator workflow: validate -> dry-run -> archive.

Example:
  goarchive dry-run --config archiver.yaml --job archive_old_orders`,
	RunE: runDryrun,
}

func init() {
	dryrunCmd.Flags().StringVarP(&dryrunJob, "job", "j", "",
		"Job name from configuration file (required)")
	_ = dryrunCmd.MarkFlagRequired("job") // Config-time error, cannot fail

	rootCmd.AddCommand(dryrunCmd)
}

func runDryrun(cmd *cobra.Command, args []string) error {
	configFile := GetConfigFile()

	// Load configuration
	cfg, err := config.Load(configFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Apply CLI overrides
	overrides := GetCLIOverrides()
	cfg.ApplyOverrides(overrides.LogLevel, overrides.LogFormat,
		overrides.BatchSize, overrides.BatchDeleteSize,
		overrides.SleepSeconds, overrides.SkipVerify)
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	// Get job config
	jobCfgValue, exists := cfg.Jobs[dryrunJob]
	if !exists {
		return fmt.Errorf("job '%s' not found in configuration", dryrunJob)
	}
	jobCfg := &jobCfgValue

	// Initialize logger
	log, err := newJobLogger(cfg, jobCfg, dryrunJob)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
	defer syncLogger(log)

	// Create database manager (source for estimation, destination for payload validation)
	dbManager := database.NewManager(cfg)

	// Setup context
	ctx := context.Background()

	// Connect to databases
	if err := dbManager.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to databases: %w", err)
	}
	defer func() {
		if err := dbManager.Close(); err != nil {
			log.Errorf("Failed to close database connections: %v", err)
		}
	}()

	// Test source connection
	if err := dbManager.Ping(ctx); err != nil {
		return fmt.Errorf("source database connection failed: %w", err)
	}

	// Build graph
	builder := graph.NewBuilder(jobCfg)
	g, err := builder.Build()
	if err != nil {
		return fmt.Errorf("failed to build dependency graph: %w", err)
	}

	// Validate no cycles
	if g.HasCycle() {
		return fmt.Errorf("dependency cycle detected in graph")
	}

	// Dry-run is the operator's go/no-go step: run the non-destructive
	// preflight profile so schema/charset/grants problems surface here,
	// not at archive time. Workflow: validate -> dry-run -> archive.
	verification := jobCfg.GetJobVerification(cfg.Verification)
	if err := runRuntimePreflight(ctx, cfg, jobCfg, dbManager, log, "dry-run",
		verification, archiver.PreflightProfileNonDestructive, false, false); err != nil {
		return err
	}

	// Create estimator
	estimator := archiver.NewEstimator(dbManager.Source, cfg, jobCfg, g, log)

	// Run estimation
	result, err := estimator.Estimate(ctx)
	if err != nil {
		return fmt.Errorf("estimation failed: %w", err)
	}

	// Display execution plan
	estimator.DisplayExecutionPlan(result)

	// Validate that the chosen batch_size fits destination limits (rolled back).
	jobProcessing := cfg.GetJobProcessing(dryrunJob)
	validator := archiver.NewPayloadValidator(
		dbManager.Source, dbManager.Destination, g, jobCfg,
		cfg.Safety, jobProcessing.BatchSize, log,
	)
	fmt.Println("\nValidating batch_size payload limits (no data is persisted)...")
	if err := validator.Validate(ctx); err != nil {
		return fmt.Errorf("batch_size payload validation failed: %w", err)
	}
	fmt.Println("batch_size payload validation passed.")

	return nil
}
