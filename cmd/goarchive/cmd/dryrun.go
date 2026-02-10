package cmd

import (
	"context"
	"fmt"

	"github.com/dbsmedya/goarchive/internal/archiver"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/spf13/cobra"
)

var dryrunJob string

var dryrunCmd = &cobra.Command{
	Use:   "dry-run",
	Short: "Simulate archive execution without making changes",
	Long: `Dry-run simulates the archive process and reports what would happen
without making any actual changes to the databases.

The dry-run shows:
  - Estimated row counts for root and child tables
  - Number of batches that would be processed
  - Configuration summary

Example:
  goarchive dry-run --config archiver.yaml --job archive_old_orders`,
	RunE: runDryrun,
}

func init() {
	dryrunCmd.Flags().StringVarP(&dryrunJob, "job", "j", "",
		"Job name from configuration file (required)")
	dryrunCmd.MarkFlagRequired("job")

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

	// Get job config
	jobCfgValue, exists := cfg.Jobs[dryrunJob]
	if !exists {
		return fmt.Errorf("job '%s' not found in configuration", dryrunJob)
	}
	jobCfg := &jobCfgValue

	// Initialize logger
	log, err := logger.New(&cfg.Logging)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}

	// Create database manager (source only for dry-run)
	dbManager := database.NewManager(cfg)

	// Setup context
	ctx := context.Background()

	// Connect to databases
	if err := dbManager.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to databases: %w", err)
	}
	defer dbManager.Close()

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

	// Create estimator
	estimator := archiver.NewEstimator(dbManager.Source, cfg, jobCfg, g, log)

	// Run estimation
	result, err := estimator.Estimate(ctx)
	if err != nil {
		return fmt.Errorf("estimation failed: %w", err)
	}

	// Display execution plan
	estimator.DisplayExecutionPlan(result)

	return nil
}
