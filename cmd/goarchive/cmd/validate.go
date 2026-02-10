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

var validateForceTriggers bool

var validateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate configuration and run preflight checks",
	Long: `Validate checks the configuration file and runs preflight checks
against the database to ensure safe execution.

Checks performed:
  - Configuration syntax and required fields
  - Database connectivity (source, destination, replica)
  - Table existence and InnoDB engine
  - Foreign key index verification
  - Foreign key coverage (all FK constraints must be covered by relations)
  - DELETE trigger detection
  - CASCADE rule warnings

Example:
  goarchive validate --config archiver.yaml`,
	RunE: runValidate,
}

func init() {
	rootCmd.AddCommand(validateCmd)
	validateCmd.Flags().BoolVar(&validateForceTriggers, "force-triggers", false, "Allow DELETE triggers (triggers will fire during delete)")
}

func runValidate(cmd *cobra.Command, args []string) error {
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

	// Initialize logger
	log, err := logger.New(&cfg.Logging)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}

	log.Info("Starting validation checks...")

	// Create database manager
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

	// Test connections
	if err := dbManager.Ping(ctx); err != nil {
		return fmt.Errorf("database connection failed: %w", err)
	}

	fmt.Printf("\n=== Configuration Validation ===\n")
	fmt.Printf("Config file: %s\n", configFile)
	fmt.Printf("Jobs found: %d\n\n", len(cfg.Jobs))

	// Validate each job
	hasErrors := false
	for jobName, jobCfgValue := range cfg.Jobs {
		jobCfg := &jobCfgValue
		fmt.Printf("--- Job: %s ---\n", jobName)
		fmt.Printf("Root table: %s\n", jobCfg.RootTable)
		fmt.Printf("Relations: %d\n", len(jobCfg.Relations))

		// Build graph
		builder := graph.NewBuilder(jobCfg)
		g, err := builder.Build()
		if err != nil {
			fmt.Printf("❌ Graph build failed: %v\n\n", err)
			hasErrors = true
			continue
		}

		// Validate no cycles
		if g.HasCycle() {
			cycleInfo := g.DetectIncompleteProcessing()
			fmt.Printf("❌ Cycle detected: %d nodes in cycle\n\n", len(cycleInfo.UnprocessedNodes))
			hasErrors = true
			continue
		}

		// Create preflight checker
		checker, err := archiver.NewPreflightChecker(
			dbManager.Source,
			cfg.Source.Database,
			g,
			log,
		)
		if err != nil {
			fmt.Printf("❌ Failed to create preflight checker: %v\n\n", err)
			hasErrors = true
			continue
		}

		// Run all checks
		if err := checker.RunAllChecks(ctx, validateForceTriggers); err != nil {
			fmt.Printf("❌ Preflight checks failed: %v\n\n", err)
			hasErrors = true
			continue
		}

		fmt.Printf("✅ All checks passed\n\n")
	}

	if hasErrors {
		return fmt.Errorf("validation failed for one or more jobs")
	}

	fmt.Println("=== Validation Complete ===")
	fmt.Println("✅ All jobs validated successfully")
	return nil
}
