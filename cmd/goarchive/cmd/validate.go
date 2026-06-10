package cmd

import (
	"context"
	"fmt"
	"sort"

	"github.com/dbsmedya/goarchive/internal/archiver"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/spf13/cobra"
)

var (
	validateForceTriggers bool
	validateJob           string
)

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
  goarchive validate --config archiver.yaml
  goarchive validate --job archive_old_orders`,
	RunE: runValidate,
}

func init() {
	rootCmd.AddCommand(validateCmd)
	validateCmd.Flags().BoolVar(&validateForceTriggers, "force-triggers", false, "Allow DELETE triggers (triggers will fire during delete)")
	validateCmd.Flags().StringVarP(&validateJob, "job", "j", "",
		"Validate only this job (default: validate all jobs)")
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
	cfg.ApplyOverrides(overrides.LogLevel, overrides.LogFormat, overrides.SkipVerify)
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	// Resolve which jobs to validate: a single job when --job is set,
	// otherwise every job in deterministic (sorted) order. Checked before
	// connecting so a mistyped --job fails fast without a database round trip.
	jobNames := cfg.ListJobs()
	sort.Strings(jobNames)
	if validateJob != "" {
		if _, exists := cfg.Jobs[validateJob]; !exists {
			return fmt.Errorf("job %q not found in %s", validateJob, configFile)
		}
		jobNames = []string{validateJob}
	}

	// Initialize logger
	log, err := logger.New(&cfg.Logging)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
	defer syncLogger(log)

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
	fmt.Printf("Config file:       %s\n", configFile)
	if validateJob != "" {
		fmt.Printf("Jobs to validate:  1 (%s)\n\n", validateJob)
	} else {
		fmt.Printf("Jobs to validate:  %d\n\n", len(jobNames))
	}

	if cfg.Safety.DisableForeignKeyChecks {
		fmt.Println("⚠️  WARNING: safety.disable_foreign_key_checks is ENABLED.")
		fmt.Println("   Destination inserts will skip FK constraint validation during copy.")
		fmt.Println("   This is an advanced option — only enable if you have verified the")
		fmt.Println("   copy order and understand the risk of inserting orphaned rows.")
		fmt.Println()
	}

	// Validate each job, collecting failures so the summary names every job
	// that did not pass.
	var failed []string
	total := len(jobNames)
	for i, jobName := range jobNames {
		jobCfgValue := cfg.Jobs[jobName]
		jobCfg := &jobCfgValue

		fmt.Printf("[%d/%d] Job: %s\n", i+1, total, jobName)
		fmt.Printf("   Root Table:  %s\n", jobCfg.RootTable)
		fmt.Printf("   Primary Key: %s\n", jobCfg.PrimaryKey)
		fmt.Printf("   Relations:   %d table(s)\n", len(jobCfg.Relations))

		if err := validateJobConfig(ctx, cfg, jobCfg, dbManager, log); err != nil {
			fmt.Printf("   ❌ FAILED: %v\n\n", err)
			failed = append(failed, jobName)
			continue
		}

		fmt.Printf("   ✅ PASSED\n\n")
	}

	fmt.Printf("=== Validation Summary ===\n")
	fmt.Printf("Passed: %d/%d\n", total-len(failed), total)
	if len(failed) > 0 {
		fmt.Printf("Failed: %d\n", len(failed))
		for _, name := range failed {
			fmt.Printf("   - %s\n", name)
		}
		return fmt.Errorf("validation failed for %d of %d job(s)", len(failed), total)
	}

	fmt.Println("✅ All jobs validated successfully")
	return nil
}

// validateJobConfig builds the dependency graph for a single job and runs the
// preflight checks against it, returning the first error encountered. Callers
// supply the job header/footer, so failures stay attributable per job.
func validateJobConfig(
	ctx context.Context,
	cfg *config.Config,
	jobCfg *config.JobConfig,
	dbManager *database.Manager,
	log *logger.Logger,
) error {
	builder := graph.NewBuilder(jobCfg)
	g, err := builder.Build()
	if err != nil {
		return fmt.Errorf("graph build failed: %w", err)
	}

	if g.HasCycle() {
		cycleInfo := g.DetectIncompleteProcessing()
		return fmt.Errorf("cycle detected: %d nodes in cycle", len(cycleInfo.UnprocessedNodes))
	}

	checker, err := archiver.NewPreflightChecker(dbManager.Source, cfg.Source.Database, g, log)
	if err != nil {
		return fmt.Errorf("failed to create preflight checker: %w", err)
	}
	if err := checker.ConfigureDestination(dbManager.Destination, cfg.Destination.Database, cfg.Destination.EffectiveJobSchema()); err != nil {
		return fmt.Errorf("failed to configure destination preflight checks: %w", err)
	}
	checker.SetVerification(jobCfg.GetJobVerification(cfg.Verification))

	if err := checker.RunAllChecks(ctx, validateForceTriggers); err != nil {
		return fmt.Errorf("preflight checks failed: %w", err)
	}
	return nil
}
