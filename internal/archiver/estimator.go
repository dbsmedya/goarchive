package archiver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/dbsmedya/goarchive/internal/sqlutil"
)

// EstimateResult holds dry-run estimation results.
type EstimateResult struct {
	RootTable        string
	RootCount        int64
	ChildCounts      map[string]int64 // table -> estimated count
	EstimatedBatches int64
	BatchSize        int
	Config           *config.Config
	JobConfig        *config.JobConfig
}

// Estimator estimates row counts and batch sizes for dry-run mode.
type Estimator struct {
	db          *sql.DB
	cfg         *config.Config
	jobCfg      *config.JobConfig
	graph       *graph.Graph
	logger      *logger.Logger
	processing  config.ProcessingConfig   // Effective processing config (job-specific or global)
	verification config.VerificationConfig // Effective verification config (job-specific or global)
}

// NewEstimator creates a new estimator.
func NewEstimator(db *sql.DB, cfg *config.Config, jobCfg *config.JobConfig, g *graph.Graph, log *logger.Logger) *Estimator {
	if log == nil {
		log = logger.NewDefault()
	}
	return &Estimator{
		db:           db,
		cfg:          cfg,
		jobCfg:       jobCfg,
		graph:        g,
		logger:       log,
		processing:   jobCfg.GetJobProcessing(cfg.Processing),
		verification: jobCfg.GetJobVerification(cfg.Verification),
	}
}

// Estimate calculates row counts and batch estimates.
//
// GA-P4-F4-T1: Estimate root count
// GA-P4-F4-T2: Estimate child counts
// GA-P4-F4-T3: Calculate batch count
func (e *Estimator) Estimate(ctx context.Context) (*EstimateResult, error) {
	result := &EstimateResult{
		RootTable:   e.jobCfg.RootTable,
		ChildCounts: make(map[string]int64),
		BatchSize:   e.processing.BatchSize,
		Config:      e.cfg,
		JobConfig:   e.jobCfg,
	}

	// GA-P4-F4-T1: Estimate root count
	rootCount, err := e.estimateRootCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to estimate root count: %w", err)
	}
	result.RootCount = rootCount

	// GA-P4-F4-T2: Estimate child counts
	for _, table := range e.graph.AllNodes() {
		if table == e.jobCfg.RootTable {
			continue // Already counted
		}
		count, err := e.estimateTableCount(ctx, table)
		if err != nil {
			e.logger.Warnf("Failed to estimate count for %s: %v", table, err)
			result.ChildCounts[table] = 0
		} else {
			result.ChildCounts[table] = count
		}
	}

	// GA-P4-F4-T3: Calculate batch count
	if rootCount > 0 && e.processing.BatchSize > 0 {
		result.EstimatedBatches = (rootCount + int64(e.processing.BatchSize) - 1) / int64(e.processing.BatchSize)
	}

	return result, nil
}

// estimateRootCount counts matching root records.
func (e *Estimator) estimateRootCount(ctx context.Context) (int64, error) {
	where := e.jobCfg.Where
	if where == "" {
		where = "1=1"
	}

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", sqlutil.QuoteIdentifier(e.jobCfg.RootTable), where)

	var count int64
	if err := e.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count root table: %w", err)
	}

	return count, nil
}

// estimateTableCount estimates total rows in a child table.
// For simplicity, returns total count without filtering (actual archiving will discover exact subset).
func (e *Estimator) estimateTableCount(ctx context.Context, table string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", sqlutil.QuoteIdentifier(table))

	var count int64
	if err := e.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, err
	}

	return count, nil
}

// DisplayExecutionPlan prints the dry-run execution plan.
//
// GA-P4-F4-T4: Display execution plan
// GA-P4-F4-T5: Display config summary
func (e *Estimator) DisplayExecutionPlan(result *EstimateResult) {
	fmt.Printf("\n=== Dry-Run Execution Plan ===\n\n")

	// Root table estimate
	fmt.Printf("Root Table: %s\n", result.RootTable)
	fmt.Printf("  Matching rows: %d\n", result.RootCount)
	fmt.Printf("  Batch size: %d\n", result.BatchSize)
	fmt.Printf("  Estimated batches: %d\n\n", result.EstimatedBatches)

	// Copy order
	copyOrder, _ := e.graph.CopyOrder()
	fmt.Printf("Copy Order (parent-first):\n")
	for i, table := range copyOrder {
		count := result.RootCount
		if table != result.RootTable {
			count = result.ChildCounts[table]
		}
		fmt.Printf("  %d. %s (~%d rows)\n", i+1, table, count)
	}
	fmt.Println()

	// Delete order
	deleteOrder, _ := e.graph.DeleteOrder()
	fmt.Printf("Delete Order (child-first):\n")
	for i, table := range deleteOrder {
		count := result.RootCount
		if table != result.RootTable {
			count = result.ChildCounts[table]
		}
		fmt.Printf("  %d. %s (~%d rows)\n", i+1, table, count)
	}
	fmt.Println()

	// Config summary (show job-specific or global)
	fmt.Printf("Configuration Summary:\n")
	fmt.Printf("  Batch size: %d", e.processing.BatchSize)
	if e.jobCfg.Processing != nil && e.jobCfg.Processing.BatchSize > 0 {
		fmt.Print(" (job-specific)")
	}
	fmt.Println()
	fmt.Printf("  Batch delete size: %d", e.processing.BatchDeleteSize)
	if e.jobCfg.Processing != nil && e.jobCfg.Processing.BatchDeleteSize > 0 {
		fmt.Print(" (job-specific)")
	}
	fmt.Println()
	fmt.Printf("  Sleep between batches: %.1fs", e.processing.SleepSeconds)
	if e.jobCfg.Processing != nil && e.jobCfg.Processing.SleepSeconds > 0 {
		fmt.Print(" (job-specific)")
	}
	fmt.Println()
	fmt.Printf("  Verification method: %s", e.verification.Method)
	if e.jobCfg.Verification != nil && e.jobCfg.Verification.Method != "" {
		fmt.Print(" (job-specific)")
	}
	fmt.Println()
	fmt.Printf("  Skip verification: %v", e.verification.SkipVerification)
	if e.jobCfg.Verification != nil {
		fmt.Print(" (job-specific)")
	}
	fmt.Println()
	fmt.Printf("  Foreign key checks: %v\n", !e.cfg.Safety.DisableForeignKeyChecks)

	if result.Config.Replica.Enabled {
		fmt.Printf("  Replication lag monitoring: enabled\n")
		fmt.Printf("    Max lag: %ds\n", result.Config.Safety.LagThreshold)
		fmt.Printf("    Check interval: %ds\n", result.Config.Safety.CheckInterval)
	} else {
		fmt.Printf("  Replication lag monitoring: disabled\n")
	}

	fmt.Println("\n=== End of Dry-Run ===")
	fmt.Println("\nℹ️  No data was modified. Use 'archive' command to execute.")
}
