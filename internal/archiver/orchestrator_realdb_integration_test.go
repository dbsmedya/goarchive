//go:build integration
// +build integration

package archiver

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/internal/archiver/testsupport"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
)

// realDBManager creates a database manager and connects to real databases.
// Uses environment variables or falls back to default test values.
func realDBManager(t *testing.T) *database.Manager {
	cfg := &config.Config{
		Source: config.DatabaseConfig{
			Host:     getEnv("TEST_SOURCE_HOST", "127.0.0.1"),
			Port:     getEnvInt("TEST_SOURCE_PORT", 3305),
			User:     getEnv("TEST_SOURCE_USER", "root"),
			Password: getEnv("TEST_SOURCE_PASSWORD", "qazokm"),
			Database: getEnv("TEST_SOURCE_DB", "sakila"),
			TLS:      "disable",
		},
		Destination: config.DatabaseConfig{
			Host:     getEnv("TEST_DEST_HOST", "127.0.0.1"),
			Port:     getEnvInt("TEST_DEST_PORT", 3307),
			User:     getEnv("TEST_DEST_USER", "root"),
			Password: getEnv("TEST_DEST_PASSWORD", "qazokm"),
			Database: getEnv("TEST_DEST_DB", "sakila_archive"),
			TLS:      "disable",
		},
		Processing: config.ProcessingConfig{
			BatchSize:       100,
			BatchDeleteSize: 50,
			SleepSeconds:    0,
		},
		Safety: config.SafetyConfig{
			DisableForeignKeyChecks: true, // Required for integration tests with partial data
		},
		Logging: config.LoggingConfig{
			Level:  "error", // Reduce noise during tests
			Format: "json",
		},
	}

	dbManager := database.NewManager(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := dbManager.Connect(ctx); err != nil {
		t.Skipf("Database not available: %v", err)
	}

	return dbManager
}

func getEnvInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		var i int
		// Parse int from string manually
		for _, c := range v {
			if c >= '0' && c <= '9' {
				i = i*10 + int(c-'0')
			}
		}
		if i > 0 {
			return i
		}
	}
	return defaultVal
}

// ============================================================================
// Execute Tests (require real MySQL)
// ============================================================================

func TestExecute_EmptyResult(t *testing.T) {
	cfg := createTestConfig()
	// Use sample database schema for integration test
	jobCfg := &config.JobConfig{
		RootTable:  "rental",
		PrimaryKey: "rental_id",
		Where:      "rental_date < '2005-01-01'", // No rentals before 2005
		Relations: []config.Relation{
			{
				Table:          "payment",
				PrimaryKey:     "payment_id",
				ForeignKey:     "rental_id",
				DependencyType: "1-N",
			},
		},
	}
	dbManager := realDBManager(t)
	testsupport.CleanupArchiverState(t, dbManager.Destination, "test_job_empty")

	orch, _ := NewOrchestrator(cfg, "test_job_empty", jobCfg, dbManager)

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	ctx := context.Background()
	result, err := orch.Execute(ctx, nil)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if result.JobName != "test_job_empty" {
		t.Errorf("Expected job name 'test_job_empty', got %s", result.JobName)
	}

	if !result.Success {
		t.Error("Expected success for empty execution")
	}
}

func TestExecute_CheckpointCallback(t *testing.T) {
	cfg := createTestConfig()
	// Use sample database schema for integration test with batch size 1 to ensure callbacks
	cfg.Processing.BatchSize = 1
	jobCfg := &config.JobConfig{
		RootTable:  "rental",
		PrimaryKey: "rental_id",
		Where:      "rental_id <= 5", // Small range for testing
		Relations: []config.Relation{
			{
				Table:          "payment",
				PrimaryKey:     "payment_id",
				ForeignKey:     "rental_id",
				DependencyType: "1-N",
			},
		},
	}
	dbManager := realDBManager(t)
	testsupport.CleanupArchiverState(t, dbManager.Destination, "test_job_checkpoint_2")

	orch, _ := NewOrchestrator(cfg, "test_job_checkpoint_2", jobCfg, dbManager)

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	var checkpointCalled int
	var lastStatus string
	checkpoint := func(rootPK interface{}, status string) error {
		checkpointCalled++
		lastStatus = status
		return nil
	}

	ctx := context.Background()
	result, _ := orch.Execute(ctx, checkpoint)

	// If no data to process, checkpoint won't be called - that's OK
	if result.RecordsCopied == 0 {
		t.Skip("No data to process, skipping checkpoint callback test")
	}

	if checkpointCalled == 0 {
		t.Error("Checkpoint callback was not called")
	}

	if lastStatus != "completed" {
		t.Errorf("Expected final status 'completed', got %s", lastStatus)
	}
}

func TestExecute_CheckpointCallbackError(t *testing.T) {
	cfg := createTestConfig()
	// Use sample database schema for integration test with batch size 1
	cfg.Processing.BatchSize = 1
	jobCfg := &config.JobConfig{
		RootTable:  "rental",
		PrimaryKey: "rental_id",
		Where:      "rental_id <= 5", // Small range for testing
		Relations: []config.Relation{
			{
				Table:          "payment",
				PrimaryKey:     "payment_id",
				ForeignKey:     "rental_id",
				DependencyType: "1-N",
			},
		},
	}
	dbManager := realDBManager(t)
	testsupport.CleanupArchiverState(t, dbManager.Destination, "test_job_ckpt_err_3")

	orch, _ := NewOrchestrator(cfg, "test_job_ckpt_err_3", jobCfg, dbManager)

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	checkpointError := errors.New("checkpoint failed")
	checkpoint := func(rootPK interface{}, status string) error {
		return checkpointError
	}

	ctx := context.Background()
	result, err := orch.Execute(ctx, checkpoint)
	if err != nil {
		t.Logf("Execute returned error (expected): %v", err)
	}

	// Skip if no data to process
	if result != nil && result.RecordsCopied == 0 {
		t.Skip("No data to process, skipping checkpoint error test")
	}

	// Should have recorded the error or returned an error
	if result != nil && len(result.Errors) == 0 && err == nil {
		t.Error("Expected errors in result or returned error")
	}
}

func TestExecute_ContextCancellation(t *testing.T) {
	cfg := createTestConfig()
	// Use sample database schema for integration test
	jobCfg := &config.JobConfig{
		RootTable:  "rental",
		PrimaryKey: "rental_id",
		Where:      "rental_date < '2005-08-01'",
		Relations: []config.Relation{
			{
				Table:          "payment",
				PrimaryKey:     "payment_id",
				ForeignKey:     "rental_id",
				DependencyType: "1-N",
			},
		},
	}
	dbManager := realDBManager(t)
	testsupport.CleanupArchiverState(t, dbManager.Destination, "test_job_cancel")

	orch, _ := NewOrchestrator(cfg, "test_job_cancel", jobCfg, dbManager)

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	result, err := orch.Execute(ctx, nil)

	// If result is nil, the test can't proceed
	if result == nil {
		t.Skip("Result is nil - likely no data to process or context cancelled before processing")
	}

	if err == nil {
		t.Error("Expected error for cancelled context")
	}

	if result.Success {
		t.Error("Expected failure for cancelled context")
	}

	foundCancelError := false
	for _, e := range result.Errors {
		if errors.Is(e, context.Canceled) {
			foundCancelError = true
			break
		}
	}
	if !foundCancelError {
		t.Error("Expected context.Canceled error in result")
	}
}

func TestExecute_ArchiveResultStats(t *testing.T) {
	cfg := createTestConfig()
	// Use sample database schema for integration test
	jobCfg := &config.JobConfig{
		RootTable:  "rental",
		PrimaryKey: "rental_id",
		Where:      "rental_date < '2005-08-01'",
		Relations: []config.Relation{
			{
				Table:          "payment",
				PrimaryKey:     "payment_id",
				ForeignKey:     "rental_id",
				DependencyType: "1-N",
			},
		},
	}
	dbManager := realDBManager(t)
	testsupport.CleanupArchiverState(t, dbManager.Destination, "test_job_stats")

	orch, _ := NewOrchestrator(cfg, "test_job_stats", jobCfg, dbManager)

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	ctx := context.Background()
	result, err := orch.Execute(ctx, nil)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Verify result fields
	if result.JobName != "test_job_stats" {
		t.Errorf("JobName mismatch: expected 'test_job_stats', got %s", result.JobName)
	}

	if result.StartedAt.IsZero() {
		t.Error("StartedAt should be set")
	}

	if result.CompletedAt.IsZero() {
		t.Error("CompletedAt should be set")
	}

	if result.Duration <= 0 {
		t.Error("Duration should be positive")
	}

	// Verify CompletedAt is after StartedAt
	if !result.CompletedAt.After(result.StartedAt) && !result.CompletedAt.Equal(result.StartedAt) {
		t.Error("CompletedAt should be >= StartedAt")
	}

	// Only 2 tables in this test: rental and payment
	if result.TablesCopied != 2 {
		t.Errorf("TablesCopied: expected 2, got %d", result.TablesCopied)
	}

	if result.TablesDeleted != 2 {
		t.Errorf("TablesDeleted: expected 2, got %d", result.TablesDeleted)
	}

	if result.Errors == nil {
		t.Error("Errors slice should not be nil")
	}

	if !result.Success {
		t.Error("Success should be true")
	}
}

func TestExecute_DurationCalculation(t *testing.T) {
	cfg := createTestConfig()
	// Use sample database schema for integration test
	jobCfg := &config.JobConfig{
		RootTable:  "rental",
		PrimaryKey: "rental_id",
		Where:      "rental_date < '2005-08-01'",
		Relations: []config.Relation{
			{
				Table:          "payment",
				PrimaryKey:     "payment_id",
				ForeignKey:     "rental_id",
				DependencyType: "1-N",
			},
		},
	}
	dbManager := realDBManager(t)
	testsupport.CleanupArchiverState(t, dbManager.Destination, "test_job_duration")

	orch, _ := NewOrchestrator(cfg, "test_job_duration", jobCfg, dbManager)

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	start := time.Now()
	ctx := context.Background()
	result, err := orch.Execute(ctx, nil)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Duration should be approximately elapsed time (within tolerance)
	diff := result.Duration - elapsed
	if diff < 0 {
		diff = -diff
	}
	if diff > time.Second {
		t.Errorf("Duration calculation seems off: got %v, expected ~%v", result.Duration, elapsed)
	}
}

// ============================================================================
// Full Workflow Integration Test (requires real MySQL)
// ============================================================================

func TestOrchestrator_FullWorkflow(t *testing.T) {
	cfg := createTestConfig()
	// Use sample database schema for integration test
	jobCfg := &config.JobConfig{
		RootTable:  "rental",
		PrimaryKey: "rental_id",
		Where:      "rental_date < '2005-08-01'",
		Relations: []config.Relation{
			{
				Table:          "payment",
				PrimaryKey:     "payment_id",
				ForeignKey:     "rental_id",
				DependencyType: "1-N",
			},
		},
	}
	dbManager := realDBManager(t)
	testsupport.CleanupArchiverState(t, dbManager.Destination, "integration_test_sakila")

	// Create orchestrator
	orch, err := NewOrchestrator(cfg, "integration_test_sakila", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewOrchestrator failed: %v", err)
	}

	// Initialize
	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Get orders
	copyOrder, err := orch.GetCopyOrder()
	if err != nil {
		t.Fatalf("GetCopyOrder failed: %v", err)
	}

	deleteOrder, err := orch.GetDeleteOrder()
	if err != nil {
		t.Fatalf("GetDeleteOrder failed: %v", err)
	}

	// Verify orders are correct
	if len(copyOrder) != len(deleteOrder) {
		t.Error("Copy and delete orders have different lengths")
	}

	// Execute
	ctx := context.Background()
	result, err := orch.Execute(ctx, nil)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected successful execution")
	}

	if result.TablesCopied != len(copyOrder) {
		t.Error("TablesCopied doesn't match copy order length")
	}
}
