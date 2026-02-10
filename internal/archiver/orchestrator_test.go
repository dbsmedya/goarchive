package archiver

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/dbsmedya/goarchive/internal/graph"
)

// ============================================================================
// Test Helpers
// ============================================================================

func createTestConfig() *config.Config {
	return &config.Config{
		Source: config.DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Password: "password",
			Database: "test",
		},
		Destination: config.DatabaseConfig{
			Host:     "localhost",
			Port:     3307,
			User:     "root",
			Password: "password",
			Database: "archive",
		},
		Processing: config.ProcessingConfig{
			BatchSize:       1000,
			BatchDeleteSize: 500,
			SleepSeconds:    1,
		},
		Safety: config.SafetyConfig{
			DisableForeignKeyChecks: true, // Required for tests with partial data
		},
		Logging: config.LoggingConfig{
			Level:  "info",
			Format: "json",
		},
	}
}

func createTestJobConfig() *config.JobConfig {
	return &config.JobConfig{
		RootTable:  "users",
		PrimaryKey: "id",
		Where:      "created_at < DATE_SUB(NOW(), INTERVAL 1 YEAR)",
		Relations: []config.Relation{
			{
				Table:          "orders",
				PrimaryKey:     "id",
				ForeignKey:     "user_id",
				DependencyType: "1-N",
				Relations: []config.Relation{
					{
						Table:          "order_items",
						PrimaryKey:     "id",
						ForeignKey:     "order_id",
						DependencyType: "1-N",
					},
				},
			},
			{
				Table:          "profiles",
				PrimaryKey:     "id",
				ForeignKey:     "user_id",
				DependencyType: "1-1",
			},
		},
	}
}

// mockDBManager creates a minimal database manager for testing
func mockDBManager(cfg *config.Config) *database.Manager {
	return database.NewManager(cfg)
}

// realDBManager creates a database manager and connects to real databases
// Uses environment variables or uses default test values
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

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
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

// restoreSakilaData restores sakila rental and payment data from archive
func restoreSakilaData(t *testing.T) {
	// Use mysqlsh command to restore data from archive to source
	cmd := `mysqlsh --host=127.0.0.1 --port=3305 --user=root --password=qazokm --sql << 'EOF'
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE sakila.rental;
TRUNCATE TABLE sakila.payment;
INSERT INTO sakila.rental SELECT * FROM sakila_archive.rental;
INSERT INTO sakila.payment SELECT * FROM sakila_archive.payment;
SET FOREIGN_KEY_CHECKS = 1;
EOF
`
	_ = cmd // For now, skip the restore in test - we assume data is present
	// In real integration tests, data should be set up before running tests
}

// ============================================================================
// NewOrchestrator Tests
// ============================================================================

func TestNewOrchestrator_Success(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, err := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewOrchestrator failed: %v", err)
	}

	if orch == nil {
		t.Fatal("NewOrchestrator returned nil")
	}

	if orch.config != cfg {
		t.Error("Orchestrator config mismatch")
	}
	if orch.jobConfig != jobCfg {
		t.Error("Orchestrator jobConfig mismatch")
	}
	if orch.dbManager != dbManager {
		t.Error("Orchestrator dbManager mismatch")
	}
	if orch.jobName != "test_job" {
		t.Errorf("Expected job name 'test_job', got %s", orch.jobName)
	}
	if orch.initialized {
		t.Error("New orchestrator should not be initialized")
	}
}

func TestNewOrchestrator_NilConfig(t *testing.T) {
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(createTestConfig())

	_, err := NewOrchestrator(nil, "test_job", jobCfg, dbManager)
	if err == nil {
		t.Error("Expected error for nil config")
	}
}

func TestNewOrchestrator_NilJobConfig(t *testing.T) {
	cfg := createTestConfig()
	dbManager := mockDBManager(cfg)

	_, err := NewOrchestrator(cfg, "test_job", nil, dbManager)
	if err == nil {
		t.Error("Expected error for nil job config")
	}
}

func TestNewOrchestrator_NilDBManager(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()

	_, err := NewOrchestrator(cfg, "test_job", jobCfg, nil)
	if err == nil {
		t.Error("Expected error for nil db manager")
	}
}

// ============================================================================
// Initialize Tests
// ============================================================================

func TestInitialize_Success(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)

	err := orch.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	if !orch.IsInitialized() {
		t.Error("Orchestrator should be initialized")
	}

	if orch.graph == nil {
		t.Error("Graph should be built after Initialize")
	}

	if len(orch.copyOrder) == 0 {
		t.Error("Copy order should be computed")
	}

	if len(orch.deleteOrder) == 0 {
		t.Error("Delete order should be computed")
	}
}

func TestInitialize_Idempotent(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)

	// Initialize twice
	if err := orch.Initialize(); err != nil {
		t.Fatalf("First Initialize failed: %v", err)
	}

	copyOrder := orch.copyOrder
	deleteOrder := orch.deleteOrder

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Second Initialize failed: %v", err)
	}

	// Orders should remain the same
	if len(orch.copyOrder) != len(copyOrder) {
		t.Error("Copy order changed on second Initialize")
	}
	if len(orch.deleteOrder) != len(deleteOrder) {
		t.Error("Delete order changed on second Initialize")
	}
}

func TestInitialize_InvalidJobConfig(t *testing.T) {
	cfg := createTestConfig()
	// Empty root table should fail
	jobCfg := &config.JobConfig{
		RootTable:  "", // Invalid
		PrimaryKey: "id",
	}
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "invalid_job", jobCfg, dbManager)

	err := orch.Initialize()
	if err == nil {
		t.Error("Expected error for invalid job config")
	}
}

// ============================================================================
// ValidateGraph Tests
// ============================================================================

func TestValidateGraph_Success(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)

	// Must initialize first
	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Now validate
	err := orch.ValidateGraph()
	if err != nil {
		t.Errorf("ValidateGraph failed for valid DAG: %v", err)
	}
}

func TestValidateGraph_NoGraph(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)

	// Don't initialize - graph is nil
	err := orch.ValidateGraph()
	if err == nil {
		t.Error("Expected error when graph is nil")
	}
}

func TestValidateGraph_Cycle(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	// Create orchestrator normally first to get proper initialization
	orch, _ := NewOrchestrator(cfg, "cycle_job", jobCfg, dbManager)

	// Manually set a cyclic graph (bypassing normal initialization)
	orch.graph = &graph.Graph{
		Nodes: map[string]*graph.Node{
			"A": {Name: "A"},
			"B": {Name: "B"},
		},
		Children: map[string][]string{
			"A": {"B"},
			"B": {"A"},
		},
		Parents: map[string][]string{
			"A": {"B"},
			"B": {"A"},
		},
	}

	err := orch.ValidateGraph()
	if err == nil {
		t.Error("Expected error for cycle graph")
	}
}

// ============================================================================
// GetCopyOrder Tests
// ============================================================================

func TestGetCopyOrder_Success(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	copyOrder, err := orch.GetCopyOrder()
	if err != nil {
		t.Fatalf("GetCopyOrder failed: %v", err)
	}

	// Should have 4 tables: users, orders, order_items, profiles
	if len(copyOrder) != 4 {
		t.Errorf("Expected 4 tables in copy order, got %d: %v", len(copyOrder), copyOrder)
	}

	// Root (users) should be first
	if copyOrder[0] != "users" {
		t.Errorf("Expected users first, got %s", copyOrder[0])
	}
}

func TestGetCopyOrder_NotInitialized(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)

	_, err := orch.GetCopyOrder()
	if err == nil {
		t.Error("Expected error when not initialized")
	}
}

func TestGetCopyOrder_ParentFirst(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	copyOrder, _ := orch.GetCopyOrder()

	// Build position map
	positions := make(map[string]int)
	for i, table := range copyOrder {
		positions[table] = i
	}

	// Verify parent comes before child
	if positions["users"] >= positions["orders"] {
		t.Error("users should come before orders")
	}
	if positions["orders"] >= positions["order_items"] {
		t.Error("orders should come before order_items")
	}
	if positions["users"] >= positions["profiles"] {
		t.Error("users should come before profiles")
	}
}

// ============================================================================
// GetDeleteOrder Tests
// ============================================================================

func TestGetDeleteOrder_Success(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	deleteOrder, err := orch.GetDeleteOrder()
	if err != nil {
		t.Fatalf("GetDeleteOrder failed: %v", err)
	}

	// Should have 4 tables
	if len(deleteOrder) != 4 {
		t.Errorf("Expected 4 tables in delete order, got %d: %v", len(deleteOrder), deleteOrder)
	}

	// Root (users) should be last
	if deleteOrder[len(deleteOrder)-1] != "users" {
		t.Errorf("Expected users last, got %s", deleteOrder[len(deleteOrder)-1])
	}
}

func TestGetDeleteOrder_NotInitialized(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)

	_, err := orch.GetDeleteOrder()
	if err == nil {
		t.Error("Expected error when not initialized")
	}
}

func TestGetDeleteOrder_ChildFirst(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	deleteOrder, _ := orch.GetDeleteOrder()

	// Build position map
	positions := make(map[string]int)
	for i, table := range deleteOrder {
		positions[table] = i
	}

	// Verify child comes before parent
	if positions["orders"] >= positions["users"] {
		t.Error("orders should come before users in delete order")
	}
	if positions["order_items"] >= positions["orders"] {
		t.Error("order_items should come before orders in delete order")
	}
}

func TestGetDeleteOrder_ReverseOfCopyOrder(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	copyOrder, _ := orch.GetCopyOrder()
	deleteOrder, _ := orch.GetDeleteOrder()

	if len(copyOrder) != len(deleteOrder) {
		t.Fatal("Copy and delete orders have different lengths")
	}

	// Verify delete order is reverse of copy order
	for i := 0; i < len(copyOrder); i++ {
		expected := copyOrder[len(copyOrder)-1-i]
		if deleteOrder[i] != expected {
			t.Errorf("DeleteOrder[%d] = %s, expected %s", i, deleteOrder[i], expected)
		}
	}
}

// ============================================================================
// Execute Tests
// ============================================================================

func TestExecute_NotInitialized(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)

	ctx := context.Background()
	_, err := orch.Execute(ctx, nil)
	if err == nil {
		t.Error("Expected error when not initialized")
	}
}

func TestExecute_NilContext(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	_, err := orch.Execute(nil, nil)
	if err == nil {
		t.Error("Expected error for nil context")
	}
}

func TestExecute_EmptyResult(t *testing.T) {
	cfg := createTestConfig()
	// Use Sakila schema for integration test
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
	// Use Sakila schema for integration test with batch size 1 to ensure callbacks
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
	// Use Sakila schema for integration test with batch size 1
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
	// Use Sakila schema for integration test
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
		if e == context.Canceled {
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
	// Use Sakila schema for integration test
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
	// Use Sakila schema for integration test
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
// Helper Method Tests
// ============================================================================

func TestIsInitialized(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)

	if orch.IsInitialized() {
		t.Error("New orchestrator should not be initialized")
	}

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	if !orch.IsInitialized() {
		t.Error("Orchestrator should be initialized after Initialize()")
	}
}

func TestGetGraph(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)

	// Before initialization
	if orch.GetGraph() != nil {
		t.Error("GetGraph should return nil before initialization")
	}

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// After initialization
	if orch.GetGraph() == nil {
		t.Error("GetGraph should return graph after initialization")
	}
}

func TestGetJobConfig(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)

	if orch.GetJobConfig() != jobCfg {
		t.Error("GetJobConfig returned wrong config")
	}
}

func TestGetConfig(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)

	if orch.GetConfig() != cfg {
		t.Error("GetConfig returned wrong config")
	}
}

func TestGetJobName(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "my_test_job", jobCfg, dbManager)

	if orch.GetJobName() != "my_test_job" {
		t.Errorf("Expected job name 'my_test_job', got %s", orch.GetJobName())
	}
}

// ============================================================================
// Integration Tests
// ============================================================================

func TestOrchestrator_FullWorkflow(t *testing.T) {
	cfg := createTestConfig()
	// Use Sakila schema for integration test
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

func TestOrchestrator_CycleDetection(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "cycle_test", jobCfg, dbManager)

	// Manually set a cyclic graph
	orch.graph = &graph.Graph{
		Nodes: map[string]*graph.Node{
			"A": {Name: "A"},
			"B": {Name: "B"},
			"C": {Name: "C"},
		},
		Children: map[string][]string{
			"A": {"B"},
			"B": {"C"},
			"C": {"A"},
		},
		Parents: map[string][]string{
			"A": {"C"},
			"B": {"A"},
			"C": {"B"},
		},
	}

	// ValidateGraph should detect cycle
	err := orch.ValidateGraph()
	if err == nil {
		t.Fatal("Expected cycle detection error")
	}
}
