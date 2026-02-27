package archiver

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
)

// ============================================================================
// Copy-Only Integration Test Setup Helpers
// ============================================================================

// setupCopyOnlyDBManager creates a database manager for copy-only tests
func setupCopyOnlyDBManager(t *testing.T, setup *IntegrationTestSetup) *database.Manager {
	var sourceCfg, destCfg DatabaseConfig
	found := 0
	for _, db := range setup.Config.Databases {
		if db.Name == "source" {
			sourceCfg = db
			found++
		}
		if db.Name == "destination" {
			destCfg = db
			found++
		}
	}
	if found != 2 {
		t.Fatal("Source and/or destination database config not found")
	}

	cfg := &config.Config{
		Source: config.DatabaseConfig{
			Host:     sourceCfg.Host,
			Port:     sourceCfg.Port,
			User:     sourceCfg.User,
			Password: sourceCfg.Password,
			Database: sourceCfg.Database,
			TLS:      "disable",
		},
		Destination: config.DatabaseConfig{
			Host:     destCfg.Host,
			Port:     destCfg.Port,
			User:     destCfg.User,
			Password: destCfg.Password,
			Database: destCfg.Database,
			TLS:      "disable",
		},
		Processing: config.ProcessingConfig{
			BatchSize:       5,
			BatchDeleteSize: 10,
			SleepSeconds:    0,
		},
		Verification: config.VerificationConfig{
			Method:           "count",
			SkipVerification: false,
		},
		Safety: config.SafetyConfig{
			DisableForeignKeyChecks: true,
		},
		Logging: config.LoggingConfig{
			Level:  "info",
			Format: "json",
		},
	}

	dbManager := database.NewManager(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := dbManager.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect database manager: %v", err)
	}

	return dbManager
}

// clearCopyOnlyDestination truncates all tables in destination for copy-only tests
func clearCopyOnlyDestination(t *testing.T, setup *IntegrationTestSetup) {
	t.Helper()
	destDB, ok := setup.GetDB("destination")
	if !ok {
		t.Fatal("Destination database not found in setup")
	}
	if _, err := destDB.Exec("SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		t.Logf("Warning: failed to disable FK checks on destination: %v", err)
	}
	for _, table := range []string{"order_payments", "order_items", "orders", "customers"} {
		if _, err := destDB.Exec(fmt.Sprintf("TRUNCATE TABLE %s", table)); err != nil {
			t.Logf("Warning: failed to truncate %s on destination: %v", table, err)
		}
	}
	// Clean up archiver_job and archiver_job_log tables for fresh test state
	if _, err := destDB.Exec("DELETE FROM archiver_job_log"); err != nil {
		t.Logf("Warning: failed to clear archiver_job_log: %v", err)
	}
	if _, err := destDB.Exec("DELETE FROM archiver_job"); err != nil {
		t.Logf("Warning: failed to clear archiver_job: %v", err)
	}
	if _, err := destDB.Exec("SET FOREIGN_KEY_CHECKS = 1"); err != nil {
		t.Logf("Warning: failed to re-enable FK checks on destination: %v", err)
	}
}

// seedCopyOnlyTestData inserts test data into source for copy-only tests
func seedCopyOnlyTestData(t *testing.T, db *sql.DB) {
	t.Helper()

	tables := []string{"order_payments", "order_items", "orders", "customers"}
	if _, err := db.Exec("SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		t.Logf("Warning: failed to disable FK checks: %v", err)
	}
	for _, table := range tables {
		if _, err := db.Exec(fmt.Sprintf("TRUNCATE TABLE %s", table)); err != nil {
			t.Logf("Warning: failed to truncate %s: %v", table, err)
		}
	}
	if _, err := db.Exec("SET FOREIGN_KEY_CHECKS = 1"); err != nil {
		t.Logf("Warning: failed to re-enable FK checks: %v", err)
	}

	queries := []string{
		`INSERT INTO customers (id, name, email, created_at) VALUES
			(1, 'Alice Johnson', 'alice@example.com', DATE_SUB(NOW(), INTERVAL 2 YEAR)),
			(2, 'Bob Smith', 'bob@example.com', DATE_SUB(NOW(), INTERVAL 1 YEAR)),
			(3, 'Carol Williams', 'carol@example.com', DATE_SUB(NOW(), INTERVAL 3 MONTH))`,

		`INSERT INTO orders (id, customer_id, total, status, created_at) VALUES
			(101, 1, 150.00, 'completed', DATE_SUB(NOW(), INTERVAL 2 YEAR)),
			(102, 1, 75.50, 'completed', DATE_SUB(NOW(), INTERVAL 700 DAY)),
			(103, 2, 320.00, 'completed', DATE_SUB(NOW(), INTERVAL 1 YEAR)),
			(104, 2, 45.00, 'completed', DATE_SUB(NOW(), INTERVAL 400 DAY)),
			(105, 3, 890.00, 'completed', DATE_SUB(NOW(), INTERVAL 2 MONTH))`,

		`INSERT INTO order_items (id, order_id, product, quantity, price) VALUES
			(1, 101, 'Widget A', 2, 50.00), (2, 101, 'Widget B', 1, 50.00),
			(3, 102, 'Gadget X', 1, 75.50),
			(4, 103, 'Premium Pack', 1, 320.00),
			(5, 104, 'Widget C', 3, 15.00),
			(6, 105, 'Deluxe Set', 1, 890.00)`,

		`INSERT INTO order_payments (id, order_id, amount, method, paid_at) VALUES
			(1, 101, 150.00, 'credit_card', DATE_SUB(NOW(), INTERVAL 2 YEAR)),
			(2, 102, 75.50, 'paypal', DATE_SUB(NOW(), INTERVAL 700 DAY)),
			(3, 103, 320.00, 'credit_card', DATE_SUB(NOW(), INTERVAL 1 YEAR)),
			(4, 104, 45.00, 'bank_transfer', DATE_SUB(NOW(), INTERVAL 400 DAY)),
			(5, 105, 890.00, 'credit_card', DATE_SUB(NOW(), INTERVAL 2 MONTH))`,
	}

	for i, query := range queries {
		if _, err := db.Exec(query); err != nil {
			t.Fatalf("Failed to execute seed query %d: %v", i+1, err)
		}
	}
}

// getTableRowCount returns the row count for a table
func getTableRowCount(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows in %s: %v", table, err)
	}
	return count
}

// ============================================================================
// Copy-Only Integration Tests
// ============================================================================

// TestCopyOnly_FullCycle_Integration tests complete copy-only workflow
func TestCopyOnly_FullCycle_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	// Clear destination and seed source with test data
	clearCopyOnlyDestination(t, setup)
	sourceDB, _ := setup.GetDB("source")
	seedCopyOnlyTestData(t, sourceDB)

	// Create orchestrator with real DB manager
	jobCfg := createCustomerOrderJobConfig()
	dbManager := setupCopyOnlyDBManager(t, setup)

	cfg := dbManager.GetConfig()
	orch, err := NewCopyOnlyOrchestrator(cfg, "test_copy_only_full", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewCopyOnlyOrchestrator failed: %v", err)
	}

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Execute copy-only (force=false, no prompt)
	result, err := orch.Execute(ctx, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Errorf("Expected successful execution, got errors: %v", result.Errors)
	}

	// Verify: source should still have all data (no deletion)
	sourceCount := getTableRowCount(t, sourceDB, "customers")
	if sourceCount != 3 {
		t.Errorf("Source should have 3 customers (no deletion), got %d", sourceCount)
	}

	// Verify: destination should have copied rows
	destDB, _ := setup.GetDB("destination")
	destCustomers := getTableRowCount(t, destDB, "customers")
	if destCustomers != 2 { // Alice and Bob match the where clause
		t.Errorf("Destination should have 2 customers, got %d", destCustomers)
	}
	destOrders := getTableRowCount(t, destDB, "orders")
	if destOrders != 4 {
		t.Errorf("Destination should have 4 orders, got %d", destOrders)
	}

	// Verify statistics
	if result.RecordsCopied == 0 {
		t.Error("Expected non-zero records copied")
	}
	if result.TablesCopied != 4 {
		t.Errorf("Expected 4 tables copied, got %d", result.TablesCopied)
	}
}

// TestCopyOnly_DestinationNotEmpty_Integration tests error when dest has data
func TestCopyOnly_DestinationNotEmpty_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	// Clear destination and seed source
	clearCopyOnlyDestination(t, setup)
	sourceDB, _ := setup.GetDB("source")
	seedCopyOnlyTestData(t, sourceDB)

	// Seed destination with some data to trigger the error
	destDB, _ := setup.GetDB("destination")
	_, err := destDB.Exec(`INSERT INTO customers (id, name, email, created_at) 
		VALUES (999, 'Test Customer', 'test@example.com', DATE_SUB(NOW(), INTERVAL 1 YEAR))`)
	if err != nil {
		t.Fatalf("Failed to seed destination: %v", err)
	}

	jobCfg := createCustomerOrderJobConfig()
	dbManager := setupCopyOnlyDBManager(t, setup)

	cfg := dbManager.GetConfig()
	orch, err := NewCopyOnlyOrchestrator(cfg, "test_dest_not_empty", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewCopyOnlyOrchestrator failed: %v", err)
	}

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Execute should fail because destination is not empty
	_, err = orch.Execute(ctx, false)
	if err == nil {
		t.Fatal("Expected error when destination not empty, got nil")
	}

	if !strings.Contains(err.Error(), "already contains data") {
		t.Errorf("Expected 'already contains data' error, got: %v", err)
	}
}

// TestCopyOnly_ConcurrentJobBlocked_Integration tests blocking when another job is running
func TestCopyOnly_ConcurrentJobBlocked_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	clearCopyOnlyDestination(t, setup)
	sourceDB, _ := setup.GetDB("source")
	seedCopyOnlyTestData(t, sourceDB)

	// Manually insert a running job record to simulate concurrent job
	destDB, _ := setup.GetDB("destination")
	resumeMgr, _ := NewResumeManager(destDB, nil)
	resumeMgr.InitializeTables(ctx)

	// Insert a fake running job on the same root table
	_, err := destDB.Exec(`
		INSERT INTO archiver_job (job_name, root_table, job_type, job_status) 
		VALUES ('concurrent_job', 'customers', 'archive', 1)`)
	if err != nil {
		t.Fatalf("Failed to insert fake job: %v", err)
	}
	defer func() {
		// Cleanup
		_, _ = destDB.Exec("DELETE FROM archiver_job WHERE job_name = 'concurrent_job'")
	}()

	jobCfg := createCustomerOrderJobConfig()
	dbManager := setupCopyOnlyDBManager(t, setup)

	cfg := dbManager.GetConfig()
	orch, err := NewCopyOnlyOrchestrator(cfg, "test_concurrent", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewCopyOnlyOrchestrator failed: %v", err)
	}

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Execute should fail due to concurrent job
	_, err = orch.Execute(ctx, false)
	if err == nil {
		t.Fatal("Expected error for concurrent job, got nil")
	}

	if !strings.Contains(err.Error(), "already running") {
		t.Errorf("Expected 'already running' error, got: %v", err)
	}
}

// TestCopyOnly_CrashRecovery_Integration tests resume after interruption
func TestCopyOnly_CrashRecovery_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, _ := SetupIntegrationTest(t)
	defer setup.Close()

	clearCopyOnlyDestination(t, setup)
	sourceDB, _ := setup.GetDB("source")
	seedCopyOnlyTestData(t, sourceDB)

	jobCfg := createCustomerOrderJobConfig()
	dbManager := setupCopyOnlyDBManager(t, setup)
	cfg := dbManager.GetConfig()

	// First run: process then cancel
	ctx1, cancel1 := context.WithCancel(context.Background())
	orch1, err := NewCopyOnlyOrchestrator(cfg, "test_copy_recovery", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewCopyOnlyOrchestrator failed: %v", err)
	}
	if err := orch1.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Cancel after short time to simulate crash
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel1()
	}()

	_, _ = orch1.Execute(ctx1, false) // Expect cancellation error

	// Second run: resume from checkpoint (need fresh dbManager to avoid state issues)
	ctx2 := context.Background()
	dbManager2 := setupCopyOnlyDBManager(t, setup)
	cfg2 := dbManager2.GetConfig()
	orch2, err := NewCopyOnlyOrchestrator(cfg2, "test_copy_recovery", jobCfg, dbManager2)
	if err != nil {
		t.Fatalf("NewCopyOnlyOrchestrator (resume) failed: %v", err)
	}
	if err := orch2.Initialize(); err != nil {
		t.Fatalf("Initialize (resume) failed: %v", err)
	}

	// Must use force=true on resume since destination may have partial data
	// Set up auto-confirm for force mode
	orch2.promptReader = bytes.NewReader([]byte("y\n"))
	result, err := orch2.Execute(ctx2, true)
	if err != nil {
		t.Fatalf("Execute (resume) failed: %v", err)
	}

	if !result.Success {
		t.Errorf("Expected successful resume, got errors: %v", result.Errors)
	}

	// Verify: destination should have all copied rows
	destDB, _ := setup.GetDB("destination")
	destCustomers := getTableRowCount(t, destDB, "customers")
	if destCustomers != 2 {
		t.Errorf("Destination should have 2 customers after resume, got %d", destCustomers)
	}

	// Verify: source still has all data
	sourceCount := getTableRowCount(t, sourceDB, "customers")
	if sourceCount != 3 {
		t.Errorf("Source should still have 3 customers, got %d", sourceCount)
	}
}

// TestCopyOnly_SkipVerification_Integration tests working with verification off
func TestCopyOnly_SkipVerification_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	clearCopyOnlyDestination(t, setup)
	sourceDB, _ := setup.GetDB("source")
	seedCopyOnlyTestData(t, sourceDB)

	jobCfg := createCustomerOrderJobConfig()
	dbManager := setupCopyOnlyDBManager(t, setup)
	cfg := dbManager.GetConfig()
	cfg.Verification.SkipVerification = true

	orch, err := NewCopyOnlyOrchestrator(cfg, "test_skip_verify", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewCopyOnlyOrchestrator failed: %v", err)
	}
	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	result, err := orch.Execute(ctx, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Errorf("Expected successful execution, got errors: %v", result.Errors)
	}

	// Verification should be skipped (0 verified records)
	if result.RecordsVerified != 0 {
		t.Errorf("Expected 0 records verified (skip verification), got %d", result.RecordsVerified)
	}

	// But records should still be copied
	if result.RecordsCopied == 0 {
		t.Error("Expected non-zero records copied")
	}

	// Verify destination has data
	destDB, _ := setup.GetDB("destination")
	destCustomers := getTableRowCount(t, destDB, "customers")
	if destCustomers != 2 {
		t.Errorf("Destination should have 2 customers, got %d", destCustomers)
	}
}

// TestCopyOnly_ForceMode_BypassesDuplicateCheck_Integration tests force mode bypasses empty check
func TestCopyOnly_ForceMode_BypassesDuplicateCheck_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	clearCopyOnlyDestination(t, setup)
	sourceDB, _ := setup.GetDB("source")
	seedCopyOnlyTestData(t, sourceDB)

	// Seed destination with some data
	destDB, _ := setup.GetDB("destination")
	_, err := destDB.Exec(`INSERT INTO customers (id, name, email, created_at) 
		VALUES (999, 'Existing Customer', 'existing@example.com', DATE_SUB(NOW(), INTERVAL 1 YEAR))`)
	if err != nil {
		t.Fatalf("Failed to seed destination: %v", err)
	}

	jobCfg := createCustomerOrderJobConfig()
	dbManager := setupCopyOnlyDBManager(t, setup)

	cfg := dbManager.GetConfig()
	orch, err := NewCopyOnlyOrchestrator(cfg, "test_force_bypass", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewCopyOnlyOrchestrator failed: %v", err)
	}

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Simulate user confirming with "y\n"
	orch.promptReader = bytes.NewReader([]byte("y\n"))

	// Execute with force=true - should bypass duplicate check
	result, err := orch.Execute(ctx, true)
	if err != nil {
		t.Fatalf("Execute with force failed: %v", err)
	}

	if !result.Success {
		t.Errorf("Expected successful execution with force, got errors: %v", result.Errors)
	}

	// Destination should now have both existing and copied data
	destCustomers := getTableRowCount(t, destDB, "customers")
	if destCustomers < 2 {
		t.Errorf("Expected at least 2 customers in destination (1 existing + copied), got %d", destCustomers)
	}
}

// TestCopyOnly_ForceMode_Cancelled_Integration tests force mode with user cancellation
func TestCopyOnly_ForceMode_Cancelled_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	clearCopyOnlyDestination(t, setup)
	sourceDB, _ := setup.GetDB("source")
	seedCopyOnlyTestData(t, sourceDB)

	jobCfg := createCustomerOrderJobConfig()
	dbManager := setupCopyOnlyDBManager(t, setup)

	cfg := dbManager.GetConfig()
	orch, err := NewCopyOnlyOrchestrator(cfg, "test_force_cancel", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewCopyOnlyOrchestrator failed: %v", err)
	}

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Simulate user cancelling with "n\n"
	orch.promptReader = bytes.NewReader([]byte("n\n"))

	// Execute with force=true but user cancels
	_, err = orch.Execute(ctx, true)
	if err == nil {
		t.Fatal("Expected error when user cancels, got nil")
	}

	if !strings.Contains(err.Error(), "cancelled") {
		t.Errorf("Expected 'cancelled' error, got: %v", err)
	}

	// Destination should be empty (no data copied)
	destDB, _ := setup.GetDB("destination")
	destCustomers := getTableRowCount(t, destDB, "customers")
	if destCustomers != 0 {
		t.Errorf("Expected 0 customers in destination after cancel, got %d", destCustomers)
	}

	// Clean up the abandoned job to prevent blocking other tests
	if _, err := destDB.Exec("DELETE FROM archiver_job WHERE job_name = 'test_force_cancel'"); err != nil {
		t.Logf("Warning: failed to clean up test job: %v", err)
	}
}

// TestCopyOnly_EmptyResultSet_Integration tests handling of no matching rows
func TestCopyOnly_EmptyResultSet_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	// Clear tables in both source and destination
	for _, dbName := range []string{"source", "destination"} {
		db, _ := setup.GetDB(dbName)
		if _, err := db.Exec("SET FOREIGN_KEY_CHECKS = 0"); err != nil {
			t.Logf("Warning: failed to disable FK checks on %s: %v", dbName, err)
		}
		for _, table := range []string{"order_payments", "order_items", "orders", "customers"} {
			if _, err := db.Exec(fmt.Sprintf("TRUNCATE TABLE %s", table)); err != nil {
				t.Logf("Warning: failed to truncate %s on %s: %v", table, dbName, err)
			}
		}
		if _, err := db.Exec("SET FOREIGN_KEY_CHECKS = 1"); err != nil {
			t.Logf("Warning: failed to re-enable FK checks on %s: %v", dbName, err)
		}
	}

	jobCfg := createCustomerOrderJobConfig()
	// Where clause that matches no rows
	jobCfg.Where = "created_at < '2020-01-01'"

	dbManager := setupCopyOnlyDBManager(t, setup)
	cfg := dbManager.GetConfig()

	orch, err := NewCopyOnlyOrchestrator(cfg, "test_empty_result", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewCopyOnlyOrchestrator failed: %v", err)
	}
	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	result, err := orch.Execute(ctx, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Errorf("Expected success with empty result, got errors: %v", result.Errors)
	}

	if result.RecordsCopied != 0 {
		t.Errorf("Expected 0 records copied, got %d", result.RecordsCopied)
	}

	// Verify: destination should have 0 rows
	destDB, _ := setup.GetDB("destination")
	destCustomers := getTableRowCount(t, destDB, "customers")
	if destCustomers != 0 {
		t.Errorf("Expected 0 customers in destination, got %d", destCustomers)
	}
}

// TestCopyOnly_ContextCancellation_Integration tests graceful shutdown
func TestCopyOnly_ContextCancellation_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, _ := SetupIntegrationTest(t)
	defer setup.Close()

	clearCopyOnlyDestination(t, setup)
	sourceDB, _ := setup.GetDB("source")
	seedLargeTestData(t, sourceDB)

	jobCfg := createCustomerOrderJobConfig()
	dbManager := setupCopyOnlyDBManager(t, setup)
	cfg := dbManager.GetConfig()

	orch, err := NewCopyOnlyOrchestrator(cfg, "test_cancellation", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewCopyOnlyOrchestrator failed: %v", err)
	}
	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	result, err := orch.Execute(ctx, false)

	// Expect cancellation error
	if err == nil {
		t.Error("Expected cancellation error")
	}

	if err != context.Canceled {
		t.Logf("Got error: %v (may be wrapped)", err)
	}

	if result != nil {
		t.Logf("Partial result: %d records copied before cancellation", result.RecordsCopied)
	}

	// Source should still have all data (no deletion happened)
	sourceCount := getTableRowCount(t, sourceDB, "customers")
	if sourceCount != 5000 {
		t.Errorf("Source should still have 5000 customers, got %d", sourceCount)
	}
}

// TestCopyOnly_ResumeTableInDestination_Integration verifies resume tables are in destination
func TestCopyOnly_ResumeTableInDestination_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	clearCopyOnlyDestination(t, setup)
	sourceDB, _ := setup.GetDB("source")
	seedCopyOnlyTestData(t, sourceDB)

	jobCfg := createCustomerOrderJobConfig()
	dbManager := setupCopyOnlyDBManager(t, setup)

	cfg := dbManager.GetConfig()
	orch, err := NewCopyOnlyOrchestrator(cfg, "test_resume_dest", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewCopyOnlyOrchestrator failed: %v", err)
	}

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	_, err = orch.Execute(ctx, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Verify: archiver_job table should exist in destination
	destDB, _ := setup.GetDB("destination")
	var tableName string
	err = destDB.QueryRow(`
		SELECT table_name FROM information_schema.tables 
		WHERE table_schema = DATABASE() AND table_name = 'archiver_job'
	`).Scan(&tableName)
	if err != nil {
		t.Errorf("archiver_job table should exist in destination: %v", err)
	}

	// Verify: job record should exist in destination
	var jobStatus int
	err = destDB.QueryRow(`
		SELECT job_status FROM archiver_job WHERE job_name = 'test_resume_dest'
	`).Scan(&jobStatus)
	if err != nil {
		t.Errorf("Job record should exist in destination: %v", err)
	}

	// Verify: job type should be 'copy-only'
	var jobType string
	err = destDB.QueryRow(`
		SELECT job_type FROM archiver_job WHERE job_name = 'test_resume_dest'
	`).Scan(&jobType)
	if err != nil {
		t.Errorf("Job type should be retrievable: %v", err)
	}
	if jobType != JobTypeCopyOnly {
		t.Errorf("Job type should be '%s', got '%s'", JobTypeCopyOnly, jobType)
	}
}
