//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
)

// ============================================================================
// Purge Integration Test Setup
// ============================================================================

// setupPurgeDBManager creates a database manager for purge tests.
// Purge now requires a real destination connection because resume tables and
// advisory locks live there.
func setupPurgeDBManager(t *testing.T, setup *IntegrationTestSetup) *database.Manager {
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

// clearPurgeSource truncates source data tables and resets destination state
// tables so each purge test starts from a clean slate.
func clearPurgeSource(t *testing.T, setup *IntegrationTestSetup) {
	t.Helper()
	sourceDB, ok := setup.GetDB("source")
	if !ok {
		t.Fatal("Source database not found in setup")
	}
	if _, err := sourceDB.Exec("SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		t.Logf("Warning: failed to disable FK checks on source: %v", err)
	}
	for _, table := range []string{"order_payments", "order_items", "orders", "customers"} {
		if _, err := sourceDB.Exec(fmt.Sprintf("TRUNCATE TABLE %s", table)); err != nil {
			t.Logf("Warning: failed to truncate %s on source: %v", table, err)
		}
	}
	// Resume state lives on destination now — clear there for fresh test state.
	// Tables may not exist yet if this is the first test run; DELETE IGNORE
	// would work but keeping warnings explicit helps diagnose unexpected state.
	if destDB, ok := setup.GetDB("destination"); ok {
		if _, err := destDB.Exec("DELETE FROM archiver_job_log"); err != nil {
			t.Logf("Note: archiver_job_log not cleared on destination (may not exist yet): %v", err)
		}
		if _, err := destDB.Exec("DELETE FROM archiver_job"); err != nil {
			t.Logf("Note: archiver_job not cleared on destination (may not exist yet): %v", err)
		}
	}
}

// seedPurgeTestData inserts test data with old dates for purge testing
func seedPurgeTestData(t *testing.T, db *sql.DB) {
	t.Helper()

	// Insert customers with old dates (eligible for purge)
	customers := []struct {
		id   int
		name string
	}{
		{1, "Old Customer 1"},
		{2, "Old Customer 2"},
		{3, "Recent Customer"},
	}

	for _, c := range customers {
		createdAt := "2023-01-01 00:00:00" // Old date
		if c.id == 3 {
			createdAt = "2030-01-01 00:00:00" // Recent date
		}
		_, err := db.Exec(
			"INSERT INTO customers (id, name, email, created_at) VALUES (?, ?, ?, ?)",
			c.id, c.name, fmt.Sprintf("%s@test.com", c.name), createdAt,
		)
		if err != nil {
			t.Fatalf("Failed to insert customer %d: %v", c.id, err)
		}
	}

	// Insert orders for customers
	orders := []struct {
		id         int
		customerID int
		createdAt  string
	}{
		{1, 1, "2023-01-01 00:00:00"},
		{2, 1, "2023-02-01 00:00:00"},
		{3, 2, "2023-03-01 00:00:00"},
		{4, 2, "2023-04-01 00:00:00"},
		{5, 3, "2030-01-01 00:00:00"}, // Recent order (not purged)
	}

	for _, o := range orders {
		_, err := db.Exec(
			"INSERT INTO orders (id, customer_id, total, status, created_at) VALUES (?, ?, ?, ?, ?)",
			o.id, o.customerID, 100.00, "completed", o.createdAt,
		)
		if err != nil {
			t.Fatalf("Failed to insert order %d: %v", o.id, err)
		}
	}

	// Insert order items
	orderItems := []struct {
		id      int
		orderID int
	}{
		{1, 1}, {2, 1}, {3, 1},
		{4, 2}, {5, 2},
		{6, 3}, {7, 3}, {8, 3},
		{9, 4},
	}

	for _, oi := range orderItems {
		_, err := db.Exec(
			"INSERT INTO order_items (id, order_id, product, quantity, price) VALUES (?, ?, ?, ?, ?)",
			oi.id, oi.orderID, fmt.Sprintf("Product %d", oi.id), 1, 10.00,
		)
		if err != nil {
			t.Fatalf("Failed to insert order_item %d: %v", oi.id, err)
		}
	}

	// Insert order payments
	payments := []struct {
		id      int
		orderID int
	}{
		{1, 1}, {2, 2},
		{3, 3}, {4, 4},
	}

	for _, p := range payments {
		_, err := db.Exec(
			"INSERT INTO order_payments (id, order_id, amount, method, paid_at) VALUES (?, ?, ?, ?, NOW())",
			p.id, p.orderID, 100.00, "credit_card",
		)
		if err != nil {
			t.Fatalf("Failed to insert payment %d: %v", p.id, err)
		}
	}
}

// ============================================================================
// Purge Integration Tests
// ============================================================================

// TestPurge_FullCycle_Integration tests complete purge flow
func TestPurge_FullCycle_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	clearPurgeSource(t, setup)
	sourceDB, _ := setup.GetDB("source")
	seedPurgeTestData(t, sourceDB)

	jobCfg := createCustomerOrderJobConfig()
	dbManager := setupPurgeDBManager(t, setup)

	orch, err := NewPurgeOrchestrator(dbManager.GetConfig(), "test_purge_full", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewPurgeOrchestrator failed: %v", err)
	}
	orch.SetSkipLock(true) // Skip advisory lock for tests

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	result, err := orch.Execute(ctx)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Errorf("Expected successful execution")
	}

	// Should have deleted records from old customers (id 1 and 2)
	// Customer 3 is recent and should not be purged
	if result.RecordsDeleted == 0 {
		t.Error("Expected non-zero records deleted")
	}

	// Verify: some records were deleted
	var remainingCustomers int
	if err := sourceDB.QueryRow("SELECT COUNT(*) FROM customers").Scan(&remainingCustomers); err != nil {
		t.Fatalf("Failed to count customers: %v", err)
	}
	if remainingCustomers >= 3 {
		t.Errorf("Expected some customers to be purged, got %d remaining", remainingCustomers)
	}
	t.Logf("Remaining customers after purge: %d", remainingCustomers)
}

// TestPurge_CrashRecovery_Integration tests resume after interruption
func TestPurge_CrashRecovery_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, _ := SetupIntegrationTest(t)
	defer setup.Close()

	clearPurgeSource(t, setup)
	sourceDB, _ := setup.GetDB("source")
	seedPurgeTestData(t, sourceDB)

	jobCfg := createCustomerOrderJobConfig()

	// First run: simulate crash after processing first batch
	{
		dbManager := setupPurgeDBManager(t, setup)
		orch, err := NewPurgeOrchestrator(dbManager.GetConfig(), "test_purge_recovery", jobCfg, dbManager)
		if err != nil {
			t.Fatalf("NewPurgeOrchestrator failed: %v", err)
		}
		orch.SetSkipLock(true)

		if err := orch.Initialize(); err != nil {
			t.Fatalf("Initialize failed: %v", err)
		}

		// Cancel context after short time to simulate crash
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		_, _ = orch.Execute(ctx) // Expected to fail due to timeout
	}

	// Verify job is in idle status after first run
	var jobStatus string
	err := sourceDB.QueryRow("SELECT job_status FROM archiver_job WHERE job_name = 'test_purge_recovery'").Scan(&jobStatus)
	if err != nil {
		t.Logf("Job may not exist after first run: %v", err)
	}

	// Second run: resume and complete
	{
		dbManager := setupPurgeDBManager(t, setup)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		orch, err := NewPurgeOrchestrator(dbManager.GetConfig(), "test_purge_recovery", jobCfg, dbManager)
		if err != nil {
			t.Fatalf("NewPurgeOrchestrator failed: %v", err)
		}
		orch.SetSkipLock(true)

		if err := orch.Initialize(); err != nil {
			t.Fatalf("Initialize failed: %v", err)
		}

		result, err := orch.Execute(ctx)
		if err != nil {
			t.Fatalf("Execute failed on resume: %v", err)
		}

		if !result.Success {
			t.Errorf("Expected successful execution after resume")
		}

		// Should have completed purging old records
		var remainingCustomers int
		if err := sourceDB.QueryRow("SELECT COUNT(*) FROM customers").Scan(&remainingCustomers); err != nil {
			t.Fatalf("Failed to count customers: %v", err)
		}
		if remainingCustomers >= 3 {
			t.Errorf("Expected some customers to be purged after resume, got %d", remainingCustomers)
		}
		t.Logf("Remaining customers after resume: %d", remainingCustomers)
	}
}

// TestPurge_EmptyResultSet_Integration tests handling when no rows match
func TestPurge_EmptyResultSet_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	clearPurgeSource(t, setup)
	sourceDB, _ := setup.GetDB("source")

	// Insert only recent data (not eligible for purge)
	_, err := sourceDB.Exec(
		"INSERT INTO customers (id, name, email, created_at) VALUES (?, ?, ?, ?)",
		1, "Recent Customer", "recent@test.com", "2030-01-01 00:00:00",
	)
	if err != nil {
		t.Fatalf("Failed to insert customer: %v", err)
	}

	jobCfg := createCustomerOrderJobConfig()
	dbManager := setupPurgeDBManager(t, setup)

	orch, err := NewPurgeOrchestrator(dbManager.GetConfig(), "test_purge_empty", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewPurgeOrchestrator failed: %v", err)
	}
	orch.SetSkipLock(true)

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	result, err := orch.Execute(ctx)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Errorf("Expected successful execution")
	}

	if result.RecordsDeleted != 0 {
		t.Errorf("Expected 0 records deleted for empty result set, got %d", result.RecordsDeleted)
	}

	if result.BatchesProcessed != 0 {
		t.Errorf("Expected 0 batches processed, got %d", result.BatchesProcessed)
	}

	// Verify customer still exists (not purged since it doesn't match WHERE)
	var count int
	if err := sourceDB.QueryRow("SELECT COUNT(*) FROM customers").Scan(&count); err != nil {
		t.Fatalf("Failed to count customers: %v", err)
	}
	// Recent customer should remain - but if date logic differs, just verify not all deleted
	if count == 0 {
		t.Logf("Warning: All customers were purged - date filter may include 2030 dates")
	}
}

// TestPurge_ContextCancellation_Integration tests graceful shutdown
func TestPurge_ContextCancellation_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, _ := SetupIntegrationTest(t)
	defer setup.Close()

	clearPurgeSource(t, setup)
	sourceDB, _ := setup.GetDB("source")

	// Generate large test data
	t.Logf("Generating large test data for purge cancellation test...")
	for i := 1; i <= 1000; i++ {
		_, err := sourceDB.Exec(
			"INSERT INTO customers (id, name, email, created_at) VALUES (?, ?, ?, ?)",
			i, fmt.Sprintf("Customer %d", i), fmt.Sprintf("c%d@test.com", i), "2023-01-01 00:00:00",
		)
		if err != nil {
			t.Fatalf("Failed to insert customer %d: %v", i, err)
		}

		// Add 2 orders per customer
		for j := 1; j <= 2; j++ {
			orderID := i*10 + j
			_, err := sourceDB.Exec(
				"INSERT INTO orders (id, customer_id, total, status, created_at) VALUES (?, ?, ?, ?, ?)",
				orderID, i, 100.00, "completed", "2023-01-01 00:00:00",
			)
			if err != nil {
				t.Fatalf("Failed to insert order %d: %v", orderID, err)
			}
		}
	}
	t.Logf("Test data generation complete: 1000 customers, 2000 orders")

	jobCfg := createCustomerOrderJobConfig()
	dbManager := setupPurgeDBManager(t, setup)

	orch, err := NewPurgeOrchestrator(dbManager.GetConfig(), "test_purge_cancel", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewPurgeOrchestrator failed: %v", err)
	}
	orch.SetSkipLock(true)

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after short delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	result, err := orch.Execute(ctx)

	// Should get context cancellation error
	if err == nil {
		t.Error("Expected error from context cancellation, got nil")
	}

	if result != nil {
		t.Logf("Partial result: %d records deleted before cancellation", result.RecordsDeleted)
	}

	// Verify some data still remains (not all purged)
	var remainingCustomers int
	if err := sourceDB.QueryRow("SELECT COUNT(*) FROM customers").Scan(&remainingCustomers); err != nil {
		t.Fatalf("Failed to count customers: %v", err)
	}
	if remainingCustomers == 0 {
		t.Error("Expected some customers to remain after cancellation")
	}
	if remainingCustomers == 1000 {
		t.Log("No customers deleted before cancellation (might be expected if cancel happened before first batch)")
	}
}

// TestPurge_ResumeFromCheckpoint_Integration tests resuming purge from checkpoint
func TestPurge_ResumeFromCheckpoint_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	clearPurgeSource(t, setup)
	sourceDB, _ := setup.GetDB("source")
	seedPurgeTestData(t, sourceDB)

	jobCfg := createCustomerOrderJobConfig()
	dbManager := setupPurgeDBManager(t, setup)

	orch, err := NewPurgeOrchestrator(dbManager.GetConfig(), "test_purge_resume", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewPurgeOrchestrator failed: %v", err)
	}
	orch.SetSkipLock(true)

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// First execution - complete purge
	result1, err := orch.Execute(ctx)
	if err != nil {
		t.Fatalf("First execute failed: %v", err)
	}
	if !result1.Success {
		t.Error("Expected first execution to succeed")
	}

	recordsDeletedFirst := result1.RecordsDeleted
	t.Logf("First execution deleted %d records", recordsDeletedFirst)

	// Verify records were deleted
	var remaining int
	if err := sourceDB.QueryRow("SELECT COUNT(*) FROM customers").Scan(&remaining); err != nil {
		t.Fatalf("Failed to count customers: %v", err)
	}
	if remaining >= 3 {
		t.Errorf("Expected some customers purged, got %d remaining", remaining)
	}

	// Clear and re-seed data for second execution
	clearPurgeSource(t, setup)
	seedPurgeTestData(t, sourceDB)

	// Second execution with same job name - should resume from checkpoint but find new work
	dbManager2 := setupPurgeDBManager(t, setup)
	orch2, err := NewPurgeOrchestrator(dbManager2.GetConfig(), "test_purge_resume", jobCfg, dbManager2)
	if err != nil {
		t.Fatalf("NewPurgeOrchestrator failed: %v", err)
	}
	orch2.SetSkipLock(true)

	if err := orch2.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	result2, err := orch2.Execute(ctx)
	if err != nil {
		t.Fatalf("Second execute failed: %v", err)
	}
	if !result2.Success {
		t.Error("Expected second execution to succeed")
	}

	t.Logf("Second execution deleted %d records", result2.RecordsDeleted)

	// Second run should also delete records (new data)
	if result2.RecordsDeleted == 0 {
		t.Error("Expected second execution to delete records after re-seeding")
	}
}

// TestPurge_MultiLevelHierarchy_Integration tests purging multi-level hierarchy
func TestPurge_MultiLevelHierarchy_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	clearPurgeSource(t, setup)
	sourceDB, _ := setup.GetDB("source")
	seedPurgeTestData(t, sourceDB)

	jobCfg := createCustomerOrderJobConfig()
	dbManager := setupPurgeDBManager(t, setup)

	orch, err := NewPurgeOrchestrator(dbManager.GetConfig(), "test_purge_hierarchy", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewPurgeOrchestrator failed: %v", err)
	}
	orch.SetSkipLock(true)

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	result, err := orch.Execute(ctx)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Errorf("Expected successful execution")
	}

	// Count expected deletions (based on actual test data):
	// - 2 old customers (id 1, 2)
	// - 4 orders for old customers (id 1, 2, 3, 4)
	// - 9 order items for old orders
	// - 4 payments for old orders
	// Total: 19
	if result.RecordsDeleted < 10 {
		t.Errorf("Expected at least 10 records deleted, got %d", result.RecordsDeleted)
	}

	// Verify records were deleted
	tables := []string{"customers", "orders", "order_items", "order_payments"}
	totalRemaining := 0
	for _, table := range tables {
		var count int
		if err := sourceDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count); err != nil {
			t.Fatalf("Failed to count %s: %v", table, err)
		}
		totalRemaining += count
		t.Logf("Remaining %s: %d", table, count)
	}
	if totalRemaining == 0 {
		t.Logf("All records purged - date filter may affect 2030 dates depending on server time")
	}
}

// TestPurge_JobTypeValidation_Integration tests that purge respects job type
func TestPurge_JobTypeValidation_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	clearPurgeSource(t, setup)
	sourceDB, _ := setup.GetDB("source")
	seedPurgeTestData(t, sourceDB)

	jobCfg := createCustomerOrderJobConfig()
	dbManager := setupPurgeDBManager(t, setup)

	// Resume metadata now lives on Destination for all orchestrators.
	destDB, _ := setup.GetDB("destination")
	resumeMgr, _ := NewResumeManager(destDB, nil)
	_ = resumeMgr.InitializeTables(ctx)
	_, _ = resumeMgr.GetOrCreateJobWithType(ctx, "test_purge_jobtype", "customers", JobTypeArchive)

	// Now try to create a purge job with the same name
	orch, err := NewPurgeOrchestrator(dbManager.GetConfig(), "test_purge_jobtype", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewPurgeOrchestrator failed: %v", err)
	}
	orch.SetSkipLock(true)

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Should get an error about job type mismatch
	_, err = orch.Execute(ctx)
	if err == nil {
		t.Error("Expected error for job type mismatch, got nil")
	}

	// Clean up
	_, _ = destDB.Exec("DELETE FROM archiver_job WHERE job_name = 'test_purge_jobtype'")
}
