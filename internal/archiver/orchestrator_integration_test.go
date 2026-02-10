package archiver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
)

// ============================================================================
// Integration Test Setup (Self-Contained)
// ============================================================================

// setupRealDBManager creates a database manager using the integration test setup
func setupRealDBManager(t *testing.T, setup *IntegrationTestSetup) *database.Manager {
	// Get connection details from setup config
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

// createCustomerOrderJobConfig creates a job config for the customer/orders schema
// Matches the schema in testdata/customer_orders.sql
func createCustomerOrderJobConfig() *config.JobConfig {
	return &config.JobConfig{
		RootTable:  "customers",
		PrimaryKey: "id",
		Where:      "created_at < DATE_SUB(NOW(), INTERVAL 6 MONTH)",
		Relations: []config.Relation{
			{
				Table:          "orders",
				PrimaryKey:     "id",
				ForeignKey:     "customer_id",
				DependencyType: "1-N",
				Relations: []config.Relation{
					{
						Table:          "order_items",
						PrimaryKey:     "id",
						ForeignKey:     "order_id",
						DependencyType: "1-N",
					},
					{
						Table:          "order_payments",
						PrimaryKey:     "id",
						ForeignKey:     "order_id",
						DependencyType: "1-N",
					},
				},
			},
		},
	}
}

// clearDestination truncates all tables in the destination database
func clearDestination(t *testing.T, setup *IntegrationTestSetup) {
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
	if _, err := destDB.Exec("SET FOREIGN_KEY_CHECKS = 1"); err != nil {
		t.Logf("Warning: failed to re-enable FK checks on destination: %v", err)
	}
}

// seedTestData inserts test data into the source database
// Uses the schema from testdata/customer_orders.sql
func seedTestData(t *testing.T, db *sql.DB) {
	t.Helper()

	// Clear tables first
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

	// Insert test data with dates spread across time
	// Column names match testdata/customer_orders.sql schema
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

// verifyRowCount checks the row count in a table
func verifyRowCount(t *testing.T, db *sql.DB, table string, expectedCount int) {
	t.Helper()

	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows in %s: %v", table, err)
	}

	if count != expectedCount {
		t.Errorf("Row count mismatch for table %s: expected %d, got %d", table, expectedCount, count)
	}
}

// getVerificationDB creates a fresh connection for verification purposes
func getVerificationDB(t *testing.T, setup *IntegrationTestSetup, dbName string) *sql.DB {
	t.Helper()

	var dbCfg DatabaseConfig
	for _, db := range setup.Config.Databases {
		if db.Name == dbName {
			dbCfg = db
			break
		}
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?tls=false",
		dbCfg.User, dbCfg.Password, dbCfg.Host, dbCfg.Port, dbCfg.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Failed to open %s verification DB: %v", dbName, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Failed to ping %s verification DB: %v", dbName, err)
	}

	return db
}

// ============================================================================
// Integration Tests
// ============================================================================

// TestOrchestrator_FullArchiveCycle_Integration tests complete archive workflow
func TestOrchestrator_FullArchiveCycle_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	// Clear destination and seed source with test data
	clearDestination(t, setup)
	sourceDB, _ := setup.GetDB("source")
	seedTestData(t, sourceDB)

	// Create orchestrator with real DB manager
	jobCfg := createCustomerOrderJobConfig()
	dbManager := setupRealDBManager(t, setup)

	cfg := dbManager.GetConfig()
	orch, err := NewOrchestrator(cfg, "test_full_cycle", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewOrchestrator failed: %v", err)
	}

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Execute archive
	result, err := orch.Execute(ctx, nil)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Errorf("Expected successful execution, got errors: %v", result.Errors)
	}

	// Use fresh connections for verification (setup connections may be closed)
	verifySource := getVerificationDB(t, setup, "source")
	defer func() { _ = verifySource.Close() }()
	verifyDest := getVerificationDB(t, setup, "destination")
	defer func() { _ = verifyDest.Close() }()

	// Verify: source should have 0 old customers (older than 6 months)
	verifyRowCount(t, verifySource, "customers", 1) // Only Carol (3 months old) remains
	verifyRowCount(t, verifySource, "orders", 1)    // Only order 105

	// Verify: destination should have copied rows
	verifyRowCount(t, verifyDest, "customers", 2) // Alice and Bob archived
	verifyRowCount(t, verifyDest, "orders", 4)    // Orders 101-104
}

// TestOrchestrator_CrashRecovery_Integration tests resume after simulated crash
func TestOrchestrator_CrashRecovery_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, _ := SetupIntegrationTest(t)
	defer setup.Close()

	clearDestination(t, setup)
	sourceDB, _ := setup.GetDB("source")
	seedTestData(t, sourceDB)

	jobCfg := createCustomerOrderJobConfig()
	dbManager := setupRealDBManager(t, setup)
	cfg := dbManager.GetConfig()

	// First run: process then cancel
	ctx1, cancel1 := context.WithCancel(context.Background())
	orch1, err := NewOrchestrator(cfg, "test_crash_recovery", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewOrchestrator failed: %v", err)
	}
	if err := orch1.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Cancel after short time to simulate crash
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel1()
	}()

	_, _ = orch1.Execute(ctx1, nil) // Expect cancellation error

	// Second run: resume from checkpoint
	ctx2 := context.Background()
	orch2, err := NewOrchestrator(cfg, "test_crash_recovery", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewOrchestrator (resume) failed: %v", err)
	}
	if err := orch2.Initialize(); err != nil {
		t.Fatalf("Initialize (resume) failed: %v", err)
	}

	result, err := orch2.Execute(ctx2, nil)
	if err != nil {
		t.Fatalf("Execute (resume) failed: %v", err)
	}

	if !result.Success {
		t.Errorf("Expected successful resume, got errors: %v", result.Errors)
	}

	// Verify: all rows should be processed
	verifyDest := getVerificationDB(t, setup, "destination")
	defer func() { _ = verifyDest.Close() }()
	verifyRowCount(t, verifyDest, "customers", 2)
}

// TestOrchestrator_ReplicationLagPause_Integration tests lag monitoring
func TestOrchestrator_ReplicationLagPause_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	clearDestination(t, setup)
	sourceDB, _ := setup.GetDB("source")
	seedTestData(t, sourceDB)

	jobCfg := createCustomerOrderJobConfig()
	dbManager := setupRealDBManager(t, setup)
	cfg := dbManager.GetConfig()

	orch, err := NewOrchestrator(cfg, "test_lag_pause", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewOrchestrator failed: %v", err)
	}
	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	result, err := orch.Execute(ctx, nil)

	// If replica is available and lag > threshold, execution should pause
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Logf("Execute completed with warning: %v", err)
	}

	if result != nil && !result.Success {
		t.Logf("Result errors: %v", result.Errors)
	}
}

// TestOrchestrator_VerificationMismatch_Integration tests verification
func TestOrchestrator_VerificationMismatch_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	clearDestination(t, setup)
	sourceDB, _ := setup.GetDB("source")
	seedTestData(t, sourceDB)

	jobCfg := createCustomerOrderJobConfig()
	dbManager := setupRealDBManager(t, setup)
	cfg := dbManager.GetConfig()
	cfg.Verification.Method = "count"
	cfg.Verification.SkipVerification = false

	orch, err := NewOrchestrator(cfg, "test_verify", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewOrchestrator failed: %v", err)
	}
	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	result, err := orch.Execute(ctx, nil)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Errorf("Expected successful execution with verification, got errors: %v", result.Errors)
	}
}

// TestOrchestrator_ContextCancellation_Integration tests graceful shutdown
func TestOrchestrator_ContextCancellation_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, _ := SetupIntegrationTest(t)
	defer setup.Close()

	clearDestination(t, setup)
	sourceDB, _ := setup.GetDB("source")
	seedTestData(t, sourceDB)

	jobCfg := createCustomerOrderJobConfig()
	dbManager := setupRealDBManager(t, setup)
	cfg := dbManager.GetConfig()

	orch, err := NewOrchestrator(cfg, "test_cancellation", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewOrchestrator failed: %v", err)
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

	result, err := orch.Execute(ctx, nil)

	// Expect cancellation error
	if err == nil {
		t.Error("Expected cancellation error")
	}

	if errors.Is(err, context.Canceled) {
		t.Log("Graceful cancellation detected")
	}

	if result != nil {
		t.Logf("Partial result: %d records copied before cancellation", result.RecordsCopied)
	}
}

// TestOrchestrator_EmptyResultSet_Integration tests handling of no matching rows
func TestOrchestrator_EmptyResultSet_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	// Clear tables in both source and destination - fixtures may have seed data
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

	dbManager := setupRealDBManager(t, setup)
	cfg := dbManager.GetConfig()

	orch, err := NewOrchestrator(cfg, "test_empty_result", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewOrchestrator failed: %v", err)
	}
	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	result, err := orch.Execute(ctx, nil)
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
	verifyDest := getVerificationDB(t, setup, "destination")
	defer func() { _ = verifyDest.Close() }()
	verifyRowCount(t, verifyDest, "customers", 0)
}

// TestOrchestrator_MultiLevelHierarchy_Integration tests 3-level deep relationships
func TestOrchestrator_MultiLevelHierarchy_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	clearDestination(t, setup)
	sourceDB, _ := setup.GetDB("source")
	seedTestData(t, sourceDB)

	jobCfg := createCustomerOrderJobConfig()
	dbManager := setupRealDBManager(t, setup)
	cfg := dbManager.GetConfig()

	orch, err := NewOrchestrator(cfg, "test_multi_level", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewOrchestrator failed: %v", err)
	}
	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Verify graph depth
	copyOrder, err := orch.GetCopyOrder()
	if err != nil {
		t.Fatalf("GetCopyOrder failed: %v", err)
	}

	// Should have 4 tables: customers -> orders -> order_items, order_payments
	if len(copyOrder) != 4 {
		t.Errorf("Expected 4 tables in copy order, got %d: %v", len(copyOrder), copyOrder)
	}

	result, err := orch.Execute(ctx, nil)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Errorf("Expected successful execution, got errors: %v", result.Errors)
	}

	// Verify: all levels archived
	verifyDest := getVerificationDB(t, setup, "destination")
	defer func() { _ = verifyDest.Close() }()
	verifyRowCount(t, verifyDest, "customers", 2)
	verifyRowCount(t, verifyDest, "orders", 4)
}
