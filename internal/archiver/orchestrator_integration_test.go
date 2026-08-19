//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/internal/archiver/testsupport"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/dbsmedya/goarchive/internal/lock"
)

// ============================================================================
// Integration Test Setup (Self-Contained)
// ============================================================================

// testReplicaPort is db3 in the compose estate — a live replica of db1
// (tests/scripts/setup-replication.sh attaches it).
const testReplicaPort = 3308

// setupRealDBManager creates a database manager using the integration test setup
func setupRealDBManager(t *testing.T, setup *IntegrationTestSetup) (*database.Manager, *config.Config) {
	t.Helper()

	// A zero ReplicationConfig is a disabled one, so Manager.Replicas stays
	// empty and every existing caller behaves exactly as before.
	return setupRealDBManagerWithReplication(t, setup, config.ReplicationConfig{})
}

// setupRealDBManagerWithReplication builds the manager with the replication
// fleet injected BEFORE Connect. Order matters: Manager.Connect dials the
// replicas listed in cfg.Replication, so assigning cfg.Replication after
// Connect would leave Manager.Replicas empty and the gate would refuse to
// construct.
func setupRealDBManagerWithReplication(
	t *testing.T,
	setup *IntegrationTestSetup,
	repl config.ReplicationConfig,
) (*database.Manager, *config.Config) {
	t.Helper()
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
		Replication: repl,
	}

	dbManager := database.NewManager(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := dbManager.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect database manager: %v", err)
	}

	return dbManager, cfg
}

// replicationServerFor describes db3 to the gate, borrowing the root
// credentials the rest of the integration harness already uses.
func replicationServerFor(t *testing.T, setup *IntegrationTestSetup, channels []string) config.ReplicationServerConfig {
	t.Helper()

	for _, db := range setup.Config.Databases {
		if db.Name == "source" {
			return config.ReplicationServerConfig{
				Host:     db.Host,
				Port:     testReplicaPort,
				User:     db.User,
				Password: db.Password,
				TLS:      "disable",
				Type:     "async",
				Channels: channels,
			}
		}
	}

	t.Fatal("source database config not found — cannot derive replica credentials")

	return config.ReplicationServerConfig{}
}

// replicaAdminDB opens a direct connection to db3 for replication
// administration (STOP/START REPLICA, SOURCE_DELAY, status reads).
func replicaAdminDB(t *testing.T, setup *IntegrationTestSetup) *sql.DB {
	t.Helper()

	server := replicationServerFor(t, setup, nil)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?tls=false", server.User, server.Password, server.Host, server.Port)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Failed to open replica admin DB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Failed to ping replica on port %d: %v", testReplicaPort, err)
	}

	return db
}

// replicaStatusField reads one named column out of SHOW REPLICA STATUS.
// Columns are located by NAME, never by position, because the column set
// differs across server versions. An unattached replica returns ("", false):
// no row is an answer, not an error.
func replicaStatusField(t *testing.T, db *sql.DB, column string) (string, bool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "SHOW REPLICA STATUS")
	if err != nil {
		t.Fatalf("SHOW REPLICA STATUS failed: %v", err)
	}
	defer func() { _ = rows.Close() }()

	names, err := rows.Columns()
	if err != nil {
		t.Fatalf("reading SHOW REPLICA STATUS columns: %v", err)
	}
	if !rows.Next() {
		return "", false
	}

	values := make([]any, len(names))
	targets := make([]any, len(names))
	for i := range values {
		targets[i] = &values[i]
	}
	if err := rows.Scan(targets...); err != nil {
		t.Fatalf("scanning SHOW REPLICA STATUS: %v", err)
	}

	for i, name := range names {
		if name != column {
			continue
		}
		switch v := values[i].(type) {
		case nil:
			return "NULL", true
		case []byte:
			return string(v), true
		default:
			return fmt.Sprintf("%v", v), true
		}
	}

	t.Fatalf("SHOW REPLICA STATUS has no column %q", column)

	return "", false
}

// waitForReplicaCaughtUp blocks until db3 is running and reporting zero lag.
// Every scenario starts from this baseline: one scenario's writes must be fully
// applied on the replica before the next scenario measures anything, or a test
// inherits the previous one's lag and blames its own setup for it.
func waitForReplicaCaughtUp(t *testing.T, db *sql.DB, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var io, sqlThread, lag string

	for time.Now().Before(deadline) {
		io, _ = replicaStatusField(t, db, "Replica_IO_Running")
		sqlThread, _ = replicaStatusField(t, db, "Replica_SQL_Running")
		lag, _ = replicaStatusField(t, db, "Seconds_Behind_Source")

		if io == "Yes" && sqlThread == "Yes" && lag == "0" {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("replica did not catch up within %s (IO=%q SQL=%q Seconds_Behind_Source=%q)",
		timeout, io, sqlThread, lag)
}

// waitForReplicaLagAbove blocks until the replica reports MORE than threshold
// seconds of lag, and returns the value observed.
//
// This wait is not a convenience — without it the lag scenario is vacuous.
// SOURCE_DELAY does NOT make Seconds_Behind_Source jump to the delay value: the
// delayed event is simply withheld, so the reported lag is the AGE of the
// oldest unapplied event and climbs from 0 in real time (measured on this
// estate: 0, 2, 4, 7, 12 over the first ~11s under SOURCE_DELAY=120). A job
// launched immediately after the write would therefore find lag=0 on its first
// check, sail through the gate, and finish before any lag accumulated — a test
// that passes having never held.
func waitForReplicaLagAbove(t *testing.T, db *sql.DB, threshold int, timeout time.Duration) int {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last string

	for time.Now().Before(deadline) {
		last, _ = replicaStatusField(t, db, "Seconds_Behind_Source")
		if seconds, err := strconv.Atoi(last); err == nil && seconds > threshold {
			return seconds
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("replica lag did not exceed %ds within %s (last Seconds_Behind_Source=%q)", threshold, timeout, last)

	return 0
}

// setReplicaSourceDelay applies SOURCE_DELAY, which requires the applier to be
// stopped for the reconfiguration.
func setReplicaSourceDelay(t *testing.T, db *sql.DB, seconds int) {
	t.Helper()

	if _, err := db.Exec("STOP REPLICA SQL_THREAD"); err != nil {
		t.Fatalf("STOP REPLICA SQL_THREAD (to set SOURCE_DELAY=%d): %v", seconds, err)
	}
	if _, err := db.Exec(fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_DELAY=%d", seconds)); err != nil {
		t.Fatalf("CHANGE REPLICATION SOURCE TO SOURCE_DELAY=%d: %v", seconds, err)
	}
	if _, err := db.Exec("START REPLICA SQL_THREAD"); err != nil {
		t.Fatalf("START REPLICA SQL_THREAD (after SOURCE_DELAY=%d): %v", seconds, err)
	}
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

// seedLargeTestData generates a large dataset for context cancellation testing
// Generates 5000 customers with related orders, items, and payments
func seedLargeTestData(t *testing.T, db *sql.DB) {
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

	const numCustomers = 5000
	const batchSize = 500

	t.Logf("Generating large test data: %d customers with related orders/items/payments...", numCustomers)

	// Generate customers in batches
	customerID := 1
	orderID := 1
	itemID := 1
	paymentID := 1

	for batch := 0; batch < numCustomers/batchSize; batch++ {
		// Build customer batch
		var customerValues []string
		for i := 0; i < batchSize; i++ {
			// 90% old customers (archive candidates), 10% new customers
			var createdAt string
			if i%10 < 9 {
				// Old customer: created 6+ months ago
				createdAt = fmt.Sprintf("DATE_SUB(NOW(), INTERVAL %d DAY)", 180+customerID%365)
			} else {
				// New customer: created recently
				createdAt = fmt.Sprintf("DATE_SUB(NOW(), INTERVAL %d DAY)", customerID%30)
			}
			name := fmt.Sprintf("Customer_%d", customerID)
			email := fmt.Sprintf("customer_%d@test.com", customerID)
			customerValues = append(customerValues,
				fmt.Sprintf("(%d, '%s', '%s', %s)", customerID, name, email, createdAt))
			customerID++
		}

		customerQuery := "INSERT INTO customers (id, name, email, created_at) VALUES " +
			strings.Join(customerValues, ", ")
		if _, err := db.Exec(customerQuery); err != nil {
			t.Fatalf("Failed to insert customers batch %d: %v", batch, err)
		}

		// Build orders batch (2-3 orders per customer)
		var orderValues []string
		startCustomerID := batch*batchSize + 1
		for c := startCustomerID; c < startCustomerID+batchSize; c++ {
			numOrders := 2 + c%2 // 2-3 orders per customer
			for o := 0; o < numOrders; o++ {
				status := "completed"
				if o%3 == 0 {
					status = "pending"
				} else if o%5 == 0 {
					status = "cancelled"
				}
				total := 50.0 + float64(orderID%1000)
				createdAt := fmt.Sprintf("DATE_SUB(NOW(), INTERVAL %d DAY)", 150+orderID%300)
				orderValues = append(orderValues,
					fmt.Sprintf("(%d, %d, %.2f, '%s', %s)", orderID, c, total, status, createdAt))
				orderID++
			}
		}

		orderQuery := "INSERT INTO orders (id, customer_id, total, status, created_at) VALUES " +
			strings.Join(orderValues, ", ")
		if _, err := db.Exec(orderQuery); err != nil {
			t.Fatalf("Failed to insert orders batch %d: %v", batch, err)
		}

		// Build order_items batch (2-4 items per order)
		var itemValues []string
		startOrderID := batch*batchSize*2 + batch*batchSize + 1
		endOrderID := orderID
		for o := startOrderID; o < endOrderID && o < orderID; o++ {
			numItems := 2 + o%3 // 2-4 items per order
			for i := 0; i < numItems; i++ {
				products := []string{"Widget A", "Widget B", "Widget C", "Gadget X", "Gadget Y", "Premium Pack", "Deluxe Set"}
				product := products[itemID%len(products)]
				quantity := 1 + itemID%5
				price := 10.0 + float64(itemID%200)
				itemValues = append(itemValues,
					fmt.Sprintf("(%d, %d, '%s', %d, %.2f)", itemID, o, product, quantity, price))
				itemID++
			}
		}

		if len(itemValues) > 0 {
			itemQuery := "INSERT INTO order_items (id, order_id, product, quantity, price) VALUES " +
				strings.Join(itemValues, ", ")
			if _, err := db.Exec(itemQuery); err != nil {
				t.Fatalf("Failed to insert order_items batch %d: %v", batch, err)
			}
		}

		// Build order_payments batch (1 payment per completed order)
		var paymentValues []string
		for o := startOrderID; o < endOrderID && o < orderID; o++ {
			// Only add payment for completed orders (skip pending/cancelled)
			if o%3 != 0 && o%5 != 0 {
				methods := []string{"credit_card", "paypal", "bank_transfer"}
				method := methods[paymentID%len(methods)]
				amount := 50.0 + float64(paymentID%1000)
				paidAt := fmt.Sprintf("DATE_SUB(NOW(), INTERVAL %d DAY)", 149+paymentID%300)
				paymentValues = append(paymentValues,
					fmt.Sprintf("(%d, %d, %.2f, '%s', %s)", paymentID, o, amount, method, paidAt))
				paymentID++
			}
		}

		if len(paymentValues) > 0 {
			paymentQuery := "INSERT INTO order_payments (id, order_id, amount, method, paid_at) VALUES " +
				strings.Join(paymentValues, ", ")
			if _, err := db.Exec(paymentQuery); err != nil {
				t.Fatalf("Failed to insert order_payments batch %d: %v", batch, err)
			}
		}
	}

	t.Logf("Test data generation complete: %d customers, %d orders, %d items, %d payments",
		customerID-1, orderID-1, itemID-1, paymentID-1)
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
	dbManager, cfg := setupRealDBManager(t, setup)

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
	dbManager, cfg := setupRealDBManager(t, setup)

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

// executeOutcome carries both of Execute's return values off the goroutine that
// runs it. The hold scenarios need Execute in flight while the test mutates the
// estate around it, and the resume must be proven on the SAME invocation --
// calling Execute a second time would prove only that a fresh run succeeds.
type executeOutcome struct {
	result *ArchiveResult
	err    error
}

// TestOrchestrator_ReplicationGate_Integration exercises the replication gate
// against the LIVE estate: db3 is a real replica of db1, so the facts the gate
// reads come from a server rather than a fixture.
//
// This replaces a test that ran Execute and then t.Logf'd whatever happened —
// it asserted nothing and could not fail, so it proved only that the code did
// not panic.
func TestOrchestrator_ReplicationGate_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Run("healthy replica passes the gate", func(t *testing.T) {
		setup, ctx := SetupIntegrationTest(t)
		defer setup.Close()

		replicaDB := replicaAdminDB(t, setup)
		waitForReplicaCaughtUp(t, replicaDB, 30*time.Second)

		clearDestination(t, setup)
		sourceDB, _ := setup.GetDB("source")
		seedTestData(t, sourceDB)

		const jobName = "repl_gate_s1"
		destDB := getVerificationDB(t, setup, "destination")
		// Registration order is load-bearing: t.Cleanup runs LIFO, so the close
		// must be registered FIRST to run LAST. A `defer destDB.Close()` here
		// would close the handle before CleanupArchiverState's own cleanup uses
		// it, leaving the archiver_job row behind — and a stale checkpoint makes
		// the NEXT run resume into "no more root IDs" and archive nothing.
		t.Cleanup(func() { _ = destDB.Close() })
		testsupport.CleanupArchiverState(t, destDB, jobName)

		repl := config.ReplicationConfig{
			Enabled:                   true,
			SecondsBehindSourceWithin: 10,
			CheckInterval:             1,
			CacheTTL:                  0,
			Servers: []config.ReplicationServerConfig{
				replicationServerFor(t, setup, nil), // nil channels = all channels
			},
		}

		dbManager, cfg := setupRealDBManagerWithReplication(t, setup, repl)
		defer func() { _ = dbManager.Close() }()

		if len(dbManager.Replicas) != 1 {
			t.Fatalf("Replicas = %d, want 1 — the fleet was not wired before Connect", len(dbManager.Replicas))
		}

		orch, err := NewOrchestrator(cfg, jobName, createCustomerOrderJobConfig(), dbManager)
		if err != nil {
			t.Fatalf("NewOrchestrator failed: %v", err)
		}
		if err := orch.Initialize(); err != nil {
			t.Fatalf("Initialize failed: %v", err)
		}

		result, err := orch.Execute(ctx, nil)
		if err != nil {
			t.Fatalf("Execute failed against a healthy replica: %v", err)
		}
		if result == nil || !result.Success {
			t.Fatalf("result = %+v, want a successful archive", result)
		}
		if result.RecordsCopied == 0 {
			t.Fatal("RecordsCopied = 0 — the gate passed but nothing was archived, so this proves nothing")
		}

		// The gate passing is not the claim; the archive actually happening is.
		verifySource := getVerificationDB(t, setup, "source")
		defer func() { _ = verifySource.Close() }()
		verifyRowCount(t, verifySource, "customers", 1) // only Carol (3 months old) remains
		verifyRowCount(t, destDB, "customers", 2)       // Alice and Bob archived
	})

	t.Run("stopped applier holds the job, then the same invocation resumes", func(t *testing.T) {
		setup, _ := SetupIntegrationTest(t)
		defer setup.Close()

		replicaDB := replicaAdminDB(t, setup)
		waitForReplicaCaughtUp(t, replicaDB, 30*time.Second)

		clearDestination(t, setup)
		sourceDB, _ := setup.GetDB("source")
		seedTestData(t, sourceDB)

		const jobName = "repl_gate_s2"
		destDB := getVerificationDB(t, setup, "destination")
		t.Cleanup(func() { _ = destDB.Close() })
		testsupport.CleanupArchiverState(t, destDB, jobName)

		verifySource := getVerificationDB(t, setup, "source")
		t.Cleanup(func() { _ = verifySource.Close() })

		const checkInterval = 1
		repl := config.ReplicationConfig{
			Enabled:                   true,
			SecondsBehindSourceWithin: 10,
			CheckInterval:             checkInterval,
			CacheTTL:                  0, // never serve a cached pass: every tick must read the replica
			Servers: []config.ReplicationServerConfig{
				replicationServerFor(t, setup, nil),
			},
		}

		dbManager, cfg := setupRealDBManagerWithReplication(t, setup, repl)
		t.Cleanup(func() { _ = dbManager.Close() })

		orch, err := NewOrchestrator(cfg, jobName, createCustomerOrderJobConfig(), dbManager)
		if err != nil {
			t.Fatalf("NewOrchestrator failed: %v", err)
		}
		if err := orch.Initialize(); err != nil {
			t.Fatalf("Initialize failed: %v", err)
		}

		// The restore is registered BEFORE the applier is stopped, so a failed
		// assertion, a t.Fatal, or a panic still hands the estate back running.
		// A test that leaves db3 detached poisons every test after it, and the
		// symptom appears far from the cause.
		t.Cleanup(func() {
			if _, err := replicaDB.Exec("START REPLICA SQL_THREAD"); err != nil {
				t.Errorf("cleanup: START REPLICA SQL_THREAD: %v", err)
			}
			waitForReplicaCaughtUp(t, replicaDB, 60*time.Second)
		})

		if _, err := replicaDB.Exec("STOP REPLICA SQL_THREAD"); err != nil {
			t.Fatalf("STOP REPLICA SQL_THREAD: %v", err)
		}
		if state, _ := replicaStatusField(t, replicaDB, "Replica_SQL_Running"); state != "No" {
			t.Fatalf("Replica_SQL_Running = %q after STOP, want %q — the scenario's premise does not hold", state, "No")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		outcomeCh := make(chan executeOutcome, 1)
		go func() {
			result, err := orch.Execute(ctx, nil) // nil CheckpointCallback
			outcomeCh <- executeOutcome{result: result, err: err}
		}()

		// Prove the HOLD: for at least 3 check intervals the invocation must not
		// return, and the source rows must still be there. Both halves matter --
		// "Execute has not returned" alone would also be true of a hang, and
		// "rows still present" alone would also be true of a crash.
		holdWindow := 3 * checkInterval * time.Second
		holdDeadline := time.Now().Add(holdWindow)
		for time.Now().Before(holdDeadline) {
			select {
			case out := <-outcomeCh:
				t.Fatalf("Execute returned during the %s hold window (err=%v, result=%+v) — the gate did not hold on a stopped applier",
					holdWindow, out.err, out.result)
			default:
			}
			time.Sleep(200 * time.Millisecond)
		}
		verifyRowCount(t, verifySource, "customers", 3) // nothing archived while held

		// Release the applier: the SAME invocation must now finish.
		if _, err := replicaDB.Exec("START REPLICA SQL_THREAD"); err != nil {
			t.Fatalf("START REPLICA SQL_THREAD: %v", err)
		}

		select {
		case out := <-outcomeCh:
			if out.err != nil {
				t.Fatalf("Execute failed after the applier recovered: %v", out.err)
			}
			if out.result == nil || !out.result.Success {
				t.Fatalf("result = %+v, want a successful archive", out.result)
			}
			if out.result.RecordsCopied == 0 {
				t.Fatal("RecordsCopied = 0 — the invocation resumed but archived nothing")
			}
		case <-time.After(60 * time.Second):
			t.Fatal("Execute did not resume within 60s of the applier restarting")
		}

		verifyRowCount(t, verifySource, "customers", 1)
		verifyRowCount(t, destDB, "customers", 2)
	})

	t.Run("replication lag holds the job, then the same invocation resumes", func(t *testing.T) {
		setup, _ := SetupIntegrationTest(t)
		defer setup.Close()

		replicaDB := replicaAdminDB(t, setup)
		waitForReplicaCaughtUp(t, replicaDB, 30*time.Second)

		clearDestination(t, setup)
		sourceDB, _ := setup.GetDB("source")
		seedTestData(t, sourceDB)

		const jobName = "repl_gate_s3"
		destDB := getVerificationDB(t, setup, "destination")
		t.Cleanup(func() { _ = destDB.Close() })
		testsupport.CleanupArchiverState(t, destDB, jobName)

		verifySource := getVerificationDB(t, setup, "source")
		t.Cleanup(func() { _ = verifySource.Close() })

		// A scratch table on the source, created BEFORE the delay so only the
		// INSERT below is withheld. Writing to the archived tables instead would
		// corrupt the row-count assertions.
		if _, err := sourceDB.Exec(
			"CREATE TABLE IF NOT EXISTS repl_lag_probe (id INT PRIMARY KEY AUTO_INCREMENT)"); err != nil {
			t.Fatalf("creating lag probe table: %v", err)
		}
		// The drop runs on verifySource, NOT on the setup-owned sourceDB:
		// `defer setup.Close()` fires when the subtest function returns, which is
		// BEFORE any t.Cleanup callback, so a cleanup that touches a setup handle
		// always finds it closed. verifySource is closed by a cleanup registered
		// earlier, and cleanups run LIFO, so it is still open here.
		t.Cleanup(func() {
			if _, err := verifySource.Exec("DROP TABLE IF EXISTS repl_lag_probe"); err != nil {
				t.Errorf("cleanup: dropping lag probe table: %v", err)
			}
		})

		const tolerance = 1
		repl := config.ReplicationConfig{
			Enabled:                   true,
			SecondsBehindSourceWithin: tolerance,
			CheckInterval:             1,
			CacheTTL:                  0,
			Servers: []config.ReplicationServerConfig{
				replicationServerFor(t, setup, nil),
			},
		}

		dbManager, cfg := setupRealDBManagerWithReplication(t, setup, repl)
		t.Cleanup(func() { _ = dbManager.Close() })

		orch, err := NewOrchestrator(cfg, jobName, createCustomerOrderJobConfig(), dbManager)
		if err != nil {
			t.Fatalf("NewOrchestrator failed: %v", err)
		}
		if err := orch.Initialize(); err != nil {
			t.Fatalf("Initialize failed: %v", err)
		}

		// Registered BEFORE the delay is applied: a failure must not leave the
		// estate lagging for every test that follows.
		t.Cleanup(func() {
			setReplicaSourceDelay(t, replicaDB, 0)
			waitForReplicaCaughtUp(t, replicaDB, 60*time.Second)
		})

		setReplicaSourceDelay(t, replicaDB, 120)
		if _, err := sourceDB.Exec("INSERT INTO repl_lag_probe () VALUES ()"); err != nil {
			t.Fatalf("writing to the lag probe table: %v", err)
		}

		// Wait for the lag to actually exceed the tolerance before starting the
		// job. See waitForReplicaLagAbove: skipping this makes the scenario
		// vacuous, because the lag starts at 0 and climbs.
		observed := waitForReplicaLagAbove(t, replicaDB, tolerance, 30*time.Second)
		t.Logf("replica reports %ds of lag (tolerance %ds) — starting the job", observed, tolerance)

		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		outcomeCh := make(chan executeOutcome, 1)
		go func() {
			result, err := orch.Execute(ctx, nil)
			outcomeCh <- executeOutcome{result: result, err: err}
		}()

		holdWindow := 3 * time.Second
		holdDeadline := time.Now().Add(holdWindow)
		for time.Now().Before(holdDeadline) {
			select {
			case out := <-outcomeCh:
				t.Fatalf("Execute returned during the %s hold window (err=%v, result=%+v) — the gate did not hold on a lagging replica",
					holdWindow, out.err, out.result)
			default:
			}
			time.Sleep(200 * time.Millisecond)
		}
		verifyRowCount(t, verifySource, "customers", 3) // nothing archived while held

		// Clear the delay: the withheld event applies within about a second, so
		// the SAME invocation must now finish.
		setReplicaSourceDelay(t, replicaDB, 0)

		select {
		case out := <-outcomeCh:
			if out.err != nil {
				t.Fatalf("Execute failed after the lag cleared: %v", out.err)
			}
			if out.result == nil || !out.result.Success {
				t.Fatalf("result = %+v, want a successful archive", out.result)
			}
			if out.result.RecordsCopied == 0 {
				t.Fatal("RecordsCopied = 0 — the invocation resumed but archived nothing")
			}
		case <-time.After(60 * time.Second):
			t.Fatal("Execute did not resume within 60s of the lag clearing")
		}

		verifyRowCount(t, verifySource, "customers", 1)
		verifyRowCount(t, destDB, "customers", 2)
	})
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
	dbManager, cfg := setupRealDBManager(t, setup)
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
	seedLargeTestData(t, sourceDB)

	jobCfg := createCustomerOrderJobConfig()
	dbManager, cfg := setupRealDBManager(t, setup)

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

	// #79: a cancelled run must leave no advisory lock behind — the next run on
	// this root table must not be told "another startup is in progress".
	destDB, _ := setup.GetDB("destination")
	assertAdvisoryLockFree(t, destDB, lock.GenerateRootTableLockName("customers"))
	assertAdvisoryLockFree(t, destDB, lock.GenerateJobLockName("test_cancellation"))
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

	dbManager, cfg := setupRealDBManager(t, setup)

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
	dbManager, cfg := setupRealDBManager(t, setup)

	orch, err := NewOrchestrator(cfg, "test_multi_level", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewOrchestrator failed: %v", err)
	}
	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Verify graph depth
	copyOrder := orch.copyOrder

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

// TestOrchestrator_BatchArchive_Integration verifies a single batch large enough
// to cover all root PKs archives every row correctly and leaves no pending log
// entries — exercising the batched processBatch path.
func TestOrchestrator_BatchArchive_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	clearDestination(t, setup)
	sourceDB, _ := setup.GetDB("source")
	seedTestData(t, sourceDB)

	jobCfg := createCustomerOrderJobConfig()
	batchSize := 1000
	batchDeleteSize := 1000
	jobCfg.Processing = &config.ProcessingOverrides{BatchSize: &batchSize, BatchDeleteSize: &batchDeleteSize}

	dbManager, cfg := setupRealDBManager(t, setup)

	orch, err := NewOrchestrator(cfg, "test_batch_archive", jobCfg, dbManager)
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
		t.Fatalf("expected success, got errors: %v", result.Errors)
	}

	verifySource := getVerificationDB(t, setup, "source")
	defer func() { _ = verifySource.Close() }()
	verifyDest := getVerificationDB(t, setup, "destination")
	defer func() { _ = verifyDest.Close() }()

	verifyRowCount(t, verifyDest, "customers", 2)
	verifyRowCount(t, verifyDest, "orders", 4)
	verifyRowCount(t, verifySource, "customers", 1)
	verifyRowCount(t, verifySource, "orders", 1)

	var jobID int64
	if err := verifyDest.QueryRow("SELECT id FROM archiver_job WHERE job_name = ?", "test_batch_archive").Scan(&jobID); err != nil {
		t.Fatalf("resolve job id: %v", err)
	}
	var pending int
	q := fmt.Sprintf("SELECT COUNT(*) FROM `archiver_job_log_%d` WHERE log_status = 0", jobID)
	if err := verifyDest.QueryRow(q).Scan(&pending); err != nil {
		t.Fatalf("count pending: %v", err)
	}
	if pending != 0 {
		t.Fatalf("expected 0 pending log entries, got %d", pending)
	}
}
