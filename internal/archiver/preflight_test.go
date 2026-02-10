// Package archiver provides comprehensive tests for the preflight checker.
package archiver

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
)

// ============================================================================
// Test Helpers
// ============================================================================

func createPreflightTestGraph() *graph.Graph {
	g := graph.NewGraph("users", "id")
	g.AddNode("orders", &graph.Node{Name: "orders", ForeignKey: "user_id", ReferenceKey: "id", DependencyType: "1-N"})
	g.AddNode("order_items", &graph.Node{Name: "order_items", ForeignKey: "order_id", ReferenceKey: "id", DependencyType: "1-N"})
	g.AddEdge("users", "orders")
	g.AddEdge("orders", "order_items")
	return g
}

// ============================================================================
// NewPreflightChecker Tests
// ============================================================================

func TestNewPreflightChecker_Success(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock: %v", err)
	}
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()

	checker, err := NewPreflightChecker(db, "testdb", g, log)
	if err != nil {
		t.Fatalf("NewPreflightChecker failed: %v", err)
	}

	if checker == nil {
		t.Fatal("NewPreflightChecker returned nil")
	}

	if checker.sourceDBName != "testdb" {
		t.Errorf("Expected sourceDBName 'testdb', got %s", checker.sourceDBName)
	}

	if checker.db != db {
		t.Error("Database mismatch")
	}

	if checker.graph != g {
		t.Error("Graph mismatch")
	}
}

func TestNewPreflightChecker_NilDB(t *testing.T) {
	g := createPreflightTestGraph()
	log := logger.NewDefault()

	_, err := NewPreflightChecker(nil, "testdb", g, log)
	if err == nil {
		t.Error("Expected error for nil database")
	}
}

func TestNewPreflightChecker_EmptyDBName(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()

	_, err := NewPreflightChecker(db, "", g, log)
	if err == nil {
		t.Error("Expected error for empty database name")
	}
}

func TestNewPreflightChecker_NilGraph(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	log := logger.NewDefault()

	_, err := NewPreflightChecker(db, "testdb", nil, log)
	if err == nil {
		t.Error("Expected error for nil graph")
	}
}

func TestNewPreflightChecker_DefaultLogger(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()

	checker, err := NewPreflightChecker(db, "testdb", g, nil)
	if err != nil {
		t.Fatalf("NewPreflightChecker failed: %v", err)
	}

	if checker.logger == nil {
		t.Error("Expected default logger to be set")
	}
}

// ============================================================================
// RunAllChecks Tests
// ============================================================================

func TestRunAllChecks_MissingTables(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	// Table existence check - only 2 of 3 tables exist
	mock.ExpectQuery("SELECT TABLE_NAME FROM information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME"}).
			AddRow("users").
			AddRow("orders"))
	// Missing: order_items

	err := checker.RunAllChecks(ctx, false)

	if err == nil {
		t.Error("Expected error for missing tables")
	}

	preflightErr, ok := err.(*PreflightError)
	if !ok {
		t.Fatalf("Expected PreflightError, got %T", err)
	}

	if preflightErr.Check != "TABLE_EXISTENCE_CHECK" {
		t.Errorf("Expected check 'TABLE_EXISTENCE_CHECK', got %s", preflightErr.Check)
	}
}

func TestRunAllChecks_NonInnoDBTables(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	// Table existence check - all exist
	mock.ExpectQuery("SELECT TABLE_NAME FROM information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME"}).
			AddRow("users").
			AddRow("orders").
			AddRow("order_items"))

	// Storage engine check - one table is MyISAM
	mock.ExpectQuery("SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "ENGINE"}).
			AddRow("users", "InnoDB").
			AddRow("orders", "MyISAM"). // Not allowed!
			AddRow("order_items", "InnoDB"))

	err := checker.RunAllChecks(ctx, false)

	if err == nil {
		t.Error("Expected error for non-InnoDB table")
	}

	preflightErr, ok := err.(*PreflightError)
	if !ok {
		t.Fatalf("Expected PreflightError, got %T", err)
	}

	if preflightErr.Check != "STORAGE_ENGINE_CHECK" {
		t.Errorf("Expected check 'STORAGE_ENGINE_CHECK', got %s", preflightErr.Check)
	}
}

// ============================================================================
// ValidateTablesExist Tests
// ============================================================================

func TestValidateTablesExist_Success(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	tables := []string{"users", "orders", "order_items"}

	mock.ExpectQuery("SELECT TABLE_NAME FROM information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME"}).
			AddRow("users").
			AddRow("orders").
			AddRow("order_items"))

	err := checker.ValidateTablesExist(ctx, tables)

	if err != nil {
		t.Fatalf("ValidateTablesExist failed: %v", err)
	}
}

func TestValidateTablesExist_MissingTables(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	tables := []string{"users", "orders", "order_items", "nonexistent"}

	mock.ExpectQuery("SELECT TABLE_NAME FROM information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME"}).
			AddRow("users").
			AddRow("orders").
			AddRow("order_items"))
	// Missing: nonexistent

	err := checker.ValidateTablesExist(ctx, tables)

	if err == nil {
		t.Error("Expected error for missing tables")
	}

	preflightErr, ok := err.(*PreflightError)
	if !ok {
		t.Fatalf("Expected PreflightError, got %T", err)
	}

	if len(preflightErr.Tables) != 1 || preflightErr.Tables[0] != "nonexistent" {
		t.Errorf("Expected missing table 'nonexistent', got %v", preflightErr.Tables)
	}
}

func TestValidateTablesExist_QueryError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	mock.ExpectQuery("SELECT TABLE_NAME FROM information_schema.TABLES").
		WillReturnError(errors.New("query failed"))

	err := checker.ValidateTablesExist(ctx, []string{"users"})

	if err == nil {
		t.Error("Expected error for query failure")
	}
}

// ============================================================================
// ValidateStorageEngine Tests
// ============================================================================

func TestValidateStorageEngine_Success(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	tables := []string{"users", "orders"}

	mock.ExpectQuery("SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "ENGINE"}).
			AddRow("users", "InnoDB").
			AddRow("orders", "InnoDB"))

	err := checker.ValidateStorageEngine(ctx, tables)

	if err != nil {
		t.Fatalf("ValidateStorageEngine failed: %v", err)
	}
}

func TestValidateStorageEngine_NonInnoDB(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	tables := []string{"users", "orders", "logs"}

	mock.ExpectQuery("SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "ENGINE"}).
			AddRow("users", "InnoDB").
			AddRow("orders", "MyISAM").
			AddRow("logs", "MEMORY"))

	err := checker.ValidateStorageEngine(ctx, tables)

	if err == nil {
		t.Error("Expected error for non-InnoDB tables")
	}

	preflightErr, ok := err.(*PreflightError)
	if !ok {
		t.Fatalf("Expected PreflightError, got %T", err)
	}

	if len(preflightErr.Tables) != 2 {
		t.Errorf("Expected 2 non-InnoDB tables, got %d", len(preflightErr.Tables))
	}
}

// ============================================================================
// ValidateForeignKeyIndexes Tests
// ============================================================================

func TestValidateForeignKeyIndexes_Success(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	// FK query
	mock.ExpectQuery("SELECT kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "CONSTRAINT_NAME", "COLUMN_NAME",
			"REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME",
			"DELETE_RULE", "UPDATE_RULE"},
		).AddRow("orders", "fk_orders_users", "user_id", "users", "id", "RESTRICT", "RESTRICT").
			AddRow("order_items", "fk_items_orders", "order_id", "orders", "id", "RESTRICT", "RESTRICT"))

	// Index checks - both columns have indexes
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.STATISTICS").
		WithArgs("testdb", "orders", "user_id").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.STATISTICS").
		WithArgs("testdb", "order_items", "order_id").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))

	err := checker.ValidateForeignKeyIndexes(ctx)

	if err != nil {
		t.Fatalf("ValidateForeignKeyIndexes failed: %v", err)
	}
}

func TestValidateForeignKeyIndexes_Unindexed(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	// FK query - only one FK to simplify test
	mock.ExpectQuery("SELECT kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "CONSTRAINT_NAME", "COLUMN_NAME",
			"REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME",
			"DELETE_RULE", "UPDATE_RULE"},
		).AddRow("orders", "fk_orders_users", "user_id", "users", "id", "RESTRICT", "RESTRICT"))

	// Index checks - user_id has no index
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.STATISTICS").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))

	err := checker.ValidateForeignKeyIndexes(ctx)

	if err == nil {
		t.Error("Expected error for unindexed FK")
	}

	preflightErr, ok := err.(*PreflightError)
	if !ok {
		t.Fatalf("Expected PreflightError, got %T", err)
	}

	if preflightErr.Check != "FK_INDEX_CHECK" {
		t.Errorf("Expected check 'FK_INDEX_CHECK', got %s", preflightErr.Check)
	}
}

// ============================================================================
// ValidateTriggers Tests
// ============================================================================

func TestValidateTriggers_NoTriggers(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()
	tables := []string{"users", "orders"}

	mock.ExpectQuery("SELECT EVENT_OBJECT_TABLE, TRIGGER_NAME FROM information_schema.TRIGGERS").
		WillReturnRows(sqlmock.NewRows([]string{"EVENT_OBJECT_TABLE", "TRIGGER_NAME"}))

	err := checker.ValidateTriggers(ctx, tables, false)

	if err != nil {
		t.Fatalf("ValidateTriggers failed: %v", err)
	}
}

func TestValidateTriggers_WithTriggers(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()
	tables := []string{"users", "orders"}

	mock.ExpectQuery("SELECT EVENT_OBJECT_TABLE, TRIGGER_NAME FROM information_schema.TRIGGERS").
		WillReturnRows(sqlmock.NewRows([]string{"EVENT_OBJECT_TABLE", "TRIGGER_NAME"}).
			AddRow("users", "trg_users_delete").
			AddRow("orders", "trg_orders_delete"))

	err := checker.ValidateTriggers(ctx, tables, false)

	if err == nil {
		t.Error("Expected error for tables with DELETE triggers")
	}
}

func TestValidateTriggers_WithForce(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()
	tables := []string{"users", "orders"}

	mock.ExpectQuery("SELECT EVENT_OBJECT_TABLE, TRIGGER_NAME FROM information_schema.TRIGGERS").
		WillReturnRows(sqlmock.NewRows([]string{"EVENT_OBJECT_TABLE", "TRIGGER_NAME"}).
			AddRow("users", "trg_users_delete"))

	// With force=true, should not error
	err := checker.ValidateTriggers(ctx, tables, true)

	if err != nil {
		t.Fatalf("ValidateTriggers should pass with force=true: %v", err)
	}
}

// ============================================================================
// CheckDeleteTriggers Tests
// ============================================================================

func TestCheckDeleteTriggers_Success(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	tables := []string{"users", "orders"}

	mock.ExpectQuery("SELECT EVENT_OBJECT_TABLE, TRIGGER_NAME FROM information_schema.TRIGGERS").
		WillReturnRows(sqlmock.NewRows([]string{"EVENT_OBJECT_TABLE", "TRIGGER_NAME"}).
			AddRow("users", "trg_users_delete").
			AddRow("orders", "trg_orders_delete"))

	triggers, err := checker.CheckDeleteTriggers(ctx, tables)

	if err != nil {
		t.Fatalf("CheckDeleteTriggers failed: %v", err)
	}

	if len(triggers) != 2 {
		t.Errorf("Expected 2 triggers, got %d", len(triggers))
	}
}

func TestCheckDeleteTriggers_Empty(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	tables := []string{"users", "orders"}

	mock.ExpectQuery("SELECT EVENT_OBJECT_TABLE, TRIGGER_NAME FROM information_schema.TRIGGERS").
		WillReturnRows(sqlmock.NewRows([]string{"EVENT_OBJECT_TABLE", "TRIGGER_NAME"}))

	triggers, err := checker.CheckDeleteTriggers(ctx, tables)

	if err != nil {
		t.Fatalf("CheckDeleteTriggers failed: %v", err)
	}

	if len(triggers) != 0 {
		t.Errorf("Expected 0 triggers, got %d", len(triggers))
	}
}

// ============================================================================
// WarnCascadeRules Tests
// ============================================================================

func TestWarnCascadeRules_WithCascade(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	// FK query with CASCADE
	mock.ExpectQuery("SELECT kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "CONSTRAINT_NAME", "COLUMN_NAME",
			"REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME",
			"DELETE_RULE", "UPDATE_RULE"},
		).AddRow("orders", "fk_orders_users", "user_id", "users", "id", "CASCADE", "RESTRICT").
			AddRow("order_items", "fk_items_orders", "order_id", "orders", "id", "CASCADE", "RESTRICT"))

	// Index checks
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.STATISTICS").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.STATISTICS").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))

	// Should not error, just warn
	err := checker.WarnCascadeRules(ctx)

	if err != nil {
		t.Fatalf("WarnCascadeRules should not error: %v", err)
	}
}

func TestWarnCascadeRules_NoCascade(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	// FK query without CASCADE
	mock.ExpectQuery("SELECT kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "CONSTRAINT_NAME", "COLUMN_NAME",
			"REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME",
			"DELETE_RULE", "UPDATE_RULE"},
		).AddRow("orders", "fk_orders_users", "user_id", "users", "id", "RESTRICT", "RESTRICT").
			AddRow("order_items", "fk_items_orders", "order_id", "orders", "id", "RESTRICT", "RESTRICT"))

	// Index checks
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.STATISTICS").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.STATISTICS").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))

	err := checker.WarnCascadeRules(ctx)

	if err != nil {
		t.Fatalf("WarnCascadeRules failed: %v", err)
	}
}

func TestWarnCascadeRules_QueryError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	mock.ExpectQuery("SELECT kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME").
		WillReturnError(errors.New("query failed"))

	err := checker.WarnCascadeRules(ctx)

	if err == nil {
		t.Error("Expected error for query failure")
	}
}

// ============================================================================
// PreflightError Tests
// ============================================================================

func TestPreflightError_Error(t *testing.T) {
	err := &PreflightError{
		Check:   "TEST_CHECK",
		Message: "test message",
		Tables:  []string{"table1", "table2"},
	}

	msg := err.Error()
	if msg == "" {
		t.Error("Expected non-empty error message")
	}

	// Should contain check name
	if msg == "" || len(msg) < len("TEST_CHECK") {
		t.Errorf("Expected error to contain check name, got: %s", msg)
	}

	// Should contain tables
	if msg == "" || len(msg) < len("table1") {
		t.Errorf("Expected error to contain table names, got: %s", msg)
	}
}

func TestPreflightError_ErrorNoTables(t *testing.T) {
	err := &PreflightError{
		Check:   "TEST_CHECK",
		Message: "test message",
	}

	msg := err.Error()
	if msg == "" {
		t.Error("Expected non-empty error message")
	}
}

// ============================================================================
// Context Cancellation Tests
// ============================================================================

func TestValidateTablesExist_ContextCancellation(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := checker.ValidateTablesExist(ctx, []string{"users"})

	if err == nil {
		t.Error("Expected error for cancelled context")
	}
}

// ============================================================================
// Setter Tests
// ============================================================================

func TestPreflightChecker_SetLogger(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)

	newLog := logger.NewDefault()
	checker.SetLogger(newLog)

	if checker.logger != newLog {
		t.Error("SetLogger did not set logger correctly")
	}
}

// ============================================================================
// isColumnIndexed Tests (via ValidateForeignKeyIndexes)
// ============================================================================

func TestIsColumnIndexed_True(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	// FK query
	mock.ExpectQuery("SELECT kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "CONSTRAINT_NAME", "COLUMN_NAME",
			"REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME",
			"DELETE_RULE", "UPDATE_RULE"},
		).AddRow("orders", "fk_orders_users", "user_id", "users", "id", "RESTRICT", "RESTRICT"))

	// Index exists
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.STATISTICS").
		WithArgs("testdb", "orders", "user_id").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))

	// Test via ValidateForeignKeyIndexes (should pass since column is indexed)
	err := checker.ValidateForeignKeyIndexes(ctx)

	if err != nil {
		t.Fatalf("Expected no error for indexed column: %v", err)
	}
}

// ============================================================================
// Integration Tests
// ============================================================================
