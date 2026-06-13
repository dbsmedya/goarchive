// Package archiver provides comprehensive tests for the preflight checker.
package archiver

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/config"
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
	g.SetPK("orders", "id")
	g.SetPK("order_items", "id")
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

func TestPreflightChecker_ValidateRootPKNumeric(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()
	g := graph.NewGraph("users", "id")
	checker, _ := NewPreflightChecker(db, "testdb", g, nil)

	mock.ExpectQuery("SELECT DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS").
		WithArgs("users", "id").
		WillReturnRows(sqlmock.NewRows([]string{"DATA_TYPE", "COLUMN_TYPE"}).AddRow("bigint", "bigint(20) unsigned"))
	if err := checker.ValidateRootPKNumeric(context.Background(), "users", "id"); err != nil {
		t.Fatalf("ValidateRootPKNumeric: %v", err)
	}
	dataType, unsigned, ok := g.GetRootPKMeta()
	if !ok || dataType != "bigint" || !unsigned {
		t.Fatalf("metadata: dataType=%q unsigned=%v ok=%v", dataType, unsigned, ok)
	}

	db2, mock2, _ := sqlmock.New()
	defer func() { _ = db2.Close() }()
	checker2, _ := NewPreflightChecker(db2, "testdb", graph.NewGraph("orders", "uuid"), nil)
	mock2.ExpectQuery("SELECT DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS").
		WithArgs("orders", "uuid").
		WillReturnRows(sqlmock.NewRows([]string{"DATA_TYPE", "COLUMN_TYPE"}).AddRow("varchar", "varchar(36)"))
	err := checker2.ValidateRootPKNumeric(context.Background(), "orders", "uuid")
	if err == nil || !strings.Contains(err.Error(), "ROOT_PK_TYPE_UNSUPPORTED") {
		t.Fatalf("expected ROOT_PK_TYPE_UNSUPPORTED, got %v", err)
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

	// Primary key column existence checks
	for i := 0; i < 3; i++ {
		mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.COLUMNS").
			WithArgs("testdb", sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	}
	// Composite primary key checks (review P1-1) - all single-column PKs
	for i := 0; i < 3; i++ {
		mock.ExpectQuery("information_schema.STATISTICS").
			WithArgs("testdb", sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	}
	mock.ExpectQuery("SELECT DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS").
		WithArgs("users", "id").
		WillReturnRows(sqlmock.NewRows([]string{"DATA_TYPE", "COLUMN_TYPE"}).AddRow("bigint", "bigint(20) unsigned"))

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

func TestValidatePrimaryKeyColumns_MissingConfiguredPKColumn(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.COLUMNS").
		WithArgs("testdb", "users", "id").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.COLUMNS").
		WithArgs("testdb", "orders", "id").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))

	err := checker.ValidatePrimaryKeyColumns(ctx, []string{"users", "orders"})
	if err == nil {
		t.Fatal("expected PK column validation error")
	}

	preflightErr, ok := err.(*PreflightError)
	if !ok {
		t.Fatalf("Expected PreflightError, got %T", err)
	}
	if preflightErr.Check != "PK_COLUMN_CHECK" {
		t.Fatalf("Expected PK_COLUMN_CHECK, got %s", preflightErr.Check)
	}
}

func TestValidatePrimaryKeyColumns_RequiresExplicitPKMapping(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	g.AddNode("legacy", &graph.Node{Name: "legacy"})
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)

	err := checker.ValidatePrimaryKeyColumns(context.Background(), []string{"legacy"})
	if err == nil {
		t.Fatal("expected explicit PK mapping error")
	}

	preflightErr, ok := err.(*PreflightError)
	if !ok {
		t.Fatalf("Expected PreflightError, got %T", err)
	}
	if preflightErr.Check != "PK_COLUMN_CHECK" {
		t.Fatalf("Expected PK_COLUMN_CHECK, got %s", preflightErr.Check)
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

func TestValidateTablesExist_ExactCaseRequired(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	tables := []string{"Users"}
	mock.ExpectQuery("SELECT TABLE_NAME FROM information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME"}).AddRow("users"))

	err := checker.ValidateTablesExist(ctx, tables)
	if err == nil {
		t.Fatal("expected case-sensitive table mismatch error")
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

func TestValidateStorageEngine_EmptyTables(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)

	err := checker.ValidateStorageEngine(context.Background(), []string{})
	if err != nil {
		t.Fatalf("expected no error for empty tables, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected query execution: %v", err)
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

func TestCheckDeleteTriggers_EmptyTablesInput(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)

	triggers, err := checker.CheckDeleteTriggers(context.Background(), []string{})
	if err != nil {
		t.Fatalf("expected no error for empty tables, got: %v", err)
	}
	if len(triggers) != 0 {
		t.Fatalf("expected no triggers, got %d", len(triggers))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected query execution: %v", err)
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

func TestConfigureDestination_Success(t *testing.T) {
	sourceDB, _, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, _, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()

	checker, err := NewPreflightChecker(sourceDB, "sourcedb", g, log)
	if err != nil {
		t.Fatalf("NewPreflightChecker failed: %v", err)
	}

	if err := checker.ConfigureDestination(destDB, "destdb", "destdb"); err != nil {
		t.Fatalf("ConfigureDestination failed: %v", err)
	}

	if checker.destinationDB != destDB {
		t.Fatal("destination DB was not set")
	}
	if checker.destinationDBName != "destdb" {
		t.Fatalf("expected destinationDBName=destdb, got %s", checker.destinationDBName)
	}
	if checker.jobSchemaName != "destdb" {
		t.Fatalf("expected jobSchemaName=destdb, got %s", checker.jobSchemaName)
	}
}

func TestValidateDestinationTablesExist_MissingTables(t *testing.T) {
	sourceDB, _, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(sourceDB, "sourcedb", g, log)
	_ = checker.ConfigureDestination(destDB, "destdb", "destdb")

	tables := []string{"users", "orders"}
	destRows := sqlmock.NewRows([]string{"TABLE_NAME"}).AddRow("users")
	destMock.ExpectQuery("SELECT TABLE_NAME").
		WithArgs("destdb").
		WillReturnRows(destRows)

	err := checker.ValidateDestinationTablesExist(context.Background(), tables)
	if err == nil {
		t.Fatal("expected destination table existence error")
	}
}

func TestValidateDestinationSchemaCompatibility_Mismatch(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(sourceDB, "sourcedb", g, log)
	_ = checker.ConfigureDestination(destDB, "destdb", "destdb")

	sourceRows := sqlmock.NewRows([]string{"ORDINAL_POSITION", "COLUMN_NAME", "COLUMN_TYPE", "IS_NULLABLE", "COLUMN_KEY", "EXTRA", "CHARACTER_SET_NAME", "COLLATION_NAME"}).
		AddRow(1, "id", "bigint(20)", "NO", "PRI", "", "", "").
		AddRow(2, "name", "varchar(255)", "YES", "", "", "", "")
	sourceMock.ExpectQuery("SELECT\\s+ORDINAL_POSITION,").
		WithArgs("sourcedb", "users").
		WillReturnRows(sourceRows)

	destRows := sqlmock.NewRows([]string{"ORDINAL_POSITION", "COLUMN_NAME", "COLUMN_TYPE", "IS_NULLABLE", "COLUMN_KEY", "EXTRA", "CHARACTER_SET_NAME", "COLLATION_NAME"}).
		AddRow(1, "id", "bigint(20)", "NO", "PRI", "", "", "").
		AddRow(2, "name", "varchar(100)", "YES", "", "", "", "") // mismatch
	destMock.ExpectQuery("SELECT\\s+ORDINAL_POSITION,").
		WithArgs("destdb", "users").
		WillReturnRows(destRows)

	err := checker.ValidateDestinationSchemaCompatibility(context.Background(), []string{"users"})
	if err == nil {
		t.Fatal("expected destination schema compatibility error")
	}
}

// runSchemaCompatibilityCheck wires sqlmock source/destination column rows for
// a single "users" table and returns the check result.
func runSchemaCompatibilityCheck(t *testing.T, sourceCols, destCols [][]driverValue) error {
	t.Helper()
	sourceDB, sourceMock, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(sourceDB, "sourcedb", g, log)
	_ = checker.ConfigureDestination(destDB, "destdb", "destdb")

	columns := []string{"ORDINAL_POSITION", "COLUMN_NAME", "COLUMN_TYPE", "IS_NULLABLE",
		"COLUMN_KEY", "EXTRA", "CHARACTER_SET_NAME", "COLLATION_NAME"}
	sourceRows := sqlmock.NewRows(columns)
	for _, row := range sourceCols {
		sourceRows.AddRow(row...)
	}
	sourceMock.ExpectQuery("SELECT\\s+ORDINAL_POSITION,").
		WithArgs("sourcedb", "users").
		WillReturnRows(sourceRows)

	destRows := sqlmock.NewRows(columns)
	for _, row := range destCols {
		destRows.AddRow(row...)
	}
	destMock.ExpectQuery("SELECT\\s+ORDINAL_POSITION,").
		WithArgs("destdb", "users").
		WillReturnRows(destRows)

	return checker.ValidateDestinationSchemaCompatibility(context.Background(), []string{"users"})
}

type driverValue = driver.Value

func TestValidateDestinationSchemaCompatibility_RelaxedDestination(t *testing.T) {
	tests := []struct {
		name       string
		sourceCols [][]driverValue
		destCols   [][]driverValue
		wantErr    bool
	}{
		{
			name: "destination may drop secondary index",
			sourceCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "auto_increment", "", ""},
				{2, "aiErrorId", "bigint", "YES", "MUL", "", "", ""},
			},
			destCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "auto_increment", "", ""},
				{2, "aiErrorId", "bigint", "YES", "", "", "", ""},
			},
			wantErr: false,
		},
		{
			name: "destination may add secondary index",
			sourceCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "name", "varchar(255)", "YES", "", "", "", ""},
			},
			destCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "name", "varchar(255)", "YES", "MUL", "", "", ""},
			},
			wantErr: false,
		},
		{
			name: "destination may drop unique index",
			sourceCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "email", "varchar(255)", "NO", "UNI", "", "", ""},
			},
			destCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "email", "varchar(255)", "NO", "", "", "", ""},
			},
			wantErr: false,
		},
		{
			name: "destination may drop auto_increment",
			sourceCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "auto_increment", "", ""},
			},
			destCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
			},
			wantErr: false,
		},
		{
			name: "destination may drop DEFAULT_GENERATED and on update",
			sourceCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "updated_at", "timestamp", "NO", "", "DEFAULT_GENERATED on update CURRENT_TIMESTAMP", "", ""},
			},
			destCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "updated_at", "timestamp", "NO", "", "", "", ""},
			},
			wantErr: false,
		},
		{
			name: "destination may be more permissive about NULLs",
			sourceCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "name", "varchar(255)", "NO", "", "", "", ""},
			},
			destCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "name", "varchar(255)", "YES", "", "", "", ""},
			},
			wantErr: false,
		},
		{
			name: "destination missing primary key is rejected",
			sourceCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
			},
			destCols: [][]driverValue{
				{1, "id", "bigint", "NO", "", "", "", ""},
			},
			wantErr: true,
		},
		{
			name: "destination stricter NULLability is rejected",
			sourceCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "name", "varchar(255)", "YES", "", "", "", ""},
			},
			destCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "name", "varchar(255)", "NO", "", "", "", ""},
			},
			wantErr: true,
		},
		{
			name: "destination-only unique index is rejected",
			sourceCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "email", "varchar(255)", "NO", "", "", "", ""},
			},
			destCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "email", "varchar(255)", "NO", "UNI", "", "", ""},
			},
			wantErr: true,
		},
		{
			name: "destination-only generated column is rejected",
			sourceCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "total", "decimal(10,2)", "YES", "", "", "", ""},
			},
			destCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "total", "decimal(10,2)", "YES", "", "STORED GENERATED", "", ""},
			},
			wantErr: true,
		},
		{
			name: "generated column on both sides is rejected (copy cannot insert into it)",
			sourceCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "total", "decimal(10,2)", "YES", "", "STORED GENERATED", "", ""},
			},
			destCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "total", "decimal(10,2)", "YES", "", "STORED GENERATED", "", ""},
			},
			wantErr: true,
		},
		{
			name: "source generated column with plain destination is allowed",
			sourceCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "total", "decimal(10,2)", "YES", "", "STORED GENERATED", "", ""},
			},
			destCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "total", "decimal(10,2)", "YES", "", "", "", ""},
			},
			wantErr: false,
		},
		{
			name: "column type mismatch is rejected",
			sourceCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "name", "varchar(255)", "YES", "", "", "", ""},
			},
			destCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "name", "varchar(100)", "YES", "", "", "", ""},
			},
			wantErr: true,
		},
		{
			// MySQL < 8.0.17 reports bigint(20); 8.0.17+ reports bigint. The
			// display width is cosmetic, so these must compare equal.
			name: "integer display width difference is allowed",
			sourceCols: [][]driverValue{
				{1, "id", "bigint(20)", "NO", "PRI", "auto_increment", "", ""},
				{2, "qty", "int(11)", "YES", "", "", "", ""},
			},
			destCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "auto_increment", "", ""},
				{2, "qty", "int", "YES", "", "", "", ""},
			},
			wantErr: false,
		},
		{
			// unsigned changes the value range, so width normalization must not
			// erase it: int(10) unsigned vs int must still mismatch.
			name: "unsigned difference is still rejected despite width",
			sourceCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "qty", "int(10) unsigned", "YES", "", "", "", ""},
			},
			destCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "qty", "int", "YES", "", "", "", ""},
			},
			wantErr: true,
		},
		{
			name: "charset mismatch is rejected under count verification",
			sourceCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "name", "varchar(255)", "YES", "", "", "utf8mb4", "utf8mb4_0900_ai_ci"},
			},
			destCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "name", "varchar(255)", "YES", "", "", "latin1", "latin1_swedish_ci"},
			},
			wantErr: true,
		},
		{
			name: "collation-only mismatch is allowed (warn only)",
			sourceCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "name", "varchar(255)", "YES", "", "", "utf8mb4", "utf8mb4_0900_ai_ci"},
			},
			destCols: [][]driverValue{
				{1, "id", "bigint", "NO", "PRI", "", "", ""},
				{2, "name", "varchar(255)", "YES", "", "", "utf8mb4", "utf8mb4_general_ci"},
			},
			wantErr: false,
		},
		{
			name: "identical charsets pass",
			sourceCols: [][]driverValue{
				{1, "name", "varchar(255)", "YES", "", "", "utf8mb4", "utf8mb4_0900_ai_ci"},
			},
			destCols: [][]driverValue{
				{1, "name", "varchar(255)", "YES", "", "", "utf8mb4", "utf8mb4_0900_ai_ci"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := runSchemaCompatibilityCheck(t, tt.sourceCols, tt.destCols)
			if tt.wantErr && err == nil {
				t.Fatal("expected schema compatibility error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("expected schemas to be compatible, got: %v", err)
			}
		})
	}
}

func newWritePermChecker(t *testing.T) (*PreflightChecker, sqlmock.Sqlmock, func()) {
	t.Helper()
	sourceDB, _, _ := sqlmock.New()
	destDB, destMock, _ := sqlmock.New()
	g := createPreflightTestGraph()
	checker, _ := NewPreflightChecker(sourceDB, "sourcedb", g, logger.NewDefault())
	_ = checker.ConfigureDestination(destDB, "destdb", "destdb")
	cleanup := func() { _ = sourceDB.Close(); _ = destDB.Close() }
	return checker, destMock, cleanup
}

func expectGrantees(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT CURRENT_USER()").
		WillReturnRows(sqlmock.NewRows([]string{"CURRENT_USER()"}).AddRow("archiver@%"))
	mock.ExpectQuery("SELECT CURRENT_ROLE()").
		WillReturnRows(sqlmock.NewRows([]string{"CURRENT_ROLE()"}).AddRow("NONE"))
}

func TestValidateDestinationWritePermissions_GlobalGrant(t *testing.T) {
	checker, destMock, cleanup := newWritePermChecker(t)
	defer cleanup()

	expectGrantees(destMock)
	destMock.ExpectQuery("FROM information_schema.USER_PRIVILEGES\\s+WHERE GRANTEE IN \\(\\?\\)").
		WithArgs("'archiver'@'%'", "INSERT").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))

	if err := checker.ValidateDestinationWritePermissions(context.Background(), []string{"users"}); err != nil {
		t.Fatalf("global INSERT grant should pass, got: %v", err)
	}
}

func TestValidateDestinationWritePermissions_RoleGrant(t *testing.T) {
	checker, destMock, cleanup := newWritePermChecker(t)
	defer cleanup()

	destMock.ExpectQuery("SELECT CURRENT_USER()").
		WillReturnRows(sqlmock.NewRows([]string{"CURRENT_USER()"}).AddRow("archiver@%"))
	destMock.ExpectQuery("SELECT CURRENT_ROLE()").
		WillReturnRows(sqlmock.NewRows([]string{"CURRENT_ROLE()"}).AddRow("`app_writer`@`%`"))
	destMock.ExpectQuery("FROM information_schema.USER_PRIVILEGES\\s+WHERE GRANTEE IN \\(\\?,\\?\\)").
		WithArgs("'archiver'@'%'", "'app_writer'@'%'", "INSERT").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))

	if err := checker.ValidateDestinationWritePermissions(context.Background(), []string{"users"}); err != nil {
		t.Fatalf("role-held global INSERT grant should pass, got: %v", err)
	}
}

func TestValidateDestinationWritePermissions_SchemaGrant(t *testing.T) {
	checker, destMock, cleanup := newWritePermChecker(t)
	defer cleanup()

	expectGrantees(destMock)
	destMock.ExpectQuery("FROM information_schema.USER_PRIVILEGES\\s+WHERE GRANTEE IN \\(\\?\\)").
		WithArgs("'archiver'@'%'", "INSERT").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))
	destMock.ExpectQuery("FROM information_schema.SCHEMA_PRIVILEGES\\s+WHERE GRANTEE IN \\(\\?\\)").
		WithArgs("'archiver'@'%'", "INSERT", "destdb").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))

	if err := checker.ValidateDestinationWritePermissions(context.Background(), []string{"users"}); err != nil {
		t.Fatalf("schema INSERT grant should pass, got: %v", err)
	}
}

func TestValidateDestinationWritePermissions_TableGrant(t *testing.T) {
	checker, destMock, cleanup := newWritePermChecker(t)
	defer cleanup()

	expectGrantees(destMock)
	destMock.ExpectQuery("FROM information_schema.USER_PRIVILEGES\\s+WHERE GRANTEE IN \\(\\?\\)").
		WithArgs("'archiver'@'%'", "INSERT").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))
	destMock.ExpectQuery("FROM information_schema.SCHEMA_PRIVILEGES\\s+WHERE GRANTEE IN \\(\\?\\)").
		WithArgs("'archiver'@'%'", "INSERT", "destdb").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))
	destMock.ExpectQuery("FROM information_schema.TABLE_PRIVILEGES\\s+WHERE GRANTEE IN \\(\\?\\)").
		WithArgs("'archiver'@'%'", "INSERT", "destdb", "users").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))

	if err := checker.ValidateDestinationWritePermissions(context.Background(), []string{"users"}); err != nil {
		t.Fatalf("table-level INSERT grant should pass, got: %v", err)
	}
}

func TestValidateDestinationWritePermissions_NoInsertPrivilege(t *testing.T) {
	checker, destMock, cleanup := newWritePermChecker(t)
	defer cleanup()

	expectGrantees(destMock)
	destMock.ExpectQuery("FROM information_schema.USER_PRIVILEGES\\s+WHERE GRANTEE IN \\(\\?\\)").
		WithArgs("'archiver'@'%'", "INSERT").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))
	destMock.ExpectQuery("FROM information_schema.SCHEMA_PRIVILEGES\\s+WHERE GRANTEE IN \\(\\?\\)").
		WithArgs("'archiver'@'%'", "INSERT", "destdb").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))
	destMock.ExpectQuery("FROM information_schema.TABLE_PRIVILEGES\\s+WHERE GRANTEE IN \\(\\?\\)").
		WithArgs("'archiver'@'%'", "INSERT", "destdb", "users").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))

	err := checker.ValidateDestinationWritePermissions(context.Background(), []string{"users"})
	if err == nil {
		t.Fatal("expected write permission error, got nil")
	}
	if !strings.Contains(err.Error(), "DEST_WRITE_PERMISSION_CHECK") {
		t.Fatalf("expected DEST_WRITE_PERMISSION_CHECK, got: %v", err)
	}
}

func TestValidateDestinationInsertTriggers_WithTriggers(t *testing.T) {
	sourceDB, _, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(sourceDB, "sourcedb", g, log)
	_ = checker.ConfigureDestination(destDB, "destdb", "destdb")

	destMock.ExpectQuery("SELECT EVENT_OBJECT_TABLE, TRIGGER_NAME FROM information_schema.TRIGGERS").
		WithArgs("destdb", "users").
		WillReturnRows(sqlmock.NewRows([]string{"EVENT_OBJECT_TABLE", "TRIGGER_NAME"}).
			AddRow("users", "trg_users_insert"))

	err := checker.ValidateDestinationInsertTriggers(context.Background(), []string{"users"})
	if err == nil {
		t.Fatal("expected destination INSERT trigger error")
	}

	preflightErr, ok := err.(*PreflightError)
	if !ok {
		t.Fatalf("Expected PreflightError, got %T", err)
	}
	if preflightErr.Check != "DEST_INSERT_TRIGGER_CHECK" {
		t.Fatalf("Expected DEST_INSERT_TRIGGER_CHECK, got %s", preflightErr.Check)
	}
}

func TestCheckInsertTriggers_EmptyTablesInput(t *testing.T) {
	sourceDB, _, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(sourceDB, "sourcedb", g, log)
	_ = checker.ConfigureDestination(destDB, "destdb", "destdb")

	triggers, err := checker.CheckInsertTriggers(context.Background(), []string{})
	if err != nil {
		t.Fatalf("expected no error for empty tables, got: %v", err)
	}
	if len(triggers) != 0 {
		t.Fatalf("expected no triggers, got %d", len(triggers))
	}
	if err := destMock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected query execution: %v", err)
	}
}

func TestValidateForeignKeyCoverage_FailsForUncoveredCascadeAndRestrict(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)

	mock.ExpectQuery("SELECT\\s+kcu\\.TABLE_NAME,").
		WillReturnRows(sqlmock.NewRows([]string{
			"table_name", "constraint_name", "column_name",
			"referenced_table_name", "referenced_column_name", "delete_rule", "update_rule",
		}).
			AddRow("external_cascade", "fk_ext_orders_1", "order_id", "orders", "id", "CASCADE", "RESTRICT").
			AddRow("external_restrict", "fk_ext_orders_2", "order_id", "orders", "id", "RESTRICT", "RESTRICT"))

	err := checker.ValidateForeignKeyCoverage(context.Background())
	if err == nil {
		t.Fatal("expected FK coverage error for uncovered references")
	}

	preflightErr, ok := err.(*PreflightError)
	if !ok {
		t.Fatalf("Expected PreflightError, got %T", err)
	}
	if preflightErr.Check != "FK_COVERAGE_CHECK" {
		t.Fatalf("Expected FK_COVERAGE_CHECK, got %s", preflightErr.Check)
	}
	if !strings.Contains(preflightErr.Error(), "ON DELETE CASCADE") {
		t.Fatalf("expected CASCADE rule in error message, got: %v", preflightErr)
	}
	if !strings.Contains(preflightErr.Error(), "ON DELETE RESTRICT") {
		t.Fatalf("expected RESTRICT rule in error message, got: %v", preflightErr)
	}
}

// ============================================================================
// ValidateInternalFKCoverage Tests
// ============================================================================

func TestValidateInternalFKCoverage_FlatConfigMissingNesting(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	// Graph: orders -> order_items, orders -> item_shipments (flat, both siblings)
	// But DB has: item_shipments.item_id -> order_items.item_id (nested FK)
	g := graph.NewGraph("orders", "order_id")
	g.AddNode("order_items", &graph.Node{Name: "order_items", ForeignKey: "order_id", ReferenceKey: "order_id", DependencyType: "1-N"})
	g.AddNode("item_shipments", &graph.Node{Name: "item_shipments", ForeignKey: "order_id", ReferenceKey: "order_id", DependencyType: "1-N"})
	g.SetPK("order_items", "item_id")
	g.SetPK("item_shipments", "shipment_id")
	g.AddEdgeWithMeta("orders", "order_items", "order_id", "order_id", "1-N")
	g.AddEdgeWithMeta("orders", "item_shipments", "order_id", "order_id", "1-N")

	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)

	// DB reports an FK from item_shipments.item_id -> order_items.item_id
	// This FK is NOT represented in the graph (item_shipments is sibling, not child of order_items)
	mock.ExpectQuery("SELECT\\s+kcu\\.TABLE_NAME,").
		WillReturnRows(sqlmock.NewRows([]string{
			"table_name", "constraint_name", "column_name",
			"referenced_table_name", "referenced_column_name", "delete_rule", "update_rule",
		}).
			AddRow("order_items", "fk_items_orders", "order_id", "orders", "order_id", "RESTRICT", "RESTRICT").
			AddRow("item_shipments", "fk_ship_orders", "order_id", "orders", "order_id", "RESTRICT", "RESTRICT").
			AddRow("item_shipments", "fk_ship_items", "item_id", "order_items", "item_id", "RESTRICT", "RESTRICT"))

	// isColumnIndexed queries for each in-graph FK row
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	err := checker.ValidateInternalFKCoverage(context.Background())
	if err == nil {
		t.Fatal("expected INTERNAL_FK_COVERAGE error for flat config with nested DB FK")
	}

	preflightErr, ok := err.(*PreflightError)
	if !ok {
		t.Fatalf("expected PreflightError, got %T", err)
	}
	if preflightErr.Check != "INTERNAL_FK_COVERAGE" {
		t.Fatalf("expected INTERNAL_FK_COVERAGE check, got %s", preflightErr.Check)
	}
	if !strings.Contains(preflightErr.Error(), "item_shipments") {
		t.Fatalf("expected error to mention item_shipments, got: %v", preflightErr)
	}
	if !strings.Contains(preflightErr.Error(), "no graph edge") {
		t.Fatalf("expected 'no graph edge' reason, got: %v", preflightErr)
	}
}

func TestValidateInternalFKCoverage_ProperlyNestedConfig(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	// Graph: orders -> order_items -> item_shipments (properly nested)
	g := graph.NewGraph("orders", "order_id")
	g.AddNode("order_items", &graph.Node{Name: "order_items", ForeignKey: "order_id", ReferenceKey: "order_id", DependencyType: "1-N"})
	g.AddNode("item_shipments", &graph.Node{Name: "item_shipments", ForeignKey: "item_id", ReferenceKey: "item_id", DependencyType: "1-N"})
	g.SetPK("order_items", "item_id")
	g.SetPK("item_shipments", "shipment_id")
	g.AddEdgeWithMeta("orders", "order_items", "order_id", "order_id", "1-N")
	g.AddEdgeWithMeta("order_items", "item_shipments", "item_id", "item_id", "1-N")

	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)

	mock.ExpectQuery("SELECT\\s+kcu\\.TABLE_NAME,").
		WillReturnRows(sqlmock.NewRows([]string{
			"table_name", "constraint_name", "column_name",
			"referenced_table_name", "referenced_column_name", "delete_rule", "update_rule",
		}).
			AddRow("order_items", "fk_items_orders", "order_id", "orders", "order_id", "RESTRICT", "RESTRICT").
			AddRow("item_shipments", "fk_ship_items", "item_id", "order_items", "item_id", "RESTRICT", "RESTRICT"))

	// isColumnIndexed for each in-graph FK row
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	err := checker.ValidateInternalFKCoverage(context.Background())
	if err != nil {
		t.Fatalf("expected no error for properly nested config, got: %v", err)
	}
}

func TestValidateInternalFKCoverage_WrongFKColumn(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	// Graph: orders -> payments with FK column "cust_id"
	// But DB has: payments.customer_id -> orders.order_id
	g := graph.NewGraph("orders", "order_id")
	g.AddNode("payments", &graph.Node{Name: "payments", ForeignKey: "cust_id", ReferenceKey: "order_id", DependencyType: "1-N"})
	g.SetPK("payments", "payment_id")
	g.AddEdgeWithMeta("orders", "payments", "cust_id", "order_id", "1-N")

	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)

	mock.ExpectQuery("SELECT\\s+kcu\\.TABLE_NAME,").
		WillReturnRows(sqlmock.NewRows([]string{
			"table_name", "constraint_name", "column_name",
			"referenced_table_name", "referenced_column_name", "delete_rule", "update_rule",
		}).
			AddRow("payments", "fk_pay_orders", "customer_id", "orders", "order_id", "RESTRICT", "RESTRICT"))

	// isColumnIndexed for payments (in graph)
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	err := checker.ValidateInternalFKCoverage(context.Background())
	if err == nil {
		t.Fatal("expected error for FK column mismatch")
	}

	preflightErr, ok := err.(*PreflightError)
	if !ok {
		t.Fatalf("expected PreflightError, got %T", err)
	}
	if !strings.Contains(preflightErr.Error(), "FK column mismatch") {
		t.Fatalf("expected 'FK column mismatch' in error, got: %v", preflightErr)
	}
	if !strings.Contains(preflightErr.Error(), "config has 'cust_id'") {
		t.Fatalf("expected config column in error, got: %v", preflightErr)
	}
	if !strings.Contains(preflightErr.Error(), "DB has 'customer_id'") {
		t.Fatalf("expected DB column in error, got: %v", preflightErr)
	}
}

func TestValidateInternalFKCoverage_WrongReferenceColumn(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	// Graph: orders (PK: order_id) -> line_items with FK "order_id" referencing "order_id"
	// But DB has: line_items.order_id -> orders.id (different referenced column)
	g := graph.NewGraph("orders", "order_id")
	g.AddNode("line_items", &graph.Node{Name: "line_items", ForeignKey: "order_id", ReferenceKey: "order_id", DependencyType: "1-N"})
	g.SetPK("line_items", "line_id")
	g.AddEdgeWithMeta("orders", "line_items", "order_id", "order_id", "1-N")

	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)

	mock.ExpectQuery("SELECT\\s+kcu\\.TABLE_NAME,").
		WillReturnRows(sqlmock.NewRows([]string{
			"table_name", "constraint_name", "column_name",
			"referenced_table_name", "referenced_column_name", "delete_rule", "update_rule",
		}).
			AddRow("line_items", "fk_line_orders", "order_id", "orders", "id", "RESTRICT", "RESTRICT"))

	// isColumnIndexed for line_items (in graph)
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	err := checker.ValidateInternalFKCoverage(context.Background())
	if err == nil {
		t.Fatal("expected error for reference column mismatch")
	}

	preflightErr, ok := err.(*PreflightError)
	if !ok {
		t.Fatalf("expected PreflightError, got %T", err)
	}
	if !strings.Contains(preflightErr.Error(), "reference column mismatch") {
		t.Fatalf("expected 'reference column mismatch' in error, got: %v", preflightErr)
	}
	if !strings.Contains(preflightErr.Error(), "config PK is 'order_id'") {
		t.Fatalf("expected config PK in error, got: %v", preflightErr)
	}
	if !strings.Contains(preflightErr.Error(), "DB references 'id'") {
		t.Fatalf("expected DB reference in error, got: %v", preflightErr)
	}
}

func TestValidateInternalFKCoverage_NoInternalFKs(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)

	// Only external FKs returned - no internal ones between graph tables
	mock.ExpectQuery("SELECT\\s+kcu\\.TABLE_NAME,").
		WillReturnRows(sqlmock.NewRows([]string{
			"table_name", "constraint_name", "column_name",
			"referenced_table_name", "referenced_column_name", "delete_rule", "update_rule",
		}).
			AddRow("external_table", "fk_ext", "user_id", "users", "id", "RESTRICT", "RESTRICT"))

	err := checker.ValidateInternalFKCoverage(context.Background())
	if err != nil {
		t.Fatalf("expected no error when no internal FKs exist, got: %v", err)
	}
}

func TestValidateInternalFKCoverage_SelfReferencingFK(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := graph.NewGraph("categories", "id")

	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)

	// Self-referencing FK: categories.parent_id -> categories.id
	mock.ExpectQuery("SELECT\\s+kcu\\.TABLE_NAME,").
		WillReturnRows(sqlmock.NewRows([]string{
			"table_name", "constraint_name", "column_name",
			"referenced_table_name", "referenced_column_name", "delete_rule", "update_rule",
		}).
			AddRow("categories", "fk_cat_parent", "parent_id", "categories", "id", "SET NULL", "RESTRICT"))

	// isColumnIndexed for categories (in graph)
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	err := checker.ValidateInternalFKCoverage(context.Background())
	if err != nil {
		t.Fatalf("expected no error for self-referencing FK, got: %v", err)
	}
}

func TestValidateInternalFKCoverage_MultipleFailures(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	// Graph: orders -> order_items, orders -> item_shipments (flat), orders -> payments (wrong FK col)
	g := graph.NewGraph("orders", "order_id")
	g.AddNode("order_items", &graph.Node{Name: "order_items", ForeignKey: "order_id", ReferenceKey: "order_id", DependencyType: "1-N"})
	g.AddNode("item_shipments", &graph.Node{Name: "item_shipments", ForeignKey: "order_id", ReferenceKey: "order_id", DependencyType: "1-N"})
	g.AddNode("payments", &graph.Node{Name: "payments", ForeignKey: "wrong_col", ReferenceKey: "order_id", DependencyType: "1-N"})
	g.SetPK("order_items", "item_id")
	g.SetPK("item_shipments", "shipment_id")
	g.SetPK("payments", "payment_id")
	g.AddEdgeWithMeta("orders", "order_items", "order_id", "order_id", "1-N")
	g.AddEdgeWithMeta("orders", "item_shipments", "order_id", "order_id", "1-N")
	g.AddEdgeWithMeta("orders", "payments", "wrong_col", "order_id", "1-N")

	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)

	mock.ExpectQuery("SELECT\\s+kcu\\.TABLE_NAME,").
		WillReturnRows(sqlmock.NewRows([]string{
			"table_name", "constraint_name", "column_name",
			"referenced_table_name", "referenced_column_name", "delete_rule", "update_rule",
		}).
			AddRow("order_items", "fk_items_orders", "order_id", "orders", "order_id", "RESTRICT", "RESTRICT").
			AddRow("item_shipments", "fk_ship_items", "item_id", "order_items", "item_id", "RESTRICT", "RESTRICT").
			AddRow("payments", "fk_pay_orders", "customer_id", "orders", "order_id", "RESTRICT", "RESTRICT"))

	// isColumnIndexed for each in-graph FK row
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	err := checker.ValidateInternalFKCoverage(context.Background())
	if err == nil {
		t.Fatal("expected error for multiple failures")
	}

	preflightErr, ok := err.(*PreflightError)
	if !ok {
		t.Fatalf("expected PreflightError, got %T", err)
	}

	errMsg := preflightErr.Error()
	if !strings.Contains(errMsg, "no graph edge") {
		t.Fatalf("expected 'no graph edge' for item_shipments, got: %v", preflightErr)
	}
	if !strings.Contains(errMsg, "FK column mismatch") {
		t.Fatalf("expected 'FK column mismatch' for payments, got: %v", preflightErr)
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
// Nil Destination Safety Tests
// ============================================================================

func TestDestinationMethods_NilDestination(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock: %v", err)
	}
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, err := NewPreflightChecker(db, "sourcedb", g, log)
	if err != nil {
		t.Fatalf("NewPreflightChecker failed: %v", err)
	}
	// Do NOT call ConfigureDestination - destinationDB stays nil

	ctx := context.Background()
	tables := []string{"users"}

	err = checker.ValidateDestinationTablesExist(ctx, tables)
	if err == nil {
		t.Fatal("ValidateDestinationTablesExist should return error when destination is nil")
	}
	if !strings.Contains(err.Error(), "destination database not configured") {
		t.Errorf("Unexpected error message: %v", err)
	}

	err = checker.ValidateDestinationSchemaCompatibility(ctx, tables)
	if err == nil {
		t.Fatal("ValidateDestinationSchemaCompatibility should return error when destination is nil")
	}
	if !strings.Contains(err.Error(), "destination database not configured") {
		t.Errorf("Unexpected error message: %v", err)
	}

	err = checker.ValidateDestinationWritePermissions(ctx, tables)
	if err == nil {
		t.Fatal("ValidateDestinationWritePermissions should return error when destination is nil")
	}
	if !strings.Contains(err.Error(), "destination database not configured") {
		t.Errorf("Unexpected error message: %v", err)
	}

	err = checker.ValidateDestinationInsertTriggers(ctx, tables)
	if err == nil {
		t.Fatal("ValidateDestinationInsertTriggers should return error when destination is nil")
	}
	if !strings.Contains(err.Error(), "destination database not configured") {
		t.Errorf("Unexpected error message: %v", err)
	}

	triggers, err := checker.CheckInsertTriggers(ctx, tables)
	if err == nil {
		t.Fatal("CheckInsertTriggers should return error when destination is nil")
	}
	if triggers != nil {
		t.Errorf("Expected nil triggers, got %v", triggers)
	}
	if !strings.Contains(err.Error(), "destination database not configured") {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestSchemaCompatibility_CharsetMismatchAllowedUnderSHA256(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()

	g := createPreflightTestGraph()
	checker, _ := NewPreflightChecker(sourceDB, "sourcedb", g, logger.NewDefault())
	_ = checker.ConfigureDestination(destDB, "destdb", "destdb")
	checker.SetVerification(config.VerificationConfig{Method: "sha256", SkipVerification: false})

	columns := []string{"ORDINAL_POSITION", "COLUMN_NAME", "COLUMN_TYPE", "IS_NULLABLE",
		"COLUMN_KEY", "EXTRA", "CHARACTER_SET_NAME", "COLLATION_NAME"}
	sourceRows := sqlmock.NewRows(columns).
		AddRow(1, "name", "varchar(255)", "YES", "", "", "utf8mb4", "utf8mb4_0900_ai_ci")
	sourceMock.ExpectQuery("SELECT\\s+ORDINAL_POSITION,").WithArgs("sourcedb", "users").WillReturnRows(sourceRows)
	destRows := sqlmock.NewRows(columns).
		AddRow(1, "name", "varchar(255)", "YES", "", "", "latin1", "latin1_swedish_ci")
	destMock.ExpectQuery("SELECT\\s+ORDINAL_POSITION,").WithArgs("destdb", "users").WillReturnRows(destRows)

	err := checker.ValidateDestinationSchemaCompatibility(context.Background(), []string{"users"})
	if err != nil {
		t.Fatalf("charset mismatch should be allowed (warn) under sha256 verification, got: %v", err)
	}
}

// ============================================================================
// Source DELETE Privilege Tests (Task 6)
// ============================================================================

func TestValidateSourceDeletePermissions_Missing(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()

	g := createPreflightTestGraph()
	checker, _ := NewPreflightChecker(sourceDB, "sourcedb", g, logger.NewDefault())

	sourceMock.ExpectQuery("SELECT CURRENT_USER()").
		WillReturnRows(sqlmock.NewRows([]string{"CURRENT_USER()"}).AddRow("archiver@%"))
	sourceMock.ExpectQuery("SELECT CURRENT_ROLE()").
		WillReturnRows(sqlmock.NewRows([]string{"CURRENT_ROLE()"}).AddRow("NONE"))
	sourceMock.ExpectQuery("FROM information_schema.USER_PRIVILEGES\\s+WHERE GRANTEE IN \\(\\?\\)").
		WithArgs("'archiver'@'%'", "DELETE").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))
	sourceMock.ExpectQuery("FROM information_schema.SCHEMA_PRIVILEGES\\s+WHERE GRANTEE IN \\(\\?\\)").
		WithArgs("'archiver'@'%'", "DELETE", "sourcedb").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))
	sourceMock.ExpectQuery("FROM information_schema.TABLE_PRIVILEGES\\s+WHERE GRANTEE IN \\(\\?\\)").
		WithArgs("'archiver'@'%'", "DELETE", "sourcedb", "users").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))

	err := checker.ValidateSourceDeletePermissions(context.Background(), []string{"users"})
	if err == nil {
		t.Fatal("expected source delete permission error, got nil")
	}
	if !strings.Contains(err.Error(), "SOURCE_DELETE_PERMISSION_CHECK") {
		t.Fatalf("expected SOURCE_DELETE_PERMISSION_CHECK, got: %v", err)
	}
}

// ============================================================================
// Grantee Resolution Helper Tests (Task 4)
// ============================================================================

func TestFormatGrantee(t *testing.T) {
	tests := []struct{ in, want string }{
		{"archiver@10.0.0.5", "'archiver'@'10.0.0.5'"},
		{"root@%", "'root'@'%'"},
		// Embedded quote NOT escaped — verified on MySQL 8.4: the GRANTEE
		// column concatenates quotes around the raw name without doubling.
		{"o'brien@%", "'o'brien'@'%'"},
	}
	for _, tt := range tests {
		if got := formatGrantee(tt.in); got != tt.want {
			t.Errorf("formatGrantee(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestRoleGrantees(t *testing.T) {
	got := roleGrantees("`app_writer`@`%`, `auditor`@`localhost`")
	want := []string{"'app_writer'@'%'", "'auditor'@'localhost'"}
	if len(got) != len(want) {
		t.Fatalf("roleGrantees returned %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("role %d = %q, want %q", i, got[i], want[i])
		}
	}
	if r := roleGrantees("NONE"); len(r) != 0 {
		t.Errorf("roleGrantees(NONE) = %v, want empty", r)
	}
	if r := roleGrantees(""); len(r) != 0 {
		t.Errorf("roleGrantees(\"\") = %v, want empty", r)
	}
}

// ============================================================================
// ValidateJobSchemaPermissions Tests
// ============================================================================

func TestValidateJobSchemaPermissions_AllMissing(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	p := &PreflightChecker{logger: logger.NewDefault(), destinationDB: db, destinationDBName: "destdb", jobSchemaName: "goarchive"}

	mock.ExpectQuery("SELECT CURRENT_USER\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"u"}).AddRow("svc@%"))
	mock.ExpectQuery("SELECT CURRENT_ROLE\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"r"}).AddRow("NONE"))
	// Loop over ["CREATE","SELECT","INSERT","UPDATE"]: global+schema check per priv.
	// CREATE: global=0, schema=0 -> missing
	mock.ExpectQuery("USER_PRIVILEGES").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	mock.ExpectQuery("SCHEMA_PRIVILEGES").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	// SELECT: global=0, schema=0 -> missing
	mock.ExpectQuery("USER_PRIVILEGES").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	mock.ExpectQuery("SCHEMA_PRIVILEGES").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	// INSERT: global=0, schema=0 -> missing
	mock.ExpectQuery("USER_PRIVILEGES").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	mock.ExpectQuery("SCHEMA_PRIVILEGES").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	// UPDATE: global=0, schema=0 -> missing
	mock.ExpectQuery("USER_PRIVILEGES").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	mock.ExpectQuery("SCHEMA_PRIVILEGES").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))

	err = p.ValidateJobSchemaPermissions(context.Background())
	if err == nil {
		t.Fatal("expected missing-privilege error")
	}
	var pe *PreflightError
	if !errors.As(err, &pe) || pe.Check != "JOB_SCHEMA_PERMISSION_CHECK" {
		t.Fatalf("expected JOB_SCHEMA_PERMISSION_CHECK PreflightError, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled mock expectations: %v", err)
	}
}

func TestValidateJobSchemaPermissions_GlobalGrant(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	p := &PreflightChecker{logger: logger.NewDefault(), destinationDB: db, destinationDBName: "destdb", jobSchemaName: "goarchive"}

	mock.ExpectQuery("SELECT CURRENT_USER\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"u"}).AddRow("svc@%"))
	mock.ExpectQuery("SELECT CURRENT_ROLE\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"r"}).AddRow("NONE"))
	// CREATE at global level
	mock.ExpectQuery("USER_PRIVILEGES").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(1))
	// SELECT at global level
	mock.ExpectQuery("USER_PRIVILEGES").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(1))
	// INSERT at global level
	mock.ExpectQuery("USER_PRIVILEGES").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(1))
	// UPDATE at global level
	mock.ExpectQuery("USER_PRIVILEGES").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(1))

	if err := p.ValidateJobSchemaPermissions(context.Background()); err != nil {
		t.Fatalf("expected all privileges to pass with global grants, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled mock expectations: %v", err)
	}
}

func TestValidateJobSchemaPermissions_SchemaGrant(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	p := &PreflightChecker{logger: logger.NewDefault(), destinationDB: db, destinationDBName: "destdb", jobSchemaName: "goarchive"}

	mock.ExpectQuery("SELECT CURRENT_USER\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"u"}).AddRow("svc@%"))
	mock.ExpectQuery("SELECT CURRENT_ROLE\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"r"}).AddRow("NONE"))
	// For each privilege: global=0, then schema=1
	for i := 0; i < 4; i++ {
		mock.ExpectQuery("USER_PRIVILEGES").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
		mock.ExpectQuery("SCHEMA_PRIVILEGES").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(1))
	}

	if err := p.ValidateJobSchemaPermissions(context.Background()); err != nil {
		t.Fatalf("expected all privileges to pass with schema grants, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled mock expectations: %v", err)
	}
}

func TestValidateJobSchemaPermissions_NilDestination(t *testing.T) {
	p := &PreflightChecker{logger: logger.NewDefault(), jobSchemaName: "goarchive"}
	err := p.ValidateJobSchemaPermissions(context.Background())
	if err == nil {
		t.Fatal("expected error for nil destination")
	}
	if !strings.Contains(err.Error(), "destination database not configured") {
		t.Fatalf("expected 'destination database not configured', got: %v", err)
	}
}

// TestValidateJobSchemaPermissions_OnlyCreateMissing verifies that when a
// grantee holds SELECT, INSERT, and UPDATE but not CREATE, the error message
// lists only CREATE and includes the CREATE DATABASE hint.
func TestValidateJobSchemaPermissions_OnlyCreateMissing(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	p := &PreflightChecker{logger: logger.NewDefault(), destinationDB: db, destinationDBName: "destdb", jobSchemaName: "goarchive"}

	mock.ExpectQuery("SELECT CURRENT_USER\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"u"}).AddRow("svc@%"))
	mock.ExpectQuery("SELECT CURRENT_ROLE\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"r"}).AddRow("NONE"))
	// CREATE: global=0, schema=0 -> missing
	mock.ExpectQuery("USER_PRIVILEGES").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	mock.ExpectQuery("SCHEMA_PRIVILEGES").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	// SELECT: global=0, schema=1 -> present
	mock.ExpectQuery("USER_PRIVILEGES").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	mock.ExpectQuery("SCHEMA_PRIVILEGES").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(1))
	// INSERT: global=0, schema=1 -> present
	mock.ExpectQuery("USER_PRIVILEGES").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	mock.ExpectQuery("SCHEMA_PRIVILEGES").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(1))
	// UPDATE: global=0, schema=1 -> present
	mock.ExpectQuery("USER_PRIVILEGES").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	mock.ExpectQuery("SCHEMA_PRIVILEGES").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(1))

	err = p.ValidateJobSchemaPermissions(context.Background())
	if err == nil {
		t.Fatal("expected JOB_SCHEMA_PERMISSION_CHECK error, got nil")
	}
	var pe *PreflightError
	if !errors.As(err, &pe) || pe.Check != "JOB_SCHEMA_PERMISSION_CHECK" {
		t.Fatalf("expected JOB_SCHEMA_PERMISSION_CHECK PreflightError, got %v", err)
	}
	msg := pe.Message
	if !strings.Contains(msg, "CREATE") {
		t.Errorf("expected message to mention CREATE, got: %s", msg)
	}
	if !strings.Contains(msg, "CREATE DATABASE") {
		t.Errorf("expected CREATE DATABASE hint in message, got: %s", msg)
	}
	// Must NOT list privileges the user already holds
	for _, held := range []string{"SELECT", "INSERT", "UPDATE"} {
		if strings.Contains(msg, held) {
			t.Errorf("message must not list already-held privilege %s, got: %s", held, msg)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled mock expectations: %v", err)
	}
}

// ============================================================================
// Integration Tests
// ============================================================================
