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
	mock.ExpectQuery("information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_TYPE", "ENGINE"}).
			AddRow("users", "BASE TABLE", "InnoDB").
			AddRow("orders", "BASE TABLE", "InnoDB"))
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

	mock.ExpectQuery("information_schema.STATISTICS AS s").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE",
		}).AddRow("users", "id", "bigint", "bigint(20) unsigned"))
	if err := checker.ValidateRootPKNumeric(context.Background(), newPreflightRun(checker)); err != nil {
		t.Fatalf("ValidateRootPKNumeric: %v", err)
	}
	dataType, unsigned, ok := g.GetRootPKMeta()
	if !ok || dataType != "bigint" || !unsigned {
		t.Fatalf("metadata: dataType=%q unsigned=%v ok=%v", dataType, unsigned, ok)
	}

	db2, mock2, _ := sqlmock.New()
	defer func() { _ = db2.Close() }()
	checker2, _ := NewPreflightChecker(db2, "testdb", graph.NewGraph("orders", "uuid"), nil)
	mock2.ExpectQuery("information_schema.STATISTICS AS s").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE",
		}).AddRow("orders", "uuid", "varchar", "varchar(36)"))
	err := checker2.ValidateRootPKNumeric(context.Background(), newPreflightRun(checker2))
	if err == nil || !strings.Contains(err.Error(), "ROOT_PK_TYPE_UNSUPPORTED") {
		t.Fatalf("expected ROOT_PK_TYPE_UNSUPPORTED, got %v", err)
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("ROOT_PK_TYPE_UNSUPPORTED must be a plain error, not *PreflightError: %v", err)
	}
}

func TestRunAllChecks_NonInnoDBTables(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	// Table existence check - all exist. The storage-engine stage now reads this same
	// memoized fact (position 1) instead of issuing its own query, so the MyISAM
	// defect must live in THIS row set.
	mock.ExpectQuery("information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_TYPE", "ENGINE"}).
			AddRow("users", "BASE TABLE", "InnoDB").
			AddRow("orders", "BASE TABLE", "MyISAM"). // Not allowed!
			AddRow("order_items", "BASE TABLE", "InnoDB"))

	// Primary key column existence check: each configured PK ("id") exists with the
	// exact same case in the general column fact. Graph.AllNodes() ranges over a map,
	// so the trailing bind args (one per requested table, after the schema) are
	// nondeterministic for this three-node graph — WithArgs is deliberately omitted
	// (phase 018, Step 7b).
	mock.ExpectQuery("SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, EXTRA").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE", "COLUMN_TYPE", "EXTRA",
		}).
			AddRow("users", "id", 1, "bigint", "bigint unsigned", "").
			AddRow("orders", "id", 1, "bigint", "bigint", "").
			AddRow("order_items", "id", 1, "bigint", "bigint", ""))
	// PK shape + root PK type now share ONE memoized Inspector.PrimaryKeys fact, so a
	// single query covers positions 3 and 4. Root is "users" and must be integer, or
	// position 4 aborts before the storage-engine check under test.
	mock.ExpectQuery("information_schema.STATISTICS AS s").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE",
		}).
			AddRow("users", "id", "bigint", "bigint unsigned").
			AddRow("orders", "id", "bigint", "bigint").
			AddRow("order_items", "id", "bigint", "bigint"))

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

	// Minimal graph containing users and orders (phase 018, Step 7b): the table set
	// now comes from run.tables (= graph.AllNodes()), so the mocked column fact must
	// cover exactly these two tables.
	g := graph.NewGraph("users", "id")
	g.AddNode("orders", &graph.Node{Name: "orders", ForeignKey: "user_id", ReferenceKey: "id", DependencyType: "1-N"})
	g.SetPK("orders", "id")
	g.AddEdge("users", "orders")
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	// orders has no "id" column at all: its column fact names a different column.
	// Graph.AllNodes() ranges over a map, so the trailing bind args are
	// nondeterministic for this two-node graph — WithArgs is deliberately omitted
	// (phase 018, Step 7b).
	mock.ExpectQuery("SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, EXTRA").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE", "COLUMN_TYPE", "EXTRA",
		}).
			AddRow("users", "id", 1, "bigint", "bigint", "").
			AddRow("orders", "order_num", 1, "bigint", "bigint", ""))

	err := checker.ValidatePrimaryKeyColumns(ctx, newPreflightRun(checker))
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
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	// A configured root plus an unconfigured "legacy" child. NewGraph always
	// configures its root, so the unconfigured table is added with AddNode and no
	// SetPK (phase 018, Step 7b).
	g := graph.NewGraph("users", "id")
	g.AddNode("legacy", &graph.Node{Name: "legacy"})
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)

	// The column fact is now fetched unconditionally for every graph table, even
	// though "legacy" is rejected without ever consulting its columns — so a Columns
	// expectation covering the configured root must still be registered, or the
	// stage returns a plain inspection error instead of *PreflightError. Graph.AllNodes()
	// ranges over a map, so the trailing bind args are nondeterministic for this
	// two-node graph — WithArgs is deliberately omitted (phase 018, Step 7b).
	mock.ExpectQuery("SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, EXTRA").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE", "COLUMN_TYPE", "EXTRA",
		}).AddRow("users", "id", 1, "bigint", "bigint", ""))

	err := checker.ValidatePrimaryKeyColumns(context.Background(), newPreflightRun(checker))
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

// TestValidatePrimaryKeyColumns_CaseMismatch verifies that a configured
// primary_key that matches the real column only case-insensitively (e.g.
// "LOG_ID" vs the actual "log_id") is rejected with a dedicated, clear
// case-mismatch error — NOT allowed to pass as "exists" and NOT reported via
// the data-loss-flavored PRIMARY_KEY_CHECK. Column names are case-sensitive.
func TestValidatePrimaryKeyColumns_CaseMismatch(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := graph.NewGraph("events", "LOG_ID")
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	// MySQL's information_schema.COLUMNS collates case-insensitively, so the real
	// column comes back as "log_id" even though the configured key is "LOG_ID".
	mock.ExpectQuery("SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, EXTRA").
		WithArgs("testdb", "events").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE", "COLUMN_TYPE", "EXTRA",
		}).AddRow("events", "log_id", 1, "bigint", "bigint", ""))

	err := checker.ValidatePrimaryKeyColumns(ctx, newPreflightRun(checker))
	if err == nil {
		t.Fatal("expected case-mismatch validation error, got nil")
	}

	preflightErr, ok := err.(*PreflightError)
	if !ok {
		t.Fatalf("Expected PreflightError, got %T: %v", err, err)
	}
	if preflightErr.Check != "PK_COLUMN_CASE_CHECK" {
		t.Fatalf("Expected PK_COLUMN_CASE_CHECK, got %s", preflightErr.Check)
	}
	// The message must name both the configured and the actual column so the
	// operator sees it is only a letter-case difference, and must call out
	// case-sensitivity rather than data loss.
	msg := err.Error()
	if !strings.Contains(msg, "LOG_ID") || !strings.Contains(msg, "log_id") {
		t.Errorf("error should name both configured and actual column, got: %s", msg)
	}
	if !strings.Contains(strings.ToLower(msg), "case") {
		t.Errorf("error should mention case-sensitivity, got: %s", msg)
	}
	if strings.Contains(msg, "over-match") {
		t.Errorf("case-mismatch error should not use the data-loss over-match wording, got: %s", msg)
	}
}

// TestValidatePrimaryKeyColumnsInspectionErrorIsPlain proves a failed column-fact fetch
// surfaces as a PLAIN error, never a *PreflightError. A *PreflightError means "the
// schema is wrong"; this means "we could not find out".
//
// The assertion is on goarchive's WRAPPER text, not the bare word "columns": the
// library's own op name for this call is literally "columns", so the raw *ObjectError
// already contains that substring. Asserting only the bare word would pass against an
// unwrapped `return err` — a plain error that is not a *PreflightError either, but not
// what this phase's DELIBERATE TEXT CHANGE (Step 6) actually produces. "pk_columns" is
// safe: the raw error does not contain that string.
func TestValidatePrimaryKeyColumnsInspectionErrorIsPlain(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	wantErr := errors.New("query failed")
	mock.ExpectQuery("SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, EXTRA").
		WillReturnError(wantErr)

	g := graph.NewGraph("orders", "id")
	p, _ := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())

	err = p.ValidatePrimaryKeyColumns(context.Background(), newPreflightRun(p))
	if err == nil {
		t.Fatal("expected an inspection error, got nil")
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("an inspection failure must be a plain error, got *PreflightError: %v", pe)
	}
	if !strings.Contains(err.Error(), "preflight pk_columns inspection failed") {
		t.Fatalf("inspection error must carry goarchive's wrapper, got: %v", err)
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected errors.Is to reach the original error, got: %v", err)
	}
}

// TestValidatePrimaryKeyColumnsConsumesTheCachedFact proves the stage reads the run's
// memoized column fact rather than fetching its own, which is the standing
// non-negotiable ("goarchive must never re-query information_schema for a fact the
// library answers").
//
// Exactly ONE expectation is registered and the explicit pre-load consumes it. A stage
// that ignored `run` and built its own Inspector would issue a second query, receive
// "all expectations were already fulfilled", and return that as a plain inspection
// error — failing the *PreflightError assertion below. mock.ExpectationsWereMet() would
// NOT catch this: the single expectation is consumed either way. The verdict
// discriminates, not the expectation bookkeeping.
func TestValidatePrimaryKeyColumnsConsumesTheCachedFact(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, EXTRA").
		WithArgs("srcdb", "orders").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE", "COLUMN_TYPE", "EXTRA",
		}).AddRow("orders", "note", 1, "varchar", "varchar(64)", ""))

	g := graph.NewGraph("orders", "id")
	p, _ := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())

	run := newPreflightRun(p)
	if _, err := run.sourceColumns(context.Background()); err != nil {
		t.Fatalf("pre-load of the column fact failed: %v", err)
	}

	err = p.ValidatePrimaryKeyColumns(context.Background(), run)

	var pe *PreflightError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PreflightError from the CACHED fact, got %T: %v", err, err)
	}
	if pe.Check != "PK_COLUMN_CHECK" {
		t.Fatalf("Check = %q", pe.Check)
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

	// The table list now comes from the run, which reads it from the graph.
	// createPreflightTestGraph's nodes are users, orders and order_items.
	mock.ExpectQuery("information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_TYPE", "ENGINE"}).
			AddRow("users", "BASE TABLE", "InnoDB").
			AddRow("orders", "BASE TABLE", "InnoDB").
			AddRow("order_items", "BASE TABLE", "InnoDB"))

	err := checker.ValidateTablesExist(ctx, newPreflightRun(checker))

	if err != nil {
		t.Fatalf("ValidateTablesExist failed: %v", err)
	}
}

func TestValidateTablesExist_MissingTables(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	// "nonexistent" is a graph node so that the run asks about it; the server
	// never reports it back, so it must be the one missing table.
	g := createPreflightTestGraph()
	g.AddNode("nonexistent", &graph.Node{Name: "nonexistent", ForeignKey: "order_id", ReferenceKey: "id", DependencyType: "1-N"})
	g.SetPK("nonexistent", "id")
	g.AddEdge("orders", "nonexistent")
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	mock.ExpectQuery("information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_TYPE", "ENGINE"}).
			AddRow("users", "BASE TABLE", "InnoDB").
			AddRow("orders", "BASE TABLE", "InnoDB").
			AddRow("order_items", "BASE TABLE", "InnoDB"))
	// Missing: nonexistent

	err := checker.ValidateTablesExist(ctx, newPreflightRun(checker))

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

	// The graph asks for "Users"; the server reports "users". A case-insensitive
	// match would wrongly pass.
	g := graph.NewGraph("Users", "id")
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	mock.ExpectQuery("information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_TYPE", "ENGINE"}).
			AddRow("users", "BASE TABLE", "InnoDB"))

	err := checker.ValidateTablesExist(ctx, newPreflightRun(checker))
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

	mock.ExpectQuery("information_schema.TABLES").
		WillReturnError(errors.New("query failed"))

	err := checker.ValidateTablesExist(ctx, newPreflightRun(checker))

	if err == nil {
		t.Error("Expected error for query failure")
	}

	// An inspection failure is not a schema verdict.
	var preflightErr *PreflightError
	if errors.As(err, &preflightErr) {
		t.Errorf("query failure must not surface as *PreflightError, got %v", err)
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

	mock.ExpectQuery("information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_TYPE", "ENGINE"}).
			AddRow("users", "BASE TABLE", "InnoDB").
			AddRow("orders", "BASE TABLE", "InnoDB"))

	err := checker.ValidateStorageEngine(ctx, newPreflightRun(checker))

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

	mock.ExpectQuery("information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_TYPE", "ENGINE"}).
			AddRow("users", "BASE TABLE", "InnoDB").
			AddRow("orders", "BASE TABLE", "MyISAM").
			AddRow("order_items", "BASE TABLE", "MEMORY"))

	err := checker.ValidateStorageEngine(ctx, newPreflightRun(checker))

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
	mock.ExpectQuery("SELECT kcu.TABLE_SCHEMA, kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_SCHEMA", "TABLE_NAME", "CONSTRAINT_NAME", "COLUMN_NAME",
			"REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME",
			"DELETE_RULE", "UPDATE_RULE"},
		).AddRow("testdb", "orders", "fk_orders_users", "user_id", "testdb", "users", "id", "RESTRICT", "RESTRICT").
			AddRow("testdb", "order_items", "fk_items_orders", "order_id", "testdb", "orders", "id", "RESTRICT", "RESTRICT"))

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
	mock.ExpectQuery("SELECT kcu.TABLE_SCHEMA, kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_SCHEMA", "TABLE_NAME", "CONSTRAINT_NAME", "COLUMN_NAME",
			"REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME",
			"DELETE_RULE", "UPDATE_RULE"},
		).AddRow("testdb", "orders", "fk_orders_users", "user_id", "testdb", "users", "id", "RESTRICT", "RESTRICT"))

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

// TestValidateForeignKeyIndexes_IgnoresOutOfGraphChild guards the .ayder/002
// fix: an FK whose child table is OUTSIDE the graph must not trip
// FK_INDEX_CHECK, even though getForeignKeys leaves its Indexed at the zero
// value (false). Flagging it would be a false positive and would shadow the
// real FK_COVERAGE_CHECK error.
func TestValidateForeignKeyIndexes_IgnoresOutOfGraphChild(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph() // graph = {users, orders, order_items}
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	// FK query returns one out-of-graph child (audit_log) referencing an
	// in-graph parent (orders). getForeignKeys does NOT index-check it, so its
	// Indexed stays false.
	mock.ExpectQuery("SELECT kcu.TABLE_SCHEMA, kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_SCHEMA", "TABLE_NAME", "CONSTRAINT_NAME", "COLUMN_NAME",
			"REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME",
			"DELETE_RULE", "UPDATE_RULE"},
		).AddRow("testdb", "audit_log", "fk_audit_orders", "order_id", "testdb", "orders", "id", "RESTRICT", "RESTRICT"))

	// No isColumnIndexed query is expected: audit_log is out of graph, so
	// getForeignKeys skips the index lookup entirely.

	if err := checker.ValidateForeignKeyIndexes(ctx); err != nil {
		t.Fatalf("expected no FK_INDEX_CHECK error for out-of-graph child, got: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
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

	mock.ExpectQuery("information_schema.TRIGGERS").
		WithArgs("testdb", "DELETE").
		WillReturnRows(sqlmock.NewRows([]string{
			"EVENT_OBJECT_TABLE", "TRIGGER_NAME", "EVENT_MANIPULATION", "ACTION_TIMING",
		}))

	if err := checker.ValidateTriggers(ctx, newPreflightRun(checker), false); err != nil {
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

	mock.ExpectQuery("information_schema.TRIGGERS").
		WithArgs("testdb", "DELETE").
		WillReturnRows(sqlmock.NewRows([]string{
			"EVENT_OBJECT_TABLE", "TRIGGER_NAME", "EVENT_MANIPULATION", "ACTION_TIMING",
		}).
			AddRow("users", "trg_users_delete", "DELETE", "AFTER").
			AddRow("orders", "trg_orders_delete", "DELETE", "AFTER"))

	err := checker.ValidateTriggers(ctx, newPreflightRun(checker), false)

	var pe *PreflightError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PreflightError, got %T: %v", err, err)
	}
	if pe.Check != "DELETE_TRIGGER_CHECK" {
		t.Fatalf("Check = %q", pe.Check)
	}
	// Two tables, but cross-table order follows graph.AllNodes() — compare as a set.
	if len(pe.Tables) != 2 {
		t.Fatalf("Tables = %v, want 2 entries", pe.Tables)
	}
}

func TestValidateTriggers_WithForce(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	mock.ExpectQuery("information_schema.TRIGGERS").
		WithArgs("testdb", "DELETE").
		WillReturnRows(sqlmock.NewRows([]string{
			"EVENT_OBJECT_TABLE", "TRIGGER_NAME", "EVENT_MANIPULATION", "ACTION_TIMING",
		}).AddRow("orders", "trg_del", "DELETE", "AFTER"))

	// The rows above DO contain a trigger; only --force-triggers turns this into a pass.
	if err := checker.ValidateTriggers(ctx, newPreflightRun(checker), true); err != nil {
		t.Fatalf("--force-triggers must downgrade to a warning, got: %v", err)
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
	mock.ExpectQuery("SELECT kcu.TABLE_SCHEMA, kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_SCHEMA", "TABLE_NAME", "CONSTRAINT_NAME", "COLUMN_NAME",
			"REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME",
			"DELETE_RULE", "UPDATE_RULE"},
		).AddRow("testdb", "orders", "fk_orders_users", "user_id", "testdb", "users", "id", "CASCADE", "RESTRICT").
			AddRow("testdb", "order_items", "fk_items_orders", "order_id", "testdb", "orders", "id", "CASCADE", "RESTRICT"))

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
	mock.ExpectQuery("SELECT kcu.TABLE_SCHEMA, kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_SCHEMA", "TABLE_NAME", "CONSTRAINT_NAME", "COLUMN_NAME",
			"REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME",
			"DELETE_RULE", "UPDATE_RULE"},
		).AddRow("testdb", "orders", "fk_orders_users", "user_id", "testdb", "users", "id", "RESTRICT", "RESTRICT").
			AddRow("testdb", "order_items", "fk_items_orders", "order_id", "testdb", "orders", "id", "RESTRICT", "RESTRICT"))

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

	mock.ExpectQuery("SELECT kcu.TABLE_SCHEMA, kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME").
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

	// The table list now comes from the run, which reads it from the graph:
	// createPreflightTestGraph's nodes are users, orders and order_items. Only
	// "users" is reported back, so orders and order_items are missing.
	destRows := sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_TYPE", "ENGINE"}).
		AddRow("users", "BASE TABLE", "InnoDB")
	destMock.ExpectQuery("information_schema.TABLES").
		WithArgs("destdb").
		WillReturnRows(destRows)

	err := checker.ValidateDestinationTablesExist(context.Background(), newPreflightRun(checker))
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

	destMock.ExpectQuery("information_schema.TRIGGERS").
		WithArgs("destdb", "INSERT").
		WillReturnRows(sqlmock.NewRows([]string{
			"EVENT_OBJECT_TABLE", "TRIGGER_NAME", "EVENT_MANIPULATION", "ACTION_TIMING",
		}).AddRow("users", "trg_users_insert", "INSERT", "BEFORE"))

	err := checker.ValidateDestinationInsertTriggers(context.Background(), newPreflightRun(checker))
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

func TestValidateForeignKeyCoverage_FailsForUncoveredCascadeAndRestrict(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)

	mock.ExpectQuery("SELECT\\s+kcu\\.TABLE_SCHEMA,").
		WillReturnRows(sqlmock.NewRows([]string{
			"table_schema", "table_name", "constraint_name", "column_name",
			"referenced_table_schema", "referenced_table_name", "referenced_column_name", "delete_rule", "update_rule",
		}).
			AddRow("testdb", "external_cascade", "fk_ext_orders_1", "order_id", "testdb", "orders", "id", "CASCADE", "RESTRICT").
			AddRow("testdb", "external_restrict", "fk_ext_orders_2", "order_id", "testdb", "orders", "id", "RESTRICT", "RESTRICT"))

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

// TestValidateForeignKeyCoverage_CrossSchemaSameNameChild proves an out-of-graph
// child in ANOTHER schema that happens to share a name with a graph table
// (otherdb.orders vs in-graph orders) is still flagged: membership must be
// schema-aware, not bare-name.
func TestValidateForeignKeyCoverage_CrossSchemaSameNameChild(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph() // graph = {users, orders, order_items} in "testdb"
	checker, _ := NewPreflightChecker(db, "testdb", g, logger.NewDefault())
	ctx := context.Background()

	// otherdb.orders (out-of-graph) references in-graph testdb.users.
	mock.ExpectQuery("SELECT kcu.TABLE_SCHEMA, kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_SCHEMA", "TABLE_NAME", "CONSTRAINT_NAME", "COLUMN_NAME",
			"REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME",
			"DELETE_RULE", "UPDATE_RULE",
		}).AddRow("otherdb", "orders", "fk_other_users", "user_id",
			"testdb", "users", "id", "CASCADE", "RESTRICT"))

	err := checker.ValidateForeignKeyCoverage(ctx)
	if err == nil {
		t.Fatal("expected FK_COVERAGE_CHECK for same-name cross-schema child, got nil")
	}
	if pfErr, ok := err.(*PreflightError); !ok || pfErr.Check != "FK_COVERAGE_CHECK" {
		t.Fatalf("expected FK_COVERAGE_CHECK, got: %v", err)
	}
	if !strings.Contains(err.Error(), "otherdb") {
		t.Fatalf("expected error to qualify the child schema, got: %v", err)
	}
}

// ============================================================================
// ValidateForeignKeyMetadataVisibility Tests
// ============================================================================

func TestValidateForeignKeyMetadataVisibility_NoGlobalSelect(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()
	checker, _ := NewPreflightChecker(db, "testdb", createPreflightTestGraph(), logger.NewDefault())

	mock.ExpectQuery("SELECT CURRENT_USER").
		WillReturnRows(sqlmock.NewRows([]string{"CURRENT_USER()"}).AddRow("app@%"))
	mock.ExpectQuery("SELECT CURRENT_ROLE").
		WillReturnRows(sqlmock.NewRows([]string{"CURRENT_ROLE()"}).AddRow("NONE"))
	mock.ExpectQuery("FROM information_schema.USER_PRIVILEGES").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))

	err := checker.ValidateForeignKeyMetadataVisibility(context.Background())
	if err == nil {
		t.Fatal("expected FK_COVERAGE_VISIBILITY_CHECK when account lacks global SELECT, got nil")
	}
	if pfErr, ok := err.(*PreflightError); !ok || pfErr.Check != "FK_COVERAGE_VISIBILITY_CHECK" {
		t.Fatalf("expected FK_COVERAGE_VISIBILITY_CHECK, got: %v", err)
	}
}

func TestValidateForeignKeyMetadataVisibility_GlobalSelectPasses(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()
	checker, _ := NewPreflightChecker(db, "testdb", createPreflightTestGraph(), logger.NewDefault())

	mock.ExpectQuery("SELECT CURRENT_USER").
		WillReturnRows(sqlmock.NewRows([]string{"CURRENT_USER()"}).AddRow("root@localhost"))
	mock.ExpectQuery("SELECT CURRENT_ROLE").
		WillReturnRows(sqlmock.NewRows([]string{"CURRENT_ROLE()"}).AddRow("NONE"))
	mock.ExpectQuery("FROM information_schema.USER_PRIVILEGES").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))

	if err := checker.ValidateForeignKeyMetadataVisibility(context.Background()); err != nil {
		t.Fatalf("expected pass with global SELECT, got: %v", err)
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
	mock.ExpectQuery("SELECT\\s+kcu\\.TABLE_SCHEMA,").
		WillReturnRows(sqlmock.NewRows([]string{
			"table_schema", "table_name", "constraint_name", "column_name",
			"referenced_table_schema", "referenced_table_name", "referenced_column_name", "delete_rule", "update_rule",
		}).
			AddRow("testdb", "order_items", "fk_items_orders", "order_id", "testdb", "orders", "order_id", "RESTRICT", "RESTRICT").
			AddRow("testdb", "item_shipments", "fk_ship_orders", "order_id", "testdb", "orders", "order_id", "RESTRICT", "RESTRICT").
			AddRow("testdb", "item_shipments", "fk_ship_items", "item_id", "testdb", "order_items", "item_id", "RESTRICT", "RESTRICT"))

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

	mock.ExpectQuery("SELECT\\s+kcu\\.TABLE_SCHEMA,").
		WillReturnRows(sqlmock.NewRows([]string{
			"table_schema", "table_name", "constraint_name", "column_name",
			"referenced_table_schema", "referenced_table_name", "referenced_column_name", "delete_rule", "update_rule",
		}).
			AddRow("testdb", "order_items", "fk_items_orders", "order_id", "testdb", "orders", "order_id", "RESTRICT", "RESTRICT").
			AddRow("testdb", "item_shipments", "fk_ship_items", "item_id", "testdb", "order_items", "item_id", "RESTRICT", "RESTRICT"))

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

	mock.ExpectQuery("SELECT\\s+kcu\\.TABLE_SCHEMA,").
		WillReturnRows(sqlmock.NewRows([]string{
			"table_schema", "table_name", "constraint_name", "column_name",
			"referenced_table_schema", "referenced_table_name", "referenced_column_name", "delete_rule", "update_rule",
		}).
			AddRow("testdb", "payments", "fk_pay_orders", "customer_id", "testdb", "orders", "order_id", "RESTRICT", "RESTRICT"))

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

	mock.ExpectQuery("SELECT\\s+kcu\\.TABLE_SCHEMA,").
		WillReturnRows(sqlmock.NewRows([]string{
			"table_schema", "table_name", "constraint_name", "column_name",
			"referenced_table_schema", "referenced_table_name", "referenced_column_name", "delete_rule", "update_rule",
		}).
			AddRow("testdb", "line_items", "fk_line_orders", "order_id", "testdb", "orders", "id", "RESTRICT", "RESTRICT"))

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
	mock.ExpectQuery("SELECT\\s+kcu\\.TABLE_SCHEMA,").
		WillReturnRows(sqlmock.NewRows([]string{
			"table_schema", "table_name", "constraint_name", "column_name",
			"referenced_table_schema", "referenced_table_name", "referenced_column_name", "delete_rule", "update_rule",
		}).
			AddRow("testdb", "external_table", "fk_ext", "user_id", "testdb", "users", "id", "RESTRICT", "RESTRICT"))

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
	mock.ExpectQuery("SELECT\\s+kcu\\.TABLE_SCHEMA,").
		WillReturnRows(sqlmock.NewRows([]string{
			"table_schema", "table_name", "constraint_name", "column_name",
			"referenced_table_schema", "referenced_table_name", "referenced_column_name", "delete_rule", "update_rule",
		}).
			AddRow("testdb", "categories", "fk_cat_parent", "parent_id", "testdb", "categories", "id", "SET NULL", "RESTRICT"))

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

	mock.ExpectQuery("SELECT\\s+kcu\\.TABLE_SCHEMA,").
		WillReturnRows(sqlmock.NewRows([]string{
			"table_schema", "table_name", "constraint_name", "column_name",
			"referenced_table_schema", "referenced_table_name", "referenced_column_name", "delete_rule", "update_rule",
		}).
			AddRow("testdb", "order_items", "fk_items_orders", "order_id", "testdb", "orders", "order_id", "RESTRICT", "RESTRICT").
			AddRow("testdb", "item_shipments", "fk_ship_items", "item_id", "testdb", "order_items", "item_id", "RESTRICT", "RESTRICT").
			AddRow("testdb", "payments", "fk_pay_orders", "customer_id", "testdb", "orders", "order_id", "RESTRICT", "RESTRICT"))

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

// TestValidateStorageEngineReportsEngineInDecoration proves the migrated check keeps
// the "<table>(<engine>)" Tables decoration the 1.8 engine produced. Step 6 of this
// phase adds the matching end-to-end assertion (chrAssertRawTables) to phase 004's
// characterization; this test is the unit-level guard for the same property.
func TestValidateStorageEngineReportsEngineInDecoration(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_TYPE", "ENGINE"}).
			AddRow("orders", "BASE TABLE", "MyISAM"))

	g := graph.NewGraph("orders", "id")
	p, err := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}

	run := newPreflightRun(p)
	err = p.ValidateStorageEngine(context.Background(), run)

	var pe *PreflightError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PreflightError, got %T: %v", err, err)
	}
	if pe.Check != "STORAGE_ENGINE_CHECK" {
		t.Fatalf("Check = %q", pe.Check)
	}
	if len(pe.Tables) != 1 || pe.Tables[0] != "orders(MyISAM)" {
		t.Fatalf("Tables = %v, want [orders(MyISAM)]", pe.Tables)
	}
}

// TestValidateStorageEngineIgnoresCase proves the library's ASCII case folding: a
// server reporting "innodb" passes. MySQL reports "InnoDB", so this changes no real
// verdict; it removes a dependency on exact server spelling.
//
// This is an assert-passes test and cannot stand alone: it also passes against a
// ValidateStorageEngine that returns nil unconditionally. Its partner above is what
// proves the check still fires. See Step 8's mutants.
func TestValidateStorageEngineIgnoresCase(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_TYPE", "ENGINE"}).
			AddRow("orders", "BASE TABLE", "innodb"))

	g := graph.NewGraph("orders", "id")
	p, _ := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())

	if err := p.ValidateStorageEngine(context.Background(), newPreflightRun(p)); err != nil {
		t.Fatalf("expected engine 'innodb' to pass under ASCII folding, got: %v", err)
	}
}

// TestValidateStorageEngineInspectionErrorIsPlain proves a failed fact fetch surfaces
// as a PLAIN error, never a *PreflightError. A *PreflightError means "the schema is
// wrong"; this means "we could not find out". Broken implementations this catches:
// `return err` unwrapped, and `return &PreflightError{Check: "STORAGE_ENGINE_CHECK"}`.
//
// This unit test is the ONLY place the branch can be tested. In production
// ValidateTablesExist consumes the same memoized fact at position 1 and aborts first,
// so the storage-engine inspection branch is unreachable end-to-end — which is exactly
// why characterization_matrix_integration_test.go's SCOPE CAVEAT requires each of
// phases 013-031 to assert this shape at unit level for the check it replaces.
func TestValidateStorageEngineInspectionErrorIsPlain(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("information_schema.TABLES").
		WillReturnError(errors.New("query failed"))

	g := graph.NewGraph("orders", "id")
	p, _ := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())

	err = p.ValidateStorageEngine(context.Background(), newPreflightRun(p))
	if err == nil {
		t.Fatal("expected an inspection error, got nil")
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("an inspection failure must be a plain error, got *PreflightError: %v", pe)
	}
	if !strings.Contains(err.Error(), "storage_engine") {
		t.Fatalf("inspection error must name the stage, got: %v", err)
	}
}

// TestValidateStorageEngineConsumesTheCachedFact proves this phase's headline property
// and a standing non-negotiable: the stage issues NO query of its own, because the fact
// was already acquired by the existence check at position 1.
//
// Exactly ONE expectation is registered and the explicit pre-load consumes it. An
// implementation that ignored `run` and built its own Inspector would issue a second
// query, receive "all expectations were already fulfilled", and return that as an
// inspection error — a plain error, so the *PreflightError assertion below fails.
//
// Note what does NOT work here: mock.ExpectationsWereMet() cannot detect the re-query,
// because the single expectation is consumed either way. The VERDICT is what
// discriminates, not the expectation bookkeeping.
func TestValidateStorageEngineConsumesTheCachedFact(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_TYPE", "ENGINE"}).
			AddRow("orders", "BASE TABLE", "MyISAM"))

	g := graph.NewGraph("orders", "id")
	p, _ := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())

	run := newPreflightRun(p)
	if _, err := run.sourceTables(context.Background()); err != nil {
		t.Fatalf("pre-load of the source fact failed: %v", err)
	}

	err = p.ValidateStorageEngine(context.Background(), run)

	var pe *PreflightError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PreflightError from the CACHED fact, got %T: %v", err, err)
	}
	if pe.Check != "STORAGE_ENGINE_CHECK" {
		t.Fatalf("Check = %q", pe.Check)
	}
}

// TestValidateStorageEngineNullEngineIsUnknown pins deviation D6: a BASE TABLE whose
// ENGINE metadata is NULL fails closed under STORAGE_ENGINE_CHECK, attributed to the
// table, decorated "<unknown>".
//
// 1.8 never reached a verdict here at all — it scanned ENGINE into a bare string, so a
// NULL aborted preflight with a driver scan error carrying no check ID and no table
// name. Asserting the error is a *PreflightError is therefore the load-bearing half:
// it is what proves the old scan-error path is gone.
//
// The "<unknown>" placeholder is part of D6's contract, not cosmetic. There is no
// released decoration to preserve, and "orders()" would read as a formatting defect
// while concealing the fact being reported.
func TestValidateStorageEngineNullEngineIsUnknown(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_TYPE", "ENGINE"}).
			AddRow("orders", "BASE TABLE", nil))

	g := graph.NewGraph("orders", "id")
	p, _ := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())

	err = p.ValidateStorageEngine(context.Background(), newPreflightRun(p))

	var pe *PreflightError
	if !errors.As(err, &pe) {
		t.Fatalf("a NULL ENGINE must fail closed as *PreflightError, not as the old "+
			"scan error; got %T: %v", err, err)
	}
	if pe.Check != "STORAGE_ENGINE_CHECK" {
		t.Fatalf("Check = %q", pe.Check)
	}
	if len(pe.Tables) != 1 || pe.Tables[0] != "orders(<unknown>)" {
		t.Fatalf("Tables = %v, want [orders(<unknown>)]", pe.Tables)
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

	run := newPreflightRun(checker)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := checker.ValidateTablesExist(ctx, run)

	if err == nil {
		t.Error("Expected error for cancelled context")
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
	mock.ExpectQuery("SELECT kcu.TABLE_SCHEMA, kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_SCHEMA", "TABLE_NAME", "CONSTRAINT_NAME", "COLUMN_NAME",
			"REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME",
			"DELETE_RULE", "UPDATE_RULE"},
		).AddRow("testdb", "orders", "fk_orders_users", "user_id", "testdb", "users", "id", "RESTRICT", "RESTRICT"))

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

	err = checker.ValidateDestinationTablesExist(ctx, newPreflightRun(checker))
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

	err = checker.ValidateDestinationInsertTriggers(ctx, newPreflightRun(checker))
	if err == nil {
		t.Fatal("ValidateDestinationInsertTriggers should return error when destination is nil")
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

// TestValidateNoInvisibleColumns_Rejected proves a participating table with an
// INVISIBLE column is rejected: SELECT * would silently omit it from both the
// copy and the verification hash, so it must be caught before any archive.
func TestValidateNoInvisibleColumns_Rejected(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	checker, _ := NewPreflightChecker(db, "testdb", createPreflightTestGraph(), logger.NewDefault())
	ctx := context.Background()

	// P1 correction (phase 018 review): the check now derives from the general column
	// fact instead of its own InvisibleColumns query. createPreflightTestGraph is a
	// three-node graph, so Graph.AllNodes()'s map order makes the trailing bind args
	// nondeterministic — WithArgs is deliberately omitted.
	mock.ExpectQuery("SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, EXTRA").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE", "COLUMN_TYPE", "EXTRA",
		}).AddRow("orders", "secret_payload", 1, "varchar", "varchar(64)", "INVISIBLE"))

	err := checker.ValidateNoInvisibleColumns(ctx, newPreflightRun(checker))
	if err == nil {
		t.Fatal("expected INVISIBLE_COLUMN_CHECK for a participating invisible column, got nil")
	}
	pfErr, ok := err.(*PreflightError)
	if !ok {
		t.Fatalf("expected *PreflightError, got %T: %v", err, err)
	}
	if pfErr.Check != "INVISIBLE_COLUMN_CHECK" {
		t.Fatalf("expected INVISIBLE_COLUMN_CHECK, got %q: %v", pfErr.Check, err)
	}
	if !strings.Contains(err.Error(), "orders.secret_payload") {
		t.Fatalf("expected error to name the offending column orders.secret_payload, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled mock expectations: %v", err)
	}
}

// TestValidateNoInvisibleColumns_Success is the negative control: no invisible
// columns among the participating tables passes.
func TestValidateNoInvisibleColumns_Success(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	checker, _ := NewPreflightChecker(db, "testdb", createPreflightTestGraph(), logger.NewDefault())
	ctx := context.Background()

	// P1 correction (phase 018 review): Columns-shaped fact, no invisible columns
	// anywhere. Three-node graph — WithArgs omitted for the same reason as above.
	mock.ExpectQuery("SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, EXTRA").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE", "COLUMN_TYPE", "EXTRA",
		}).AddRow("orders", "id", 1, "bigint", "bigint", ""))

	if err := checker.ValidateNoInvisibleColumns(ctx, newPreflightRun(checker)); err != nil {
		t.Fatalf("expected no error when no invisible columns are present, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled mock expectations: %v", err)
	}
}

// TestValidateNoInvisibleColumnsFansOutPerColumn proves the migrated check preserves the
// "<table>.<column>" Tables shape. The library emits ONE finding per table with a Columns
// slice; goarchive reports one entry per column, and phase 004's characterization asserts
// exactly that (orders.note, orders.doubled).
//
// Assertion shape follows the ordering contract: exact ordinal order WITHIN a table, and
// set equality ACROSS tables. Cross-table order follows graph.AllNodes(), which ranges a
// map, so a flat ordered assertion would flake roughly half the time.
func TestValidateNoInvisibleColumnsFansOutPerColumn(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Rows arrive as the library's ORDER BY TABLE_NAME, ORDINAL_POSITION would deliver
	// them — "order_lines" sorts before "orders" ('_' < 's'). Cross-table row order is
	// in fact irrelevant (the library buckets into a map), but matching the real query
	// keeps the fixture honest.
	//
	// "zeta" precedes "alpha" DELIBERATELY: ordinal order here is not alphabetical
	// order, so the within-table assertion below genuinely fails if anything sorts the
	// columns. With alphabetically-ordered names the assertion could not tell the two
	// apart and would pin nothing.
	//
	// P1 correction (phase 018 review): Columns-shaped fact (six columns, EXTRA =
	// "INVISIBLE" on all three rows) instead of the retired InvisibleColumns query.
	// This is a two-node graph, so Graph.AllNodes()'s map order still makes the
	// trailing bind args nondeterministic — WithArgs is deliberately omitted.
	mock.ExpectQuery("SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, EXTRA").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE", "COLUMN_TYPE", "EXTRA",
		}).
			AddRow("order_lines", "hidden", 1, "varchar", "varchar(64)", "INVISIBLE").
			AddRow("orders", "zeta", 1, "varchar", "varchar(64)", "INVISIBLE").
			AddRow("orders", "alpha", 2, "varchar", "varchar(64)", "INVISIBLE"))

	// AddNode is REQUIRED. AddEdgeWithMeta only records the edge in Children/Parents; it
	// does NOT register the node, so without this the child never reaches AllNodes(),
	// is never requested, and the library filters its row out — the test would then fail
	// for a reason unrelated to what it checks.
	g := graph.NewGraph("orders", "id")
	g.AddNode("order_lines", &graph.Node{
		Name:           "order_lines",
		ForeignKey:     "order_id",
		ReferenceKey:   "id",
		DependencyType: "1-N",
	})
	g.SetPK("order_lines", "id")
	g.AddEdgeWithMeta("orders", "order_lines", "order_id", "id", "1-N")

	p, err := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}

	err = p.ValidateNoInvisibleColumns(context.Background(), newPreflightRun(p))

	var pe *PreflightError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PreflightError, got %T: %v", err, err)
	}
	if pe.Check != "INVISIBLE_COLUMN_CHECK" {
		t.Fatalf("Check = %q", pe.Check)
	}
	if len(pe.Tables) != 3 {
		t.Fatalf("Tables = %v, want exactly 3 entries", pe.Tables)
	}

	got := map[string][]string{}
	for _, entry := range pe.Tables {
		table, column, ok := strings.Cut(entry, ".")
		if !ok {
			t.Fatalf("entry %q is not in <table>.<column> form (fan-out lost)", entry)
		}
		got[table] = append(got[table], column)
	}

	want := map[string][]string{
		"orders":      {"zeta", "alpha"},
		"order_lines": {"hidden"},
	}
	if len(got) != len(want) {
		t.Fatalf("tables = %v, want exactly the keys of %v", got, want)
	}
	for table, wantCols := range want {
		gotCols, ok := got[table]
		if !ok {
			t.Fatalf("no entries for table %q; got %v", table, got)
		}
		if strings.Join(gotCols, ",") != strings.Join(wantCols, ",") {
			t.Fatalf("table %q columns = %v, want %v (ORDINAL_POSITION order)",
				table, gotCols, wantCols)
		}
	}
}

// TestValidateNoInvisibleColumnsPasses is the negative control: no invisible columns
// among the participating tables passes.
//
// This is an assert-passes test and cannot stand alone: it also passes against a
// ValidateNoInvisibleColumns that returns nil unconditionally. Its partner above is what
// proves the check still fires.
func TestValidateNoInvisibleColumnsPasses(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	// P1 correction (phase 018 review): Columns-shaped fact, no invisible columns.
	// Single-table graph, so the bind args are deterministic.
	mock.ExpectQuery("SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, EXTRA").
		WithArgs("srcdb", "orders").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE", "COLUMN_TYPE", "EXTRA",
		}).AddRow("orders", "id", 1, "bigint", "bigint", ""))

	g := graph.NewGraph("orders", "id")
	p, _ := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())

	if err := p.ValidateNoInvisibleColumns(context.Background(), newPreflightRun(p)); err != nil {
		t.Fatalf("expected pass, got %v", err)
	}
}

// TestValidateNoInvisibleColumnsInspectionErrorIsPlain proves a failed fact fetch
// surfaces as a PLAIN error, never a *PreflightError. A *PreflightError means "the schema
// is wrong"; this means "we could not find out".
//
// Required by the SCOPE CAVEAT in characterization_matrix_integration_test.go: the
// integration probe dies at the FIRST query RunWithProfile issues, so it can only pin
// this shape for ValidateTablesExist. Every phase 013-031 must assert it at unit level
// for the check it replaces.
//
// The assertion is on goarchive's WRAPPER text, not on the stage word alone, and that
// distinction is load-bearing here: the library's own op name for this call is literally
// "invisible_columns", so the raw *ObjectError already contains that substring
// ("validations: invisible_columns in schema `srcdb`: query metadata: ..."). Asserting
// only the stage word would pass against `return err` unwrapped — a plain error that is
// not a *PreflightError either. Verified empirically, not assumed.
func TestValidateNoInvisibleColumnsInspectionErrorIsPlain(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	// P1 correction (phase 018 review): the fetch now goes through Columns, but the
	// stage's OWN wrapper text ("preflight invisible_columns inspection failed") is
	// unchanged and must keep passing unamended (phase 015 pinned this string).
	mock.ExpectQuery("SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, EXTRA").
		WithArgs("srcdb", "orders").
		WillReturnError(errors.New("query failed"))

	g := graph.NewGraph("orders", "id")
	p, _ := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())

	err = p.ValidateNoInvisibleColumns(context.Background(), newPreflightRun(p))
	if err == nil {
		t.Fatal("expected an inspection error, got nil")
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("an inspection failure must be a plain error, got *PreflightError: %v", pe)
	}
	if !strings.Contains(err.Error(), "preflight invisible_columns inspection failed") {
		t.Fatalf("inspection error must carry goarchive's wrapper, got: %v", err)
	}
}

// TestValidateNoInvisibleColumnsConsumesTheCachedFact proves the stage reads the run's
// memoized fact rather than fetching its own, which is the standing non-negotiable
// ("goarchive must never re-query information_schema for a fact the library answers").
//
// Nothing else covers this. The memoization tests in preflight_facts_test.go prove the
// ACCESSOR caches; they say nothing about whether this stage uses it. An implementation
// that ignored `run` and built its own Inspector passes every other test in this phase,
// both characterization invisible-column tests, and mutants M1 and M2.
//
// Exactly ONE expectation is registered and the explicit pre-load consumes it. A stage
// that re-queried would receive "all expectations were already fulfilled" and return it
// as an inspection error — a plain error, so the *PreflightError assertion fails. Note
// mock.ExpectationsWereMet() would NOT catch this: the single expectation is consumed
// either way. The verdict discriminates, not the expectation bookkeeping.
func TestValidateNoInvisibleColumnsConsumesTheCachedFact(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	// P1 correction (phase 018 review): Columns-shaped fact, EXTRA = "INVISIBLE" on
	// "note".
	mock.ExpectQuery("SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, EXTRA").
		WithArgs("srcdb", "orders").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE", "COLUMN_TYPE", "EXTRA",
		}).AddRow("orders", "note", 1, "varchar", "varchar(64)", "INVISIBLE"))

	g := graph.NewGraph("orders", "id")
	p, _ := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())

	run := newPreflightRun(p)
	if _, err := run.invisibleColumns(context.Background()); err != nil {
		t.Fatalf("pre-load of the invisible-column fact failed: %v", err)
	}

	err = p.ValidateNoInvisibleColumns(context.Background(), run)

	var pe *PreflightError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PreflightError from the CACHED fact, got %T: %v", err, err)
	}
	if pe.Check != "INVISIBLE_COLUMN_CHECK" {
		t.Fatalf("Check = %q", pe.Check)
	}
	if len(pe.Tables) != 1 || pe.Tables[0] != "orders.note" {
		t.Fatalf("Tables = %v, want [orders.note]", pe.Tables)
	}
}

// TestValidateNoInvisibleColumnsConsumesSourceColumnsFact is the cross-fact test the
// P1 correction (phase 018 review) requires: it proves position 6
// (ValidateNoInvisibleColumns) shares position 2's Columns fact rather than issuing a
// second query. Pre-loading via sourceColumns — not invisibleColumns — mimics the real
// RunWithProfile flow, where ValidatePrimaryKeyColumns (position 2) is what actually
// populates the cache first.
//
// Exactly ONE Columns expectation is registered and it must be enough: a stage or
// accessor that still issued its own query would receive "all expectations were
// already fulfilled" and return a plain inspection error, failing the "no error"
// assertion below. mock.ExpectationsWereMet() alone would not catch this — the single
// expectation is consumed either way — so the verdict (no error at all) is what
// discriminates.
func TestValidateNoInvisibleColumnsConsumesSourceColumnsFact(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, EXTRA").
		WithArgs("srcdb", "orders").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE", "COLUMN_TYPE", "EXTRA",
		}).AddRow("orders", "id", 1, "bigint", "bigint", ""))

	g := graph.NewGraph("orders", "id")
	p, _ := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())

	run := newPreflightRun(p)
	if _, err := run.sourceColumns(context.Background()); err != nil {
		t.Fatalf("pre-load of the column fact failed: %v", err)
	}

	// The accessor's contract is that a table with NO invisible columns is absent from
	// the result, so an empty slice means "none". Assert it here, on a fixture whose
	// only table is clean: without the len(cols) > 0 filter the derivation would emit
	// {Table: "orders", Columns: nil}. Nothing downstream would notice — the stage
	// guards on len(offenders) > 0 and an empty Columns slice fans out to zero
	// offenders — so this is the only assertion that holds the filter in place.
	inv, err := run.invisibleColumns(context.Background())
	if err != nil {
		t.Fatalf("invisibleColumns from the cached fact: %v", err)
	}
	if len(inv) != 0 {
		t.Fatalf("a table with no invisible columns must be absent from the result, got %v", inv)
	}

	if err := p.ValidateNoInvisibleColumns(context.Background(), run); err != nil {
		t.Fatalf("expected pass using the already-cached column fact, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected exactly one Columns query total, got: %v", err)
	}
}

// ============================================================================
// Integration Tests
// ============================================================================

// TestValidateTriggersInspectionErrorIsPlain proves a failed source fetch surfaces as a
// PLAIN error, never a *PreflightError. Required by the SCOPE CAVEAT in
// characterization_matrix_integration_test.go for every check phases 013-031 replace —
// and this phase replaces two, so both sides are asserted.
//
// The assertion is on goarchive's WRAPPER text, not the bare stage word.
func TestValidateTriggersInspectionErrorIsPlain(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("information_schema.TRIGGERS").
		WithArgs("srcdb", "DELETE").
		WillReturnError(errors.New("query failed"))

	g := graph.NewGraph("orders", "id")
	p, _ := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())

	err = p.ValidateTriggers(context.Background(), newPreflightRun(p), false)
	if err == nil {
		t.Fatal("expected an inspection error, got nil")
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("an inspection failure must be a plain error, got *PreflightError: %v", pe)
	}
	if !strings.Contains(err.Error(), "preflight delete_triggers inspection failed") {
		t.Fatalf("inspection error must carry goarchive's wrapper, got: %v", err)
	}
}

// TestValidateTriggersConsumesTheCachedFact proves the source stage reads the run's
// memoized fact rather than fetching its own. Nothing else covers this: the memoization
// tests prove the ACCESSOR caches, not that this stage uses it. An implementation that
// built its own Inspector passes every other test in this phase.
//
// Exactly ONE expectation is registered and the pre-load consumes it, so a re-querying
// stage receives "all expectations were already fulfilled" and returns it as an
// inspection error — a plain error, so the *PreflightError assertion fails.
// mock.ExpectationsWereMet() would NOT catch this; the verdict discriminates.
func TestValidateTriggersConsumesTheCachedFact(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("information_schema.TRIGGERS").
		WithArgs("srcdb", "DELETE").
		WillReturnRows(sqlmock.NewRows([]string{
			"EVENT_OBJECT_TABLE", "TRIGGER_NAME", "EVENT_MANIPULATION", "ACTION_TIMING",
		}).AddRow("orders", "trg_del", "DELETE", "AFTER"))

	g := graph.NewGraph("orders", "id")
	p, _ := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())

	run := newPreflightRun(p)
	if _, err := run.sourceDeleteTriggers(context.Background()); err != nil {
		t.Fatalf("pre-load of the source trigger fact failed: %v", err)
	}

	err = p.ValidateTriggers(context.Background(), run, false)

	var pe *PreflightError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PreflightError from the CACHED fact, got %T: %v", err, err)
	}
	if pe.Check != "DELETE_TRIGGER_CHECK" {
		t.Fatalf("Check = %q", pe.Check)
	}
	if len(pe.Tables) != 1 || pe.Tables[0] != "orders(trg_del)" {
		t.Fatalf("Tables = %v, want [orders(trg_del)]", pe.Tables)
	}
}

// TestValidateDestinationInsertTriggersInspectionErrorIsPlain is the destination
// counterpart. Both sides are required: the stages use different accessors on different
// pools, so the source test proves nothing about this path.
func TestValidateDestinationInsertTriggersInspectionErrorIsPlain(t *testing.T) {
	srcDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = srcDB.Close() }()
	dstDB, dstMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = dstDB.Close() }()

	dstMock.ExpectQuery("information_schema.TRIGGERS").
		WithArgs("dstdb", "INSERT").
		WillReturnError(errors.New("query failed"))

	p := chrDestCheckerForFacts(t, srcDB, dstDB)

	err = p.ValidateDestinationInsertTriggers(context.Background(), newPreflightRun(p))
	if err == nil {
		t.Fatal("expected an inspection error, got nil")
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("an inspection failure must be a plain error, got *PreflightError: %v", pe)
	}
	if !strings.Contains(err.Error(), "preflight destination_insert_triggers inspection failed") {
		t.Fatalf("inspection error must carry goarchive's wrapper, got: %v", err)
	}
}

// TestValidateDestinationInsertTriggersConsumesTheCachedFact is the destination
// counterpart of the cached-fact proof.
func TestValidateDestinationInsertTriggersConsumesTheCachedFact(t *testing.T) {
	srcDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = srcDB.Close() }()
	dstDB, dstMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = dstDB.Close() }()

	dstMock.ExpectQuery("information_schema.TRIGGERS").
		WithArgs("dstdb", "INSERT").
		WillReturnRows(sqlmock.NewRows([]string{
			"EVENT_OBJECT_TABLE", "TRIGGER_NAME", "EVENT_MANIPULATION", "ACTION_TIMING",
		}).AddRow("orders", "trg_ins", "INSERT", "BEFORE"))

	p := chrDestCheckerForFacts(t, srcDB, dstDB)
	run := newPreflightRun(p)
	ctx := context.Background()

	if _, err := run.destInsertTriggers(ctx); err != nil {
		t.Fatalf("pre-load of the destination trigger fact failed: %v", err)
	}

	err = p.ValidateDestinationInsertTriggers(ctx, run)

	var pe *PreflightError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PreflightError from the CACHED fact, got %T: %v", err, err)
	}
	if pe.Check != "DEST_INSERT_TRIGGER_CHECK" {
		t.Fatalf("Check = %q", pe.Check)
	}
	if len(pe.Tables) != 1 || pe.Tables[0] != "orders(trg_ins)" {
		t.Fatalf("Tables = %v, want [orders(trg_ins)]", pe.Tables)
	}
}

// TestValidateSingleColumnPrimaryKeyInspectionErrorIsPlain proves the inspection-error
// path for ValidateSingleColumnPrimaryKey keeps the released COMPOSITE_PK_LOOKUP prefix
// and stays a plain error, never a *PreflightError. Asserting only a substring match
// would let a broken unwrapped `return err` pass, since the library's own *ObjectError
// already contains its op name ("primary_keys") — the prefix assertion below checks for
// goarchive's own prefix, not the library's.
func TestValidateSingleColumnPrimaryKeyInspectionErrorIsPlain(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	wantErr := errors.New("boom")
	mock.ExpectQuery("information_schema.STATISTICS AS s").
		WithArgs("srcdb").
		WillReturnError(wantErr)

	g := graph.NewGraph("orders", "id")
	p, err := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}
	run := newPreflightRun(p)

	gotErr := p.ValidateSingleColumnPrimaryKey(context.Background(), run)
	if gotErr == nil {
		t.Fatal("expected an inspection error, got nil")
	}
	if !strings.Contains(gotErr.Error(), "COMPOSITE_PK_LOOKUP:") {
		t.Fatalf("expected COMPOSITE_PK_LOOKUP: prefix, got: %v", gotErr)
	}
	var pe *PreflightError
	if errors.As(gotErr, &pe) {
		t.Fatalf("inspection failure must be a plain error, not *PreflightError: %v", gotErr)
	}
}

// TestValidateRootPKNumericInspectionErrorIsPlain is the ValidateRootPKNumeric
// counterpart, on the ROOT_PK_TYPE_LOOKUP prefix.
func TestValidateRootPKNumericInspectionErrorIsPlain(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	wantErr := errors.New("boom")
	mock.ExpectQuery("information_schema.STATISTICS AS s").
		WithArgs("srcdb").
		WillReturnError(wantErr)

	g := graph.NewGraph("orders", "id")
	p, err := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}
	run := newPreflightRun(p)

	gotErr := p.ValidateRootPKNumeric(context.Background(), run)
	if gotErr == nil {
		t.Fatal("expected an inspection error, got nil")
	}
	if !strings.Contains(gotErr.Error(), "ROOT_PK_TYPE_LOOKUP:") {
		t.Fatalf("expected ROOT_PK_TYPE_LOOKUP: prefix, got: %v", gotErr)
	}
	var pe *PreflightError
	if errors.As(gotErr, &pe) {
		t.Fatalf("inspection failure must be a plain error, not *PreflightError: %v", gotErr)
	}
}

// TestPKStagesShareOneCachedFact proves both PK stages consume a single run-level fact.
// Exactly ONE PrimaryKeys expectation is registered, and the two stages run in sequence
// exactly as RunWithProfile calls them (positions 3 and 4). A stage that built its own
// Inspector would issue a second query, receive "all expectations were already
// fulfilled", and fail here — which no other test in this phase would catch.
//
// The GetRootPKMeta assertion is load-bearing: without it, a ValidateRootPKNumeric that
// silently returned nil would still pass. It is also, with the rebuilt
// TestPreflightChecker_ValidateRootPKNumeric, the ONLY coverage of that write-back --
// make e2e cannot detect its deletion, because loadRootPKMeta runs afterwards on every
// orchestrator path and overwrites the value before any production reader sees it.
func TestPKStagesShareOneCachedFact(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("information_schema.STATISTICS AS s").
		WithArgs("srcdb").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE",
		}).AddRow("orders", "id", "bigint", "bigint unsigned"))

	g := graph.NewGraph("orders", "id")
	p, err := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}

	run := newPreflightRun(p)
	ctx := context.Background()

	if err := p.ValidateSingleColumnPrimaryKey(ctx, run); err != nil {
		t.Fatalf("shape stage must pass from the cached fact: %v", err)
	}
	if err := p.ValidateRootPKNumeric(ctx, run); err != nil {
		t.Fatalf("root PK stage must pass from the SAME cached fact: %v", err)
	}

	dataType, unsigned, ok := g.GetRootPKMeta()
	if !ok || dataType != "bigint" || !unsigned {
		t.Fatalf("root PK metadata: dataType=%q unsigned=%v ok=%v", dataType, unsigned, ok)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the single PrimaryKeys expectation was not observed: %v", err)
	}
}
