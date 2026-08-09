// Package archiver provides comprehensive tests for the preflight checker.
package archiver

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
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
		t.Fatal("Expected error for nil database")
	}
	if !strings.Contains(err.Error(), "database is nil") {
		t.Errorf("Expected error to identify the nil database, got: %v", err)
	}
}

func TestNewPreflightChecker_EmptyDBName(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()

	_, err := NewPreflightChecker(db, "", g, log)
	if err == nil {
		t.Fatal("Expected error for empty database name")
	}
	if !strings.Contains(err.Error(), "source database name is required") {
		t.Errorf("Expected error to identify the empty source database name, got: %v", err)
	}
}

func TestNewPreflightChecker_NilGraph(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	log := logger.NewDefault()

	_, err := NewPreflightChecker(db, "testdb", nil, log)
	if err == nil {
		t.Fatal("Expected error for nil graph")
	}
	if !strings.Contains(err.Error(), "graph is nil") {
		t.Errorf("Expected error to identify the nil graph, got: %v", err)
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
		t.Fatal("Expected default logger to be set")
	}

	if checker.logger.Level() != zapcore.InfoLevel {
		t.Errorf("Expected default logger to be at info level, got: %v", checker.logger.Level())
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
//
// Phase 024 migrates this validator onto run.fkOutgoing + validations.CheckFKIndexed.
// TestValidateForeignKeyIndexes_Success is RETIRED, not rewritten: it asserted only
// "no error", which TestValidateForeignKeyIndexesConsumesTheCachedFact's sibling
// contract (below) — proving the stage reads the memoized fact rather than querying —
// makes redundant once the query-plumbing details it exercised (the old
// KEY_COLUMN_USAGE + per-column STATISTICS shape) no longer exist.
//
// TestValidateForeignKeyIndexes_IgnoresOutOfGraphChild is also RETIRED (deliberate scope
// reduction, Ambiguity 3): it guarded 1.8's manual in-graph filter, which no longer
// exists. OutgoingFrom IS the filter now — the selector cannot return an out-of-graph
// child — and TestPreflightRunFKOutgoingUsesOutgoingSelector (preflight_facts_test.go)
// pins exactly that, more strongly, because it also fails under a mutated selector,
// which the retired test did not.
// ============================================================================

// TestValidateForeignKeyIndexes_Unindexed rewrites the old query-level test against the
// new pipeline. The primary INNODB_FOREIGN path always sets Indexed:true by
// construction, so provoking Indexed:false requires the full fallback: primary fails,
// KEY_COLUMN_USAGE succeeds, and the STATISTICS supporting-index query returns no rows
// for the child table (so foreignKeyColumnsIndexed has no candidate to match).
func TestValidateForeignKeyIndexes_Unindexed(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	ctx := context.Background()

	mock.ExpectQuery("INNODB_FOREIGN AS f").WillReturnError(errors.New("PROCESS denied"))
	mock.ExpectQuery("KEY_COLUMN_USAGE AS kcu").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_SCHEMA", "TABLE_NAME", "CONSTRAINT_NAME", "COLUMN_NAME",
			"REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME",
			"DELETE_RULE", "UPDATE_RULE", "ORDINAL_POSITION",
		}).AddRow("testdb", "orders", "fk_orders_users", "user_id", "testdb", "users", "id", "RESTRICT", "RESTRICT", 1))
	mock.ExpectQuery("information_schema.STATISTICS").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_SCHEMA", "TABLE_NAME", "INDEX_NAME", "COLUMN_NAME", "SEQ_IN_INDEX",
		})) // no supporting index rows for orders.user_id

	err := checker.ValidateForeignKeyIndexes(ctx, newPreflightRun(checker))

	if err == nil {
		t.Fatal("Expected error for unindexed FK")
	}

	preflightErr, ok := err.(*PreflightError)
	if !ok {
		t.Fatalf("Expected PreflightError, got %T", err)
	}

	if preflightErr.Check != "FK_INDEX_CHECK" {
		t.Errorf("Expected check 'FK_INDEX_CHECK', got %s", preflightErr.Check)
	}
	if len(preflightErr.Tables) != 1 || preflightErr.Tables[0] != "orders.user_id" {
		t.Errorf("Expected Tables = [orders.user_id], got %v", preflightErr.Tables)
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
	core, recorded := observer.New(zapcore.DebugLevel)
	checker, _ := NewPreflightChecker(db, "testdb", g, logger.NewDefault())
	checker.logger = &logger.Logger{SugaredLogger: zap.New(core).Sugar()}
	ctx := context.Background()

	run := &preflightRun{
		checker: checker, tables: g.AllNodes(),
		fkIn: validations.ForeignKeyResult{Keys: []validations.ForeignKey{
			{ConstraintName: "fk_orders_users", ChildSchema: "testdb", ChildTable: "orders",
				ChildColumns: []string{"user_id"}, ParentSchema: "testdb", ParentTable: "users",
				ParentColumns: []string{"id"}, OnDelete: "CASCADE"},
		}},
		fkOut: validations.ForeignKeyResult{Keys: []validations.ForeignKey{
			{ConstraintName: "fk_items_orders", ChildSchema: "testdb", ChildTable: "order_items",
				ChildColumns: []string{"order_id"}, ParentSchema: "testdb", ParentTable: "orders",
				ParentColumns: []string{"id"}, OnDelete: "CASCADE"},
		}},
		fkInLoaded: true, fkOutLoaded: true,
	}

	// Should not error, just warn
	err := checker.WarnCascadeRules(ctx, run)

	if err != nil {
		t.Fatalf("WarnCascadeRules should not error: %v", err)
	}
	// No query is expected: WarnCascadeRules consumes only the preloaded facts.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded facts: %v", err)
	}

	// The pair (WithCascade/NoCascade) is only meaningful if the log line actually
	// names the fixture's cascading constraints — WarnCascadeRules returns nil in
	// both cases, so err == nil alone cannot distinguish them.
	const want = "ON DELETE CASCADE rules detected (2): " +
		"[testdb.order_items.order_id->testdb.orders.id testdb.orders.user_id->testdb.users.id]"
	var found bool
	for _, entry := range recorded.FilterLevelExact(zapcore.WarnLevel).All() {
		if entry.Message == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("want %q; warnings were: %v",
			want, recorded.FilterLevelExact(zapcore.WarnLevel).All())
	}
}

func TestWarnCascadeRules_NoCascade(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	core, recorded := observer.New(zapcore.DebugLevel)
	checker, _ := NewPreflightChecker(db, "testdb", g, logger.NewDefault())
	checker.logger = &logger.Logger{SugaredLogger: zap.New(core).Sugar()}
	ctx := context.Background()

	run := &preflightRun{
		checker: checker, tables: g.AllNodes(),
		fkIn: validations.ForeignKeyResult{Keys: []validations.ForeignKey{
			{ConstraintName: "fk_orders_users", ChildSchema: "testdb", ChildTable: "orders",
				ChildColumns: []string{"user_id"}, ParentSchema: "testdb", ParentTable: "users",
				ParentColumns: []string{"id"}, OnDelete: "RESTRICT"},
		}},
		fkOut: validations.ForeignKeyResult{Keys: []validations.ForeignKey{
			{ConstraintName: "fk_items_orders", ChildSchema: "testdb", ChildTable: "order_items",
				ChildColumns: []string{"order_id"}, ParentSchema: "testdb", ParentTable: "orders",
				ParentColumns: []string{"id"}, OnDelete: "RESTRICT"},
		}},
		fkInLoaded: true, fkOutLoaded: true,
	}

	err := checker.WarnCascadeRules(ctx, run)

	if err != nil {
		t.Fatalf("WarnCascadeRules failed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded facts: %v", err)
	}

	// RESTRICT-only fixture: no cascade rules exist, so no cascade warning may be
	// logged at any level. This is what the vacuous version of this test missed —
	// it never looked at the log, so a build that always warned still passed.
	for _, entry := range recorded.All() {
		if strings.Contains(entry.Message, "ON DELETE CASCADE rules detected") {
			t.Fatalf("no CASCADE rules were present in the fixture; got log: %q", entry.Message)
		}
	}
}

// TestWarnCascadeRulesInspectionErrorIsPlain — contract 2, both branches. A memoized
// fetch error must surface as a PLAIN error carrying goarchive's "cascade" inspection
// wrapper, never a *PreflightError.
//
// Both cases are exercised because the two branches are byte-identical: a build that
// wrapped only the incoming error would still pass a test that checked only the
// incoming error. The outgoing case must preload a SUCCESSFUL incoming fact, or the
// stage returns at the first branch and never reaches the second.
//
// In production neither branch is reachable — both facts are memoized by earlier
// unconditional stages, so a fetch error aborts the run long before this point. They are
// defensive, and this test is what keeps them correct.
func TestWarnCascadeRulesInspectionErrorIsPlain(t *testing.T) {
	wantErr := errors.New("fk fetch failed")

	for _, tc := range []struct {
		name string
		run  func(c *PreflightChecker, g *graph.Graph) *preflightRun
	}{
		{"incoming", func(c *PreflightChecker, g *graph.Graph) *preflightRun {
			return &preflightRun{checker: c, tables: g.AllNodes(),
				fkInErr: wantErr, fkInLoaded: true}
		}},
		{"outgoing", func(c *PreflightChecker, g *graph.Graph) *preflightRun {
			return &preflightRun{checker: c, tables: g.AllNodes(),
				fkIn: validations.ForeignKeyResult{}, fkInLoaded: true,
				fkOutErr: wantErr, fkOutLoaded: true}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer func() { _ = db.Close() }()

			g := createPreflightTestGraph()
			checker, _ := NewPreflightChecker(db, "testdb", g, logger.NewDefault())

			err = checker.WarnCascadeRules(context.Background(), tc.run(checker, g))
			if err == nil {
				t.Fatal("expected an inspection error, got nil")
			}
			if !errors.Is(err, wantErr) {
				t.Fatalf("inspection error must wrap %v, got: %v", wantErr, err)
			}
			var pe *PreflightError
			if errors.As(err, &pe) {
				t.Fatalf("inspection failure must be plain, got *PreflightError: %v", pe)
			}
			if !strings.Contains(err.Error(), "preflight cascade inspection failed") {
				t.Fatalf("inspection error must carry goarchive's wrapper, got: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("stage queried despite the preloaded facts: %v", err)
			}
		})
	}
}

// TestWarnCascadeRulesSortsTheUnion is the sort proof, and the fixture is arranged so it
// is also the stage-level dedup proof AND the only test that observes the union's SECOND
// operand. Three properties, one fixture, each independently falsifiable:
//
//		incoming = [zebra, middle]      outgoing = [middle, alpha]
//		first-seen union, deduped       = zebra, middle, alpha   (3)
//		sorted                          = alpha, middle, zebra   (3)
//
//	  - remove sort.Strings  -> order is zebra, middle, alpha        -> FAILS
//	  - remove the dedup     -> middle appears twice, count 4        -> FAILS
//	  - drop the outgoing set-> alpha disappears, count 2            -> FAILS
//
// That third one matters: `middle` is deliberately the ONLY constraint present in both
// sets. If every outgoing constraint were also incoming — the arrangement an earlier
// revision used — passing one set or two would be indistinguishable, and the union's
// second argument would be untested everywhere in the suite.
//
// Asserting the exact rendered slice is what makes all three fail; independent substring
// checks would not.
func TestWarnCascadeRulesSortsTheUnion(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	cascade := func(name, child string) validations.ForeignKey {
		return validations.ForeignKey{
			ConstraintName: name, ChildSchema: "srcdb", ChildTable: child,
			ChildColumns: []string{"oid"}, ParentSchema: "srcdb", ParentTable: "orders",
			ParentColumns: []string{"id"}, OnDelete: "CASCADE",
		}
	}
	zebra, middle, alpha := cascade("fk_z", "zebra"), cascade("fk_m", "middle"), cascade("fk_a", "alpha")

	core, recorded := observer.New(zapcore.DebugLevel)
	g := createPreflightTestGraph()
	checker, _ := NewPreflightChecker(db, "testdb", g, logger.NewDefault())
	checker.logger = &logger.Logger{SugaredLogger: zap.New(core).Sugar()}

	run := &preflightRun{
		checker: checker, tables: g.AllNodes(),
		fkIn:       validations.ForeignKeyResult{Keys: []validations.ForeignKey{zebra, middle}},
		fkOut:      validations.ForeignKeyResult{Keys: []validations.ForeignKey{middle, alpha}},
		fkInLoaded: true, fkOutLoaded: true,
	}

	if err := checker.WarnCascadeRules(context.Background(), run); err != nil {
		t.Fatalf("WarnCascadeRules: %v", err)
	}

	const want = "ON DELETE CASCADE rules detected (3): " +
		"[srcdb.alpha.oid->srcdb.orders.id srcdb.middle.oid->srcdb.orders.id " +
		"srcdb.zebra.oid->srcdb.orders.id]"
	var found bool
	for _, entry := range recorded.FilterLevelExact(zapcore.WarnLevel).All() {
		if entry.Message == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("want %q; warnings were: %v",
			want, recorded.FilterLevelExact(zapcore.WarnLevel).All())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded facts: %v", err)
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

	// destinationDBName and jobSchema must be distinct literals, or an assignment
	// bug like p.jobSchemaName = destinationDBName (instead of jobSchema) is
	// byte-identical and undetectable.
	if err := checker.ConfigureDestination(destDB, "destdb", "jobschemadb"); err != nil {
		t.Fatalf("ConfigureDestination failed: %v", err)
	}

	if checker.destinationDB != destDB {
		t.Fatal("destination DB was not set")
	}
	if checker.destinationDBName != "destdb" {
		t.Fatalf("expected destinationDBName=destdb, got %s", checker.destinationDBName)
	}
	if checker.jobSchemaName != "jobschemadb" {
		t.Fatalf("expected jobSchemaName=jobschemadb, got %s", checker.jobSchemaName)
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

// specColT is a terse ColumnSpec builder mirroring specCol, kept separate so this
// file's fixtures do not depend on preflight_schema_policy_test.go's internals.
func specColT(ordinal int, name, colType string) validations.ColumnSpec {
	return validations.ColumnSpec{
		Name: name, Ordinal: ordinal, Type: colType, NormalizedType: colType,
		Generated: validations.GeneratedNone,
	}
}

// preloadedSchemaSpecPair builds a specPair for table "users" with a PRIMARY index on
// "id" on both sides, so it clears the primary-key shape every fixture needs.
func preloadedSchemaSpecPair(a, b []validations.ColumnSpec) specPair {
	primary := validations.IndexSpec{
		Name: "PRIMARY", Unique: true, Type: "BTREE", Visible: true,
		Parts: []validations.IndexPart{{Column: "id"}},
	}
	return specPair{
		Table: "users",
		A: validations.TableSpec{Schema: "sourcedb", Table: "users", Columns: a,
			Indexes: []validations.IndexSpec{primary}, Captured: validations.SectionIndexes},
		B: validations.TableSpec{Schema: "destdb", Table: "users", Columns: b,
			Indexes: []validations.IndexSpec{primary}, Captured: validations.SectionIndexes},
	}
}

// runPreloadedSchemaCompatibilityCheck preloads run.specs with one table's spec pair and
// calls the stage. No sqlmock expectations are registered on either pool: tableSpecs is
// the fact acquisition boundary now, so a mismatch case exercises evaluateSchemaCompatibility
// through the real stage without touching a database at all.
func runPreloadedSchemaCompatibilityCheck(t *testing.T, sourceCols, destCols []validations.ColumnSpec) error {
	t.Helper()
	sourceDB, sourceMock, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(sourceDB, "sourcedb", g, log)
	_ = checker.ConfigureDestination(destDB, "destdb", "destdb")

	run := &preflightRun{
		checker: checker, tables: g.AllNodes(),
		specs:       []specPair{preloadedSchemaSpecPair(sourceCols, destCols)},
		specsLoaded: true,
	}

	err := checker.ValidateDestinationSchemaCompatibility(context.Background(), run)
	if serr := sourceMock.ExpectationsWereMet(); serr != nil {
		t.Fatalf("stage queried the source despite the preloaded fact: %v", serr)
	}
	if derr := destMock.ExpectationsWereMet(); derr != nil {
		t.Fatalf("stage queried the destination despite the preloaded fact: %v", derr)
	}
	return err
}

// TestValidateDestinationSchemaCompatibility_Mismatch proves the stage surfaces a
// DEST_SCHEMA_COMPATIBILITY_CHECK error from a cached spec pair that disagrees. The fact
// is preloaded directly: tableSpecs, not this stage, now owns fetching TableSpec, so the
// "SELECT ORDINAL_POSITION" sqlmock model this test used to wire no longer applies.
func TestValidateDestinationSchemaCompatibility_Mismatch(t *testing.T) {
	sourceCols := []validations.ColumnSpec{
		specColT(1, "id", "bigint"),
		specColT(2, "name", "varchar(255)"),
	}
	destCols := []validations.ColumnSpec{
		specColT(1, "id", "bigint"),
		specColT(2, "name", "varchar(100)"), // mismatch
	}

	err := runPreloadedSchemaCompatibilityCheck(t, sourceCols, destCols)
	if err == nil {
		t.Fatal("expected destination schema compatibility error")
	}
}

// TestValidateDestinationSchemaCompatibility_RelaxedDestination ports the 1.8
// column-comparison matrix onto specPair fixtures. Several 1.8 cases move on rather than
// being ported as-is, and none of the moved cases are silently dropped:
//
//   - destination missing PK, and destination-only unique index: INDEX-level concepts
//     TableSpec/DiffSpecs represents through Indexes, not a per-column COLUMN_KEY
//     projection, so they do not fit this table-driven column-matrix fixture. Both are
//     now covered elsewhere: destination missing PK by phase 029's
//     TestAbsoluteInvariantPrimaryKeyMustMatch (all four violation shapes) and
//     TestCharacterizationDestSchemaMissingPKFails; destination-only unique constraints
//     by this phase's TestD3* family in preflight_schema_policy_test.go and the four
//     _FatalUnderD3 characterizations in characterization_unique_index_integration_test.go.
//   - destination-only generated column: rule 6 (the destination-generated invariant) is
//     an ABSOLUTE invariant judged independently of any SpecDiff (1.8 rejected it even
//     when the source was identically generated, which emits no diff at all).
//     ColumnGeneratedMismatch is deliberately ignored by disposeDiff today, subsumed by
//     phase 029's invariant — covered by TestAbsoluteInvariantDestinationGeneratedIsFatal
//     and, at the characterization layer, TestCharacterizationDestSchemaGeneratedDestinationFails,
//     not here.
//   - destination may drop/add a secondary index, and destination may drop a unique
//     index: also INDEX-level concepts with no ColumnSpec representation at all (no
//     COLUMN_KEY equivalent). Already covered end-to-end against real MySQL metadata by
//     TestCharacterizationDestSchemaLooserDestinationPasses and the GAP1-4 /
//     Renamed/SourceOnly cases in characterization_unique_index_integration_test.go.
//   - integer display width and unsigned: column-level, but already covered by
//     TestSchemaCompatOrdinaryDisplayWidthEmitsNoDiff / TestSchemaCompatUnsignedIsNotIgnored
//     (preflight_schema_policy_test.go), so not duplicated here.
func TestValidateDestinationSchemaCompatibility_RelaxedDestination(t *testing.T) {
	tests := []struct {
		name       string
		sourceCols []validations.ColumnSpec
		destCols   []validations.ColumnSpec
		wantErr    bool
	}{
		{
			name: "destination may drop auto_increment",
			sourceCols: []validations.ColumnSpec{
				withAutoIncrement(specColT(1, "id", "bigint")),
			},
			destCols: []validations.ColumnSpec{
				specColT(1, "id", "bigint"),
			},
			wantErr: false,
		},
		{
			name: "destination may drop DEFAULT_GENERATED and on update",
			sourceCols: []validations.ColumnSpec{
				specColT(1, "id", "bigint"),
				withDefaultAndOnUpdate(specColT(2, "updated_at", "timestamp")),
			},
			destCols: []validations.ColumnSpec{
				specColT(1, "id", "bigint"),
				specColT(2, "updated_at", "timestamp"),
			},
			wantErr: false,
		},
		{
			name: "destination may be more permissive about NULLs",
			sourceCols: []validations.ColumnSpec{
				specColT(1, "id", "bigint"),
				specColT(2, "name", "varchar(255)"),
			},
			destCols: []validations.ColumnSpec{
				specColT(1, "id", "bigint"),
				withNullable(specColT(2, "name", "varchar(255)")),
			},
			wantErr: false,
		},
		{
			name: "destination stricter NULLability is rejected",
			sourceCols: []validations.ColumnSpec{
				specColT(1, "id", "bigint"),
				withNullable(specColT(2, "name", "varchar(255)")),
			},
			destCols: []validations.ColumnSpec{
				specColT(1, "id", "bigint"),
				specColT(2, "name", "varchar(255)"),
			},
			wantErr: true,
		},
		{
			name: "source generated column with plain destination is allowed",
			sourceCols: []validations.ColumnSpec{
				specColT(1, "id", "bigint"),
				withStoredGenerated(specColT(2, "total", "decimal(10,2)")),
			},
			destCols: []validations.ColumnSpec{
				specColT(1, "id", "bigint"),
				specColT(2, "total", "decimal(10,2)"),
			},
			wantErr: false,
		},
		{
			name: "column type mismatch is rejected",
			sourceCols: []validations.ColumnSpec{
				specColT(1, "id", "bigint"),
				specColT(2, "name", "varchar(255)"),
			},
			destCols: []validations.ColumnSpec{
				specColT(1, "id", "bigint"),
				specColT(2, "name", "varchar(100)"),
			},
			wantErr: true,
		},
		{
			name: "charset mismatch is rejected under count verification",
			sourceCols: []validations.ColumnSpec{
				specColT(1, "id", "bigint"),
				withCharset(specColT(2, "name", "varchar(255)"), "utf8mb4", "utf8mb4_0900_ai_ci"),
			},
			destCols: []validations.ColumnSpec{
				specColT(1, "id", "bigint"),
				withCharset(specColT(2, "name", "varchar(255)"), "latin1", "latin1_swedish_ci"),
			},
			wantErr: true,
		},
		{
			name: "collation-only mismatch is allowed (warn only)",
			sourceCols: []validations.ColumnSpec{
				specColT(1, "id", "bigint"),
				withCharset(specColT(2, "name", "varchar(255)"), "utf8mb4", "utf8mb4_0900_ai_ci"),
			},
			destCols: []validations.ColumnSpec{
				specColT(1, "id", "bigint"),
				withCharset(specColT(2, "name", "varchar(255)"), "utf8mb4", "utf8mb4_general_ci"),
			},
			wantErr: false,
		},
		{
			name: "identical charsets pass",
			sourceCols: []validations.ColumnSpec{
				specColT(1, "id", "bigint"),
				withCharset(specColT(2, "name", "varchar(255)"), "utf8mb4", "utf8mb4_0900_ai_ci"),
			},
			destCols: []validations.ColumnSpec{
				specColT(1, "id", "bigint"),
				withCharset(specColT(2, "name", "varchar(255)"), "utf8mb4", "utf8mb4_0900_ai_ci"),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := runPreloadedSchemaCompatibilityCheck(t, tt.sourceCols, tt.destCols)
			if tt.wantErr && err == nil {
				t.Fatal("expected schema compatibility error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("expected schemas to be compatible, got: %v", err)
			}
		})
	}
}

func withAutoIncrement(c validations.ColumnSpec) validations.ColumnSpec {
	c.AutoIncrement = true
	return c
}

func withDefaultAndOnUpdate(c validations.ColumnSpec) validations.ColumnSpec {
	def := "CURRENT_TIMESTAMP"
	c.Default, c.DefaultIsExpression, c.OnUpdate = &def, true, true
	return c
}

func withNullable(c validations.ColumnSpec) validations.ColumnSpec {
	c.Nullable = true
	return c
}

func withStoredGenerated(c validations.ColumnSpec) validations.ColumnSpec {
	c.Generated = validations.GeneratedStored
	return c
}

func withCharset(c validations.ColumnSpec, charset, collation string) validations.ColumnSpec {
	c.Charset, c.Collation = charset, collation
	return c
}

// TestValidateDestinationSchemaCompatibilityInspectionErrorIsPlain — contract 2. A
// memoized tableSpecs error must surface as a PLAIN error carrying goarchive's
// "destination_schema_compatibility" inspection wrapper, never a *PreflightError.
func TestValidateDestinationSchemaCompatibilityInspectionErrorIsPlain(t *testing.T) {
	sourceDB, sourceMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = destDB.Close() }()

	wantErr := errors.New("table spec fetch failed")
	g := createPreflightTestGraph()
	checker, _ := NewPreflightChecker(sourceDB, "sourcedb", g, logger.NewDefault())
	_ = checker.ConfigureDestination(destDB, "destdb", "destdb")
	run := &preflightRun{
		checker: checker, tables: g.AllNodes(),
		specsErr: wantErr, specsLoaded: true,
	}

	err = checker.ValidateDestinationSchemaCompatibility(context.Background(), run)
	if err == nil {
		t.Fatal("expected an inspection error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("inspection error must wrap %v, got: %v", wantErr, err)
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("inspection failure must be plain, got *PreflightError: %v", pe)
	}
	if !strings.Contains(err.Error(), "preflight destination_schema_compatibility inspection failed") {
		t.Fatalf("inspection error must carry goarchive's wrapper, got: %v", err)
	}
	if err := sourceMock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried the source despite the preloaded error: %v", err)
	}
	if err := destMock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried the destination despite the preloaded error: %v", err)
	}
}

// TestValidateDestinationSchemaCompatibilityConsumesCachedFact — contract 3. The stage
// must consume the run's cached specs rather than issuing its own queries: zero sqlmock
// expectations are registered on either pool, and the preloaded pair is deliberately
// incompatible so the stage's own verdict, not an empty result, is what proves it ran.
func TestValidateDestinationSchemaCompatibilityConsumesCachedFact(t *testing.T) {
	sourceDB, sourceMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = destDB.Close() }()

	g := createPreflightTestGraph()
	checker, _ := NewPreflightChecker(sourceDB, "sourcedb", g, logger.NewDefault())
	_ = checker.ConfigureDestination(destDB, "destdb", "destdb")

	sourceCols := []validations.ColumnSpec{specColT(1, "id", "bigint"), specColT(2, "name", "varchar(255)")}
	destCols := []validations.ColumnSpec{specColT(1, "id", "bigint"), specColT(2, "name", "varchar(100)")} // mismatch
	run := &preflightRun{
		checker: checker, tables: g.AllNodes(),
		specs:       []specPair{preloadedSchemaSpecPair(sourceCols, destCols)},
		specsLoaded: true,
	}

	err = checker.ValidateDestinationSchemaCompatibility(context.Background(), run)
	if err == nil {
		t.Fatal("expected the preloaded mismatch to surface as an error")
	}
	var pe *PreflightError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PreflightError, got %T: %v", err, err)
	}
	if pe.Check != "DEST_SCHEMA_COMPATIBILITY_CHECK" {
		t.Fatalf("expected Check == DEST_SCHEMA_COMPATIBILITY_CHECK, got %q", pe.Check)
	}
	if err := sourceMock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried the source despite the preloaded fact: %v", err)
	}
	if err := destMock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried the destination despite the preloaded fact: %v", err)
	}
}

// TestValidateDestinationSchemaCompatibilityAbortsOnInspectionIntegrityFailure proves the
// stage propagates evaluateSchemaCompatibility's OWN error, not just its verdict: an
// asymmetric index-capture (one side's TableSpec never captured indexes — the shape
// tableSpecs is supposed to prevent) must abort preflight rather than silently pass. This
// is the routed-through-the-stage counterpart to
// TestSchemaCompatUncapturedIndexesAbortBeforeDiffSpecs, and is what actually catches a
// stage that swallows evaluateSchemaCompatibility's error (`verdict, _ := evaluate...`).
func TestValidateDestinationSchemaCompatibilityAbortsOnInspectionIntegrityFailure(t *testing.T) {
	sourceDB, sourceMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = destDB.Close() }()

	g := createPreflightTestGraph()
	checker, _ := NewPreflightChecker(sourceDB, "sourcedb", g, logger.NewDefault())
	_ = checker.ConfigureDestination(destDB, "destdb", "destdb")

	pair := preloadedSchemaSpecPair(
		[]validations.ColumnSpec{specColT(1, "id", "bigint")},
		[]validations.ColumnSpec{specColT(1, "id", "bigint")},
	)
	pair.B.Captured = 0 // destination side never captured indexes

	run := &preflightRun{
		checker: checker, tables: g.AllNodes(),
		specs: []specPair{pair}, specsLoaded: true,
	}

	err = checker.ValidateDestinationSchemaCompatibility(context.Background(), run)
	if err == nil {
		t.Fatal("expected the stage to abort on an inspection-integrity failure")
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("an inspection-integrity abort must not be a *PreflightError, got: %v", pe)
	}
	if !strings.Contains(err.Error(), "did not capture its index section") {
		t.Fatalf("abort must come from checkAbsoluteInvariants' Captured guard, got: %v", err)
	}
	if strings.Contains(err.Error(), "IndexUnconfirmed") {
		t.Fatalf("the guard must run before DiffSpecs; this came from the diff loop: %v", err)
	}
	if err := sourceMock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried the source despite the preloaded fact: %v", err)
	}
	if err := destMock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried the destination despite the preloaded fact: %v", err)
	}
}

// TestValidateDestinationWritePermissionsNamesTables proves the migrated check reports
// the offending TABLE names rather than privilege names.
//
// The decoration DELIBERATELY CHANGES: 1.8 put a bare "orders" in Tables, 2.0 puts
// "orders(absent)". That follows the same decorate-the-Tables-entry convention as D5 and
// D6, and phase 008's characterization stays stable because chrTableNames strips the
// parenthetical before comparing. It is a change, not a preservation — do not describe it
// as preserving the 1.8 shape.
func TestValidateDestinationWritePermissionsNamesTables(t *testing.T) {
	findings := []validations.Finding{
		{Check: validations.IDTablePrivileges, Tables: []string{"orders"},
			Facts: validations.PrivilegeFact{Schema: "dst", Table: "orders",
				Privilege: validations.PrivilegeInsert, State: validations.GrantAbsent}},
		{Check: validations.IDTablePrivileges, Tables: []string{"lines"},
			Facts: validations.PrivilegeFact{Schema: "dst", Table: "lines",
				Privilege: validations.PrivilegeInsert, State: validations.GrantUnconfirmed}},
	}
	got, err := privilegeOffenders("destination_write", findings)
	if err != nil {
		t.Fatalf("privilegeOffenders: %v", err)
	}
	want := []string{"orders(absent)", "lines(unconfirmed)"}
	if len(got) != len(want) {
		t.Fatalf("privilegeOffenders = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("privilegeOffenders = %v, want %v", got, want)
		}
	}
}

// TestValidateDestinationWritePermissionsInspectionErrorIsPlain proves an inspection
// failure (destGrants returning an error) surfaces as a plain error carrying goarchive's
// own wrapper text, never as a *PreflightError. Mirrors
// TestValidateJobSchemaPermissionsInspectionErrorIsPlain; only the stage word and the
// validator differ.
func TestValidateDestinationWritePermissionsInspectionErrorIsPlain(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	wantErr := errors.New("grants failed")
	p := &PreflightChecker{
		logger: logger.NewDefault(), destinationDB: db, destinationDBName: "destdb",
	}
	run := &preflightRun{
		checker: p, tables: []string{"orders"},
		dstGrantsErr: wantErr, dstGrantsLoaded: true,
	}

	err = p.ValidateDestinationWritePermissions(context.Background(), run)
	if err == nil {
		t.Fatal("expected an inspection error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("inspection error must wrap %v, got: %v", wantErr, err)
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("inspection failure must be plain, got *PreflightError: %v", pe)
	}
	if !strings.Contains(err.Error(), "preflight destination_write inspection failed") {
		t.Fatalf("inspection error must carry goarchive's wrapper, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded error: %v", err)
	}
}

// TestValidateDestinationWritePermissionsConsumesTheCachedFact proves the stage consumes
// the memoized destGrants fact instead of acquiring its own. Zero SQL expectations are
// registered: a re-querying stage gets no match, returns a plain error, and fails the
// *PreflightError assertion below. A zero-value Grants reports GrantUnknown for every
// privilege (it is unpopulated), so all graph tables fail closed — which also pins D1's
// treatment of GrantUnknown end-to-end.
//
// run.tables must be set: this validator reads run.graphTables(), and an empty slice
// makes CheckTablePrivileges return no findings, passing this test vacuously.
func TestValidateDestinationWritePermissionsConsumesTheCachedFact(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	p := &PreflightChecker{
		logger: logger.NewDefault(), destinationDB: db, destinationDBName: "destdb",
	}
	run := &preflightRun{
		checker: p, tables: []string{"orders", "lines"},
		dstGrants: validations.Grants{}, dstGrantsLoaded: true,
	}

	err = p.ValidateDestinationWritePermissions(context.Background(), run)
	var pe *PreflightError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PreflightError from the cached fact, got %T: %v", err, err)
	}
	if pe.Check != "DEST_WRITE_PERMISSION_CHECK" {
		t.Fatalf("Check = %q, want DEST_WRITE_PERMISSION_CHECK", pe.Check)
	}
	for _, want := range []string{"orders(unknown)", "lines(unknown)"} {
		found := false
		for _, got := range pe.Tables {
			if got == want {
				found = true
			}
		}
		if !found {
			t.Errorf("Tables %v does not contain %q", pe.Tables, want)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded fact: %v", err)
	}
}

// TestValidateSourceDeletePermissionsInspectionErrorIsPlain proves an inspection
// failure (sourceGrants returning an error) surfaces as a plain error carrying
// goarchive's own wrapper text, never as a *PreflightError. Mirrors
// TestValidateDestinationWritePermissionsInspectionErrorIsPlain; only the stage word,
// the validator, and the source-side checker fields differ.
func TestValidateSourceDeletePermissionsInspectionErrorIsPlain(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	wantErr := errors.New("grants failed")
	p := &PreflightChecker{
		logger: logger.NewDefault(), db: db, sourceDBName: "sourcedb",
	}
	run := &preflightRun{
		checker: p, tables: []string{"orders"},
		srcGrantsErr: wantErr, srcGrantsLoaded: true,
	}

	err = p.ValidateSourceDeletePermissions(context.Background(), run)
	if err == nil {
		t.Fatal("expected an inspection error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("inspection error must wrap %v, got: %v", wantErr, err)
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("inspection failure must be plain, got *PreflightError: %v", pe)
	}
	if !strings.Contains(err.Error(), "preflight source_delete inspection failed") {
		t.Fatalf("inspection error must carry goarchive's wrapper, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded error: %v", err)
	}
}

// TestValidateSourceDeletePermissionsConsumesTheCachedFact proves the stage consumes
// the memoized sourceGrants fact instead of acquiring its own. Zero SQL expectations
// are registered: a re-querying stage gets no match, returns a plain error, and fails
// the *PreflightError assertion below. A zero-value Grants reports GrantUnknown for
// every privilege (it is unpopulated), so all graph tables fail closed — which also
// pins D1's treatment of GrantUnknown end-to-end.
//
// run.tables must be set: this validator reads run.graphTables(), and an empty slice
// makes CheckTablePrivileges return no findings, passing this test vacuously.
func TestValidateSourceDeletePermissionsConsumesTheCachedFact(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	p := &PreflightChecker{
		logger: logger.NewDefault(), db: db, sourceDBName: "sourcedb",
	}
	run := &preflightRun{
		checker: p, tables: []string{"orders", "lines"},
		srcGrants: validations.Grants{}, srcGrantsLoaded: true,
	}

	err = p.ValidateSourceDeletePermissions(context.Background(), run)
	var pe *PreflightError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PreflightError from the cached fact, got %T: %v", err, err)
	}
	if pe.Check != "SOURCE_DELETE_PERMISSION_CHECK" {
		t.Fatalf("Check = %q, want SOURCE_DELETE_PERMISSION_CHECK", pe.Check)
	}
	for _, want := range []string{"orders(unknown)", "lines(unknown)"} {
		found := false
		for _, got := range pe.Tables {
			if got == want {
				found = true
			}
		}
		if !found {
			t.Errorf("Tables %v does not contain %q", pe.Tables, want)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded fact: %v", err)
	}
}

// TestValidateSourceSelectPermissionsInspectionErrorIsPlain proves an inspection
// failure (sourceGrants returning an error) surfaces as a plain error carrying
// goarchive's own wrapper text, never as a *PreflightError. Mirrors
// TestValidateSourceDeletePermissionsInspectionErrorIsPlain; only the stage word and
// the validator differ.
func TestValidateSourceSelectPermissionsInspectionErrorIsPlain(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	wantErr := errors.New("grants failed")
	p := &PreflightChecker{
		logger: logger.NewDefault(), db: db, sourceDBName: "sourcedb",
	}
	run := &preflightRun{
		checker: p, tables: []string{"orders"},
		srcGrantsErr: wantErr, srcGrantsLoaded: true,
	}

	err = p.ValidateSourceSelectPermissions(context.Background(), run)
	if err == nil {
		t.Fatal("expected an inspection error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("inspection error must wrap %v, got: %v", wantErr, err)
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("inspection failure must be plain, got *PreflightError: %v", pe)
	}
	if !strings.Contains(err.Error(), "preflight source_select inspection failed") {
		t.Fatalf("inspection error must carry goarchive's wrapper, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded error: %v", err)
	}
}

// TestValidateSourceSelectPermissionsConsumesTheCachedFact proves the stage consumes
// the memoized sourceGrants fact instead of acquiring its own. Zero SQL expectations
// are registered: a re-querying stage gets no match, returns a plain error, and fails
// the *PreflightError assertion below. A zero-value Grants reports GrantUnknown for
// every privilege (it is unpopulated), so all graph tables fail closed.
//
// run.tables must be set: this validator reads run.graphTables(), and an empty slice
// makes CheckTablePrivileges return no findings, passing this test vacuously.
func TestValidateSourceSelectPermissionsConsumesTheCachedFact(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	p := &PreflightChecker{
		logger: logger.NewDefault(), db: db, sourceDBName: "sourcedb",
	}
	run := &preflightRun{
		checker: p, tables: []string{"orders", "lines"},
		srcGrants: validations.Grants{}, srcGrantsLoaded: true,
	}

	err = p.ValidateSourceSelectPermissions(context.Background(), run)
	var pe *PreflightError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PreflightError from the cached fact, got %T: %v", err, err)
	}
	if pe.Check != "SOURCE_SELECT_PERMISSION_CHECK" {
		t.Fatalf("Check = %q, want SOURCE_SELECT_PERMISSION_CHECK", pe.Check)
	}
	for _, want := range []string{"orders(unknown)", "lines(unknown)"} {
		found := false
		for _, got := range pe.Tables {
			if got == want {
				found = true
			}
		}
		if !found {
			t.Errorf("Tables %v does not contain %q", pe.Tables, want)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded fact: %v", err)
	}
}

// TestValidateForeignKeyIndexesInspectionErrorIsPlain — contract 2. Preload fkOutErr /
// fkOutLoaded so no query is issued, then assert the returned error wraps the cause,
// carries goarchive's own wrapper text, and is NOT a *PreflightError. Mirrors
// TestValidateSourceSelectPermissionsInspectionErrorIsPlain; only the stage word and the
// validator differ.
func TestValidateForeignKeyIndexesInspectionErrorIsPlain(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	wantErr := errors.New("fk fetch failed")
	p := &PreflightChecker{
		logger: logger.NewDefault(), db: db, sourceDBName: "sourcedb",
	}
	run := &preflightRun{
		checker: p, tables: []string{"orders"},
		fkOutErr: wantErr, fkOutLoaded: true,
	}

	err = p.ValidateForeignKeyIndexes(context.Background(), run)
	if err == nil {
		t.Fatal("expected an inspection error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("inspection error must wrap %v, got: %v", wantErr, err)
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("inspection failure must be plain, got *PreflightError: %v", pe)
	}
	if !strings.Contains(err.Error(), "preflight fk_index inspection failed") {
		t.Fatalf("inspection error must carry goarchive's wrapper, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded error: %v", err)
	}
}

// TestValidateForeignKeyIndexesConsumesTheCachedFact — contract 3. Preload fkOut with an
// UNINDEXED constraint and set fkOutLoaded, registering NO sqlmock expectation: a stage
// that re-queried would get no match, return a plain error, and fail the *PreflightError
// assertion below.
//
// The preloaded fact must have Indexed:false and a non-empty ChildColumns. An indexed or
// empty fact yields no findings, and the test would pass vacuously against a stage that
// never consulted the fact at all.
func TestValidateForeignKeyIndexesConsumesTheCachedFact(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	p := &PreflightChecker{
		logger: logger.NewDefault(), db: db, sourceDBName: "sourcedb",
	}
	run := &preflightRun{
		checker: p, tables: []string{"order_lines"},
		fkOut: validations.ForeignKeyResult{
			Keys: []validations.ForeignKey{{
				ConstraintName: "fk_ol_o", ChildSchema: "sourcedb", ChildTable: "order_lines",
				ChildColumns: []string{"order_id"}, ParentTable: "orders", Indexed: false,
			}},
		},
		fkOutLoaded: true,
	}

	err = p.ValidateForeignKeyIndexes(context.Background(), run)
	var pe *PreflightError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PreflightError from the cached fact, got %T: %v", err, err)
	}
	if pe.Check != "FK_INDEX_CHECK" {
		t.Fatalf("Check = %q, want FK_INDEX_CHECK", pe.Check)
	}
	if len(pe.Tables) != 1 || pe.Tables[0] != "order_lines.order_id" {
		t.Fatalf("Tables = %v, want [order_lines.order_id]", pe.Tables)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded fact: %v", err)
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

// TestValidateForeignKeyCoverage_FailsForUncoveredCascadeAndRestrict preloads the
// run's fkIn fact directly (Step 5's cached-fact pattern) rather than mocking SQL:
// ValidateForeignKeyCoverage now reads validations.ForeignKeyResult via
// run.fkIncoming, not the deleted raw information_schema query.
func TestValidateForeignKeyCoverage_FailsForUncoveredCascadeAndRestrict(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)
	run := &preflightRun{
		checker: checker, tables: g.AllNodes(),
		fkIn: validations.ForeignKeyResult{
			Keys: []validations.ForeignKey{
				{ConstraintName: "fk_ext_orders_1", ChildSchema: "testdb", ChildTable: "external_cascade",
					ParentSchema: "testdb", ParentTable: "orders", OnDelete: "CASCADE"},
				{ConstraintName: "fk_ext_orders_2", ChildSchema: "testdb", ChildTable: "external_restrict",
					ParentSchema: "testdb", ParentTable: "orders", OnDelete: "RESTRICT"},
			},
			Visibility: validations.VisibilityComplete,
		},
		fkInLoaded: true,
	}

	err := checker.ValidateForeignKeyCoverage(context.Background(), run)
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
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded fact: %v", err)
	}
}

// TestValidateForeignKeyCoverage_CrossSchemaSameNameChild proves an out-of-graph
// child in ANOTHER schema that happens to share a name with a graph table
// (otherdb.orders vs in-graph orders) is still flagged: membership must be
// schema-aware, not bare-name. Preloads the run's fkIn fact directly, same as above.
func TestValidateForeignKeyCoverage_CrossSchemaSameNameChild(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph() // graph = {users, orders, order_items} in "testdb"
	checker, _ := NewPreflightChecker(db, "testdb", g, logger.NewDefault())
	ctx := context.Background()

	// otherdb.orders (out-of-graph) references in-graph testdb.users.
	run := &preflightRun{
		checker: checker, tables: g.AllNodes(),
		fkIn: validations.ForeignKeyResult{
			Keys: []validations.ForeignKey{
				{ConstraintName: "fk_other_users", ChildSchema: "otherdb", ChildTable: "orders",
					ParentSchema: "testdb", ParentTable: "users", OnDelete: "CASCADE"},
			},
			Visibility: validations.VisibilityComplete,
		},
		fkInLoaded: true,
	}

	err := checker.ValidateForeignKeyCoverage(ctx, run)
	if err == nil {
		t.Fatal("expected FK_COVERAGE_CHECK for same-name cross-schema child, got nil")
	}
	if pfErr, ok := err.(*PreflightError); !ok || pfErr.Check != "FK_COVERAGE_CHECK" {
		t.Fatalf("expected FK_COVERAGE_CHECK, got: %v", err)
	}
	if !strings.Contains(err.Error(), "otherdb") {
		t.Fatalf("expected error to qualify the child schema, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded fact: %v", err)
	}
}

// ============================================================================
// ValidateForeignKeyMetadataVisibility Tests
// ============================================================================

// TestValidateForeignKeyMetadataVisibilityInspectionErrorIsPlain — contract 2. A
// memoized fkIncoming error must surface as a PLAIN error carrying goarchive's
// "fk_metadata_visibility" inspection wrapper, never a *PreflightError.
func TestValidateForeignKeyMetadataVisibilityInspectionErrorIsPlain(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	wantErr := errors.New("fk fetch failed")
	g := createPreflightTestGraph()
	checker, _ := NewPreflightChecker(db, "testdb", g, logger.NewDefault())
	run := &preflightRun{
		checker: checker, tables: g.AllNodes(),
		fkInErr: wantErr, fkInLoaded: true,
	}

	err = checker.ValidateForeignKeyMetadataVisibility(context.Background(), run)
	if err == nil {
		t.Fatal("expected an inspection error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("inspection error must wrap %v, got: %v", wantErr, err)
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("inspection failure must be plain, got *PreflightError: %v", pe)
	}
	if !strings.Contains(err.Error(), "preflight fk_metadata_visibility inspection failed") {
		t.Fatalf("inspection error must carry goarchive's wrapper, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded error: %v", err)
	}
}

// TestValidateForeignKeyMetadataVisibilityConsumesTheCachedFact — contract 3, and the
// Step 6 rewrite of the deleted TestValidateForeignKeyMetadataVisibility_NoGlobalSelect:
// deviation D2 replaced the 1.8 global-SELECT mechanism that test exercised through
// sqlmock rows with the library's typed completeness proof. Preloading fkIn/fkInLoaded
// with VisibilityUnconfirmed and a non-empty tables slice (CheckFKClosure returns nil,
// vacuously, for an empty target) and registering ZERO sqlmock expectations proves the
// stage judges the cached fact, not a fresh query.
func TestValidateForeignKeyMetadataVisibilityConsumesTheCachedFact(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()
	g := createPreflightTestGraph()
	checker, _ := NewPreflightChecker(db, "testdb", g, logger.NewDefault())
	run := &preflightRun{
		checker: checker, tables: g.AllNodes(),
		fkIn:       validations.ForeignKeyResult{Visibility: validations.VisibilityUnconfirmed},
		fkInLoaded: true,
	}

	err := checker.ValidateForeignKeyMetadataVisibility(context.Background(), run)
	if err == nil {
		t.Fatal("expected FK_COVERAGE_VISIBILITY_CHECK when visibility is unconfirmed, got nil")
	}
	if pfErr, ok := err.(*PreflightError); !ok || pfErr.Check != "FK_COVERAGE_VISIBILITY_CHECK" {
		t.Fatalf("expected FK_COVERAGE_VISIBILITY_CHECK, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded fact: %v", err)
	}
}

// TestValidateForeignKeyMetadataVisibilityPassesOnVisibilityComplete is Step 6's
// positive counterpart: VisibilityComplete preloaded, zero sqlmock expectations.
func TestValidateForeignKeyMetadataVisibilityPassesOnVisibilityComplete(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()
	g := createPreflightTestGraph()
	checker, _ := NewPreflightChecker(db, "testdb", g, logger.NewDefault())
	run := &preflightRun{
		checker: checker, tables: g.AllNodes(),
		fkIn:       validations.ForeignKeyResult{Visibility: validations.VisibilityComplete},
		fkInLoaded: true,
	}

	if err := checker.ValidateForeignKeyMetadataVisibility(context.Background(), run); err != nil {
		t.Fatalf("expected pass with VisibilityComplete, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded fact: %v", err)
	}
}

// TestValidateForeignKeyCoverageInspectionErrorIsPlain — contract 2. A memoized
// fkIncoming error must surface as a PLAIN error carrying goarchive's "fk_coverage"
// inspection wrapper, never a *PreflightError.
func TestValidateForeignKeyCoverageInspectionErrorIsPlain(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	wantErr := errors.New("fk fetch failed")
	g := createPreflightTestGraph()
	checker, _ := NewPreflightChecker(db, "testdb", g, logger.NewDefault())
	run := &preflightRun{
		checker: checker, tables: g.AllNodes(),
		fkInErr: wantErr, fkInLoaded: true,
	}

	err = checker.ValidateForeignKeyCoverage(context.Background(), run)
	if err == nil {
		t.Fatal("expected an inspection error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("inspection error must wrap %v, got: %v", wantErr, err)
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("inspection failure must be plain, got *PreflightError: %v", pe)
	}
	if !strings.Contains(err.Error(), "preflight fk_coverage inspection failed") {
		t.Fatalf("inspection error must carry goarchive's wrapper, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded error: %v", err)
	}
}

// TestValidateForeignKeyCoverageConsumesTheCachedFact — contract 3. Preload fkIn with
// ONE external-child key and fkInLoaded, registering NO sqlmock expectation: a stage
// that re-queried would get no match, return a plain error, and fail the
// *PreflightError assertion below.
func TestValidateForeignKeyCoverageConsumesTheCachedFact(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()
	g := createPreflightTestGraph()
	checker, _ := NewPreflightChecker(db, "testdb", g, logger.NewDefault())
	run := &preflightRun{
		checker: checker, tables: g.AllNodes(),
		fkIn: validations.ForeignKeyResult{
			Keys: []validations.ForeignKey{
				{ConstraintName: "fk_ext_orders", ChildSchema: "testdb", ChildTable: "external_child",
					ParentSchema: "testdb", ParentTable: "orders", OnDelete: "RESTRICT"},
			},
			Visibility: validations.VisibilityComplete,
		},
		fkInLoaded: true,
	}

	err := checker.ValidateForeignKeyCoverage(context.Background(), run)
	if err == nil {
		t.Fatal("expected FK_COVERAGE_CHECK for the uncovered external child, got nil")
	}
	if pfErr, ok := err.(*PreflightError); !ok || pfErr.Check != "FK_COVERAGE_CHECK" {
		t.Fatalf("expected FK_COVERAGE_CHECK, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded fact: %v", err)
	}
}

// ============================================================================
// ValidateInternalFKCoverage Tests
// ============================================================================

// TestValidateInternalFKCoverage_FlatConfigMissingNesting is also the explicit
// contract-3 proof: the preloaded fact plus ZERO registered sqlmock expectations
// proves the stage judges the cached fkWithin fact rather than issuing its own query.
func TestValidateInternalFKCoverage_FlatConfigMissingNesting(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	// Graph: orders -> order_items, orders -> item_shipments (flat, both siblings)
	// But DB has: item_shipments.shipment_item_id -> order_items.item_id (nested FK)
	g := graph.NewGraph("orders", "order_id")
	g.AddNode("order_items", &graph.Node{Name: "order_items", ForeignKey: "order_id", ReferenceKey: "order_id", DependencyType: "1-N"})
	g.AddNode("item_shipments", &graph.Node{Name: "item_shipments", ForeignKey: "order_id", ReferenceKey: "order_id", DependencyType: "1-N"})
	g.SetPK("order_items", "item_id")
	g.SetPK("item_shipments", "shipment_id")
	g.AddEdgeWithMeta("orders", "order_items", "order_id", "order_id", "1-N")
	g.AddEdgeWithMeta("orders", "item_shipments", "order_id", "order_id", "1-N")

	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)

	// DB reports an FK from item_shipments.shipment_item_id -> order_items.item_id
	// This FK is NOT represented in the graph (item_shipments is sibling, not child of order_items).
	// Child and parent column names are deliberately distinct so that swapping
	// childColumn/parentColumn in reconcileInternalFKs produces a different, and
	// therefore detectable, message.
	run := &preflightRun{
		checker: checker, tables: g.AllNodes(),
		fkWi: validations.ForeignKeyResult{Keys: []validations.ForeignKey{
			{ConstraintName: "fk_items_orders", ChildTable: "order_items", ChildColumns: []string{"order_id"},
				ParentTable: "orders", ParentColumns: []string{"order_id"}},
			{ConstraintName: "fk_ship_orders", ChildTable: "item_shipments", ChildColumns: []string{"order_id"},
				ParentTable: "orders", ParentColumns: []string{"order_id"}},
			{ConstraintName: "fk_ship_items", ChildTable: "item_shipments", ChildColumns: []string{"shipment_item_id"},
				ParentTable: "order_items", ParentColumns: []string{"item_id"}},
		}},
		fkWiLoaded: true,
	}

	err := checker.ValidateInternalFKCoverage(context.Background(), run)
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
	// Distinct child/parent column names in the fixture mean this assertion only
	// passes if the message names them in the correct child -> parent order; a
	// swap of childColumn/parentColumn in reconcileInternalFKs would instead
	// produce "item_shipments.item_id -> order_items.shipment_item_id".
	if !strings.Contains(preflightErr.Error(), "item_shipments.shipment_item_id -> order_items.item_id") {
		t.Fatalf("expected error to name child->parent columns in order, got: %v", preflightErr)
	}
	if !strings.Contains(preflightErr.Error(), "no graph edge") {
		t.Fatalf("expected 'no graph edge' reason, got: %v", preflightErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded fact: %v", err)
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

	run := &preflightRun{
		checker: checker, tables: g.AllNodes(),
		fkWi: validations.ForeignKeyResult{Keys: []validations.ForeignKey{
			{ConstraintName: "fk_items_orders", ChildTable: "order_items", ChildColumns: []string{"order_id"},
				ParentTable: "orders", ParentColumns: []string{"order_id"}},
			{ConstraintName: "fk_ship_items", ChildTable: "item_shipments", ChildColumns: []string{"item_id"},
				ParentTable: "order_items", ParentColumns: []string{"item_id"}},
		}},
		fkWiLoaded: true,
	}

	err := checker.ValidateInternalFKCoverage(context.Background(), run)
	if err != nil {
		t.Fatalf("expected no error for properly nested config, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded fact: %v", err)
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

	run := &preflightRun{
		checker: checker, tables: g.AllNodes(),
		fkWi: validations.ForeignKeyResult{Keys: []validations.ForeignKey{
			{ConstraintName: "fk_pay_orders", ChildTable: "payments", ChildColumns: []string{"customer_id"},
				ParentTable: "orders", ParentColumns: []string{"order_id"}},
		}},
		fkWiLoaded: true,
	}

	err := checker.ValidateInternalFKCoverage(context.Background(), run)
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
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded fact: %v", err)
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

	run := &preflightRun{
		checker: checker, tables: g.AllNodes(),
		fkWi: validations.ForeignKeyResult{Keys: []validations.ForeignKey{
			{ConstraintName: "fk_line_orders", ChildTable: "line_items", ChildColumns: []string{"order_id"},
				ParentTable: "orders", ParentColumns: []string{"id"}},
		}},
		fkWiLoaded: true,
	}

	err := checker.ValidateInternalFKCoverage(context.Background(), run)
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
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded fact: %v", err)
	}
}

// TestValidateInternalFKCoverage_NoInternalFKs preloads an EMPTY ForeignKeyResult, not
// an external/out-of-graph foreign key. fkWithin is defined as the both-endpoints-in-graph
// set, so seeding it with a key the Within selector could never return would violate the
// accessor's own fact contract and test a state that cannot occur.
func TestValidateInternalFKCoverage_NoInternalFKs(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)

	run := &preflightRun{
		checker: checker, tables: g.AllNodes(),
		fkWi:       validations.ForeignKeyResult{},
		fkWiLoaded: true,
	}

	err := checker.ValidateInternalFKCoverage(context.Background(), run)
	if err != nil {
		t.Fatalf("expected no error when no internal FKs exist, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded fact: %v", err)
	}
}

func TestValidateInternalFKCoverage_SelfReferencingFK(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	g := graph.NewGraph("categories", "id")

	log := logger.NewDefault()
	checker, _ := NewPreflightChecker(db, "testdb", g, log)

	// Self-referencing FK: categories.parent_id -> categories.id
	run := &preflightRun{
		checker: checker, tables: g.AllNodes(),
		fkWi: validations.ForeignKeyResult{Keys: []validations.ForeignKey{
			{ConstraintName: "fk_cat_parent", ChildTable: "categories", ChildColumns: []string{"parent_id"},
				ParentTable: "categories", ParentColumns: []string{"id"}},
		}},
		fkWiLoaded: true,
	}

	err := checker.ValidateInternalFKCoverage(context.Background(), run)
	if err != nil {
		t.Fatalf("expected no error for self-referencing FK, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded fact: %v", err)
	}
}

// TestValidateInternalFKCoverage_MultipleFailures is also the sort proof. Facts are
// supplied payments-first; sorted output must put item_shipments first ("  - i" <
// "  - p"). Asserting the exact adjacent block is what fails if the sort is removed —
// two independent strings.Contains checks would not.
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

	// Facts are supplied payments-first, then item_shipments, then order_items (the
	// third matches its edge and yields no line, exactly as in the 1.8 fixture).
	run := &preflightRun{
		checker: checker, tables: g.AllNodes(),
		fkWi: validations.ForeignKeyResult{Keys: []validations.ForeignKey{
			{ConstraintName: "fk_pay_orders", ChildTable: "payments", ChildColumns: []string{"customer_id"},
				ParentTable: "orders", ParentColumns: []string{"order_id"}},
			{ConstraintName: "fk_ship_items", ChildTable: "item_shipments", ChildColumns: []string{"item_id"},
				ParentTable: "order_items", ParentColumns: []string{"item_id"}},
			{ConstraintName: "fk_items_orders", ChildTable: "order_items", ChildColumns: []string{"order_id"},
				ParentTable: "orders", ParentColumns: []string{"order_id"}},
		}},
		fkWiLoaded: true,
	}

	err := checker.ValidateInternalFKCoverage(context.Background(), run)
	if err == nil {
		t.Fatal("expected error for multiple failures")
	}

	preflightErr, ok := err.(*PreflightError)
	if !ok {
		t.Fatalf("expected PreflightError, got %T", err)
	}

	wantBlock := "  - item_shipments.item_id -> order_items.item_id (constraint: fk_ship_items) [no graph edge]\n" +
		"  - payments.customer_id -> orders.order_id (constraint: fk_pay_orders) [FK column mismatch: config has 'wrong_col', DB has 'customer_id']"
	if !strings.Contains(preflightErr.Error(), wantBlock) {
		t.Fatalf("discrepancies must appear sorted; got:\n%s", preflightErr.Error())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded fact: %v", err)
	}
}

// TestValidateInternalFKCoverageInspectionErrorIsPlain — contract 2. A memoized
// fkWithin error must surface as a PLAIN error carrying goarchive's
// "internal_fk_coverage" inspection wrapper, never a *PreflightError.
func TestValidateInternalFKCoverageInspectionErrorIsPlain(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	wantErr := errors.New("fk fetch failed")
	g := createPreflightTestGraph()
	checker, _ := NewPreflightChecker(db, "testdb", g, logger.NewDefault())
	run := &preflightRun{
		checker: checker, tables: g.AllNodes(),
		fkWiErr: wantErr, fkWiLoaded: true,
	}

	err = checker.ValidateInternalFKCoverage(context.Background(), run)
	if err == nil {
		t.Fatal("expected an inspection error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("inspection error must wrap %v, got: %v", wantErr, err)
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("inspection failure must be plain, got *PreflightError: %v", pe)
	}
	if !strings.Contains(err.Error(), "preflight internal_fk_coverage inspection failed") {
		t.Fatalf("inspection error must carry goarchive's wrapper, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded error: %v", err)
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
	if !strings.Contains(msg, "TEST_CHECK") {
		t.Errorf("Expected error to contain check name, got: %s", msg)
	}

	// Should contain tables
	if !strings.Contains(msg, "table1") || !strings.Contains(msg, "table2") {
		t.Errorf("Expected error to contain table names, got: %s", msg)
	}
}

func TestPreflightError_ErrorNoTables(t *testing.T) {
	err := &PreflightError{
		Check:   "TEST_CHECK",
		Message: "test message",
	}

	msg := err.Error()
	want := "TEST_CHECK: test message"
	if msg != want {
		t.Errorf("expected error %q, got %q", want, msg)
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

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected error chain to contain context.Canceled (cancellation must "+
			"propagate, not just any query failure), got: %v", err)
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
	// destinationDB stays nil, but destinationDBName is deliberately non-empty
	// (unlike the old fixture, which left both unset) so the two conditions are
	// separable: a guard mistakenly keyed on destinationDBName would not trip
	// here. Built directly since this is package archiver and
	// NewPreflightChecker/ConfigureDestination refuse a nil destination handle.
	checker := &PreflightChecker{
		db:                db,
		sourceDBName:      "sourcedb",
		destinationDB:     nil,
		destinationDBName: "destdb",
		graph:             g,
		logger:            log,
	}

	ctx := context.Background()

	err = checker.ValidateDestinationTablesExist(ctx, newPreflightRun(checker))
	if err == nil {
		t.Fatal("ValidateDestinationTablesExist should return error when destination is nil")
	}
	if !strings.Contains(err.Error(), "destination database not configured") {
		t.Errorf("Unexpected error message: %v", err)
	}
	// The outer p.destinationDB == nil guard must short-circuit before destTables'
	// inner dstInspector == nil guard ever runs. If it didn't, the identical text
	// would come back wrapped by inspectionError("destination_table_existence",
	// ...), which a substring check can't tell apart from the plain guard error but
	// an exact match can.
	if err.Error() != "destination database not configured; call ConfigureDestination first" {
		t.Errorf("expected the outer guard's plain, unwrapped error, got: %v", err)
	}

	err = checker.ValidateDestinationSchemaCompatibility(ctx, newPreflightRun(checker))
	if err == nil {
		t.Fatal("ValidateDestinationSchemaCompatibility should return error when destination is nil")
	}
	if !strings.Contains(err.Error(), "destination database not configured") {
		t.Errorf("Unexpected error message: %v", err)
	}

	err = checker.ValidateDestinationWritePermissions(ctx, nil)
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

	sourceCols := []validations.ColumnSpec{
		withCharset(specColT(1, "name", "varchar(255)"), "utf8mb4", "utf8mb4_0900_ai_ci"),
	}
	destCols := []validations.ColumnSpec{
		withCharset(specColT(1, "name", "varchar(255)"), "latin1", "latin1_swedish_ci"),
	}
	run := &preflightRun{
		checker: checker, tables: g.AllNodes(),
		specs:       []specPair{preloadedSchemaSpecPair(sourceCols, destCols)},
		specsLoaded: true,
	}

	err := checker.ValidateDestinationSchemaCompatibility(context.Background(), run)
	if err != nil {
		t.Fatalf("charset mismatch should be allowed (warn) under sha256 verification, got: %v", err)
	}
	if err := sourceMock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried the source despite the preloaded fact: %v", err)
	}
	if err := destMock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried the destination despite the preloaded fact: %v", err)
	}
}

// ============================================================================
// Source DELETE Privilege Tests (Task 6)
// ============================================================================

// ============================================================================
// ValidateJobSchemaPermissions Tests
//
// Inspector.Grants owns six statements (CURRENT_USER, ENABLED_ROLES,
// partial_revokes, global grants, schema grants, table grants). The stage no
// longer issues its own privilege queries — it consumes the run-scoped
// destGrants fact — so per-sequence sqlmock tests reproducing that library-
// internal query order no longer apply here. Sequence-level coverage of
// Inspector.Grants itself lives in the library; this file's contracts are the
// cache/stage boundary (below) and the real-MySQL fixtures in
// preflight_schema_integration_test.go and
// characterization_permissions_integration_test.go.
// ============================================================================

// TestValidateJobSchemaPermissions_NilDestination proves the stage's own
// destinationDB == nil guard returns before ever touching the run, so a nil run is
// safe to pass here. destinationDBName is deliberately non-empty (unlike the old
// fixture, which left both unset) so a guard mistakenly keyed on
// destinationDBName instead of destinationDB would not trip here and would go on
// to dereference the nil run.
func TestValidateJobSchemaPermissions_NilDestination(t *testing.T) {
	p := &PreflightChecker{
		logger:            logger.NewDefault(),
		jobSchemaName:     "goarchive",
		destinationDBName: "destdb",
	}
	err := p.ValidateJobSchemaPermissions(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for nil destination")
	}
	if !strings.Contains(err.Error(), "destination database not configured") {
		t.Fatalf("expected 'destination database not configured', got: %v", err)
	}
}

// TestValidateJobSchemaPermissionsInspectionErrorIsPlain proves a destGrants
// inspection failure surfaces as a plain error carrying goarchive's own wrapper text,
// never as a *PreflightError: a *PreflightError means "the schema is wrong", while
// this means "we could not find out". The raw error contains neither "job_schema" nor
// the wrapper text, so an unwrapped `return err` cannot pass this test.
func TestValidateJobSchemaPermissionsInspectionErrorIsPlain(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	wantErr := errors.New("grants failed")
	p := &PreflightChecker{
		logger: logger.NewDefault(), destinationDB: db,
		destinationDBName: "destdb", jobSchemaName: "jobs",
	}
	run := &preflightRun{
		checker: p, dstGrantsErr: wantErr, dstGrantsLoaded: true,
	}

	err = p.ValidateJobSchemaPermissions(context.Background(), run)
	if err == nil {
		t.Fatal("expected an inspection error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("inspection error must wrap %v, got: %v", wantErr, err)
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("inspection failure must be plain, got *PreflightError: %v", pe)
	}
	if !strings.Contains(err.Error(), "preflight job_schema inspection failed") {
		t.Fatalf("inspection error must carry goarchive's wrapper, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded error: %v", err)
	}
}

// TestValidateJobSchemaPermissionsConsumesTheCachedFact proves the stage reads the
// destGrants fact through the run's cache rather than bypassing it to query again.
// An unpopulated validations.Grants{} is intentional: Grants.resolve returns
// GrantUnknown when !g.populated, so D1 must return a JOB_SCHEMA_PERMISSION_CHECK
// *PreflightError whose states include CREATE(unknown), SELECT(unknown),
// INSERT(unknown), and UPDATE(unknown). If the stage bypassed the accessor and
// queried again, sqlmock would return an unexpected-query inspection error and the
// *PreflightError assertion below would fail.
func TestValidateJobSchemaPermissionsConsumesTheCachedFact(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	p := &PreflightChecker{
		logger: logger.NewDefault(), destinationDB: db,
		destinationDBName: "destdb", jobSchemaName: "jobs",
	}
	run := &preflightRun{
		checker: p, dstGrants: validations.Grants{}, dstGrantsLoaded: true,
	}

	err = p.ValidateJobSchemaPermissions(context.Background(), run)
	var pe *PreflightError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PreflightError from the cached fact, got %T: %v", err, err)
	}
	if pe.Check != "JOB_SCHEMA_PERMISSION_CHECK" {
		t.Fatalf("Check = %q, want JOB_SCHEMA_PERMISSION_CHECK", pe.Check)
	}
	for _, want := range []string{
		"CREATE(unknown)", "SELECT(unknown)", "INSERT(unknown)", "UPDATE(unknown)",
	} {
		if !strings.Contains(pe.Message, want) {
			t.Errorf("message %q does not contain %q", pe.Message, want)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stage queried despite the preloaded fact: %v", err)
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
// Exactly ONE PrimaryKeys expectation is registered per case, and the two stages run in
// sequence exactly as RunWithProfile calls them (positions 3 and 4). A stage that built
// its own Inspector would issue a second query, receive "all expectations were already
// fulfilled", and fail here — which no other test catches.
//
// The varchar case is what keeps this non-vacuous. ValidateSingleColumnPrimaryKey passes
// on it (the key IS single-column) while ValidateRootPKNumeric must reject it, so the
// second stage can only produce the right answer by reading the shared fact. Without that
// case a ValidateRootPKNumeric that silently returned nil would pass: the sole expectation
// is already consumed by the first stage.
//
// This replaces a GetRootPKMeta assertion that covered the graph write-back deleted in
// phase 032. There is no write-back to assert on any more.
func TestPKStagesShareOneCachedFact(t *testing.T) {
	cases := []struct {
		name       string
		dataType   string
		columnType string
		wantErr    string // "" means the root-PK stage must pass
	}{
		{"integer key passes both stages", "bigint", "bigint unsigned", ""},
		{"varchar key passes the shape stage and fails the root-PK stage",
			"varchar", "varchar(36)", "ROOT_PK_TYPE_UNSUPPORTED"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer func() { _ = db.Close() }()

			mock.ExpectQuery("information_schema.STATISTICS AS s").
				WithArgs("srcdb").
				WillReturnRows(sqlmock.NewRows([]string{
					"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE",
				}).AddRow("orders", "id", tc.dataType, tc.columnType))

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

			gotErr := p.ValidateRootPKNumeric(ctx, run)
			switch tc.wantErr {
			case "":
				if gotErr != nil {
					t.Fatalf("root PK stage must pass from the SAME cached fact: %v", gotErr)
				}
			default:
				if gotErr == nil || !strings.Contains(gotErr.Error(), tc.wantErr) {
					t.Fatalf("expected %s from the SAME cached fact, got %v", tc.wantErr, gotErr)
				}
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("the single PrimaryKeys expectation was not observed: %v", err)
			}
		})
	}
}
