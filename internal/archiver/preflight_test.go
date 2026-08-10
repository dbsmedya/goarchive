// Package archiver provides comprehensive tests for the preflight checker.
package archiver

import (
	"context"
	"database/sql"
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

// newSourceOnlyChecker returns a checker with NO database handle.
//
// Preflight stages read p.db in exactly one place — the Inspector newPreflightRun
// builds — and the library answers a nil Querier with a named error ("validations:
// nil Querier") rather than a panic. So once a test preloads the fact its stage
// consumes, no handle is needed at all, and a stage that wrongly issues its own query
// fails as an assertion instead of being absorbed by a mock that answers it.
//
// NewPreflightChecker rejects a nil handle, hence the struct literal. It is confined
// to this helper and newDestChecker so the fixture is reviewed once rather than once
// per test. Tests needing a captured logger or a verification config assign the field
// after construction.
func newSourceOnlyChecker(g *graph.Graph, srcDBName string) *PreflightChecker {
	return &PreflightChecker{
		db:           nil,
		sourceDBName: srcDBName,
		graph:        g,
		logger:       logger.NewDefault(),
	}
}

// newDestChecker returns a checker whose destination handle is a deny-all stub.
//
// Five stages — ValidateDestinationTablesExist, ValidateDestinationSchemaCompatibility,
// ValidateDestinationWritePermissions, ValidateJobSchemaPermissions and
// ValidateDestinationInsertTriggers — guard on p.destinationDB == nil BEFORE they read
// the run, so a nil destination handle can never reach a preloaded destination fact.
// The source handle stays nil.
//
// The sqlmock control handle is deliberately discarded. A caller that cannot reach it
// cannot program an expectation, so reintroducing SQL here requires reintroducing
// sqlmock.New at the call site — visible in review rather than one more line in an
// existing fixture.
func newDestChecker(t *testing.T, g *graph.Graph, srcDBName, dstDBName, jobSchema string) *PreflightChecker {
	t.Helper()
	dstDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = dstDB.Close() })

	return &PreflightChecker{
		db:                nil,
		sourceDBName:      srcDBName,
		destinationDB:     dstDB,
		destinationDBName: dstDBName,
		jobSchemaName:     jobSchema,
		graph:             g,
		logger:            logger.NewDefault(),
	}
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
	g := createPreflightTestGraph()
	checker := newSourceOnlyChecker(g, "testdb")
	inspector := &fakePreflightInspector{tablesResult: []validations.TableInfo{
		{Table: "users", Type: "BASE TABLE", Engine: "InnoDB"},
		{Table: "orders", Type: "BASE TABLE", Engine: "InnoDB"},
	}}
	checker.inspectorFactory = func(validations.Querier, string) preflightInspector {
		return inspector
	}
	ctx := context.Background()

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
	if inspector.tablesCalls != 1 {
		t.Fatalf("Tables calls = %d, want 1", inspector.tablesCalls)
	}
}

// TestPreflightChecker_ValidateRootPKNumeric preloads run.pks directly, once per
// handle: the first proves an integer single-column root PK passes, the second proves
// a non-integer one fails ROOT_PK_TYPE_UNSUPPORTED as a plain error.
func TestPreflightChecker_ValidateRootPKNumeric(t *testing.T) {
	g := graph.NewGraph("users", "id")
	checker := newSourceOnlyChecker(g, "testdb")

	run := newPreflightRun(checker)
	run.pks = []validations.PKInfo{
		{Table: "users", Kind: validations.PKSingle, Columns: []string{"id"},
			DataType: "bigint", IsInteger: true, Unsigned: true},
	}
	run.pksLoaded = true
	if err := checker.ValidateRootPKNumeric(context.Background(), run); err != nil {
		t.Fatalf("ValidateRootPKNumeric: %v", err)
	}

	checker2 := newSourceOnlyChecker(graph.NewGraph("orders", "uuid"), "testdb")
	run2 := newPreflightRun(checker2)
	run2.pks = []validations.PKInfo{
		{Table: "orders", Kind: validations.PKSingle, Columns: []string{"uuid"},
			DataType: "varchar", IsInteger: false},
	}
	run2.pksLoaded = true
	err := checker2.ValidateRootPKNumeric(context.Background(), run2)
	if err == nil || !strings.Contains(err.Error(), "ROOT_PK_TYPE_UNSUPPORTED") {
		t.Fatalf("expected ROOT_PK_TYPE_UNSUPPORTED, got %v", err)
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("ROOT_PK_TYPE_UNSUPPORTED must be a plain error, not *PreflightError: %v", err)
	}
}

func TestRunAllChecks_NonInnoDBTables(t *testing.T) {
	g := createPreflightTestGraph()
	checker := newSourceOnlyChecker(g, "testdb")
	inspector := &fakePreflightInspector{
		tablesResult: []validations.TableInfo{
			{Table: "users", Type: "BASE TABLE", Engine: "InnoDB"},
			{Table: "orders", Type: "BASE TABLE", Engine: "MyISAM"},
			{Table: "order_items", Type: "BASE TABLE", Engine: "InnoDB"},
		},
		columnsResult: []validations.TableColumns{
			{Table: "users", Columns: []validations.ColumnInfo{{Name: "id", DataType: "bigint", Unsigned: true}}},
			{Table: "orders", Columns: []validations.ColumnInfo{{Name: "id", DataType: "bigint"}}},
			{Table: "order_items", Columns: []validations.ColumnInfo{{Name: "id", DataType: "bigint"}}},
		},
		primaryKeysResult: []validations.PKInfo{
			{Table: "users", Kind: validations.PKSingle, Columns: []string{"id"}, DataType: "bigint", IsInteger: true, Unsigned: true},
			{Table: "orders", Kind: validations.PKSingle, Columns: []string{"id"}, DataType: "bigint", IsInteger: true},
			{Table: "order_items", Kind: validations.PKSingle, Columns: []string{"id"}, DataType: "bigint", IsInteger: true},
		},
	}
	checker.inspectorFactory = func(validations.Querier, string) preflightInspector {
		return inspector
	}
	ctx := context.Background()

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
	if inspector.tablesCalls != 1 || inspector.columnsCalls != 1 || inspector.primaryKeysCalls != 1 {
		t.Fatalf("fact calls tables/columns/PKs = %d/%d/%d, want 1/1/1",
			inspector.tablesCalls, inspector.columnsCalls, inspector.primaryKeysCalls)
	}
}

func TestValidatePrimaryKeyColumns_MissingConfiguredPKColumn(t *testing.T) {
	// Minimal graph containing users and orders (phase 018, Step 7b): the table set
	// now comes from run.tables (= graph.AllNodes()), so the preloaded column fact
	// must cover exactly these two tables.
	g := graph.NewGraph("users", "id")
	g.AddNode("orders", &graph.Node{Name: "orders", ForeignKey: "user_id", ReferenceKey: "id", DependencyType: "1-N"})
	g.SetPK("orders", "id")
	g.AddEdge("users", "orders")
	checker := newSourceOnlyChecker(g, "testdb")
	ctx := context.Background()

	// orders has no "id" column at all: its column fact names a different column.
	run := newPreflightRun(checker)
	run.srcColumns = []validations.TableColumns{
		{Table: "users", Columns: []validations.ColumnInfo{{Name: "id", Ordinal: 1, DataType: "bigint"}}},
		{Table: "orders", Columns: []validations.ColumnInfo{{Name: "order_num", Ordinal: 1, DataType: "bigint"}}},
	}
	run.srcColumnsLoaded = true

	err := checker.ValidatePrimaryKeyColumns(ctx, run)
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
	// A configured root plus an unconfigured "legacy" child. NewGraph always
	// configures its root, so the unconfigured table is added with AddNode and no
	// SetPK (phase 018, Step 7b).
	g := graph.NewGraph("users", "id")
	g.AddNode("legacy", &graph.Node{Name: "legacy"})
	checker := newSourceOnlyChecker(g, "testdb")

	// "legacy" is rejected without ever consulting its columns, so the preloaded
	// column fact covers only the configured root — matching what the mock this
	// replaces returned.
	run := newPreflightRun(checker)
	run.srcColumns = []validations.TableColumns{
		{Table: "users", Columns: []validations.ColumnInfo{{Name: "id", Ordinal: 1, DataType: "bigint"}}},
	}
	run.srcColumnsLoaded = true

	err := checker.ValidatePrimaryKeyColumns(context.Background(), run)
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
	g := graph.NewGraph("events", "LOG_ID")
	checker := newSourceOnlyChecker(g, "testdb")
	ctx := context.Background()

	// MySQL's information_schema.COLUMNS collates case-insensitively, so the real
	// column comes back as "log_id" even though the configured key is "LOG_ID".
	run := newPreflightRun(checker)
	run.srcColumns = []validations.TableColumns{
		{Table: "events", Columns: []validations.ColumnInfo{{Name: "log_id", Ordinal: 1, DataType: "bigint"}}},
	}
	run.srcColumnsLoaded = true

	err := checker.ValidatePrimaryKeyColumns(ctx, run)
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
	wantErr := errors.New("query failed")

	g := graph.NewGraph("orders", "id")
	p := newSourceOnlyChecker(g, "srcdb")

	run := newPreflightRun(p)
	run.srcColumnsErr = wantErr
	run.srcColumnsLoaded = true

	err := p.ValidatePrimaryKeyColumns(context.Background(), run)
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
// The checker has no database handle at all. A stage that ignored `run` and built its
// own Inspector would issue a query against a nil Querier and get back a plain
// "validations: nil Querier" error — failing the *PreflightError assertion below. The
// verdict discriminates, not any query bookkeeping.
func TestValidatePrimaryKeyColumnsConsumesTheCachedFact(t *testing.T) {
	g := graph.NewGraph("orders", "id")
	p := newSourceOnlyChecker(g, "srcdb")

	run := newPreflightRun(p)
	run.srcColumns = []validations.TableColumns{
		{Table: "orders", Columns: []validations.ColumnInfo{{Name: "note", Ordinal: 1, DataType: "varchar"}}},
	}
	run.srcColumnsLoaded = true

	err := p.ValidatePrimaryKeyColumns(context.Background(), run)

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
	g := createPreflightTestGraph()
	checker := newSourceOnlyChecker(g, "testdb")
	ctx := context.Background()

	// The table list comes from the run, which reads it from the graph.
	// createPreflightTestGraph's nodes are users, orders and order_items.
	run := newPreflightRun(checker)
	run.srcTables = []validations.TableInfo{
		{Table: "users", Type: "BASE TABLE", Engine: "InnoDB"},
		{Table: "orders", Type: "BASE TABLE", Engine: "InnoDB"},
		{Table: "order_items", Type: "BASE TABLE", Engine: "InnoDB"},
	}
	run.srcTablesLoaded = true

	err := checker.ValidateTablesExist(ctx, run)

	if err != nil {
		t.Fatalf("ValidateTablesExist failed: %v", err)
	}
}

// TestValidateTablesExist_MissingTables — REFERENCE SHAPE 1: a source-side fact,
// preloaded, no database handle.
//
// That a requested name the server never reports produces a finding is a LIBRARY
// verdict; dbsgomysql's TestCheckTablesExist covers it across four cases. What is
// goarchive's own is asserted here: the check ID stamped on the error, and the
// flattening of Finding.Tables into PreflightError.Tables by findingsToPreflightError.
// This is one of only two caller-level tests of that mapper.
func TestValidateTablesExist_MissingTables(t *testing.T) {
	// "nonexistent" is a graph node so that the run asks about it; the preloaded
	// fact omits it, so it must be the one missing table.
	g := createPreflightTestGraph()
	g.AddNode("nonexistent", &graph.Node{Name: "nonexistent", ForeignKey: "order_id", ReferenceKey: "id", DependencyType: "1-N"})
	g.SetPK("nonexistent", "id")
	g.AddEdge("orders", "nonexistent")
	checker := newSourceOnlyChecker(g, "testdb")

	run := newPreflightRun(checker)
	run.srcTables = []validations.TableInfo{
		{Table: "users", Type: "BASE TABLE", Engine: "InnoDB"},
		{Table: "orders", Type: "BASE TABLE", Engine: "InnoDB"},
		{Table: "order_items", Type: "BASE TABLE", Engine: "InnoDB"},
	}
	run.srcTablesLoaded = true

	err := checker.ValidateTablesExist(context.Background(), run)

	preflightErr, ok := err.(*PreflightError)
	if !ok {
		t.Fatalf("expected *PreflightError, got %T: %v", err, err)
	}
	if preflightErr.Check != "TABLE_EXISTENCE_CHECK" {
		t.Errorf("Check = %q, want TABLE_EXISTENCE_CHECK", preflightErr.Check)
	}
	if len(preflightErr.Tables) != 1 || preflightErr.Tables[0] != "nonexistent" {
		t.Errorf("Tables = %v, want [nonexistent]", preflightErr.Tables)
	}
}

func TestValidateTablesExist_ExactCaseRequired(t *testing.T) {
	// The graph asks for "Users"; the preloaded fact reports "users". A
	// case-insensitive match would wrongly pass.
	g := graph.NewGraph("Users", "id")
	checker := newSourceOnlyChecker(g, "testdb")
	ctx := context.Background()

	run := newPreflightRun(checker)
	run.srcTables = []validations.TableInfo{
		{Table: "users", Type: "BASE TABLE", Engine: "InnoDB"},
	}
	run.srcTablesLoaded = true

	err := checker.ValidateTablesExist(ctx, run)
	if err == nil {
		t.Fatal("expected case-sensitive table mismatch error")
	}

	// Bare "err != nil" cannot tell a policy verdict from a stray-query error;
	// the check ID confirms which one this is.
	preflightErr, ok := err.(*PreflightError)
	if !ok {
		t.Fatalf("expected *PreflightError, got %T: %v", err, err)
	}
	if preflightErr.Check != "TABLE_EXISTENCE_CHECK" {
		t.Errorf("Check = %q, want TABLE_EXISTENCE_CHECK", preflightErr.Check)
	}
}

func TestValidateTablesExist_QueryError(t *testing.T) {
	g := createPreflightTestGraph()
	checker := newSourceOnlyChecker(g, "testdb")
	ctx := context.Background()

	// The sentinel is required: this test's own assertions are only "some plain
	// error, and not a *PreflightError" — without errors.Is a stray query, which
	// also yields a plain error, would be indistinguishable from this injected one.
	errInspect := errors.New("query failed")
	run := newPreflightRun(checker)
	run.srcTablesErr = errInspect
	run.srcTablesLoaded = true

	err := checker.ValidateTablesExist(ctx, run)

	if !errors.Is(err, errInspect) {
		t.Fatalf("expected the memoized load error to propagate, got: %v", err)
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
	g := createPreflightTestGraph()
	checker := newSourceOnlyChecker(g, "testdb")
	ctx := context.Background()

	run := newPreflightRun(checker)
	run.srcTables = []validations.TableInfo{
		{Table: "users", Type: "BASE TABLE", Engine: "InnoDB"},
		{Table: "orders", Type: "BASE TABLE", Engine: "InnoDB"},
	}
	run.srcTablesLoaded = true

	err := checker.ValidateStorageEngine(ctx, run)

	if err != nil {
		t.Fatalf("ValidateStorageEngine failed: %v", err)
	}
}

func TestValidateStorageEngine_NonInnoDB(t *testing.T) {
	g := createPreflightTestGraph()
	checker := newSourceOnlyChecker(g, "testdb")
	ctx := context.Background()

	run := newPreflightRun(checker)
	run.srcTables = []validations.TableInfo{
		{Table: "users", Type: "BASE TABLE", Engine: "InnoDB"},
		{Table: "orders", Type: "BASE TABLE", Engine: "MyISAM"},
		{Table: "order_items", Type: "BASE TABLE", Engine: "MEMORY"},
	}
	run.srcTablesLoaded = true

	err := checker.ValidateStorageEngine(ctx, run)

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

// TestValidateForeignKeyIndexes_Unindexed provokes Indexed:false directly. In
// production the primary INNODB_FOREIGN path always sets Indexed:true by construction,
// and reaching false requires the library's internal fallback chain (primary fails,
// KEY_COLUMN_USAGE succeeds, the STATISTICS supporting-index query returns no rows for
// the child table). That whole chain collapses into ONE preloaded run.fkOut carrying a
// single ForeignKey with Indexed explicitly false — CheckFKIndexed reads only that
// field, so the fallback machinery that produces it is out of scope here.
func TestValidateForeignKeyIndexes_Unindexed(t *testing.T) {
	g := createPreflightTestGraph()
	checker := newSourceOnlyChecker(g, "testdb")
	ctx := context.Background()

	run := newPreflightRun(checker)
	run.fkOut = validations.ForeignKeyResult{Keys: []validations.ForeignKey{
		{ConstraintName: "fk_orders_users", ChildSchema: "testdb", ChildTable: "orders",
			ChildColumns: []string{"user_id"}, ParentSchema: "testdb", ParentTable: "users",
			ParentColumns: []string{"id"}, OnDelete: "RESTRICT", OnUpdate: "RESTRICT", Indexed: false},
	}}
	run.fkOutLoaded = true

	err := checker.ValidateForeignKeyIndexes(ctx, run)

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
}

// ============================================================================
// ValidateTriggers Tests
// ============================================================================

func TestValidateTriggers_NoTriggers(t *testing.T) {
	g := createPreflightTestGraph()
	checker := newSourceOnlyChecker(g, "testdb")
	ctx := context.Background()

	run := newPreflightRun(checker)
	run.srcDelTriggers = []validations.TriggerInfo{}
	run.srcDelTriggersLoaded = true

	if err := checker.ValidateTriggers(ctx, run, false); err != nil {
		t.Fatalf("ValidateTriggers failed: %v", err)
	}
}

func TestValidateTriggers_WithTriggers(t *testing.T) {
	g := createPreflightTestGraph()
	checker := newSourceOnlyChecker(g, "testdb")
	ctx := context.Background()

	run := newPreflightRun(checker)
	run.srcDelTriggers = []validations.TriggerInfo{
		{Table: "users", Name: "trg_users_delete", Event: "DELETE", Timing: "AFTER"},
		{Table: "orders", Name: "trg_orders_delete", Event: "DELETE", Timing: "AFTER"},
	}
	run.srcDelTriggersLoaded = true

	err := checker.ValidateTriggers(ctx, run, false)

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
	g := createPreflightTestGraph()
	checker := newSourceOnlyChecker(g, "testdb")
	ctx := context.Background()

	run := newPreflightRun(checker)
	run.srcDelTriggers = []validations.TriggerInfo{
		{Table: "orders", Name: "trg_del", Event: "DELETE", Timing: "AFTER"},
	}
	run.srcDelTriggersLoaded = true

	// The fact above DOES contain a trigger; only --force-triggers turns this into a pass.
	if err := checker.ValidateTriggers(ctx, run, true); err != nil {
		t.Fatalf("--force-triggers must downgrade to a warning, got: %v", err)
	}
}

// ============================================================================
// WarnCascadeRules Tests
// ============================================================================

// TestWarnCascadeRules_WithCascade — REFERENCE SHAPE 2: a test that never programmed
// SQL to begin with. The handle and the mock.ExpectationsWereMet() call were both
// dead: with zero expectations that method returns nil unconditionally
// (sqlmock.go:187), so it asserted nothing. What proves no query was issued is the
// preloaded fact plus the specific assertion below — a stage that refetched would get
// "validations: nil Querier" and fail it.
func TestWarnCascadeRules_WithCascade(t *testing.T) {
	g := createPreflightTestGraph()
	core, recorded := observer.New(zapcore.DebugLevel)
	checker := newSourceOnlyChecker(g, "testdb")
	checker.logger = &logger.Logger{SugaredLogger: zap.New(core).Sugar()}
	ctx := context.Background()

	run := newPreflightRun(checker)
	run.fkInLoaded, run.fkOutLoaded = true, true
	run.fkIn = validations.ForeignKeyResult{Keys: []validations.ForeignKey{
		{ConstraintName: "fk_orders_users", ChildSchema: "testdb", ChildTable: "orders",
			ChildColumns: []string{"user_id"}, ParentSchema: "testdb", ParentTable: "users",
			ParentColumns: []string{"id"}, OnDelete: "CASCADE"},
	}}
	run.fkOut = validations.ForeignKeyResult{Keys: []validations.ForeignKey{
		{ConstraintName: "fk_items_orders", ChildSchema: "testdb", ChildTable: "order_items",
			ChildColumns: []string{"order_id"}, ParentSchema: "testdb", ParentTable: "orders",
			ParentColumns: []string{"id"}, OnDelete: "CASCADE"},
	}}

	// Should not error, just warn
	err := checker.WarnCascadeRules(ctx, run)

	if err != nil {
		t.Fatalf("WarnCascadeRules should not error: %v", err)
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
	g := createPreflightTestGraph()
	core, recorded := observer.New(zapcore.DebugLevel)
	checker := newSourceOnlyChecker(g, "testdb")
	checker.logger = &logger.Logger{SugaredLogger: zap.New(core).Sugar()}
	ctx := context.Background()

	run := newPreflightRun(checker)
	run.fkIn = validations.ForeignKeyResult{Keys: []validations.ForeignKey{
		{ConstraintName: "fk_orders_users", ChildSchema: "testdb", ChildTable: "orders",
			ChildColumns: []string{"user_id"}, ParentSchema: "testdb", ParentTable: "users",
			ParentColumns: []string{"id"}, OnDelete: "RESTRICT"},
	}}
	run.fkOut = validations.ForeignKeyResult{Keys: []validations.ForeignKey{
		{ConstraintName: "fk_items_orders", ChildSchema: "testdb", ChildTable: "order_items",
			ChildColumns: []string{"order_id"}, ParentSchema: "testdb", ParentTable: "orders",
			ParentColumns: []string{"id"}, OnDelete: "RESTRICT"},
	}}
	run.fkInLoaded, run.fkOutLoaded = true, true

	err := checker.WarnCascadeRules(ctx, run)

	if err != nil {
		t.Fatalf("WarnCascadeRules failed: %v", err)
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
		run  func(r *preflightRun)
	}{
		{"incoming", func(r *preflightRun) {
			r.fkInErr, r.fkInLoaded = wantErr, true
		}},
		{"outgoing", func(r *preflightRun) {
			r.fkIn, r.fkInLoaded = validations.ForeignKeyResult{}, true
			r.fkOutErr, r.fkOutLoaded = wantErr, true
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := createPreflightTestGraph()
			checker := newSourceOnlyChecker(g, "testdb")

			run := newPreflightRun(checker)
			tc.run(run)

			err := checker.WarnCascadeRules(context.Background(), run)
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
	checker := newSourceOnlyChecker(g, "testdb")
	checker.logger = &logger.Logger{SugaredLogger: zap.New(core).Sugar()}

	run := newPreflightRun(checker)
	run.fkIn = validations.ForeignKeyResult{Keys: []validations.ForeignKey{zebra, middle}}
	run.fkOut = validations.ForeignKeyResult{Keys: []validations.ForeignKey{middle, alpha}}
	run.fkInLoaded, run.fkOutLoaded = true, true

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

func TestConfigureDestination_RejectsInvalidConfiguration(t *testing.T) {
	// ConfigureDestination only validates and stores the handle; these rejection
	// branches must not open sqlmock databases or increase the consumer-policy budget.
	destDB := new(sql.DB)

	tests := []struct {
		name              string
		db                *sql.DB
		destinationDBName string
		jobSchema         string
		want              string
	}{
		{
			name:              "nil destination database",
			destinationDBName: "destdb",
			jobSchema:         "jobschemadb",
			want:              "destination database is nil",
		},
		{
			name:      "empty destination database name",
			db:        destDB,
			jobSchema: "jobschemadb",
			want:      "destination database name is required",
		},
		{
			name:              "empty job schema",
			db:                destDB,
			destinationDBName: "destdb",
			want:              "job schema is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checker := &PreflightChecker{}
			err := checker.ConfigureDestination(tt.db, tt.destinationDBName, tt.jobSchema)
			if err == nil {
				t.Fatalf("ConfigureDestination returned nil, want %q", tt.want)
			}
			if err.Error() != tt.want {
				t.Fatalf("ConfigureDestination error = %q, want %q", err, tt.want)
			}
			if checker.destinationDB != nil || checker.destinationDBName != "" || checker.jobSchemaName != "" {
				t.Fatalf("rejected configuration mutated checker: db=%v destination=%q job_schema=%q",
					checker.destinationDB, checker.destinationDBName, checker.jobSchemaName)
			}
		})
	}
}

// TestValidateDestinationTablesExist_MissingTables — REFERENCE SHAPE 3: a
// destination-side fact. ValidateDestinationTablesExist guards on
// p.destinationDB == nil before it reads the run, so this one keeps a handle — a
// deny-all stub from newDestChecker, whose control handle is discarded.
//
// This is the mapper's second and last caller-level test, so it asserts the check ID
// and the flattened list rather than merely that an error came back.
func TestValidateDestinationTablesExist_MissingTables(t *testing.T) {
	g := createPreflightTestGraph()
	checker := newDestChecker(t, g, "sourcedb", "destdb", "destdb")

	// The table list comes from the run, which reads it from the graph:
	// createPreflightTestGraph's nodes are users, orders and order_items. Only
	// "users" is reported back, so orders and order_items are missing.
	run := newPreflightRun(checker)
	run.dstTables = []validations.TableInfo{
		{Table: "users", Type: "BASE TABLE", Engine: "InnoDB"},
	}
	run.dstTablesLoaded = true

	err := checker.ValidateDestinationTablesExist(context.Background(), run)

	preflightErr, ok := err.(*PreflightError)
	if !ok {
		t.Fatalf("expected *PreflightError, got %T: %v", err, err)
	}
	if preflightErr.Check != "DEST_TABLE_EXISTENCE_CHECK" {
		t.Errorf("Check = %q, want DEST_TABLE_EXISTENCE_CHECK", preflightErr.Check)
	}
	// Tables is fed by Graph.AllNodes(), which ranges over a map with no sort
	// (internal/graph/types.go:133), so its order is not stable across runs — measured
	// flaky ~8-10% of the time as a fixed-index check, which is why a single passing run
	// proves nothing here. Length + set membership is the order-independent equivalent.
	if len(preflightErr.Tables) != 2 {
		t.Fatalf("Tables = %v, want length 2", preflightErr.Tables)
	}
	got := map[string]bool{}
	for _, tbl := range preflightErr.Tables {
		got[tbl] = true
	}
	for _, want := range []string{"orders", "order_items"} {
		if !got[want] {
			t.Errorf("Tables %v does not contain %q", preflightErr.Tables, want)
		}
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
// calls the stage. tableSpecs is the fact-acquisition boundary, so a mismatch case
// exercises evaluateSchemaCompatibility through the real stage without touching a
// database at all.
//
// The source handle is nil and the destination handle is newDestChecker's deny-all stub,
// which ValidateDestinationSchemaCompatibility requires only because it guards on
// p.destinationDB == nil before reading the run. A stage that ignored the preloaded specs
// would get "validations: nil Querier" on the source side and fail its caller's
// assertion — which is what proves no query was issued. The two
// mock.ExpectationsWereMet() calls this helper used to make could not prove that: with
// zero expectations programmed, that method returns nil unconditionally (sqlmock.go:187).
func runPreloadedSchemaCompatibilityCheck(t *testing.T, sourceCols, destCols []validations.ColumnSpec) error {
	t.Helper()
	g := createPreflightTestGraph()
	checker := newDestChecker(t, g, "sourcedb", "destdb", "destdb")

	run := newPreflightRun(checker)
	run.specs = []specPair{preloadedSchemaSpecPair(sourceCols, destCols)}
	run.specsLoaded = true

	return checker.ValidateDestinationSchemaCompatibility(context.Background(), run)
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
	wantErr := errors.New("table spec fetch failed")
	g := createPreflightTestGraph()
	checker := newDestChecker(t, g, "sourcedb", "destdb", "destdb")

	run := newPreflightRun(checker)
	run.specsErr = wantErr
	run.specsLoaded = true

	err := checker.ValidateDestinationSchemaCompatibility(context.Background(), run)
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
}

// TestValidateDestinationSchemaCompatibilityConsumesCachedFact — contract 3. The stage
// must consume the run's cached specs rather than issuing its own queries: zero sqlmock
// expectations are registered on either pool, and the preloaded pair is deliberately
// incompatible so the stage's own verdict, not an empty result, is what proves it ran.
func TestValidateDestinationSchemaCompatibilityConsumesCachedFact(t *testing.T) {
	g := createPreflightTestGraph()
	checker := newDestChecker(t, g, "sourcedb", "destdb", "destdb")

	sourceCols := []validations.ColumnSpec{specColT(1, "id", "bigint"), specColT(2, "name", "varchar(255)")}
	destCols := []validations.ColumnSpec{specColT(1, "id", "bigint"), specColT(2, "name", "varchar(100)")} // mismatch
	run := newPreflightRun(checker)
	run.specs = []specPair{preloadedSchemaSpecPair(sourceCols, destCols)}
	run.specsLoaded = true

	err := checker.ValidateDestinationSchemaCompatibility(context.Background(), run)
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
}

// TestValidateDestinationSchemaCompatibilityAbortsOnInspectionIntegrityFailure proves the
// stage propagates evaluateSchemaCompatibility's OWN error, not just its verdict: an
// asymmetric index-capture (one side's TableSpec never captured indexes — the shape
// tableSpecs is supposed to prevent) must abort preflight rather than silently pass. This
// is the routed-through-the-stage counterpart to
// TestSchemaCompatUncapturedIndexesAbortBeforeDiffSpecs, and is what actually catches a
// stage that swallows evaluateSchemaCompatibility's error (`verdict, _ := evaluate...`).
func TestValidateDestinationSchemaCompatibilityAbortsOnInspectionIntegrityFailure(t *testing.T) {
	g := createPreflightTestGraph()
	checker := newDestChecker(t, g, "sourcedb", "destdb", "destdb")

	pair := preloadedSchemaSpecPair(
		[]validations.ColumnSpec{specColT(1, "id", "bigint")},
		[]validations.ColumnSpec{specColT(1, "id", "bigint")},
	)
	pair.B.Captured = 0 // destination side never captured indexes

	run := newPreflightRun(checker)
	run.specs = []specPair{pair}
	run.specsLoaded = true

	err := checker.ValidateDestinationSchemaCompatibility(context.Background(), run)
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
	wantErr := errors.New("grants failed")
	g := graph.NewGraph("orders", "id")
	p := newDestChecker(t, g, "sourcedb", "destdb", "destdb")

	run := newPreflightRun(p)
	run.dstGrantsErr = wantErr
	run.dstGrantsLoaded = true

	err := p.ValidateDestinationWritePermissions(context.Background(), run)
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
	g := graph.NewGraph("orders", "id")
	g.AddNode("lines", nil)
	p := newDestChecker(t, g, "sourcedb", "destdb", "destdb")

	run := newPreflightRun(p)
	run.dstGrants = validations.Grants{}
	run.dstGrantsLoaded = true

	err := p.ValidateDestinationWritePermissions(context.Background(), run)
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
}

// TestValidateSourceDeletePermissionsInspectionErrorIsPlain proves an inspection
// failure (sourceGrants returning an error) surfaces as a plain error carrying
// goarchive's own wrapper text, never as a *PreflightError. Mirrors
// TestValidateDestinationWritePermissionsInspectionErrorIsPlain; only the stage word,
// the validator, and the source-side checker fields differ.
func TestValidateSourceDeletePermissionsInspectionErrorIsPlain(t *testing.T) {
	wantErr := errors.New("grants failed")
	g := graph.NewGraph("orders", "id")
	p := newSourceOnlyChecker(g, "sourcedb")

	run := newPreflightRun(p)
	run.srcGrantsErr = wantErr
	run.srcGrantsLoaded = true

	err := p.ValidateSourceDeletePermissions(context.Background(), run)
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
	g := graph.NewGraph("orders", "id")
	g.AddNode("lines", nil)
	p := newSourceOnlyChecker(g, "sourcedb")

	run := newPreflightRun(p)
	run.srcGrants = validations.Grants{}
	run.srcGrantsLoaded = true

	err := p.ValidateSourceDeletePermissions(context.Background(), run)
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
}

// TestValidateSourceSelectPermissionsInspectionErrorIsPlain proves an inspection
// failure (sourceGrants returning an error) surfaces as a plain error carrying
// goarchive's own wrapper text, never as a *PreflightError. Mirrors
// TestValidateSourceDeletePermissionsInspectionErrorIsPlain; only the stage word and
// the validator differ.
func TestValidateSourceSelectPermissionsInspectionErrorIsPlain(t *testing.T) {
	wantErr := errors.New("grants failed")
	g := graph.NewGraph("orders", "id")
	p := newSourceOnlyChecker(g, "sourcedb")

	run := newPreflightRun(p)
	run.srcGrantsErr = wantErr
	run.srcGrantsLoaded = true

	err := p.ValidateSourceSelectPermissions(context.Background(), run)
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
	g := graph.NewGraph("orders", "id")
	g.AddNode("lines", nil)
	p := newSourceOnlyChecker(g, "sourcedb")

	run := newPreflightRun(p)
	run.srcGrants = validations.Grants{}
	run.srcGrantsLoaded = true

	err := p.ValidateSourceSelectPermissions(context.Background(), run)
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
}

// TestValidateForeignKeyIndexesInspectionErrorIsPlain — contract 2. Preload fkOutErr /
// fkOutLoaded so no query is issued, then assert the returned error wraps the cause,
// carries goarchive's own wrapper text, and is NOT a *PreflightError. Mirrors
// TestValidateSourceSelectPermissionsInspectionErrorIsPlain; only the stage word and the
// validator differ.
func TestValidateForeignKeyIndexesInspectionErrorIsPlain(t *testing.T) {
	wantErr := errors.New("fk fetch failed")
	g := graph.NewGraph("orders", "id")
	p := newSourceOnlyChecker(g, "sourcedb")

	run := newPreflightRun(p)
	run.fkOutErr = wantErr
	run.fkOutLoaded = true

	err := p.ValidateForeignKeyIndexes(context.Background(), run)
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
	g := graph.NewGraph("order_lines", "id")
	p := newSourceOnlyChecker(g, "sourcedb")

	run := newPreflightRun(p)
	run.fkOut = validations.ForeignKeyResult{
		Keys: []validations.ForeignKey{{
			ConstraintName: "fk_ol_o", ChildSchema: "sourcedb", ChildTable: "order_lines",
			ChildColumns: []string{"order_id"}, ParentTable: "orders", Indexed: false,
		}},
	}
	run.fkOutLoaded = true

	err := p.ValidateForeignKeyIndexes(context.Background(), run)
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
}

func TestValidateDestinationInsertTriggers_WithTriggers(t *testing.T) {
	g := createPreflightTestGraph()
	checker := newDestChecker(t, g, "sourcedb", "destdb", "destdb")

	run := newPreflightRun(checker)
	run.dstInsTriggers = []validations.TriggerInfo{
		{Table: "users", Name: "trg_users_insert", Event: "INSERT", Timing: "BEFORE"},
	}
	run.dstInsTriggersLoaded = true

	err := checker.ValidateDestinationInsertTriggers(context.Background(), run)
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
	g := createPreflightTestGraph()
	checker := newSourceOnlyChecker(g, "testdb")

	run := newPreflightRun(checker)
	run.fkIn = validations.ForeignKeyResult{
		Keys: []validations.ForeignKey{
			{ConstraintName: "fk_ext_orders_1", ChildSchema: "testdb", ChildTable: "external_cascade",
				ParentSchema: "testdb", ParentTable: "orders", OnDelete: "CASCADE"},
			{ConstraintName: "fk_ext_orders_2", ChildSchema: "testdb", ChildTable: "external_restrict",
				ParentSchema: "testdb", ParentTable: "orders", OnDelete: "RESTRICT"},
		},
		Visibility: validations.VisibilityComplete,
	}
	run.fkInLoaded = true

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
}

// TestValidateForeignKeyCoverage_CrossSchemaSameNameChild proves an out-of-graph
// child in ANOTHER schema that happens to share a name with a graph table
// (otherdb.orders vs in-graph orders) is still flagged: membership must be
// schema-aware, not bare-name. Preloads the run's fkIn fact directly, same as above.
func TestValidateForeignKeyCoverage_CrossSchemaSameNameChild(t *testing.T) {
	g := createPreflightTestGraph() // graph = {users, orders, order_items} in "testdb"
	checker := newSourceOnlyChecker(g, "testdb")
	ctx := context.Background()

	// otherdb.orders (out-of-graph) references in-graph testdb.users.
	run := newPreflightRun(checker)
	run.fkIn = validations.ForeignKeyResult{
		Keys: []validations.ForeignKey{
			{ConstraintName: "fk_other_users", ChildSchema: "otherdb", ChildTable: "orders",
				ParentSchema: "testdb", ParentTable: "users", OnDelete: "CASCADE"},
		},
		Visibility: validations.VisibilityComplete,
	}
	run.fkInLoaded = true

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
}

// ============================================================================
// ValidateForeignKeyMetadataVisibility Tests
// ============================================================================

// TestValidateForeignKeyMetadataVisibilityInspectionErrorIsPlain — contract 2. A
// memoized fkIncoming error must surface as a PLAIN error carrying goarchive's
// "fk_metadata_visibility" inspection wrapper, never a *PreflightError.
func TestValidateForeignKeyMetadataVisibilityInspectionErrorIsPlain(t *testing.T) {
	wantErr := errors.New("fk fetch failed")
	g := createPreflightTestGraph()
	checker := newSourceOnlyChecker(g, "testdb")

	run := newPreflightRun(checker)
	run.fkInErr = wantErr
	run.fkInLoaded = true

	err := checker.ValidateForeignKeyMetadataVisibility(context.Background(), run)
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
}

// TestValidateForeignKeyMetadataVisibilityConsumesTheCachedFact — contract 3, and the
// Step 6 rewrite of the deleted TestValidateForeignKeyMetadataVisibility_NoGlobalSelect:
// deviation D2 replaced the 1.8 global-SELECT mechanism that test exercised through
// sqlmock rows with the library's typed completeness proof. Preloading fkIn/fkInLoaded
// with VisibilityUnconfirmed and a non-empty tables slice (CheckFKClosure returns nil,
// vacuously, for an empty target) and registering ZERO sqlmock expectations proves the
// stage judges the cached fact, not a fresh query.
func TestValidateForeignKeyMetadataVisibilityConsumesTheCachedFact(t *testing.T) {
	g := createPreflightTestGraph()
	checker := newSourceOnlyChecker(g, "testdb")

	run := newPreflightRun(checker)
	run.fkIn = validations.ForeignKeyResult{Visibility: validations.VisibilityUnconfirmed}
	run.fkInLoaded = true

	err := checker.ValidateForeignKeyMetadataVisibility(context.Background(), run)
	if err == nil {
		t.Fatal("expected FK_COVERAGE_VISIBILITY_CHECK when visibility is unconfirmed, got nil")
	}
	if pfErr, ok := err.(*PreflightError); !ok || pfErr.Check != "FK_COVERAGE_VISIBILITY_CHECK" {
		t.Fatalf("expected FK_COVERAGE_VISIBILITY_CHECK, got: %v", err)
	}
}

func TestValidateForeignKeyMetadataVisibilityDowngradeDiagnostics(t *testing.T) {
	primaryErr := errors.New("sentinel primary registry failure: private server detail")
	tests := []struct {
		name      string
		reason    validations.ForeignKeyDowngradeReason
		want      string
		forbidden string
	}{
		{
			name:   "primary query error",
			reason: validations.ForeignKeyDowngradePrimaryQueryError,
			want:   "privileges, connectivity, server state, or another query error",
		},
		{
			name:      "primary read error",
			reason:    validations.ForeignKeyDowngradePrimaryReadError,
			want:      "rows could not be read or decoded",
			forbidden: "PROCESS",
		},
		{
			name:      "missing downgrade reason",
			reason:    validations.ForeignKeyDowngradeNone,
			want:      "reported reason: none",
			forbidden: "GRANT PROCESS",
		},
		{
			name:      "unknown downgrade reason",
			reason:    validations.ForeignKeyDowngradeReason(255),
			want:      "ForeignKeyDowngradeReason(255)",
			forbidden: "GRANT PROCESS",
		},
	}

	seenMessages := make(map[string]string, len(tests))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checker := newSourceOnlyChecker(createPreflightTestGraph(), "testdb")
			core, recorded := observer.New(zapcore.DebugLevel)
			checker.logger = &logger.Logger{SugaredLogger: zap.New(core).Sugar()}

			run := newPreflightRun(checker)
			run.fkIn = validations.ForeignKeyResult{
				Visibility:      validations.VisibilityUnconfirmed,
				DowngradeReason: tt.reason,
				PrimaryError:    primaryErr,
			}
			run.fkInLoaded = true

			err := checker.ValidateForeignKeyMetadataVisibility(context.Background(), run)
			preflightErr, ok := err.(*PreflightError)
			if !ok {
				t.Fatalf("error = %T %v, want *PreflightError", err, err)
			}
			if preflightErr.Check != "FK_COVERAGE_VISIBILITY_CHECK" {
				t.Fatalf("Check = %q, want FK_COVERAGE_VISIBILITY_CHECK", preflightErr.Check)
			}
			if !strings.Contains(preflightErr.Message, tt.want) {
				t.Fatalf("message %q does not contain %q", preflightErr.Message, tt.want)
			}
			if tt.forbidden != "" && strings.Contains(preflightErr.Message, tt.forbidden) {
				t.Fatalf("message %q unexpectedly contains %q", preflightErr.Message, tt.forbidden)
			}
			if strings.Contains(preflightErr.Message, primaryErr.Error()) {
				t.Fatalf("operator error leaked raw primary error: %q", preflightErr.Message)
			}

			errorLogs := recorded.FilterLevelExact(zapcore.ErrorLevel).All()
			if len(errorLogs) != 1 {
				t.Fatalf("error-level logs = %d, want exactly 1: %v", len(errorLogs), errorLogs)
			}
			if !strings.Contains(errorLogs[0].Message, primaryErr.Error()) {
				t.Fatalf("error log %q does not retain primary error %q",
					errorLogs[0].Message, primaryErr)
			}

			if previous, duplicate := seenMessages[preflightErr.Message]; duplicate {
				t.Fatalf("diagnostic duplicates %q branch: %q", previous, preflightErr.Message)
			}
			seenMessages[preflightErr.Message] = tt.name
		})
	}
}

// TestValidateForeignKeyMetadataVisibilityPassesOnVisibilityComplete is Step 6's
// positive counterpart: VisibilityComplete preloaded, zero sqlmock expectations.
// The pass/fail err check alone cannot distinguish "the PASSED branch ran" from any
// other code path that happens to return nil, so this also asserts the specific debug
// log line that only that branch emits — the success-case analogue of reference shape
// 3's check-ID strengthening (there is no *PreflightError to inspect on the nil path).
func TestValidateForeignKeyMetadataVisibilityPassesOnVisibilityComplete(t *testing.T) {
	g := createPreflightTestGraph()
	checker := newSourceOnlyChecker(g, "testdb")
	core, recorded := observer.New(zapcore.DebugLevel)
	checker.logger = &logger.Logger{SugaredLogger: zap.New(core).Sugar()}

	run := newPreflightRun(checker)
	// The deliberately inconsistent downgrade reason proves findings/visibility remain
	// the verdict authority. A reason can select failure wording, but cannot manufacture
	// a failure when CheckFKClosure reports no visibility finding.
	run.fkIn = validations.ForeignKeyResult{
		Visibility:      validations.VisibilityComplete,
		DowngradeReason: validations.ForeignKeyDowngradePrimaryQueryError,
	}
	run.fkInLoaded = true

	if err := checker.ValidateForeignKeyMetadataVisibility(context.Background(), run); err != nil {
		t.Fatalf("expected pass with VisibilityComplete, got: %v", err)
	}

	const want = "FK metadata visibility check PASSED (InnoDB metadata registry readable)"
	var found bool
	for _, entry := range recorded.FilterLevelExact(zapcore.DebugLevel).All() {
		if entry.Message == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("want debug log %q; logs were: %v", want, recorded.All())
	}
}

// TestValidateForeignKeyCoverageInspectionErrorIsPlain — contract 2. A memoized
// fkIncoming error must surface as a PLAIN error carrying goarchive's "fk_coverage"
// inspection wrapper, never a *PreflightError.
func TestValidateForeignKeyCoverageInspectionErrorIsPlain(t *testing.T) {
	wantErr := errors.New("fk fetch failed")
	g := createPreflightTestGraph()
	checker := newSourceOnlyChecker(g, "testdb")
	run := newPreflightRun(checker)
	run.fkInErr, run.fkInLoaded = wantErr, true

	err := checker.ValidateForeignKeyCoverage(context.Background(), run)
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
}

// TestValidateForeignKeyCoverageConsumesTheCachedFact — contract 3. Preload fkIn with
// ONE external-child key and fkInLoaded, registering NO sqlmock expectation: a stage
// that re-queried would get no match, return a plain error, and fail the
// *PreflightError assertion below.
func TestValidateForeignKeyCoverageConsumesTheCachedFact(t *testing.T) {
	g := createPreflightTestGraph()
	checker := newSourceOnlyChecker(g, "testdb")
	run := newPreflightRun(checker)
	run.fkIn = validations.ForeignKeyResult{
		Keys: []validations.ForeignKey{
			{ConstraintName: "fk_ext_orders", ChildSchema: "testdb", ChildTable: "external_child",
				ParentSchema: "testdb", ParentTable: "orders", OnDelete: "RESTRICT"},
		},
		Visibility: validations.VisibilityComplete,
	}
	run.fkInLoaded = true

	err := checker.ValidateForeignKeyCoverage(context.Background(), run)
	if err == nil {
		t.Fatal("expected FK_COVERAGE_CHECK for the uncovered external child, got nil")
	}
	if pfErr, ok := err.(*PreflightError); !ok || pfErr.Check != "FK_COVERAGE_CHECK" {
		t.Fatalf("expected FK_COVERAGE_CHECK, got: %v", err)
	}
}

// ============================================================================
// ValidateInternalFKCoverage Tests
// ============================================================================

// TestValidateInternalFKCoverage_FlatConfigMissingNesting is also the explicit
// contract-3 proof: the preloaded fact plus ZERO registered sqlmock expectations
// proves the stage judges the cached fkWithin fact rather than issuing its own query.
func TestValidateInternalFKCoverage_FlatConfigMissingNesting(t *testing.T) {
	// Graph: orders -> order_items, orders -> item_shipments (flat, both siblings)
	// But DB has: item_shipments.shipment_item_id -> order_items.item_id (nested FK)
	g := graph.NewGraph("orders", "order_id")
	g.AddNode("order_items", &graph.Node{Name: "order_items", ForeignKey: "order_id", ReferenceKey: "order_id", DependencyType: "1-N"})
	g.AddNode("item_shipments", &graph.Node{Name: "item_shipments", ForeignKey: "order_id", ReferenceKey: "order_id", DependencyType: "1-N"})
	g.SetPK("order_items", "item_id")
	g.SetPK("item_shipments", "shipment_id")
	g.AddEdgeWithMeta("orders", "order_items", "order_id", "order_id", "1-N")
	g.AddEdgeWithMeta("orders", "item_shipments", "order_id", "order_id", "1-N")

	checker := newSourceOnlyChecker(g, "testdb")

	// DB reports an FK from item_shipments.shipment_item_id -> order_items.item_id
	// This FK is NOT represented in the graph (item_shipments is sibling, not child of order_items).
	// Child and parent column names are deliberately distinct so that swapping
	// childColumn/parentColumn in reconcileInternalFKs produces a different, and
	// therefore detectable, message.
	run := newPreflightRun(checker)
	run.fkWi = validations.ForeignKeyResult{Keys: []validations.ForeignKey{
		{ConstraintName: "fk_items_orders", ChildTable: "order_items", ChildColumns: []string{"order_id"},
			ParentTable: "orders", ParentColumns: []string{"order_id"}},
		{ConstraintName: "fk_ship_orders", ChildTable: "item_shipments", ChildColumns: []string{"order_id"},
			ParentTable: "orders", ParentColumns: []string{"order_id"}},
		{ConstraintName: "fk_ship_items", ChildTable: "item_shipments", ChildColumns: []string{"shipment_item_id"},
			ParentTable: "order_items", ParentColumns: []string{"item_id"}},
	}}
	run.fkWiLoaded = true

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
}

func TestValidateInternalFKCoverage_ProperlyNestedConfig(t *testing.T) {
	// Graph: orders -> order_items -> item_shipments (properly nested)
	g := graph.NewGraph("orders", "order_id")
	g.AddNode("order_items", &graph.Node{Name: "order_items", ForeignKey: "order_id", ReferenceKey: "order_id", DependencyType: "1-N"})
	g.AddNode("item_shipments", &graph.Node{Name: "item_shipments", ForeignKey: "item_id", ReferenceKey: "item_id", DependencyType: "1-N"})
	g.SetPK("order_items", "item_id")
	g.SetPK("item_shipments", "shipment_id")
	g.AddEdgeWithMeta("orders", "order_items", "order_id", "order_id", "1-N")
	g.AddEdgeWithMeta("order_items", "item_shipments", "item_id", "item_id", "1-N")

	checker := newSourceOnlyChecker(g, "testdb")

	run := newPreflightRun(checker)
	run.fkWi = validations.ForeignKeyResult{Keys: []validations.ForeignKey{
		{ConstraintName: "fk_items_orders", ChildTable: "order_items", ChildColumns: []string{"order_id"},
			ParentTable: "orders", ParentColumns: []string{"order_id"}},
		{ConstraintName: "fk_ship_items", ChildTable: "item_shipments", ChildColumns: []string{"item_id"},
			ParentTable: "order_items", ParentColumns: []string{"item_id"}},
	}}
	run.fkWiLoaded = true

	err := checker.ValidateInternalFKCoverage(context.Background(), run)
	if err != nil {
		t.Fatalf("expected no error for properly nested config, got: %v", err)
	}
}

func TestValidateInternalFKCoverage_WrongFKColumn(t *testing.T) {
	// Graph: orders -> payments with FK column "cust_id"
	// But DB has: payments.customer_id -> orders.order_id
	g := graph.NewGraph("orders", "order_id")
	g.AddNode("payments", &graph.Node{Name: "payments", ForeignKey: "cust_id", ReferenceKey: "order_id", DependencyType: "1-N"})
	g.SetPK("payments", "payment_id")
	g.AddEdgeWithMeta("orders", "payments", "cust_id", "order_id", "1-N")

	checker := newSourceOnlyChecker(g, "testdb")

	run := newPreflightRun(checker)
	run.fkWi = validations.ForeignKeyResult{Keys: []validations.ForeignKey{
		{ConstraintName: "fk_pay_orders", ChildTable: "payments", ChildColumns: []string{"customer_id"},
			ParentTable: "orders", ParentColumns: []string{"order_id"}},
	}}
	run.fkWiLoaded = true

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
}

func TestValidateInternalFKCoverage_WrongReferenceColumn(t *testing.T) {
	// Graph: orders (PK: order_id) -> line_items with FK "order_id" referencing "order_id"
	// But DB has: line_items.order_id -> orders.id (different referenced column)
	g := graph.NewGraph("orders", "order_id")
	g.AddNode("line_items", &graph.Node{Name: "line_items", ForeignKey: "order_id", ReferenceKey: "order_id", DependencyType: "1-N"})
	g.SetPK("line_items", "line_id")
	g.AddEdgeWithMeta("orders", "line_items", "order_id", "order_id", "1-N")

	checker := newSourceOnlyChecker(g, "testdb")

	run := newPreflightRun(checker)
	run.fkWi = validations.ForeignKeyResult{Keys: []validations.ForeignKey{
		{ConstraintName: "fk_line_orders", ChildTable: "line_items", ChildColumns: []string{"order_id"},
			ParentTable: "orders", ParentColumns: []string{"id"}},
	}}
	run.fkWiLoaded = true

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
}

// TestValidateInternalFKCoverage_NoInternalFKs preloads an EMPTY ForeignKeyResult, not
// an external/out-of-graph foreign key. fkWithin is defined as the both-endpoints-in-graph
// set, so seeding it with a key the Within selector could never return would violate the
// accessor's own fact contract and test a state that cannot occur.
func TestValidateInternalFKCoverage_NoInternalFKs(t *testing.T) {
	g := createPreflightTestGraph()
	checker := newSourceOnlyChecker(g, "testdb")

	run := newPreflightRun(checker)
	run.fkWi = validations.ForeignKeyResult{}
	run.fkWiLoaded = true

	err := checker.ValidateInternalFKCoverage(context.Background(), run)
	if err != nil {
		t.Fatalf("expected no error when no internal FKs exist, got: %v", err)
	}
}

func TestValidateInternalFKCoverage_SelfReferencingFK(t *testing.T) {
	g := graph.NewGraph("categories", "id")

	checker := newSourceOnlyChecker(g, "testdb")

	// Self-referencing FK: categories.parent_id -> categories.id
	run := newPreflightRun(checker)
	run.fkWi = validations.ForeignKeyResult{Keys: []validations.ForeignKey{
		{ConstraintName: "fk_cat_parent", ChildTable: "categories", ChildColumns: []string{"parent_id"},
			ParentTable: "categories", ParentColumns: []string{"id"}},
	}}
	run.fkWiLoaded = true

	err := checker.ValidateInternalFKCoverage(context.Background(), run)
	if err != nil {
		t.Fatalf("expected no error for self-referencing FK, got: %v", err)
	}
}

// TestValidateInternalFKCoverage_MultipleFailures is also the sort proof. Facts are
// supplied payments-first; sorted output must put item_shipments first ("  - i" <
// "  - p"). Asserting the exact adjacent block is what fails if the sort is removed —
// two independent strings.Contains checks would not.
func TestValidateInternalFKCoverage_MultipleFailures(t *testing.T) {
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

	checker := newSourceOnlyChecker(g, "testdb")

	// Facts are supplied payments-first, then item_shipments, then order_items (the
	// third matches its edge and yields no line, exactly as in the 1.8 fixture).
	run := newPreflightRun(checker)
	run.fkWi = validations.ForeignKeyResult{Keys: []validations.ForeignKey{
		{ConstraintName: "fk_pay_orders", ChildTable: "payments", ChildColumns: []string{"customer_id"},
			ParentTable: "orders", ParentColumns: []string{"order_id"}},
		{ConstraintName: "fk_ship_items", ChildTable: "item_shipments", ChildColumns: []string{"item_id"},
			ParentTable: "order_items", ParentColumns: []string{"item_id"}},
		{ConstraintName: "fk_items_orders", ChildTable: "order_items", ChildColumns: []string{"order_id"},
			ParentTable: "orders", ParentColumns: []string{"order_id"}},
	}}
	run.fkWiLoaded = true

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
}

// TestValidateInternalFKCoverageInspectionErrorIsPlain — contract 2. A memoized
// fkWithin error must surface as a PLAIN error carrying goarchive's
// "internal_fk_coverage" inspection wrapper, never a *PreflightError.
func TestValidateInternalFKCoverageInspectionErrorIsPlain(t *testing.T) {
	wantErr := errors.New("fk fetch failed")
	g := createPreflightTestGraph()
	checker := newSourceOnlyChecker(g, "testdb")
	run := newPreflightRun(checker)
	run.fkWiErr, run.fkWiLoaded = wantErr, true

	err := checker.ValidateInternalFKCoverage(context.Background(), run)
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
	g := graph.NewGraph("orders", "id")
	p := newSourceOnlyChecker(g, "srcdb")

	run := newPreflightRun(p)
	run.srcTables = []validations.TableInfo{
		{Table: "orders", Type: "BASE TABLE", Engine: "MyISAM"},
	}
	run.srcTablesLoaded = true

	err := p.ValidateStorageEngine(context.Background(), run)

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
	g := graph.NewGraph("orders", "id")
	p := newSourceOnlyChecker(g, "srcdb")

	run := newPreflightRun(p)
	run.srcTables = []validations.TableInfo{
		{Table: "orders", Type: "BASE TABLE", Engine: "innodb"},
	}
	run.srcTablesLoaded = true

	if err := p.ValidateStorageEngine(context.Background(), run); err != nil {
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
// TestValidateStorageEngineInspectionErrorIsPlain — REFERENCE SHAPE 4: a preloaded
// fact ERROR. Every fact has a companion *Err field, so a load failure is injected
// rather than mocked.
//
// The sentinel is load-bearing, not stylistic: this test's own assertion is only "some
// plain error", so without errors.Is a stray query — which also yields a plain error —
// would be indistinguishable from the injected failure. That makes this strictly
// stronger than the WillReturnError mock it replaces.
func TestValidateStorageEngineInspectionErrorIsPlain(t *testing.T) {
	errInspect := errors.New("query failed")

	g := graph.NewGraph("orders", "id")
	p := newSourceOnlyChecker(g, "srcdb")

	run := newPreflightRun(p)
	run.srcTablesErr = errInspect
	run.srcTablesLoaded = true

	err := p.ValidateStorageEngine(context.Background(), run)
	if !errors.Is(err, errInspect) {
		t.Fatalf("the memoized load error must propagate, got: %v", err)
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
// The checker carries no database handle at all (newSourceOnlyChecker): an
// implementation that ignored `run` and built its own Inspector would hit
// "validations: nil Querier" — a plain error — so the *PreflightError assertion below
// fails. The VERDICT is what discriminates, not any mock bookkeeping.
func TestValidateStorageEngineConsumesTheCachedFact(t *testing.T) {
	g := graph.NewGraph("orders", "id")
	p := newSourceOnlyChecker(g, "srcdb")

	run := newPreflightRun(p)
	run.srcTables = []validations.TableInfo{
		{Table: "orders", Type: "BASE TABLE", Engine: "MyISAM"},
	}
	run.srcTablesLoaded = true

	err := p.ValidateStorageEngine(context.Background(), run)

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
	g := graph.NewGraph("orders", "id")
	p := newSourceOnlyChecker(g, "srcdb")

	run := newPreflightRun(p)
	run.srcTables = []validations.TableInfo{
		{Table: "orders", Type: "BASE TABLE", Engine: ""},
	}
	run.srcTablesLoaded = true

	err := p.ValidateStorageEngine(context.Background(), run)

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
	g := createPreflightTestGraph()
	log := logger.NewDefault()
	// destinationDB stays nil, but destinationDBName is deliberately non-empty
	// (unlike the old fixture, which left both unset) so the two conditions are
	// separable: a guard mistakenly keyed on destinationDBName would not trip
	// here. Built directly since this is package archiver and
	// NewPreflightChecker/ConfigureDestination refuse a nil destination handle.
	// The source db stays nil too: all four guards below trip on
	// p.destinationDB == nil before ever reading p.db or the run.
	checker := &PreflightChecker{
		db:                nil,
		sourceDBName:      "sourcedb",
		destinationDB:     nil,
		destinationDBName: "destdb",
		graph:             g,
		logger:            log,
	}

	ctx := context.Background()

	err := checker.ValidateDestinationTablesExist(ctx, newPreflightRun(checker))
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
	g := createPreflightTestGraph()
	checker := newDestChecker(t, g, "sourcedb", "destdb", "destdb")
	checker.SetVerification(config.VerificationConfig{Method: "sha256", SkipVerification: false})

	sourceCols := []validations.ColumnSpec{
		withCharset(specColT(1, "name", "varchar(255)"), "utf8mb4", "utf8mb4_0900_ai_ci"),
	}
	destCols := []validations.ColumnSpec{
		withCharset(specColT(1, "name", "varchar(255)"), "latin1", "latin1_swedish_ci"),
	}
	run := newPreflightRun(checker)
	run.specs = []specPair{preloadedSchemaSpecPair(sourceCols, destCols)}
	run.specsLoaded = true

	err := checker.ValidateDestinationSchemaCompatibility(context.Background(), run)
	if err != nil {
		t.Fatalf("charset mismatch should be allowed (warn) under sha256 verification, got: %v", err)
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
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("nil-destination guard dereferenced the nil run: %v", recovered)
		}
	}()

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
	wantErr := errors.New("grants failed")

	g := createPreflightTestGraph()
	p := newDestChecker(t, g, "", "destdb", "jobs")

	run := newPreflightRun(p)
	run.dstGrantsErr = wantErr
	run.dstGrantsLoaded = true

	err := p.ValidateJobSchemaPermissions(context.Background(), run)
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
	g := createPreflightTestGraph()
	p := newDestChecker(t, g, "", "destdb", "jobs")

	run := newPreflightRun(p)
	run.dstGrants = validations.Grants{}
	run.dstGrantsLoaded = true

	err := p.ValidateJobSchemaPermissions(context.Background(), run)
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
}

// TestValidateNoInvisibleColumns_Rejected proves a participating table with an
// INVISIBLE column is rejected: SELECT * would silently omit it from both the
// copy and the verification hash, so it must be caught before any archive.
func TestValidateNoInvisibleColumns_Rejected(t *testing.T) {
	checker := newSourceOnlyChecker(createPreflightTestGraph(), "testdb")
	ctx := context.Background()

	// The check derives from the general column fact instead of its own
	// InvisibleColumns query.
	run := newPreflightRun(checker)
	run.srcColumns = []validations.TableColumns{
		{Table: "orders", Columns: []validations.ColumnInfo{
			{Name: "secret_payload", Ordinal: 1, DataType: "varchar", Invisible: true},
		}},
	}
	run.srcColumnsLoaded = true

	err := checker.ValidateNoInvisibleColumns(ctx, run)
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
}

// TestValidateNoInvisibleColumns_Success is the negative control: no invisible
// columns among the participating tables passes.
func TestValidateNoInvisibleColumns_Success(t *testing.T) {
	checker := newSourceOnlyChecker(createPreflightTestGraph(), "testdb")
	ctx := context.Background()

	// Columns-shaped fact, no invisible columns anywhere.
	run := newPreflightRun(checker)
	run.srcColumns = []validations.TableColumns{
		{Table: "orders", Columns: []validations.ColumnInfo{
			{Name: "id", Ordinal: 1, DataType: "bigint"},
		}},
	}
	run.srcColumnsLoaded = true

	if err := checker.ValidateNoInvisibleColumns(ctx, run); err != nil {
		t.Fatalf("expected no error when no invisible columns are present, got: %v", err)
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
	// AddNode is REQUIRED. AddEdgeWithMeta only records the edge in Children/Parents; it
	// does NOT register the node, so without this the child never reaches AllNodes(),
	// is never requested, and the fact below would be pointless for a table not asked
	// about — the test would then fail for a reason unrelated to what it checks.
	g := graph.NewGraph("orders", "id")
	g.AddNode("order_lines", &graph.Node{
		Name:           "order_lines",
		ForeignKey:     "order_id",
		ReferenceKey:   "id",
		DependencyType: "1-N",
	})
	g.SetPK("order_lines", "id")
	g.AddEdgeWithMeta("orders", "order_lines", "order_id", "id", "1-N")

	p := newSourceOnlyChecker(g, "srcdb")

	// "zeta" precedes "alpha" DELIBERATELY: ordinal order here is not alphabetical
	// order, so the within-table assertion below genuinely fails if anything sorts the
	// columns. With alphabetically-ordered names the assertion could not tell the two
	// apart and would pin nothing.
	run := newPreflightRun(p)
	run.srcColumns = []validations.TableColumns{
		{Table: "order_lines", Columns: []validations.ColumnInfo{
			{Name: "hidden", Ordinal: 1, DataType: "varchar", Invisible: true},
		}},
		{Table: "orders", Columns: []validations.ColumnInfo{
			{Name: "zeta", Ordinal: 1, DataType: "varchar", Invisible: true},
			{Name: "alpha", Ordinal: 2, DataType: "varchar", Invisible: true},
		}},
	}
	run.srcColumnsLoaded = true

	err := p.ValidateNoInvisibleColumns(context.Background(), run)

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
	g := graph.NewGraph("orders", "id")
	p := newSourceOnlyChecker(g, "srcdb")

	// Columns-shaped fact, no invisible columns.
	run := newPreflightRun(p)
	run.srcColumns = []validations.TableColumns{
		{Table: "orders", Columns: []validations.ColumnInfo{
			{Name: "id", Ordinal: 1, DataType: "bigint"},
		}},
	}
	run.srcColumnsLoaded = true

	if err := p.ValidateNoInvisibleColumns(context.Background(), run); err != nil {
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
	errInspect := errors.New("query failed")

	g := graph.NewGraph("orders", "id")
	p := newSourceOnlyChecker(g, "srcdb")

	run := newPreflightRun(p)
	run.srcColumnsErr = errInspect
	run.srcColumnsLoaded = true

	err := p.ValidateNoInvisibleColumns(context.Background(), run)
	if err == nil {
		t.Fatal("expected an inspection error, got nil")
	}
	if !errors.Is(err, errInspect) {
		t.Fatalf("the memoized load error must propagate, got: %v", err)
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("an inspection failure must be a plain error, got *PreflightError: %v", pe)
	}
	// The stage's OWN wrapper text ("preflight invisible_columns inspection failed") is
	// what must be asserted here (phase 015 pinned this string).
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
	g := graph.NewGraph("orders", "id")
	p := newSourceOnlyChecker(g, "srcdb")

	// Columns-shaped fact, EXTRA = "INVISIBLE" on "note".
	run := newPreflightRun(p)
	run.srcColumns = []validations.TableColumns{
		{Table: "orders", Columns: []validations.ColumnInfo{
			{Name: "note", Ordinal: 1, DataType: "varchar", Invisible: true},
		}},
	}
	run.srcColumnsLoaded = true

	err := p.ValidateNoInvisibleColumns(context.Background(), run)

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
	g := graph.NewGraph("orders", "id")
	p := newSourceOnlyChecker(g, "srcdb")

	run := newPreflightRun(p)
	run.srcColumns = []validations.TableColumns{
		{Table: "orders", Columns: []validations.ColumnInfo{
			{Name: "id", Ordinal: 1, DataType: "bigint"},
		}},
	}
	run.srcColumnsLoaded = true

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
	errInspect := errors.New("query failed")

	g := graph.NewGraph("orders", "id")
	p := newSourceOnlyChecker(g, "srcdb")

	run := newPreflightRun(p)
	run.srcDelTriggersErr = errInspect
	run.srcDelTriggersLoaded = true

	err := p.ValidateTriggers(context.Background(), run, false)
	if err == nil {
		t.Fatal("expected an inspection error, got nil")
	}
	if !errors.Is(err, errInspect) {
		t.Fatalf("the memoized load error must propagate, got: %v", err)
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
	g := graph.NewGraph("orders", "id")
	p := newSourceOnlyChecker(g, "srcdb")

	run := newPreflightRun(p)
	run.srcDelTriggers = []validations.TriggerInfo{
		{Table: "orders", Name: "trg_del", Event: "DELETE", Timing: "AFTER"},
	}
	run.srcDelTriggersLoaded = true

	err := p.ValidateTriggers(context.Background(), run, false)

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
	errInspect := errors.New("query failed")

	g := graph.NewGraph("orders", "id")
	p := newDestChecker(t, g, "srcdb", "dstdb", "dstdb")

	run := newPreflightRun(p)
	run.dstInsTriggersErr = errInspect
	run.dstInsTriggersLoaded = true

	err := p.ValidateDestinationInsertTriggers(context.Background(), run)
	if err == nil {
		t.Fatal("expected an inspection error, got nil")
	}
	if !errors.Is(err, errInspect) {
		t.Fatalf("the memoized load error must propagate, got: %v", err)
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
	g := graph.NewGraph("orders", "id")
	p := newDestChecker(t, g, "srcdb", "dstdb", "dstdb")
	run := newPreflightRun(p)
	ctx := context.Background()

	run.dstInsTriggers = []validations.TriggerInfo{
		{Table: "orders", Name: "trg_ins", Event: "INSERT", Timing: "BEFORE"},
	}
	run.dstInsTriggersLoaded = true

	err := p.ValidateDestinationInsertTriggers(ctx, run)

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
	wantErr := errors.New("boom")

	g := graph.NewGraph("orders", "id")
	p := newSourceOnlyChecker(g, "srcdb")

	run := newPreflightRun(p)
	run.pksErr = wantErr
	run.pksLoaded = true

	gotErr := p.ValidateSingleColumnPrimaryKey(context.Background(), run)
	if gotErr == nil {
		t.Fatal("expected an inspection error, got nil")
	}
	if !errors.Is(gotErr, wantErr) {
		t.Fatalf("inspection error must wrap %v, got: %v", wantErr, gotErr)
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
	wantErr := errors.New("boom")

	g := graph.NewGraph("orders", "id")
	p := newSourceOnlyChecker(g, "srcdb")

	run := newPreflightRun(p)
	run.pksErr = wantErr
	run.pksLoaded = true

	gotErr := p.ValidateRootPKNumeric(context.Background(), run)
	if gotErr == nil {
		t.Fatal("expected an inspection error, got nil")
	}
	if !errors.Is(gotErr, wantErr) {
		t.Fatalf("inspection error must wrap %v, got: %v", wantErr, gotErr)
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
// One pks fact is preloaded per case, and the two stages run in sequence exactly as
// RunWithProfile calls them (positions 3 and 4), against that SAME fact. A stage that
// built its own Inspector would query a nil source handle and fail here — which no other
// test catches.
//
// The varchar case is what keeps this non-vacuous. ValidateSingleColumnPrimaryKey passes
// on it (the key IS single-column) while ValidateRootPKNumeric must reject it, so the
// second stage can only produce the right answer by reading the shared fact. Without that
// case a ValidateRootPKNumeric that silently returned nil would pass: the fact is already
// populated by the first stage either way.
//
// This replaces a GetRootPKMeta assertion that covered the graph write-back deleted in
// phase 032. There is no write-back to assert on any more.
func TestPKStagesShareOneCachedFact(t *testing.T) {
	cases := []struct {
		name      string
		dataType  string
		isInteger bool
		unsigned  bool
		wantErr   string // "" means the root-PK stage must pass
	}{
		{"integer key passes both stages", "bigint", true, true, ""},
		{"varchar key passes the shape stage and fails the root-PK stage",
			"varchar", false, false, "ROOT_PK_TYPE_UNSUPPORTED"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := graph.NewGraph("orders", "id")
			p := newSourceOnlyChecker(g, "srcdb")

			run := newPreflightRun(p)
			run.pks = []validations.PKInfo{{
				Table:     "orders",
				Kind:      validations.PKSingle,
				Columns:   []string{"id"},
				DataType:  tc.dataType,
				IsInteger: tc.isInteger,
				Unsigned:  tc.unsigned,
			}}
			run.pksLoaded = true
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
		})
	}
}
