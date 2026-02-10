// Package verifier provides comprehensive tests for data integrity verification.
package verifier

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/dbsmedya/goarchive/internal/types"
)

// ============================================================================
// Test Helpers
// ============================================================================

func createTestGraph() *graph.Graph {
	g := graph.NewGraph("users", "id")
	g.AddNode("orders", &graph.Node{Name: "orders", ForeignKey: "user_id", ReferenceKey: "id", DependencyType: "1-N"})
	g.AddNode("order_items", &graph.Node{Name: "order_items", ForeignKey: "order_id", ReferenceKey: "id", DependencyType: "1-N"})
	g.AddEdge("users", "orders")
	g.AddEdge("orders", "order_items")
	return g
}

func createTestRecordSet() *types.RecordSet {
	return &types.RecordSet{
		RootPKs: []interface{}{1, 2, 3},
		Records: map[string][]interface{}{
			"users":       {1, 2, 3},
			"orders":      {10, 11, 12, 13, 14, 15},
			"order_items": {100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111},
		},
	}
}

// ============================================================================
// NewVerifier Tests
// ============================================================================

func TestNewVerifier_Success(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock: %v", err)
	}
	defer db.Close()

	g := createTestGraph()
	log := logger.NewDefault()

	v, err := NewVerifier(db, db, g, MethodCount, log)
	if err != nil {
		t.Fatalf("NewVerifier failed: %v", err)
	}

	if v == nil {
		t.Fatal("NewVerifier returned nil")
	}

	if v.method != MethodCount {
		t.Errorf("Expected method %s, got %s", MethodCount, v.method)
	}

	if v.chunkSize != 1000 {
		t.Errorf("Expected default chunk size 1000, got %d", v.chunkSize)
	}
}

func TestNewVerifier_NilSource(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	g := createTestGraph()
	log := logger.NewDefault()

	_, err := NewVerifier(nil, db, g, MethodCount, log)
	if err == nil {
		t.Error("Expected error for nil source database")
	}
}

func TestNewVerifier_NilDestination(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	g := createTestGraph()
	log := logger.NewDefault()

	_, err := NewVerifier(db, nil, g, MethodCount, log)
	if err == nil {
		t.Error("Expected error for nil destination database")
	}
}

func TestNewVerifier_NilGraph(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	log := logger.NewDefault()

	_, err := NewVerifier(db, db, nil, MethodCount, log)
	if err == nil {
		t.Error("Expected error for nil graph")
	}
}

func TestNewVerifier_DefaultMethod(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	g := createTestGraph()
	log := logger.NewDefault()

	v, err := NewVerifier(db, db, g, "", log)
	if err != nil {
		t.Fatalf("NewVerifier failed: %v", err)
	}

	if v.method != MethodCount {
		t.Errorf("Expected default method %s, got %s", MethodCount, v.method)
	}
}

func TestNewVerifier_DefaultLogger(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	g := createTestGraph()

	v, err := NewVerifier(db, db, g, MethodCount, nil)
	if err != nil {
		t.Fatalf("NewVerifier failed: %v", err)
	}

	if v.logger == nil {
		t.Error("Expected default logger to be set")
	}
}

// ============================================================================
// Verify (Count Method) Tests
// ============================================================================

func TestVerify_Count_Success(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer sourceDB.Close()
	destDB, destMock, _ := sqlmock.New()
	defer destDB.Close()

	g := createTestGraph()
	log := logger.NewDefault()
	v, _ := NewVerifier(sourceDB, destDB, g, MethodCount, log)

	recordSet := createTestRecordSet()
	ctx := context.Background()

	// Setup expectations for users table
	sourceMock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `users`").
		WithArgs(1, 2, 3).
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(3))
	destMock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `users`").
		WithArgs(1, 2, 3).
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(3))

	// Setup expectations for orders table
	sourceMock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `orders`").
		WithArgs(10, 11, 12, 13, 14, 15).
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(6))
	destMock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `orders`").
		WithArgs(10, 11, 12, 13, 14, 15).
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(6))

	// Setup expectations for order_items table
	sourceMock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `order_items`").
		WithArgs(100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111).
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(12))
	destMock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `order_items`").
		WithArgs(100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111).
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(12))

	stats, err := v.Verify(ctx, recordSet)

	if err != nil {
		t.Fatalf("Verify failed: %v", err)
	}

	if stats.TablesVerified != 3 {
		t.Errorf("Expected 3 tables verified, got %d", stats.TablesVerified)
	}

	if stats.TablesPassed != 3 {
		t.Errorf("Expected 3 tables passed, got %d", stats.TablesPassed)
	}

	if stats.TablesFailed != 0 {
		t.Errorf("Expected 0 tables failed, got %d", stats.TablesFailed)
	}

	if stats.TotalRows != 21 {
		t.Errorf("Expected 21 total rows, got %d", stats.TotalRows)
	}

	if err := sourceMock.ExpectationsWereMet(); err != nil {
		t.Errorf("Source mock expectations not met: %v", err)
	}
	if err := destMock.ExpectationsWereMet(); err != nil {
		t.Errorf("Dest mock expectations not met: %v", err)
	}
}

func TestVerify_Count_Mismatch(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer sourceDB.Close()
	destDB, destMock, _ := sqlmock.New()
	defer destDB.Close()

	g := createTestGraph()
	log := logger.NewDefault()
	v, _ := NewVerifier(sourceDB, destDB, g, MethodCount, log)

	recordSet := createTestRecordSet()
	ctx := context.Background()

	// Setup expectations for users table - counts don't match
	sourceMock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `users`").
		WithArgs(1, 2, 3).
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(3))
	destMock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `users`").
		WithArgs(1, 2, 3).
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(2)) // Mismatch!

	stats, err := v.Verify(ctx, recordSet)

	if err == nil {
		t.Error("Expected error for count mismatch")
	}

	if stats.TablesFailed != 1 {
		t.Errorf("Expected 1 table failed, got %d", stats.TablesFailed)
	}
}

func TestVerify_Count_EmptyRecordSet(t *testing.T) {
	sourceDB, _, _ := sqlmock.New()
	defer sourceDB.Close()
	destDB, _, _ := sqlmock.New()
	defer destDB.Close()

	g := createTestGraph()
	log := logger.NewDefault()
	v, _ := NewVerifier(sourceDB, destDB, g, MethodCount, log)

	emptyRecordSet := &types.RecordSet{
		RootPKs: []interface{}{},
		Records: map[string][]interface{}{},
	}
	ctx := context.Background()

	stats, err := v.Verify(ctx, emptyRecordSet)

	if err != nil {
		t.Fatalf("Verify failed: %v", err)
	}

	if stats.TablesVerified != 0 {
		t.Errorf("Expected 0 tables verified, got %d", stats.TablesVerified)
	}
}

func TestVerify_Count_QueryError(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer sourceDB.Close()
	destDB, _, _ := sqlmock.New()
	defer destDB.Close()

	g := createTestGraph()
	log := logger.NewDefault()
	v, _ := NewVerifier(sourceDB, destDB, g, MethodCount, log)

	recordSet := createTestRecordSet()
	ctx := context.Background()

	// Setup expectations with error
	sourceMock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `users`").
		WithArgs(1, 2, 3).
		WillReturnError(errors.New("query failed"))

	_, err := v.Verify(ctx, recordSet)

	if err == nil {
		t.Error("Expected error for query failure")
	}
}

// ============================================================================
// Verify (Skip Method) Tests
// ============================================================================

func TestVerify_Skip(t *testing.T) {
	sourceDB, _, _ := sqlmock.New()
	defer sourceDB.Close()
	destDB, _, _ := sqlmock.New()
	defer destDB.Close()

	g := createTestGraph()
	log := logger.NewDefault()
	v, _ := NewVerifier(sourceDB, destDB, g, MethodSkip, log)

	recordSet := createTestRecordSet()
	ctx := context.Background()

	stats, err := v.Verify(ctx, recordSet)

	if err != nil {
		t.Fatalf("Verify failed: %v", err)
	}

	if stats.Method != MethodSkip {
		t.Errorf("Expected method %s, got %s", MethodSkip, stats.Method)
	}

	if stats.TablesVerified != 0 {
		t.Errorf("Expected 0 tables verified when skipping, got %d", stats.TablesVerified)
	}
}

// ============================================================================
// Verify (SHA256 Method) Tests
// ============================================================================

func TestVerify_SHA256_Success(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer sourceDB.Close()
	destDB, destMock, _ := sqlmock.New()
	defer destDB.Close()

	g := createTestGraph()
	log := logger.NewDefault()
	v, _ := NewVerifier(sourceDB, destDB, g, MethodSHA256, log)
	v.SetChunkSize(100) // Small chunk for testing

	recordSet := &types.RecordSet{
		RootPKs: []interface{}{1},
		Records: map[string][]interface{}{
			"users": {1},
		},
	}
	ctx := context.Background()

	// Setup expectations for source query
	rows := sqlmock.NewRows([]string{"id", "name", "email"}).
		AddRow(1, "John Doe", "john@example.com")
	sourceMock.ExpectQuery("SELECT \\* FROM `users`").
		WithArgs(1).
		WillReturnRows(rows)

	// Setup expectations for destination query (same data)
	destRows := sqlmock.NewRows([]string{"id", "name", "email"}).
		AddRow(1, "John Doe", "john@example.com")
	destMock.ExpectQuery("SELECT \\* FROM `users`").
		WithArgs(1).
		WillReturnRows(destRows)

	stats, err := v.Verify(ctx, recordSet)

	if err != nil {
		t.Fatalf("Verify failed: %v", err)
	}

	if stats.TablesVerified != 1 {
		t.Errorf("Expected 1 table verified, got %d", stats.TablesVerified)
	}

	if stats.TablesPassed != 1 {
		t.Errorf("Expected 1 table passed, got %d", stats.TablesPassed)
	}
}

func TestVerify_SHA256_Mismatch(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer sourceDB.Close()
	destDB, destMock, _ := sqlmock.New()
	defer destDB.Close()

	g := createTestGraph()
	log := logger.NewDefault()
	v, _ := NewVerifier(sourceDB, destDB, g, MethodSHA256, log)
	v.SetChunkSize(100)

	recordSet := &types.RecordSet{
		RootPKs: []interface{}{1},
		Records: map[string][]interface{}{
			"users": {1},
		},
	}
	ctx := context.Background()

	// Source has different data
	rows := sqlmock.NewRows([]string{"id", "name", "email"}).
		AddRow(1, "John Doe", "john@example.com")
	sourceMock.ExpectQuery("SELECT \\* FROM `users`").
		WithArgs(1).
		WillReturnRows(rows)

	// Destination has different data
	destRows := sqlmock.NewRows([]string{"id", "name", "email"}).
		AddRow(1, "Jane Doe", "jane@example.com")
	destMock.ExpectQuery("SELECT \\* FROM `users`").
		WithArgs(1).
		WillReturnRows(destRows)

	stats, err := v.Verify(ctx, recordSet)

	if err == nil {
		t.Error("Expected error for SHA256 mismatch")
	}

	if stats.TablesFailed != 1 {
		t.Errorf("Expected 1 table failed, got %d", stats.TablesFailed)
	}
}

func TestVerify_SHA256_CountMismatch(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer sourceDB.Close()
	destDB, destMock, _ := sqlmock.New()
	defer destDB.Close()

	g := createTestGraph()
	log := logger.NewDefault()
	v, _ := NewVerifier(sourceDB, destDB, g, MethodSHA256, log)
	v.SetChunkSize(100)

	recordSet := &types.RecordSet{
		RootPKs: []interface{}{1, 2},
		Records: map[string][]interface{}{
			"users": {1, 2},
		},
	}
	ctx := context.Background()

	// Source has 2 rows
	rows := sqlmock.NewRows([]string{"id", "name"}).
		AddRow(1, "User1").
		AddRow(2, "User2")
	sourceMock.ExpectQuery("SELECT \\* FROM `users`").
		WithArgs(1, 2).
		WillReturnRows(rows)

	// Destination has only 1 row
	destRows := sqlmock.NewRows([]string{"id", "name"}).
		AddRow(1, "User1")
	destMock.ExpectQuery("SELECT \\* FROM `users`").
		WithArgs(1, 2).
		WillReturnRows(destRows)

	stats, err := v.Verify(ctx, recordSet)

	if err == nil {
		t.Error("Expected error for count mismatch in SHA256 mode")
	}

	if stats.TablesFailed != 1 {
		t.Errorf("Expected 1 table failed, got %d", stats.TablesFailed)
	}
}

// ============================================================================
// verifyByCount Tests
// ============================================================================

func TestVerifyByCount_EmptyPKs(t *testing.T) {
	sourceDB, _, _ := sqlmock.New()
	defer sourceDB.Close()
	destDB, _, _ := sqlmock.New()
	defer destDB.Close()

	g := createTestGraph()
	log := logger.NewDefault()
	v, _ := NewVerifier(sourceDB, destDB, g, MethodCount, log)
	ctx := context.Background()

	result, err := v.verifyByCount(ctx, "users", []interface{}{})

	if err != nil {
		t.Fatalf("verifyByCount failed: %v", err)
	}

	if !result.Match {
		t.Error("Expected match for empty PKs")
	}

	if result.SourceCount != 0 {
		t.Errorf("Expected 0 source count, got %d", result.SourceCount)
	}
}

func TestVerifyByCount_ErrorMessage(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer sourceDB.Close()
	destDB, destMock, _ := sqlmock.New()
	defer destDB.Close()

	g := createTestGraph()
	log := logger.NewDefault()
	v, _ := NewVerifier(sourceDB, destDB, g, MethodCount, log)
	ctx := context.Background()

	// Source has 3 rows
	sourceMock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `users`").
		WithArgs(1, 2, 3).
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(3))

	// Destination has 2 rows
	destMock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `users`").
		WithArgs(1, 2, 3).
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(2))

	result, err := v.verifyByCount(ctx, "users", []interface{}{1, 2, 3})

	if err != nil {
		t.Fatalf("verifyByCount failed: %v", err)
	}

	if result.Match {
		t.Error("Expected no match for different counts")
	}

	if result.ErrorMessage == "" {
		t.Error("Expected error message for mismatch")
	}
}

// ============================================================================
// verifyBySHA256 Tests
// ============================================================================

func TestVerifyBySHA256_EmptyPKs(t *testing.T) {
	sourceDB, _, _ := sqlmock.New()
	defer sourceDB.Close()
	destDB, _, _ := sqlmock.New()
	defer destDB.Close()

	g := createTestGraph()
	log := logger.NewDefault()
	v, _ := NewVerifier(sourceDB, destDB, g, MethodSHA256, log)
	ctx := context.Background()

	result, err := v.verifyBySHA256(ctx, "users", []interface{}{})

	if err != nil {
		t.Fatalf("verifyBySHA256 failed: %v", err)
	}

	if !result.Match {
		t.Error("Expected match for empty PKs")
	}
}

// ============================================================================
// Context Cancellation Tests
// ============================================================================

func TestVerify_ContextCancellation(t *testing.T) {
	sourceDB, _, _ := sqlmock.New()
	defer sourceDB.Close()
	destDB, _, _ := sqlmock.New()
	defer destDB.Close()

	g := createTestGraph()
	log := logger.NewDefault()
	v, _ := NewVerifier(sourceDB, destDB, g, MethodCount, log)

	recordSet := createTestRecordSet()

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := v.Verify(ctx, recordSet)

	if err == nil {
		t.Error("Expected error for cancelled context")
	}
}

// ============================================================================
// Setter/Getter Tests
// ============================================================================

func TestSetChunkSize(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	g := createTestGraph()
	log := logger.NewDefault()
	v, _ := NewVerifier(db, db, g, MethodSHA256, log)

	v.SetChunkSize(500)
	if v.GetChunkSize() != 500 {
		t.Errorf("Expected chunk size 500, got %d", v.GetChunkSize())
	}

	// Test that 0 or negative doesn't change
	v.SetChunkSize(0)
	if v.GetChunkSize() != 500 {
		t.Errorf("Chunk size should not change with 0, got %d", v.GetChunkSize())
	}

	v.SetChunkSize(-1)
	if v.GetChunkSize() != 500 {
		t.Errorf("Chunk size should not change with negative, got %d", v.GetChunkSize())
	}
}

func TestGetMethod(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	g := createTestGraph()
	log := logger.NewDefault()

	v, _ := NewVerifier(db, db, g, MethodCount, log)
	if v.GetMethod() != MethodCount {
		t.Errorf("Expected method %s, got %s", MethodCount, v.GetMethod())
	}

	v2, _ := NewVerifier(db, db, g, MethodSHA256, log)
	if v2.GetMethod() != MethodSHA256 {
		t.Errorf("Expected method %s, got %s", MethodSHA256, v2.GetMethod())
	}
}

func TestSetLogger(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	g := createTestGraph()
	log := logger.NewDefault()
	v, _ := NewVerifier(db, db, g, MethodCount, log)

	newLog := logger.NewDefault()
	v.SetLogger(newLog)

	if v.logger != newLog {
		t.Error("SetLogger did not set logger correctly")
	}
}

// ============================================================================
// serializeRow Tests
// ============================================================================

func TestSerializeRow(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	g := createTestGraph()
	log := logger.NewDefault()
	v, _ := NewVerifier(db, db, g, MethodSHA256, log)

	tests := []struct {
		name     string
		columns  []string
		values   []interface{}
		contains []string
	}{
		{
			name:     "basic row",
			columns:  []string{"id", "name"},
			values:   []interface{}{int64(1), "test"},
			contains: []string{"id=1", "name=test"},
		},
		{
			name:     "with nil",
			columns:  []string{"id", "optional"},
			values:   []interface{}{int64(1), nil},
			contains: []string{"id=1", "optional=NULL"},
		},
		{
			name:     "with bytes",
			columns:  []string{"id", "data"},
			values:   []interface{}{int64(1), []byte("hello")},
			contains: []string{"id=1", "data=hello"},
		},
		{
			name:     "with float",
			columns:  []string{"id", "value"},
			values:   []interface{}{int64(1), float64(3.14)},
			contains: []string{"id=1"},
		},
		{
			name:     "with bool",
			columns:  []string{"id", "active"},
			values:   []interface{}{int64(1), true},
			contains: []string{"id=1", "active=true"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := v.serializeRow(tt.columns, tt.values)
			for _, expected := range tt.contains {
				if !containsSubstring(result, expected) {
					t.Errorf("Expected result to contain %q, got %q", expected, result)
				}
			}
		})
	}
}

func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstringHelper(s, substr))
}

func containsSubstringHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// ============================================================================
// Unsupported Method Test
// ============================================================================

func TestVerify_UnsupportedMethod(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer sourceDB.Close()
	destDB, _, _ := sqlmock.New()
	defer destDB.Close()

	g := createTestGraph()
	log := logger.NewDefault()
	v, _ := NewVerifier(sourceDB, destDB, g, "unsupported", log)

	recordSet := createTestRecordSet()
	ctx := context.Background()

	// Setup one successful query first
	sourceMock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `users`").
		WithArgs(1, 2, 3).
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(3))

	_, err := v.Verify(ctx, recordSet)

	if err == nil {
		t.Error("Expected error for unsupported method")
	}
}
