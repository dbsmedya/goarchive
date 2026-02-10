// Package archiver provides comprehensive tests for the delete phase.
package archiver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
)

// ============================================================================
// Test Helpers
// ============================================================================

func createDeleteTestGraph() *graph.Graph {
	// Create graph: users -> orders -> order_items
	g := graph.NewGraph("users", "id")
	g.AddNode("orders", &graph.Node{Name: "orders", ForeignKey: "user_id", ReferenceKey: "id", DependencyType: "1-N"})
	g.AddNode("order_items", &graph.Node{Name: "order_items", ForeignKey: "order_id", ReferenceKey: "id", DependencyType: "1-N"})
	g.AddEdge("users", "orders")
	g.AddEdge("orders", "order_items")
	return g
}

func createDeepDeleteGraph() *graph.Graph {
	// Create deep graph: A -> B -> C -> D
	g := graph.NewGraph("A", "id")
	g.AddNode("B", &graph.Node{Name: "B"})
	g.AddNode("C", &graph.Node{Name: "C"})
	g.AddNode("D", &graph.Node{Name: "D"})
	g.AddEdge("A", "B")
	g.AddEdge("B", "C")
	g.AddEdge("C", "D")
	return g
}

func createDeleteRecordSet() *RecordSet {
	return &RecordSet{
		RootPKs: []interface{}{1, 2, 3},
		Records: map[string][]interface{}{
			"users":       {1, 2, 3},
			"orders":      {10, 11, 12, 13, 14, 15},
			"order_items": {100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111},
		},
	}
}

// ============================================================================
// NewDeletePhase Tests
// ============================================================================

func TestNewDeletePhase_Success(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock: %v", err)
	}
	defer db.Close()

	g := createDeleteTestGraph()
	log := logger.NewDefault()

	dp, err := NewDeletePhase(db, g, 500, log)
	if err != nil {
		t.Fatalf("NewDeletePhase failed: %v", err)
	}

	if dp == nil {
		t.Fatal("NewDeletePhase returned nil")
	}

	if dp.batchSize != 500 {
		t.Errorf("Expected batch size 500, got %d", dp.batchSize)
	}

	if dp.db != db {
		t.Error("Database mismatch")
	}

	if dp.graph != g {
		t.Error("Graph mismatch")
	}
}

func TestNewDeletePhase_NilDB(t *testing.T) {
	g := createDeleteTestGraph()
	log := logger.NewDefault()

	_, err := NewDeletePhase(nil, g, 500, log)
	if err == nil {
		t.Error("Expected error for nil database")
	}
}

func TestNewDeletePhase_NilGraph(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	log := logger.NewDefault()

	_, err := NewDeletePhase(db, nil, 500, log)
	if err == nil {
		t.Error("Expected error for nil graph")
	}
}

func TestNewDeletePhase_DefaultBatchSize(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	g := createDeleteTestGraph()
	log := logger.NewDefault()

	dp, err := NewDeletePhase(db, g, 0, log)
	if err != nil {
		t.Fatalf("NewDeletePhase failed: %v", err)
	}

	if dp.batchSize != 500 {
		t.Errorf("Expected default batch size 500, got %d", dp.batchSize)
	}
}

func TestNewDeletePhase_DefaultLogger(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	g := createDeleteTestGraph()

	dp, err := NewDeletePhase(db, g, 500, nil)
	if err != nil {
		t.Fatalf("NewDeletePhase failed: %v", err)
	}

	if dp.logger == nil {
		t.Error("Expected default logger to be set")
	}
}

// ============================================================================
// Delete Tests
// ============================================================================

func TestDelete_Success(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	g := createDeleteTestGraph()
	log := logger.NewDefault()
	dp, _ := NewDeletePhase(db, g, 1000, log)

	recordSet := createDeleteRecordSet()
	ctx := context.Background()

	// Delete order should be: order_items -> orders -> users (reverse topological)

	// Expect delete from order_items (all 12 in one batch since batchSize=1000)
	mock.ExpectExec("DELETE FROM `order_items` WHERE `id` IN").
		WithArgs(100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111).
		WillReturnResult(sqlmock.NewResult(0, 12))

	// Expect delete from orders (all 6 in one batch)
	mock.ExpectExec("DELETE FROM `orders` WHERE `id` IN").
		WithArgs(10, 11, 12, 13, 14, 15).
		WillReturnResult(sqlmock.NewResult(0, 6))

	// Expect delete from users (all 3 in one batch)
	mock.ExpectExec("DELETE FROM `users` WHERE `id` IN").
		WithArgs(1, 2, 3).
		WillReturnResult(sqlmock.NewResult(0, 3))

	stats, err := dp.Delete(ctx, recordSet)

	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if stats.TablesProcessed != 3 {
		t.Errorf("Expected 3 tables processed, got %d", stats.TablesProcessed)
	}

	if stats.RowsDeleted != 21 {
		t.Errorf("Expected 21 rows deleted, got %d", stats.RowsDeleted)
	}

	if stats.TablesSkipped != 0 {
		t.Errorf("Expected 0 tables skipped, got %d", stats.TablesSkipped)
	}

	if stats.Duration <= 0 {
		t.Error("Expected duration > 0")
	}

	// Verify per-table stats
	if stats.RowsPerTable["order_items"] != 12 {
		t.Errorf("Expected 12 order_items deleted, got %d", stats.RowsPerTable["order_items"])
	}
	if stats.RowsPerTable["orders"] != 6 {
		t.Errorf("Expected 6 orders deleted, got %d", stats.RowsPerTable["orders"])
	}
	if stats.RowsPerTable["users"] != 3 {
		t.Errorf("Expected 3 users deleted, got %d", stats.RowsPerTable["users"])
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Mock expectations not met: %v", err)
	}
}

func TestDelete_EmptyRecordSet(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	g := createDeleteTestGraph()
	log := logger.NewDefault()
	dp, _ := NewDeletePhase(db, g, 500, log)

	emptyRecordSet := &RecordSet{
		RootPKs: []interface{}{},
		Records: map[string][]interface{}{},
	}
	ctx := context.Background()

	stats, err := dp.Delete(ctx, emptyRecordSet)

	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if stats.TablesProcessed != 0 {
		t.Errorf("Expected 0 tables processed, got %d", stats.TablesProcessed)
	}

	if stats.RowsDeleted != 0 {
		t.Errorf("Expected 0 rows deleted, got %d", stats.RowsDeleted)
	}
}

func TestDelete_SkipsEmptyTables(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	g := createDeleteTestGraph()
	log := logger.NewDefault()
	dp, _ := NewDeletePhase(db, g, 500, log)

	// Record set with only users (no orders or order_items)
	partialRecordSet := &RecordSet{
		RootPKs: []interface{}{1, 2},
		Records: map[string][]interface{}{
			"users": {1, 2},
			// orders and order_items are empty/not present
		},
	}
	ctx := context.Background()

	// Only expect delete from users
	mock.ExpectExec("DELETE FROM `users` WHERE `id` IN").
		WithArgs(1, 2).
		WillReturnResult(sqlmock.NewResult(0, 2))

	stats, err := dp.Delete(ctx, partialRecordSet)

	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if stats.TablesProcessed != 1 {
		t.Errorf("Expected 1 table processed, got %d", stats.TablesProcessed)
	}

	if stats.TablesSkipped != 2 {
		t.Errorf("Expected 2 tables skipped, got %d", stats.TablesSkipped)
	}

	if stats.RowsDeleted != 2 {
		t.Errorf("Expected 2 rows deleted, got %d", stats.RowsDeleted)
	}
}

func TestDelete_BatchProcessing(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	g := createDeleteTestGraph()
	log := logger.NewDefault()
	dp, _ := NewDeletePhase(db, g, 3, log) // Small batch size

	// Record set with 7 order_items (will need 3 batches: 3, 3, 1)
	recordSet := &RecordSet{
		RootPKs: []interface{}{1},
		Records: map[string][]interface{}{
			"users":       {1},
			"orders":      {10},
			"order_items": {100, 101, 102, 103, 104, 105, 106},
		},
	}
	ctx := context.Background()

	// First batch of order_items (3)
	mock.ExpectExec("DELETE FROM `order_items` WHERE `id` IN").
		WithArgs(100, 101, 102).
		WillReturnResult(sqlmock.NewResult(0, 3))

	// Second batch of order_items (3)
	mock.ExpectExec("DELETE FROM `order_items` WHERE `id` IN").
		WithArgs(103, 104, 105).
		WillReturnResult(sqlmock.NewResult(0, 3))

	// Third batch of order_items (1)
	mock.ExpectExec("DELETE FROM `order_items` WHERE `id` IN").
		WithArgs(106).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Delete from orders
	mock.ExpectExec("DELETE FROM `orders` WHERE `id` IN").
		WithArgs(10).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Delete from users
	mock.ExpectExec("DELETE FROM `users` WHERE `id` IN").
		WithArgs(1).
		WillReturnResult(sqlmock.NewResult(0, 1))

	stats, err := dp.Delete(ctx, recordSet)

	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if stats.RowsDeleted != 9 {
		t.Errorf("Expected 9 rows deleted, got %d", stats.RowsDeleted)
	}
}

func TestDelete_QueryError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	g := createDeleteTestGraph()
	log := logger.NewDefault()
	dp, _ := NewDeletePhase(db, g, 500, log)

	recordSet := createDeleteRecordSet()
	ctx := context.Background()

	// First delete fails
	mock.ExpectExec("DELETE FROM `order_items` WHERE `id` IN").
		WithArgs(100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111).
		WillReturnError(errors.New("delete failed"))

	_, err := dp.Delete(ctx, recordSet)

	if err == nil {
		t.Error("Expected error for delete failure")
	}
}

func TestDelete_PartialDelete(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	g := createDeleteTestGraph()
	log := logger.NewDefault()
	dp, _ := NewDeletePhase(db, g, 500, log)

	recordSet := &RecordSet{
		RootPKs: []interface{}{1, 2, 3},
		Records: map[string][]interface{}{
			"users": {1, 2, 3},
		},
	}
	ctx := context.Background()

	// Only 2 out of 3 rows deleted (idempotent - should not error)
	mock.ExpectExec("DELETE FROM `users` WHERE `id` IN").
		WithArgs(1, 2, 3).
		WillReturnResult(sqlmock.NewResult(0, 2))

	stats, err := dp.Delete(ctx, recordSet)

	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if stats.RowsDeleted != 2 {
		t.Errorf("Expected 2 rows deleted, got %d", stats.RowsDeleted)
	}
}

func TestDelete_ZeroRowsDeleted(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	g := createDeleteTestGraph()
	log := logger.NewDefault()
	dp, _ := NewDeletePhase(db, g, 500, log)

	recordSet := &RecordSet{
		RootPKs: []interface{}{1},
		Records: map[string][]interface{}{
			"users": {1},
		},
	}
	ctx := context.Background()

	// Zero rows deleted (idempotent - may have been deleted already)
	mock.ExpectExec("DELETE FROM `users` WHERE `id` IN").
		WithArgs(1).
		WillReturnResult(sqlmock.NewResult(0, 0))

	stats, err := dp.Delete(ctx, recordSet)

	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if stats.RowsDeleted != 0 {
		t.Errorf("Expected 0 rows deleted, got %d", stats.RowsDeleted)
	}
}

// ============================================================================
// Context Cancellation Tests
// ============================================================================

func TestDelete_ContextCancellation(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	g := createDeleteTestGraph()
	log := logger.NewDefault()
	dp, _ := NewDeletePhase(db, g, 500, log)

	recordSet := createDeleteRecordSet()

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := dp.Delete(ctx, recordSet)

	if err == nil {
		t.Error("Expected error for cancelled context")
	}
}

func TestDelete_ContextTimeout(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	g := createDeleteTestGraph()
	log := logger.NewDefault()
	dp, _ := NewDeletePhase(db, g, 500, log)

	recordSet := &RecordSet{
		RootPKs: []interface{}{1},
		Records: map[string][]interface{}{
			"users": {1},
		},
	}

	// Create context with very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// Small delay to ensure timeout
	time.Sleep(10 * time.Millisecond)

	// The query will be attempted but context is already cancelled
	mock.ExpectExec("DELETE FROM `users` WHERE `id` IN").
		WithArgs(1).
		WillReturnError(context.DeadlineExceeded)

	_, err := dp.Delete(ctx, recordSet)

	if err == nil {
		t.Error("Expected error for timeout context")
	}
}

// ============================================================================
// Delete Order Tests
// ============================================================================

func TestDelete_ReverseTopologicalOrder(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	g := createDeepDeleteGraph() // A -> B -> C -> D
	log := logger.NewDefault()
	dp, _ := NewDeletePhase(db, g, 500, log)

	recordSet := &RecordSet{
		RootPKs: []interface{}{1},
		Records: map[string][]interface{}{
			"A": {1},
			"B": {2},
			"C": {3},
			"D": {4},
		},
	}
	ctx := context.Background()

	// Delete order should be: D -> C -> B -> A (reverse of copy order)
	// D first (no children)
	mock.ExpectExec("DELETE FROM `D` WHERE `id` IN").
		WithArgs(4).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// C next
	mock.ExpectExec("DELETE FROM `C` WHERE `id` IN").
		WithArgs(3).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// B next
	mock.ExpectExec("DELETE FROM `B` WHERE `id` IN").
		WithArgs(2).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// A last (root)
	mock.ExpectExec("DELETE FROM `A` WHERE `id` IN").
		WithArgs(1).
		WillReturnResult(sqlmock.NewResult(0, 1))

	stats, err := dp.Delete(ctx, recordSet)

	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if stats.TablesProcessed != 4 {
		t.Errorf("Expected 4 tables processed, got %d", stats.TablesProcessed)
	}

	// Verify order in RowsPerTable
	// Note: We can't verify exact order from stats alone, but the test will fail
	// if the order is wrong due to mock expectation order
}

// ============================================================================
// Setter/Getter Tests
// ============================================================================

func TestDeletePhase_SetBatchSize(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	g := createDeleteTestGraph()
	log := logger.NewDefault()
	dp, _ := NewDeletePhase(db, g, 500, log)

	dp.SetBatchSize(1000)
	if dp.GetBatchSize() != 1000 {
		t.Errorf("Expected batch size 1000, got %d", dp.GetBatchSize())
	}

	// Test that 0 or negative doesn't change
	dp.SetBatchSize(0)
	if dp.GetBatchSize() != 1000 {
		t.Errorf("Batch size should not change with 0, got %d", dp.GetBatchSize())
	}

	dp.SetBatchSize(-1)
	if dp.GetBatchSize() != 1000 {
		t.Errorf("Batch size should not change with negative, got %d", dp.GetBatchSize())
	}
}

func TestDeletePhase_GetGraph(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	g := createDeleteTestGraph()
	log := logger.NewDefault()
	dp, _ := NewDeletePhase(db, g, 500, log)

	if dp.GetGraph() != g {
		t.Error("GetGraph returned wrong graph")
	}
}

func TestDeletePhase_SetLogger(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	g := createDeleteTestGraph()
	log := logger.NewDefault()
	dp, _ := NewDeletePhase(db, g, 500, log)

	newLog := logger.NewDefault()
	dp.SetLogger(newLog)

	if dp.logger != newLog {
		t.Error("SetLogger did not set logger correctly")
	}
}

// ============================================================================
// DeleteStats Tests
// ============================================================================

func TestDeleteStats_Populated(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	g := createDeleteTestGraph()
	log := logger.NewDefault()
	dp, _ := NewDeletePhase(db, g, 500, log)

	recordSet := createDeleteRecordSet()
	ctx := context.Background()

	mock.ExpectExec("DELETE FROM `order_items` WHERE `id` IN").
		WillReturnResult(sqlmock.NewResult(0, 12))
	mock.ExpectExec("DELETE FROM `orders` WHERE `id` IN").
		WillReturnResult(sqlmock.NewResult(0, 6))
	mock.ExpectExec("DELETE FROM `users` WHERE `id` IN").
		WillReturnResult(sqlmock.NewResult(0, 3))

	stats, err := dp.Delete(ctx, recordSet)

	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if stats.RowsPerTable == nil {
		t.Fatal("RowsPerTable should not be nil")
	}

	if len(stats.RowsPerTable) != 3 {
		t.Errorf("Expected 3 entries in RowsPerTable, got %d", len(stats.RowsPerTable))
	}
}

// ============================================================================
// Error Handling Tests
// ============================================================================

func TestDelete_RowsAffectedError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	g := createDeleteTestGraph()
	log := logger.NewDefault()
	dp, _ := NewDeletePhase(db, g, 500, log)

	recordSet := &RecordSet{
		RootPKs: []interface{}{1},
		Records: map[string][]interface{}{
			"users": {1},
		},
	}
	ctx := context.Background()

	// Return a result that will fail on RowsAffected
	mock.ExpectExec("DELETE FROM `users` WHERE `id` IN").
		WithArgs(1).
		WillReturnResult(sqlmock.NewErrorResult(errors.New("rows affected error")))

	_, err := dp.Delete(ctx, recordSet)

	if err == nil {
		t.Error("Expected error when RowsAffected fails")
	}
}

func TestDelete_DeleteOrderError(t *testing.T) {
	// This test verifies behavior when graph.DeleteOrder() fails
	// This would require a graph with a cycle, which should be detected earlier
	// but we'll test the error path

	db, _, _ := sqlmock.New()
	defer db.Close()

	// Create a graph that will fail during delete order
	g := graph.NewGraph("A", "id")
	// Add self-reference to create cycle
	g.AddEdge("A", "A")

	log := logger.NewDefault()
	dp, _ := NewDeletePhase(db, g, 500, log)

	recordSet := &RecordSet{
		RootPKs: []interface{}{1},
		Records: map[string][]interface{}{
			"A": {1},
		},
	}
	ctx := context.Background()

	_, err := dp.Delete(ctx, recordSet)

	if err == nil {
		t.Error("Expected error for cyclic graph")
	}
}
