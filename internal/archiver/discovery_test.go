package archiver

import (
	"context"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/internal/graph"
)

// ============================================================================
// Test Helpers
// ============================================================================

func createTestGraph() *graph.Graph {
	// Create a simple graph: users -> orders -> order_items
	//                          \-> profiles
	g := graph.NewGraph("users", "id")
	g.AddNode("orders", &graph.Node{Name: "orders", ForeignKey: "user_id", ReferenceKey: "id", DependencyType: "1-N"})
	g.AddNode("order_items", &graph.Node{Name: "order_items", ForeignKey: "order_id", ReferenceKey: "id", DependencyType: "1-N"})
	g.AddNode("profiles", &graph.Node{Name: "profiles", ForeignKey: "user_id", ReferenceKey: "id", DependencyType: "1-1"})
	g.AddEdge("users", "orders")
	g.AddEdge("orders", "order_items")
	g.AddEdge("users", "profiles")
	return g
}

func createDeepGraph() *graph.Graph {
	// Create a deep graph: A -> B -> C -> D -> E
	g := graph.NewGraph("A", "id")
	g.AddNode("B", &graph.Node{Name: "B"})
	g.AddNode("C", &graph.Node{Name: "C"})
	g.AddNode("D", &graph.Node{Name: "D"})
	g.AddNode("E", &graph.Node{Name: "E"})
	g.AddEdge("A", "B")
	g.AddEdge("B", "C")
	g.AddEdge("C", "D")
	g.AddEdge("D", "E")
	return g
}

// ============================================================================
// NewRecordDiscovery Tests
// ============================================================================

func TestNewRecordDiscovery_Success(t *testing.T) {
	g := createTestGraph()

	discovery, err := NewRecordDiscovery(g, nil, 100)
	if err != nil {
		t.Fatalf("NewRecordDiscovery failed: %v", err)
	}

	if discovery == nil {
		t.Fatal("NewRecordDiscovery returned nil")
	}

	if discovery.graph != g {
		t.Error("Graph mismatch")
	}

	if discovery.batchSize != 100 {
		t.Errorf("Expected batch size 100, got %d", discovery.batchSize)
	}
}

func TestNewRecordDiscovery_NilGraph(t *testing.T) {
	_, err := NewRecordDiscovery(nil, nil, 100)
	if err == nil {
		t.Error("Expected error for nil graph")
	}
}

func TestNewRecordDiscovery_DefaultBatchSize(t *testing.T) {
	g := createTestGraph()

	discovery, err := NewRecordDiscovery(g, nil, 0)
	if err != nil {
		t.Fatalf("NewRecordDiscovery failed: %v", err)
	}

	if discovery.batchSize != 1000 {
		t.Errorf("Expected default batch size 1000, got %d", discovery.batchSize)
	}
}

func TestNewRecordDiscovery_NegativeBatchSize(t *testing.T) {
	g := createTestGraph()

	discovery, err := NewRecordDiscovery(g, nil, -1)
	if err != nil {
		t.Fatalf("NewRecordDiscovery failed: %v", err)
	}

	if discovery.batchSize != 1000 {
		t.Errorf("Expected default batch size 1000 for negative input, got %d", discovery.batchSize)
	}
}

// ============================================================================
// Discover Tests
// ============================================================================

func TestDiscover_EmptyRootPKs(t *testing.T) {
	g := createTestGraph()
	discovery, _ := NewRecordDiscovery(g, nil, 100)

	ctx := context.Background()
	result, err := discovery.Discover(ctx, []interface{}{})

	if err != nil {
		t.Fatalf("Discover failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if len(result.RootPKs) != 0 {
		t.Errorf("Expected 0 root PKs, got %d", len(result.RootPKs))
	}

	if len(result.Records) != 0 {
		t.Errorf("Expected 0 records, got %d", len(result.Records))
	}

	if result.Stats.RecordsFound != 0 {
		t.Errorf("Expected 0 records found, got %d", result.Stats.RecordsFound)
	}
}

func TestDiscover_SingleRootPK(t *testing.T) {
	g := createTestGraph()
	discovery, _ := NewRecordDiscovery(g, nil, 100)

	ctx := context.Background()
	rootPKs := []interface{}{"user1"}
	result, err := discovery.Discover(ctx, rootPKs)

	if err != nil {
		t.Fatalf("Discover failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	// Should have root table
	if len(result.RootPKs) != 1 {
		t.Errorf("Expected 1 root PK, got %d", len(result.RootPKs))
	}

	// Should have discovered records for users, orders, order_items, profiles
	expectedTables := []string{"users", "orders", "order_items", "profiles"}
	for _, table := range expectedTables {
		if _, ok := result.Records[table]; !ok {
			t.Errorf("Expected records for table %s", table)
		}
	}
}

func TestDiscover_MultipleRootPKs(t *testing.T) {
	g := createTestGraph()
	discovery, _ := NewRecordDiscovery(g, nil, 100)

	ctx := context.Background()
	rootPKs := []interface{}{"user1", "user2", "user3"}
	result, err := discovery.Discover(ctx, rootPKs)

	if err != nil {
		t.Fatalf("Discover failed: %v", err)
	}

	if len(result.RootPKs) != 3 {
		t.Errorf("Expected 3 root PKs, got %d", len(result.RootPKs))
	}

	// All root PKs should be in users table
	if len(result.Records["users"]) != 3 {
		t.Errorf("Expected 3 users, got %d", len(result.Records["users"]))
	}
}

func TestDiscover_BFSTraversalOrder(t *testing.T) {
	g := createTestGraph()
	discovery, _ := NewRecordDiscovery(g, nil, 100)

	ctx := context.Background()
	rootPKs := []interface{}{"user1"}
	result, err := discovery.Discover(ctx, rootPKs)

	if err != nil {
		t.Fatalf("Discover failed: %v", err)
	}

	// Verify BFS levels
	// Level 0: users (root)
	// Level 1: orders, profiles (children of users)
	// Level 2: order_items (children of orders)

	if result.Stats.BFSLevels != 3 {
		t.Errorf("Expected 3 BFS levels, got %d", result.Stats.BFSLevels)
	}
}

func TestDiscover_DeepGraph(t *testing.T) {
	g := createDeepGraph() // A -> B -> C -> D -> E
	discovery, _ := NewRecordDiscovery(g, nil, 100)

	ctx := context.Background()
	rootPKs := []interface{}{"a1"}
	result, err := discovery.Discover(ctx, rootPKs)

	if err != nil {
		t.Fatalf("Discover failed: %v", err)
	}

	// Should traverse all 5 levels
	if result.Stats.BFSLevels != 5 {
		t.Errorf("Expected 5 BFS levels for deep graph, got %d", result.Stats.BFSLevels)
	}

	// Should have records for all tables
	expectedTables := []string{"A", "B", "C", "D", "E"}
	for _, table := range expectedTables {
		if _, ok := result.Records[table]; !ok {
			t.Errorf("Expected records for table %s", table)
		}
	}
}

func TestDiscover_RecordsPopulated(t *testing.T) {
	g := createTestGraph()
	discovery, _ := NewRecordDiscovery(g, nil, 100)

	ctx := context.Background()
	rootPKs := []interface{}{"user1"}
	result, err := discovery.Discover(ctx, rootPKs)

	if err != nil {
		t.Fatalf("Discover failed: %v", err)
	}

	// Verify records are populated
	if len(result.Records["users"]) == 0 {
		t.Error("Users should have records")
	}

	if len(result.Records["orders"]) == 0 {
		t.Error("Orders should have records")
	}

	if len(result.Records["profiles"]) == 0 {
		t.Error("Profiles should have records")
	}

	if len(result.Records["order_items"]) == 0 {
		t.Error("Order_items should have records")
	}
}

// ============================================================================
// Context Cancellation Tests
// ============================================================================

func TestDiscover_ContextCancellation(t *testing.T) {
	g := createTestGraph()
	discovery, _ := NewRecordDiscovery(g, nil, 100)

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	rootPKs := []interface{}{"user1"}
	_, err := discovery.Discover(ctx, rootPKs)

	if err == nil {
		t.Error("Expected error for cancelled context")
	}

	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

func TestDiscover_ContextTimeout(t *testing.T) {
	g := createDeepGraph()
	discovery, _ := NewRecordDiscovery(g, nil, 100)

	// Create context with very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// Small delay to ensure timeout
	time.Sleep(10 * time.Millisecond)

	rootPKs := []interface{}{"a1"}
	_, err := discovery.Discover(ctx, rootPKs)

	// Should get timeout or cancellation error
	if err == nil {
		// Timeout might not trigger if discovery is fast enough - that's ok
		t.Skip("Discovery completed before timeout")
	}
}

// ============================================================================
// DiscoverBatch Tests
// ============================================================================

func TestDiscoverBatch_Success(t *testing.T) {
	g := createTestGraph()
	discovery, _ := NewRecordDiscovery(g, nil, 100)

	ctx := context.Background()
	rootPKs := []interface{}{"user1", "user2", "user3"}
	result, err := discovery.DiscoverBatch(ctx, rootPKs)

	if err != nil {
		t.Fatalf("DiscoverBatch failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}
}

func TestDiscoverBatch_RespectsBatchSize(t *testing.T) {
	g := createTestGraph()
	batchSize := 2
	discovery, _ := NewRecordDiscovery(g, nil, batchSize)

	ctx := context.Background()
	rootPKs := []interface{}{"user1", "user2", "user3", "user4", "user5"}
	result, err := discovery.DiscoverBatch(ctx, rootPKs)

	if err != nil {
		t.Fatalf("DiscoverBatch failed: %v", err)
	}

	// Should only process batchSize root PKs
	if len(result.RootPKs) != batchSize {
		t.Errorf("Expected %d root PKs (batch size), got %d", batchSize, len(result.RootPKs))
	}

	if len(result.Records["users"]) != batchSize {
		t.Errorf("Expected %d users (batch size), got %d", batchSize, len(result.Records["users"]))
	}
}

func TestDiscoverBatch_EmptyPKs(t *testing.T) {
	g := createTestGraph()
	discovery, _ := NewRecordDiscovery(g, nil, 100)

	ctx := context.Background()
	result, err := discovery.DiscoverBatch(ctx, []interface{}{})

	if err != nil {
		t.Fatalf("DiscoverBatch failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if len(result.RootPKs) != 0 {
		t.Errorf("Expected 0 root PKs, got %d", len(result.RootPKs))
	}
}

// ============================================================================
// DiscoveryStats Tests
// ============================================================================

func TestDiscoveryStats_Populated(t *testing.T) {
	g := createTestGraph()
	discovery, _ := NewRecordDiscovery(g, nil, 100)

	ctx := context.Background()
	rootPKs := []interface{}{"user1"}
	result, err := discovery.Discover(ctx, rootPKs)

	if err != nil {
		t.Fatalf("Discover failed: %v", err)
	}

	// TablesScanned should be 4 (users, orders, order_items, profiles)
	if result.Stats.TablesScanned != 4 {
		t.Errorf("Expected 4 tables scanned, got %d", result.Stats.TablesScanned)
	}

	// RecordsFound should be > 0
	if result.Stats.RecordsFound == 0 {
		t.Error("Expected records found > 0")
	}

	// BFSLevels should be 3
	if result.Stats.BFSLevels != 3 {
		t.Errorf("Expected 3 BFS levels, got %d", result.Stats.BFSLevels)
	}

	// Duration should be > 0
	if result.Stats.Duration <= 0 {
		t.Error("Expected duration > 0")
	}
}

func TestDiscoveryStats_MultipleRootPKs(t *testing.T) {
	g := createTestGraph()
	discovery, _ := NewRecordDiscovery(g, nil, 100)

	ctx := context.Background()
	rootPKs := []interface{}{"user1", "user2", "user3"}
	result, err := discovery.Discover(ctx, rootPKs)

	if err != nil {
		t.Fatalf("Discover failed: %v", err)
	}

	// TablesScanned should still be 4
	if result.Stats.TablesScanned != 4 {
		t.Errorf("Expected 4 tables scanned, got %d", result.Stats.TablesScanned)
	}

	// RecordsFound should be proportional to number of root PKs
	// With 3 users, each having orders and profiles, and orders having items
	expectedMinRecords := 3 + 3 + 3 + 6 // users + profiles + orders + order_items (at least)
	if result.Stats.RecordsFound < int64(expectedMinRecords) {
		t.Errorf("Expected at least %d records, got %d", expectedMinRecords, result.Stats.RecordsFound)
	}
}

func TestDiscoveryStats_EmptyDiscovery(t *testing.T) {
	g := createTestGraph()
	discovery, _ := NewRecordDiscovery(g, nil, 100)

	ctx := context.Background()
	result, err := discovery.Discover(ctx, []interface{}{})

	if err != nil {
		t.Fatalf("Discover failed: %v", err)
	}

	if result.Stats.TablesScanned != 0 {
		t.Errorf("Expected 0 tables scanned, got %d", result.Stats.TablesScanned)
	}

	if result.Stats.RecordsFound != 0 {
		t.Errorf("Expected 0 records found, got %d", result.Stats.RecordsFound)
	}

	if result.Stats.BFSLevels != 0 {
		t.Errorf("Expected 0 BFS levels, got %d", result.Stats.BFSLevels)
	}
}

// ============================================================================
// RecordSet Tests
// ============================================================================

func TestRecordSet_ContainsAllTables(t *testing.T) {
	g := createTestGraph()
	discovery, _ := NewRecordDiscovery(g, nil, 100)

	ctx := context.Background()
	rootPKs := []interface{}{"user1"}
	result, err := discovery.Discover(ctx, rootPKs)

	if err != nil {
		t.Fatalf("Discover failed: %v", err)
	}

	// Should contain all 4 tables
	expectedTables := map[string]bool{
		"users":       false,
		"orders":      false,
		"order_items": false,
		"profiles":    false,
	}

	for table := range result.Records {
		if _, ok := expectedTables[table]; !ok {
			t.Errorf("Unexpected table %s in results", table)
		}
		expectedTables[table] = true
	}

	for table, found := range expectedTables {
		if !found {
			t.Errorf("Expected table %s not found in results", table)
		}
	}
}

func TestRecordSet_RootPKsMatch(t *testing.T) {
	g := createTestGraph()
	discovery, _ := NewRecordDiscovery(g, nil, 100)

	ctx := context.Background()
	rootPKs := []interface{}{"user1", "user2", "user3"}
	result, err := discovery.Discover(ctx, rootPKs)

	if err != nil {
		t.Fatalf("Discover failed: %v", err)
	}

	// RootPKs should match input
	if len(result.RootPKs) != len(rootPKs) {
		t.Errorf("RootPKs length mismatch: expected %d, got %d", len(rootPKs), len(result.RootPKs))
	}

	// Root table records should match root PKs
	if len(result.Records["users"]) != len(rootPKs) {
		t.Errorf("Users records length mismatch: expected %d, got %d", len(rootPKs), len(result.Records["users"]))
	}
}

// ============================================================================
// Helper Method Tests
// ============================================================================

func TestDiscoveryGetGraph(t *testing.T) {
	g := createTestGraph()
	discovery, _ := NewRecordDiscovery(g, nil, 100)

	if discovery.GetGraph() != g {
		t.Error("GetGraph returned wrong graph")
	}
}

func TestGetBatchSize(t *testing.T) {
	g := createTestGraph()
	discovery, _ := NewRecordDiscovery(g, nil, 500)

	if discovery.GetBatchSize() != 500 {
		t.Errorf("Expected batch size 500, got %d", discovery.GetBatchSize())
	}
}

func TestSetLogger(t *testing.T) {
	g := createTestGraph()
	discovery, _ := NewRecordDiscovery(g, nil, 100)

	// Get default logger
	originalLogger := discovery.logger

	// Set new logger (use the same one for test)
	discovery.SetLogger(originalLogger)

	if discovery.logger != originalLogger {
		t.Error("SetLogger did not set logger correctly")
	}
}

// ============================================================================
// Simulate Discovery Tests
// ============================================================================

func TestSimulateDiscovery_Orders(t *testing.T) {
	g := createTestGraph()
	discovery, _ := NewRecordDiscovery(g, nil, 100)

	parentPKs := []interface{}{"user1", "user2", "user3"}
	childPKs := discovery.simulateDiscovery("orders", parentPKs)

	// Each user should have at least 1 order, some have 2
	if len(childPKs) < len(parentPKs) {
		t.Errorf("Expected at least %d orders, got %d", len(parentPKs), len(childPKs))
	}

	// Check naming pattern
	for _, pk := range childPKs {
		pkStr, ok := pk.(string)
		if !ok {
			t.Error("PK should be a string")
			continue
		}
		if len(pkStr) < 6 || pkStr[:6] != "order_" {
			t.Errorf("Expected order_ prefix, got %s", pkStr)
		}
	}
}

func TestSimulateDiscovery_Profiles(t *testing.T) {
	g := createTestGraph()
	discovery, _ := NewRecordDiscovery(g, nil, 100)

	parentPKs := []interface{}{"user1", "user2", "user3"}
	childPKs := discovery.simulateDiscovery("profiles", parentPKs)

	// Each user should have exactly 1 profile
	if len(childPKs) != len(parentPKs) {
		t.Errorf("Expected %d profiles, got %d", len(parentPKs), len(childPKs))
	}
}

func TestSimulateDiscovery_UnknownTable(t *testing.T) {
	g := createTestGraph()
	discovery, _ := NewRecordDiscovery(g, nil, 100)

	parentPKs := []interface{}{"user1"}
	childPKs := discovery.simulateDiscovery("unknown_table", parentPKs)

	// Unknown table should return empty
	if len(childPKs) != 0 {
		t.Errorf("Expected 0 records for unknown table, got %d", len(childPKs))
	}
}

// ============================================================================
// Integration Tests
// ============================================================================

func TestDiscovery_FullWorkflow(t *testing.T) {
	g := createTestGraph()
	discovery, err := NewRecordDiscovery(g, nil, 1000)
	if err != nil {
		t.Fatalf("NewRecordDiscovery failed: %v", err)
	}

	ctx := context.Background()
	rootPKs := []interface{}{"user1", "user2", "user3", "user4", "user5"}
	result, err := discovery.Discover(ctx, rootPKs)

	if err != nil {
		t.Fatalf("Discover failed: %v", err)
	}

	// Verify all expected tables
	expectedTables := []string{"users", "orders", "order_items", "profiles"}
	for _, table := range expectedTables {
		if _, ok := result.Records[table]; !ok {
			t.Errorf("Missing table %s", table)
		}
	}

	// Verify stats
	if result.Stats.TablesScanned != len(expectedTables) {
		t.Errorf("Tables scanned mismatch: expected %d, got %d", len(expectedTables), result.Stats.TablesScanned)
	}

	if result.Stats.RecordsFound == 0 {
		t.Error("Expected records found > 0")
	}

	if result.Stats.BFSLevels != 3 {
		t.Errorf("Expected 3 BFS levels, got %d", result.Stats.BFSLevels)
	}
}

func TestDiscovery_BatchVsFull(t *testing.T) {
	g := createTestGraph()
	discovery, _ := NewRecordDiscovery(g, nil, 2) // Batch size 2

	ctx := context.Background()
	rootPKs := []interface{}{"user1", "user2", "user3", "user4"}

	// Full discovery
	fullResult, err := discovery.Discover(ctx, rootPKs)
	if err != nil {
		t.Fatalf("Discover failed: %v", err)
	}

	// Batch discovery (should only process 2)
	batchResult, err := discovery.DiscoverBatch(ctx, rootPKs)
	if err != nil {
		t.Fatalf("DiscoverBatch failed: %v", err)
	}

	// Full should have more records
	if len(fullResult.RootPKs) <= len(batchResult.RootPKs) {
		t.Error("Full discovery should have more root PKs than batch")
	}

	if fullResult.Stats.RecordsFound <= batchResult.Stats.RecordsFound {
		t.Error("Full discovery should have more total records than batch")
	}
}
