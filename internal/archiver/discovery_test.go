package archiver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
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
	// FK/ref names match the Node metadata already declared above.
	g.AddEdgeWithMeta("users", "orders", "user_id", "id", "1-N")
	g.AddEdgeWithMeta("orders", "order_items", "order_id", "id", "1-N")
	g.AddEdgeWithMeta("users", "profiles", "user_id", "id", "1-1")
	return g
}

func createDeepGraph() *graph.Graph {
	// Create a deep graph: A -> B -> C -> D -> E
	g := graph.NewGraph("A", "id")
	g.AddNode("B", &graph.Node{Name: "B"})
	g.AddNode("C", &graph.Node{Name: "C"})
	g.AddNode("D", &graph.Node{Name: "D"})
	g.AddNode("E", &graph.Node{Name: "E"})
	// Synthetic FK names; only edge presence + a non-empty FK column matter
	// to the mocked queries.
	g.AddEdgeWithMeta("A", "B", "a_id", "id", "1-N")
	g.AddEdgeWithMeta("B", "C", "b_id", "id", "1-N")
	g.AddEdgeWithMeta("C", "D", "c_id", "id", "1-N")
	g.AddEdgeWithMeta("D", "E", "d_id", "id", "1-N")
	return g
}

func createDiamondGraph() *graph.Graph {
	// Diamond graph:
	// A -> B -> D
	// A -> C -> D
	g := graph.NewGraph("A", "id")
	g.AddNode("B", &graph.Node{Name: "B"})
	g.AddNode("C", &graph.Node{Name: "C"})
	g.AddNode("D", &graph.Node{Name: "D"})
	g.AddEdgeWithMeta("A", "B", "a_id", "id", "1-N")
	g.AddEdgeWithMeta("A", "C", "a_id", "id", "1-N")
	g.AddEdgeWithMeta("B", "D", "b_id", "id", "1-N")
	g.AddEdgeWithMeta("C", "D", "c_id", "id", "1-N")
	return g
}

// simulatedChildren reproduces the deleted simulateDiscovery fixtures, moved
// out of the production binary: synthetic child PKs derived from parent PKs.
func simulatedChildren(childTable string, parentPKs []interface{}) []interface{} {
	if len(parentPKs) == 0 {
		return []interface{}{}
	}
	switch childTable {
	case "orders":
		childPKs := []interface{}{}
		for i, parentPK := range parentPKs {
			numOrders := 1 + (i % 2)
			for j := 0; j < numOrders; j++ {
				childPKs = append(childPKs, fmt.Sprintf("order_%v_%d", parentPK, j+1))
			}
		}
		return childPKs
	case "profiles":
		childPKs := []interface{}{}
		for _, parentPK := range parentPKs {
			childPKs = append(childPKs, fmt.Sprintf("profile_%v", parentPK))
		}
		return childPKs
	case "order_items":
		childPKs := []interface{}{}
		for i, parentPK := range parentPKs {
			numItems := 2 + (i % 2)
			for j := 0; j < numItems; j++ {
				childPKs = append(childPKs, fmt.Sprintf("item_%v_%d", parentPK, j+1))
			}
		}
		return childPKs
	case "unknown_table":
		return []interface{}{}
	default:
		childPKs := []interface{}{}
		for i, parentPK := range parentPKs {
			numChildren := 1 + (i % 2)
			for j := 0; j < numChildren; j++ {
				childPKs = append(childPKs, fmt.Sprintf("%s_child_%v_%d", childTable, parentPK, j+1))
			}
		}
		return childPKs
	}
}

// newSimulatedRecordDiscovery returns a RecordDiscovery backed by a sqlmock DB
// pre-programmed to answer the exact BFS walk Discover will perform for
// rootPKs, returning the same synthetic children the old in-binary simulation
// produced. rootPKs must match what the test passes to Discover.
func newSimulatedRecordDiscovery(t *testing.T, g *graph.Graph, batchSize int, rootPKs ...interface{}) *RecordDiscovery {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	effectiveBatch := batchSize
	if effectiveBatch <= 0 {
		effectiveBatch = 1000 // NewRecordDiscovery default
	}

	// Mirror Discover's traversal: topological order, children per table,
	// chunked queries, dedup accumulation — programming one expectation per
	// (edge, chunk) in the order the queries will be issued.
	if len(rootPKs) > 0 {
		records := map[string][]interface{}{g.Root: rootPKs}
		seen := map[string]map[interface{}]struct{}{}
		order, err := g.CopyOrder()
		if err != nil {
			t.Fatalf("CopyOrder failed: %v", err)
		}
		for _, table := range order {
			parents := records[table]
			if len(parents) == 0 {
				continue
			}
			for _, child := range g.GetChildren(table) {
				childPK := g.GetPK(child)
				for i := 0; i < len(parents); i += effectiveBatch {
					end := i + effectiveBatch
					if end > len(parents) {
						end = len(parents)
					}
					chunkChildren := simulatedChildren(child, parents[i:end])
					rows := sqlmock.NewRows([]string{childPK})
					for _, pk := range chunkChildren {
						rows.AddRow(pk)
					}
					mock.ExpectQuery("SELECT .+ FROM `" + child + "` WHERE").WillReturnRows(rows)
				}
				all := simulatedChildren(child, parents)
				set := tableSeen(seen, records[child], child)
				records[child] = appendUnique(records[child], all, set)
			}
		}
	}

	// Every programmed expectation must be consumed — this is what proves the
	// mirror-walk matches Discover's real query sequence (reviewer requirement).
	t.Cleanup(func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet sqlmock expectations: %v", err)
		}
	})

	discovery, err := NewRecordDiscovery(g, db, batchSize)
	if err != nil {
		t.Fatalf("NewRecordDiscovery failed: %v", err)
	}
	return discovery
}

// ============================================================================
// NewRecordDiscovery Tests
// ============================================================================

func TestNewRecordDiscovery_Success(t *testing.T) {
	g := createTestGraph()

	discovery := newSimulatedRecordDiscovery(t, g, 100)

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

	discovery := newSimulatedRecordDiscovery(t, g, 0)

	if discovery.batchSize != 1000 {
		t.Errorf("Expected default batch size 1000, got %d", discovery.batchSize)
	}
}

func TestNewRecordDiscovery_NegativeBatchSize(t *testing.T) {
	g := createTestGraph()

	discovery := newSimulatedRecordDiscovery(t, g, -1)

	if discovery.batchSize != 1000 {
		t.Errorf("Expected default batch size 1000 for negative input, got %d", discovery.batchSize)
	}
}

// ============================================================================
// Discover Tests
// ============================================================================

func TestDiscover_EmptyRootPKs(t *testing.T) {
	g := createTestGraph()
	discovery := newSimulatedRecordDiscovery(t, g, 100)

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
	discovery := newSimulatedRecordDiscovery(t, g, 100, "user1")

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

func TestDiscover_NilDBErrorsForNonEmptyInput(t *testing.T) {
	g := createTestGraph()
	discovery, err := NewRecordDiscovery(g, nil, 100)
	if err != nil {
		t.Fatalf("NewRecordDiscovery failed: %v", err)
	}

	_, err = discovery.Discover(context.Background(), []interface{}{"user1"})
	if err == nil {
		t.Fatal("expected nil DB discovery to fail")
	}
}

func TestDiscover_MultipleRootPKs(t *testing.T) {
	g := createTestGraph()
	discovery := newSimulatedRecordDiscovery(t, g, 100, "user1", "user2", "user3")

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
	discovery := newSimulatedRecordDiscovery(t, g, 100, "user1")

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
	discovery := newSimulatedRecordDiscovery(t, g, 100, "a1")

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

func TestDiscover_DiamondDependencyAccumulatesAllPaths(t *testing.T) {
	g := createDiamondGraph()
	discovery := newSimulatedRecordDiscovery(t, g, 100, "a1", "a2")

	ctx := context.Background()
	rootPKs := []interface{}{"a1", "a2"}
	result, err := discovery.Discover(ctx, rootPKs)
	if err != nil {
		t.Fatalf("Discover failed: %v", err)
	}

	if _, ok := result.Records["D"]; !ok {
		t.Fatal("Expected records for table D")
	}

	// In simulation mode:
	// B generates 3 records from 2 parents, C generates 3 records from 2 parents.
	// D should be discovered from both paths, so expect 4-6 records (deterministic here: 4).
	// More importantly, D count must be strictly greater than only one parent path contribution.
	if len(result.Records["D"]) <= len(result.Records["B"]) {
		t.Fatalf("Expected D to include records from both B and C paths; got D=%d, B=%d",
			len(result.Records["D"]), len(result.Records["B"]))
	}
}

func TestDiscover_RecordsPopulated(t *testing.T) {
	g := createTestGraph()
	discovery := newSimulatedRecordDiscovery(t, g, 100, "user1")

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
	discovery := newSimulatedRecordDiscovery(t, g, 100)

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
	discovery := newSimulatedRecordDiscovery(t, g, 100)

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
// DiscoveryStats Tests
// ============================================================================

func TestDiscoveryStats_Populated(t *testing.T) {
	g := createTestGraph()
	discovery := newSimulatedRecordDiscovery(t, g, 100, "user1")

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
	discovery := newSimulatedRecordDiscovery(t, g, 100, "user1", "user2", "user3")

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
	discovery := newSimulatedRecordDiscovery(t, g, 100)

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
	discovery := newSimulatedRecordDiscovery(t, g, 100, "user1")

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
	discovery := newSimulatedRecordDiscovery(t, g, 100, "user1", "user2", "user3")

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

func TestSetLogger(t *testing.T) {
	g := createTestGraph()
	discovery := newSimulatedRecordDiscovery(t, g, 100)

	// Get default logger
	originalLogger := discovery.logger

	// Set new logger (use the same one for test)
	discovery.SetLogger(originalLogger)

	if discovery.logger != originalLogger {
		t.Error("SetLogger did not set logger correctly")
	}
}

// ============================================================================
// Integration Tests
// ============================================================================

func TestDiscovery_FullWorkflow(t *testing.T) {
	g := createTestGraph()
	discovery := newSimulatedRecordDiscovery(t, g, 1000, "user1", "user2", "user3", "user4", "user5")

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

func TestAppendUnique_DedupsAcrossCalls(t *testing.T) {
	seen := make(map[interface{}]struct{})
	got := appendUnique(nil, []interface{}{int64(1), int64(2), int64(1)}, seen)
	got = appendUnique(got, []interface{}{int64(2), int64(3)}, seen)
	want := []interface{}{int64(1), int64(2), int64(3)}
	if len(got) != len(want) {
		t.Fatalf("appendUnique = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("appendUnique[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestAppendUnique_TypeDistinguishesKeys(t *testing.T) {
	// int64(1) and "1" are distinct PKs, exactly as the old "%T:%v" keys were.
	seen := make(map[interface{}]struct{})
	got := appendUnique(nil, []interface{}{int64(1), "1"}, seen)
	if len(got) != 2 {
		t.Fatalf("expected int64(1) and \"1\" to be distinct, got %v", got)
	}
}

func TestTableSeen_SeedsFromExistingRecords(t *testing.T) {
	// If a table already has recorded PKs (the root table), the first dedup
	// set for it must be seeded from them or duplicates would slip through.
	seen := make(map[string]map[interface{}]struct{})
	existing := []interface{}{int64(10), int64(20)}
	set := tableSeen(seen, existing, "users")
	got := appendUnique(existing, []interface{}{int64(10), int64(30)}, set)
	if len(got) != 3 {
		t.Fatalf("expected seeded dedup to yield 3 entries, got %v", got)
	}
	if same := tableSeen(seen, nil, "users"); len(same) != 3 {
		t.Fatalf("expected persistent set with 3 keys on second call, got %d", len(same))
	}
}
