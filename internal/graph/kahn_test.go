package graph

import (
	"errors"
	"reflect"
	"sort"
	"testing"
)

func TestCalculateInDegrees_SingleRelation(t *testing.T) {
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders", ForeignKey: "user_id", ReferenceKey: "id", DependencyType: "1-N"})
	g.AddEdge("users", "orders")

	inDegrees := g.CalculateInDegrees()

	// Root has in-degree 0
	if inDegrees["users"] != 0 {
		t.Errorf("Expected users in-degree 0, got %d", inDegrees["users"])
	}
	// Child has in-degree 1
	if inDegrees["orders"] != 1 {
		t.Errorf("Expected orders in-degree 1, got %d", inDegrees["orders"])
	}
}

func TestCalculateInDegrees_MultipleRelations(t *testing.T) {
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders", ForeignKey: "user_id", ReferenceKey: "id", DependencyType: "1-N"})
	g.AddNode("profiles", &Node{Name: "profiles", ForeignKey: "user_id", ReferenceKey: "id", DependencyType: "1-1"})
	g.AddNode("sessions", &Node{Name: "sessions", ForeignKey: "user_id", ReferenceKey: "id", DependencyType: "1-N"})
	g.AddEdge("users", "orders")
	g.AddEdge("users", "profiles")
	g.AddEdge("users", "sessions")

	inDegrees := g.CalculateInDegrees()

	// Root has in-degree 0
	if inDegrees["users"] != 0 {
		t.Errorf("Expected users in-degree 0, got %d", inDegrees["users"])
	}
	// All children have in-degree 1
	if inDegrees["orders"] != 1 {
		t.Errorf("Expected orders in-degree 1, got %d", inDegrees["orders"])
	}
	if inDegrees["profiles"] != 1 {
		t.Errorf("Expected profiles in-degree 1, got %d", inDegrees["profiles"])
	}
	if inDegrees["sessions"] != 1 {
		t.Errorf("Expected sessions in-degree 1, got %d", inDegrees["sessions"])
	}
}

func TestCalculateInDegrees_NestedRelations(t *testing.T) {
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders", ForeignKey: "user_id", ReferenceKey: "id", DependencyType: "1-N"})
	g.AddNode("order_items", &Node{Name: "order_items", ForeignKey: "order_id", ReferenceKey: "id", DependencyType: "1-N"})
	g.AddEdge("users", "orders")
	g.AddEdge("orders", "order_items")

	inDegrees := g.CalculateInDegrees()

	// Root has in-degree 0
	if inDegrees["users"] != 0 {
		t.Errorf("Expected users in-degree 0, got %d", inDegrees["users"])
	}
	// orders has in-degree 1 (from users)
	if inDegrees["orders"] != 1 {
		t.Errorf("Expected orders in-degree 1, got %d", inDegrees["orders"])
	}
	// order_items has in-degree 1 (from orders)
	if inDegrees["order_items"] != 1 {
		t.Errorf("Expected order_items in-degree 1, got %d", inDegrees["order_items"])
	}
}

func TestCalculateInDegrees_DeepNesting(t *testing.T) {
	g := NewGraph("level1", "id")
	g.AddNode("level2", &Node{Name: "level2"})
	g.AddNode("level3", &Node{Name: "level3"})
	g.AddNode("level4", &Node{Name: "level4"})
	g.AddNode("level5", &Node{Name: "level5"})
	g.AddEdge("level1", "level2")
	g.AddEdge("level2", "level3")
	g.AddEdge("level3", "level4")
	g.AddEdge("level4", "level5")

	inDegrees := g.CalculateInDegrees()

	// level1 (root) has in-degree 0
	if inDegrees["level1"] != 0 {
		t.Errorf("Expected level1 in-degree 0, got %d", inDegrees["level1"])
	}
	// All others have in-degree 1
	for i := 2; i <= 5; i++ {
		nodeName := "level" + string(rune('0'+i))
		if inDegrees[nodeName] != 1 {
			t.Errorf("Expected %s in-degree 1, got %d", nodeName, inDegrees[nodeName])
		}
	}
}

func TestCalculateInDegrees_MultiParent(t *testing.T) {
	// Create a graph where one child has multiple parents
	// This shouldn't happen in our tree structure but test it anyway
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddNode("shared_table", &Node{Name: "shared_table"})
	g.AddEdge("users", "orders")
	g.AddEdge("users", "shared_table")
	// Manually add another parent relationship
	g.Parents["shared_table"] = append(g.Parents["shared_table"], "orders")
	// Note: We need to also add to Children for in-degree calculation
	g.Children["orders"] = append(g.Children["orders"], "shared_table")

	inDegrees := g.CalculateInDegrees()

	// Root has in-degree 0
	if inDegrees["users"] != 0 {
		t.Errorf("Expected users in-degree 0, got %d", inDegrees["users"])
	}
	// orders has in-degree 1
	if inDegrees["orders"] != 1 {
		t.Errorf("Expected orders in-degree 1, got %d", inDegrees["orders"])
	}
	// shared_table has in-degree 2 (from both users and orders)
	if inDegrees["shared_table"] != 2 {
		t.Errorf("Expected shared_table in-degree 2, got %d", inDegrees["shared_table"])
	}
}

func TestCalculateInDegrees_EmptyGraph(t *testing.T) {
	g := NewGraph("users", "id")
	// No additional nodes or edges

	inDegrees := g.CalculateInDegrees()

	// Should have only root with in-degree 0
	if len(inDegrees) != 1 {
		t.Errorf("Expected 1 entry in in-degrees map, got %d", len(inDegrees))
	}
	if inDegrees["users"] != 0 {
		t.Errorf("Expected users in-degree 0, got %d", inDegrees["users"])
	}
}

func TestCalculateInDegrees_EmptyMap(t *testing.T) {
	// Create graph without any nodes (just the struct, not using NewGraph)
	g := &Graph{
		Nodes:    make(map[string]*Node),
		Children: make(map[string][]string),
		Parents:  make(map[string][]string),
	}

	inDegrees := g.CalculateInDegrees()

	// Should return empty map
	if len(inDegrees) != 0 {
		t.Errorf("Expected empty in-degrees map, got %d entries", len(inDegrees))
	}
}

func TestCalculateInDegrees_ComplexGraph(t *testing.T) {
	// Build a complex graph:
	// users -> orders -> order_items
	//     \          -> shipments
	//      -> profiles
	//      -> sessions
	g := NewGraph("users", "id")

	// Level 1
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddNode("profiles", &Node{Name: "profiles"})
	g.AddNode("sessions", &Node{Name: "sessions"})
	g.AddEdge("users", "orders")
	g.AddEdge("users", "profiles")
	g.AddEdge("users", "sessions")

	// Level 2 (children of orders)
	g.AddNode("order_items", &Node{Name: "order_items"})
	g.AddNode("shipments", &Node{Name: "shipments"})
	g.AddEdge("orders", "order_items")
	g.AddEdge("orders", "shipments")

	inDegrees := g.CalculateInDegrees()

	// Verify counts
	expected := map[string]int{
		"users":       0,
		"orders":      1,
		"profiles":    1,
		"sessions":    1,
		"order_items": 1,
		"shipments":   1,
	}

	for node, expectedDegree := range expected {
		if inDegrees[node] != expectedDegree {
			t.Errorf("Expected %s in-degree %d, got %d", node, expectedDegree, inDegrees[node])
		}
	}
}

func TestCalculateInDegrees_ReturnsMapForAllNodes(t *testing.T) {
	g := NewGraph("root", "id")
	g.AddNode("child1", &Node{Name: "child1"})
	g.AddNode("child2", &Node{Name: "child2"})
	g.AddNode("child3", &Node{Name: "child3"})
	g.AddEdge("root", "child1")
	g.AddEdge("root", "child2")
	g.AddEdge("root", "child3")

	inDegrees := g.CalculateInDegrees()

	// Map should contain all nodes
	if len(inDegrees) != 4 {
		t.Errorf("Expected 4 entries in map, got %d", len(inDegrees))
	}

	// Verify all nodes are present
	expectedNodes := []string{"root", "child1", "child2", "child3"}
	for _, node := range expectedNodes {
		if _, exists := inDegrees[node]; !exists {
			t.Errorf("Node %s not found in in-degrees map", node)
		}
	}
}

func TestGetZeroInDegreeNodes_Single(t *testing.T) {
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddEdge("users", "orders")

	inDegrees := g.CalculateInDegrees()
	zeroNodes := g.GetZeroInDegreeNodes(inDegrees)

	// Should return only root
	if len(zeroNodes) != 1 || zeroNodes[0] != "users" {
		t.Errorf("Expected [users], got %v", zeroNodes)
	}
}

func TestGetZeroInDegreeNodes_Multiple(t *testing.T) {
	// This is an unusual case for our tree structure but test it
	g := NewGraph("root1", "id")
	g.AddNode("root2", &Node{Name: "root2"})
	g.AddNode("child", &Node{Name: "child"})
	g.AddEdge("root1", "child")
	g.AddEdge("root2", "child")

	inDegrees := g.CalculateInDegrees()
	zeroNodes := g.GetZeroInDegreeNodes(inDegrees)

	// Should return both roots
	if len(zeroNodes) != 2 {
		t.Errorf("Expected 2 zero in-degree nodes, got %d: %v", len(zeroNodes), zeroNodes)
	}

	sort.Strings(zeroNodes)
	if zeroNodes[0] != "root1" || zeroNodes[1] != "root2" {
		t.Errorf("Expected [root1, root2], got %v", zeroNodes)
	}
}

func TestGetZeroInDegreeNodes_None(t *testing.T) {
	// Create a simple cycle (not possible with builder but test directly)
	g := &Graph{
		Nodes: map[string]*Node{
			"a": {Name: "a"},
			"b": {Name: "b"},
		},
		Children: map[string][]string{
			"a": {"b"},
			"b": {"a"},
		},
		Parents: map[string][]string{
			"a": {"b"},
			"b": {"a"},
		},
	}

	inDegrees := g.CalculateInDegrees()
	zeroNodes := g.GetZeroInDegreeNodes(inDegrees)

	// Should return empty (cycle - no starting point)
	if len(zeroNodes) != 0 {
		t.Errorf("Expected 0 zero in-degree nodes, got %d: %v", len(zeroNodes), zeroNodes)
	}
}

func TestGetZeroInDegreeNodes_EmptyGraph(t *testing.T) {
	g := NewGraph("users", "id")
	inDegrees := g.CalculateInDegrees()
	zeroNodes := g.GetZeroInDegreeNodes(inDegrees)

	// Should return root
	if len(zeroNodes) != 1 || zeroNodes[0] != "users" {
		t.Errorf("Expected [users], got %v", zeroNodes)
	}
}

func TestGetZeroInDegreeNodes_EmptyMap(t *testing.T) {
	g := NewGraph("users", "id")
	zeroNodes := g.GetZeroInDegreeNodes(map[string]int{})

	// Should return empty
	if len(zeroNodes) != 0 {
		t.Errorf("Expected empty result for empty map, got %v", zeroNodes)
	}
}

func TestKahnIntegration(t *testing.T) {
	// Integration test: typical Kahn's algorithm first step
	// Build graph, calculate in-degrees, get starting nodes
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddNode("order_items", &Node{Name: "order_items"})
	g.AddNode("profiles", &Node{Name: "profiles"})
	g.AddEdge("users", "orders")
	g.AddEdge("orders", "order_items")
	g.AddEdge("users", "profiles")

	// Step 1: Calculate in-degrees
	inDegrees := g.CalculateInDegrees()

	// Step 2: Get zero in-degree nodes
	zeroNodes := g.GetZeroInDegreeNodes(inDegrees)

	// In Kahn's algorithm, we start with nodes that have no dependencies
	if len(zeroNodes) != 1 || zeroNodes[0] != "users" {
		t.Errorf("Expected [users] as starting node, got %v", zeroNodes)
	}

	// Verify all in-degrees are correct for the algorithm
	expected := map[string]int{
		"users":       0,
		"orders":      1,
		"order_items": 1,
		"profiles":    1,
	}

	if !reflect.DeepEqual(inDegrees, expected) {
		t.Errorf("In-degrees mismatch.\nExpected: %v\nGot: %v", expected, inDegrees)
	}
}

func TestCalculateInDegrees_DoesNotModifyGraph(t *testing.T) {
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddEdge("users", "orders")

	// Store original state
	originalNodeCount := g.NodeCount()
	originalEdgeCount := g.EdgeCount()

	// Calculate in-degrees multiple times
	_ = g.CalculateInDegrees()
	_ = g.CalculateInDegrees()

	// Verify graph is unchanged
	if g.NodeCount() != originalNodeCount {
		t.Error("CalculateInDegrees modified graph node count")
	}
	if g.EdgeCount() != originalEdgeCount {
		t.Error("CalculateInDegrees modified graph edge count")
	}
}

func TestCalculateInDegrees_IsolatedNodes(t *testing.T) {
	// Nodes with no edges should have in-degree 0
	g := NewGraph("users", "id")
	g.AddNode("orphan1", &Node{Name: "orphan1"})
	g.AddNode("orphan2", &Node{Name: "orphan2"})
	// Don't add any edges

	inDegrees := g.CalculateInDegrees()

	// All nodes should have in-degree 0
	if inDegrees["users"] != 0 {
		t.Errorf("Expected users in-degree 0, got %d", inDegrees["users"])
	}
	if inDegrees["orphan1"] != 0 {
		t.Errorf("Expected orphan1 in-degree 0, got %d", inDegrees["orphan1"])
	}
	if inDegrees["orphan2"] != 0 {
		t.Errorf("Expected orphan2 in-degree 0, got %d", inDegrees["orphan2"])
	}
}

// ============================================================================
// ProcessingQueue Tests (GA-P2-F2-T2)
// ============================================================================

func TestNewProcessingQueue(t *testing.T) {
	pq := NewProcessingQueue()
	if pq == nil {
		t.Fatal("NewProcessingQueue returned nil")
	}
	if pq.Len() != 0 {
		t.Errorf("Expected empty queue, got length %d", pq.Len())
	}
	if !pq.IsEmpty() {
		t.Error("Expected IsEmpty() to return true for new queue")
	}
}

func TestProcessingQueue_Enqueue(t *testing.T) {
	pq := NewProcessingQueue()
	pq.Enqueue("node1")
	if pq.Len() != 1 {
		t.Errorf("Expected length 1 after enqueue, got %d", pq.Len())
	}
	pq.Enqueue("node2")
	if pq.Len() != 2 {
		t.Errorf("Expected length 2 after second enqueue, got %d", pq.Len())
	}
}

func TestProcessingQueue_Dequeue(t *testing.T) {
	pq := NewProcessingQueue()
	pq.Enqueue("node1")

	node, ok := pq.Dequeue()
	if !ok {
		t.Error("Expected Dequeue to return true")
	}
	if node != "node1" {
		t.Errorf("Expected 'node1', got %q", node)
	}
	if pq.Len() != 0 {
		t.Errorf("Expected length 0 after dequeue, got %d", pq.Len())
	}
}

func TestProcessingQueue_Dequeue_Empty(t *testing.T) {
	pq := NewProcessingQueue()

	node, ok := pq.Dequeue()
	if ok {
		t.Error("Expected Dequeue to return false for empty queue")
	}
	if node != "" {
		t.Errorf("Expected empty string, got %q", node)
	}
}

func TestProcessingQueue_FIFOOrder(t *testing.T) {
	pq := NewProcessingQueue()
	items := []string{"first", "second", "third", "fourth"}

	// Enqueue items
	for _, item := range items {
		pq.Enqueue(item)
	}

	// Dequeue and verify FIFO order
	for _, expected := range items {
		node, ok := pq.Dequeue()
		if !ok {
			t.Fatalf("Dequeue failed unexpectedly")
		}
		if node != expected {
			t.Errorf("FIFO order broken: expected %q, got %q", expected, node)
		}
	}
}

func TestProcessingQueue_Len(t *testing.T) {
	pq := NewProcessingQueue()

	// Test empty queue
	if pq.Len() != 0 {
		t.Errorf("Expected length 0, got %d", pq.Len())
	}

	// Add items
	for i := 1; i <= 5; i++ {
		pq.Enqueue("node")
		if pq.Len() != i {
			t.Errorf("After %d enqueues, expected length %d, got %d", i, i, pq.Len())
		}
	}

	// Remove items
	for i := 4; i >= 0; i-- {
		pq.Dequeue()
		if pq.Len() != i {
			t.Errorf("After dequeue, expected length %d, got %d", i, pq.Len())
		}
	}
}

func TestProcessingQueue_IsEmpty(t *testing.T) {
	pq := NewProcessingQueue()

	// Should be empty initially
	if !pq.IsEmpty() {
		t.Error("New queue should be empty")
	}

	// Add item
	pq.Enqueue("node")
	if pq.IsEmpty() {
		t.Error("Queue with item should not be empty")
	}

	// Remove item
	pq.Dequeue()
	if !pq.IsEmpty() {
		t.Error("Queue after removing all items should be empty")
	}
}

func TestProcessingQueue_MultipleOperations(t *testing.T) {
	pq := NewProcessingQueue()

	// Mixed operations
	pq.Enqueue("a")
	pq.Enqueue("b")
	node1, _ := pq.Dequeue()
	pq.Enqueue("c")
	node2, _ := pq.Dequeue()
	pq.Enqueue("d")
	node3, _ := pq.Dequeue()
	node4, _ := pq.Dequeue()

	// Verify order: a, b, c, d
	expected := []string{"a", "b", "c", "d"}
	actual := []string{node1, node2, node3, node4}

	for i, exp := range expected {
		if actual[i] != exp {
			t.Errorf("At position %d: expected %q, got %q", i, exp, actual[i])
		}
	}

	if !pq.IsEmpty() {
		t.Error("Queue should be empty after all dequeues")
	}
}

func TestInitializeQueue(t *testing.T) {
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddNode("order_items", &Node{Name: "order_items"})
	g.AddEdge("users", "orders")
	g.AddEdge("orders", "order_items")

	inDegrees := g.CalculateInDegrees()
	pq := g.InitializeQueue(inDegrees)

	if pq == nil {
		t.Fatal("InitializeQueue returned nil")
	}

	// Should have only "users" (in-degree 0)
	if pq.Len() != 1 {
		t.Errorf("Expected queue length 1, got %d", pq.Len())
	}

	node, ok := pq.Dequeue()
	if !ok {
		t.Fatal("Dequeue failed")
	}
	if node != "users" {
		t.Errorf("Expected 'users' in queue, got %q", node)
	}
}

func TestInitializeQueue_EmptyInDegrees(t *testing.T) {
	g := NewGraph("users", "id")

	inDegrees := g.CalculateInDegrees()
	pq := g.InitializeQueue(inDegrees)

	if pq == nil {
		t.Fatal("InitializeQueue returned nil")
	}

	// Should have root node
	if pq.Len() != 1 {
		t.Errorf("Expected queue length 1, got %d", pq.Len())
	}

	node, _ := pq.Dequeue()
	if node != "users" {
		t.Errorf("Expected 'users', got %q", node)
	}
}

func TestInitializeQueue_NoZeroInDegree(t *testing.T) {
	// Create a cycle where no node has in-degree 0
	g := &Graph{
		Nodes: map[string]*Node{
			"a": {Name: "a"},
			"b": {Name: "b"},
		},
		Children: map[string][]string{
			"a": {"b"},
			"b": {"a"},
		},
		Parents: map[string][]string{
			"a": {"b"},
			"b": {"a"},
		},
	}

	inDegrees := g.CalculateInDegrees()
	pq := g.InitializeQueue(inDegrees)

	if pq == nil {
		t.Fatal("InitializeQueue returned nil")
	}

	// Should be empty (cycle detected - no starting nodes)
	if !pq.IsEmpty() {
		t.Error("Expected empty queue for cycle graph")
	}
}

func TestInitializeQueue_MultipleZeroInDegree(t *testing.T) {
	// Graph with multiple roots (unusual but test it)
	g := NewGraph("root1", "id")
	g.AddNode("root2", &Node{Name: "root2"})
	g.AddNode("child", &Node{Name: "child"})
	g.AddEdge("root1", "child")
	g.AddEdge("root2", "child")

	inDegrees := g.CalculateInDegrees()
	pq := g.InitializeQueue(inDegrees)

	if pq.Len() != 2 {
		t.Errorf("Expected queue length 2, got %d", pq.Len())
	}

	// Collect both nodes
	var nodes []string
	for !pq.IsEmpty() {
		node, _ := pq.Dequeue()
		nodes = append(nodes, node)
	}

	sort.Strings(nodes)
	if nodes[0] != "root1" || nodes[1] != "root2" {
		t.Errorf("Expected [root1, root2], got %v", nodes)
	}
}

func TestKahnSteps1And2_Integration(t *testing.T) {
	// Integration test combining in-degree calculation and queue initialization
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddNode("order_items", &Node{Name: "order_items"})
	g.AddNode("profiles", &Node{Name: "profiles"})
	g.AddEdge("users", "orders")
	g.AddEdge("orders", "order_items")
	g.AddEdge("users", "profiles")

	// Step 1: Calculate in-degrees
	inDegrees := g.CalculateInDegrees()

	// Step 2: Initialize queue with zero in-degree nodes
	pq := g.InitializeQueue(inDegrees)

	// Verify queue state
	if pq.Len() != 1 {
		t.Fatalf("Expected 1 node in queue, got %d", pq.Len())
	}

	node, _ := pq.Dequeue()
	if node != "users" {
		t.Errorf("Expected 'users' as first node, got %q", node)
	}

	// After removing users, orders and profiles should be next
	// (their in-degrees would be decremented in full algorithm)
	if !pq.IsEmpty() {
		t.Error("Queue should be empty after dequeuing the only zero in-degree node")
	}
}

// ============================================================================
// TopologicalSort Tests (GA-P2-F2-T3)
// ============================================================================

func TestTopologicalSort_SingleNode(t *testing.T) {
	// Single node graph should return [root]
	g := NewGraph("users", "id")

	result, err := g.TopologicalSort()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := []string{"users"}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestTopologicalSort_SimpleParentChild(t *testing.T) {
	// Simple parent-child should return [parent, child]
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders", ForeignKey: "user_id", ReferenceKey: "id", DependencyType: "1-N"})
	g.AddEdge("users", "orders")

	result, err := g.TopologicalSort()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Result should have users before orders
	if len(result) != 2 {
		t.Fatalf("Expected 2 nodes, got %d", len(result))
	}

	// Find positions
	userIdx := -1
	ordersIdx := -1
	for i, node := range result {
		if node == "users" {
			userIdx = i
		}
		if node == "orders" {
			ordersIdx = i
		}
	}

	if userIdx == -1 || ordersIdx == -1 {
		t.Fatal("Missing nodes in result")
	}

	if userIdx >= ordersIdx {
		t.Errorf("Expected users before orders, but users at %d, orders at %d", userIdx, ordersIdx)
	}
}

func TestTopologicalSort_DeepChain(t *testing.T) {
	// Deep chain: level1 -> level2 -> level3 -> level4 -> level5
	g := NewGraph("level1", "id")
	g.AddNode("level2", &Node{Name: "level2"})
	g.AddNode("level3", &Node{Name: "level3"})
	g.AddNode("level4", &Node{Name: "level4"})
	g.AddNode("level5", &Node{Name: "level5"})
	g.AddEdge("level1", "level2")
	g.AddEdge("level2", "level3")
	g.AddEdge("level3", "level4")
	g.AddEdge("level4", "level5")

	result, err := g.TopologicalSort()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result) != 5 {
		t.Fatalf("Expected 5 nodes, got %d", len(result))
	}

	// Build position map
	positions := make(map[string]int)
	for i, node := range result {
		positions[node] = i
	}

	// Verify ordering constraints
	constraints := [][2]string{
		{"level1", "level2"},
		{"level2", "level3"},
		{"level3", "level4"},
		{"level4", "level5"},
	}

	for _, c := range constraints {
		parentIdx := positions[c[0]]
		childIdx := positions[c[1]]
		if parentIdx >= childIdx {
			t.Errorf("Expected %s before %s, but %s at %d, %s at %d",
				c[0], c[1], c[0], parentIdx, c[1], childIdx)
		}
	}
}

func TestTopologicalSort_DiamondDependency(t *testing.T) {
	// Diamond: A -> B, A -> C, B -> D, C -> D
	// Valid orders: A,B,C,D or A,C,B,D
	g := NewGraph("A", "id")
	g.AddNode("B", &Node{Name: "B"})
	g.AddNode("C", &Node{Name: "C"})
	g.AddNode("D", &Node{Name: "D"})
	g.AddEdge("A", "B")
	g.AddEdge("A", "C")
	g.AddEdge("B", "D")
	g.AddEdge("C", "D")

	result, err := g.TopologicalSort()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result) != 4 {
		t.Fatalf("Expected 4 nodes, got %d", len(result))
	}

	// Build position map
	positions := make(map[string]int)
	for i, node := range result {
		positions[node] = i
	}

	// Verify A comes first and D comes last
	if positions["A"] != 0 {
		t.Errorf("Expected A at position 0, got %d", positions["A"])
	}
	if positions["D"] != 3 {
		t.Errorf("Expected D at position 3, got %d", positions["D"])
	}

	// Verify B and C are between A and D
	if positions["B"] <= positions["A"] || positions["B"] >= positions["D"] {
		t.Errorf("B should be between A and D, got position %d", positions["B"])
	}
	if positions["C"] <= positions["A"] || positions["C"] >= positions["D"] {
		t.Errorf("C should be between A and D, got position %d", positions["C"])
	}
}

func TestTopologicalSort_TwoIndependentTrees(t *testing.T) {
	// Two separate trees: tree1_root -> tree1_child, tree2_root -> tree2_child
	// Both trees should be fully sorted within themselves
	g := NewGraph("tree1_root", "id")
	g.AddNode("tree1_child", &Node{Name: "tree1_child"})
	g.AddNode("tree2_root", &Node{Name: "tree2_root"})
	g.AddNode("tree2_child", &Node{Name: "tree2_child"})
	g.AddEdge("tree1_root", "tree1_child")
	g.AddEdge("tree2_root", "tree2_child")

	result, err := g.TopologicalSort()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result) != 4 {
		t.Fatalf("Expected 4 nodes, got %d", len(result))
	}

	// Build position map
	positions := make(map[string]int)
	for i, node := range result {
		positions[node] = i
	}

	// Verify tree1 ordering
	if positions["tree1_root"] >= positions["tree1_child"] {
		t.Errorf("tree1_root should come before tree1_child")
	}

	// Verify tree2 ordering
	if positions["tree2_root"] >= positions["tree2_child"] {
		t.Errorf("tree2_root should come before tree2_child")
	}
}

func TestTopologicalSort_ComplexGraph(t *testing.T) {
	// Complex graph:
	// users -> orders -> order_items
	//     \          -> shipments
	//      -> profiles
	//      -> sessions
	g := NewGraph("users", "id")

	// Level 1
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddNode("profiles", &Node{Name: "profiles"})
	g.AddNode("sessions", &Node{Name: "sessions"})
	g.AddEdge("users", "orders")
	g.AddEdge("users", "profiles")
	g.AddEdge("users", "sessions")

	// Level 2
	g.AddNode("order_items", &Node{Name: "order_items"})
	g.AddNode("shipments", &Node{Name: "shipments"})
	g.AddEdge("orders", "order_items")
	g.AddEdge("orders", "shipments")

	result, err := g.TopologicalSort()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result) != 6 {
		t.Fatalf("Expected 6 nodes, got %d", len(result))
	}

	// Build position map
	positions := make(map[string]int)
	for i, node := range result {
		positions[node] = i
	}

	// Verify users comes first
	if positions["users"] != 0 {
		t.Errorf("Expected users at position 0, got %d", positions["users"])
	}

	// Verify all level 1 nodes come before their children
	constraints := [][2]string{
		{"users", "orders"},
		{"users", "profiles"},
		{"users", "sessions"},
		{"orders", "order_items"},
		{"orders", "shipments"},
	}

	for _, c := range constraints {
		parentIdx := positions[c[0]]
		childIdx := positions[c[1]]
		if parentIdx >= childIdx {
			t.Errorf("Expected %s before %s, but %s at %d, %s at %d",
				c[0], c[1], c[0], parentIdx, c[1], childIdx)
		}
	}
}

func TestTopologicalSort_CycleError(t *testing.T) {
	// Create a cycle: A -> B -> C -> A
	g := &Graph{
		Nodes: map[string]*Node{
			"A": {Name: "A"},
			"B": {Name: "B"},
			"C": {Name: "C"},
		},
		Children: map[string][]string{
			"A": {"B"},
			"B": {"C"},
			"C": {"A"},
		},
		Parents: map[string][]string{
			"A": {"C"},
			"B": {"A"},
			"C": {"B"},
		},
	}

	result, err := g.TopologicalSort()
	if err == nil {
		t.Fatal("Expected error for cycle graph, got nil")
	}
	var cycleErr *CycleError
	if !errors.As(err, &cycleErr) {
		t.Errorf("Expected *CycleError, got %T: %v", err, err)
	}
	if result != nil {
		t.Errorf("Expected nil result for cycle, got %v", result)
	}
}

func TestTopologicalSort_SimpleCycle(t *testing.T) {
	// Simple 2-node cycle: A <-> B
	g := &Graph{
		Nodes: map[string]*Node{
			"A": {Name: "A"},
			"B": {Name: "B"},
		},
		Children: map[string][]string{
			"A": {"B"},
			"B": {"A"},
		},
		Parents: map[string][]string{
			"A": {"B"},
			"B": {"A"},
		},
	}

	result, err := g.TopologicalSort()
	if err == nil {
		t.Fatal("Expected error for cycle graph, got nil")
	}
	var cycleErr *CycleError
	if !errors.As(err, &cycleErr) {
		t.Errorf("Expected *CycleError, got %T: %v", err, err)
	}
	if result != nil {
		t.Errorf("Expected nil result for cycle, got %v", result)
	}
}

func TestTopologicalSort_SelfCycle(t *testing.T) {
	// Self-referencing node: A -> A
	g := &Graph{
		Nodes: map[string]*Node{
			"A": {Name: "A"},
		},
		Children: map[string][]string{
			"A": {"A"},
		},
		Parents: map[string][]string{
			"A": {"A"},
		},
	}

	result, err := g.TopologicalSort()
	if err == nil {
		t.Fatal("Expected error for self-cycle, got nil")
	}
	var cycleErr *CycleError
	if !errors.As(err, &cycleErr) {
		t.Errorf("Expected *CycleError, got %T: %v", err, err)
	}
	if result != nil {
		t.Errorf("Expected nil result for cycle, got %v", result)
	}
}

func TestTopologicalSort_ProcessesAllNodes(t *testing.T) {
	// Verify all nodes are included in the result
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddNode("order_items", &Node{Name: "order_items"})
	g.AddNode("profiles", &Node{Name: "profiles"})
	g.AddNode("sessions", &Node{Name: "sessions"})
	g.AddEdge("users", "orders")
	g.AddEdge("orders", "order_items")
	g.AddEdge("users", "profiles")
	g.AddEdge("users", "sessions")

	result, err := g.TopologicalSort()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Result should contain exactly 5 nodes
	if len(result) != 5 {
		t.Errorf("Expected 5 nodes in result, got %d", len(result))
	}

	// Check for duplicates
	seen := make(map[string]bool)
	for _, node := range result {
		if seen[node] {
			t.Errorf("Duplicate node %s in result", node)
		}
		seen[node] = true
	}

	// Check all expected nodes are present
	expected := []string{"users", "orders", "order_items", "profiles", "sessions"}
	for _, node := range expected {
		if !seen[node] {
			t.Errorf("Missing node %s in result", node)
		}
	}
}

func TestTopologicalSort_ValidOrder(t *testing.T) {
	// Test that result is a valid topological order (parents before children)
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddNode("order_items", &Node{Name: "order_items"})
	g.AddNode("shipments", &Node{Name: "shipments"})
	g.AddEdge("users", "orders")
	g.AddEdge("orders", "order_items")
	g.AddEdge("orders", "shipments")

	result, err := g.TopologicalSort()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Build position map
	positions := make(map[string]int)
	for i, node := range result {
		positions[node] = i
	}

	// Verify all edges go forward in the ordering
	edges := [][2]string{
		{"users", "orders"},
		{"orders", "order_items"},
		{"orders", "shipments"},
	}

	for _, edge := range edges {
		parentPos := positions[edge[0]]
		childPos := positions[edge[1]]
		if parentPos >= childPos {
			t.Errorf("Invalid topological order: %s (pos %d) should come before %s (pos %d)",
				edge[0], parentPos, edge[1], childPos)
		}
	}
}

// ============================================================================
// Topological Order Generation Tests (GA-P2-F2-T4)
// ============================================================================

func TestCopyOrder_SameAsTopologicalSort(t *testing.T) {
	// CopyOrder should return the same result as TopologicalSort
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddNode("order_items", &Node{Name: "order_items"})
	g.AddEdge("users", "orders")
	g.AddEdge("orders", "order_items")

	copyOrder, err := g.CopyOrder()
	if err != nil {
		t.Fatalf("CopyOrder error: %v", err)
	}

	topoOrder, err := g.TopologicalSort()
	if err != nil {
		t.Fatalf("TopologicalSort error: %v", err)
	}

	if !reflect.DeepEqual(copyOrder, topoOrder) {
		t.Errorf("CopyOrder %v != TopologicalSort %v", copyOrder, topoOrder)
	}
}

func TestCopyOrder_ParentsBeforeChildren(t *testing.T) {
	// Verify CopyOrder returns parents before children
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddNode("order_items", &Node{Name: "order_items"})
	g.AddEdge("users", "orders")
	g.AddEdge("orders", "order_items")

	result, err := g.CopyOrder()
	if err != nil {
		t.Fatalf("CopyOrder error: %v", err)
	}

	// Build position map
	positions := make(map[string]int)
	for i, node := range result {
		positions[node] = i
	}

	// Verify ordering: users < orders < order_items
	if positions["users"] >= positions["orders"] {
		t.Error("users should come before orders in copy order")
	}
	if positions["orders"] >= positions["order_items"] {
		t.Error("orders should come before order_items in copy order")
	}
}

func TestCopyOrder_CycleError(t *testing.T) {
	// CopyOrder should return error for cyclic graph
	g := &Graph{
		Nodes: map[string]*Node{
			"A": {Name: "A"},
			"B": {Name: "B"},
		},
		Children: map[string][]string{
			"A": {"B"},
			"B": {"A"},
		},
		Parents: map[string][]string{
			"A": {"B"},
			"B": {"A"},
		},
	}

	result, err := g.CopyOrder()
	if err == nil {
		t.Fatal("Expected error for cycle, got nil")
	}
	var cycleErr *CycleError
	if !errors.As(err, &cycleErr) {
		t.Errorf("Expected *CycleError, got %T: %v", err, err)
	}
	if result != nil {
		t.Errorf("Expected nil result, got %v", result)
	}
}

func TestDeleteOrder_ReverseOfCopyOrder(t *testing.T) {
	// DeleteOrder should be reverse of CopyOrder
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddNode("order_items", &Node{Name: "order_items"})
	g.AddEdge("users", "orders")
	g.AddEdge("orders", "order_items")

	copyOrder, err := g.CopyOrder()
	if err != nil {
		t.Fatalf("CopyOrder error: %v", err)
	}

	deleteOrder, err := g.DeleteOrder()
	if err != nil {
		t.Fatalf("DeleteOrder error: %v", err)
	}

	// Verify delete order is reverse of copy order
	if len(deleteOrder) != len(copyOrder) {
		t.Fatalf("Length mismatch: copy=%d, delete=%d", len(copyOrder), len(deleteOrder))
	}

	for i := 0; i < len(copyOrder); i++ {
		expected := copyOrder[len(copyOrder)-1-i]
		if deleteOrder[i] != expected {
			t.Errorf("DeleteOrder[%d] = %s, expected %s", i, deleteOrder[i], expected)
		}
	}
}

func TestDeleteOrder_SingleNode(t *testing.T) {
	// DeleteOrder on single node should return [node]
	g := NewGraph("users", "id")

	result, err := g.DeleteOrder()
	if err != nil {
		t.Fatalf("DeleteOrder error: %v", err)
	}

	expected := []string{"users"}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestDeleteOrder_ParentChild(t *testing.T) {
	// DeleteOrder on parent-child should return [child, parent]
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddEdge("users", "orders")

	result, err := g.DeleteOrder()
	if err != nil {
		t.Fatalf("DeleteOrder error: %v", err)
	}

	// Result should have exactly 2 elements
	if len(result) != 2 {
		t.Fatalf("Expected 2 nodes, got %d", len(result))
	}

	// orders (child) should come before users (parent)
	if result[0] != "orders" || result[1] != "users" {
		t.Errorf("Expected [orders, users], got %v", result)
	}
}

func TestDeleteOrder_ChildrenBeforeParents(t *testing.T) {
	// Verify DeleteOrder returns children before parents
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddNode("order_items", &Node{Name: "order_items"})
	g.AddNode("shipments", &Node{Name: "shipments"})
	g.AddEdge("users", "orders")
	g.AddEdge("orders", "order_items")
	g.AddEdge("orders", "shipments")

	result, err := g.DeleteOrder()
	if err != nil {
		t.Fatalf("DeleteOrder error: %v", err)
	}

	// Build position map
	positions := make(map[string]int)
	for i, node := range result {
		positions[node] = i
	}

	// In delete order: children should come before parents
	// order_items < orders, shipments < orders, orders < users
	if positions["order_items"] >= positions["orders"] {
		t.Error("order_items should come before orders in delete order")
	}
	if positions["shipments"] >= positions["orders"] {
		t.Error("shipments should come before orders in delete order")
	}
	if positions["orders"] >= positions["users"] {
		t.Error("orders should come before users in delete order")
	}
}

func TestDeleteOrder_CycleError(t *testing.T) {
	// DeleteOrder should return error for cyclic graph
	g := &Graph{
		Nodes: map[string]*Node{
			"A": {Name: "A"},
			"B": {Name: "B"},
		},
		Children: map[string][]string{
			"A": {"B"},
			"B": {"A"},
		},
		Parents: map[string][]string{
			"A": {"B"},
			"B": {"A"},
		},
	}

	result, err := g.DeleteOrder()
	if err == nil {
		t.Fatal("Expected error for cycle, got nil")
	}
	var cycleErr *CycleError
	if !errors.As(err, &cycleErr) {
		t.Errorf("Expected *CycleError, got %T: %v", err, err)
	}
	if result != nil {
		t.Errorf("Expected nil result, got %v", result)
	}
}

func TestHasCycle_FalseForValidDAG(t *testing.T) {
	// Valid DAG should not have cycle
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddNode("order_items", &Node{Name: "order_items"})
	g.AddEdge("users", "orders")
	g.AddEdge("orders", "order_items")

	if g.HasCycle() {
		t.Error("HasCycle returned true for valid DAG")
	}
}

func TestHasCycle_TrueForCycle(t *testing.T) {
	// Cycle graph should return true
	g := &Graph{
		Nodes: map[string]*Node{
			"A": {Name: "A"},
			"B": {Name: "B"},
			"C": {Name: "C"},
		},
		Children: map[string][]string{
			"A": {"B"},
			"B": {"C"},
			"C": {"A"},
		},
		Parents: map[string][]string{
			"A": {"C"},
			"B": {"A"},
			"C": {"B"},
		},
	}

	if !g.HasCycle() {
		t.Error("HasCycle returned false for cyclic graph")
	}
}

func TestHasCycle_SingleNode(t *testing.T) {
	// Single node graph should not have cycle
	g := NewGraph("users", "id")

	if g.HasCycle() {
		t.Error("HasCycle returned true for single node")
	}
}

func TestHasCycle_SelfCycle(t *testing.T) {
	// Self-referencing node is a cycle
	g := &Graph{
		Nodes: map[string]*Node{
			"A": {Name: "A"},
		},
		Children: map[string][]string{
			"A": {"A"},
		},
		Parents: map[string][]string{
			"A": {"A"},
		},
	}

	if !g.HasCycle() {
		t.Error("HasCycle returned false for self-cycle")
	}
}

func TestDetectIncompleteProcessing_NoCycle(t *testing.T) {
	// Valid DAG should return nil
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddNode("order_items", &Node{Name: "order_items"})
	g.AddEdge("users", "orders")
	g.AddEdge("orders", "order_items")

	info := g.DetectIncompleteProcessing()
	if info != nil {
		t.Errorf("Expected nil for valid DAG, got %v", info)
	}
}

func TestDetectIncompleteProcessing_WithCycle(t *testing.T) {
	// Cycle graph should return CycleInfo
	g := &Graph{
		Nodes: map[string]*Node{
			"A": {Name: "A"},
			"B": {Name: "B"},
			"C": {Name: "C"},
		},
		Children: map[string][]string{
			"A": {"B"},
			"B": {"C"},
			"C": {"A"},
		},
		Parents: map[string][]string{
			"A": {"C"},
			"B": {"A"},
			"C": {"B"},
		},
	}

	info := g.DetectIncompleteProcessing()
	if info == nil {
		t.Fatal("Expected CycleInfo for cyclic graph, got nil")
	}

	// All 3 nodes should be unprocessed (part of cycle)
	if info.TotalNodes != 3 {
		t.Errorf("Expected TotalNodes=3, got %d", info.TotalNodes)
	}
	if info.ProcessedNodes != 0 {
		t.Errorf("Expected ProcessedNodes=0, got %d", info.ProcessedNodes)
	}
	if len(info.UnprocessedNodes) != 3 {
		t.Errorf("Expected 3 unprocessed nodes, got %d", len(info.UnprocessedNodes))
	}
}

func TestDetectIncompleteProcessing_PartialCycle(t *testing.T) {
	// Graph with valid part and cycle part
	// Valid: root -> child
	// Cycle: A -> B -> C -> A
	g := &Graph{
		Nodes: map[string]*Node{
			"root":  {Name: "root"},
			"child": {Name: "child"},
			"A":     {Name: "A"},
			"B":     {Name: "B"},
			"C":     {Name: "C"},
		},
		Children: map[string][]string{
			"root": {"child"},
			"A":    {"B"},
			"B":    {"C"},
			"C":    {"A"},
		},
		Parents: map[string][]string{
			"child": {"root"},
			"A":     {"C"},
			"B":     {"A"},
			"C":     {"B"},
		},
	}

	info := g.DetectIncompleteProcessing()
	if info == nil {
		t.Fatal("Expected CycleInfo for partial cycle, got nil")
	}

	// 5 total nodes, 2 processed (root, child), 3 unprocessed (A, B, C)
	if info.TotalNodes != 5 {
		t.Errorf("Expected TotalNodes=5, got %d", info.TotalNodes)
	}
	if info.ProcessedNodes != 2 {
		t.Errorf("Expected ProcessedNodes=2, got %d", info.ProcessedNodes)
	}
	if len(info.UnprocessedNodes) != 3 {
		t.Errorf("Expected 3 unprocessed nodes, got %d", len(info.UnprocessedNodes))
	}

	// Verify unprocessed nodes are A, B, C
	unprocessedSet := make(map[string]bool)
	for _, node := range info.UnprocessedNodes {
		unprocessedSet[node] = true
	}
	if !unprocessedSet["A"] || !unprocessedSet["B"] || !unprocessedSet["C"] {
		t.Errorf("Unprocessed nodes should be A, B, C, got %v", info.UnprocessedNodes)
	}
}

func TestDetectIncompleteProcessing_CycleInfoContents(t *testing.T) {
	// Verify CycleInfo fields are correctly populated
	g := &Graph{
		Nodes: map[string]*Node{
			"A": {Name: "A"},
			"B": {Name: "B"},
		},
		Children: map[string][]string{
			"A": {"B"},
			"B": {"A"},
		},
		Parents: map[string][]string{
			"A": {"B"},
			"B": {"A"},
		},
	}

	info := g.DetectIncompleteProcessing()
	if info == nil {
		t.Fatal("Expected CycleInfo, got nil")
	}

	// Verify all fields
	if info.TotalNodes != 2 {
		t.Errorf("TotalNodes: expected 2, got %d", info.TotalNodes)
	}
	if info.ProcessedNodes != 0 {
		t.Errorf("ProcessedNodes: expected 0, got %d", info.ProcessedNodes)
	}
	if len(info.UnprocessedNodes) != 2 {
		t.Errorf("UnprocessedNodes length: expected 2, got %d", len(info.UnprocessedNodes))
	}

	// Verify unprocessed nodes contain both A and B
	hasA, hasB := false, false
	for _, node := range info.UnprocessedNodes {
		if node == "A" {
			hasA = true
		}
		if node == "B" {
			hasB = true
		}
	}
	if !hasA || !hasB {
		t.Errorf("UnprocessedNodes should contain A and B, got %v", info.UnprocessedNodes)
	}
}

func TestCycleInfo_ProcessedPlusUnprocessedEqualsTotal(t *testing.T) {
	// Verify the invariant: ProcessedNodes + len(UnprocessedNodes) = TotalNodes
	g := &Graph{
		Nodes: map[string]*Node{
			"root": {Name: "root"},
			"A":    {Name: "A"},
			"B":    {Name: "B"},
		},
		Children: map[string][]string{
			"A": {"B"},
			"B": {"A"},
		},
		Parents: map[string][]string{
			"A": {"B"},
			"B": {"A"},
		},
	}

	info := g.DetectIncompleteProcessing()
	if info == nil {
		t.Fatal("Expected CycleInfo, got nil")
	}

	actual := info.ProcessedNodes + len(info.UnprocessedNodes)
	if actual != info.TotalNodes {
		t.Errorf("ProcessedNodes(%d) + len(UnprocessedNodes)(%d) = %d, expected TotalNodes=%d",
			info.ProcessedNodes, len(info.UnprocessedNodes), actual, info.TotalNodes)
	}
}
