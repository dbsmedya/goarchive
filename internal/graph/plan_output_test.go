package graph

import (
	"reflect"
	"sort"
	"testing"
)

// ============================================================================
// Plan Command Output Tests (GA-P2-F4-T4)
// ============================================================================
// These tests verify that CopyOrder and DeleteOrder produce correct output
// for display in the `goarchive plan` command.

// TestPlanOutput_SimpleParentChild verifies basic parent-child ordering
func TestPlanOutput_SimpleParentChild(t *testing.T) {
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders", ForeignKey: "user_id", ReferenceKey: "id", DependencyType: "1-N"})
	g.AddEdge("users", "orders")

	// Copy order: parent before child
	copyOrder, err := g.CopyOrder()
	if err != nil {
		t.Fatalf("CopyOrder error: %v", err)
	}
	if len(copyOrder) != 2 {
		t.Fatalf("Expected 2 tables in copy order, got %d", len(copyOrder))
	}
	if copyOrder[0] != "users" || copyOrder[1] != "orders" {
		t.Errorf("Copy order should be [users, orders], got %v", copyOrder)
	}

	// Delete order: child before parent
	deleteOrder, err := g.DeleteOrder()
	if err != nil {
		t.Fatalf("DeleteOrder error: %v", err)
	}
	if len(deleteOrder) != 2 {
		t.Fatalf("Expected 2 tables in delete order, got %d", len(deleteOrder))
	}
	if deleteOrder[0] != "orders" || deleteOrder[1] != "users" {
		t.Errorf("Delete order should be [orders, users], got %v", deleteOrder)
	}
}

// TestPlanOutput_RootTableFirstInCopy verifies root is first in copy order
func TestPlanOutput_RootTableFirstInCopy(t *testing.T) {
	g := NewGraph("customers", "customer_id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddNode("order_items", &Node{Name: "order_items"})
	g.AddNode("payments", &Node{Name: "payments"})
	g.AddEdge("customers", "orders")
	g.AddEdge("customers", "payments")
	g.AddEdge("orders", "order_items")

	copyOrder, err := g.CopyOrder()
	if err != nil {
		t.Fatalf("CopyOrder error: %v", err)
	}

	// Root should always be first in copy order
	if copyOrder[0] != "customers" {
		t.Errorf("Root table 'customers' should be first in copy order, got %v", copyOrder)
	}
}

// TestPlanOutput_RootTableLastInDelete verifies root is last in delete order
func TestPlanOutput_RootTableLastInDelete(t *testing.T) {
	g := NewGraph("customers", "customer_id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddNode("order_items", &Node{Name: "order_items"})
	g.AddNode("payments", &Node{Name: "payments"})
	g.AddEdge("customers", "orders")
	g.AddEdge("customers", "payments")
	g.AddEdge("orders", "order_items")

	deleteOrder, err := g.DeleteOrder()
	if err != nil {
		t.Fatalf("DeleteOrder error: %v", err)
	}

	// Root should always be last in delete order
	if deleteOrder[len(deleteOrder)-1] != "customers" {
		t.Errorf("Root table 'customers' should be last in delete order, got %v", deleteOrder)
	}
}

// TestPlanOutput_NestedRelations verifies correct ordering with deeply nested relations
func TestPlanOutput_NestedRelations(t *testing.T) {
	// customers -> orders -> order_items -> item_reviews
	g := NewGraph("customers", "customer_id")
	g.AddNode("orders", &Node{Name: "orders", ForeignKey: "customer_id"})
	g.AddNode("order_items", &Node{Name: "order_items", ForeignKey: "order_id"})
	g.AddNode("item_reviews", &Node{Name: "item_reviews", ForeignKey: "item_id"})
	g.AddEdge("customers", "orders")
	g.AddEdge("orders", "order_items")
	g.AddEdge("order_items", "item_reviews")

	copyOrder, err := g.CopyOrder()
	if err != nil {
		t.Fatalf("CopyOrder error: %v", err)
	}

	// Verify complete ordering chain in copy order
	positions := make(map[string]int)
	for i, table := range copyOrder {
		positions[table] = i
	}

	if positions["customers"] >= positions["orders"] {
		t.Error("customers should come before orders in copy order")
	}
	if positions["orders"] >= positions["order_items"] {
		t.Error("orders should come before order_items in copy order")
	}
	if positions["order_items"] >= positions["item_reviews"] {
		t.Error("order_items should come before item_reviews in copy order")
	}

	deleteOrder, err := g.DeleteOrder()
	if err != nil {
		t.Fatalf("DeleteOrder error: %v", err)
	}

	// Verify reverse ordering in delete order
	positions = make(map[string]int)
	for i, table := range deleteOrder {
		positions[table] = i
	}

	if positions["item_reviews"] >= positions["order_items"] {
		t.Error("item_reviews should come before order_items in delete order")
	}
	if positions["order_items"] >= positions["orders"] {
		t.Error("order_items should come before orders in delete order")
	}
	if positions["orders"] >= positions["customers"] {
		t.Error("orders should come before customers in delete order")
	}
}

// TestPlanOutput_MultipleBranches verifies ordering with multiple independent branches
func TestPlanOutput_MultipleBranches(t *testing.T) {
	// store -> inventory
	//     \-> staff
	//     \-> customers -> orders
	g := NewGraph("store", "store_id")
	g.AddNode("inventory", &Node{Name: "inventory"})
	g.AddNode("staff", &Node{Name: "staff"})
	g.AddNode("customers", &Node{Name: "customers"})
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddEdge("store", "inventory")
	g.AddEdge("store", "staff")
	g.AddEdge("store", "customers")
	g.AddEdge("customers", "orders")

	copyOrder, err := g.CopyOrder()
	if err != nil {
		t.Fatalf("CopyOrder error: %v", err)
	}

	positions := make(map[string]int)
	for i, table := range copyOrder {
		positions[table] = i
	}

	// Verify store is first
	if positions["store"] != 0 {
		t.Errorf("store should be first in copy order, got position %d", positions["store"])
	}

	// Verify all direct children come after store
	for _, child := range []string{"inventory", "staff", "customers"} {
		if positions[child] <= positions["store"] {
			t.Errorf("%s should come after store in copy order", child)
		}
	}

	// Verify orders comes after customers
	if positions["orders"] <= positions["customers"] {
		t.Error("orders should come after customers in copy order")
	}
}

// TestPlanOutput_DiamondPattern verifies ordering with diamond dependency pattern
func TestPlanOutput_DiamondPattern(t *testing.T) {
	// Diamond: A -> B, A -> C, B -> D, C -> D
	g := NewGraph("A", "id")
	g.AddNode("B", &Node{Name: "B"})
	g.AddNode("C", &Node{Name: "C"})
	g.AddNode("D", &Node{Name: "D"})
	g.AddEdge("A", "B")
	g.AddEdge("A", "C")
	g.AddEdge("B", "D")
	g.AddEdge("C", "D")

	copyOrder, err := g.CopyOrder()
	if err != nil {
		t.Fatalf("CopyOrder error: %v", err)
	}

	positions := make(map[string]int)
	for i, table := range copyOrder {
		positions[table] = i
	}

	// A must be first
	if positions["A"] != 0 {
		t.Errorf("A should be first in copy order, got position %d", positions["A"])
	}

	// D must be last
	if positions["D"] != 3 {
		t.Errorf("D should be last in copy order, got position %d", positions["D"])
	}

	// B and C must be between A and D
	if positions["B"] <= positions["A"] || positions["B"] >= positions["D"] {
		t.Errorf("B should be between A and D, got position %d", positions["B"])
	}
	if positions["C"] <= positions["A"] || positions["C"] >= positions["D"] {
		t.Errorf("C should be between A and D, got position %d", positions["C"])
	}
}

// TestPlanOutput_AllEdgesDisplayed verifies all edges can be retrieved for display
func TestPlanOutput_AllEdgesDisplayed(t *testing.T) {
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddNode("profiles", &Node{Name: "profiles"})
	g.AddNode("order_items", &Node{Name: "order_items"})
	g.AddEdge("users", "orders")
	g.AddEdge("users", "profiles")
	g.AddEdge("orders", "order_items")

	edges := g.AllEdges()
	if len(edges) != 3 {
		t.Errorf("Expected 3 edges, got %d", len(edges))
	}

	// Verify all expected edges exist
	expectedEdges := map[string]bool{
		"users->orders":       false,
		"users->profiles":     false,
		"orders->order_items": false,
	}

	for _, edge := range edges {
		key := edge.From + "->" + edge.To
		if _, exists := expectedEdges[key]; exists {
			expectedEdges[key] = true
		}
	}

	for edge, found := range expectedEdges {
		if !found {
			t.Errorf("Expected edge %s not found", edge)
		}
	}
}

// TestPlanOutput_EdgeMetadata verifies edge metadata is available for display
func TestPlanOutput_EdgeMetadata(t *testing.T) {
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddEdgeWithMeta("users", "orders", "user_id", "id", "1-N")

	meta := g.GetEdgeMeta("users", "orders")
	if meta == nil {
		t.Fatal("Expected edge metadata, got nil")
	}

	if meta.ForeignKey != "user_id" {
		t.Errorf("Expected ForeignKey 'user_id', got %q", meta.ForeignKey)
	}
	if meta.ReferenceKey != "id" {
		t.Errorf("Expected ReferenceKey 'id', got %q", meta.ReferenceKey)
	}
	if meta.DependencyType != "1-N" {
		t.Errorf("Expected DependencyType '1-N', got %q", meta.DependencyType)
	}
}

// TestPlanOutput_NodeCount verifies node count is accurate for display
func TestPlanOutput_NodeCount(t *testing.T) {
	g := NewGraph("users", "id")
	if g.NodeCount() != 1 {
		t.Errorf("Expected 1 node, got %d", g.NodeCount())
	}

	g.AddNode("orders", &Node{Name: "orders"})
	g.AddNode("profiles", &Node{Name: "profiles"})
	if g.NodeCount() != 3 {
		t.Errorf("Expected 3 nodes, got %d", g.NodeCount())
	}
}

// TestPlanOutput_EdgeCount verifies edge count is accurate for display
func TestPlanOutput_EdgeCount(t *testing.T) {
	g := NewGraph("users", "id")
	if g.EdgeCount() != 0 {
		t.Errorf("Expected 0 edges, got %d", g.EdgeCount())
	}

	g.AddNode("orders", &Node{Name: "orders"})
	g.AddNode("profiles", &Node{Name: "profiles"})
	g.AddEdge("users", "orders")
	g.AddEdge("users", "profiles")

	if g.EdgeCount() != 2 {
		t.Errorf("Expected 2 edges, got %d", g.EdgeCount())
	}
}

// TestPlanOutput_LeafNodes verifies leaf nodes are correctly identified
func TestPlanOutput_LeafNodes(t *testing.T) {
	// users -> orders -> order_items
	//     \-> profiles
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddNode("profiles", &Node{Name: "profiles"})
	g.AddNode("order_items", &Node{Name: "order_items"})
	g.AddEdge("users", "orders")
	g.AddEdge("users", "profiles")
	g.AddEdge("orders", "order_items")

	leaves := g.LeafNodes()
	sort.Strings(leaves)

	expected := []string{"order_items", "profiles"}
	if !reflect.DeepEqual(leaves, expected) {
		t.Errorf("Expected leaf nodes %v, got %v", expected, leaves)
	}
}

// TestPlanOutput_CompleteOrderingValidatesFKConstraints verifies ordering respects FK constraints
func TestPlanOutput_CompleteOrderingValidatesFKConstraints(t *testing.T) {
	// Complex schema: sample database structure
	// customer -> rental -> payment
	//        \-> inventory (indirect via store)
	g := NewGraph("customer", "customer_id")
	g.AddNode("rental", &Node{Name: "rental"})
	g.AddNode("payment", &Node{Name: "payment"})
	g.AddNode("inventory", &Node{Name: "inventory"})
	g.AddEdge("customer", "rental")
	g.AddEdge("customer", "payment")
	g.AddEdge("rental", "inventory")

	copyOrder, err := g.CopyOrder()
	if err != nil {
		t.Fatalf("CopyOrder error: %v", err)
	}

	// Verify FK constraints would be satisfied in copy order
	// (parent copied before child)
	positions := make(map[string]int)
	for i, table := range copyOrder {
		positions[table] = i
	}

	// rental references customer
	if positions["rental"] < positions["customer"] {
		t.Error("rental should come after customer in copy order (FK constraint)")
	}
	// payment references customer
	if positions["payment"] < positions["customer"] {
		t.Error("payment should come after customer in copy order (FK constraint)")
	}

	deleteOrder, err := g.DeleteOrder()
	if err != nil {
		t.Fatalf("DeleteOrder error: %v", err)
	}

	// Verify FK constraints would be satisfied in delete order
	// (child deleted before parent)
	positions = make(map[string]int)
	for i, table := range deleteOrder {
		positions[table] = i
	}

	// rental should be deleted before customer
	if positions["rental"] > positions["customer"] {
		t.Error("rental should come before customer in delete order (FK constraint)")
	}
	// payment should be deleted before customer
	if positions["payment"] > positions["customer"] {
		t.Error("payment should come before customer in delete order (FK constraint)")
	}
}

// TestPlanOutput_SingleNodeGraph verifies output for single-node graph
func TestPlanOutput_SingleNodeGraph(t *testing.T) {
	g := NewGraph("users", "id")

	copyOrder, err := g.CopyOrder()
	if err != nil {
		t.Fatalf("CopyOrder error: %v", err)
	}
	if len(copyOrder) != 1 || copyOrder[0] != "users" {
		t.Errorf("Expected [users], got %v", copyOrder)
	}

	deleteOrder, err := g.DeleteOrder()
	if err != nil {
		t.Fatalf("DeleteOrder error: %v", err)
	}
	if len(deleteOrder) != 1 || deleteOrder[0] != "users" {
		t.Errorf("Expected [users], got %v", deleteOrder)
	}
}

// TestPlanOutput_CycleDetectionError verifies cycle detection in plan output
func TestPlanOutput_CycleDetectionError(t *testing.T) {
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

	_, err := g.CopyOrder()
	if err == nil {
		t.Fatal("Expected error for cycle, got nil")
	}

	cycleInfo := g.DetectIncompleteProcessing()
	if cycleInfo == nil {
		t.Fatal("Expected cycle info, got nil")
	}

	// Verify cycle info contains expected data
	if cycleInfo.TotalNodes != 3 {
		t.Errorf("Expected TotalNodes=3, got %d", cycleInfo.TotalNodes)
	}
	if len(cycleInfo.CycleParticipants) != 3 {
		t.Errorf("Expected 3 cycle participants, got %d", len(cycleInfo.CycleParticipants))
	}
}

// TestPlanOutput_ValidAndInvalidSubgraphs verifies partial cycle detection
func TestPlanOutput_ValidAndInvalidSubgraphs(t *testing.T) {
	// Valid part: root -> valid_child
	// Cycle part: A -> B -> C -> A
	g := &Graph{
		Nodes: map[string]*Node{
			"root":        {Name: "root"},
			"valid_child": {Name: "valid_child"},
			"A":           {Name: "A"},
			"B":           {Name: "B"},
			"C":           {Name: "C"},
		},
		Children: map[string][]string{
			"root": {"valid_child"},
			"A":    {"B"},
			"B":    {"C"},
			"C":    {"A"},
		},
		Parents: map[string][]string{
			"valid_child": {"root"},
			"A":           {"C"},
			"B":           {"A"},
			"C":           {"B"},
		},
	}

	cycleInfo := g.DetectIncompleteProcessing()
	if cycleInfo == nil {
		t.Fatal("Expected cycle info for partial cycle graph")
	}

	// 2 nodes should be processed (root, valid_child)
	if cycleInfo.ProcessedNodes != 2 {
		t.Errorf("Expected ProcessedNodes=2, got %d", cycleInfo.ProcessedNodes)
	}

	// 3 nodes should be unprocessed (A, B, C in cycle)
	if len(cycleInfo.UnprocessedNodes) != 3 {
		t.Errorf("Expected 3 unprocessed nodes, got %d", len(cycleInfo.UnprocessedNodes))
	}

	// All unprocessed nodes should be cycle participants
	if len(cycleInfo.CycleParticipants) != 3 {
		t.Errorf("Expected 3 cycle participants, got %d", len(cycleInfo.CycleParticipants))
	}
}

// TestPlanOutput_LargeGraphPerformance verifies plan output for large graphs
func TestPlanOutput_LargeGraphPerformance(t *testing.T) {
	// Create a balanced binary tree with depth 5 (63 nodes)
	g := NewGraph("N0", "id")

	nodeCount := 1
	for depth := 0; depth < 5; depth++ {
		nodesAtDepth := 1 << depth
		startID := (1 << depth) - 1
		for i := 0; i < nodesAtDepth; i++ {
			parentID := startID + i
			leftChild := nodeName(nodeCount)
			rightChild := nodeName(nodeCount + 1)
			nodeCount += 2

			g.AddNode(leftChild, &Node{Name: leftChild})
			g.AddNode(rightChild, &Node{Name: rightChild})
			g.AddEdge(nodeName(parentID), leftChild)
			g.AddEdge(nodeName(parentID), rightChild)
		}
	}

	copyOrder, err := g.CopyOrder()
	if err != nil {
		t.Fatalf("CopyOrder error: %v", err)
	}

	if len(copyOrder) != 63 {
		t.Errorf("Expected 63 nodes in copy order, got %d", len(copyOrder))
	}

	// Verify root is first
	if copyOrder[0] != "N0" {
		t.Errorf("Root should be first, got %s", copyOrder[0])
	}

	// Verify copy and delete are reverses
	deleteOrder, err := g.DeleteOrder()
	if err != nil {
		t.Fatalf("DeleteOrder error: %v", err)
	}

	for i := 0; i < len(copyOrder); i++ {
		expected := copyOrder[len(copyOrder)-1-i]
		if deleteOrder[i] != expected {
			t.Errorf("DeleteOrder[%d] = %s, expected %s", i, deleteOrder[i], expected)
		}
	}
}

// TestPlanOutput_DisconnectedNodes verifies handling of disconnected nodes
func TestPlanOutput_DisconnectedNodes(t *testing.T) {
	// Root with children, plus some disconnected nodes
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddNode("orphan1", &Node{Name: "orphan1"})
	g.AddNode("orphan2", &Node{Name: "orphan2"})
	g.AddEdge("users", "orders")
	// orphan1 and orphan2 have no edges

	copyOrder, err := g.CopyOrder()
	if err != nil {
		t.Fatalf("CopyOrder error: %v", err)
	}

	if len(copyOrder) != 4 {
		t.Errorf("Expected 4 nodes in copy order, got %d", len(copyOrder))
	}

	// All nodes should be present
	nodeSet := make(map[string]bool)
	for _, node := range copyOrder {
		nodeSet[node] = true
	}

	for _, expected := range []string{"users", "orders", "orphan1", "orphan2"} {
		if !nodeSet[expected] {
			t.Errorf("Missing node %s in copy order", expected)
		}
	}
}

// TestPlanOutput_GetNodeMetadata verifies node metadata is accessible for display
func TestPlanOutput_GetNodeMetadata(t *testing.T) {
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{
		Name:           "orders",
		ForeignKey:     "user_id",
		ReferenceKey:   "id",
		DependencyType: "1-N",
		IsRoot:         false,
	})
	g.AddEdge("users", "orders")

	// Get root node
	rootNode := g.GetNode("users")
	if rootNode == nil {
		t.Fatal("Expected root node, got nil")
	}
	if !rootNode.IsRoot {
		t.Error("users should be marked as root")
	}

	// Get child node
	childNode := g.GetNode("orders")
	if childNode == nil {
		t.Fatal("Expected orders node, got nil")
	}
	if childNode.IsRoot {
		t.Error("orders should not be marked as root")
	}
	if childNode.ForeignKey != "user_id" {
		t.Errorf("Expected ForeignKey 'user_id', got %q", childNode.ForeignKey)
	}
	if childNode.ReferenceKey != "id" {
		t.Errorf("Expected ReferenceKey 'id', got %q", childNode.ReferenceKey)
	}
	if childNode.DependencyType != "1-N" {
		t.Errorf("Expected DependencyType '1-N', got %q", childNode.DependencyType)
	}
}

// TestPlanOutput_InDegreeAndOutDegree verifies degree calculations for display
func TestPlanOutput_InDegreeAndOutDegree(t *testing.T) {
	// users -> orders -> order_items
	//     \-> profiles
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddNode("profiles", &Node{Name: "profiles"})
	g.AddNode("order_items", &Node{Name: "order_items"})
	g.AddEdge("users", "orders")
	g.AddEdge("users", "profiles")
	g.AddEdge("orders", "order_items")

	// Check in-degrees
	if g.InDegree("users") != 0 {
		t.Errorf("users in-degree should be 0, got %d", g.InDegree("users"))
	}
	if g.InDegree("orders") != 1 {
		t.Errorf("orders in-degree should be 1, got %d", g.InDegree("orders"))
	}
	if g.InDegree("profiles") != 1 {
		t.Errorf("profiles in-degree should be 1, got %d", g.InDegree("profiles"))
	}
	if g.InDegree("order_items") != 1 {
		t.Errorf("order_items in-degree should be 1, got %d", g.InDegree("order_items"))
	}

	// Check out-degrees
	if g.OutDegree("users") != 2 {
		t.Errorf("users out-degree should be 2, got %d", g.OutDegree("users"))
	}
	if g.OutDegree("orders") != 1 {
		t.Errorf("orders out-degree should be 1, got %d", g.OutDegree("orders"))
	}
	if g.OutDegree("profiles") != 0 {
		t.Errorf("profiles out-degree should be 0, got %d", g.OutDegree("profiles"))
	}
	if g.OutDegree("order_items") != 0 {
		t.Errorf("order_items out-degree should be 0, got %d", g.OutDegree("order_items"))
	}
}

// TestPlanOutput_AllNodesList verifies all nodes can be listed
func TestPlanOutput_AllNodesList(t *testing.T) {
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddNode("profiles", &Node{Name: "profiles"})
	g.AddEdge("users", "orders")

	nodes := g.AllNodes()
	if len(nodes) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(nodes))
	}

	nodeSet := make(map[string]bool)
	for _, node := range nodes {
		nodeSet[node] = true
	}

	for _, expected := range []string{"users", "orders", "profiles"} {
		if !nodeSet[expected] {
			t.Errorf("Missing node %s in AllNodes", expected)
		}
	}
}

// TestPlanOutput_HasNode verifies node existence check
func TestPlanOutput_HasNode(t *testing.T) {
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders"})

	if !g.HasNode("users") {
		t.Error("HasNode('users') should return true")
	}
	if !g.HasNode("orders") {
		t.Error("HasNode('orders') should return true")
	}
	if g.HasNode("nonexistent") {
		t.Error("HasNode('nonexistent') should return false")
	}
}

// TestPlanOutput_CopyOrderPreservesAllTables verifies no tables are lost in copy order
func TestPlanOutput_CopyOrderPreservesAllTables(t *testing.T) {
	g := NewGraph("A", "id")
	g.AddNode("B", &Node{Name: "B"})
	g.AddNode("C", &Node{Name: "C"})
	g.AddNode("D", &Node{Name: "D"})
	g.AddNode("E", &Node{Name: "E"})
	g.AddEdge("A", "B")
	g.AddEdge("A", "C")
	g.AddEdge("B", "D")
	g.AddEdge("C", "E")

	copyOrder, err := g.CopyOrder()
	if err != nil {
		t.Fatalf("CopyOrder error: %v", err)
	}

	// Verify all 5 tables are in the output
	if len(copyOrder) != 5 {
		t.Errorf("Expected 5 tables, got %d", len(copyOrder))
	}

	// Verify no duplicates
	seen := make(map[string]bool)
	for _, table := range copyOrder {
		if seen[table] {
			t.Errorf("Duplicate table %s in copy order", table)
		}
		seen[table] = true
	}

	// Verify all expected tables are present
	expected := map[string]bool{"A": true, "B": true, "C": true, "D": true, "E": true}
	for table := range expected {
		if !seen[table] {
			t.Errorf("Missing table %s in copy order", table)
		}
	}
}

// TestPlanOutput_DeleteOrderPreservesAllTables verifies no tables are lost in delete order
func TestPlanOutput_DeleteOrderPreservesAllTables(t *testing.T) {
	g := NewGraph("A", "id")
	g.AddNode("B", &Node{Name: "B"})
	g.AddNode("C", &Node{Name: "C"})
	g.AddNode("D", &Node{Name: "D"})
	g.AddNode("E", &Node{Name: "E"})
	g.AddEdge("A", "B")
	g.AddEdge("A", "C")
	g.AddEdge("B", "D")
	g.AddEdge("C", "E")

	deleteOrder, err := g.DeleteOrder()
	if err != nil {
		t.Fatalf("DeleteOrder error: %v", err)
	}

	// Verify all 5 tables are in the output
	if len(deleteOrder) != 5 {
		t.Errorf("Expected 5 tables, got %d", len(deleteOrder))
	}

	// Verify no duplicates
	seen := make(map[string]bool)
	for _, table := range deleteOrder {
		if seen[table] {
			t.Errorf("Duplicate table %s in delete order", table)
		}
		seen[table] = true
	}

	// Verify all expected tables are present
	expected := map[string]bool{"A": true, "B": true, "C": true, "D": true, "E": true}
	for table := range expected {
		if !seen[table] {
			t.Errorf("Missing table %s in delete order", table)
		}
	}
}

// TestPlanOutput_CopyAndDeleteAreConsistent verifies copy and delete orders are consistent
func TestPlanOutput_CopyAndDeleteAreConsistent(t *testing.T) {
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddNode("order_items", &Node{Name: "order_items"})
	g.AddNode("profiles", &Node{Name: "profiles"})
	g.AddEdge("users", "orders")
	g.AddEdge("orders", "order_items")
	g.AddEdge("users", "profiles")

	copyOrder, err := g.CopyOrder()
	if err != nil {
		t.Fatalf("CopyOrder error: %v", err)
	}

	deleteOrder, err := g.DeleteOrder()
	if err != nil {
		t.Fatalf("DeleteOrder error: %v", err)
	}

	// Verify they have the same elements
	if len(copyOrder) != len(deleteOrder) {
		t.Fatalf("Copy and delete orders have different lengths: %d vs %d", len(copyOrder), len(deleteOrder))
	}

	copySet := make(map[string]int)
	deleteSet := make(map[string]int)

	for i, table := range copyOrder {
		copySet[table] = i
	}
	for i, table := range deleteOrder {
		deleteSet[table] = i
	}

	// Verify each table's position in delete is reversed from copy
	for table, copyPos := range copySet {
		deletePos, exists := deleteSet[table]
		if !exists {
			t.Errorf("Table %s in copy order but not in delete order", table)
			continue
		}
		expectedDeletePos := len(copyOrder) - 1 - copyPos
		if deletePos != expectedDeletePos {
			t.Errorf("Table %s: copy pos=%d, delete pos=%d, expected delete pos=%d",
				table, copyPos, deletePos, expectedDeletePos)
		}
	}
}

// TestPlanOutput_RootPKAccessible verifies root primary key is accessible
func TestPlanOutput_RootPKAccessible(t *testing.T) {
	g := NewGraph("customers", "customer_id")

	if g.Root != "customers" {
		t.Errorf("Expected Root='customers', got %q", g.Root)
	}
	if g.RootPK != "customer_id" {
		t.Errorf("Expected RootPK='customer_id', got %q", g.RootPK)
	}
}
