package graph

import (
	"sort"
	"testing"
)

// edgeCount, leafNodes, inDegree, and outDegree are in-package test helpers
// preserving the bodies of the deleted Graph.EdgeCount/LeafNodes/InDegree/
// OutDegree methods (dead-code cleanup, issue #9) for assertions in this and
// other _test.go files in package graph.

func edgeCount(g *Graph) int {
	count := 0
	for _, children := range g.Children {
		count += len(children)
	}
	return count
}

func leafNodes(g *Graph) []string {
	var leaves []string
	for name := range g.Nodes {
		if len(g.Children[name]) == 0 {
			leaves = append(leaves, name)
		}
	}
	return leaves
}

func inDegree(g *Graph, name string) int {
	return len(g.Parents[name])
}

func outDegree(g *Graph, name string) int {
	return len(g.Children[name])
}

func TestNewGraph(t *testing.T) {
	g := NewGraph("orders", "id")

	if g == nil {
		t.Fatal("NewGraph() returned nil")
	}

	if g.Root != "orders" {
		t.Errorf("expected root 'orders', got %q", g.Root)
	}

	if g.RootPK != "id" {
		t.Errorf("expected root PK 'id', got %q", g.RootPK)
	}

	if g.Nodes == nil {
		t.Error("Nodes map is nil")
	}

	if g.Children == nil {
		t.Error("Children map is nil")
	}

	if g.Parents == nil {
		t.Error("Parents map is nil")
	}

	// Root node should be automatically added
	if _, exists := g.Nodes["orders"]; !exists {
		t.Error("root node should be automatically added to graph")
	}

	// Root node should have IsRoot = true
	rootNode := g.Nodes["orders"]
	if !rootNode.IsRoot {
		t.Error("root node should have IsRoot = true")
	}
}

func TestGraph_RootPKMeta(t *testing.T) {
	g := NewGraph("users", "id")
	dataType, unsigned, ok := g.GetRootPKMeta()
	if ok {
		t.Fatalf("expected ok=false for unset metadata, got dataType=%q unsigned=%v", dataType, unsigned)
	}

	g.SetRootPKMeta("bigint", true)
	dataType, unsigned, ok = g.GetRootPKMeta()
	if !ok {
		t.Fatal("expected ok=true after SetRootPKMeta")
	}
	if dataType != "bigint" || !unsigned {
		t.Fatalf("metadata: dataType=%q unsigned=%v, want bigint/true", dataType, unsigned)
	}
}

func TestAddNode(t *testing.T) {
	g := NewGraph("orders", "id")

	// Test adding a node with nil
	g.AddNode("order_items", nil)
	if _, exists := g.Nodes["order_items"]; !exists {
		t.Error("AddNode with nil should create node")
	}
	if g.Nodes["order_items"].Name != "order_items" {
		t.Error("node should have correct name")
	}

	// Test adding a node with values
	node := &Node{
		ForeignKey:     "order_id",
		ReferenceKey:   "id",
		DependencyType: "1-N",
	}
	g.AddNode("payments", node)

	if g.Nodes["payments"].ForeignKey != "order_id" {
		t.Error("node should have correct ForeignKey")
	}
	if g.Nodes["payments"].ReferenceKey != "id" {
		t.Error("node should have correct ReferenceKey")
	}
	if g.Nodes["payments"].DependencyType != "1-N" {
		t.Error("node should have correct DependencyType")
	}

	// Test that Name is set even if not provided in node
	g.AddNode("shipments", &Node{})
	if g.Nodes["shipments"].Name != "shipments" {
		t.Error("Name should be set from parameter")
	}
}

func TestAddEdge(t *testing.T) {
	g := NewGraph("orders", "id")
	g.AddNode("order_items", nil)
	g.AddNode("payments", nil)

	// Add edge
	g.AddEdge("orders", "order_items")

	// Check children
	children := g.Children["orders"]
	if len(children) != 1 || children[0] != "order_items" {
		t.Errorf("expected orders to have 1 child (order_items), got %v", children)
	}

	// Check parents (reverse mapping)
	parents := g.Parents["order_items"]
	if len(parents) != 1 || parents[0] != "orders" {
		t.Errorf("expected order_items to have 1 parent (orders), got %v", parents)
	}

	// Add another edge
	g.AddEdge("orders", "payments")

	children = g.Children["orders"]
	if len(children) != 2 {
		t.Errorf("expected orders to have 2 children, got %d", len(children))
	}
}

func TestAddEdgeWithMeta(t *testing.T) {
	g := NewGraph("orders", "id")
	g.AddNode("order_items", nil)

	g.AddEdgeWithMeta("orders", "order_items", "order_id", "id", "1-N")

	// Check edge exists
	children := g.GetChildren("orders")
	if len(children) != 1 {
		t.Fatal("edge should be added")
	}

	// Check metadata
	meta := g.GetEdgeMeta("orders", "order_items")
	if meta == nil {
		t.Fatal("edge metadata should exist")
	}

	if meta.ForeignKey != "order_id" {
		t.Errorf("expected ForeignKey 'order_id', got %q", meta.ForeignKey)
	}

	if meta.ReferenceKey != "id" {
		t.Errorf("expected ReferenceKey 'id', got %q", meta.ReferenceKey)
	}

	if meta.DependencyType != "1-N" {
		t.Errorf("expected DependencyType '1-N', got %q", meta.DependencyType)
	}
}

func TestGetChildren(t *testing.T) {
	g := NewGraph("orders", "id")
	g.AddNode("order_items", nil)
	g.AddNode("payments", nil)
	g.AddNode("shipments", nil)

	g.AddEdge("orders", "order_items")
	g.AddEdge("orders", "payments")

	children := g.GetChildren("orders")
	if len(children) != 2 {
		t.Errorf("expected 2 children, got %d", len(children))
	}

	// Check that both children are present
	childSet := make(map[string]bool)
	for _, c := range children {
		childSet[c] = true
	}
	if !childSet["order_items"] || !childSet["payments"] {
		t.Error("expected order_items and payments as children")
	}

	// Check non-existent parent returns nil/empty
	noChildren := g.GetChildren("nonexistent")
	if len(noChildren) > 0 {
		t.Error("non-existent parent should return empty children")
	}
}

func TestGetParents(t *testing.T) {
	g := NewGraph("orders", "id")
	g.AddNode("order_items", nil)
	g.AddNode("order_item_details", nil)

	g.AddEdge("orders", "order_items")
	g.AddEdge("order_items", "order_item_details")

	// order_items should have orders as parent
	parents := g.GetParents("order_items")
	if len(parents) != 1 || parents[0] != "orders" {
		t.Errorf("expected order_items to have orders as parent, got %v", parents)
	}

	// order_item_details should have order_items as parent
	parents = g.GetParents("order_item_details")
	if len(parents) != 1 || parents[0] != "order_items" {
		t.Errorf("expected order_item_details to have order_items as parent, got %v", parents)
	}

	// Root should have no parents
	parents = g.GetParents("orders")
	if len(parents) > 0 {
		t.Errorf("root should have no parents, got %v", parents)
	}
}

func TestGetNode(t *testing.T) {
	g := NewGraph("orders", "id")

	node := g.GetNode("orders")
	if node == nil {
		t.Fatal("GetNode should return root node")
	}
	if node.Name != "orders" {
		t.Error("node should have correct name")
	}

	// Non-existent node
	nonExistent := g.GetNode("nonexistent")
	if nonExistent != nil {
		t.Error("non-existent node should return nil")
	}
}

func TestHasNode(t *testing.T) {
	g := NewGraph("orders", "id")
	g.AddNode("order_items", nil)

	if !g.HasNode("orders") {
		t.Error("should have root node")
	}

	if !g.HasNode("order_items") {
		t.Error("should have added node")
	}

	if g.HasNode("nonexistent") {
		t.Error("should not have non-existent node")
	}
}

func TestNodeCount(t *testing.T) {
	g := NewGraph("orders", "id")
	if g.NodeCount() != 1 {
		t.Errorf("expected 1 node (root), got %d", g.NodeCount())
	}

	g.AddNode("order_items", nil)
	if g.NodeCount() != 2 {
		t.Errorf("expected 2 nodes, got %d", g.NodeCount())
	}

	g.AddNode("payments", nil)
	if g.NodeCount() != 3 {
		t.Errorf("expected 3 nodes, got %d", g.NodeCount())
	}
}

func TestAllNodes(t *testing.T) {
	g := NewGraph("orders", "id")
	g.AddNode("order_items", nil)
	g.AddNode("payments", nil)

	nodes := g.AllNodes()
	if len(nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(nodes))
	}

	nodeSet := make(map[string]bool)
	for _, n := range nodes {
		nodeSet[n] = true
	}

	if !nodeSet["orders"] || !nodeSet["order_items"] || !nodeSet["payments"] {
		t.Error("AllNodes should return all table names")
	}
}

func TestAllEdges(t *testing.T) {
	g := NewGraph("orders", "id")
	g.AddNode("order_items", nil)
	g.AddNode("payments", nil)

	g.AddEdge("orders", "order_items")
	g.AddEdge("orders", "payments")

	edges := g.AllEdges()
	if len(edges) != 2 {
		t.Errorf("expected 2 edges, got %d", len(edges))
	}

	// Check that both edges are present
	hasOrderItems := false
	hasPayments := false
	for _, e := range edges {
		if e.From == "orders" && e.To == "order_items" {
			hasOrderItems = true
		}
		if e.From == "orders" && e.To == "payments" {
			hasPayments = true
		}
	}

	if !hasOrderItems || !hasPayments {
		t.Error("AllEdges should return all edges")
	}
}

func TestEmptyGraphOperations(t *testing.T) {
	g := NewGraph("root", "id")

	// Operations on empty graph should not panic
	children := g.GetChildren("nonexistent")
	if len(children) > 0 {
		t.Error("GetChildren on non-existent node should return empty")
	}

	parents := g.GetParents("nonexistent")
	if len(parents) > 0 {
		t.Error("GetParents on non-existent node should return empty")
	}

	node := g.GetNode("nonexistent")
	if node != nil {
		t.Error("GetNode on non-existent node should return nil")
	}

	if g.HasNode("nonexistent") {
		t.Error("HasNode on non-existent node should return false")
	}

	edges := g.AllEdges()
	if len(edges) != 0 {
		t.Error("AllEdges on empty graph should return empty slice")
	}
}

func TestComplexGraph(t *testing.T) {
	// Create a more complex graph:
	// orders -> order_items -> order_item_details
	//        -> payments
	//        -> shipments
	g := NewGraph("orders", "id")
	g.AddNode("order_items", nil)
	g.AddNode("order_item_details", nil)
	g.AddNode("payments", nil)
	g.AddNode("shipments", nil)

	g.AddEdge("orders", "order_items")
	g.AddEdge("orders", "payments")
	g.AddEdge("orders", "shipments")
	g.AddEdge("order_items", "order_item_details")

	// Verify structure
	if g.NodeCount() != 5 {
		t.Errorf("expected 5 nodes, got %d", g.NodeCount())
	}

	if edgeCount(g) != 4 {
		t.Errorf("expected 4 edges, got %d", edgeCount(g))
	}

	// Verify orders children
	orderChildren := g.GetChildren("orders")
	if len(orderChildren) != 3 {
		t.Errorf("expected 3 children of orders, got %d", len(orderChildren))
	}

	// Verify leaf nodes
	leaves := leafNodes(g)
	if len(leaves) != 3 {
		t.Errorf("expected 3 leaves (order_item_details, payments, shipments), got %d: %v", len(leaves), leaves)
	}

	// Sort for consistent comparison
	sort.Strings(leaves)
	expected := []string{"order_item_details", "payments", "shipments"}
	for i, leaf := range leaves {
		if leaf != expected[i] {
			t.Errorf("expected leaf %q, got %q", expected[i], leaf)
		}
	}
}

func TestNodeStructFields(t *testing.T) {
	node := &Node{
		Name:           "test_table",
		ForeignKey:     "order_id",
		ReferenceKey:   "id",
		DependencyType: "1-N",
		IsRoot:         false,
	}

	if node.Name != "test_table" {
		t.Error("Name field not set correctly")
	}
	if node.ForeignKey != "order_id" {
		t.Error("ForeignKey field not set correctly")
	}
	if node.ReferenceKey != "id" {
		t.Error("ReferenceKey field not set correctly")
	}
	if node.DependencyType != "1-N" {
		t.Error("DependencyType field not set correctly")
	}
	if node.IsRoot {
		t.Error("IsRoot field not set correctly")
	}
}

func TestEdgeStructFields(t *testing.T) {
	edge := Edge{
		From: "orders",
		To:   "order_items",
	}

	if edge.From != "orders" {
		t.Error("From field not set correctly")
	}
	if edge.To != "order_items" {
		t.Error("To field not set correctly")
	}
}

func TestEdgeMetaStructFields(t *testing.T) {
	meta := &EdgeMeta{
		ForeignKey:     "order_id",
		ReferenceKey:   "id",
		DependencyType: "1-1",
	}

	if meta.ForeignKey != "order_id" {
		t.Error("ForeignKey field not set correctly")
	}
	if meta.ReferenceKey != "id" {
		t.Error("ReferenceKey field not set correctly")
	}
	if meta.DependencyType != "1-1" {
		t.Error("DependencyType field not set correctly")
	}
}

func TestGetEdgeMetaNonExistent(t *testing.T) {
	g := NewGraph("orders", "id")

	meta := g.GetEdgeMeta("orders", "nonexistent")
	if meta != nil {
		t.Error("GetEdgeMeta on non-existent edge should return nil")
	}
}
