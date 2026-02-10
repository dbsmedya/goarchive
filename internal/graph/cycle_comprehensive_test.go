package graph

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// ============================================================================
// Large Cycle Scenarios (4+ tables)
// ============================================================================

// TestCycle_FourTableCycle tests detection of a 4-node cycle: A→B→C→D→A
func TestCycle_FourTableCycle(t *testing.T) {
	g := NewGraph("A", "id")
	g.AddNode("B", &Node{Name: "B"})
	g.AddNode("C", &Node{Name: "C"})
	g.AddNode("D", &Node{Name: "D"})

	// Create cycle: A → B → C → D → A
	g.AddEdge("A", "B")
	g.AddEdge("B", "C")
	g.AddEdge("C", "D")
	g.Children["D"] = append(g.Children["D"], "A")
	g.Parents["A"] = append(g.Parents["A"], "D")

	_, err := g.TopologicalSort()
	if err == nil {
		t.Fatal("Expected error for 4-table cycle")
	}

	cycleErr, ok := err.(*CycleError)
	if !ok {
		t.Fatalf("Expected *CycleError, got %T", err)
	}

	if cycleErr.Info.TotalNodes != 4 {
		t.Errorf("Expected 4 total nodes, got %d", cycleErr.Info.TotalNodes)
	}

	if len(cycleErr.Info.CycleParticipants) != 4 {
		t.Errorf("Expected 4 cycle participants, got %d", len(cycleErr.Info.CycleParticipants))
	}

	// Verify all nodes are in cycle participants
	participantSet := make(map[string]bool)
	for _, p := range cycleErr.Info.CycleParticipants {
		participantSet[p] = true
	}
	for _, node := range []string{"A", "B", "C", "D"} {
		if !participantSet[node] {
			t.Errorf("Expected %s to be in cycle participants", node)
		}
	}
}

// TestCycle_FiveTableCycle tests detection of a 5-node cycle
func TestCycle_FiveTableCycle(t *testing.T) {
	g := NewGraph("A", "id")
	for _, name := range []string{"B", "C", "D", "E"} {
		g.AddNode(name, &Node{Name: name})
	}

	// Create cycle: A → B → C → D → E → A
	g.AddEdge("A", "B")
	g.AddEdge("B", "C")
	g.AddEdge("C", "D")
	g.AddEdge("D", "E")
	g.Children["E"] = append(g.Children["E"], "A")
	g.Parents["A"] = append(g.Parents["A"], "E")

	cycleInfo := g.DetectIncompleteProcessing()
	if cycleInfo == nil {
		t.Fatal("Expected cycle detection for 5-table cycle")
	}

	if len(cycleInfo.CycleParticipants) != 5 {
		t.Errorf("Expected 5 cycle participants, got %d", len(cycleInfo.CycleParticipants))
	}

	// Verify cycle path exists and contains all nodes
	if len(cycleInfo.CyclePath) < 5 {
		t.Errorf("Expected cycle path with at least 5 nodes, got %d", len(cycleInfo.CyclePath))
	}
}

// TestCycle_TenTableCycle tests detection of a 10-node cycle (stress test)
func TestCycle_TenTableCycle(t *testing.T) {
	g := NewGraph(nodeName(0), "id")

	// Create 10 nodes
	for i := 1; i <= 9; i++ {
		g.AddNode(nodeName(i), &Node{Name: nodeName(i)})
	}

	// Create cycle: N0 → N1 → N2 → ... → N9 → N0
	for i := 0; i < 9; i++ {
		g.AddEdge(nodeName(i), nodeName(i+1))
	}
	g.Children[nodeName(9)] = append(g.Children[nodeName(9)], nodeName(0))
	g.Parents[nodeName(0)] = append(g.Parents[nodeName(0)], nodeName(9))

	cycleInfo := g.DetectIncompleteProcessing()
	if cycleInfo == nil {
		t.Fatal("Expected cycle detection for 10-table cycle")
	}

	if len(cycleInfo.CycleParticipants) != 10 {
		t.Errorf("Expected 10 cycle participants, got %d", len(cycleInfo.CycleParticipants))
	}

	// Verify all nodes N0-N9 are in cycle participants
	participantSet := make(map[string]bool)
	for _, p := range cycleInfo.CycleParticipants {
		participantSet[p] = true
	}
	for i := 0; i <= 9; i++ {
		if !participantSet[nodeName(i)] {
			t.Errorf("Expected %s to be in cycle participants", nodeName(i))
		}
	}

	// Verify error message contains cycle information
	cycleErr := &CycleError{Info: cycleInfo}
	msg := cycleErr.Error()
	if !strings.Contains(msg, "cycle detected") {
		t.Error("Error message should mention 'cycle detected'")
	}
}

// ============================================================================
// Multiple Disconnected Cycles
// ============================================================================

// TestCycle_TwoSeparateTwoTableCycles tests graph with two independent 2-node cycles
func TestCycle_TwoSeparateTwoTableCycles(t *testing.T) {
	// Graph structure:
	// Valid: root → child1
	// Cycle 1: A ↔ B
	// Cycle 2: C ↔ D
	g := NewGraph("root", "id")
	g.AddNode("child1", &Node{Name: "child1"})
	g.AddNode("A", &Node{Name: "A"})
	g.AddNode("B", &Node{Name: "B"})
	g.AddNode("C", &Node{Name: "C"})
	g.AddNode("D", &Node{Name: "D"})

	// Valid edge
	g.AddEdge("root", "child1")

	// First cycle: A ↔ B
	g.AddEdge("A", "B")
	g.Children["B"] = append(g.Children["B"], "A")
	g.Parents["A"] = append(g.Parents["A"], "B")

	// Second cycle: C ↔ D
	g.AddEdge("C", "D")
	g.Children["D"] = append(g.Children["D"], "C")
	g.Parents["C"] = append(g.Parents["C"], "D")

	cycleInfo := g.DetectIncompleteProcessing()
	if cycleInfo == nil {
		t.Fatal("Expected cycle detection")
	}

	// Should have processed root and child1
	if cycleInfo.ProcessedNodes != 2 {
		t.Errorf("Expected 2 processed nodes, got %d", cycleInfo.ProcessedNodes)
	}

	// Should have 4 unprocessed nodes (the cycles)
	if len(cycleInfo.UnprocessedNodes) != 4 {
		t.Errorf("Expected 4 unprocessed nodes, got %d", len(cycleInfo.UnprocessedNodes))
	}

	// All 4 should be cycle participants
	if len(cycleInfo.CycleParticipants) != 4 {
		t.Errorf("Expected 4 cycle participants, got %d", len(cycleInfo.CycleParticipants))
	}
}

// TestCycle_ValidDAGWithMultipleIsolatedCycles tests valid tree with separate cycles
func TestCycle_ValidDAGWithMultipleIsolatedCycles(t *testing.T) {
	// Structure:
	// root
	//   ├── valid1
	//   │     └── valid2
	//   ├── cycle1_A ↔ cycle1_B
	//   └── cycle2_A → cycle2_B → cycle2_C → cycle2_A
	g := NewGraph("root", "id")

	// Valid branch
	g.AddNode("valid1", &Node{Name: "valid1"})
	g.AddNode("valid2", &Node{Name: "valid2"})
	g.AddEdge("root", "valid1")
	g.AddEdge("valid1", "valid2")

	// First cycle: 2-node
	g.AddNode("cycle1_A", &Node{Name: "cycle1_A"})
	g.AddNode("cycle1_B", &Node{Name: "cycle1_B"})
	g.AddEdge("root", "cycle1_A")
	g.AddEdge("cycle1_A", "cycle1_B")
	g.Children["cycle1_B"] = append(g.Children["cycle1_B"], "cycle1_A")
	g.Parents["cycle1_A"] = append(g.Parents["cycle1_A"], "cycle1_B")

	// Second cycle: 3-node
	g.AddNode("cycle2_A", &Node{Name: "cycle2_A"})
	g.AddNode("cycle2_B", &Node{Name: "cycle2_B"})
	g.AddNode("cycle2_C", &Node{Name: "cycle2_C"})
	g.AddEdge("root", "cycle2_A")
	g.AddEdge("cycle2_A", "cycle2_B")
	g.AddEdge("cycle2_B", "cycle2_C")
	g.Children["cycle2_C"] = append(g.Children["cycle2_C"], "cycle2_A")
	g.Parents["cycle2_A"] = append(g.Parents["cycle2_A"], "cycle2_C")

	cycleInfo := g.DetectIncompleteProcessing()
	if cycleInfo == nil {
		t.Fatal("Expected cycle detection")
	}

	// Should process root, valid1, valid2 (3 nodes)
	if cycleInfo.ProcessedNodes != 3 {
		t.Errorf("Expected 3 processed nodes, got %d", cycleInfo.ProcessedNodes)
	}

	// Should have 5 unprocessed (2 in first cycle + 3 in second)
	if len(cycleInfo.UnprocessedNodes) != 5 {
		t.Errorf("Expected 5 unprocessed nodes, got %d", len(cycleInfo.UnprocessedNodes))
	}
}

// ============================================================================
// Diamond Pattern Cycles
// ============================================================================

// TestCycle_DiamondWithCrossEdgeCycle tests diamond pattern that creates cycle
func TestCycle_DiamondWithCrossEdgeCycle(t *testing.T) {
	// Diamond pattern:
	//     A
	//    / \
	//   B   C
	//    \ /
	//     D
	// With cross edge: D → B (creates cycle B → D → B)
	g := NewGraph("A", "id")
	g.AddNode("B", &Node{Name: "B"})
	g.AddNode("C", &Node{Name: "C"})
	g.AddNode("D", &Node{Name: "D"})

	g.AddEdge("A", "B")
	g.AddEdge("A", "C")
	g.AddEdge("B", "D")
	g.AddEdge("C", "D")

	// Cross edge creates cycle
	g.Children["D"] = append(g.Children["D"], "B")
	g.Parents["B"] = append(g.Parents["B"], "D")

	cycleInfo := g.DetectIncompleteProcessing()
	if cycleInfo == nil {
		t.Fatal("Expected cycle detection for diamond with cross edge")
	}

	// B and D should be cycle participants
	participantSet := make(map[string]bool)
	for _, p := range cycleInfo.CycleParticipants {
		participantSet[p] = true
	}
	if !participantSet["B"] || !participantSet["D"] {
		t.Error("B and D should be cycle participants")
	}

	// A and C can be processed before hitting the cycle (B and D)
	// A has in-degree 0, C has in-degree 1 (from A) and can be processed after A
	if cycleInfo.ProcessedNodes != 2 { // A and C processed
		t.Errorf("Expected 2 processed nodes (A, C), got %d", cycleInfo.ProcessedNodes)
	}
}

// TestCycle_DiamondWithBottomToTopCycle tests diamond with bottom-to-top edge
func TestCycle_DiamondWithBottomToTopCycle(t *testing.T) {
	//     A
	//    / \
	//   B   C
	//    \ /
	//     D
	// With edge: D → A (creates cycle A→B→D→A and A→C→D→A)
	g := NewGraph("A", "id")
	g.AddNode("B", &Node{Name: "B"})
	g.AddNode("C", &Node{Name: "C"})
	g.AddNode("D", &Node{Name: "D"})

	g.AddEdge("A", "B")
	g.AddEdge("A", "C")
	g.AddEdge("B", "D")
	g.AddEdge("C", "D")

	// D → A creates cycle involving all nodes
	g.Children["D"] = append(g.Children["D"], "A")
	g.Parents["A"] = append(g.Parents["A"], "D")

	cycleInfo := g.DetectIncompleteProcessing()
	if cycleInfo == nil {
		t.Fatal("Expected cycle detection")
	}

	// All 4 nodes should be in cycle
	if len(cycleInfo.CycleParticipants) != 4 {
		t.Errorf("Expected 4 cycle participants, got %d", len(cycleInfo.CycleParticipants))
	}
}

// ============================================================================
// Deeply Nested Cycle Scenarios
// ============================================================================

// TestCycle_DeepHierarchyWithCycleAtLeaf tests deep tree with cycle at leaves
func TestCycle_DeepHierarchyWithCycleAtLeaf(t *testing.T) {
	// Structure:
	// root
	//   └── L1
	//        └── L2
	//             └── L3
	//                  ├── L4_valid
	//                  └── L4_cycle_A ↔ L4_cycle_B
	g := NewGraph("root", "id")

	// Build deep chain
	levels := []string{"root", "L1", "L2", "L3"}
	for i := 1; i < len(levels); i++ {
		g.AddNode(levels[i], &Node{Name: levels[i]})
		g.AddEdge(levels[i-1], levels[i])
	}

	// Valid leaf
	g.AddNode("L4_valid", &Node{Name: "L4_valid"})
	g.AddEdge("L3", "L4_valid")

	// Cyclic leaves
	g.AddNode("L4_cycle_A", &Node{Name: "L4_cycle_A"})
	g.AddNode("L4_cycle_B", &Node{Name: "L4_cycle_B"})
	g.AddEdge("L3", "L4_cycle_A")
	g.AddEdge("L4_cycle_A", "L4_cycle_B")
	g.Children["L4_cycle_B"] = append(g.Children["L4_cycle_B"], "L4_cycle_A")
	g.Parents["L4_cycle_A"] = append(g.Parents["L4_cycle_A"], "L4_cycle_B")

	cycleInfo := g.DetectIncompleteProcessing()
	if cycleInfo == nil {
		t.Fatal("Expected cycle detection")
	}

	// Should process root, L1, L2, L3, L4_valid (5 nodes)
	if cycleInfo.ProcessedNodes != 5 {
		t.Errorf("Expected 5 processed nodes, got %d", cycleInfo.ProcessedNodes)
	}

	// Cycle participants should be the two leaf cycle nodes
	if len(cycleInfo.CycleParticipants) != 2 {
		t.Errorf("Expected 2 cycle participants, got %d", len(cycleInfo.CycleParticipants))
	}
}

// TestCycle_CycleInMiddleOfHierarchy tests cycle in middle blocking descendants
func TestCycle_CycleInMiddleOfHierarchy(t *testing.T) {
	// Structure:
	// root
	//   ├── valid_branch
	//   └── cycle_A → cycle_B → cycle_A (cycle)
	//              \
	//               └── blocked_child (blocked by cycle)
	g := NewGraph("root", "id")

	// Valid branch
	g.AddNode("valid_branch", &Node{Name: "valid_branch"})
	g.AddEdge("root", "valid_branch")

	// Cycle in middle
	g.AddNode("cycle_A", &Node{Name: "cycle_A"})
	g.AddNode("cycle_B", &Node{Name: "cycle_B"})
	g.AddEdge("root", "cycle_A")
	g.AddEdge("cycle_A", "cycle_B")
	g.Children["cycle_B"] = append(g.Children["cycle_B"], "cycle_A")
	g.Parents["cycle_A"] = append(g.Parents["cycle_A"], "cycle_B")

	// Child of cycle node (blocked)
	g.AddNode("blocked_child", &Node{Name: "blocked_child"})
	g.AddEdge("cycle_A", "blocked_child")

	cycleInfo := g.DetectIncompleteProcessing()
	if cycleInfo == nil {
		t.Fatal("Expected cycle detection")
	}

	// Should process root and valid_branch
	if cycleInfo.ProcessedNodes != 2 {
		t.Errorf("Expected 2 processed nodes, got %d", cycleInfo.ProcessedNodes)
	}

	// Unprocessed: cycle_A, cycle_B, blocked_child
	if len(cycleInfo.UnprocessedNodes) != 3 {
		t.Errorf("Expected 3 unprocessed nodes, got %d", len(cycleInfo.UnprocessedNodes))
	}

	// Cycle participants: cycle_A, cycle_B (blocked_child is just blocked)
	if len(cycleInfo.CycleParticipants) != 2 {
		t.Errorf("Expected 2 cycle participants, got %d", len(cycleInfo.CycleParticipants))
	}

	// Verify blocked_child is in unprocessed but not cycle participants
	unprocessedSet := make(map[string]bool)
	for _, u := range cycleInfo.UnprocessedNodes {
		unprocessedSet[u] = true
	}
	if !unprocessedSet["blocked_child"] {
		t.Error("blocked_child should be in unprocessed nodes")
	}

	participantSet := make(map[string]bool)
	for _, p := range cycleInfo.CycleParticipants {
		participantSet[p] = true
	}
	if participantSet["blocked_child"] {
		t.Error("blocked_child should NOT be a cycle participant")
	}
}

// ============================================================================
// Performance/Scale Tests
// ============================================================================

// TestCycle_LargeGraphWithNoCycle tests performance on large valid DAG
func TestCycle_LargeGraphWithNoCycle(t *testing.T) {
	// Create a large balanced tree: 127 nodes in a perfect binary tree
	g := NewGraph(nodeName(0), "id")

	// Build a binary tree with 127 nodes (2^7 - 1)
	// In a binary heap: node i has children at 2i+1 and 2i+2
	// N0: children N1, N2
	// N1: children N3, N4
	// N2: children N5, N6
	// etc.
	totalNodes := 127
	for i := 0; i < totalNodes; i++ {
		leftChild := 2*i + 1
		rightChild := 2*i + 2

		if leftChild < totalNodes {
			g.AddNode(nodeName(leftChild), &Node{Name: nodeName(leftChild)})
			g.AddEdge(nodeName(i), nodeName(leftChild))
		}
		if rightChild < totalNodes {
			g.AddNode(nodeName(rightChild), &Node{Name: nodeName(rightChild)})
			g.AddEdge(nodeName(i), nodeName(rightChild))
		}
	}

	// Verify node count
	if g.NodeCount() != totalNodes {
		t.Errorf("Expected %d nodes, got %d", totalNodes, g.NodeCount())
	}

	// Time the cycle detection
	start := time.Now()
	cycleInfo := g.DetectIncompleteProcessing()
	duration := time.Since(start)

	if cycleInfo != nil {
		t.Errorf("Expected no cycle in valid tree, got: %v", cycleInfo)
	}

	// Should complete in reasonable time (< 100ms for 127 nodes)
	if duration > 100*time.Millisecond {
		t.Errorf("Cycle detection too slow: %v for 127 nodes", duration)
	}
}

// TestCycle_LargeGraphWithSingleCycle tests performance on large graph with one cycle
func TestCycle_LargeGraphWithSingleCycle(t *testing.T) {
	// Create a chain: N0 → N1 → N2 → ... → N49
	// Then add edge N49 → N25 to create a cycle
	// N0 through N24 can be processed (25 nodes)
	// N25 through N49 form the cycle (25 nodes)
	g := NewGraph(nodeName(0), "id")

	// Create a chain of 50 nodes: N0 -> N1 -> ... -> N49
	for i := 1; i < 50; i++ {
		g.AddNode(nodeName(i), &Node{Name: nodeName(i)})
		g.AddEdge(nodeName(i-1), nodeName(i))
	}

	// Add cycle: N49 → N25 (creates cycle N25 → N26 → ... → N49 → N25)
	g.Children[nodeName(49)] = append(g.Children[nodeName(49)], nodeName(25))
	g.Parents[nodeName(25)] = append(g.Parents[nodeName(25)], nodeName(49))

	start := time.Now()
	cycleInfo := g.DetectIncompleteProcessing()
	duration := time.Since(start)

	if cycleInfo == nil {
		t.Fatal("Expected cycle detection")
	}

	// T0-T24 can be processed (outside the cycle), T25-T49 are in the cycle
	if cycleInfo.ProcessedNodes != 25 {
		t.Errorf("Expected 25 processed nodes (T0-T24), got %d", cycleInfo.ProcessedNodes)
	}

	if len(cycleInfo.UnprocessedNodes) != 25 {
		t.Errorf("Expected 25 unprocessed nodes (T25-T49 in cycle), got %d", len(cycleInfo.UnprocessedNodes))
	}

	// Should complete reasonably fast
	if duration > 100*time.Millisecond {
		t.Errorf("Cycle detection too slow: %v for 50 nodes with cycle", duration)
	}
}

// ============================================================================
// Edge Cases
// ============================================================================

// TestCycle_SingleNodeNoEdges tests single node without any edges (valid)
func TestCycle_SingleNodeNoEdges(t *testing.T) {
	g := NewGraph("only_node", "id")

	// Single node with no children is valid
	cycleInfo := g.DetectIncompleteProcessing()
	if cycleInfo != nil {
		t.Error("Single node without edges should be valid")
	}

	// Should be able to topologically sort
	order, err := g.TopologicalSort()
	if err != nil {
		t.Errorf("Single node should sort successfully: %v", err)
	}
	if len(order) != 1 || order[0] != "only_node" {
		t.Errorf("Expected [only_node], got %v", order)
	}
}

// TestCycle_SingleNodeSelfCycle tests single node with self-reference
func TestCycle_SingleNodeSelfCycle(t *testing.T) {
	g := NewGraph("self_ref", "id")

	// Add self-reference
	g.Children["self_ref"] = append(g.Children["self_ref"], "self_ref")
	g.Parents["self_ref"] = append(g.Parents["self_ref"], "self_ref")

	cycleInfo := g.DetectIncompleteProcessing()
	if cycleInfo == nil {
		t.Fatal("Expected cycle detection for self-referencing node")
	}

	if len(cycleInfo.CycleParticipants) != 1 {
		t.Errorf("Expected 1 cycle participant, got %d", len(cycleInfo.CycleParticipants))
	}

	if cycleInfo.CycleParticipants[0] != "self_ref" {
		t.Errorf("Expected self_ref as participant, got %s", cycleInfo.CycleParticipants[0])
	}

	// Cycle path should be [self_ref, self_ref]
	if len(cycleInfo.CyclePath) != 2 {
		t.Errorf("Expected cycle path of length 2, got %d", len(cycleInfo.CyclePath))
	}
}

// TestCycle_TwoNodesNoEdges tests two disconnected nodes (valid DAG)
func TestCycle_TwoNodesNoEdges(t *testing.T) {
	g := NewGraph("node_a", "id")
	g.AddNode("node_b", &Node{Name: "node_b"})

	// No edges between them - this is a valid DAG
	cycleInfo := g.DetectIncompleteProcessing()
	if cycleInfo != nil {
		t.Error("Two disconnected nodes should be valid")
	}

	// Should sort (order may vary but both should be present)
	order, err := g.TopologicalSort()
	if err != nil {
		t.Errorf("Should sort successfully: %v", err)
	}
	if len(order) != 2 {
		t.Errorf("Expected 2 nodes in order, got %d", len(order))
	}
}

// TestCycle_TwoNodesOneWay tests A → B (valid)
func TestCycle_TwoNodesOneWay(t *testing.T) {
	g := NewGraph("A", "id")
	g.AddNode("B", &Node{Name: "B"})
	g.AddEdge("A", "B")

	cycleInfo := g.DetectIncompleteProcessing()
	if cycleInfo != nil {
		t.Error("A → B should be valid (no cycle)")
	}

	order, err := g.TopologicalSort()
	if err != nil {
		t.Errorf("Should sort successfully: %v", err)
	}
	if len(order) != 2 || order[0] != "A" || order[1] != "B" {
		t.Errorf("Expected [A, B], got %v", order)
	}
}

// TestCycle_EmptyGraph tests handling of effectively empty graph
func TestCycle_RootOnly(t *testing.T) {
	g := NewGraph("root", "id")

	cycleInfo := g.DetectIncompleteProcessing()
	if cycleInfo != nil {
		t.Error("Root-only graph should have no cycles")
	}

	if !g.HasCycle() {
		t.Log("HasCycle() correctly returns false for root-only graph")
	} else {
		t.Error("HasCycle() should return false for root-only graph")
	}
}

// ============================================================================
// Mixed Valid/Cyclic Subgraphs
// ============================================================================

// TestCycle_MultipleValidBranchesWithOneCyclicBranch tests tree with one bad branch
func TestCycle_MultipleValidBranchesWithOneCyclicBranch(t *testing.T) {
	// Structure:
	//        root
	//       / | \
	//     v1  v2  c1
	//    /    |    \
	//   v3    v4   c2
	//              /
	//             c1 (cycle back)
	g := NewGraph("root", "id")

	// Valid branches using N1, N2, etc.
	for i := 1; i <= 4; i++ {
		g.AddNode(nodeName(i), &Node{Name: nodeName(i)})
	}
	g.AddEdge("root", nodeName(1))
	g.AddEdge("root", nodeName(2))
	g.AddEdge(nodeName(1), nodeName(3))
	g.AddEdge(nodeName(2), nodeName(4))

	// Cyclic branch
	g.AddNode("c1", &Node{Name: "c1"})
	g.AddNode("c2", &Node{Name: "c2"})
	g.AddEdge("root", "c1")
	g.AddEdge("c1", "c2")
	g.Children["c2"] = append(g.Children["c2"], "c1")
	g.Parents["c1"] = append(g.Parents["c1"], "c2")

	cycleInfo := g.DetectIncompleteProcessing()
	if cycleInfo == nil {
		t.Fatal("Expected cycle detection")
	}

	// Should process root, N1, N2, N3, N4
	if cycleInfo.ProcessedNodes != 5 {
		t.Errorf("Expected 5 processed nodes, got %d", cycleInfo.ProcessedNodes)
	}

	// Unprocessed: c1, c2
	if len(cycleInfo.UnprocessedNodes) != 2 {
		t.Errorf("Expected 2 unprocessed nodes, got %d", len(cycleInfo.UnprocessedNodes))
	}
}

// TestCycle_CycleWithMultipleEntryPoints tests cycle accessible from multiple parents
func TestCycle_CycleWithMultipleEntryPoints(t *testing.T) {
	// Structure:
	//      root
	//      /  \
	//     A    B
	//      \  /
	//       C
	//       |
	//       D
	//       |
	//       E
	//       |
	//       C (cycle back: E → C creates cycle C→D→E→C)
	g := NewGraph("root", "id")
	g.AddNode("A", &Node{Name: "A"})
	g.AddNode("B", &Node{Name: "B"})
	g.AddNode("C", &Node{Name: "C"})
	g.AddNode("D", &Node{Name: "D"})
	g.AddNode("E", &Node{Name: "E"})

	g.AddEdge("root", "A")
	g.AddEdge("root", "B")
	g.AddEdge("A", "C")
	g.AddEdge("B", "C")
	g.AddEdge("C", "D")
	g.AddEdge("D", "E")

	// Create cycle E → C (cycle is C→D→E→C)
	g.Children["E"] = append(g.Children["E"], "C")
	g.Parents["C"] = append(g.Parents["C"], "E")

	cycleInfo := g.DetectIncompleteProcessing()
	if cycleInfo == nil {
		t.Fatal("Expected cycle detection")
	}

	// root, A, B can be processed (before cycle)
	if cycleInfo.ProcessedNodes != 3 {
		t.Errorf("Expected 3 processed nodes (root, A, B), got %d", cycleInfo.ProcessedNodes)
	}

	// C, D, E are in the cycle
	if len(cycleInfo.UnprocessedNodes) != 3 {
		t.Errorf("Expected 3 unprocessed nodes (C, D, E in cycle), got %d", len(cycleInfo.UnprocessedNodes))
	}

	// Verify cycle participants
	participantSet := make(map[string]bool)
	for _, p := range cycleInfo.CycleParticipants {
		participantSet[p] = true
	}
	if !participantSet["C"] || !participantSet["D"] || !participantSet["E"] {
		t.Error("C, D, E should all be cycle participants")
	}
}

// ============================================================================
// Cycle Path Accuracy Tests
// ============================================================================

// TestCycle_PathAccuracy_TwoNode verifies cycle path for 2-node cycle
func TestCycle_PathAccuracy_TwoNode(t *testing.T) {
	g := NewGraph("A", "id")
	g.AddNode("B", &Node{Name: "B"})

	g.AddEdge("A", "B")
	g.Children["B"] = append(g.Children["B"], "A")
	g.Parents["A"] = append(g.Parents["A"], "B")

	cycleInfo := g.DetectIncompleteProcessing()
	if cycleInfo == nil {
		t.Fatal("Expected cycle detection")
	}

	// Path should start and end with same node
	if len(cycleInfo.CyclePath) < 2 {
		t.Fatalf("Expected cycle path with at least 2 nodes, got %d", len(cycleInfo.CyclePath))
	}

	first := cycleInfo.CyclePath[0]
	last := cycleInfo.CyclePath[len(cycleInfo.CyclePath)-1]
	if first != last {
		t.Errorf("Cycle path should start and end with same node: %s vs %s", first, last)
	}

	// Path should contain both A and B
	pathSet := make(map[string]bool)
	for _, node := range cycleInfo.CyclePath {
		pathSet[node] = true
	}
	if !pathSet["A"] || !pathSet["B"] {
		t.Error("Cycle path should contain both A and B")
	}
}

// TestCycle_PathAccuracy_ThreeNode verifies cycle path for 3-node cycle
func TestCycle_PathAccuracy_ThreeNode(t *testing.T) {
	g := NewGraph("A", "id")
	g.AddNode("B", &Node{Name: "B"})
	g.AddNode("C", &Node{Name: "C"})

	g.AddEdge("A", "B")
	g.AddEdge("B", "C")
	g.Children["C"] = append(g.Children["C"], "A")
	g.Parents["A"] = append(g.Parents["A"], "C")

	cycleInfo := g.DetectIncompleteProcessing()
	if cycleInfo == nil {
		t.Fatal("Expected cycle detection")
	}

	// Verify path contains all 3 nodes in some order that forms a cycle
	if len(cycleInfo.CyclePath) < 3 {
		t.Errorf("Expected cycle path with at least 3 nodes, got %d", len(cycleInfo.CyclePath))
	}

	// Check error message contains the path
	cycleErr := &CycleError{Info: cycleInfo}
	msg := cycleErr.Error()
	if !strings.Contains(msg, "Cycle path:") {
		t.Error("Error message should contain 'Cycle path:'")
	}
}

// ============================================================================
// Helper Functions
// ============================================================================

// nodeName generates a node name from an index (N0, N1, N2, ...)
func nodeName(i int) string {
	return fmt.Sprintf("N%d", i)
}
