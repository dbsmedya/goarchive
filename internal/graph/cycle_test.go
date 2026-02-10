package graph

import (
	"strings"
	"testing"
)

// ============================================================================
// CycleError Error Message Tests (GA-P2-F3-T3)
// ============================================================================

func TestCycleError_ImplementsErrorInterface(t *testing.T) {
	// Verify CycleError implements the error interface
	info := &CycleInfo{
		TotalNodes:        3,
		ProcessedNodes:    0,
		UnprocessedNodes:  []string{"A", "B", "C"},
		CycleParticipants: []string{"A", "B", "C"},
	}
	cycleErr := &CycleError{Info: info}

	// Verify CycleError implements error interface (compile-time check)
	var _ error = cycleErr
}

func TestCycleError_ErrorMessage_ContainsBasicInfo(t *testing.T) {
	info := &CycleInfo{
		TotalNodes:        5,
		ProcessedNodes:    2,
		UnprocessedNodes:  []string{"A", "B", "C"},
		CycleParticipants: []string{"A", "B"},
	}
	cycleErr := &CycleError{Info: info}

	msg := cycleErr.Error()

	// Should contain basic cycle info
	if !strings.Contains(msg, "cycle detected") {
		t.Error("Error message should contain 'cycle detected'")
	}
	if !strings.Contains(msg, "3 of 5") {
		t.Error("Error message should contain '3 of 5 tables could not be processed'")
	}
}

func TestCycleError_ErrorMessage_ListsCycleParticipants(t *testing.T) {
	info := &CycleInfo{
		TotalNodes:        3,
		ProcessedNodes:    0,
		UnprocessedNodes:  []string{"A", "B", "C"},
		CycleParticipants: []string{"A", "B", "C"},
	}
	cycleErr := &CycleError{Info: info}

	msg := cycleErr.Error()

	// Should list tables in cycle
	if !strings.Contains(msg, "Tables in cycle:") {
		t.Error("Error message should contain 'Tables in cycle:'")
	}
	if !strings.Contains(msg, "A") || !strings.Contains(msg, "B") || !strings.Contains(msg, "C") {
		t.Error("Error message should list all cycle participants")
	}
}

func TestCycleError_ErrorMessage_ListsBlockedTables(t *testing.T) {
	info := &CycleInfo{
		TotalNodes:        5,
		ProcessedNodes:    0,
		UnprocessedNodes:  []string{"A", "B", "C", "D", "E"},
		CycleParticipants: []string{"A", "B", "C"},
	}
	cycleErr := &CycleError{Info: info}

	msg := cycleErr.Error()

	// Should list blocked tables
	if !strings.Contains(msg, "Tables blocked by cycle:") {
		t.Error("Error message should contain 'Tables blocked by cycle:'")
	}
	if !strings.Contains(msg, "D") || !strings.Contains(msg, "E") {
		t.Error("Error message should list blocked tables D and E")
	}
}

func TestCycleError_ErrorMessage_WithCyclePath(t *testing.T) {
	info := &CycleInfo{
		TotalNodes:        3,
		ProcessedNodes:    0,
		UnprocessedNodes:  []string{"A", "B", "C"},
		CycleParticipants: []string{"A", "B", "C"},
		CyclePath:         []string{"A", "B", "C", "A"},
	}
	cycleErr := &CycleError{Info: info}

	msg := cycleErr.Error()

	// Should show cycle path
	if !strings.Contains(msg, "Cycle path:") {
		t.Error("Error message should contain 'Cycle path:'")
	}
	if !strings.Contains(msg, "A -> B -> C -> A") {
		t.Errorf("Error message should show cycle path 'A -> B -> C -> A', got:\n%s", msg)
	}
}

func TestCycleError_SimpleTwoTableCycle(t *testing.T) {
	// Create a simple 2-node cycle: A <-> B
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

	_, err := g.TopologicalSort()
	if err == nil {
		t.Fatal("Expected error for cycle graph")
	}

	cycleErr, ok := err.(*CycleError)
	if !ok {
		t.Fatalf("Expected *CycleError, got %T", err)
	}

	msg := cycleErr.Error()

	// Verify message contents
	if !strings.Contains(msg, "cycle detected") {
		t.Error("Message should contain 'cycle detected'")
	}
	if !strings.Contains(msg, "Tables in cycle:") {
		t.Error("Message should list tables in cycle")
	}
	if !strings.Contains(msg, "A") || !strings.Contains(msg, "B") {
		t.Error("Message should mention tables A and B")
	}
}

func TestCycleError_ComplexThreeTableCycle(t *testing.T) {
	// Create a 3-node cycle: A -> B -> C -> A
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

	_, err := g.TopologicalSort()
	if err == nil {
		t.Fatal("Expected error for cycle graph")
	}

	cycleErr, ok := err.(*CycleError)
	if !ok {
		t.Fatalf("Expected *CycleError, got %T", err)
	}

	msg := cycleErr.Error()

	// Verify message mentions all three tables
	if !strings.Contains(msg, "A") || !strings.Contains(msg, "B") || !strings.Contains(msg, "C") {
		t.Errorf("Message should mention all three tables, got:\n%s", msg)
	}

	// Verify cycle info
	if len(cycleErr.Info.CycleParticipants) != 3 {
		t.Errorf("Expected 3 cycle participants, got %d", len(cycleErr.Info.CycleParticipants))
	}
}

func TestCycleError_PartialCycleWithBlockedTables(t *testing.T) {
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
			"D":     {Name: "D"},
		},
		Children: map[string][]string{
			"root": {"child"},
			"A":    {"B"},
			"B":    {"C"},
			"C":    {"A", "D"}, // C -> A (cycle), C -> D (D blocked by cycle)
		},
		Parents: map[string][]string{
			"child": {"root"},
			"A":     {"C"},
			"B":     {"A"},
			"C":     {"B"},
			"D":     {"C"},
		},
	}

	_, err := g.TopologicalSort()
	if err == nil {
		t.Fatal("Expected error for partial cycle graph")
	}

	cycleErr, ok := err.(*CycleError)
	if !ok {
		t.Fatalf("Expected *CycleError, got %T", err)
	}

	msg := cycleErr.Error()

	// Should mention blocked tables
	if !strings.Contains(msg, "Tables blocked by cycle:") {
		t.Errorf("Message should mention blocked tables, got:\n%s", msg)
	}

	// Verify cycle info
	if cycleErr.Info.TotalNodes != 6 {
		t.Errorf("Expected TotalNodes=6, got %d", cycleErr.Info.TotalNodes)
	}
	if cycleErr.Info.ProcessedNodes != 2 {
		t.Errorf("Expected ProcessedNodes=2, got %d", cycleErr.Info.ProcessedNodes)
	}
}

func TestCycleError_SelfCycle(t *testing.T) {
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

	_, err := g.TopologicalSort()
	if err == nil {
		t.Fatal("Expected error for self-cycle graph")
	}

	cycleErr, ok := err.(*CycleError)
	if !ok {
		t.Fatalf("Expected *CycleError, got %T", err)
	}

	// Verify cycle info
	if len(cycleErr.Info.CycleParticipants) != 1 {
		t.Errorf("Expected 1 cycle participant, got %d", len(cycleErr.Info.CycleParticipants))
	}
	if cycleErr.Info.CycleParticipants[0] != "A" {
		t.Errorf("Expected participant 'A', got %s", cycleErr.Info.CycleParticipants[0])
	}
}

// ============================================================================
// CycleInfo Structure Tests
// ============================================================================

func TestCycleInfo_FieldsPopulatedCorrectly(t *testing.T) {
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
		t.Fatal("Expected CycleInfo, got nil")
	}

	// Verify all fields
	if info.TotalNodes != 3 {
		t.Errorf("TotalNodes: expected 3, got %d", info.TotalNodes)
	}
	if info.ProcessedNodes != 0 {
		t.Errorf("ProcessedNodes: expected 0, got %d", info.ProcessedNodes)
	}
	if len(info.UnprocessedNodes) != 3 {
		t.Errorf("UnprocessedNodes: expected 3, got %d", len(info.UnprocessedNodes))
	}
	if len(info.CycleParticipants) != 3 {
		t.Errorf("CycleParticipants: expected 3, got %d", len(info.CycleParticipants))
	}
}

func TestCycleInfo_NoCycleReturnsNil(t *testing.T) {
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

// ============================================================================
// Cycle Path Finding Tests
// ============================================================================

func TestFindCyclePath_TwoNodeCycle(t *testing.T) {
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

	allowedNodes := map[string]bool{"A": true, "B": true}
	path := g.FindCyclePath("A", allowedNodes)

	if len(path) == 0 {
		t.Fatal("Expected cycle path, got empty")
	}

	// Path should start with A and end with A
	if path[0] != "A" {
		t.Errorf("Path should start with 'A', got %s", path[0])
	}
	if path[len(path)-1] != "A" {
		t.Errorf("Path should end with 'A', got %s", path[len(path)-1])
	}
}

func TestFindCyclePath_ThreeNodeCycle(t *testing.T) {
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

	allowedNodes := map[string]bool{"A": true, "B": true, "C": true}
	path := g.FindCyclePath("A", allowedNodes)

	if len(path) == 0 {
		t.Fatal("Expected cycle path, got empty")
	}

	// Path should start with A and include all nodes
	if path[0] != "A" {
		t.Errorf("Path should start with 'A', got %s", path[0])
	}

	// Should contain all three nodes
	hasA, hasB, hasC := false, false, false
	for _, node := range path {
		switch node {
		case "A":
			hasA = true
		case "B":
			hasB = true
		case "C":
			hasC = true
		}
	}
	if !hasA || !hasB || !hasC {
		t.Errorf("Path should contain A, B, and C, got %v", path)
	}
}

func TestFindCyclePath_SelfCycle(t *testing.T) {
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

	allowedNodes := map[string]bool{"A": true}
	path := g.FindCyclePath("A", allowedNodes)

	// For self-cycle, path should just be [A, A] or similar
	if len(path) == 0 {
		t.Fatal("Expected cycle path for self-cycle")
	}
	if path[0] != "A" {
		t.Errorf("Path should start with 'A', got %s", path[0])
	}
}

func TestFindCyclePath_NoCycle(t *testing.T) {
	g := NewGraph("users", "id")
	g.AddNode("orders", &Node{Name: "orders"})
	g.AddEdge("users", "orders")

	// No cycle exists
	allowedNodes := map[string]bool{"users": true, "orders": true}
	path := g.FindCyclePath("users", allowedNodes)

	// Should return nil or empty path when no cycle exists
	if len(path) > 0 {
		t.Logf("Path returned for non-cycle graph: %v (may be expected)", path)
	}
}

// ============================================================================
// CycleError Message Format Tests
// ============================================================================

func TestCycleError_MessageFormat_Simple(t *testing.T) {
	// Test that message format is clear and actionable
	info := &CycleInfo{
		TotalNodes:        2,
		ProcessedNodes:    0,
		UnprocessedNodes:  []string{"orders", "order_items"},
		CycleParticipants: []string{"orders", "order_items"},
		CyclePath:         []string{"orders", "order_items", "orders"},
	}
	cycleErr := &CycleError{Info: info}

	msg := cycleErr.Error()

	// Check for multi-line format
	lines := strings.Split(msg, "\n")
	if len(lines) < 2 {
		t.Errorf("Expected multi-line error message, got:\n%s", msg)
	}

	// First line should contain summary
	if !strings.Contains(lines[0], "cycle detected") {
		t.Errorf("First line should contain 'cycle detected', got:\n%s", lines[0])
	}
}

func TestCycleError_MessageFormat_WithBlocked(t *testing.T) {
	// Test format when some tables are blocked by cycle
	info := &CycleInfo{
		TotalNodes:        5,
		ProcessedNodes:    1,
		UnprocessedNodes:  []string{"orders", "items", "shipments", "tracking"},
		CycleParticipants: []string{"orders", "items"},
		CyclePath:         []string{"orders", "items", "orders"},
	}
	cycleErr := &CycleError{Info: info}

	msg := cycleErr.Error()

	// Should have lines for cycle and blocked tables
	if !strings.Contains(msg, "Tables in cycle:") {
		t.Error("Should contain 'Tables in cycle:' section")
	}
	if !strings.Contains(msg, "Tables blocked by cycle:") {
		t.Error("Should contain 'Tables blocked by cycle:' section")
	}
}

func TestCycleError_NilCyclePath(t *testing.T) {
	// Test that nil CyclePath doesn't cause panic
	info := &CycleInfo{
		TotalNodes:        3,
		ProcessedNodes:    0,
		UnprocessedNodes:  []string{"A", "B", "C"},
		CycleParticipants: []string{"A", "B", "C"},
		CyclePath:         nil,
	}
	cycleErr := &CycleError{Info: info}

	// Should not panic
	msg := cycleErr.Error()

	// Should still contain basic info
	if !strings.Contains(msg, "cycle detected") {
		t.Error("Message should contain 'cycle detected'")
	}
}

func TestCycleError_EmptyCyclePath(t *testing.T) {
	// Test that empty CyclePath doesn't cause issues
	info := &CycleInfo{
		TotalNodes:        3,
		ProcessedNodes:    0,
		UnprocessedNodes:  []string{"A", "B", "C"},
		CycleParticipants: []string{"A", "B", "C"},
		CyclePath:         []string{},
	}
	cycleErr := &CycleError{Info: info}

	// Should not panic
	msg := cycleErr.Error()

	// Should still contain basic info
	if !strings.Contains(msg, "cycle detected") {
		t.Error("Message should contain 'cycle detected'")
	}
}
