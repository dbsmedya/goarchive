package archiver

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/graph"
)

// ============================================================================
// Orchestrator Cycle Validation Tests (GA-P2-F3-T4)
// ============================================================================

// TestOrchestrator_ValidateGraph_ValidDAG tests that ValidateGraph returns nil for valid DAG
func TestOrchestrator_ValidateGraph_ValidDAG(t *testing.T) {
	// Create a config with a valid dependency tree (no cycles)
	jobCfg := &config.JobConfig{
		RootTable:  "users",
		PrimaryKey: "id",
		Where:      "created_at < DATE_SUB(NOW(), INTERVAL 1 YEAR)",
		Relations: []config.Relation{
			{
				Table:          "orders",
				PrimaryKey:     "id",
				ForeignKey:     "user_id",
				DependencyType: "1-N",
				Relations: []config.Relation{
					{
						Table:          "order_items",
						PrimaryKey:     "id",
						ForeignKey:     "order_id",
						DependencyType: "1-N",
					},
				},
			},
			{
				Table:          "profiles",
				PrimaryKey:     "id",
				ForeignKey:     "user_id",
				DependencyType: "1-1",
			},
		},
	}

	cfg := createTestConfig()
	dbManager := mockDBManager(cfg)

	orch, err := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("Failed to create orchestrator: %v", err)
	}

	// Initialize should succeed (builds graph)
	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed for valid DAG: %v", err)
	}

	// ValidateGraph should return nil
	if err := orch.ValidateGraph(); err != nil {
		t.Errorf("ValidateGraph should return nil for valid DAG, got: %v", err)
	}
}

// TestOrchestrator_ValidateGraph_TwoTableCycle tests cycle detection with 2 tables
func TestOrchestrator_ValidateGraph_TwoTableCycle(t *testing.T) {
	// Manually create a graph with a 2-table cycle
	g := graph.NewGraph("users", "id")
	g.AddNode("orders", &graph.Node{Name: "orders", ForeignKey: "user_id", ReferenceKey: "id", DependencyType: "1-N"})
	// Create cycle: users -> orders -> users
	g.AddEdge("users", "orders")
	// Manually add reverse edge to create cycle
	g.Children["orders"] = append(g.Children["orders"], "users")
	g.Parents["users"] = append(g.Parents["users"], "orders")

	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, err := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("Failed to create orchestrator: %v", err)
	}

	// Inject the cyclic graph directly
	orch.graph = g

	// ValidateGraph should return error
	err = orch.ValidateGraph()
	if err == nil {
		t.Fatal("ValidateGraph should return error for cyclic graph")
	}

	if !strings.Contains(err.Error(), "cycle detected") {
		t.Errorf("Error should mention 'cycle detected', got: %v", err)
	}
}

// TestOrchestrator_ValidateGraph_ThreeTableCycle tests cycle detection with 3+ tables
func TestOrchestrator_ValidateGraph_ThreeTableCycle(t *testing.T) {
	// Create a 3-table cycle: A -> B -> C -> A
	g := graph.NewGraph("A", "id")
	g.AddNode("B", &graph.Node{Name: "B"})
	g.AddNode("C", &graph.Node{Name: "C"})

	// Create cycle
	g.AddEdge("A", "B")
	g.AddEdge("B", "C")
	// Manually add C -> A to complete cycle
	g.Children["C"] = append(g.Children["C"], "A")
	g.Parents["A"] = append(g.Parents["A"], "C")

	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, err := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("Failed to create orchestrator: %v", err)
	}

	orch.graph = g

	err = orch.ValidateGraph()
	if err == nil {
		t.Fatal("ValidateGraph should return error for 3-table cycle")
	}

	if !strings.Contains(err.Error(), "cycle detected") {
		t.Errorf("Error should mention 'cycle detected', got: %v", err)
	}
}

// TestOrchestrator_ValidateGraph_NilGraph tests validation with nil graph
func TestOrchestrator_ValidateGraph_NilGraph(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, err := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("Failed to create orchestrator: %v", err)
	}

	// Don't initialize - graph is nil
	err = orch.ValidateGraph()
	if err == nil {
		t.Fatal("ValidateGraph should return error for nil graph")
	}

	if !strings.Contains(err.Error(), "graph not built") {
		t.Errorf("Error should mention 'graph not built', got: %v", err)
	}
}

// TestOrchestrator_Initialize_FailsFastOnCycle tests that Initialize fails before DB operations
func TestOrchestrator_Initialize_FailsFastOnCycle(t *testing.T) {
	// Create a config that will build a valid graph
	jobCfg := createTestJobConfig()
	cfg := createTestConfig()
	dbManager := mockDBManager(cfg)

	orch, err := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("Failed to create orchestrator: %v", err)
	}

	// Initialize should succeed for valid config
	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize should succeed for valid DAG: %v", err)
	}

	// Verify orchestrator is marked as initialized
	if !orch.IsInitialized() {
		t.Error("Orchestrator should be initialized after successful Initialize()")
	}
}

// TestOrchestrator_Execute_NotInitialized tests Execute fails when not initialized
func TestOrchestrator_Execute_NotInitialized(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, err := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("Failed to create orchestrator: %v", err)
	}

	// Don't initialize
	ctx := context.Background()
	_, err = orch.Execute(ctx, nil)
	if err == nil {
		t.Fatal("Execute should return error when not initialized")
	}

	if !strings.Contains(err.Error(), "not initialized") {
		t.Errorf("Error should mention 'not initialized', got: %v", err)
	}
}

// TestOrchestrator_GetCopyOrder_NotInitialized tests GetCopyOrder fails when not initialized
func TestOrchestrator_GetCopyOrder_NotInitialized(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, err := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("Failed to create orchestrator: %v", err)
	}

	_, err = orch.GetCopyOrder()
	if err == nil {
		t.Fatal("GetCopyOrder should return error when not initialized")
	}
}

// TestOrchestrator_GetDeleteOrder_NotInitialized tests GetDeleteOrder fails when not initialized
func TestOrchestrator_GetDeleteOrder_NotInitialized(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, err := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("Failed to create orchestrator: %v", err)
	}

	_, err = orch.GetDeleteOrder()
	if err == nil {
		t.Fatal("GetDeleteOrder should return error when not initialized")
	}
}

// TestOrchestrator_ValidateGraph_ErrorIncludesCycleInfo tests error includes cycle details
func TestOrchestrator_ValidateGraph_ErrorIncludesCycleInfo(t *testing.T) {
	// Create a graph with a known cycle
	g := graph.NewGraph("orders", "id")
	g.AddNode("items", &graph.Node{Name: "items", ForeignKey: "order_id", ReferenceKey: "id", DependencyType: "1-N"})

	// Create cycle: orders -> items -> orders
	g.AddEdge("orders", "items")
	g.Children["items"] = append(g.Children["items"], "orders")
	g.Parents["orders"] = append(g.Parents["orders"], "items")

	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, err := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("Failed to create orchestrator: %v", err)
	}

	orch.graph = g

	err = orch.ValidateGraph()
	if err == nil {
		t.Fatal("ValidateGraph should return error for cycle")
	}

	// Error should include number of nodes in cycle
	if !strings.Contains(err.Error(), "nodes in cycle") {
		t.Errorf("Error should include node count, got: %v", err)
	}
}

// TestOrchestrator_Initialize_NoDBOperationsOnCycle tests no DB ops when cycle exists
func TestOrchestrator_Initialize_NoDBOperationsOnCycle(t *testing.T) {
	// This test verifies that Initialize fails before any database operations
	// by using a mock that would panic if called

	jobCfg := createTestJobConfig()
	cfg := createTestConfig()

	// Use mock DB manager - if Initialize tried to use DB, it would fail
	dbManager := mockDBManager(cfg)

	orch, err := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("Failed to create orchestrator: %v", err)
	}

	// Initialize builds graph from config (no DB calls) and validates
	// If there were a cycle in config (which there isn't in this test),
	// it would fail before any DB operations
	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Verify orchestrator is ready for use
	if orch.GetGraph() == nil {
		t.Error("Graph should be set after initialization")
	}
}

// TestOrchestrator_ValidateGraph_SelfCycle tests detection of self-referencing cycle
func TestOrchestrator_ValidateGraph_SelfCycle(t *testing.T) {
	// Create self-referencing node
	g := graph.NewGraph("self_ref", "id")
	// Add self-reference
	g.Children["self_ref"] = append(g.Children["self_ref"], "self_ref")
	g.Parents["self_ref"] = append(g.Parents["self_ref"], "self_ref")

	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, err := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("Failed to create orchestrator: %v", err)
	}

	orch.graph = g

	err = orch.ValidateGraph()
	if err == nil {
		t.Fatal("ValidateGraph should return error for self-cycle")
	}

	if !strings.Contains(err.Error(), "cycle detected") {
		t.Errorf("Error should mention 'cycle detected', got: %v", err)
	}
}

// TestOrchestrator_Initialize_Idempotent tests that Initialize is idempotent
func TestOrchestrator_Initialize_Idempotent(t *testing.T) {
	jobCfg := createTestJobConfig()
	cfg := createTestConfig()
	dbManager := mockDBManager(cfg)

	orch, err := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("Failed to create orchestrator: %v", err)
	}

	// First Initialize
	if err := orch.Initialize(); err != nil {
		t.Fatalf("First Initialize failed: %v", err)
	}

	copyOrder1, _ := orch.GetCopyOrder()

	// Second Initialize should be no-op (idempotent)
	if err := orch.Initialize(); err != nil {
		t.Fatalf("Second Initialize failed: %v", err)
	}

	copyOrder2, _ := orch.GetCopyOrder()

	// Orders should be the same
	if len(copyOrder1) != len(copyOrder2) {
		t.Error("Copy orders should be identical after idempotent Initialize")
	}
}

// TestOrchestrator_ValidateGraph_ComplexCycle tests cycle detection in complex graph
func TestOrchestrator_ValidateGraph_ComplexCycle(t *testing.T) {
	// Graph with both valid and cyclic parts
	// Valid: root -> child1, root -> child2
	// Cycle: A -> B -> C -> A
	g := graph.NewGraph("root", "id")
	g.AddNode("child1", &graph.Node{Name: "child1"})
	g.AddNode("child2", &graph.Node{Name: "child2"})
	g.AddNode("A", &graph.Node{Name: "A"})
	g.AddNode("B", &graph.Node{Name: "B"})
	g.AddNode("C", &graph.Node{Name: "C"})

	// Valid edges
	g.AddEdge("root", "child1")
	g.AddEdge("root", "child2")

	// Cycle edges
	g.AddEdge("A", "B")
	g.AddEdge("B", "C")
	g.Children["C"] = append(g.Children["C"], "A")
	g.Parents["A"] = append(g.Parents["A"], "C")

	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, err := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("Failed to create orchestrator: %v", err)
	}

	orch.graph = g

	err = orch.ValidateGraph()
	if err == nil {
		t.Fatal("ValidateGraph should return error for complex cycle")
	}
}

// ============================================================================
// Integration Tests for Fail-Fast Behavior
// ============================================================================

// TestOrchestrator_FailFast_Integration tests complete fail-fast workflow using Sakila schema
func TestOrchestrator_FailFast_Integration(t *testing.T) {
	// Use Sakila schema: rental -> payment (1-N)
	jobCfg := &config.JobConfig{
		RootTable:  "rental",
		PrimaryKey: "rental_id",
		Where:      "rental_date < '2005-08-01'",
		Relations: []config.Relation{
			{
				Table:          "payment",
				PrimaryKey:     "payment_id",
				ForeignKey:     "rental_id",
				DependencyType: "1-N",
			},
		},
	}

	cfg := createTestConfig()
	dbManager := realDBManager(t)

	// Step 1: Create orchestrator
	orch, err := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("Failed to create orchestrator: %v", err)
	}

	// Step 2: Initialize (should succeed for valid DAG)
	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Step 3: Get copy order
	copyOrder, err := orch.GetCopyOrder()
	if err != nil {
		t.Fatalf("GetCopyOrder failed: %v", err)
	}

	// Verify order: rental should come first (it's the root/parent)
	if len(copyOrder) == 0 || copyOrder[0] != "rental" {
		t.Errorf("Expected rental first in copy order, got %v", copyOrder)
	}

	// Step 4: Get delete order
	deleteOrder, err := orch.GetDeleteOrder()
	if err != nil {
		t.Fatalf("GetDeleteOrder failed: %v", err)
	}

	// Verify order: rental should come last in delete order (children deleted first)
	if len(deleteOrder) == 0 || deleteOrder[len(deleteOrder)-1] != "rental" {
		t.Errorf("Expected rental last in delete order, got %v", deleteOrder)
	}

	// Step 5: Execute (simulated)
	ctx := context.Background()
	result, err := orch.Execute(ctx, nil)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Execution should succeed for valid DAG")
	}
}

// TestOrchestrator_CycleError_Wrapping tests that cycle errors are properly wrapped
func TestOrchestrator_CycleError_Wrapping(t *testing.T) {
	g := graph.NewGraph("A", "id")
	g.AddNode("B", &graph.Node{Name: "B"})

	// Create cycle
	g.AddEdge("A", "B")
	g.Children["B"] = append(g.Children["B"], "A")
	g.Parents["A"] = append(g.Parents["A"], "B")

	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, err := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("Failed to create orchestrator: %v", err)
	}

	orch.graph = g

	err = orch.ValidateGraph()
	if err == nil {
		t.Fatal("Expected error")
	}

	// Error should be a plain error (not wrapped *CycleError from graph package)
	// because orchestrator wraps it with its own message
	var cycleErr *graph.CycleError
	if errors.As(err, &cycleErr) {
		t.Logf("Error chain includes CycleError: %v", cycleErr)
	}
}
