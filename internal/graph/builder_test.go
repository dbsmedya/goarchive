package graph

import (
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/internal/config"
)

func TestNewBuilder(t *testing.T) {
	job := &config.JobConfig{
		RootTable:  "users",
		PrimaryKey: "id",
	}

	builder := NewBuilder(job)
	if builder == nil {
		t.Fatal("NewBuilder returned nil")
	}
	if builder.job != job {
		t.Error("Builder job field not set correctly")
	}
}

func TestBuild_SingleRelation(t *testing.T) {
	job := &config.JobConfig{
		RootTable:  "users",
		PrimaryKey: "id",
		Relations: []config.Relation{
			{
				Table:          "orders",
				PrimaryKey:     "id",
				ForeignKey:     "user_id",
				DependencyType: "1-N",
			},
		},
	}

	builder := NewBuilder(job)
	graph, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	// Check root node
	if !graph.HasNode("users") {
		t.Error("Root node 'users' not found in graph")
	}

	// Check child node
	if !graph.HasNode("orders") {
		t.Error("Child node 'orders' not found in graph")
	}

	// Check edge
	children := graph.GetChildren("users")
	if len(children) != 1 || children[0] != "orders" {
		t.Errorf("Expected users->orders edge, got children: %v", children)
	}

	// Check node metadata
	node := graph.GetNode("orders")
	if node == nil {
		t.Fatal("orders node is nil")
	}
	if node.ForeignKey != "user_id" {
		t.Errorf("Expected ForeignKey 'user_id', got %q", node.ForeignKey)
	}
	if node.ReferenceKey != "id" {
		t.Errorf("Expected ReferenceKey 'id', got %q", node.ReferenceKey)
	}
	if node.DependencyType != "1-N" {
		t.Errorf("Expected DependencyType '1-N', got %q", node.DependencyType)
	}
	if node.IsRoot {
		t.Error("orders node should not be root")
	}
}

func TestBuild_MultipleRelations(t *testing.T) {
	job := &config.JobConfig{
		RootTable:  "users",
		PrimaryKey: "id",
		Relations: []config.Relation{
			{Table: "orders", PrimaryKey: "id", ForeignKey: "user_id", DependencyType: "1-N"},
			{Table: "profiles", PrimaryKey: "id", ForeignKey: "user_id", DependencyType: "1-1"},
			{Table: "sessions", PrimaryKey: "id", ForeignKey: "user_id", DependencyType: "1-N"},
		},
	}

	builder := NewBuilder(job)
	graph, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	// Check all nodes exist
	expectedNodes := []string{"users", "orders", "profiles", "sessions"}
	for _, node := range expectedNodes {
		if !graph.HasNode(node) {
			t.Errorf("Node %q not found in graph", node)
		}
	}

	// Check node count (root + 3 children)
	if graph.NodeCount() != 4 {
		t.Errorf("Expected 4 nodes, got %d", graph.NodeCount())
	}

	// Check all children of root
	children := graph.GetChildren("users")
	if len(children) != 3 {
		t.Errorf("Expected 3 children, got %d: %v", len(children), children)
	}

	// Check edge count
	if graph.EdgeCount() != 3 {
		t.Errorf("Expected 3 edges, got %d", graph.EdgeCount())
	}
}

func TestBuild_NestedRelations(t *testing.T) {
	job := &config.JobConfig{
		RootTable:  "users",
		PrimaryKey: "id",
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
		},
	}

	builder := NewBuilder(job)
	graph, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	// Check all nodes exist
	if !graph.HasNode("users") {
		t.Error("Root node 'users' not found")
	}
	if !graph.HasNode("orders") {
		t.Error("Node 'orders' not found")
	}
	if !graph.HasNode("order_items") {
		t.Error("Node 'order_items' not found")
	}

	// Check parent-child relationships
	if children := graph.GetChildren("users"); len(children) != 1 || children[0] != "orders" {
		t.Errorf("Expected users->orders, got %v", children)
	}
	if children := graph.GetChildren("orders"); len(children) != 1 || children[0] != "order_items" {
		t.Errorf("Expected orders->order_items, got %v", children)
	}

	// Check reverse relationships
	if parents := graph.GetParents("orders"); len(parents) != 1 || parents[0] != "users" {
		t.Errorf("Expected orders parent to be users, got %v", parents)
	}
	if parents := graph.GetParents("order_items"); len(parents) != 1 || parents[0] != "orders" {
		t.Errorf("Expected order_items parent to be orders, got %v", parents)
	}
}

func TestBuild_DeepNesting(t *testing.T) {
	job := &config.JobConfig{
		RootTable:  "level1",
		PrimaryKey: "id",
		Relations: []config.Relation{
			{
				Table:          "level2",
				PrimaryKey:     "id",
				ForeignKey:     "level1_id",
				DependencyType: "1-N",
				Relations: []config.Relation{
					{
						Table:          "level3",
						PrimaryKey:     "id",
						ForeignKey:     "level2_id",
						DependencyType: "1-N",
						Relations: []config.Relation{
							{
								Table:          "level4",
								PrimaryKey:     "id",
								ForeignKey:     "level3_id",
								DependencyType: "1-N",
							},
						},
					},
				},
			},
		},
	}

	builder := NewBuilder(job)
	graph, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	// Check all 4 levels exist
	levels := []string{"level1", "level2", "level3", "level4"}
	for _, level := range levels {
		if !graph.HasNode(level) {
			t.Errorf("Node %q not found", level)
		}
	}

	// Verify chain: level1 -> level2 -> level3 -> level4
	if graph.NodeCount() != 4 {
		t.Errorf("Expected 4 nodes, got %d", graph.NodeCount())
	}
	if graph.EdgeCount() != 3 {
		t.Errorf("Expected 3 edges, got %d", graph.EdgeCount())
	}

	// Check the chain
	if children := graph.GetChildren("level1"); len(children) != 1 || children[0] != "level2" {
		t.Errorf("Expected level1->level2, got %v", children)
	}
	if children := graph.GetChildren("level2"); len(children) != 1 || children[0] != "level3" {
		t.Errorf("Expected level2->level3, got %v", children)
	}
	if children := graph.GetChildren("level3"); len(children) != 1 || children[0] != "level4" {
		t.Errorf("Expected level3->level4, got %v", children)
	}
}

func TestBuild_NoRelations(t *testing.T) {
	job := &config.JobConfig{
		RootTable:  "users",
		PrimaryKey: "id",
		Relations:  []config.Relation{},
	}

	builder := NewBuilder(job)
	graph, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	// Should only have root node
	if graph.NodeCount() != 1 {
		t.Errorf("Expected 1 node (root only), got %d", graph.NodeCount())
	}
	if graph.EdgeCount() != 0 {
		t.Errorf("Expected 0 edges, got %d", graph.EdgeCount())
	}
	if !graph.HasNode("users") {
		t.Error("Root node 'users' not found")
	}
}

func TestBuild_NilJob(t *testing.T) {
	builder := NewBuilder(nil)
	_, err := builder.Build()
	if err == nil {
		t.Fatal("Expected error for nil job, got nil")
	}
	if err.Error() != "job configuration is nil" {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestBuild_EmptyRootTable(t *testing.T) {
	job := &config.JobConfig{
		RootTable:  "",
		PrimaryKey: "id",
	}

	builder := NewBuilder(job)
	_, err := builder.Build()
	if err == nil {
		t.Fatal("Expected error for empty root table, got nil")
	}
	if err.Error() != "root table is not specified" {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestBuild_EmptyPrimaryKey(t *testing.T) {
	job := &config.JobConfig{
		RootTable:  "users",
		PrimaryKey: "",
	}

	builder := NewBuilder(job)
	_, err := builder.Build()
	if err == nil {
		t.Fatal("Expected error for empty primary key, got nil")
	}
	expectedMsg := `primary key is not specified for root table "users"`
	if err.Error() != expectedMsg {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestBuild_EmptyRelationTable(t *testing.T) {
	job := &config.JobConfig{
		RootTable:  "users",
		PrimaryKey: "id",
		Relations: []config.Relation{
			{
				Table:          "",
				ForeignKey:     "user_id",
				DependencyType: "1-N",
			},
		},
	}

	builder := NewBuilder(job)
	_, err := builder.Build()
	if err == nil {
		t.Fatal("Expected error for empty relation table, got nil")
	}
	expectedMsg := `relation table name is empty under parent "users"`
	if !strings.Contains(err.Error(), expectedMsg) {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestBuild_EmptyForeignKey(t *testing.T) {
	job := &config.JobConfig{
		RootTable:  "users",
		PrimaryKey: "id",
		Relations: []config.Relation{
			{
				Table:          "orders",
				ForeignKey:     "",
				DependencyType: "1-N",
			},
		},
	}

	builder := NewBuilder(job)
	_, err := builder.Build()
	if err == nil {
		t.Fatal("Expected error for empty foreign key, got nil")
	}
	expectedMsg := `foreign key is not specified for relation "orders"`
	if !strings.Contains(err.Error(), expectedMsg) {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestBuild_InvalidDependencyType(t *testing.T) {
	job := &config.JobConfig{
		RootTable:  "users",
		PrimaryKey: "id",
		Relations: []config.Relation{
			{
				Table:          "orders",
				ForeignKey:     "user_id",
				DependencyType: "invalid",
			},
		},
	}

	builder := NewBuilder(job)
	_, err := builder.Build()
	if err == nil {
		t.Fatal("Expected error for invalid dependency type, got nil")
	}
	expectedMsg := `invalid dependency type "invalid" for relation "orders" (must be '1-1' or '1-N')`
	if !strings.Contains(err.Error(), expectedMsg) {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestBuild_DuplicateTable(t *testing.T) {
	job := &config.JobConfig{
		RootTable:  "users",
		PrimaryKey: "id",
		Relations: []config.Relation{
			{Table: "orders", PrimaryKey: "id", ForeignKey: "user_id", DependencyType: "1-N"},
			{Table: "orders", PrimaryKey: "id", ForeignKey: "buyer_id", DependencyType: "1-N"},
		},
	}

	builder := NewBuilder(job)
	_, err := builder.Build()
	if err == nil {
		t.Fatal("Expected error for duplicate table, got nil")
	}
	expectedMsg := `duplicate relation: table "orders" appears multiple times in the graph`
	if !strings.Contains(err.Error(), expectedMsg) {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestBuild_DefaultDependencyType(t *testing.T) {
	job := &config.JobConfig{
		RootTable:  "users",
		PrimaryKey: "id",
		Relations: []config.Relation{
			{
				Table:      "orders",
				PrimaryKey: "id",
				ForeignKey: "user_id",
				// DependencyType not specified - should default to "1-N"
			},
		},
	}

	builder := NewBuilder(job)
	graph, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	node := graph.GetNode("orders")
	if node == nil {
		t.Fatal("orders node is nil")
	}
	if node.DependencyType != "1-N" {
		t.Errorf("Expected default DependencyType '1-N', got %q", node.DependencyType)
	}

	// Check edge metadata also has default
	meta := graph.GetEdgeMeta("users", "orders")
	if meta == nil {
		t.Fatal("edge metadata is nil")
	}
	if meta.DependencyType != "1-N" {
		t.Errorf("Expected edge metadata DependencyType '1-N', got %q", meta.DependencyType)
	}
}

func TestBuildFromJob(t *testing.T) {
	job := &config.JobConfig{
		RootTable:  "users",
		PrimaryKey: "id",
		Relations: []config.Relation{
			{Table: "orders", PrimaryKey: "id", ForeignKey: "user_id", DependencyType: "1-N"},
		},
	}

	graph, err := BuildFromJob(job)
	if err != nil {
		t.Fatalf("BuildFromJob() failed: %v", err)
	}

	if graph == nil {
		t.Fatal("BuildFromJob returned nil graph")
	}

	if !graph.HasNode("users") || !graph.HasNode("orders") {
		t.Error("Graph missing expected nodes")
	}
}

func TestBuildFromJob_Error(t *testing.T) {
	job := &config.JobConfig{
		RootTable:  "", // Empty root table should cause error
		PrimaryKey: "id",
	}

	_, err := BuildFromJob(job)
	if err == nil {
		t.Fatal("Expected error for invalid job, got nil")
	}
}

func TestBuild_VerifyNodeMetadata(t *testing.T) {
	job := &config.JobConfig{
		RootTable:  "companies",
		PrimaryKey: "company_id",
		Relations: []config.Relation{
			{
				Table:          "departments",
				PrimaryKey:     "id",
				ForeignKey:     "company_id",
				DependencyType: "1-N",
			},
		},
	}

	builder := NewBuilder(job)
	graph, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	// Check child node metadata
	deptNode := graph.GetNode("departments")
	if deptNode == nil {
		t.Fatal("departments node is nil")
	}
	if deptNode.Name != "departments" {
		t.Errorf("Expected Name 'departments', got %q", deptNode.Name)
	}
	if deptNode.ForeignKey != "company_id" {
		t.Errorf("Expected ForeignKey 'company_id', got %q", deptNode.ForeignKey)
	}
	if deptNode.ReferenceKey != "company_id" {
		t.Errorf("Expected ReferenceKey 'company_id', got %q", deptNode.ReferenceKey)
	}
	if deptNode.DependencyType != "1-N" {
		t.Errorf("Expected DependencyType '1-N', got %q", deptNode.DependencyType)
	}
	if deptNode.IsRoot {
		t.Error("departments should not be root")
	}
}

func TestBuild_VerifyEdgeMetadata(t *testing.T) {
	job := &config.JobConfig{
		RootTable:  "customers",
		PrimaryKey: "customer_id",
		Relations: []config.Relation{
			{
				Table:          "invoices",
				PrimaryKey:     "id",
				ForeignKey:     "cust_id",
				DependencyType: "1-N",
			},
		},
	}

	builder := NewBuilder(job)
	graph, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	// Check edge metadata
	meta := graph.GetEdgeMeta("customers", "invoices")
	if meta == nil {
		t.Fatal("edge metadata is nil")
	}
	if meta.ForeignKey != "cust_id" {
		t.Errorf("Expected ForeignKey 'cust_id', got %q", meta.ForeignKey)
	}
	if meta.ReferenceKey != "customer_id" {
		t.Errorf("Expected ReferenceKey 'customer_id', got %q", meta.ReferenceKey)
	}
	if meta.DependencyType != "1-N" {
		t.Errorf("Expected DependencyType '1-N', got %q", meta.DependencyType)
	}
}

func TestBuild_ComplexGraph(t *testing.T) {
	// Build a complex graph with multiple branches and nesting
	job := &config.JobConfig{
		RootTable:  "users",
		PrimaryKey: "id",
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
					{
						Table:          "shipments",
						PrimaryKey:     "id",
						ForeignKey:     "order_id",
						DependencyType: "1-1",
					},
				},
			},
			{
				Table:          "profiles",
				PrimaryKey:     "id",
				ForeignKey:     "user_id",
				DependencyType: "1-1",
			},
			{
				Table:          "sessions",
				PrimaryKey:     "id",
				ForeignKey:     "user_id",
				DependencyType: "1-N",
			},
		},
	}

	builder := NewBuilder(job)
	graph, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	// Expected nodes: users, orders, order_items, shipments, profiles, sessions (6)
	if graph.NodeCount() != 6 {
		t.Errorf("Expected 6 nodes, got %d", graph.NodeCount())
	}

	// Expected edges:
	// users -> orders, users -> profiles, users -> sessions (3)
	// orders -> order_items, orders -> shipments (2)
	// Total: 5 edges
	if graph.EdgeCount() != 5 {
		t.Errorf("Expected 5 edges, got %d", graph.EdgeCount())
	}

	// Verify structure
	// users children
	userChildren := graph.GetChildren("users")
	if len(userChildren) != 3 {
		t.Errorf("Expected 3 children for users, got %d: %v", len(userChildren), userChildren)
	}

	// orders children
	orderChildren := graph.GetChildren("orders")
	if len(orderChildren) != 2 {
		t.Errorf("Expected 2 children for orders, got %d: %v", len(orderChildren), orderChildren)
	}

	// Leaf nodes (no children)
	leaves := graph.LeafNodes()
	expectedLeaves := map[string]bool{
		"order_items": true,
		"shipments":   true,
		"profiles":    true,
		"sessions":    true,
	}
	if len(leaves) != 4 {
		t.Errorf("Expected 4 leaf nodes, got %d: %v", len(leaves), leaves)
	}
	for _, leaf := range leaves {
		if !expectedLeaves[leaf] {
			t.Errorf("Unexpected leaf node: %s", leaf)
		}
	}

	// Verify in-degrees
	if graph.InDegree("users") != 0 {
		t.Errorf("Expected users in-degree 0, got %d", graph.InDegree("users"))
	}
	if graph.InDegree("orders") != 1 {
		t.Errorf("Expected orders in-degree 1, got %d", graph.InDegree("orders"))
	}
	if graph.InDegree("order_items") != 1 {
		t.Errorf("Expected order_items in-degree 1, got %d", graph.InDegree("order_items"))
	}

	// Verify out-degrees
	if graph.OutDegree("users") != 3 {
		t.Errorf("Expected users out-degree 3, got %d", graph.OutDegree("users"))
	}
	if graph.OutDegree("orders") != 2 {
		t.Errorf("Expected orders out-degree 2, got %d", graph.OutDegree("orders"))
	}
	if graph.OutDegree("profiles") != 0 {
		t.Errorf("Expected profiles out-degree 0, got %d", graph.OutDegree("profiles"))
	}
}

func TestBuild_DuplicateInNestedRelations(t *testing.T) {
	// Test that duplicate detection works across nested relations
	job := &config.JobConfig{
		RootTable:  "users",
		PrimaryKey: "id",
		Relations: []config.Relation{
			{
				Table:          "orders",
				PrimaryKey:     "id",
				ForeignKey:     "user_id",
				DependencyType: "1-N",
				Relations: []config.Relation{
					{
						Table:          "users", // Duplicate: users is root
						PrimaryKey:     "id",
						ForeignKey:     "order_id",
						DependencyType: "1-N",
					},
				},
			},
		},
	}

	builder := NewBuilder(job)
	_, err := builder.Build()
	if err == nil {
		t.Fatal("Expected error for duplicate table in nested relations, got nil")
	}
	expectedMsg := `duplicate relation: table "users" appears multiple times in the graph`
	if !strings.Contains(err.Error(), expectedMsg) {
		t.Errorf("Unexpected error message: %v", err)
	}
}

// TestBuild_MissingPrimaryKey tests that builder FAILS when primary_key is not specified for a relation
// GA-P2-F1-T3: Enforce explicit primary key specification
func TestBuild_MissingPrimaryKey(t *testing.T) {
	job := &config.JobConfig{
		RootTable:  "users",
		PrimaryKey: "id",
		Relations: []config.Relation{
			{
				Table:          "orders",
				ForeignKey:     "user_id",
				DependencyType: "1-N",
				// PrimaryKey not specified - should FAIL
			},
		},
	}

	builder := NewBuilder(job)
	_, err := builder.Build()
	if err == nil {
		t.Fatal("Expected error for missing primary_key, got nil")
	}
	expectedMsg := `primary_key is not specified for relation "orders"`
	if !strings.Contains(err.Error(), expectedMsg) {
		t.Errorf("Expected error about missing primary_key, got: %v", err)
	}
}

// TestBuild_MissingPrimaryKeyInNestedRelation tests validation in deeply nested relations
// GA-P2-F1-T3: Enforce explicit primary key specification
func TestBuild_MissingPrimaryKeyInNestedRelation(t *testing.T) {
	job := &config.JobConfig{
		RootTable:  "users",
		PrimaryKey: "id",
		Relations: []config.Relation{
			{
				Table:          "orders",
				PrimaryKey:     "id",
				ForeignKey:     "user_id",
				DependencyType: "1-N",
				Relations: []config.Relation{
					{
						Table:          "order_items",
						ForeignKey:     "order_id",
						DependencyType: "1-N",
						// PrimaryKey not specified - should FAIL
					},
				},
			},
		},
	}

	builder := NewBuilder(job)
	_, err := builder.Build()
	if err == nil {
		t.Fatal("Expected error for missing primary_key in nested relation, got nil")
	}
	expectedMsg := `primary_key is not specified for relation "order_items"`
	if !strings.Contains(err.Error(), expectedMsg) {
		t.Errorf("Expected error about missing primary_key for order_items, got: %v", err)
	}
}

// TestBuild_CustomPrimaryKeys tests that custom PKs work correctly
// GA-P2-F1-T3: Support custom primary keys
func TestBuild_CustomPrimaryKeys(t *testing.T) {
	job := &config.JobConfig{
		RootTable:  "users",
		PrimaryKey: "user_id",
		Relations: []config.Relation{
			{
				Table:          "orders",
				PrimaryKey:     "order_id",
				ForeignKey:     "user_id",
				DependencyType: "1-N",
				Relations: []config.Relation{
					{
						Table:          "order_items",
						PrimaryKey:     "item_id",
						ForeignKey:     "order_id",
						DependencyType: "1-N",
					},
				},
			},
		},
	}

	builder := NewBuilder(job)
	graph, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	// Verify all nodes exist
	if !graph.HasNode("users") || !graph.HasNode("orders") || !graph.HasNode("order_items") {
		t.Error("Missing expected nodes in graph")
	}

	// Verify correct PKs are set
	if pk := graph.GetPK("users"); pk != "user_id" {
		t.Errorf("Expected users PK 'user_id', got %q", pk)
	}
	if pk := graph.GetPK("orders"); pk != "order_id" {
		t.Errorf("Expected orders PK 'order_id', got %q", pk)
	}
	if pk := graph.GetPK("order_items"); pk != "item_id" {
		t.Errorf("Expected order_items PK 'item_id', got %q", pk)
	}

	// Verify correct reference keys
	ordersNode := graph.GetNode("orders")
	if ordersNode.ReferenceKey != "user_id" {
		t.Errorf("Expected orders ReferenceKey 'user_id', got %q", ordersNode.ReferenceKey)
	}

	itemsNode := graph.GetNode("order_items")
	if itemsNode.ReferenceKey != "order_id" {
		t.Errorf("Expected order_items ReferenceKey 'order_id', got %q", itemsNode.ReferenceKey)
	}
}
