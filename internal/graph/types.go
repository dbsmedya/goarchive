// Package graph provides dependency graph structures and algorithms for GoArchive.
package graph

// Node represents a table in the dependency graph.
type Node struct {
	Name           string // Table name
	ForeignKey     string // FK column in this table pointing to parent (empty for root)
	ReferenceKey   string // PK column in parent that FK references (empty for root)
	DependencyType string // "1-1" or "1-N"
	IsRoot         bool   // True if this is the root table
}

// Edge represents a dependency relationship between tables.
type Edge struct {
	From string // Parent table name
	To   string // Child table name
}

// Graph represents the complete dependency structure for an archive job.
type Graph struct {
	Nodes        map[string]*Node    // table name -> node
	Children     map[string][]string // table name -> child table names (outgoing edges)
	Parents      map[string][]string // table name -> parent table names (incoming edges)
	Root         string              // Root table name
	RootPK       string              // Primary key column of root table
	pkColumns    map[string]string   // table name -> primary key column name (for all tables)
	edgeMetadata map[Edge]*EdgeMeta  // Edge -> metadata
}

// EdgeMeta contains metadata about an edge relationship.
type EdgeMeta struct {
	ForeignKey     string // FK column in child table
	ReferenceKey   string // PK column in parent table
	DependencyType string // "1-1" or "1-N"
}

// NewGraph creates a new empty graph with the specified root table.
func NewGraph(root, rootPK string) *Graph {
	g := &Graph{
		Nodes:        make(map[string]*Node),
		Children:     make(map[string][]string),
		Parents:      make(map[string][]string),
		Root:         root,
		RootPK:       rootPK,
		pkColumns:    make(map[string]string),
		edgeMetadata: make(map[Edge]*EdgeMeta),
	}

	// Add root node
	g.Nodes[root] = &Node{
		Name:   root,
		IsRoot: true,
	}

	// Store root table PK
	g.pkColumns[root] = rootPK

	return g
}

// AddNode adds a table node to the graph.
// If node is nil, a new node with default values is created.
func (g *Graph) AddNode(name string, node *Node) {
	if node == nil {
		node = &Node{Name: name}
	}
	node.Name = name
	g.Nodes[name] = node
}

// AddEdge adds a parent -> child relationship to the graph.
// It also maintains the reverse mapping for efficient parent lookups.
func (g *Graph) AddEdge(parent, child string) {
	// Add to children map (forward edges)
	g.Children[parent] = append(g.Children[parent], child)

	// Add to parents map (reverse edges)
	g.Parents[child] = append(g.Parents[child], parent)
}

// AddEdgeWithMeta adds an edge with metadata about the relationship.
func (g *Graph) AddEdgeWithMeta(parent, child, foreignKey, referenceKey, depType string) {
	g.AddEdge(parent, child)

	edge := Edge{From: parent, To: child}
	g.edgeMetadata[edge] = &EdgeMeta{
		ForeignKey:     foreignKey,
		ReferenceKey:   referenceKey,
		DependencyType: depType,
	}
}

// GetChildren returns all direct children of a table.
func (g *Graph) GetChildren(parent string) []string {
	return g.Children[parent]
}

// GetParents returns all direct parents of a table.
func (g *Graph) GetParents(child string) []string {
	return g.Parents[child]
}

// GetNode returns the node for a given table name, or nil if not found.
func (g *Graph) GetNode(name string) *Node {
	return g.Nodes[name]
}

// GetEdgeMeta returns metadata for an edge, or nil if not found.
func (g *Graph) GetEdgeMeta(parent, child string) *EdgeMeta {
	edge := Edge{From: parent, To: child}
	return g.edgeMetadata[edge]
}

// HasNode returns true if the graph contains a node with the given name.
func (g *Graph) HasNode(name string) bool {
	_, exists := g.Nodes[name]
	return exists
}

// NodeCount returns the number of nodes in the graph.
func (g *Graph) NodeCount() int {
	return len(g.Nodes)
}

// EdgeCount returns the number of edges in the graph.
func (g *Graph) EdgeCount() int {
	count := 0
	for _, children := range g.Children {
		count += len(children)
	}
	return count
}

// AllNodes returns a slice of all table names in the graph.
func (g *Graph) AllNodes() []string {
	nodes := make([]string, 0, len(g.Nodes))
	for name := range g.Nodes {
		nodes = append(nodes, name)
	}
	return nodes
}

// AllEdges returns a slice of all edges in the graph.
func (g *Graph) AllEdges() []Edge {
	var edges []Edge
	for parent, children := range g.Children {
		for _, child := range children {
			edges = append(edges, Edge{From: parent, To: child})
		}
	}
	return edges
}

// LeafNodes returns all nodes with no children (leaf tables).
func (g *Graph) LeafNodes() []string {
	var leaves []string
	for name := range g.Nodes {
		if len(g.Children[name]) == 0 {
			leaves = append(leaves, name)
		}
	}
	return leaves
}

// InDegree returns the number of incoming edges (parents) for a node.
func (g *Graph) InDegree(name string) int {
	return len(g.Parents[name])
}

// OutDegree returns the number of outgoing edges (children) for a node.
func (g *Graph) OutDegree(name string) int {
	return len(g.Children[name])
}

// SetPK sets the primary key column name for a table.
// GA-P3-F3-T9: Support configurable PK columns for child tables
func (g *Graph) SetPK(table, pkColumn string) {
	g.pkColumns[table] = pkColumn
}

// GetPK returns the primary key column name for a table.
// Returns "id" as default if not explicitly set.
// GA-P3-F3-T9: Support configurable PK columns for child tables
func (g *Graph) GetPK(table string) string {
	if pk, exists := g.pkColumns[table]; exists {
		return pk
	}
	// Default to "id" for backward compatibility
	return "id"
}
