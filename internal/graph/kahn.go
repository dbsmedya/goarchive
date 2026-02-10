package graph

import (
	"container/list"
	"errors"
	"fmt"
	"strings"
)

// ProcessingQueue wraps a list-based queue for Kahn's algorithm processing.
// It holds nodes that are ready to be processed (have in-degree of 0).
type ProcessingQueue struct {
	queue *list.List
}

// NewProcessingQueue creates a new empty processing queue.
func NewProcessingQueue() *ProcessingQueue {
	return &ProcessingQueue{
		queue: list.New(),
	}
}

// InitializeQueue creates a processing queue populated with all nodes
// that have in-degree of 0 (no dependencies). This is step 2 of Kahn's algorithm.
func (g *Graph) InitializeQueue(inDegree map[string]int) *ProcessingQueue {
	pq := NewProcessingQueue()

	for name, degree := range inDegree {
		if degree == 0 {
			pq.Enqueue(name)
		}
	}

	return pq
}

// Enqueue adds a node to the back of the queue.
func (pq *ProcessingQueue) Enqueue(node string) {
	pq.queue.PushBack(node)
}

// Dequeue removes and returns the node at the front of the queue.
// Returns empty string and false if queue is empty.
func (pq *ProcessingQueue) Dequeue() (string, bool) {
	if pq.queue.Len() == 0 {
		return "", false
	}
	elem := pq.queue.Front()
	pq.queue.Remove(elem)
	return elem.Value.(string), true
}

// Len returns the number of nodes in the queue.
func (pq *ProcessingQueue) Len() int {
	return pq.queue.Len()
}

// IsEmpty returns true if the queue has no nodes.
func (pq *ProcessingQueue) IsEmpty() bool {
	return pq.queue.Len() == 0
}

// CalculateInDegrees computes the number of incoming edges for each node
// in the graph. This is the first step of Kahn's algorithm for topological sorting.
// Returns a map of table name -> in-degree count.
func (g *Graph) CalculateInDegrees() map[string]int {
	inDegree := make(map[string]int)

	// Initialize all nodes with 0
	for name := range g.Nodes {
		inDegree[name] = 0
	}

	// Count incoming edges by iterating through all children relationships
	for _, children := range g.Children {
		for _, child := range children {
			inDegree[child]++
		}
	}

	return inDegree
}

// GetZeroInDegreeNodes returns all nodes with in-degree of 0.
// These are the starting nodes for Kahn's algorithm (nodes with no dependencies).
func (g *Graph) GetZeroInDegreeNodes(inDegree map[string]int) []string {
	var nodes []string
	for name, degree := range inDegree {
		if degree == 0 {
			nodes = append(nodes, name)
		}
	}
	return nodes
}

// ErrCycleDetected is returned when the dependency graph contains a cycle,
// making topological sorting impossible.
var ErrCycleDetected = errors.New("cycle detected in dependency graph")

// CycleInfo contains information about incomplete processing due to cycles.
type CycleInfo struct {
	TotalNodes        int      // Total number of nodes in the graph
	ProcessedNodes    int      // Number of nodes successfully processed
	UnprocessedNodes  []string // Nodes that couldn't be processed (part of or blocked by cycle)
	CycleParticipants []string // Nodes that are actually part of a cycle (subset of UnprocessedNodes)
	CyclePath         []string // Ordered path showing the cycle (e.g., [A, B, C, A])
}

// CycleError represents a cycle detection error with detailed information about
// which tables are involved and which are blocked by the cycle.
type CycleError struct {
	Info *CycleInfo
}

// Error implements the error interface with a descriptive message that includes
// the tables in the cycle and any tables blocked by the cycle.
func (e *CycleError) Error() string {
	msg := fmt.Sprintf("cycle detected in dependency graph: %d of %d tables could not be processed",
		len(e.Info.UnprocessedNodes), e.Info.TotalNodes)

	// Show the cycle path if available
	if len(e.Info.CyclePath) > 0 {
		msg += fmt.Sprintf("\nCycle path: %s", strings.Join(e.Info.CyclePath, " -> "))
	}

	// List tables that are actually part of the cycle
	if len(e.Info.CycleParticipants) > 0 {
		msg += fmt.Sprintf("\nTables in cycle: %s", strings.Join(e.Info.CycleParticipants, ", "))
	}

	// List tables that are blocked by the cycle but not part of it
	if len(e.Info.UnprocessedNodes) > len(e.Info.CycleParticipants) {
		participantSet := make(map[string]bool)
		for _, p := range e.Info.CycleParticipants {
			participantSet[p] = true
		}

		var blocked []string
		for _, u := range e.Info.UnprocessedNodes {
			if !participantSet[u] {
				blocked = append(blocked, u)
			}
		}

		if len(blocked) > 0 {
			msg += fmt.Sprintf("\nTables blocked by cycle: %s", strings.Join(blocked, ", "))
		}
	}

	return msg
}

// DetectIncompleteProcessing runs Kahn's algorithm and returns information
// about any nodes that couldn't be processed. If all nodes are processed,
// returns nil (no cycle). This is useful for diagnosing dependency issues.
func (g *Graph) DetectIncompleteProcessing() *CycleInfo {
	inDegree := g.CalculateInDegrees()
	queue := g.InitializeQueue(inDegree)

	processed := make(map[string]bool)

	// Process all reachable nodes
	for !queue.IsEmpty() {
		node, _ := queue.Dequeue()
		processed[node] = true

		for _, child := range g.GetChildren(node) {
			inDegree[child]--
			if inDegree[child] == 0 {
				queue.Enqueue(child)
			}
		}
	}

	// Check if all nodes were processed
	if len(processed) == len(g.Nodes) {
		return nil // No cycle detected
	}

	// Collect unprocessed nodes
	var unprocessed []string
	for name := range g.Nodes {
		if !processed[name] {
			unprocessed = append(unprocessed, name)
		}
	}

	// Build unprocessed set for cycle participant detection
	unprocessedSet := make(map[string]bool)
	for _, node := range unprocessed {
		unprocessedSet[node] = true
	}

	// Find actual cycle participants
	var cycleParticipants []string
	for _, node := range unprocessed {
		if g.canReachSelfInSet(node, unprocessedSet) {
			cycleParticipants = append(cycleParticipants, node)
		}
	}

	// Find the actual cycle path for better error messages
	var cyclePath []string
	if len(cycleParticipants) > 0 {
		cyclePath = g.FindCyclePath(cycleParticipants[0], unprocessedSet)
	}

	return &CycleInfo{
		TotalNodes:        len(g.Nodes),
		ProcessedNodes:    len(processed),
		UnprocessedNodes:  unprocessed,
		CycleParticipants: cycleParticipants,
		CyclePath:         cyclePath,
	}
}

// HasCycle returns true if the dependency graph contains a cycle.
// This is a convenience method that wraps DetectIncompleteProcessing.
func (g *Graph) HasCycle() bool {
	return g.DetectIncompleteProcessing() != nil
}

// FindCycleParticipants identifies nodes that are actually part of a cycle.
// Unlike UnprocessedNodes which includes nodes blocked by cycles, this returns
// only nodes that form the cycle itself. Uses DFS to detect back-edges.
func (g *Graph) FindCycleParticipants() []string {
	// Build set of unprocessed nodes first
	cycleInfo := g.DetectIncompleteProcessing()
	if cycleInfo == nil {
		return nil // No cycles
	}

	unprocessedSet := make(map[string]bool)
	for _, node := range cycleInfo.UnprocessedNodes {
		unprocessedSet[node] = true
	}

	// For each unprocessed node, check if it can reach itself
	// A node is a cycle participant if there's a path back to itself
	participants := make(map[string]bool)

	for _, startNode := range cycleInfo.UnprocessedNodes {
		if g.canReachSelf(startNode, unprocessedSet) {
			participants[startNode] = true
		}
	}

	// Convert to slice
	var result []string
	for node := range participants {
		result = append(result, node)
	}

	return result
}

// FindCyclePath finds the actual path that forms a cycle starting from the given node.
// Returns the ordered list of nodes forming the cycle (including the start node at both ends).
func (g *Graph) FindCyclePath(start string, allowedNodes map[string]bool) []string {
	visited := make(map[string]bool)
	path := []string{start}

	if g.dfsFindPath(start, start, visited, allowedNodes, &path) {
		return path
	}

	return nil
}

// dfsFindPath performs DFS to find a path back to the target node.
// Returns true if a path is found, and populates the path slice via pointer.
func (g *Graph) dfsFindPath(current, target string, visited, allowedNodes map[string]bool, path *[]string) bool {
	// Check all children of current node
	for _, child := range g.GetChildren(current) {
		// Skip if not in allowed set
		if !allowedNodes[child] {
			continue
		}

		// Found path back to target - append target to complete the cycle
		if child == target {
			*path = append(*path, target)
			return true
		}

		// Skip if already visited
		if visited[child] {
			continue
		}

		// Mark as visited and recurse
		visited[child] = true
		*path = append(*path, child)

		if g.dfsFindPath(child, target, visited, allowedNodes, path) {
			return true
		}

		// Backtrack
		*path = (*path)[:len(*path)-1]
	}

	return false
}

// canReachSelf checks if a node can reach itself through the subgraph
// defined by the allowedNodes set. Uses DFS with path tracking.
func (g *Graph) canReachSelf(start string, allowedNodes map[string]bool) bool {
	visited := make(map[string]bool)
	return g.dfsCanReach(start, start, visited, allowedNodes, true)
}

// canReachSelfInSet is an alias for canReachSelf for clarity in different contexts.
func (g *Graph) canReachSelfInSet(start string, nodeSet map[string]bool) bool {
	return g.canReachSelf(start, nodeSet)
}

// dfsCanReach performs DFS to check if we can reach the target node.
// isStart is true only for the initial call to avoid immediate self-match.
func (g *Graph) dfsCanReach(current, target string, visited, allowedNodes map[string]bool, isStart bool) bool {
	// Found a path back to start (but not on first call)
	if current == target && !isStart {
		return true
	}

	// Skip if already visited or not in allowed set
	if visited[current] {
		return false
	}
	if !allowedNodes[current] {
		return false
	}

	visited[current] = true

	// Check all children
	for _, child := range g.GetChildren(current) {
		if g.dfsCanReach(child, target, visited, allowedNodes, false) {
			return true
		}
	}

	return false
}

// TopologicalSort returns tables in topological order using Kahn's algorithm.
// The result is a valid copy order (parent tables first, child tables after).
// Returns ErrCycleDetected if the graph contains a cycle.
func (g *Graph) TopologicalSort() ([]string, error) {
	// Step 1: Calculate in-degrees for all nodes
	inDegree := g.CalculateInDegrees()

	// Step 2: Initialize queue with all zero in-degree nodes
	queue := g.InitializeQueue(inDegree)

	var result []string
	processed := 0

	// Step 3: Process nodes iteratively
	for !queue.IsEmpty() {
		// Dequeue the next node
		node, _ := queue.Dequeue()

		// Add to result (this node has all dependencies satisfied)
		result = append(result, node)
		processed++

		// Decrement in-degrees of all children
		// When a child's in-degree becomes 0, add it to the queue
		for _, child := range g.GetChildren(node) {
			inDegree[child]--
			if inDegree[child] == 0 {
				queue.Enqueue(child)
			}
		}
	}

	// Step 4: Check for cycles
	// If we didn't process all nodes, there must be a cycle
	if processed != len(g.Nodes) {
		cycleInfo := g.DetectIncompleteProcessing()
		return nil, &CycleError{Info: cycleInfo}
	}

	return result, nil
}

// CopyOrder returns the order in which tables should be copied during archiving.
// Parent tables are copied before child tables to satisfy foreign key constraints.
// This is the topological order of the dependency graph.
func (g *Graph) CopyOrder() ([]string, error) {
	return g.TopologicalSort()
}

// DeleteOrder returns the order in which tables should be deleted during archiving.
// Child tables are deleted before parent tables to satisfy foreign key constraints.
// This is the reverse of the topological order.
func (g *Graph) DeleteOrder() ([]string, error) {
	copyOrder, err := g.TopologicalSort()
	if err != nil {
		return nil, err
	}

	// Reverse the copy order to get delete order
	deleteOrder := make([]string, len(copyOrder))
	for i, table := range copyOrder {
		deleteOrder[len(copyOrder)-1-i] = table
	}

	return deleteOrder, nil
}

// Validate checks the graph for structural issues such as cycles.
// This should be called after building the graph to fail fast at startup
// rather than discovering issues during processing.
// Returns a CycleError if the graph contains cycles, nil otherwise.
func (g *Graph) Validate() error {
	// Check for cycles using Kahn's algorithm
	cycleInfo := g.DetectIncompleteProcessing()
	if cycleInfo != nil {
		return &CycleError{Info: cycleInfo}
	}

	return nil
}
