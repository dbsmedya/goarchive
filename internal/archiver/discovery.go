// Package archiver provides the core archive orchestration logic for GoArchive.
package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/dbsmedya/goarchive/internal/sqlutil"
	"github.com/dbsmedya/goarchive/internal/types"
)

// RecordDiscovery performs BFS traversal to discover related records
// starting from root table primary keys.
//
// GA-P3-F2-T1: BFS Traversal Structure
// GA-P3-F2-T3: Multi-level discovery
type RecordDiscovery struct {
	graph     *graph.Graph
	db        *sql.DB
	batchSize int
	logger    *logger.Logger
}

// NewRecordDiscovery creates a new discovery service with the given dependency graph,
// database connection, and batch size limit for IN clause chunking.
//
// Note: db can be nil for testing/simulation mode. In production, a valid
// database connection should always be provided.
func NewRecordDiscovery(g *graph.Graph, db *sql.DB, batchSize int) (*RecordDiscovery, error) {
	if g == nil {
		return nil, fmt.Errorf("graph is nil")
	}
	if batchSize <= 0 {
		batchSize = 1000 // Default batch size for IN clause
	}

	return &RecordDiscovery{
		graph:     g,
		db:        db,
		batchSize: batchSize,
		logger:    logger.NewDefault(),
	}, nil
}

// Discover finds all related records starting from the given root primary keys.
// It performs a breadth-first search traversal of the dependency graph to discover
// all child records through foreign key relationships.
//
// GA-P3-F2-T1: Queue-based BFS implementation
// GA-P3-F2-T3: Handles arbitrary nesting depth
// GA-P3-F2-T5: Logs discovery statistics
func (d *RecordDiscovery) Discover(ctx context.Context, rootPKs []interface{}) (*types.RecordSet, error) {
	startTime := time.Now()

	// GA-P3-F2-T6: Handle empty input
	if len(rootPKs) == 0 {
		d.logger.Debug("No root PKs provided, returning empty result")
		return &types.RecordSet{
			RootPKs: []interface{}{},
			Records: make(map[string][]interface{}),
			Stats: types.DiscoveryStats{
				TablesScanned: 0,
				RecordsFound:  0,
				BFSLevels:     0,
				Duration:      0,
			},
		}, nil
	}

	result := &types.RecordSet{
		RootPKs: rootPKs,
		Records: make(map[string][]interface{}),
	}

	// Add root table records
	rootTable := d.graph.Root
	result.Records[rootTable] = rootPKs

	// GA-P3-F2-T1: BFS queue structure
	// Each queue item contains: table name, PKs to process, and depth level
	type queueItem struct {
		table string
		pks   []interface{}
		level int
	}

	queue := []queueItem{
		{table: rootTable, pks: rootPKs, level: 0},
	}

	// Track visited tables to avoid reprocessing
	visited := make(map[string]bool)
	visited[rootTable] = true
	maxLevel := 0

	d.logger.Infof("Starting BFS discovery from root table %q with %d PKs", rootTable, len(rootPKs))

	// GA-P3-F2-T1: BFS traversal loop
	for len(queue) > 0 {
		// Check for context cancellation (graceful shutdown)
		select {
		case <-ctx.Done():
			result.Stats.Duration = time.Since(startTime)
			d.logger.Warnf("Discovery interrupted: %v", ctx.Err())
			return result, ctx.Err()
		default:
			// Continue processing
		}

		// Dequeue front item
		item := queue[0]
		queue = queue[1:]

		if item.level > maxLevel {
			maxLevel = item.level
		}

		// Get children of current table from dependency graph
		children := d.graph.GetChildren(item.table)
		if len(children) == 0 {
			// GA-P3-F2-T6: No children for this table (leaf node)
			d.logger.Debugf("Table %q has no children (leaf node)", item.table)
			continue
		}

		d.logger.Debugf("Processing table %q at level %d with %d PKs, %d children",
			item.table, item.level, len(item.pks), len(children))

		// GA-P3-F2-T3: Multi-level discovery - process each child table
		for _, childTable := range children {
			if visited[childTable] {
				// Already processed this table (prevents revisiting in DAG)
				continue
			}
			visited[childTable] = true

			// GA-P3-F2-T2: Fetch child IDs via database query or simulation
			var childPKs []interface{}
			var err error
			if d.db == nil {
				// Simulation mode for testing
				childPKs = d.simulateDiscovery(childTable, item.pks)
			} else {
				childPKs, err = d.fetchChildIDs(ctx, item.table, childTable, item.pks)
				if err != nil {
					return nil, fmt.Errorf("failed to discover %s records: %w", childTable, err)
				}
			}

			// GA-P3-F2-T6: Handle empty children (no related records)
			if len(childPKs) == 0 {
				d.logger.Debugf("No %s records found for %d parent PKs", childTable, len(item.pks))
				continue
			}

			d.logger.Debugf("Discovered %d %s records from %d parent PKs",
				len(childPKs), childTable, len(item.pks))

			// Store discovered PKs
			result.Records[childTable] = childPKs

			// Enqueue child table for further traversal
			queue = append(queue, queueItem{
				table: childTable,
				pks:   childPKs,
				level: item.level + 1,
			})
		}
	}

	// GA-P3-F2-T5: Populate statistics
	result.Stats.TablesScanned = len(result.Records)
	for _, pks := range result.Records {
		result.Stats.RecordsFound += int64(len(pks))
	}
	result.Stats.BFSLevels = maxLevel + 1
	result.Stats.Duration = time.Since(startTime)

	// GA-P3-F2-T5: Log completion statistics
	d.logger.Infof("Discovery complete: %d tables, %d records, %d levels, duration: %s",
		result.Stats.TablesScanned,
		result.Stats.RecordsFound,
		result.Stats.BFSLevels,
		result.Stats.Duration,
	)

	return result, nil
}

// fetchChildIDs queries the child table for all PKs that reference the parent PKs.
//
// GA-P3-F2-T2: Fetch child IDs using SQL queries with IN clause
// GA-P3-F2-T4: Returns only PKs (memory-efficient)
//
// Query format:
//
//	SELECT DISTINCT child_pk FROM child_table WHERE fk_column IN (parent_pks)
//
// For large parent PK sets, the query is chunked to avoid exceeding database limits.
func (d *RecordDiscovery) fetchChildIDs(ctx context.Context, parentTable, childTable string, parentPKs []interface{}) ([]interface{}, error) {
	if len(parentPKs) == 0 {
		return []interface{}{}, nil
	}

	// Get edge metadata to find FK column and reference key
	edgeMeta := d.graph.GetEdgeMeta(parentTable, childTable)
	if edgeMeta == nil {
		return nil, fmt.Errorf("no edge metadata found for %s -> %s", parentTable, childTable)
	}

	foreignKey := edgeMeta.ForeignKey // FK column in child table
	// referenceKey is the PK column in parent table (what FK references)
	// Used for documentation/debugging but not directly in the query
	_ = edgeMeta.ReferenceKey

	// GA-P3-F3-T9: Get PK column from graph (supports configurable PKs)
	childPK := d.graph.GetPK(childTable)

	// GA-P3-F2-T4: Fetch only PKs, not full rows (memory-efficient)
	// Use DISTINCT to avoid duplicates in 1-N relationships
	var allChildPKs []interface{}

	// Chunk parent PKs to avoid exceeding IN clause limits
	// MySQL default max_allowed_packet is 64MB, but IN clause with 1000+ items can be slow
	for i := 0; i < len(parentPKs); i += d.batchSize {
		end := i + d.batchSize
		if end > len(parentPKs) {
			end = len(parentPKs)
		}
		chunk := parentPKs[i:end]

		// Build query: SELECT DISTINCT id FROM child_table WHERE fk_column IN (?, ?, ...)
		placeholders := make([]string, len(chunk))
		for j := range placeholders {
			placeholders[j] = "?"
		}

		query := fmt.Sprintf(
			"SELECT DISTINCT %s FROM %s WHERE %s IN (%s)",
			sqlutil.QuoteIdentifier(childPK),
			sqlutil.QuoteIdentifier(childTable),
			sqlutil.QuoteIdentifier(foreignKey),
			strings.Join(placeholders, ", "),
		)

		rows, err := d.db.QueryContext(ctx, query, chunk...)
		if err != nil {
			return nil, fmt.Errorf("query failed for %s (chunk %d-%d): %w", childTable, i, end, err)
		}

		// Scan results
		for rows.Next() {
			var pk interface{}
			if err := rows.Scan(&pk); err != nil {
				rows.Close()
				return nil, fmt.Errorf("failed to scan %s PK: %w", childTable, err)
			}

			// MySQL driver returns int64 for integers, []byte for strings
			// Convert []byte to string for consistency
			if b, ok := pk.([]byte); ok {
				pk = string(b)
			}

			allChildPKs = append(allChildPKs, pk)
		}

		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("error iterating %s results: %w", childTable, err)
		}
		rows.Close()
	}

	return allChildPKs, nil
}

// DiscoverBatch is a convenience wrapper that enforces batch size limits on root PKs.
// This is useful when the caller has already fetched a large set of root PKs
// and wants to process them in manageable chunks.
func (d *RecordDiscovery) DiscoverBatch(ctx context.Context, rootPKs []interface{}) (*types.RecordSet, error) {
	if len(rootPKs) == 0 {
		return &types.RecordSet{
			RootPKs: []interface{}{},
			Records: make(map[string][]interface{}),
			Stats:   types.DiscoveryStats{},
		}, nil
	}

	// Limit root batch size if needed
	if len(rootPKs) > d.batchSize {
		d.logger.Warnf("Root PKs (%d) exceed batch size (%d), truncating", len(rootPKs), d.batchSize)
		rootPKs = rootPKs[:d.batchSize]
	}

	return d.Discover(ctx, rootPKs)
}

// GetGraph returns the dependency graph used by this discovery service.
func (d *RecordDiscovery) GetGraph() *graph.Graph {
	return d.graph
}

// GetBatchSize returns the configured batch size for IN clause chunking.
func (d *RecordDiscovery) GetBatchSize() int {
	return d.batchSize
}

// SetLogger sets a custom logger for the discovery service.
func (d *RecordDiscovery) SetLogger(log *logger.Logger) {
	d.logger = log
}

// simulateDiscovery simulates child record discovery for testing purposes.
// It generates synthetic child PKs based on the table name and parent PKs,
// allowing tests to run without a real database connection.
//
// This method is used by tests to verify discovery logic without requiring
// an actual database. It simulates:
//   - 1-N relationships: Returns multiple child records per parent (e.g., orders)
//   - 1-1 relationships: Returns exactly one child record per parent (e.g., profiles)
//   - Multi-level: Supports traversing the full graph depth
func (d *RecordDiscovery) simulateDiscovery(childTable string, parentPKs []interface{}) []interface{} {
	if len(parentPKs) == 0 {
		return []interface{}{}
	}

	switch childTable {
	case "orders":
		// 1-N relationship: 1-2 orders per user
		childPKs := []interface{}{}
		for i, parentPK := range parentPKs {
			// Each user has 1-2 orders
			numOrders := 1 + (i % 2)
			for j := 0; j < numOrders; j++ {
				childPK := fmt.Sprintf("order_%s_%d", parentPK, j+1)
				childPKs = append(childPKs, childPK)
			}
		}
		return childPKs

	case "profiles":
		// 1-1 relationship: exactly 1 profile per user
		childPKs := []interface{}{}
		for _, parentPK := range parentPKs {
			childPK := fmt.Sprintf("profile_%s", parentPK)
			childPKs = append(childPKs, childPK)
		}
		return childPKs

	case "order_items":
		// 1-N relationship: 2-3 items per order
		childPKs := []interface{}{}
		for i, parentPK := range parentPKs {
			// Each order has 2-3 items
			numItems := 2 + (i % 2)
			for j := 0; j < numItems; j++ {
				childPK := fmt.Sprintf("item_%s_%d", parentPK, j+1)
				childPKs = append(childPKs, childPK)
			}
		}
		return childPKs

	case "unknown_table":
		// Explicitly unknown table for testing - return empty
		return []interface{}{}

	default:
		// For generic test tables (A, B, C, etc.),
		// simulate a 1-N relationship with 1-2 records per parent
		childPKs := []interface{}{}
		for i, parentPK := range parentPKs {
			numChildren := 1 + (i % 2) // 1-2 children per parent
			for j := 0; j < numChildren; j++ {
				childPK := fmt.Sprintf("%s_child_%s_%d", childTable, parentPK, j+1)
				childPKs = append(childPKs, childPK)
			}
		}
		return childPKs
	}
}
