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
// A valid database connection is required for non-empty discovery.
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

	order, err := d.graph.CopyOrder()
	if err != nil {
		return nil, fmt.Errorf("failed to compute discovery order: %w", err)
	}

	levels := map[string]int{rootTable: 0}
	maxLevel := 0
	d.logger.Infof("Starting graph discovery from root table %q with %d PKs", rootTable, len(rootPKs))

	// One dedup set per child table, held for the whole traversal — child PKs
	// arriving via different parent edges dedup without rebuilding a map per edge.
	seen := make(map[string]map[interface{}]struct{}, len(order))

	// Process tables in topological order and accumulate child PKs across all parent paths.
	for _, table := range order {
		// Check for context cancellation (graceful shutdown)
		select {
		case <-ctx.Done():
			result.Stats.Duration = time.Since(startTime)
			d.logger.Warnf("Discovery interrupted: %v", ctx.Err())
			return result, ctx.Err()
		default:
		}

		parentPKs := result.Records[table]
		if len(parentPKs) == 0 {
			continue
		}

		level := levels[table]
		if level > maxLevel {
			maxLevel = level
		}

		children := d.graph.GetChildren(table)
		if len(children) == 0 {
			d.logger.Debugf("Table %q has no children (leaf node)", table)
			continue
		}

		d.logger.Debugf("Processing table %q at level %d with %d PKs, %d children",
			table, level, len(parentPKs), len(children))

		for _, childTable := range children {
			// GA-P3-F2-T2: Fetch child IDs via database query
			if d.db == nil {
				return nil, fmt.Errorf("discovery database is nil")
			}
			childPKs, err := d.fetchChildIDs(ctx, table, childTable, parentPKs)
			if err != nil {
				return nil, fmt.Errorf("failed to discover %s records: %w", childTable, err)
			}

			if len(childPKs) == 0 {
				d.logger.Debugf("No %s records found for %d parent PKs", childTable, len(parentPKs))
				continue
			}

			d.logger.Debugf("Discovered %d %s records from %d parent PKs",
				len(childPKs), childTable, len(parentPKs))

			set := tableSeen(seen, result.Records[childTable], childTable)
			result.Records[childTable] = appendUnique(result.Records[childTable], childPKs, set)

			nextLevel := level + 1
			if current, ok := levels[childTable]; !ok || nextLevel > current {
				levels[childTable] = nextLevel
			}
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

// tableSeen returns the persistent dedup set for table, creating it on first
// use seeded from PKs already recorded for that table (the root table is
// pre-populated before the BFS loop runs).
func tableSeen(seen map[string]map[interface{}]struct{}, existing []interface{}, table string) map[interface{}]struct{} {
	m, ok := seen[table]
	if !ok {
		m = make(map[interface{}]struct{}, len(existing))
		for _, v := range existing {
			m[v] = struct{}{}
		}
		seen[table] = m
	}
	return m
}

// appendUnique appends incoming PKs not already in seen. PK values are int64
// or string (fetchChildIDs converts []byte), so they are valid map keys and
// the map distinguishes types the same way the old "%T:%v" string keys did.
func appendUnique(existing, incoming []interface{}, seen map[interface{}]struct{}) []interface{} {
	for _, v := range incoming {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		existing = append(existing, v)
	}
	return existing
}

// fetchChildIDs queries the child table for all PKs that reference the parent PKs.
//
// GA-P3-F2-T2: Fetch child IDs using SQL queries with IN clause
// GA-P3-F2-T4: Returns only PKs (memory-efficient)
//
// Query format:
//
//	SELECT child_pk FROM child_table WHERE fk_column IN (parent_pks)
//
// child_pk is the table's PRIMARY KEY (preflight enforces a single-column PK),
// so every returned value is already unique; cross-chunk dedup happens in
// appendUnique.
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
	// child_pk is the table's PRIMARY KEY (preflight enforces a single-column PK),
	// so every returned value is already unique; cross-chunk dedup happens in
	// appendUnique.
	var allChildPKs []interface{}

	// Chunk parent PKs to avoid exceeding IN clause limits
	// MySQL default max_allowed_packet is 64MB, but IN clause with 1000+ items can be slow
	for i := 0; i < len(parentPKs); i += d.batchSize {
		end := i + d.batchSize
		if end > len(parentPKs) {
			end = len(parentPKs)
		}
		chunk := parentPKs[i:end]

		// child_pk is the table's PRIMARY KEY (preflight enforces a single-column PK),
		// so every returned value is already unique; cross-chunk dedup happens in
		// appendUnique.
		placeholders := make([]string, len(chunk))
		for j := range placeholders {
			placeholders[j] = "?"
		}

		query := fmt.Sprintf(
			"SELECT %s FROM %s WHERE %s IN (%s)",
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
				_ = rows.Close() // Ignore error during cleanup of failed operation
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
			_ = rows.Close() // Ignore error during cleanup of failed operation
			return nil, fmt.Errorf("error iterating %s results: %w", childTable, err)
		}
		_ = rows.Close() // Ignore error during cleanup of failed operation
	}

	return allChildPKs, nil
}

// SetLogger sets a custom logger for the discovery service.
func (d *RecordDiscovery) SetLogger(log *logger.Logger) {
	d.logger = log
}
