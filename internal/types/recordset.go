// Package types contains shared types used across multiple packages to avoid import cycles.
package types

import "time"

// RecordSet represents a set of discovered records organized by table.
type RecordSet struct {
	RootPKs []interface{}            // Root table primary keys
	Records map[string][]interface{} // table name -> PKs
	Stats   DiscoveryStats
}

// DiscoveryStats contains statistics about the discovery process.
type DiscoveryStats struct {
	TablesScanned int           // Number of tables processed
	RecordsFound  int64         // Total records discovered across all tables
	BFSLevels     int           // Depth of BFS traversal
	Duration      time.Duration // Time taken for discovery
}
