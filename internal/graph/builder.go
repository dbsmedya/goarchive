package graph

import (
	"fmt"

	"github.com/dbsmedya/goarchive/internal/config"
)

// Builder constructs a dependency graph from job configuration.
type Builder struct {
	job *config.JobConfig
}

// NewBuilder creates a new graph builder for the given job configuration.
func NewBuilder(job *config.JobConfig) *Builder {
	return &Builder{job: job}
}

// Build constructs the dependency graph from the job configuration.
// It parses all relations (including nested) and creates the graph structure.
func (b *Builder) Build() (*Graph, error) {
	if b.job == nil {
		return nil, fmt.Errorf("job configuration is nil")
	}

	if b.job.RootTable == "" {
		return nil, fmt.Errorf("root table is not specified")
	}

	if b.job.PrimaryKey == "" {
		return nil, fmt.Errorf("primary key is not specified for root table %q", b.job.RootTable)
	}

	// Create graph with root table
	g := NewGraph(b.job.RootTable, b.job.PrimaryKey)

	// Parse all relations starting from root
	if err := b.parseRelations(g, b.job.RootTable, b.job.PrimaryKey, b.job.Relations); err != nil {
		return nil, fmt.Errorf("failed to parse relations: %w", err)
	}

	// Validate graph structure (fail fast on cycles)
	if err := g.Validate(); err != nil {
		return nil, fmt.Errorf("graph validation failed: %w", err)
	}

	return g, nil
}

// parseRelations recursively parses relations and adds them to the graph.
// parentTable is the table these relations belong to.
// parentPK is the primary key of the parent table (used as reference key for children).
func (b *Builder) parseRelations(g *Graph, parentTable, parentPK string, relations []config.Relation) error {
	for _, rel := range relations {
		if rel.Table == "" {
			return fmt.Errorf("relation table name is empty under parent %q", parentTable)
		}

		if rel.ForeignKey == "" {
			return fmt.Errorf("foreign key is not specified for relation %q", rel.Table)
		}

		// Determine dependency type (default to "1-N" if not specified)
		depType := rel.DependencyType
		if depType == "" {
			depType = "1-N"
		}

		// Validate dependency type
		if depType != "1-1" && depType != "1-N" {
			return fmt.Errorf("invalid dependency type %q for relation %q (must be '1-1' or '1-N')", depType, rel.Table)
		}

		// Check for duplicate nodes (same table appearing twice)
		if g.HasNode(rel.Table) {
			return fmt.Errorf("duplicate relation: table %q appears multiple times in the graph", rel.Table)
		}

		// Create node for this relation
		node := &Node{
			Name:           rel.Table,
			ForeignKey:     rel.ForeignKey,
			ReferenceKey:   parentPK,
			DependencyType: depType,
			IsRoot:         false,
		}
		g.AddNode(rel.Table, node)

		// Add edge from parent to child with metadata
		g.AddEdgeWithMeta(parentTable, rel.Table, rel.ForeignKey, parentPK, depType)

		// GA-P2-F1-T3: Enforce explicit primary key specification
		// FAIL if primary_key is not specified - no default fallback to "id"
		childPK := rel.PrimaryKey
		if childPK == "" {
			return fmt.Errorf("primary_key is not specified for relation %q (explicit PK required, no default to 'id')", rel.Table)
		}
		g.SetPK(rel.Table, childPK)

		// Recursively parse nested relations
		if len(rel.Relations) > 0 {
			if err := b.parseRelations(g, rel.Table, childPK, rel.Relations); err != nil {
				return err
			}
		}
	}

	return nil
}

// BuildFromJob is a convenience function that builds a graph directly from a job config.
func BuildFromJob(job *config.JobConfig) (*Graph, error) {
	return NewBuilder(job).Build()
}
