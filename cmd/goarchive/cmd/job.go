package cmd

import (
	"context"
	"fmt"

	"github.com/dbsmedya/goarchive/v3/internal/database"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// loadJob loads the configuration file and picks the named job for subcommand typ, before
// any connection is opened (spec §5, R4). Commands that accept every job type pass "".
func loadJob(name string, typ jobspec.Type) (*jobspec.Document, *jobspec.Spec, error) {
	doc, err := jobspec.Load(GetConfigFile())
	if err != nil {
		return nil, nil, usage(err)
	}
	spec, err := doc.Job(name, typ)
	if err != nil {
		return nil, nil, usage(err)
	}
	return doc, spec, nil
}

// connect opens the source, destination and replica pools. internal/database keeps the
// source/destination identity guard, the UTC pin and the AUTO_INCREMENT-zero session mode
// (spec §9).
func connect(ctx context.Context, doc *jobspec.Document) (*database.Manager, error) {
	m := database.NewManager(doc.Shared)
	if err := m.Connect(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect to databases: %w", err)
	}
	return m, nil
}
