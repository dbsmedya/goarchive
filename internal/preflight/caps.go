package preflight

import (
	"context"
	"fmt"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
)

// Cap is the statement cap of one kind of key-addressed statement (spec §4.3): the most
// key tuples one statement may carry before the range optimizer could exceed its memory
// budget and fall back to a full scan.
type Cap struct {
	Table     string
	Server    string   // "source" or "destination"
	Statement string   // "by key": copy read, verification, delete; "discovery": child rows by foreign_key
	Columns   []string // the columns of the index the statement addresses
	Tuples    int

	parts []key.Column // Columns' key types, for MaxTuples (the index addressed)
	// bind are the types of the values the statement binds, when they differ from parts:
	// discovery binds the parent's key, projected into references order, against the
	// child's foreign_key index (engine discoveryStatements; Codex M7c-1 review B2). The
	// packet bound (checkMaxAllowedPacket) charges them.
	bind []key.Column
}

// String names the cap as a run's "Statement cap:" log line does: server, statement, table and
// the addressed index's columns.
func (c Cap) String() string {
	return c.Server + " " + c.Statement + " on " + c.Table + columnList(c.Columns)
}

// binds are the types of the values c's statements bind.
func (c Cap) binds() []key.Column {
	if c.bind != nil {
		return c.bind
	}
	return c.parts
}

// reportStatementCaps computes and logs the effective caps from each server's
// range_optimizer_max_mem_size. The engine reads the variable again on its own sessions;
// these are what an operator sees before a run.
func reportStatementCaps(ctx context.Context, f *facts) error {
	srcMem, err := f.src.RangeOptimizerMaxMemSize(ctx)
	if err != nil {
		return inspectionError("statement_caps", err)
	}
	f.srcMem = srcMem
	specs, err := f.sourceSpecs(ctx)
	if err != nil {
		return inspectionError("statement_caps", err)
	}
	var caps []Cap
	for _, n := range f.tree.CopyOrder() {
		def := f.keys[n.Table]
		caps = append(caps, Cap{Table: n.Table, Server: "source", Statement: "by key", Columns: def.Names(),
			Tuples: key.MaxTuples(def.Columns, srcMem), parts: def.Columns})
		if n.Parent == nil {
			continue
		}
		cols := make([]key.Column, 0, len(n.ForeignKey))
		for _, name := range n.ForeignKey {
			spec, ok := columnSpec(specs[n.Table], name)
			if !ok {
				return fmt.Errorf("preflight statement caps: %s has no column %q", n.Table, name)
			}
			c, err := key.ColumnFromSpec(n.Table, spec)
			if err != nil {
				return fmt.Errorf("preflight statement caps: %w", err)
			}
			cols = append(cols, c)
		}
		parent := f.keys[n.Parent.Table]
		idx, err := n.Projection(parent.Names())
		if err != nil {
			return fmt.Errorf("preflight statement caps: %w", err)
		}
		caps = append(caps, Cap{Table: n.Table, Server: "source", Statement: "discovery", Columns: n.ForeignKey,
			Tuples: key.MaxTuples(cols, srcMem), parts: cols, bind: jobspec.ProjectColumns(parent.Columns, idx)})
	}
	// The destination's budget is read for every job type: purge has no destination
	// tables, but its tracking log statements run there (checkMaxAllowedPacket).
	dstMem, err := f.dst.RangeOptimizerMaxMemSize(ctx)
	if err != nil {
		return inspectionError("statement_caps", err)
	}
	f.dstMem = dstMem
	if f.dstKeys != nil {
		for _, t := range f.tables {
			def := f.dstKeys[t]
			caps = append(caps, Cap{Table: t, Server: "destination", Statement: "by key", Columns: def.Names(),
				Tuples: key.MaxTuples(def.Columns, dstMem), parts: def.Columns})
		}
	}
	f.caps = caps
	return nil
}
