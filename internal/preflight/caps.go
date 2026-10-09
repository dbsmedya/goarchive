package preflight

import (
	"context"
	"fmt"

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
}

// reportStatementCaps computes and logs the effective caps from each server's
// range_optimizer_max_mem_size. The engine reads the variable again on its own sessions;
// these are what an operator sees before a run.
func reportStatementCaps(ctx context.Context, f *facts) error {
	srcMem, err := f.src.RangeOptimizerMaxMemSize(ctx)
	if err != nil {
		return inspectionError("statement_caps", err)
	}
	specs, err := f.sourceSpecs(ctx)
	if err != nil {
		return inspectionError("statement_caps", err)
	}
	var caps []Cap
	for _, n := range f.tree.CopyOrder() {
		def := f.keys[n.Table]
		caps = append(caps, Cap{n.Table, "source", "by key", def.Names(), key.MaxTuples(def.Columns, srcMem)})
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
		caps = append(caps, Cap{n.Table, "source", "discovery", n.ForeignKey, key.MaxTuples(cols, srcMem)})
	}
	if f.dstKeys != nil {
		dstMem, err := f.dst.RangeOptimizerMaxMemSize(ctx)
		if err != nil {
			return inspectionError("statement_caps", err)
		}
		for _, t := range f.tables {
			def := f.dstKeys[t]
			caps = append(caps, Cap{t, "destination", "by key", def.Names(), key.MaxTuples(def.Columns, dstMem)})
		}
	}
	for _, c := range caps {
		f.cfg.Logger.Infof("Statement cap: %s %s on %s%s: %d key tuples per statement", c.Server, c.Statement, c.Table, columnList(c.Columns), c.Tuples)
	}
	f.caps = caps
	return nil
}
