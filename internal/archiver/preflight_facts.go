package archiver

import (
	"context"
	"fmt"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
)

// preflightRun holds everything one preflight run acquires from the servers.
//
// It exists for exactly the duration of RunWithProfile and is never stored on
// PreflightChecker (spec §2). Two properties follow from that and are deliberate:
//
//   - Per-stage laziness. Each fact is fetched by the first stage that needs it, at
//     that stage's position in the fixed check order. A late stage's query error can
//     therefore never surface ahead of an earlier stage's finding.
//   - Detached values only. The cache holds plain values — []TableInfo, Grants,
//     ForeignKeyResult. It never holds a *sql.Conn or anything else that pins a pool
//     slot, so a max_connections:1 configuration cannot self-deadlock.
//
// The inspectors are deliberately NOT exposed by any accessor. A check that could
// reach an *Inspector could issue its own query and bypass the memoized fact
// methods, which would defeat both properties above. Fields, loaders and helpers
// are added by the phase that introduces their first production consumer.
//
// A run is used by one goroutine; nothing here is synchronised.
type preflightRun struct {
	tables []string

	srcInspector *validations.Inspector
	dstInspector *validations.Inspector

	srcTables       []validations.TableInfo
	srcTablesErr    error
	srcTablesLoaded bool

	dstTables       []validations.TableInfo
	dstTablesErr    error
	dstTablesLoaded bool

	invisible       []validations.InvisibleColumns
	invisibleErr    error
	invisibleLoaded bool

	srcDelTriggers       []validations.TriggerInfo
	srcDelTriggersErr    error
	srcDelTriggersLoaded bool

	dstInsTriggers       []validations.TriggerInfo
	dstInsTriggersErr    error
	dstInsTriggersLoaded bool

	pks       []validations.PKInfo
	pksErr    error
	pksLoaded bool

	srcColumns       []validations.TableColumns
	srcColumnsErr    error
	srcColumnsLoaded bool

	checker *PreflightChecker
}

// newPreflightRun captures the graph's node list and constructs the source
// inspector. NewInspector performs no I/O, so construction cannot fail and issues
// no query. The destination inspector is constructed only when a destination has
// been configured; destTables reports the unconfigured case itself.
func newPreflightRun(p *PreflightChecker) *preflightRun {
	r := &preflightRun{
		tables:       p.graph.AllNodes(),
		srcInspector: validations.NewInspector(p.db, p.sourceDBName),
		checker:      p,
	}
	if p.destinationDB != nil && p.destinationDBName != "" {
		r.dstInspector = validations.NewInspector(p.destinationDB, p.destinationDBName)
	}
	return r
}

func (r *preflightRun) graphTables() []string { return r.tables }

// sourceTables returns the source-side table facts, fetching them on first use.
// Both the value and any error are memoized: one broken connection must not produce
// two different verdicts within a run.
func (r *preflightRun) sourceTables(ctx context.Context) ([]validations.TableInfo, error) {
	if !r.srcTablesLoaded {
		r.srcTables, r.srcTablesErr = r.srcInspector.Tables(ctx, r.tables)
		r.srcTablesLoaded = true
	}
	return r.srcTables, r.srcTablesErr
}

// destTables returns the destination-side table facts, fetching them on first use.
// Both the value and any error are memoized, for the same reason sourceTables does it.
func (r *preflightRun) destTables(ctx context.Context) ([]validations.TableInfo, error) {
	if r.dstInspector == nil {
		return nil, fmt.Errorf("destination database not configured; call ConfigureDestination first")
	}
	if !r.dstTablesLoaded {
		r.dstTables, r.dstTablesErr = r.dstInspector.Tables(ctx, r.tables)
		r.dstTablesLoaded = true
	}
	return r.dstTables, r.dstTablesErr
}

// invisibleColumns returns one fact per graph table that has at least one INVISIBLE
// column, fetched on first use. Tables without invisible columns are absent from the
// result, so an empty slice means "none". Both the value and any error are memoized, for
// the same reason sourceTables does it: one broken connection must not produce two
// different verdicts within a run.
func (r *preflightRun) invisibleColumns(ctx context.Context) ([]validations.InvisibleColumns, error) {
	if !r.invisibleLoaded {
		r.invisible, r.invisibleErr = r.srcInspector.InvisibleColumns(ctx, r.tables)
		r.invisibleLoaded = true
	}
	return r.invisible, r.invisibleErr
}

// sourceDeleteTriggers returns DELETE triggers on the graph tables, fetched on first
// use. Triggers are sorted per table by firing order (BEFORE before AFTER) and then by
// name, so the reported trigger is deterministic. Both the value and any error are
// memoized.
func (r *preflightRun) sourceDeleteTriggers(ctx context.Context) ([]validations.TriggerInfo, error) {
	if !r.srcDelTriggersLoaded {
		r.srcDelTriggers, r.srcDelTriggersErr =
			r.srcInspector.Triggers(ctx, r.tables, validations.TriggerDelete)
		r.srcDelTriggersLoaded = true
	}
	return r.srcDelTriggers, r.srcDelTriggersErr
}

// destInsertTriggers returns INSERT triggers on the destination graph tables. It is a
// separate fact from sourceDeleteTriggers, on a separate connection and a separate
// event: neither memoizes the other, and a wrong event on one side is invisible from
// the other.
func (r *preflightRun) destInsertTriggers(ctx context.Context) ([]validations.TriggerInfo, error) {
	if r.dstInspector == nil {
		return nil, fmt.Errorf("destination database not configured; call ConfigureDestination first")
	}
	if !r.dstInsTriggersLoaded {
		r.dstInsTriggers, r.dstInsTriggersErr =
			r.dstInspector.Triggers(ctx, r.tables, validations.TriggerInsert)
		r.dstInsTriggersLoaded = true
	}
	return r.dstInsTriggers, r.dstInsTriggersErr
}

// primaryKeys returns one primary-key fact per graph table, fetched on first use.
// Missing tables are absent from the result; the existence check at position 1 has
// already rejected those. Both the value and any error are memoized, for the same
// reason sourceTables does it: one broken connection must not produce two different
// verdicts within a run.
func (r *preflightRun) primaryKeys(ctx context.Context) ([]validations.PKInfo, error) {
	if !r.pksLoaded {
		r.pks, r.pksErr = r.srcInspector.PrimaryKeys(ctx, r.tables)
		r.pksLoaded = true
	}
	return r.pks, r.pksErr
}

// expectedPKs maps each graph table to its configured primary_key. Tables with no
// configured key are omitted — ValidatePrimaryKeyColumns rejects those at position 2,
// and CheckPKMatchesExpected skips an empty expectation anyway.
func (r *preflightRun) expectedPKs() map[string]string {
	out := make(map[string]string, len(r.tables))
	for _, table := range r.tables {
		if r.checker.graph.HasPK(table) {
			out[table] = r.checker.graph.GetPK(table)
		}
	}
	return out
}

// sourceColumns returns every column of every graph table, with the server's exact
// spelling, fetched on first use. Both the value and any error are memoized, for the
// same reason sourceTables does it: one broken connection must not produce two
// different verdicts within a run.
//
// The complete fact is cached, not a name-only projection: ColumnInfo also carries
// DataType, Unsigned, Invisible and Generated, and a later phase needing any of those
// must not have to re-query. Columns returns slices.Clone'd values, so this is already
// detached.
func (r *preflightRun) sourceColumns(ctx context.Context) ([]validations.TableColumns, error) {
	if !r.srcColumnsLoaded {
		r.srcColumns, r.srcColumnsErr = r.srcInspector.Columns(ctx, r.tables)
		r.srcColumnsLoaded = true
	}
	return r.srcColumns, r.srcColumnsErr
}

// columnNamesByTable projects the column fact down to what the PK-identity policy
// needs. It lives at the stage/policy boundary, NOT in the accessor, so the cache stays
// lossless while judgePrimaryKeyColumns stays a pure function over plain Go values.
func columnNamesByTable(facts []validations.TableColumns) map[string][]string {
	out := make(map[string][]string, len(facts))
	for _, fact := range facts {
		names := make([]string, 0, len(fact.Columns))
		for _, col := range fact.Columns {
			names = append(names, col.Name)
		}
		out[fact.Table] = names
	}
	return out
}

// rootPKInfo returns the primary-key fact for the graph root, reporting whether one
// was found.
//
// It selects BY ROOT NAME rather than by position. Graph.AllNodes() ranges over a map,
// so r.tables — and therefore the fact order, which PrimaryKeys preserves from the
// request — is nondeterministic. Indexing the slice would be a coin flip on any graph
// with more than one table.
func (r *preflightRun) rootPKInfo(ctx context.Context) (validations.PKInfo, bool, error) {
	facts, err := r.primaryKeys(ctx)
	if err != nil {
		return validations.PKInfo{}, false, err
	}
	root := r.checker.graph.Root
	for _, pk := range facts {
		if pk.Table == root {
			return pk, true, nil
		}
	}
	return validations.PKInfo{}, false, nil
}
