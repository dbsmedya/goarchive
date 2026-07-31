package archiver

import (
	"context"
	"database/sql"
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

	dstGrants       validations.Grants
	dstGrantsErr    error
	dstGrantsLoaded bool

	srcGrants       validations.Grants
	srcGrantsErr    error
	srcGrantsLoaded bool

	fkOut       validations.ForeignKeyResult
	fkOutErr    error
	fkOutLoaded bool

	fkIn       validations.ForeignKeyResult
	fkInErr    error
	fkInLoaded bool

	fkWi       validations.ForeignKeyResult
	fkWiErr    error
	fkWiLoaded bool

	specs       []specPair
	specsErr    error
	specsLoaded bool

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
// column. Tables without invisible columns are absent from the result, so an empty
// slice means "none".
//
// This is a PROJECTION over sourceColumns, not a second query: ColumnInfo.Invisible
// already carries the same predicate Inspector.InvisibleColumns used
// (parseColumnExtra's case-insensitive "INVISIBLE" substring check on EXTRA, matching
// the SQL EXTRA LIKE '%INVISIBLE%' the library used before), in the same table and
// column order (request order, then ORDINAL_POSITION). Re-querying would violate the
// standing non-negotiable ("never re-query a fact the library answers") and could
// observe different schema state than position 2 already acquired. Deriving from the
// cache means invisibleColumns inherits sourceColumns' memoization for free: a second
// call here costs nothing and cannot see a different answer.
func (r *preflightRun) invisibleColumns(ctx context.Context) ([]validations.InvisibleColumns, error) {
	facts, err := r.sourceColumns(ctx)
	if err != nil {
		return nil, err
	}
	var out []validations.InvisibleColumns
	for _, fact := range facts {
		var cols []string
		for _, col := range fact.Columns {
			if col.Invisible {
				cols = append(cols, col.Name)
			}
		}
		if len(cols) > 0 {
			out = append(out, validations.InvisibleColumns{Table: fact.Table, Columns: cols})
		}
	}
	return out, nil
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

// readGrants obtains a dedicated connection, reads the privilege fact through it, and
// RELEASES IT IMMEDIATELY — before any other query runs.
//
// Two reasons, both load-bearing:
//
//   - Answer quality. Inspector.Grants gives its strongest answers through an exact
//     *sql.Conn; through a pool it "may confirm a direct-account positive but degrades
//     every negative", which under D1's GrantPresent-or-fail rule would turn correct
//     passes into failures.
//   - Deadlock safety. GoArchive supports max_connections: 1, applied straight to
//     SetMaxOpenConns (internal/database/database.go:137). Holding this Conn across any
//     other query would block that configuration forever.
//
// GoArchive never issues SET ROLE, so no session affinity survives the read and none is
// needed. What is cached by the caller is the detached Grants VALUE, never the Conn.
func readGrants(ctx context.Context, db *sql.DB, schema string) (validations.Grants, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return validations.Grants{}, fmt.Errorf("acquire connection for privilege inspection: %w", err)
	}
	grants, gerr := validations.NewInspector(conn, schema).Grants(ctx)
	if cerr := conn.Close(); cerr != nil && gerr == nil {
		return validations.Grants{}, fmt.Errorf("release privilege-inspection connection: %w", cerr)
	}
	if gerr != nil {
		return validations.Grants{}, gerr
	}
	return grants, nil
}

// destGrants returns the destination account's privilege fact, read once per run. It
// serves both the data-table checks and the tracking-schema check: Grants is an
// account-wide fact, so one read answers for every schema on that server.
func (r *preflightRun) destGrants(ctx context.Context) (validations.Grants, error) {
	if r.checker.destinationDB == nil {
		return validations.Grants{}, fmt.Errorf("destination database not configured; call ConfigureDestination first")
	}
	if !r.dstGrantsLoaded {
		r.dstGrants, r.dstGrantsErr = readGrants(ctx, r.checker.destinationDB, r.checker.destinationDBName)
		r.dstGrantsLoaded = true
	}
	return r.dstGrants, r.dstGrantsErr
}

// sourceGrants returns the source account's privilege fact, read once per run.
func (r *preflightRun) sourceGrants(ctx context.Context) (validations.Grants, error) {
	if !r.srcGrantsLoaded {
		r.srcGrants, r.srcGrantsErr = readGrants(ctx, r.checker.db, r.checker.sourceDBName)
		r.srcGrantsLoaded = true
	}
	return r.srcGrants, r.srcGrantsErr
}

// fkOutgoing returns constraints whose CHILD is a graph table — the set FK_INDEX_CHECK
// judges, matching 1.8's in-graph-child filter. The selector IS the filter: OutgoingFrom
// already restricts to in-graph children, which is why the migrated check carries no
// manual graph-membership loop (spec §3.5).
//
// ForeignKeyResult.Visibility is deliberately not consulted by this fact's only consumer;
// see Ambiguity 2. Completeness is enforced on the INCOMING fetch (phase 025), where it
// is the whole question.
func (r *preflightRun) fkOutgoing(ctx context.Context) (validations.ForeignKeyResult, error) {
	if !r.fkOutLoaded {
		r.fkOut, r.fkOutErr = r.srcInspector.ForeignKeys(ctx, validations.OutgoingFrom(r.tables...))
		r.fkOutLoaded = true
	}
	return r.fkOut, r.fkOutErr
}

// fkIncoming returns constraints whose PARENT is a graph table — the set that can pull
// rows out from outside the archive. Its ForeignKeyResult also carries the metadata
// visibility proof, so FK_COVERAGE_VISIBILITY_CHECK and FK_COVERAGE_CHECK read facts and
// proof from the same result (spec §3.5): they cannot disagree about what was seen.
func (r *preflightRun) fkIncoming(ctx context.Context) (validations.ForeignKeyResult, error) {
	if !r.fkInLoaded {
		r.fkIn, r.fkInErr = r.srcInspector.ForeignKeys(ctx, validations.IncomingTo(r.tables...))
		r.fkInLoaded = true
	}
	return r.fkIn, r.fkInErr
}

// fkWithin returns constraints with BOTH endpoints in the graph — the set
// INTERNAL_FK_COVERAGE reconciles against the relations configuration.
func (r *preflightRun) fkWithin(ctx context.Context) (validations.ForeignKeyResult, error) {
	if !r.fkWiLoaded {
		r.fkWi, r.fkWiErr = r.srcInspector.ForeignKeys(ctx, validations.Within(r.tables...))
		r.fkWiLoaded = true
	}
	return r.fkWi, r.fkWiErr
}

// specPair holds one graph table's specification from both sides. Side A is the source,
// side B is the destination — the argument order DiffSpecs uses, so a SideA diff always
// means "the source lacks it".
type specPair struct {
	Table string
	A     validations.TableSpec
	B     validations.TableSpec
}

// tableSpecs captures both sides' specifications for every graph table, fetched on first
// use.
//
// Capture options are fixed by spec §3.3: WithIndexes() on BOTH sides, and deliberately
// NOT WithConstraints() or WithComment(). Constraints and comments therefore stay outside
// comparison scope — silently ignored, exactly as the 1.8 column-only comparison did.
// Capturing indexes symmetrically is what makes an IndexUnconfirmed diff mean "the
// library could not look", which the evaluator treats as an inspection-integrity failure.
func (r *preflightRun) tableSpecs(ctx context.Context) ([]specPair, error) {
	if r.specsLoaded {
		return r.specs, r.specsErr
	}
	r.specsLoaded = true
	if r.dstInspector == nil {
		r.specsErr = fmt.Errorf("destination database not configured; call ConfigureDestination first")
		return nil, r.specsErr
	}

	pairs := make([]specPair, 0, len(r.tables))
	for _, table := range r.tables {
		a, err := r.srcInspector.TableSpec(ctx,
			validations.Ref(r.checker.sourceDBName, table), validations.WithIndexes())
		if err != nil {
			r.specsErr = fmt.Errorf("source table %s: %w", table, err)
			return nil, r.specsErr
		}
		b, err := r.dstInspector.TableSpec(ctx,
			validations.Ref(r.checker.destinationDBName, table), validations.WithIndexes())
		if err != nil {
			r.specsErr = fmt.Errorf("destination table %s: %w", table, err)
			return nil, r.specsErr
		}
		pairs = append(pairs, specPair{Table: table, A: a, B: b})
	}
	r.specs = pairs
	return pairs, nil
}
