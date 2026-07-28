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
}

// newPreflightRun captures the graph's node list and constructs the source
// inspector. NewInspector performs no I/O, so construction cannot fail and issues
// no query. The destination inspector is constructed only when a destination has
// been configured; destTables reports the unconfigured case itself.
func newPreflightRun(p *PreflightChecker) *preflightRun {
	r := &preflightRun{
		tables:       p.graph.AllNodes(),
		srcInspector: validations.NewInspector(p.db, p.sourceDBName),
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
