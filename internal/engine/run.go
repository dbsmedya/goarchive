package engine

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/dbsmedya/goarchive/v3/internal/database"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
	"github.com/dbsmedya/goarchive/v3/internal/types"
)

// run is one job's state for the length of a run.
type run struct {
	cfg  Config
	log  *logger.Logger
	typ  jobspec.Type
	tree *jobspec.Tree
	root key.Def
	keys map[string]key.Def              // every table's key, as the source reports it (preflight)
	meta map[string]types.ColumnMetadata // every table's columns, as the source reports them
	caps caps
	job  *tracking.Job

	report   *Report
	progress *progressTracker // nil unless --progress is on
}

// caps are the statement caps preflight computed (spec §4.3), by table. The engine never
// sends a key-addressed statement with more tuples than the cap of the index it addresses
// (M5-1): the table's PRIMARY KEY on each server for reads, verification and deletes, the
// child's foreign_key columns for discovery.
type caps struct {
	source, destination, discovery map[string]int
}

func capsOf(res *preflight.Result) caps {
	c := caps{source: map[string]int{}, destination: map[string]int{}, discovery: map[string]int{}}
	for _, cp := range res.Caps {
		switch {
		case cp.Server == "source" && cp.Statement == "by key":
			c.source[cp.Table] = cp.Tuples
		case cp.Server == "source" && cp.Statement == "discovery":
			c.discovery[cp.Table] = cp.Tuples
		case cp.Server == "destination" && cp.Statement == "by key":
			c.destination[cp.Table] = cp.Tuples
		}
	}
	return c
}

func newRun(ctx context.Context, cfg Config, res *preflight.Result, job *tracking.Job) (*run, error) {
	r := &run{
		cfg: cfg, log: cfg.Logger, typ: cfg.Spec.Type, tree: jobspec.NewTree(cfg.Spec),
		keys: res.Keys, caps: capsOf(res), job: job,
		report: &Report{Copied: map[string]int64{}, Deleted: map[string]int64{}, Conversions: map[string]map[uint16]uint64{}},
	}
	r.root = res.Keys[cfg.Spec.RootTable]
	var tables []string
	for _, n := range r.tree.CopyOrder() {
		tables = append(tables, n.Table)
		if _, ok := r.keys[n.Table]; !ok {
			return nil, fmt.Errorf("engine: preflight returned no key for %s", n.Table)
		}
		if r.caps.source[n.Table] < 1 {
			return nil, fmt.Errorf("engine: preflight returned no statement cap for %s", n.Table)
		}
		if n.Parent != nil && r.caps.discovery[n.Table] < 1 {
			return nil, fmt.Errorf("engine: preflight returned no discovery cap for %s", n.Table)
		}
		if r.typ != jobspec.Purge && r.caps.destination[n.Table] < 1 {
			return nil, fmt.Errorf("engine: preflight returned no destination statement cap for %s", n.Table)
		}
	}
	if r.typ != jobspec.Purge {
		src, err := database.ReadServerIdentity(ctx, cfg.Source)
		if err != nil {
			return nil, err
		}
		meta, err := sourceColumns(ctx, cfg.Source, src.Schema, tables)
		if err != nil {
			return nil, err
		}
		r.meta = meta
	}
	return r, nil
}

// byKeyChunk is how many keys of table one read, verification statement carries: within
// the batch size, the source cap and the destination cap, so both servers see the same
// chunks in the same order.
func (r *run) byKeyChunk(table string) int {
	n := min(r.cfg.Processing.BatchSize, r.caps.source[table])
	if d, ok := r.caps.destination[table]; ok {
		n = min(n, d)
	}
	return n
}

func (r *run) discoveryChunk(table string) int {
	return min(r.cfg.Processing.BatchSize, r.caps.discovery[table])
}

func (r *run) deleteChunk(table string) int {
	return min(r.cfg.Processing.BatchDeleteSize, r.caps.source[table])
}

// execute recovers the job's logged work, then runs the loop.
func (r *run) execute(ctx context.Context) error {
	if err := r.recover(ctx); err != nil {
		return fmt.Errorf("engine: recovery: %w", err)
	}
	if r.report.Stopped {
		return nil
	}
	if r.cfg.Progress > 0 {
		stop, err := r.startProgress(ctx)
		if err != nil {
			return err
		}
		defer stop()
	}
	return r.loop(ctx)
}

// startProgress counts the where range, now that recovery is done, and starts the
// --progress reporter (R50). The returned stop prints the final line.
func (r *run) startProgress(ctx context.Context) (func(), error) {
	var total int64
	if err := r.cfg.Source.QueryRowContext(ctx, rangeCountSQL(r.root, r.cfg.Spec.Where)).Scan(&total); err != nil {
		return nil, fmt.Errorf("engine: --progress count on %s: %w", r.root.Table, err)
	}
	r.progress = newProgressTracker(total)
	r.trackProgress()
	out := r.cfg.Out
	if out == nil {
		out = os.Stdout
	}
	rep := newProgressReporter(r.progress, r.cfg.Progress, r.typ, out)
	rep.start()
	return rep.stop, nil
}

// trackProgress feeds the run's totals to the progress tracker.
func (r *run) trackProgress() {
	var copied, deleted int64
	for _, n := range r.report.Copied {
		copied += n
	}
	for _, n := range r.report.Deleted {
		deleted += n
	}
	r.progress.update(r.report.Roots+r.report.Skipped, copied, deleted)
}

// loop runs batches from the start of the where range (spec §6.5): each fetch continues
// after the last key of the previous batch, held in memory only.
func (r *run) loop(ctx context.Context) error {
	sentinel := newSentinelGate(r.cfg.Processing.SentinelFile, r.log)
	sentinel.progress = r.progress
	var last key.Key
	for {
		if stopRequested(r.cfg.Stop) {
			r.report.Stopped = true
			return nil
		}
		if err := sentinel.wait(ctx, r.cfg.Stop); err != nil {
			return err
		}
		if stopRequested(r.cfg.Stop) {
			r.report.Stopped = true
			return nil
		}
		roots, err := r.fetch(ctx, r.cfg.Source, last)
		if err != nil {
			return err
		}
		if len(roots) == 0 {
			r.progress.markComplete()
			return nil
		}
		last = roots[len(roots)-1]
		if err := r.batch(ctx, roots); err != nil {
			return fmt.Errorf("engine: batch %d: %w", r.report.Batches+1, err)
		}
		r.report.Batches++
		r.trackProgress()
		if len(roots) < r.cfg.Processing.BatchSize {
			r.progress.markComplete()
			return nil
		}
		if err := interruptibleSleep(ctx, r.cfg.Stop, seconds(r.cfg.Processing.SleepSeconds)); err != nil {
			return err
		}
	}
}

// fetch reads the next batch of root keys (spec §7.2) on q: the source pool for a run, the
// connection Explain already holds for dry-run (Codex M6 B1).
func (r *run) fetch(ctx context.Context, q reader, last key.Key) ([]key.Key, error) {
	var args []any
	if last != nil {
		args = key.AfterArgs(last)
	}
	args = append(args, r.cfg.Processing.BatchSize)
	rows, err := q.QueryContext(ctx, fetchSQL(r.root, r.cfg.Spec.Where, last != nil), args...)
	if err != nil {
		return nil, fmt.Errorf("engine: root fetch on %s: %w", r.root.Table, err)
	}
	defer func() { _ = rows.Close() }()
	var out []key.Key
	for rows.Next() {
		k, err := r.root.Scan(rows)
		if err != nil {
			return nil, fmt.Errorf("engine: root fetch on %s: %w", r.root.Table, err)
		}
		out = append(out, k)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("engine: root fetch on %s: %w", r.root.Table, err)
	}
	return out, nil
}

// batch runs the job type's phases over one batch of root keys (spec §7.1).
func (r *run) batch(ctx context.Context, roots []key.Key) error {
	src, err := r.cfg.Source.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()

	if r.typ == jobspec.CopyOnly && r.cfg.Spec.ResumeEnabled() {
		if roots, err = r.dropLogged(ctx, roots); err != nil || len(roots) == 0 {
			return err
		}
	}
	t, err := r.discover(ctx, src, roots)
	if err != nil {
		return err
	}
	switch r.typ {
	case jobspec.Archive:
		if err := r.copy(ctx, src, t, tracking.StatusCopied); err != nil {
			return err
		}
		r.reached(atCommitted)
		if err := r.deleteTree(ctx, src, t); err != nil {
			return err
		}
		if err := r.complete(ctx, tracking.StatusCopied, roots); err != nil {
			return err
		}
	case jobspec.CopyOnly:
		if err := r.copy(ctx, src, t, tracking.StatusCompleted); err != nil {
			return err
		}
		r.reached(atCommitted)
	case jobspec.Purge:
		if err := r.waitForLag(ctx); err != nil {
			return err
		}
		if err := r.logDeleting(ctx, roots); err != nil {
			return err
		}
		r.reached(atLogged)
		if err := r.deleteTree(ctx, src, t); err != nil {
			return err
		}
		if err := r.complete(ctx, tracking.StatusDeleting, roots); err != nil {
			return err
		}
	}
	r.report.Roots += int64(len(roots))
	r.log.Infof("Batch %d: %d roots of %s (%s)", r.report.Batches+1, len(roots), r.root.Table, r.typ)
	return nil
}

func (r *run) reached(p point) {
	if r.cfg.at != nil {
		r.cfg.at(p)
	}
}

func (r *run) waitForLag(ctx context.Context) error {
	if r.cfg.Lag == nil || r.typ == jobspec.CopyOnly {
		return nil
	}
	if err := r.cfg.Lag.WaitForLag(ctx); err != nil {
		return fmt.Errorf("engine: replication lag gate: %w", err)
	}
	return nil
}

// inDestinationTx runs f in a destination transaction and commits it.
func (r *run) inDestinationTx(ctx context.Context, f func(*sql.Tx) error) error {
	tx, err := r.cfg.Destination.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := f(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// logDeleting logs purge roots as deleting, in their own transaction, before any delete
// (spec §6.3).
func (r *run) logDeleting(ctx context.Context, roots []key.Key) error {
	return r.inDestinationTx(ctx, func(tx *sql.Tx) error {
		return r.job.Insert(ctx, tx, tracking.StatusDeleting, roots)
	})
}

// complete moves roots from status from to completed after their deletes (spec §6.3).
func (r *run) complete(ctx context.Context, from tracking.Status, roots []key.Key) error {
	return r.inDestinationTx(ctx, func(tx *sql.Tx) error {
		return r.job.Transition(ctx, tx, from, tracking.StatusCompleted, roots)
	})
}

// prepare passes st through the fault seam.
func (r *run) prepare(st *statement) {
	if r.cfg.fault != nil {
		r.cfg.fault(st)
	}
}

// queryKeys runs a statement that reads keys of d on q, then checks for warning 3170 on the
// same session (spec §7.1a).
func (r *run) queryKeys(ctx context.Context, q queryer, d key.Def, st statement) ([]key.Key, error) {
	r.prepare(&st)
	rows, err := q.QueryContext(ctx, st.query, st.args...)
	if err != nil {
		return nil, fmt.Errorf("engine: %s on %s: %w", st.op, st.table, err)
	}
	var out []key.Key
	for rows.Next() {
		k, err := d.Scan(rows)
		if err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("engine: %s on %s: %w", st.op, st.table, err)
		}
		out = append(out, k)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("engine: %s on %s: %w", st.op, st.table, err)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := key.CheckRangeWarning(ctx, q); err != nil {
		return nil, fmt.Errorf("engine: %s on %s: %w", st.op, st.table, err)
	}
	return out, nil
}

// reader is a pool, a pinned connection or a transaction, for a read that checks no warning.
type reader interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// queryer is a pinned connection or a transaction: never a pool, whose SHOW WARNINGS might
// run on another session (key.CheckRangeWarning refuses one).
type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}
