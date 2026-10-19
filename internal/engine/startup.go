package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dbsmedya/dbsgomysql/pkg/sqlutil"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/lock"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
)

// A run holds two lock sets for its whole length (source-table-locks spec §4, §5): the job
// lock on the destination, beside the tracking tables, and a lock on every table of its
// tree on the source, the one server every conflicting run shares. The heartbeat is kept
// from 2.x (core spec §9).
const (
	// staleAfter is how old a running job's heartbeat may be before the unfinished-work
	// guard's refusal calls it stopped rather than recent.
	staleAfter = 60 * time.Second
	// heartbeatFailures consecutive failed heartbeats fail the run.
	heartbeatFailures = 3
)

// heartbeatEvery and keepAliveEvery are the heartbeat and the lock-ownership check, as in
// 2.x. They are variables so a test can shorten them.
var (
	heartbeatEvery = 15 * time.Second
	keepAliveEvery = 30 * time.Second
)

// OverlapError refuses a run while another job on the same source data shares a table
// with it and is marked running or holds unfinished roots in its log (unfinished-work guard
// spec §3, §7). Why holds, by job name, why a job recorded on another source server counts as
// this source's data (§3a).
type OverlapError struct {
	Job, Database string
	Registry      string // the qualified archiver_job, for the remedy
	Blockers      []tracking.Blocker
	Why           map[string]string
}

func (e *OverlapError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "engine: job %q shares source tables in %s with jobs that are marked running or stopped with unfinished work "+
		"(registry %s). This run did nothing.", e.Job, sqlutil.QuoteIdentifier(e.Database), e.Registry)
	var pending, recent, stopped, unreadable bool
	var idle []tracking.Blocker
	for _, j := range e.Blockers {
		b.WriteString("\n  " + j.Name + ": " + blockerState(j))
		if j.Unreadable != nil {
			fmt.Fprintf(&b, "; its config_fingerprint cannot be read (%v)", j.Unreadable)
			unreadable = unreadable || j.Pending
		} else {
			b.WriteString("; shares " + strings.Join(j.Shared, ", "))
		}
		if why := e.Why[j.Name]; why != "" {
			fmt.Fprintf(&b, "; recorded on source server %s, %s", j.SourceServerUUID, why)
		}
		pending = pending || j.Pending
		if j.Status == tracking.JobRunning {
			if live(j) {
				recent = true
			} else {
				stopped = true
				if !j.Pending {
					idle = append(idle, j)
				}
			}
		}
	}
	if pending {
		b.WriteString("\nRerun each job with unfinished roots so that its recovery finishes them, then rerun this one. " +
			"A job that cannot be rerun (its source or config is gone): docs/README_JOBS_SCHEMA.md, \"Abandoning logged roots\".")
	}
	if stopped {
		b.WriteString("\nA job marked running whose heartbeat stopped: confirm its process is gone first " +
			"(docs/README_JOBS_SCHEMA.md, \"Clearing a stale running job\").")
		// The row is named by id: a job name may hold ' or \, which a quoted literal would garble.
		for _, j := range idle {
			fmt.Fprintf(&b, " %s has no unfinished roots: once its process is gone, mark it idle with "+
				"UPDATE %s SET job_status = 0 WHERE id = %d.", j.Name, e.Registry, j.ID)
		}
	}
	if recent {
		b.WriteString("\nA job marked running with a recent heartbeat is a live process on another server that holds this data, " +
			"or a run on this source that lost its locks moments ago and stops at its next check: stop it or wait for it to end, then rerun this job.")
	}
	if unreadable {
		b.WriteString("\nA job whose config_fingerprint cannot be read: restore it from a known-good record, or resolve its unfinished " +
			"work by hand (docs/README_JOBS_SCHEMA.md, \"Abandoning logged roots\").")
	}
	return b.String()
}

// blockerState is a blocker's status, heartbeat and log, for the refusal.
func blockerState(j tracking.Blocker) string {
	var st string
	switch {
	case j.Status == tracking.JobRunning && !j.Beat:
		st = "marked running, never beat"
	case j.Status == tracking.JobRunning:
		st = fmt.Sprintf("marked running, heartbeat %s ago", j.HeartbeatAge)
	case j.Status == tracking.JobFailed:
		st = "failed"
	default:
		st = "idle"
	}
	switch {
	case j.Pending:
		return st + ", unfinished roots in its log"
	case !j.Registered:
		return st + ", unregistered, no log yet"
	}
	return st + ", no unfinished roots in its log"
}

// live reports whether a blocker is a run in progress: marked running, with a heartbeat at most
// staleAfter old (EXC-1). Any other blocker waits for its job's rerun, by its schedule or by an
// operator.
func live(j tracking.Blocker) bool {
	return j.Status == tracking.JobRunning && j.Beat && j.HeartbeatAge <= staleAfter
}

// Live reports whether every blocker is live, so that the refusal ends without anyone acting:
// the run exits 4 (not started) and is retried; otherwise it exits 1 (EXC-1). A refusal with no
// blockers is not live.
func (e *OverlapError) Live() bool {
	for _, j := range e.Blockers {
		if !live(j) {
			return false
		}
	}
	return len(e.Blockers) > 0
}

// locks are a run's two lock sets and the identities of the table locks, for refusals.
type locks struct {
	job, tables *lock.Set
	table       map[string][2]string // lock name → {database, table}
	stopWatch   func()
}

// lockJob takes the job lock, then the source table locks, starting each keep-alive as soon
// as its set is held, then runs the unfinished-work guard (source-table-locks spec §5.1;
// unfinished-work guard spec §6). A lost set calls fail with the reason. On an error nothing
// is held.
func lockJob(ctx context.Context, fail context.CancelCauseFunc, cfg Config, store *tracking.Store, source tracking.Binding) (_ *locks, err error) {
	sourceDatabase := source.SourceDatabase
	l := &locks{table: map[string][2]string{}}
	watch, stopWatch := context.WithCancel(ctx)
	var wg sync.WaitGroup
	l.stopWatch = func() { stopWatch(); wg.Wait() }
	defer func() {
		if err != nil {
			l.release(cfg.Logger)
		}
	}()
	keep := func(s *lock.Set, what string) {
		lost := s.StartKeepAlive(watch, keepAliveEvery)
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-watch.Done():
			case e := <-lost:
				cfg.Logger.Errorw("advisory lock ownership lost; aborting the run", "locks", what, "error", e)
				fail(fmt.Errorf("engine: the %s was lost: %w", what, e))
			}
		}()
	}

	jobName := lock.JobLockName(cfg.JobSchema, cfg.Name)
	job := lock.NewSet(cfg.Destination, jobName)
	if err := job.Acquire(ctx); err != nil {
		var held *lock.HeldError
		if errors.As(err, &held) {
			return nil, &JobRunningError{Job: cfg.Name, Holder: held.Holder}
		}
		return nil, fmt.Errorf("engine: job lock: %w", err)
	}
	l.job = job
	keep(job, "job lock")
	cfg.Logger.Debugw("job lock held", "lock", jobName, "job", cfg.JobSchema+"."+cfg.Name)

	var names, tree []string
	for _, n := range jobspec.NewTree(cfg.Spec).CopyOrder() {
		name := lock.TableLockName(sourceDatabase, n.Table)
		l.table[name] = [2]string{sourceDatabase, n.Table}
		names = append(names, name)
		tree = append(tree, n.Table)
	}
	tables := lock.NewSet(cfg.Source, names...)
	if err := tables.Acquire(ctx); err != nil {
		var held *lock.HeldError
		if errors.As(err, &held) {
			id := l.table[held.Name]
			return nil, &TableLockedError{Database: id[0], Table: id[1], HolderConnectionID: held.Holder}
		}
		return nil, fmt.Errorf("engine: source table locks: %w", err)
	}
	l.tables = tables
	keep(tables, "source table locks")
	for _, name := range tables.Names() {
		id := l.table[name]
		cfg.Logger.Debugw("source table lock held", "lock", name, "table", id[0]+"."+id[1])
	}

	// The unfinished-work guard, under every table lock (spec §6).
	facts, err := readSourceData(ctx, cfg.Source)
	if err != nil {
		return nil, fmt.Errorf("engine: cannot read the source's gtid_mode, gtid_executed and Group Replication state, which decide "+
			"whose unfinished work this run must respect: %w. This run did nothing", err)
	}
	same := func(stored string) bool {
		ok, _ := sameData(source.SourceServerUUID, facts, stored)
		return ok
	}
	blockers, err := store.Blockers(ctx, sourceDatabase, same, tree, cfg.Name)
	if err != nil {
		return nil, err
	}
	if len(blockers) > 0 {
		why := map[string]string{}
		for _, b := range blockers {
			_, why[b.Name] = sameData(source.SourceServerUUID, facts, b.SourceServerUUID)
		}
		return nil, &OverlapError{Job: cfg.Name, Database: sourceDatabase,
			Registry: sqlutil.QuoteIdentifier(cfg.JobSchema) + "." + sqlutil.QuoteIdentifier("archiver_job"), Blockers: blockers, Why: why}
	}
	return l, nil
}

// check is the guarded commit's ownership check (spec §4 step 4): both sets, each on its
// own session. A run without locks (Explain) never commits; a commit without them fails. A
// cancelled ctx (a second signal, or a loss the keep-alive already reported as the run's
// cause) is returned as it is: the check did not find the locks lost.
func (l *locks) check(ctx context.Context) error {
	if l == nil {
		return errors.New("engine: a commit without the run's locks")
	}
	if err := l.tables.Check(ctx); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("engine: the source table locks were lost: %w", err)
	}
	if err := l.job.Check(ctx); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("engine: the job lock was lost: %w", err)
	}
	return nil
}

// release stops the keep-alives and releases both sets on a cleanup context, the table
// locks first.
func (l *locks) release(log *logger.Logger) {
	if l.stopWatch != nil {
		l.stopWatch()
	}
	for _, s := range []*lock.Set{l.tables, l.job} {
		if s == nil {
			continue
		}
		ctx, cancel := cleanupContext(context.Background())
		if err := s.Release(ctx); err != nil {
			log.Warnw("advisory lock release failed", "error", err)
		}
		cancel()
	}
}

// cleanupTimeout bounds every cleanup statement, so a wedged server cannot hang the exit
// (2.x issue #79).
const cleanupTimeout = 5 * time.Second

// cleanupContext is the context for cleanup after work on parent: a lock release, the final
// job status, the FOREIGN_KEY_CHECKS reset. The run's cancellation, which is often why
// cleanup is needed, cannot end it, and cleanupTimeout bounds it.
func cleanupContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), cleanupTimeout)
}

// heartbeat beats the job's heartbeat until the returned stop is called. heartbeatFailures
// failed beats in a row cancel the run with the reason.
func heartbeat(ctx context.Context, fail context.CancelCauseFunc, job *tracking.Job, log *logger.Logger) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(heartbeatEvery)
		defer t.Stop()
		failures := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := job.Heartbeat(ctx); err != nil {
					failures++
					log.Warnw("heartbeat failed", "error", err)
					if failures >= heartbeatFailures {
						fail(fmt.Errorf("engine: the heartbeat failed %d times in a row: %w", failures, err))
						return
					}
				} else {
					failures = 0
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// finalStatus writes the job's status at the end of a run that marked it running, on a
// cleanup context: every way out of such a run writes it (M5-F1), unless another run of the
// job now holds its lock (F1 I1). The write runs only on a session that holds the job lock
// while it runs (Codex P1): the run's own job-lock session, after its ownership check; or, when
// that session has lost the lock, a fresh session that takes the lock again first. When a
// taker holds it, the row is the taker's: nothing is written, and the taker's connection id is
// returned for the run's error. A lock that cannot be taken for any other reason leaves the
// status unwritten too, since an unproven write could overwrite a taker's.
func finalStatus(ctx context.Context, cfg Config, job *tracking.Job, st tracking.JobStatus, jobLock *lock.Set) (taker int64) {
	statusCtx, done := cleanupContext(ctx)
	defer done()
	log := cfg.Logger
	write := func(conn *sql.Conn) error { return job.SetStatusOn(statusCtx, conn, st) }
	err := jobLock.Do(statusCtx, write)
	if errors.Is(err, lock.ErrLost) {
		if cfg.at != nil {
			cfg.at(atFinalWrite)
		}
		again := lock.NewSet(cfg.Destination, jobLock.Names()...)
		err = again.Acquire(statusCtx)
		var held *lock.HeldError
		if errors.As(err, &held) {
			log.Errorw("the job is now run by another session, which owns its status; the final status is not written",
				"job", cfg.Name, "destination_connection", held.Holder)
			return held.Holder
		}
		if err == nil {
			err = again.Do(statusCtx, write)
			if rerr := again.Release(statusCtx); rerr != nil {
				log.Warnw("advisory lock release failed", "error", rerr)
			}
		}
	}
	if err != nil {
		log.Errorw("failed to write the final job status", "job", cfg.Name, "status", st, "error", err)
	}
	return 0
}

// withCause adds the reason the run's context was cancelled, when there is one, to err.
func withCause(ctx context.Context, err error) error {
	if cause := context.Cause(ctx); err != nil && cause != nil && !errors.Is(cause, context.Canceled) {
		return fmt.Errorf("%w (%v)", cause, err)
	}
	return err
}
