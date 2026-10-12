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
	// staleAfter is how old a running job's heartbeat may be before the same-root guard
	// calls it stale rather than live.
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

// ConcurrentError refuses a run while another job is marked running on the same root table.
type ConcurrentError struct {
	RootTable string
	JobTable  string // the qualified archiver_job, for the remedy
	Jobs      []tracking.RunningJob
}

func (e *ConcurrentError) Error() string {
	var live, stale []string
	for _, j := range e.Jobs {
		switch {
		case j.Beat && j.HeartbeatAge <= staleAfter:
			live = append(live, j.Name)
		case j.Beat:
			stale = append(stale, fmt.Sprintf("%s (heartbeat %s ago)", j.Name, j.HeartbeatAge))
		default:
			stale = append(stale, j.Name+" (no heartbeat)")
		}
	}
	if len(live) > 0 {
		return fmt.Sprintf("engine: another job is running on the root table %q: %s", e.RootTable, strings.Join(live, ", "))
	}
	return fmt.Sprintf("engine: the root table %q has jobs marked running whose heartbeat stopped: %s. "+
		"A crashed run leaves its job marked running. When no process runs them, mark them idle with "+
		"UPDATE %s SET job_status = 0 WHERE job_name = '<name>'", e.RootTable, strings.Join(stale, ", "), e.JobTable)
}

// locks are a run's two lock sets and the identities of the table locks, for refusals.
type locks struct {
	job, tables *lock.Set
	table       map[string][2]string // lock name → {database, table}
	stopWatch   func()
}

// lockJob takes the job lock, then the source table locks, starting each keep-alive as soon
// as its set is held, then runs the same-root guard (spec §5.1). A lost set calls fail with
// the reason. On an error nothing is held.
func lockJob(ctx context.Context, fail context.CancelCauseFunc, cfg Config, store *tracking.Store, sourceDatabase string) (_ *locks, err error) {
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
			if held.Holder == 0 {
				return nil, fmt.Errorf("engine: job %q is already running: its advisory lock was held by another session a moment ago", cfg.Name)
			}
			return nil, fmt.Errorf("engine: job %q is already running: its advisory lock is held by another session (destination connection %d)", cfg.Name, held.Holder)
		}
		return nil, fmt.Errorf("engine: job lock: %w", err)
	}
	l.job = job
	keep(job, "job lock")
	cfg.Logger.Debugw("job lock held", "lock", jobName, "job", cfg.JobSchema+"."+cfg.Name)

	var names []string
	for _, n := range jobspec.NewTree(cfg.Spec).CopyOrder() {
		name := lock.TableLockName(sourceDatabase, n.Table)
		l.table[name] = [2]string{sourceDatabase, n.Table}
		names = append(names, name)
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

	running, err := store.Running(ctx, sourceDatabase, cfg.Spec.RootTable, cfg.Name)
	if err != nil {
		return nil, err
	}
	if len(running) > 0 {
		return nil, &ConcurrentError{RootTable: cfg.Spec.RootTable, JobTable: sqlutil.QuoteIdentifier(cfg.JobSchema) + ".archiver_job", Jobs: running}
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
// job now holds its lock (F1 I1). The row is then that run's: nothing is written, and the
// holder's connection id is returned for the run's error. A failed holder lookup writes the
// status, as before.
func finalStatus(ctx context.Context, job *tracking.Job, st tracking.JobStatus, name string, jobLock *lock.Set, dst *sql.DB, log *logger.Logger) (taker int64) {
	statusCtx, done := cleanupContext(ctx)
	defer done()
	holder, err := lock.Holder(statusCtx, dst, jobLock.Names()[0])
	switch {
	case err != nil:
		log.Warnw("could not read the job lock's holder; writing the final status", "job", name, "error", err)
	case holder != 0 && holder != jobLock.ConnectionID():
		log.Errorw("the job is now run by another session, which owns its status; the final status is not written",
			"job", name, "destination_connection", holder)
		return holder
	}
	if err := job.SetStatus(statusCtx, st); err != nil {
		log.Errorw("failed to write the final job status", "job", name, "status", st, "error", err)
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
