package engine

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dbsmedya/dbsgomysql/pkg/sqlutil"

	"github.com/dbsmedya/goarchive/v3/internal/lock"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
)

// The advisory locks and the heartbeat are kept from 2.x (spec §9). Both locks live on the
// destination server, beside the tracking tables.
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

// lockJob takes the job's advisory lock. The root table's lock serializes startups on one
// root table, so the same-root guard and the job lock are decided together.
func lockJob(ctx context.Context, cfg Config, store *tracking.Store) (*lock.AdvisoryLock, error) {
	root := cfg.Spec.RootTable
	rootLock := lock.NewRootTableLock(cfg.Destination, root)
	held, err := rootLock.AcquireLock(ctx, lock.TimeoutMedium)
	if err != nil {
		return nil, fmt.Errorf("engine: root-table lock: %w", err)
	}
	if !held {
		return nil, fmt.Errorf("engine: timed out waiting for the root-table lock of %q: another job is starting on it", root)
	}
	defer releaseLock(rootLock, cfg.Logger)

	running, err := store.Running(ctx, root, cfg.Name)
	if err != nil {
		return nil, err
	}
	if len(running) > 0 {
		return nil, &ConcurrentError{RootTable: root, JobTable: sqlutil.QuoteIdentifier(cfg.JobSchema) + ".archiver_job", Jobs: running}
	}
	jobLock := lock.NewJobLock(cfg.Destination, cfg.Name)
	held, err = jobLock.TryAcquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("engine: job lock: %w", err)
	}
	if !held {
		return nil, fmt.Errorf("engine: job %q is already running: its advisory lock is held by another session", cfg.Name)
	}
	return jobLock, nil
}

// releaseLock releases l on a context cancellation cannot skip, bounded so a wedged server
// cannot hang the exit (2.x issue #79).
func releaseLock(l *lock.AdvisoryLock, log *logger.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := l.ReleaseLock(ctx); err != nil {
		log.Warnw("advisory lock release failed", "error", err)
	}
}

// keepAlive beats the job's heartbeat and checks the job lock's ownership until the
// returned stop is called. Losing the lock, or heartbeatFailures failed beats in a row,
// cancels the run with the reason.
func keepAlive(ctx context.Context, fail context.CancelCauseFunc, jobLock *lock.AdvisoryLock, job *tracking.Job, log *logger.Logger) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	lost := jobLock.StartKeepAlive(ctx, keepAliveEvery)
	wg.Add(2)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
		case err := <-lost:
			if err != nil {
				log.Errorw("advisory lock ownership lost; aborting the run", "error", err)
				fail(fmt.Errorf("engine: the job's advisory lock was lost: %w", err))
			}
		}
	}()
	go func() {
		defer wg.Done()
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
		jobLock.StopKeepAlive()
		wg.Wait()
	}
}
