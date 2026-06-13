package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/dbsmedya/goarchive/internal/lock"
	"github.com/dbsmedya/goarchive/internal/logger"
)

const heartbeatStaleThreshold = 60 * time.Second
const advisoryKeepAliveInterval = 30 * time.Second
const heartbeatFailureLimit = 3

// finalJobStatus picks the status to persist on orchestrator exit.
// Non-nil execErr → Failed (visible in archiver_job for post-mortem).
// Nil execErr → Idle (clean completion).
func finalJobStatus(execErr error) JobStatus {
	if execErr != nil {
		return JobStatusFailed
	}
	return JobStatusIdle
}

type jobStartup struct {
	resumeMgr      *ResumeManager
	jobState       *JobState
	staleAtStartup bool
	runCtx         context.Context
	cancelRun      context.CancelFunc
	failMu         sync.Mutex
	failErr        error
	// cleanup releases startup-acquired resources and writes the final job status.
	// Pass the orchestrator's final error (or nil) — non-nil → JobStatusFailed,
	// nil → JobStatusIdle. Use it via:
	//   defer func() { startup.cleanup(err) }()
	// where `err` is a named return value of the calling function.
	cleanup func(execErr error)
}

func (s *jobStartup) fail(err error) {
	if err == nil {
		return
	}
	s.failMu.Lock()
	if s.failErr == nil {
		s.failErr = err
		s.cancelRun()
	}
	s.failMu.Unlock()
}

func (s *jobStartup) failureErr() error {
	s.failMu.Lock()
	defer s.failMu.Unlock()
	return s.failErr
}

func beginJobStartup(
	ctx context.Context,
	destDB *sql.DB,
	log *logger.Logger,
	jobName string,
	rootTable string,
	jobType string,
	commandName string,
	force bool,
	jobSchema string,
) (*jobStartup, error) {
	runCtx, cancelRun := context.WithCancel(ctx)
	startup := &jobStartup{
		runCtx:    runCtx,
		cancelRun: cancelRun,
	}

	resumeMgr, err := NewResumeManager(destDB, log, jobSchema)
	if err != nil {
		cancelRun()
		return nil, fmt.Errorf("failed to create resume manager: %w", err)
	}
	if err := resumeMgr.InitializeTables(ctx); err != nil {
		cancelRun()
		return nil, fmt.Errorf("failed to initialize resume tables: %w", err)
	}

	rootLock := lock.NewRootTableLock(destDB, rootTable)
	rootHeld, err := rootLock.AcquireLock(ctx, lock.TimeoutMedium)
	if err != nil {
		cancelRun()
		return nil, fmt.Errorf("failed to acquire root-table lock: %w", err)
	}
	if !rootHeld {
		cancelRun()
		return nil, fmt.Errorf("timed out acquiring root-table lock for %q (another startup in progress)", rootTable)
	}
	rootLockHeld := true
	defer func() {
		if rootLockHeld {
			_, _ = rootLock.ReleaseLock(context.Background())
		}
	}()

	staleAtStartup, _, err := resumeMgr.IsHeartbeatStale(ctx, jobName, heartbeatStaleThreshold)
	if err != nil {
		cancelRun()
		return nil, fmt.Errorf("failed to determine heartbeat staleness for job %q: %w", jobName, err)
	}

	if err := CheckSameRootConcurrency(ctx, destDB, jobSchema, rootTable, jobName, commandName); err != nil {
		cancelRun()
		return nil, err
	}

	jobLock := lock.NewJobLock(destDB, jobName)
	acquiredJob, err := jobLock.TryAcquire(ctx)
	if err != nil {
		cancelRun()
		return nil, fmt.Errorf("job-name lock errored: %w", err)
	}
	jobLockHeld := false
	// Destructive commands (archive, purge) delete from the source. They MUST
	// hold the advisory lock for the whole run — otherwise a second process that
	// acquires the freed lock could delete the same rows concurrently. A held
	// GET_LOCK cannot be safely stolen, so --force may NOT bypass it for these
	// commands; it only bypasses a stale heartbeat for non-destructive copy-only
	// (review P1-4).
	requireLock := jobType == JobTypeArchive || jobType == JobTypePurge
	if !acquiredJob {
		if !force {
			cancelRun()
			return nil, fmt.Errorf("job %q is already running (lock held). Use --force only after verifying the holder is dead", jobName)
		}
		if !staleAtStartup {
			cancelRun()
			return nil, fmt.Errorf("job %q lock is held by a live instance (heartbeat fresh). --force cannot bypass a live lock", jobName)
		}
		if requireLock {
			cancelRun()
			return nil, fmt.Errorf("job %q advisory lock is held by another connection and could not be acquired; refusing to run destructive %q without the lock. A stale heartbeat does not release GET_LOCK — verify the previous process is dead AND its MySQL session has closed (the lock auto-releases on session close), then retry", jobName, commandName)
		}
		log.Warn(forceLockBypassBanner)
		log.Warnw("--force proceeding past stale lock (authorized bypass; non-destructive command)", "job", jobName)
	} else {
		jobLockHeld = true
	}

	jobState, err := resumeMgr.GetOrCreateJobWithType(ctx, jobName, rootTable, jobType)
	if err != nil {
		if jobLockHeld {
			_, _ = jobLock.ReleaseLock(context.Background())
		}
		cancelRun()
		return nil, fmt.Errorf("failed to get/create job: %w", err)
	}
	if err := resumeMgr.UpdateJobStatus(ctx, jobName, JobStatusRunning); err != nil {
		if jobLockHeld {
			_, _ = jobLock.ReleaseLock(context.Background())
		}
		cancelRun()
		return nil, fmt.Errorf("failed to mark job running: %w", err)
	}
	if err := resumeMgr.Heartbeat(ctx, jobName); err != nil {
		if jobLockHeld {
			_, _ = jobLock.ReleaseLock(context.Background())
		}
		cancelRun()
		return nil, fmt.Errorf("failed to seed heartbeat: %w", err)
	}

	if _, releaseErr := rootLock.ReleaseLock(ctx); releaseErr != nil {
		log.Warnw("root-table lock release error (proceeding)", "error", releaseErr)
	}
	rootLockHeld = false

	if jobLockHeld {
		lockLost := jobLock.StartKeepAlive(runCtx, advisoryKeepAliveInterval)
		go func() {
			select {
			case <-runCtx.Done():
			case err := <-lockLost:
				if err != nil {
					log.Errorw("advisory lock ownership lost; aborting job", "error", err)
					startup.fail(fmt.Errorf("advisory lock ownership lost: %w", err))
				}
			}
		}()
	}

	heartbeatCtx, stopHeartbeat := context.WithCancel(runCtx)
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		consecutiveFailures := 0
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-t.C:
				if err := resumeMgr.Heartbeat(context.Background(), jobName); err != nil {
					consecutiveFailures++
					log.Warnw("heartbeat failed", "error", err)
					if consecutiveFailures >= heartbeatFailureLimit {
						startup.fail(fmt.Errorf("heartbeat failed %d consecutive times: %w", heartbeatFailureLimit, err))
						return
					}
				} else {
					consecutiveFailures = 0
				}
			}
		}
	}()

	cleanup := func(execErr error) {
		cancelRun()
		stopHeartbeat()
		jobLock.StopKeepAlive()
		resetCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		statusErr := execErr
		if failErr := startup.failureErr(); failErr != nil {
			statusErr = failErr
		}
		finalStatus := finalJobStatus(statusErr)

		if err := resumeMgr.UpdateJobStatus(resetCtx, jobName, finalStatus); err != nil {
			log.Errorw("failed to write final job status",
				"job", jobName,
				"target_status", finalStatus,
				"error", err)
		} else if finalStatus == JobStatusFailed {
			log.Warnw("job marked failed", "job", jobName, "exec_error", statusErr.Error())
		}

		if jobLockHeld {
			_, _ = jobLock.ReleaseLock(context.Background())
		}
	}

	startup.resumeMgr = resumeMgr
	startup.jobState = jobState
	startup.staleAtStartup = staleAtStartup
	startup.cleanup = cleanup
	return startup, nil
}
