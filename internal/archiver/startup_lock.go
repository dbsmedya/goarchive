package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/dbsmedya/goarchive/internal/lock"
	"github.com/dbsmedya/goarchive/internal/logger"
)

const heartbeatStaleThreshold = 60 * time.Second

type jobStartup struct {
	resumeMgr      *ResumeManager
	jobState       *JobState
	staleAtStartup bool
	cleanup        func()
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
) (*jobStartup, error) {
	resumeMgr, err := NewResumeManager(destDB, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create resume manager: %w", err)
	}
	if err := resumeMgr.InitializeTables(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize resume tables: %w", err)
	}

	rootLock := lock.NewRootTableLock(destDB, rootTable)
	rootHeld, err := rootLock.AcquireLock(ctx, lock.TimeoutMedium)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire root-table lock: %w", err)
	}
	if !rootHeld {
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
		return nil, fmt.Errorf("failed to determine heartbeat staleness for job %q: %w", jobName, err)
	}

	if err := CheckSameRootConcurrency(ctx, destDB, rootTable, jobName, commandName); err != nil {
		return nil, err
	}

	jobLock := lock.NewJobLock(destDB, jobName)
	acquiredJob, err := jobLock.TryAcquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("job-name lock errored: %w", err)
	}
	jobLockHeld := false
	if !acquiredJob {
		if !force {
			return nil, fmt.Errorf("job %q is already running (lock held). Use --force only after verifying the holder is dead", jobName)
		}
		if !staleAtStartup {
			return nil, fmt.Errorf("job %q lock is held by a live instance (heartbeat fresh). --force cannot bypass a live lock", jobName)
		}
		log.Warn(forceLockBypassBanner)
		log.Warnw("--force proceeding past stale lock (authorized bypass)", "job", jobName)
	} else {
		jobLockHeld = true
	}

	jobState, err := resumeMgr.GetOrCreateJobWithType(ctx, jobName, rootTable, jobType)
	if err != nil {
		if jobLockHeld {
			_, _ = jobLock.ReleaseLock(context.Background())
		}
		return nil, fmt.Errorf("failed to get/create job: %w", err)
	}
	if err := resumeMgr.UpdateJobStatus(ctx, jobName, JobStatusRunning); err != nil {
		if jobLockHeld {
			_, _ = jobLock.ReleaseLock(context.Background())
		}
		return nil, fmt.Errorf("failed to mark job running: %w", err)
	}
	if err := resumeMgr.Heartbeat(ctx, jobName); err != nil {
		if jobLockHeld {
			_, _ = jobLock.ReleaseLock(context.Background())
		}
		return nil, fmt.Errorf("failed to seed heartbeat: %w", err)
	}

	if _, releaseErr := rootLock.ReleaseLock(ctx); releaseErr != nil {
		log.Warnw("root-table lock release error (proceeding)", "error", releaseErr)
	}
	rootLockHeld = false

	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-t.C:
				if err := resumeMgr.Heartbeat(context.Background(), jobName); err != nil {
					log.Warnw("heartbeat failed", "error", err)
				}
			}
		}
	}()

	cleanup := func() {
		stopHeartbeat()
		resetCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := resumeMgr.UpdateJobStatus(resetCtx, jobName, JobStatusIdle); err != nil {
			log.Errorw("failed to reset job status to idle", "job", jobName, "error", err)
		}
		if jobLockHeld {
			_, _ = jobLock.ReleaseLock(context.Background())
		}
	}

	return &jobStartup{
		resumeMgr:      resumeMgr,
		jobState:       jobState,
		staleAtStartup: staleAtStartup,
		cleanup:        cleanup,
	}, nil
}
