// Package archiver: cooperative graceful-stop helpers shared by the archive,
// copy-only, and purge orchestrators.
//
// The orchestrators run a copy→verify→delete loop that must not be torn apart by
// a single Ctrl-C — an interruption mid-cycle would leave root PKs in a
// non-terminal (pending/copied) status and, under verification.method: count,
// wedge the next run until an operator hand-edits the log table.
//
// The contract: the work context (from database.SetupGracefulShutdown) is only
// canceled on the SECOND signal. The first signal closes a stop channel, which
// the loops observe at BATCH BOUNDARIES via stopRequested. So the in-flight batch
// always runs to a terminal state before the loop exits.
package archiver

import (
	"context"
	"errors"
	"time"

	"github.com/dbsmedya/goarchive/internal/logger"
)

// stopRequested reports whether a cooperative graceful stop has been requested
// (the stop channel is closed). A nil channel never reports stop — that is the
// default for tests and any caller that did not wire a stop channel.
func stopRequested(stop <-chan struct{}) bool {
	if stop == nil {
		return false
	}
	select {
	case <-stop:
		return true
	default:
		return false
	}
}

// isCancellation reports whether err is a context cancellation/deadline error —
// i.e. a clean operator-initiated interruption rather than a real failure. Such
// an error must NOT mark a root PK as failed; the row is left in its current
// non-terminal status so status-aware replay recovers it on the next run.
func isCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// markFailedUnlessCanceled records a per-root failure, EXCEPT when the cause is a
// clean context cancellation/deadline (operator hard-stop). On cancellation it
// leaves the root in its current non-terminal status (pending) so status-aware
// replay recovers it — matching the archive orchestrator, which never MarkFails.
// This is what keeps a Ctrl-C from turning a root into a 'failed' row that an
// operator would have to clean up by hand.
func markFailedUnlessCanceled(ctx context.Context, resumeMgr *ResumeManager, log *logger.Logger, jobName string, rootID interface{}, cause error) {
	if isCancellation(cause) {
		if log != nil {
			log.Warnf("Operation canceled for root pk=%v; leaving it pending for replay (not marking failed)", rootID)
		}
		return
	}
	_ = resumeMgr.MarkFailed(ctx, jobName, rootID, cause.Error())
}

// interruptibleSleep pauses for d between batches. It returns:
//   - ctx.Err() if the work context is canceled (hard stop / second signal),
//   - nil if d elapses OR a cooperative stop is requested (stop closed).
//
// On a cooperative stop it returns nil (not an error) so the caller falls
// through to its boundary stopRequested check and exits cleanly. A nil stop
// channel simply never fires that case.
func interruptibleSleep(ctx context.Context, stop <-chan struct{}, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-stop:
		return nil
	case <-timer.C:
		return nil
	}
}
