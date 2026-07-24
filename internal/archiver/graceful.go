// Package archiver: cooperative graceful-stop helpers shared by the archive,
// copy-only, and purge orchestrators.
//
// The orchestrators run a copy→verify→delete loop that must not be torn apart by
// a single Ctrl-C. An interruption mid-cycle leaves root PKs in a non-terminal
// (pending/copied) status, which the next run's status-aware recovery replays —
// except in count-mode archive jobs and strict-INSERT archive/copy-only jobs,
// where resume refuses outright and demands operator intervention (hand-editing
// log_status, or removing the destination rows already written) before the job
// can run again. See recover's gates in batch_pipeline.go.
//
// The contract: the work context (from database.SetupGracefulShutdown) is only
// canceled on the SECOND signal. The first signal closes a stop channel, which
// the loops observe at BATCH BOUNDARIES via stopRequested. So the in-flight batch
// always runs to a terminal state before the loop exits.
package archiver

import (
	"context"
	"time"
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
