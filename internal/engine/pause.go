package engine

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/logger"
)

// The run loop stops cooperatively. The caller's stop channel (internal/database's
// SetupGracefulShutdown) closes on the first signal, and the loop checks it at batch
// boundaries only, so the batch in flight always reaches a terminal state. The context is
// cancelled on the second signal, which aborts the batch: its destination transaction or
// its delete chunk rolls back, and the next run's recovery finishes what was logged.

// stopRequested reports whether stop is closed. A nil channel never stops.
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

// interruptibleSleep waits for d. It returns ctx.Err() when ctx is cancelled, and nil when
// d elapses or stop closes, so the caller's boundary check ends the loop cleanly.
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

// seconds converts a configured number of seconds.
func seconds(s float64) time.Duration { return time.Duration(s * float64(time.Second)) }

// sentinelPollInterval is how often the sentinel file is checked while paused.
const sentinelPollInterval = time.Second

// sentinelStillPausedEvery is how many polls pass between "still paused" reminders.
const sentinelStillPausedEvery = 30

// sentinelGate is the operator's pause switch (processing.sentinel_file): while the file
// exists, the run waits before its next batch or recovery page. Create the file to pause,
// delete it to resume.
type sentinelGate struct {
	path   string
	logger *logger.Logger

	// statErrWarned logs a stat error other than not-exist once, so a misconfigured path
	// does not silently disable the pause.
	statErrWarned bool

	// progress shows the pause on the --progress line; nil when progress is off.
	progress *progressTracker

	// Test seams, nil in production.
	presentFn func(path string) bool
	sleepFn   func(ctx context.Context, d time.Duration) error
}

// newSentinelGate returns a gate for path. An empty path disables it.
func newSentinelGate(path string, log *logger.Logger) *sentinelGate {
	return &sentinelGate{path: path, logger: log}
}

// present reports whether the file exists. A stat error counts as absent, so a transient
// failure never pauses the run forever.
func (g *sentinelGate) present() bool {
	if g.presentFn != nil {
		return g.presentFn(g.path)
	}
	_, err := os.Stat(g.path)
	if err == nil {
		return true
	}
	if !os.IsNotExist(err) && !g.statErrWarned {
		g.logger.Warnf("Sentinel file %q could not be checked (treating as absent / not paused): %v", g.path, err)
		g.statErrWarned = true
	}
	return false
}

func (g *sentinelGate) sleep(ctx context.Context, stop <-chan struct{}, d time.Duration) error {
	if g.sleepFn != nil {
		return g.sleepFn(ctx, d)
	}
	return interruptibleSleep(ctx, stop, d)
}

// wait blocks while the file is present. It returns nil when the file is absent or removed,
// or when stop closes; it returns ctx.Err() when ctx is cancelled.
func (g *sentinelGate) wait(ctx context.Context, stop <-chan struct{}) error {
	if g == nil || g.path == "" || !g.present() || stopRequested(stop) {
		return nil
	}
	g.logger.Warnf("Paused: sentinel file %q is present; remove it to resume", g.path)
	g.progress.setPaused(fmt.Sprintf("sentinel file %q present", g.path))
	defer g.progress.clearPaused()
	for waited := 1; ; waited++ {
		if err := g.sleep(ctx, stop, sentinelPollInterval); err != nil {
			g.logger.Warnf("Sentinel pause interrupted: %v", err)
			return err
		}
		if stopRequested(stop) {
			g.logger.Warnf("Sentinel pause ended by graceful stop request")
			return nil
		}
		if !g.present() {
			g.logger.Infow(fmt.Sprintf("Resumed: sentinel file %q removed", g.path), logger.Live)
			return nil
		}
		if waited%sentinelStillPausedEvery == 0 {
			g.logger.Warnf("Still paused: sentinel file %q present after ~%ds", g.path, waited)
		}
	}
}
