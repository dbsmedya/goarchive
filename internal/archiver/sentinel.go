// Package archiver: operator pause switch via a sentinel file.
package archiver

import (
	"context"
	"os"
	"time"

	"github.com/dbsmedya/goarchive/internal/logger"
)

// sentinelPollInterval is how often the sentinel pause file is re-checked.
const sentinelPollInterval = time.Second

// sentinelStillPausedEvery controls how often (in poll iterations) a "still
// paused" reminder is logged while waiting, to avoid log spam at the 1s cadence.
const sentinelStillPausedEvery = 30

// sentinelGate implements an operator pause switch. Before each batch, if the
// configured sentinel file is present, processing blocks and re-checks every
// sentinelPollInterval until the file is removed. Presence is the signal: create
// the file to pause, delete it to resume. The wait is context-interruptible, so
// shutdown/cancellation aborts the pause immediately.
type sentinelGate struct {
	path   string
	logger *logger.Logger

	// Test seams (nil in production): presentFn overrides the filesystem check,
	// sleepFn overrides the interruptible poll sleep.
	presentFn func(path string) bool
	sleepFn   func(ctx context.Context, d time.Duration) error
}

// newSentinelGate builds a gate for the given path. An empty path disables the
// gate (wait returns immediately).
func newSentinelGate(path string, log *logger.Logger) *sentinelGate {
	if log == nil {
		log = logger.NewDefault()
	}
	return &sentinelGate{path: path, logger: log}
}

// present reports whether the sentinel file currently exists. Any stat error
// (including not-exist) is treated as "absent" so a transient stat failure never
// pauses the run indefinitely.
func (g *sentinelGate) present() bool {
	if g.presentFn != nil {
		return g.presentFn(g.path)
	}
	_, err := os.Stat(g.path)
	return err == nil
}

// sleep waits for d or until ctx is cancelled, returning ctx.Err() on cancel.
func (g *sentinelGate) sleep(ctx context.Context, d time.Duration) error {
	if g.sleepFn != nil {
		return g.sleepFn(ctx, d)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// wait blocks while the sentinel file is present, re-checking every
// sentinelPollInterval, until it is absent or ctx is cancelled. It returns nil
// immediately when no sentinel is configured or the file is absent. On ctx
// cancellation it returns the context error; the current batch is left
// unprocessed and remains recoverable on the next run.
func (g *sentinelGate) wait(ctx context.Context) error {
	if g == nil || g.path == "" {
		return nil
	}
	if !g.present() {
		return nil
	}

	g.logger.Warnf("Paused: sentinel file %q is present; remove it to resume", g.path)
	waited := 0
	for {
		if err := g.sleep(ctx, sentinelPollInterval); err != nil {
			g.logger.Warnf("Sentinel pause interrupted: %v", err)
			return err
		}
		if !g.present() {
			g.logger.Infof("Resumed: sentinel file %q removed", g.path)
			return nil
		}
		waited++
		if waited%sentinelStillPausedEvery == 0 {
			g.logger.Warnf("Still paused: sentinel file %q present after ~%ds", g.path, waited)
		}
	}
}
