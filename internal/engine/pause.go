package engine

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// The run loop stops cooperatively. The caller's stop channel (internal/database's
// SetupGracefulShutdown) closes on the first signal, and the loop checks it at batch
// boundaries only, so the batch in flight always reaches a terminal state. The context is
// cancelled on the second signal, which aborts the batch: its destination transaction or
// its delete chunk rolls back, and the next run's recovery finishes what was logged. A
// replication hold is the exception: a first signal ends it at once, and the next run's
// recovery finishes what the held batch logged (LAG-1).

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

// seconds converts a configured number of seconds by the conversion the validators judge
// by (config.SleepDuration), so what they accept converts in range.
func seconds(s float64) time.Duration { return config.SleepDuration(s) }

// followSleep sleeps the sleep that pick reads from the store, and follows a change of it at
// once (spec §4.5, D6): the value is read when the sleep starts and again on every change of the
// store, and the sleep ends when the time already slept reaches it, so a shorter value ends it
// and a longer one extends it. It waits on the loaded snapshot's signal beside its timer, so a
// change made between the load and the wait is not missed. It returns ctx.Err() when ctx is
// cancelled, and nil when the sleep is over or stop closes, as pollSleep does; a nil stop never
// ends it, which is how the delete sleep runs (spec fact 8).
func followSleep(ctx context.Context, stop <-chan struct{}, store *tuning.Store, pick func(tuning.Settings) float64) error {
	began := time.Now()
	for {
		snap := store.Load()
		left := seconds(pick(snap.Settings)) - time.Since(began)
		if left <= 0 {
			return nil
		}
		timer := time.NewTimer(left)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-stop:
			timer.Stop()
			return nil
		case <-snap.Changed():
			timer.Stop()
		case <-timer.C:
		}
	}
}

// pollSleep waits for d. It returns ctx.Err() when ctx is cancelled, and nil when d elapses, stop
// closes, or changed closes (changed may be nil, and then never fires), so the caller's boundary
// check ends the loop cleanly. It is the pause gate's poll of the sentinel file, which the
// socket's pause and resume cut short by closing the snapshot's signal.
func pollSleep(ctx context.Context, stop <-chan struct{}, d time.Duration, changed <-chan struct{}) error {
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
	case <-changed:
		return nil
	case <-timer.C:
		return nil
	}
}

// sentinelPollInterval is how often the sentinel file is checked while paused.
const sentinelPollInterval = time.Second

// sentinelStillPausedEvery is how many polls pass between "still paused" reminders.
const sentinelStillPausedEvery = 30

// sentinelGate is the run's pause gate (spec §4.7, D9), named for its first input. Two inputs
// hold the run before its next batch or recovery page: the sentinel file
// (processing.sentinel_file) exists, or the store's socket pause is set. Create the file or send
// `pause` to pause; remove the file and send `resume` to resume. The gate waits on a one-second
// poll of the file and on the loaded snapshot's signal, so the socket's `resume` ends the wait at
// once. `resume` under a present file clears the socket pause and the run stays paused, and the
// log says so.
type sentinelGate struct {
	path   string
	logger *logger.Logger
	// tune is the settings store whose socket pause is the gate's second input; nil without one.
	tune *tuning.Store

	// statErrWarned logs a stat error other than not-exist once, so a misconfigured path
	// does not silently disable the pause.
	statErrWarned bool

	// progress shows the pause on the --progress line; nil when progress is off.
	progress *progressTracker

	// Test seams, nil in production.
	presentFn func(path string) bool
	sleepFn   func(ctx context.Context, d time.Duration) error
}

// newSentinelGate returns a gate for path. An empty path disables the sentinel input.
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

// load returns the store's current snapshot, nil without a store.
func (g *sentinelGate) load() *tuning.Snapshot {
	if g.tune == nil {
		return nil
	}
	return g.tune.Load()
}

// pauseState is which inputs hold the run.
type pauseState struct{ socket, sentinel bool }

func (p pauseState) held() bool { return p.socket || p.sentinel }

// state reads both inputs: the snapshot's pause flag, and the file (checked once, and only when
// a path is configured).
func (g *sentinelGate) state(snap *tuning.Snapshot) pauseState {
	var st pauseState
	if snap != nil {
		st.socket = snap.Paused
	}
	if g.path != "" {
		st.sentinel = g.present()
	}
	return st
}

// describe names the inputs that hold, for the log.
func (g *sentinelGate) describe(st pauseState) string {
	switch {
	case st.socket && st.sentinel:
		return fmt.Sprintf("the control socket pause is set and sentinel file %q is present", g.path)
	case st.socket:
		return "the control socket pause is set"
	}
	return fmt.Sprintf("sentinel file %q is present", g.path)
}

// reason is the pause's reason on the --progress line; a configured path is folded onto one line
// by the tracker.
func (g *sentinelGate) reason(st pauseState) string {
	switch {
	case st.socket && st.sentinel:
		return fmt.Sprintf("control socket, sentinel file %q present", g.path)
	case st.socket:
		return "control socket"
	}
	return fmt.Sprintf("sentinel file %q present", g.path)
}

func (g *sentinelGate) pausedLine(st pauseState) string {
	switch {
	case st.socket && st.sentinel:
		return fmt.Sprintf("Paused: control socket pause is set and sentinel file %q is present; send resume and remove the file to resume", g.path)
	case st.socket:
		return "Paused: control socket pause is set; send resume to resume"
	}
	return fmt.Sprintf("Paused: sentinel file %q is present; remove it to resume", g.path)
}

// resumedLine says what ended the pause, from the inputs that held it.
func (g *sentinelGate) resumedLine(was pauseState) string {
	switch {
	case was.socket && was.sentinel:
		return fmt.Sprintf("Resumed: control socket pause cleared and sentinel file %q removed", g.path)
	case was.socket:
		return "Resumed: control socket pause cleared"
	}
	return fmt.Sprintf("Resumed: sentinel file %q removed", g.path)
}

// continuesLine says that the pause goes on after one input changed, and which still holds. A
// `resume` while the file is present lands here: the run is not resumed silently against it.
func (g *sentinelGate) continuesLine(was, now pauseState) string {
	switch {
	case was.socket && !now.socket:
		return fmt.Sprintf("Pause continues: control socket pause cleared, but sentinel file %q is still present; remove it to resume", g.path)
	case was.sentinel && !now.sentinel:
		return fmt.Sprintf("Pause continues: sentinel file %q removed, but the control socket pause is still set; send resume to resume", g.path)
	case !was.socket && now.socket:
		return "Pause continues: the control socket pause is now set as well"
	}
	return fmt.Sprintf("Pause continues: sentinel file %q is now present as well", g.path)
}

func (g *sentinelGate) sleep(ctx context.Context, stop <-chan struct{}, d time.Duration, snap *tuning.Snapshot) error {
	if g.sleepFn != nil {
		return g.sleepFn(ctx, d)
	}
	var changed <-chan struct{}
	if snap != nil {
		changed = snap.Changed()
	}
	return pollSleep(ctx, stop, d, changed)
}

// wait blocks while either input holds. It returns nil when neither does, or when stop closes;
// it returns ctx.Err() when ctx is cancelled. Each pass loads a snapshot, reads both inputs by
// it, and waits on that snapshot's signal, so a change of the socket pause is never missed.
func (g *sentinelGate) wait(ctx context.Context, stop <-chan struct{}) error {
	if g == nil {
		return nil
	}
	snap := g.load()
	st := g.state(snap)
	if !st.held() || stopRequested(stop) {
		return nil
	}
	g.logger.Warnw(g.pausedLine(st), logger.Live)
	g.progress.setPaused(g.reason(st))
	defer g.progress.clearPaused()
	for waited := 1; ; waited++ {
		if err := g.sleep(ctx, stop, sentinelPollInterval, snap); err != nil {
			g.logger.Warnf("Pause interrupted: %v", err)
			return err
		}
		if stopRequested(stop) {
			g.logger.Warnf("Pause ended by graceful stop request")
			return nil
		}
		snap = g.load()
		now := g.state(snap)
		if !now.held() {
			g.logger.Infow(g.resumedLine(st), logger.Live)
			return nil
		}
		if now != st {
			g.logger.Warnw(g.continuesLine(st, now), logger.Live)
			g.progress.setPaused(g.reason(now))
			st = now
		}
		if waited%sentinelStillPausedEvery == 0 {
			g.logger.Warnw(fmt.Sprintf("Still paused: %s, after ~%ds", g.describe(now), waited), logger.Live)
		}
	}
}
