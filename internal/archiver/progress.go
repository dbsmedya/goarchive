// Package archiver: --progress tracking and rendering (spec:
// .ayder/superpowers_20260828/specs/2026-08-28-progress-cli-design.md).
package archiver

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// progressTracker holds run-progress counters. The batch loop writes at batch
// boundaries; the reporter goroutine reads snapshots concurrently. A nil
// tracker (feature off) makes every method a no-op so call sites carry no
// nil checks. totalRoots is a startup ESTIMATE: candidate drift (rows aging
// into or out of the job WHERE) may make the loop process more or fewer
// roots — snapshot math clamps instead of trusting it.
type progressTracker struct {
	totalRoots int64
	startedAt  time.Time
	nowFn      func() time.Time // test seam; nil = time.Now

	rootsDone   atomic.Int64
	rowsCopied  atomic.Int64
	rowsDeleted atomic.Int64
	completed   atomic.Bool
	paused      atomic.Pointer[string] // nil = not paused; else sanitized reason
}

func newProgressTracker(totalRoots int64) *progressTracker {
	return &progressTracker{totalRoots: totalRoots, startedAt: time.Now()}
}

// RecordBatch folds one completed batch into the counters.
func (t *progressTracker) RecordBatch(stats BatchStats) {
	if t == nil {
		return
	}
	t.rootsDone.Add(int64(stats.RootsProcessed))
	t.rowsCopied.Add(stats.RecordsCopied)
	t.rowsDeleted.Add(stats.RecordsDeleted)
}

// SeedRows pre-loads row totals recovered by a prior run's replay so displayed
// rows match the final summary. Recovered roots are NOT added to the roots
// axis: the total is counted after recovery and excludes them.
func (t *progressTracker) SeedRows(copied, deleted int64) {
	if t == nil {
		return
	}
	t.rowsCopied.Add(copied)
	t.rowsDeleted.Add(deleted)
}

// MarkComplete records the loop's natural completion (empty fetch). A
// completed snapshot forces remaining=0 / percent=100 / eta=0s, which closes
// the drift-underrun case (rows aged out of the WHERE). Graceful-stop and
// fatal exits never call this.
func (t *progressTracker) MarkComplete() {
	if t == nil {
		return
	}
	t.completed.Store(true)
}

// SetPaused/ClearPaused implement the sentinel gate's pauseNotifier. The
// reason is newline-sanitized so no configured path can forge a second line.
func (t *progressTracker) SetPaused(reason string) {
	if t == nil {
		return
	}
	r := sanitizeChannelName(reason)
	t.paused.Store(&r)
}

func (t *progressTracker) ClearPaused() {
	if t == nil {
		return
	}
	t.paused.Store(nil)
}

func (t *progressTracker) now() time.Time {
	if t.nowFn != nil {
		return t.nowFn()
	}
	return time.Now()
}

// snapshot is nil-safe like every other method (spec contract): a nil
// tracker yields the zero snapshot rather than a panic.
func (t *progressTracker) snapshot() progressSnapshot {
	if t == nil {
		return progressSnapshot{}
	}
	s := progressSnapshot{
		total:     t.totalRoots,
		done:      t.rootsDone.Load(),
		copied:    t.rowsCopied.Load(),
		deleted:   t.rowsDeleted.Load(),
		elapsed:   t.now().Sub(t.startedAt),
		completed: t.completed.Load(),
	}
	if p := t.paused.Load(); p != nil {
		s.pausedReason = *p
	}
	return s
}

// progressSnapshot is one immutable read of the tracker.
type progressSnapshot struct {
	total, done     int64
	copied, deleted int64
	elapsed         time.Duration
	completed       bool
	pausedReason    string // "" = not paused
}

// remaining clamps at zero: drift may push done past the estimated total.
func (s progressSnapshot) remaining() int64 {
	if s.completed || s.done >= s.total {
		return 0
	}
	return s.total - s.done
}

// percent caps at 100 and defines total==0 as 100 (nothing to do), so no
// division by zero is reachable under drift.
func (s progressSnapshot) percent() float64 {
	if s.completed || s.total == 0 || s.done >= s.total {
		return 100
	}
	return float64(s.done) / float64(s.total) * 100
}

// etaString renders ETA with the spec's explicit precedence:
// paused → completed/zero-remaining → calculating → calculated.
func (s progressSnapshot) etaString() string {
	switch {
	case s.pausedReason != "":
		return "paused"
	case s.completed || s.remaining() == 0:
		return "0s"
	case s.done == 0:
		return "calculating"
	}
	rate := float64(s.done) / float64(s.elapsed)
	eta := time.Duration(float64(s.remaining()) / rate)
	return eta.Truncate(time.Second).String()
}

// formatProgressLine renders one plain stdout line. mode selects which row
// fields apply: batchFull shows copied+deleted, batchCopyVerify copied only,
// batchDeleteOnly deleted only.
func formatProgressLine(s progressSnapshot, mode batchMode) string {
	line := fmt.Sprintf("progress: roots %d/~%d (%.1f%%) remaining=%d",
		s.done, s.total, s.percent(), s.remaining())
	if mode == batchFull || mode == batchCopyVerify {
		line += fmt.Sprintf(" copied_rows=%d", s.copied)
	}
	if mode == batchFull || mode == batchDeleteOnly {
		line += fmt.Sprintf(" deleted_rows=%d", s.deleted)
	}
	line += fmt.Sprintf(" elapsed=%s eta=%s",
		s.elapsed.Truncate(time.Second), s.etaString())
	if s.pausedReason != "" {
		line += fmt.Sprintf(" [PAUSED: %s]", s.pausedReason)
	}
	return line
}

// progressReporter prints one progress line per tick and exactly one final
// line at stop. It exists only while --progress is active; failures before
// activation (startup, recovery, COUNT) never construct one, so they print
// no progress output at all.
type progressReporter struct {
	tracker  *progressTracker
	interval time.Duration
	mode     batchMode
	out      io.Writer

	// tickCh is a test seam: when non-nil it replaces the real ticker.
	tickCh <-chan time.Time

	stopCh  chan struct{}
	wg      sync.WaitGroup
	started bool
	once    sync.Once
}

func newProgressReporter(t *progressTracker, interval time.Duration, mode batchMode, out io.Writer) *progressReporter {
	return &progressReporter{
		tracker:  t,
		interval: interval,
		mode:     mode,
		out:      out,
		stopCh:   make(chan struct{}),
	}
}

func (r *progressReporter) start() {
	ticks := r.tickCh
	if ticks == nil {
		ticker := time.NewTicker(r.interval)
		ticks = ticker.C
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			defer ticker.Stop()
			r.loop(ticks)
		}()
	} else {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.loop(ticks)
		}()
	}
	r.started = true
}

func (r *progressReporter) loop(ticks <-chan time.Time) {
	for {
		select {
		case <-r.stopCh:
			return
		case <-ticks:
			_, _ = fmt.Fprintln(r.out, formatProgressLine(r.tracker.snapshot(), r.mode))
		}
	}
}

// stop terminates the ticker goroutine, waits for it, then prints the final
// line — exactly once, only if the reporter was started. The snapshot printed
// is the last completed-batch state: a fatal error mid-batch lands here after
// rows may have been copied but before RecordBatch, so "true totals" are not
// guaranteed and not claimed.
func (r *progressReporter) stop() {
	r.once.Do(func() {
		close(r.stopCh)
		r.wg.Wait()
		if r.started {
			_, _ = fmt.Fprintln(r.out, formatProgressLine(r.tracker.snapshot(), r.mode))
		}
	})
}

// startProgress is the single --progress activation point shared by the three
// orchestrators. It must be called after crash recovery so the count uses the
// fetcher's current checkpoint. The returned stop function is always non-nil.
func startProgress(ctx context.Context, interval time.Duration, fetcher *RootIDFetcher,
	mode batchMode, seedCopied, seedDeleted int64, out io.Writer,
) (*progressTracker, func(), error) {
	noop := func() {}
	if interval <= 0 {
		return nil, noop, nil
	}
	total, err := fetcher.CountRemaining(ctx)
	if err != nil {
		return nil, noop, fmt.Errorf("--progress startup count failed: %w", err)
	}
	tracker := newProgressTracker(total)
	tracker.SeedRows(seedCopied, seedDeleted)
	reporter := newProgressReporter(tracker, interval, mode, out)
	reporter.start()
	return tracker, reporter.stop, nil
}
