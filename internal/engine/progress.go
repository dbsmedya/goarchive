package engine

import (
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
)

// The --progress reporter (R50), kept from 2.x: once recovery is done, the engine reads the
// optimizer's estimate of the where range (PRC-1), then prints one line at each interval and
// one at the end of the run.

// progressTracker holds a run's progress counters. The batch loop writes them at batch
// boundaries and the reporter reads snapshots concurrently. A nil tracker (progress off)
// makes every method a no-op. totalRoots is the optimizer's estimate taken at startup
// (estimateRoots), off in either direction, and rows can age into or out of the where range,
// so the snapshot arithmetic clamps instead of trusting it.
type progressTracker struct {
	totalRoots int64
	startedAt  time.Time        // Run's start: elapsed counts from here (spec §3.5)
	loopAt     time.Time        // the first batch: the ETA's rate counts from here
	nowFn      func() time.Time // test seam; nil means time.Now

	rootsDone   atomic.Int64
	rowsCopied  atomic.Int64
	rowsDeleted atomic.Int64
	completed   atomic.Bool
	paused      atomic.Pointer[string] // nil: not paused
}

// newProgressTracker starts the counters as the first batch begins; began is Run's start.
func newProgressTracker(totalRoots int64, began time.Time) *progressTracker {
	return &progressTracker{totalRoots: totalRoots, startedAt: began, loopAt: time.Now()}
}

// update sets the counters to the run's totals so far.
func (t *progressTracker) update(roots, copied, deleted int64) {
	if t == nil {
		return
	}
	t.rootsDone.Store(roots)
	t.rowsCopied.Store(copied)
	t.rowsDeleted.Store(deleted)
}

// markComplete records that the loop exhausted the where range. A completed snapshot shows
// nothing remaining, whatever the estimate said. A stop or a failure never calls it.
func (t *progressTracker) markComplete() {
	if t == nil {
		return
	}
	t.completed.Store(true)
}

// setPaused and clearPaused follow the sentinel pause. The reason is folded onto one line,
// so a configured path cannot forge a second progress line.
func (t *progressTracker) setPaused(reason string) {
	if t == nil {
		return
	}
	r := strings.NewReplacer("\n", " ", "\r", " ").Replace(reason)
	t.paused.Store(&r)
}

func (t *progressTracker) clearPaused() {
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
		running:   t.now().Sub(t.loopAt),
		completed: t.completed.Load(),
	}
	if s.completed {
		// A finished run's total is what it did: the estimate was only ever a guide (PRC-1).
		s.total = s.done
	}
	if p := t.paused.Load(); p != nil {
		s.pausedReason = *p
	}
	return s
}

// progressSnapshot is one read of the tracker.
type progressSnapshot struct {
	total, done     int64
	copied, deleted int64
	elapsed         time.Duration // since Run began
	running         time.Duration // since the first batch
	completed       bool
	pausedReason    string // empty: not paused
}

// remaining clamps at zero: rows aging into the range can push done past the estimate.
func (s progressSnapshot) remaining() int64 {
	if s.completed || s.done >= s.total {
		return 0
	}
	return s.total - s.done
}

// percent caps at 100, and an estimate of zero is 100: nothing to do.
func (s progressSnapshot) percent() float64 {
	if s.completed || s.total == 0 || s.done >= s.total {
		return 100
	}
	return float64(s.done) / float64(s.total) * 100
}

// etaString: paused, then completed, then calculating, then unknown past the estimate, then the
// estimate.
func (s progressSnapshot) etaString() string {
	switch {
	case s.pausedReason != "":
		return "paused"
	case s.completed:
		return "0s"
	case s.done == 0:
		return "calculating"
	case s.done >= s.total:
		// Past the estimate, the run is not done: no time can be told (PRC-1).
		return "unknown"
	}
	// Roots per running time, so startup and recovery do not slow the estimate.
	rate := float64(s.done) / float64(s.running)
	return time.Duration(float64(s.remaining()) / rate).Truncate(time.Second).String()
}

// formatProgressLine renders one stdout line, its columns at fixed widths so successive lines
// align (spec §3.5): widths are fixed, not measured, because each line is printed on its own.
// Archive shows copied and deleted rows, copy-only copied rows, purge deleted rows.
func formatProgressLine(s progressSnapshot, typ jobspec.Type) string {
	w := len(strconv.FormatInt(s.total, 10))
	line := fmt.Sprintf("progress: roots %*d/~%d (%5.1f%%)  remaining=%-*d", w, s.done, s.total, s.percent(), w, s.remaining())
	if typ != jobspec.Purge {
		line += fmt.Sprintf("  copied_rows=%-9d", s.copied)
	}
	if typ != jobspec.CopyOnly {
		line += fmt.Sprintf("  deleted_rows=%-9d", s.deleted)
	}
	line += fmt.Sprintf("  elapsed=%-7s  eta=%s", s.elapsed.Truncate(time.Second), s.etaString())
	if s.pausedReason != "" {
		line += fmt.Sprintf(" [PAUSED: %s]", s.pausedReason)
	}
	return line
}

// progressFields are a snapshot as one log entry's fields (a JSON console log, spec §3.2 C).
func progressFields(s progressSnapshot, typ jobspec.Type) []any {
	f := []any{"roots_done", s.done, "roots_total", s.total, "percent", math.Round(s.percent()*10) / 10,
		"remaining", s.remaining()}
	if typ != jobspec.Purge {
		f = append(f, "copied_rows", s.copied)
	}
	if typ != jobspec.CopyOnly {
		f = append(f, "deleted_rows", s.deleted)
	}
	f = append(f, "elapsed_seconds", s.elapsed.Seconds(), "eta", s.etaString())
	if s.pausedReason != "" {
		f = append(f, "paused", s.pausedReason)
	}
	return f
}

// progressReporter prints one line per tick and exactly one final line at stop, only if it
// was started. With log set, each line is a progress entry written past the level filter
// instead (a JSON console log).
type progressReporter struct {
	tracker  *progressTracker
	interval time.Duration
	typ      jobspec.Type
	out      io.Writer
	log      *logger.Logger

	tickCh <-chan time.Time // test seam: replaces the ticker when non-nil

	stopCh  chan struct{}
	wg      sync.WaitGroup
	started bool
	once    sync.Once
}

func newProgressReporter(t *progressTracker, interval time.Duration, typ jobspec.Type, out io.Writer) *progressReporter {
	return &progressReporter{tracker: t, interval: interval, typ: typ, out: out, stopCh: make(chan struct{})}
}

func (r *progressReporter) start() {
	ticks := r.tickCh
	var ticker *time.Ticker
	if ticks == nil {
		ticker = time.NewTicker(r.interval)
		ticks = ticker.C
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		if ticker != nil {
			defer ticker.Stop()
		}
		for {
			select {
			case <-r.stopCh:
				return
			case <-ticks:
				r.emit()
			}
		}
	}()
	r.started = true
}

// emit writes one snapshot: a line on out, or an entry on log.
func (r *progressReporter) emit() {
	s := r.tracker.snapshot()
	if r.log != nil {
		r.log.Always().Infow("progress", progressFields(s, r.typ)...)
		return
	}
	_, _ = fmt.Fprintln(r.out, formatProgressLine(s, r.typ))
}

// stop ends the ticks and prints the final line once. After a failure the line shows the
// last finished batch; rows of the batch that failed are not counted.
func (r *progressReporter) stop() {
	r.once.Do(func() {
		close(r.stopCh)
		r.wg.Wait()
		if r.started {
			r.emit()
		}
	})
}
