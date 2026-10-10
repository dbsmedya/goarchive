package engine

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// The --progress reporter (R50), kept from 2.x: once recovery is done, the engine counts the
// where range, then prints one line at each interval and one at the end of the run.

// progressTracker holds a run's progress counters. The batch loop writes them at batch
// boundaries and the reporter reads snapshots concurrently. A nil tracker (progress off)
// makes every method a no-op. totalRoots is an estimate taken at startup: rows can age into
// or out of the where range, so the snapshot arithmetic clamps instead of trusting it.
type progressTracker struct {
	totalRoots int64
	startedAt  time.Time
	nowFn      func() time.Time // test seam; nil means time.Now

	rootsDone   atomic.Int64
	rowsCopied  atomic.Int64
	rowsDeleted atomic.Int64
	completed   atomic.Bool
	paused      atomic.Pointer[string] // nil: not paused
}

func newProgressTracker(totalRoots int64) *progressTracker {
	return &progressTracker{totalRoots: totalRoots, startedAt: time.Now()}
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
		completed: t.completed.Load(),
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
	elapsed         time.Duration
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

// etaString: paused, then nothing remaining, then calculating, then the estimate.
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
	return time.Duration(float64(s.remaining()) / rate).Truncate(time.Second).String()
}

// formatProgressLine renders one stdout line. Archive shows copied and deleted rows,
// copy-only copied rows, purge deleted rows.
func formatProgressLine(s progressSnapshot, typ jobspec.Type) string {
	line := fmt.Sprintf("progress: roots %d/~%d (%.1f%%) remaining=%d", s.done, s.total, s.percent(), s.remaining())
	if typ != jobspec.Purge {
		line += fmt.Sprintf(" copied_rows=%d", s.copied)
	}
	if typ != jobspec.CopyOnly {
		line += fmt.Sprintf(" deleted_rows=%d", s.deleted)
	}
	line += fmt.Sprintf(" elapsed=%s eta=%s", s.elapsed.Truncate(time.Second), s.etaString())
	if s.pausedReason != "" {
		line += fmt.Sprintf(" [PAUSED: %s]", s.pausedReason)
	}
	return line
}

// progressReporter prints one line per tick and exactly one final line at stop, only if it
// was started.
type progressReporter struct {
	tracker  *progressTracker
	interval time.Duration
	typ      jobspec.Type
	out      io.Writer

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
				_, _ = fmt.Fprintln(r.out, formatProgressLine(r.tracker.snapshot(), r.typ))
			}
		}
	}()
	r.started = true
}

// stop ends the ticks and prints the final line once. After a failure the line shows the
// last finished batch; rows of the batch that failed are not counted.
func (r *progressReporter) stop() {
	r.once.Do(func() {
		close(r.stopCh)
		r.wg.Wait()
		if r.started {
			_, _ = fmt.Fprintln(r.out, formatProgressLine(r.tracker.snapshot(), r.typ))
		}
	})
}
