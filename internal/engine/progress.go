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

// The run's phases, as the status names its state.
const (
	// PhaseStarting is everything before recovery: connecting, preflight, locks, registration.
	PhaseStarting = "starting"
	// PhaseRecovering is the replay of the roots a log still holds.
	PhaseRecovering = "recovering"
	// PhaseRunning is the batch loop, working or sleeping between batches.
	PhaseRunning = "running"
)

// Progress is a run's progress, for every run (TEL-1, spec §4.8): the command builds it before the
// control socket binds and hands it in through Config.Tracker. It holds the phase, the activity, the
// counters of committed work (in total and per table), the pause, and, only with --progress, the
// estimate the reporter and the status show. The engine writes it as it works, at each commit, and
// readers (the --progress reporter, the control server's status) copy it concurrently, without ever
// waiting for the run. A nil Progress makes every method a no-op.
//
// The estimate is the optimizer's, taken after recovery (estimateRoots), off in either direction,
// and rows can age into or out of the where range, so the snapshot arithmetic clamps instead of
// trusting it.
type Progress struct {
	startedAt time.Time        // when the command began the run: elapsed counts from here (spec §3.5)
	nowFn     func() time.Time // test seam; nil means time.Now

	phase atomic.Pointer[string]   // nil: starting
	act   atomic.Pointer[activity] // nil: no step

	batches, roots, rootsSkipped, rootsRecovered atomic.Int64
	rowsCopied, rowsDeleted                      atomic.Int64
	completed                                    atomic.Bool

	mu     sync.Mutex
	tables map[string]*TableCounts
	order  []string

	paused       atomic.Pointer[string]    // nil: not paused
	waitingSince atomic.Pointer[time.Time] // when the loop began waiting in the pause gate
	sentinel     atomic.Bool               // the pause gate saw the sentinel file at its last check

	totalRoots  atomic.Int64
	hasEstimate atomic.Bool
	loopAt      atomic.Pointer[time.Time] // the estimate was taken: the ETA's rate counts from here
}

// activity is the step a run is on and the table it works on, where the step has one.
type activity struct{ step, table string }

// TableCounts are the rows a run has committed on one table.
type TableCounts struct {
	Name    string
	Copied  int64
	Deleted int64
}

// ProgressEstimate is the optimizer's estimate of the roots in range, which only a --progress run reads,
// with the percentage done and, when one can be told, the ETA.
type ProgressEstimate struct {
	Roots    int64
	Percent  float64
	ETA      time.Duration
	ETAKnown bool
}

// ProgressStatus is one read of the tracker for the control socket's status. Its counters count
// committed work and never go down within a run; the values are exact and two may be a moment apart.
type ProgressStatus struct {
	Phase string
	// Step is what the run is doing (fetch, discover, copy, delete, log, sleep, wait); Table the table it
	// works on where the step has one. Both are empty when the run does nothing yet.
	Step, Table string
	// WaitingSince is when the loop began waiting in the pause gate; zero when it is not waiting.
	WaitingSince time.Time
	// Sentinel says that the pause gate saw the sentinel file at its last check.
	Sentinel bool

	Batches, Roots, RootsSkipped, RootsRecovered, RowsCopied, RowsDeleted int64
	Tables                                                                []TableCounts
	// Estimate is nil without --progress, and before the estimate is read.
	Estimate *ProgressEstimate
}

// NewProgress returns a tracker for a run that began at began.
func NewProgress(began time.Time) *Progress {
	return &Progress{startedAt: began, tables: map[string]*TableCounts{}}
}

func (p *Progress) now() time.Time {
	if p.nowFn != nil {
		return p.nowFn()
	}
	return time.Now()
}

func (p *Progress) setPhase(phase string) {
	if p == nil {
		return
	}
	p.phase.Store(&phase)
}

// at records the step the run is on and its table; an empty step clears it.
func (p *Progress) at(step, table string) {
	if p == nil {
		return
	}
	if step == "" {
		p.act.Store(nil)
		return
	}
	p.act.Store(&activity{step, table})
}

// counts returns table's counters, made on first use, in the order the tables first appear.
func (p *Progress) counts(table string) *TableCounts {
	c := p.tables[table]
	if c == nil {
		c = &TableCounts{Name: table}
		p.tables[table] = c
		p.order = append(p.order, table)
	}
	return c
}

// copied counts n rows of table committed to the destination.
func (p *Progress) copied(table string, n int64) {
	if p == nil {
		return
	}
	p.rowsCopied.Add(n)
	p.mu.Lock()
	p.counts(table).Copied += n
	p.mu.Unlock()
}

// deleted counts n rows of table deleted from the source and committed.
func (p *Progress) deleted(table string, n int64) {
	if p == nil {
		return
	}
	p.rowsDeleted.Add(n)
	p.mu.Lock()
	p.counts(table).Deleted += n
	p.mu.Unlock()
}

// addRoots counts roots of a completed batch; addBatch counts the batch; skipped counts roots a
// copy-only resume left alone; recovered counts roots a recovery page replayed.
func (p *Progress) addRoots(n int64) {
	if p != nil {
		p.roots.Add(n)
	}
}

func (p *Progress) addBatch() {
	if p != nil {
		p.batches.Add(1)
	}
}

func (p *Progress) skipped(n int64) {
	if p != nil {
		p.rootsSkipped.Add(n)
	}
}

func (p *Progress) recovered(n int64) {
	if p != nil {
		p.rootsRecovered.Add(n)
	}
}

// markComplete records that the loop exhausted the where range. A completed snapshot shows
// nothing remaining, whatever the estimate said. A stop or a failure never calls it.
func (p *Progress) markComplete() {
	if p == nil {
		return
	}
	p.completed.Store(true)
}

// setEstimate records the optimizer's estimate of the roots in range, now that recovery is done; the
// ETA's rate counts from here.
func (p *Progress) setEstimate(total int64) {
	if p == nil {
		return
	}
	now := p.now()
	p.totalRoots.Store(total)
	p.loopAt.Store(&now)
	p.hasEstimate.Store(true)
}

// setPaused and clearPaused follow the pause gate. The reason is folded onto one line, so a
// configured path cannot forge a second progress line. The time the wait began is kept while the
// reason changes and ends with the wait.
func (p *Progress) setPaused(reason string) {
	if p == nil {
		return
	}
	r := strings.NewReplacer("\n", " ", "\r", " ").Replace(reason)
	p.paused.Store(&r)
	if p.waitingSince.Load() == nil {
		now := p.now()
		p.waitingSince.CompareAndSwap(nil, &now)
	}
}

func (p *Progress) clearPaused() {
	if p == nil {
		return
	}
	p.paused.Store(nil)
	p.waitingSince.Store(nil)
}

// sentinelSeen records whether the pause gate saw the sentinel file at its last check.
func (p *Progress) sentinelSeen(seen bool) {
	if p != nil {
		p.sentinel.Store(seen)
	}
}

// Status copies the tracker for the control socket's status. It reads only memory.
func (p *Progress) Status() ProgressStatus {
	if p == nil {
		return ProgressStatus{}
	}
	st := ProgressStatus{
		Phase: PhaseStarting, Sentinel: p.sentinel.Load(),
		Batches: p.batches.Load(), Roots: p.roots.Load(), RootsSkipped: p.rootsSkipped.Load(),
		RootsRecovered: p.rootsRecovered.Load(), RowsCopied: p.rowsCopied.Load(), RowsDeleted: p.rowsDeleted.Load(),
	}
	if ph := p.phase.Load(); ph != nil {
		st.Phase = *ph
	}
	if a := p.act.Load(); a != nil {
		st.Step, st.Table = a.step, a.table
	}
	if w := p.waitingSince.Load(); w != nil {
		st.WaitingSince = *w
	}
	p.mu.Lock()
	for _, name := range p.order {
		st.Tables = append(st.Tables, *p.tables[name])
	}
	p.mu.Unlock()
	if p.hasEstimate.Load() {
		s := p.snapshot()
		eta, known := s.eta()
		st.Estimate = &ProgressEstimate{Roots: s.total, Percent: s.percent(), ETA: eta, ETAKnown: known}
	}
	return st
}

func (p *Progress) snapshot() progressSnapshot {
	if p == nil {
		return progressSnapshot{}
	}
	s := progressSnapshot{
		total:     p.totalRoots.Load(),
		done:      p.roots.Load() + p.rootsSkipped.Load(),
		copied:    p.rowsCopied.Load(),
		deleted:   p.rowsDeleted.Load(),
		elapsed:   p.now().Sub(p.startedAt),
		completed: p.completed.Load(),
	}
	if l := p.loopAt.Load(); l != nil {
		s.running = p.now().Sub(*l)
	}
	if s.completed {
		// A finished run's total is what it did: the estimate was only ever a guide (PRC-1).
		s.total = s.done
	}
	if r := p.paused.Load(); r != nil {
		s.pausedReason = *r
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

// eta is the time left, when one can be told: a completed run has none left; before the first root, or
// past the estimate, no time can be told (PRC-1).
func (s progressSnapshot) eta() (time.Duration, bool) {
	switch {
	case s.completed:
		return 0, true
	case s.done == 0, s.done >= s.total, s.running <= 0:
		return 0, false
	}
	// Roots per running time, so startup and recovery do not slow the estimate.
	rate := float64(s.done) / float64(s.running)
	return time.Duration(float64(s.remaining()) / rate).Truncate(time.Second), true
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
	d, _ := s.eta()
	return d.String()
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
	tracker  *Progress
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

func newProgressReporter(t *Progress, interval time.Duration, typ jobspec.Type, out io.Writer) *progressReporter {
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
