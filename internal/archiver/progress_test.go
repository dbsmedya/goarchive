package archiver

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func mkSnap(total, done, copied, deleted int64, elapsed time.Duration) progressSnapshot {
	return progressSnapshot{total: total, done: done, copied: copied,
		deleted: deleted, elapsed: elapsed}
}

func TestProgressSnapshot_Math(t *testing.T) {
	s := mkSnap(12000, 4000, 45210, 45210, 7*time.Minute)
	assert.InDelta(t, 33.3, s.percent(), 0.05)
	assert.Equal(t, int64(8000), s.remaining())

	over := mkSnap(12000, 12100, 0, 0, time.Minute)
	assert.Equal(t, 100.0, over.percent())
	assert.Equal(t, int64(0), over.remaining())

	zt := mkSnap(0, 37, 0, 0, time.Minute)
	assert.Equal(t, 100.0, zt.percent())
	assert.Equal(t, int64(0), zt.remaining())

	zz := mkSnap(0, 0, 0, 0, 0)
	assert.Equal(t, 100.0, zz.percent())
	assert.Equal(t, int64(0), zz.remaining())
}

func TestProgressSnapshot_ETAPrecedence(t *testing.T) {
	p := mkSnap(100, 50, 0, 0, time.Minute)
	p.pausedReason = `sentinel file "/tmp/x" present`
	assert.Equal(t, "paused", p.etaString())

	c := mkSnap(100, 60, 0, 0, time.Minute)
	c.completed = true
	assert.Equal(t, "0s", c.etaString())
	assert.Equal(t, int64(0), c.remaining())
	assert.Equal(t, 100.0, c.percent())

	r := mkSnap(100, 100, 0, 0, time.Minute)
	assert.Equal(t, "0s", r.etaString())

	n := mkSnap(100, 0, 0, 0, time.Minute)
	assert.Equal(t, "calculating", n.etaString())

	e := mkSnap(12000, 4000, 0, 0, 7*time.Minute+10*time.Second)
	assert.Equal(t, "14m20s", e.etaString())
}

func TestFormatProgressLine_PerMode(t *testing.T) {
	s := mkSnap(12000, 4000, 45210, 43000, 7*time.Minute+10*time.Second)

	full := formatProgressLine(s, batchFull)
	assert.Contains(t, full, "progress: roots 4000/~12000 (33.3%) remaining=8000")
	assert.Contains(t, full, "copied_rows=45210")
	assert.Contains(t, full, "deleted_rows=43000")
	assert.Contains(t, full, "elapsed=7m10s")

	co := formatProgressLine(s, batchCopyVerify)
	assert.Contains(t, co, "copied_rows=45210")
	assert.NotContains(t, co, "deleted_rows")

	pu := formatProgressLine(s, batchDeleteOnly)
	assert.Contains(t, pu, "deleted_rows=43000")
	assert.NotContains(t, pu, "copied_rows")
}

func TestFormatProgressLine_PausedAnnotation(t *testing.T) {
	s := mkSnap(12000, 4000, 45210, 45210, 9*time.Minute+40*time.Second)
	s.pausedReason = `sentinel file "/var/run/goarchive.pause" present`
	line := formatProgressLine(s, batchFull)
	assert.Contains(t, line, "eta=paused")
	assert.Contains(t, line, `[PAUSED: sentinel file "/var/run/goarchive.pause" present]`)
	assert.False(t, strings.Contains(line, "\n"), "single physical line")
}

func TestProgressTracker_NilSafe(t *testing.T) {
	var tr *progressTracker
	tr.RecordBatch(BatchStats{RootsProcessed: 5})
	tr.SeedRows(1, 2)
	tr.MarkComplete()
	tr.SetPaused("x")
	tr.ClearPaused()
	assert.Equal(t, progressSnapshot{}, tr.snapshot())
}

func TestProgressTracker_AccumulateAndSnapshot(t *testing.T) {
	tr := newProgressTracker(1000)
	tr.nowFn = func() time.Time { return tr.startedAt.Add(30 * time.Second) }
	tr.SeedRows(500, 200)
	tr.RecordBatch(BatchStats{RootsProcessed: 100, RecordsCopied: 1100, RecordsDeleted: 900})
	tr.RecordBatch(BatchStats{RootsProcessed: 50, RecordsCopied: 400, RecordsDeleted: 300})

	s := tr.snapshot()
	assert.Equal(t, int64(1000), s.total)
	assert.Equal(t, int64(150), s.done)
	assert.Equal(t, int64(2000), s.copied)
	assert.Equal(t, int64(1400), s.deleted)
	assert.Equal(t, 30*time.Second, s.elapsed)
	assert.False(t, s.completed)
	assert.Empty(t, s.pausedReason)

	tr.SetPaused("reason\nwith newline")
	assert.Equal(t, "reason with newline", tr.snapshot().pausedReason)
	tr.ClearPaused()
	assert.Empty(t, tr.snapshot().pausedReason)

	tr.MarkComplete()
	done := tr.snapshot()
	assert.True(t, done.completed)
	assert.Equal(t, int64(0), done.remaining())
	assert.Equal(t, 100.0, done.percent())
	assert.Equal(t, "0s", done.etaString())
}

func TestProgressReporter_TicksAndFinalLineOnce(t *testing.T) {
	tr := newProgressTracker(100)
	tr.nowFn = func() time.Time { return tr.startedAt.Add(10 * time.Second) }

	var buf strings.Builder
	r := newProgressReporter(tr, time.Second, batchFull, &buf)
	ticks := make(chan time.Time)
	r.tickCh = ticks
	r.start()

	tr.RecordBatch(BatchStats{RootsProcessed: 10, RecordsCopied: 40})
	ticks <- time.Time{}
	ticks <- time.Time{}
	r.stop()
	r.stop()

	out := buf.String()
	lines := strings.Split(strings.TrimSpace(out), "\n")
	assert.Len(t, lines, 3)
	for _, l := range lines {
		assert.True(t, strings.HasPrefix(l, "progress: "), l)
	}
	assert.Contains(t, lines[0], "roots 10/~100")
	assert.Equal(t, lines[1], lines[0])
}

func TestProgressReporter_FinalLineReflectsCompletion(t *testing.T) {
	tr := newProgressTracker(100)
	tr.nowFn = func() time.Time { return tr.startedAt.Add(time.Minute) }
	var buf strings.Builder
	r := newProgressReporter(tr, time.Second, batchDeleteOnly, &buf)
	r.tickCh = make(chan time.Time)
	r.start()

	tr.RecordBatch(BatchStats{RootsProcessed: 60, RecordsDeleted: 500})
	tr.MarkComplete()
	r.stop()

	out := strings.TrimSpace(buf.String())
	assert.Equal(t, 1, strings.Count(out, "\n")+1, "exactly one line")
	assert.Contains(t, out, "remaining=0")
	assert.Contains(t, out, "(100.0%)")
	assert.Contains(t, out, "eta=0s")
	assert.Contains(t, out, "deleted_rows=500")
	assert.NotContains(t, out, "copied_rows")
}

func TestProgressReporter_StopWithoutStartPrintsNothing(t *testing.T) {
	var buf strings.Builder
	r := newProgressReporter(newProgressTracker(1), time.Second, batchFull, &buf)
	r.stop()
	assert.Empty(t, buf.String())
}
