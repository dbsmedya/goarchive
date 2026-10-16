package engine

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
)

// The --progress reporter is 2.x's (R50), ported to the engine: the same line, the same
// arithmetic, one line per tick and one final line.

func snap(total, done, copied, deleted int64, elapsed time.Duration) progressSnapshot {
	return progressSnapshot{total: total, done: done, copied: copied, deleted: deleted, elapsed: elapsed, running: elapsed}
}

func TestProgressSnapshotMath(t *testing.T) {
	for _, tc := range []struct {
		name      string
		s         progressSnapshot
		percent   float64
		remaining int64
	}{
		{"a third", snap(12000, 4000, 0, 0, 7*time.Minute), 33.333, 8000},
		{"past the estimate", snap(12000, 12100, 0, 0, time.Minute), 100, 0},
		{"nothing estimated, some done", snap(0, 37, 0, 0, time.Minute), 100, 0},
		{"nothing at all", snap(0, 0, 0, 0, 0), 100, 0},
	} {
		if got := tc.s.percent(); got < tc.percent-0.05 || got > tc.percent+0.05 {
			t.Errorf("%s: percent = %.3f, want %.3f", tc.name, got, tc.percent)
		}
		if got := tc.s.remaining(); got != tc.remaining {
			t.Errorf("%s: remaining = %d, want %d", tc.name, got, tc.remaining)
		}
	}
}

func TestProgressETAPrecedence(t *testing.T) {
	paused := snap(100, 50, 0, 0, time.Minute)
	paused.pausedReason = "sentinel"
	completed := snap(100, 60, 0, 0, time.Minute)
	completed.completed = true
	for _, tc := range []struct {
		name string
		s    progressSnapshot
		want string
	}{
		{"paused", paused, "paused"},
		{"completed", completed, "0s"},
		{"at the estimate, still running", snap(100, 100, 0, 0, time.Minute), "unknown"},
		{"past the estimate, still running", snap(100, 130, 0, 0, time.Minute), "unknown"},
		{"nothing estimated, some done", snap(0, 37, 0, 0, time.Minute), "unknown"},
		{"nothing done yet", snap(100, 0, 0, 0, time.Minute), "calculating"},
		{"a third in 7m10s", snap(12000, 4000, 0, 0, 7*time.Minute+10*time.Second), "14m20s"},
	} {
		if got := tc.s.etaString(); got != tc.want {
			t.Errorf("%s: eta = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestFormatProgressLine(t *testing.T) {
	s := snap(12000, 4000, 45210, 43000, 7*time.Minute+10*time.Second)
	for _, tc := range []struct {
		typ          jobspec.Type
		want, absent []string
	}{
		{jobspec.Archive, []string{"progress: roots  4000/~12000 ( 33.3%)  remaining=8000 ", "copied_rows=45210", "deleted_rows=43000", "elapsed=7m10s", "eta=14m20s"}, nil},
		{jobspec.CopyOnly, []string{"copied_rows=45210"}, []string{"deleted_rows"}},
		{jobspec.Purge, []string{"deleted_rows=43000"}, []string{"copied_rows"}},
	} {
		line := formatProgressLine(s, tc.typ)
		for _, w := range tc.want {
			if !strings.Contains(line, w) {
				t.Errorf("%s: %q lacks %q", tc.typ, line, w)
			}
		}
		for _, a := range tc.absent {
			if strings.Contains(line, a) {
				t.Errorf("%s: %q has %q", tc.typ, line, a)
			}
		}
	}
	s.pausedReason = `sentinel file "/var/run/goarchive.pause" present`
	line := formatProgressLine(s, jobspec.Archive)
	if !strings.Contains(line, "eta=paused") || !strings.Contains(line, `[PAUSED: sentinel file "/var/run/goarchive.pause" present]`) || strings.Contains(line, "\n") {
		t.Errorf("paused line = %q", line)
	}
}

func TestProgressTracker(t *testing.T) {
	var none *progressTracker
	none.update(1, 2, 3)
	none.markComplete()
	none.setPaused("x")
	none.clearPaused()
	if none.snapshot() != (progressSnapshot{}) {
		t.Error("a nil tracker has a snapshot")
	}

	tr := newProgressTracker(1000, time.Now())
	tr.nowFn = func() time.Time { return tr.startedAt.Add(30 * time.Second) }
	tr.update(150, 2000, 1400)
	if s := tr.snapshot(); s.total != 1000 || s.done != 150 || s.copied != 2000 || s.deleted != 1400 || s.elapsed != 30*time.Second || s.completed {
		t.Errorf("snapshot = %+v", s)
	}
	tr.setPaused("reason\nwith newline")
	if got := tr.snapshot().pausedReason; got != "reason with newline" {
		t.Errorf("paused reason = %q, want one line", got)
	}
	tr.clearPaused()
	tr.markComplete()
	if s := tr.snapshot(); !s.completed || s.remaining() != 0 || s.etaString() != "0s" || s.pausedReason != "" {
		t.Errorf("completed snapshot = %+v", s)
	}

	// A naturally completed run's total is its done count (PRC-1, spec rev 3 §3.4): the closing
	// line must not read 2567/~15420 at 100%. A stop keeps the estimate (it never marks complete).
	over := newProgressTracker(15420, time.Now())
	over.update(2567, 0, 0)
	if s := over.snapshot(); s.total != 15420 || s.etaString() == "unknown" {
		t.Errorf("running snapshot = %+v, want the estimate 15420", s)
	}
	over.markComplete()
	if s := over.snapshot(); s.total != 2567 || s.remaining() != 0 || s.etaString() != "0s" ||
		!strings.HasPrefix(formatProgressLine(s, jobspec.Archive), "progress: roots 2567/~2567 (100.0%)") {
		t.Errorf("completed snapshot = %+v, line %q; want the done count 2567 as the total", s, formatProgressLine(s, jobspec.Archive))
	}
}

func TestProgressReporter(t *testing.T) {
	tr := newProgressTracker(100, time.Now())
	tr.nowFn = func() time.Time { return tr.startedAt.Add(10 * time.Second) }
	var out strings.Builder
	r := newProgressReporter(tr, time.Second, jobspec.Archive, &out)
	ticks := make(chan time.Time)
	r.tickCh = ticks
	r.start()
	tr.update(10, 40, 0)
	ticks <- time.Time{}
	ticks <- time.Time{}
	r.stop()
	r.stop()
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 3 || !strings.Contains(lines[0], "roots  10/~100") || lines[1] != lines[0] {
		t.Errorf("two ticks and a stop printed %q, want three lines", lines)
	}

	var silent strings.Builder
	newProgressReporter(newProgressTracker(1, time.Now()), time.Second, jobspec.Archive, &silent).stop()
	if silent.Len() != 0 {
		t.Errorf("a reporter stopped before it started printed %q", silent.String())
	}
}

// TestFormatProgressLineAligned (spec §3.5): fixed-width columns, so successive lines line up.
func TestFormatProgressLineAligned(t *testing.T) {
	line := func(done, copied, deleted int64, completed bool) string {
		s := progressSnapshot{total: 12000, done: done, copied: copied, deleted: deleted,
			elapsed: 42 * time.Second, running: 30 * time.Second, completed: completed}
		return formatProgressLine(s, jobspec.Archive)
	}
	got := []string{line(4000, 21640, 21640, false), line(8000, 43310, 43310, false), line(12000, 64920, 64920, true)}
	want := []string{
		"progress: roots  4000/~12000 ( 33.3%)  remaining=8000   copied_rows=21640      deleted_rows=21640      elapsed=42s      eta=",
		"progress: roots  8000/~12000 ( 66.7%)  remaining=4000   copied_rows=43310      deleted_rows=43310      elapsed=42s      eta=",
		"progress: roots 12000/~12000 (100.0%)  remaining=0      copied_rows=64920      deleted_rows=64920      elapsed=42s      eta=0s",
	}
	for i := range want {
		if !strings.HasPrefix(got[i], want[i]) {
			t.Errorf("line %d:\n got %q\nwant %q", i, got[i], want[i])
		}
	}
	for _, col := range []string{"remaining=", "copied_rows=", "deleted_rows=", "elapsed=", "eta="} {
		at := strings.Index(got[0], col)
		for i := range got {
			if strings.Index(got[i], col) != at {
				t.Errorf("%s is at %d on line %d, %d on line 0", col, strings.Index(got[i], col), i, at)
			}
		}
	}
	wide := formatProgressLine(progressSnapshot{total: 9, done: 9, copied: 1234567890, completed: true}, jobspec.CopyOnly)
	if !strings.Contains(wide, "copied_rows=1234567890  elapsed=") {
		t.Errorf("an over-width count = %q, want it to push only its own line", wide)
	}
}

// TestProgressClock (spec §3.5): elapsed counts from Run's start; the ETA's rate from the
// first batch, so a slow startup and recovery do not slow the estimate.
func TestProgressClock(t *testing.T) {
	began := time.Unix(1000, 0)
	tr := &progressTracker{totalRoots: 100, startedAt: began, loopAt: began.Add(20 * time.Second)}
	tr.nowFn = func() time.Time { return began.Add(30 * time.Second) }
	tr.update(50, 0, 0)
	s := tr.snapshot()
	if s.elapsed != 30*time.Second {
		t.Errorf("elapsed = %v, want 30s (from Run's start, after a 20s startup)", s.elapsed)
	}
	if got := s.etaString(); got != "10s" {
		t.Errorf("eta = %s, want 10s (50 roots in the 10s of batches)", got)
	}
}

// TestProgressReporterEntries (spec §3.2 C): with a JSON console log the reporter writes one
// progress entry per tick, past the level filter, with the line's numbers as fields.
func TestProgressReporterEntries(t *testing.T) {
	var out bytes.Buffer
	log, err := logger.New(&config.LoggingConfig{Level: "error", Format: "json", Output: "stdout"}, logger.Options{Stdout: &out})
	if err != nil {
		t.Fatal(err)
	}
	tr := newProgressTracker(100, time.Now())
	r := newProgressReporter(tr, time.Second, jobspec.Purge, nil)
	r.log = log
	ticks := make(chan time.Time)
	r.tickCh = ticks
	r.start()
	tr.update(10, 0, 40)
	ticks <- time.Time{}
	r.stop()
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("one tick and a stop wrote %q, want two entries", out.String())
	}
	var e map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &e); err != nil {
		t.Fatalf("entry %q: %v", lines[0], err)
	}
	if e["msg"] != "progress" || e["roots_done"] != 10.0 || e["roots_total"] != 100.0 || e["deleted_rows"] != 40.0 ||
		e["remaining"] != 90.0 || e["copied_rows"] != nil || e["elapsed_seconds"] == nil || e["eta"] == nil {
		t.Errorf("progress entry = %v", e)
	}
}
