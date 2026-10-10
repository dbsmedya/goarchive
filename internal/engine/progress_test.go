package engine

import (
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// The --progress reporter is 2.x's (R50), ported to the engine: the same line, the same
// arithmetic, one line per tick and one final line.

func snap(total, done, copied, deleted int64, elapsed time.Duration) progressSnapshot {
	return progressSnapshot{total: total, done: done, copied: copied, deleted: deleted, elapsed: elapsed}
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
		{"all done", snap(100, 100, 0, 0, time.Minute), "0s"},
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
		{jobspec.Archive, []string{"progress: roots 4000/~12000 (33.3%) remaining=8000", "copied_rows=45210", "deleted_rows=43000", "elapsed=7m10s", "eta=14m20s"}, nil},
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

	tr := newProgressTracker(1000)
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
}

func TestProgressReporter(t *testing.T) {
	tr := newProgressTracker(100)
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
	if len(lines) != 3 || !strings.Contains(lines[0], "roots 10/~100") || lines[1] != lines[0] {
		t.Errorf("two ticks and a stop printed %q, want three lines", lines)
	}

	var silent strings.Builder
	newProgressReporter(newProgressTracker(1), time.Second, jobspec.Archive, &silent).stop()
	if silent.Len() != 0 {
		t.Errorf("a reporter stopped before it started printed %q", silent.String())
	}
}
