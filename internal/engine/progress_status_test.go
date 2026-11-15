package engine

import (
	"sync"
	"testing"
	"time"
)

// The progress tracker as the control socket's status reads it (TEL-1 C6, spec §4.8): it exists for
// every run, from the start, and carries the phase, the activity, the committed-work counters (also
// per table), the pause and, only with --progress, the estimate.

func statusOf(p *Progress) ProgressStatus { return p.Status() }

// TestProgressNilIsInert: a nil tracker (a caller that gave none, an explain run) makes every hook a
// no-op and its status empty.
func TestProgressNilIsInert(t *testing.T) {
	var p *Progress
	p.setPhase(PhaseRunning)
	p.at("copy", "orders")
	p.copied("orders", 3)
	p.deleted("orders", 3)
	p.addRoots(3)
	p.addBatch()
	p.skipped(1)
	p.recovered(2)
	p.sentinelSeen(true)
	p.setPaused("x")
	p.clearPaused()
	p.markComplete()
	p.setEstimate(10)
	if st := p.Status(); st.Phase != "" || st.Batches != 0 || len(st.Tables) != 0 || st.Estimate != nil {
		t.Errorf("a nil tracker's status = %+v, want it empty", st)
	}
}

// TestProgressStatusCounters: the counters count committed work, in total and per table, in the
// order the tables first appear, and only go up.
func TestProgressStatusCounters(t *testing.T) {
	p := NewProgress(time.Now())
	if st := statusOf(p); st.Phase != PhaseStarting || st.Batches != 0 || len(st.Tables) != 0 || st.Estimate != nil {
		t.Fatalf("a new tracker's status = %+v, want it starting, with nothing counted and no estimate", st)
	}
	p.copied("orders", 3)
	p.copied("order_lines", 6)
	p.deleted("order_lines", 4)
	p.deleted("order_lines", 2)
	p.deleted("orders", 3)
	p.addRoots(3)
	p.addBatch()
	p.skipped(2)
	p.recovered(5)
	st := statusOf(p)
	if st.RowsCopied != 9 || st.RowsDeleted != 9 || st.Roots != 3 || st.Batches != 1 || st.RootsSkipped != 2 || st.RootsRecovered != 5 {
		t.Errorf("counters = %+v", st)
	}
	want := []TableCounts{{"orders", 3, 3}, {"order_lines", 6, 6}}
	if len(st.Tables) != 2 || st.Tables[0] != want[0] || st.Tables[1] != want[1] {
		t.Errorf("tables = %+v, want %+v in the order the tables first appeared", st.Tables, want)
	}
	// The status is a copy: nothing a reader does reaches the tracker.
	st.Tables[0].Copied = 999
	if got := statusOf(p).Tables[0].Copied; got != 3 {
		t.Errorf("a reader changed the tracker: orders copied = %d", got)
	}
	p.deleted("line_notes", 1) // a table first met by a delete
	if tables := statusOf(p).Tables; len(tables) != 3 || tables[2] != (TableCounts{"line_notes", 0, 1}) {
		t.Errorf("tables = %+v, want line_notes appended with 0 copied and 1 deleted", tables)
	}
}

// TestProgressStatusPhaseAndActivity: the state word's input (starting, recovering, running) and the
// step the run is on with the table it has; clearing the step leaves none.
func TestProgressStatusPhaseAndActivity(t *testing.T) {
	p := NewProgress(time.Now())
	for _, phase := range []string{PhaseRecovering, PhaseRunning} {
		p.setPhase(phase)
		if got := statusOf(p).Phase; got != phase {
			t.Errorf("phase = %q, want %q", got, phase)
		}
	}
	if st := statusOf(p); st.Step != "" || st.Table != "" {
		t.Errorf("activity before any step = %q on %q, want none", st.Step, st.Table)
	}
	p.at("delete", "order_lines")
	if st := statusOf(p); st.Step != "delete" || st.Table != "order_lines" {
		t.Errorf("activity = %q on %q, want delete on order_lines", st.Step, st.Table)
	}
	p.at("sleep", "")
	if st := statusOf(p); st.Step != "sleep" || st.Table != "" {
		t.Errorf("activity = %q on %q, want sleep with no table", st.Step, st.Table)
	}
	p.at("", "")
	if st := statusOf(p); st.Step != "" {
		t.Errorf("activity after clearing = %q, want none", st.Step)
	}
}

// TestProgressStatusPause: a run waiting in the pause gate has the time it began waiting, kept when
// the reason changes and gone when the wait ends; the sentinel flag is what the gate last saw.
func TestProgressStatusPause(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	p := NewProgress(now)
	p.nowFn = func() time.Time { return now }
	if st := statusOf(p); !st.WaitingSince.IsZero() || st.Sentinel {
		t.Fatalf("a new tracker's pause = %v, %v", st.WaitingSince, st.Sentinel)
	}
	p.sentinelSeen(true)
	p.setPaused("control socket")
	began := now
	now = now.Add(time.Minute)
	p.setPaused("control socket, sentinel file present") // the reason changed; the wait did not
	if st := statusOf(p); !st.WaitingSince.Equal(began) || !st.Sentinel {
		t.Errorf("pause = waiting since %v, sentinel %v, want since %v and the sentinel seen", st.WaitingSince, st.Sentinel, began)
	}
	p.clearPaused()
	p.sentinelSeen(false)
	if st := statusOf(p); !st.WaitingSince.IsZero() || st.Sentinel {
		t.Errorf("pause after the wait = %v, %v, want none", st.WaitingSince, st.Sentinel)
	}
	p.setPaused("control socket") // a later wait starts its own time
	if st := statusOf(p); !st.WaitingSince.Equal(now) {
		t.Errorf("a second wait began %v, want %v", st.WaitingSince, now)
	}
}

// TestProgressStatusEstimate: without --progress there is no estimate; with it the status carries the
// estimate, the percentage and, once the ETA can be told, the ETA.
func TestProgressStatusEstimate(t *testing.T) {
	began := time.Unix(1000, 0)
	p := NewProgress(began)
	p.nowFn = func() time.Time { return began }
	p.setEstimate(100)
	if e := statusOf(p).Estimate; e == nil || e.Roots != 100 || e.Percent != 0 || e.ETAKnown {
		t.Fatalf("estimate before any root = %+v, want 100 roots, 0%%, no ETA yet", e)
	}
	p.nowFn = func() time.Time { return began.Add(10 * time.Second) }
	p.addRoots(50)
	if e := statusOf(p).Estimate; e == nil || e.Percent != 50 || !e.ETAKnown || e.ETA != 10*time.Second {
		t.Errorf("estimate half way = %+v, want 50%% and 10s (50 roots in 10 s)", e)
	}
	p.addRoots(60) // rows aged into the range: past the estimate, no time can be told
	if e := statusOf(p).Estimate; e == nil || e.Percent != 100 || e.ETAKnown {
		t.Errorf("estimate past the estimate = %+v, want 100%% and no ETA", e)
	}
	p.markComplete()
	if e := statusOf(p).Estimate; e == nil || e.Roots != 110 || e.Percent != 100 || !e.ETAKnown || e.ETA != 0 {
		t.Errorf("estimate of a completed run = %+v, want its done count 110 as the total, 100%% and 0s", e)
	}
}

// TestProgressConcurrent: the loop writes while a status read copies, as the control server and the
// --progress reporter read; asserted by the race detector the unit checks run under, and by the final
// counts.
func TestProgressConcurrent(t *testing.T) {
	p := NewProgress(time.Now())
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			table := []string{"a", "b", "c", "d"}[i]
			for range 500 {
				p.copied(table, 1)
				p.deleted(table, 1)
				p.at("copy", table)
				p.addRoots(1)
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				_ = p.Status()
				_ = p.snapshot()
			}
		}
	}()
	wg.Wait()
	close(done)
	st := p.Status()
	if st.RowsCopied != 2000 || st.RowsDeleted != 2000 || st.Roots != 2000 || len(st.Tables) != 4 {
		t.Errorf("status = %+v, want 2000 of each over four tables", st)
	}
}
