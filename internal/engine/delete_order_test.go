package engine

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// The order of the delete path (D16, spec §4.5, review R1 finding 2) is proved from the
// boundaries a run records through Config.at (observation only, nil in production): where the
// deleteTree call begins, where each sleep starts and ends, where the gate decision starts and
// ends, where the source session was taken, where the chunk was taken and the statement goes out,
// and where it committed. The order is a fact about which came before which. It is not a bound
// on how long the steps between two boundaries take: taking a session may wait for the pool or
// open a connection, and a correct run must not fail because it did.

// traceEvent is one recorded boundary.
type traceEvent struct {
	point point
	at    time.Time
}

// deleteBoundaries are the points a trace holds.
var deleteBoundaries = []point{atDeleteTree, atDeleteSleep, atDeleteSlept, atDeleteGate, atDeleteGated, atDeleteSession, atDeleteIssue, atDeleted}

// tracer records the delete path's boundaries that a run reports through Config.at, with the time
// of each from one clock. It observes: it neither delays nor changes anything, so the run's real
// sleep, real gate and real database are what is observed.
type tracer struct {
	mu     sync.Mutex
	events []traceEvent
}

// at is Config.at.
func (tr *tracer) at(p point) {
	if !slices.Contains(deleteBoundaries, p) {
		return
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.events = append(tr.events, traceEvent{p, time.Now()})
}

func (tr *tracer) trace() []traceEvent {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return slices.Clone(tr.events)
}

// TestTracerKeepsTheDeleteBoundariesInOrder: only the delete path's boundaries are recorded, in the
// order they were reported, with non-decreasing times.
func TestTracerKeepsTheDeleteBoundariesInOrder(t *testing.T) {
	var tr tracer
	for _, p := range []point{atLogged, atDeleteTree, atCommitted, atDeleteGate, atDeleteGated, atMarked, atDeleteSession, atDeleteIssue, atDeleteWritten, atDeleted} {
		tr.at(p)
	}
	var got []point
	var prev time.Time
	for _, ev := range tr.trace() {
		got = append(got, ev.point)
		if ev.at.Before(prev) {
			t.Fatalf("time went backwards at %s", ev.point)
		}
		prev = ev.at
	}
	want := []point{atDeleteTree, atDeleteGate, atDeleteGated, atDeleteSession, atDeleteIssue, atDeleted}
	if !slices.Equal(got, want) {
		t.Fatalf("recorded %v, want %v", got, want)
	}
}

// checkDeleteOrder judges a trace of the delete path against D16: in each deleteTree call the first
// DELETE has no sleep before it, and every DELETE after it is sleep (start, end), gate (start, end),
// source session, chunk and statement, committed, in that order. The trace may end inside a DELETE
// (a stop, a hold, an error). sleep is the configured delete_sleep_seconds: each sleep must have
// lasted at least that long between its own boundaries (the pacing floor), which says nothing of the
// time the other steps took.
func checkDeleteOrder(trace []traceEvent, sleep time.Duration) error {
	if len(trace) == 0 {
		return fmt.Errorf("the trace is empty")
	}
	if trace[0].point != atDeleteTree {
		return fmt.Errorf("event 1 is %s, want %s: a trace begins with a deleteTree call", trace[0].point, atDeleteTree)
	}
	// seq is what one DELETE records, after a sleep or, for a batch's first, without one.
	withSleep := []point{atDeleteSleep, atDeleteSlept, atDeleteGate, atDeleteGated, atDeleteSession, atDeleteIssue, atDeleted}
	first := withSleep[2:]

	var (
		batch    = 0 // the deleteTree call
		issued   = 0 // DELETEs completed or begun before the current one in this call
		step     = 0 // position in the current DELETE's sequence
		sleepAt  time.Time
		expected = func() []point {
			if issued == 0 {
				return first
			}
			return withSleep
		}
	)
	for i, ev := range trace {
		if ev.point == atDeleteTree {
			if step != 0 && step != len(expected()) {
				// A new call while the last DELETE was still going: that call ended early. Only
				// a complete DELETE may be followed by a new batch.
				return fmt.Errorf("event %d: a new deleteTree call began inside DELETE %d of batch %d, after %s", i+1, issued+1, batch, expected()[step-1])
			}
			batch, issued, step = batch+1, 0, 0
			continue
		}
		if step == len(expected()) { // the DELETE is complete: the next begins
			step = 0
			issued++
		}
		want := expected()[step]
		if ev.point != want {
			return fmt.Errorf("event %d: %s, want %s (DELETE %d of batch %d)%s", i+1, ev.point, want, issued+1, batch, explainDeviation(ev.point, want, issued))
		}
		switch ev.point {
		case atDeleteSleep:
			sleepAt = ev.at
		case atDeleteSlept:
			if got := ev.at.Sub(sleepAt); got < sleep {
				return fmt.Errorf("event %d: the sleep before DELETE %d of batch %d lasted %v, want at least %v", i+1, issued+1, batch, got, sleep)
			}
		}
		step++
	}
	return nil
}

// explainDeviation names the likely meaning of an out-of-order event.
func explainDeviation(got, want point, issued int) string {
	switch {
	case got == atDeleteSleep && issued == 0:
		return ": a sleep before the first DELETE of a batch, where none has been issued"
	case got == atDeleteGate && want == atDeleteSleep:
		return ": the gate was asked before the sleep"
	case got == atDeleteSleep && want == atDeleteGated:
		return ": the sleep began inside the gate decision"
	case got == atDeleteSleep && want == atDeleteSession:
		return ": the sleep came after the gate, before the session"
	case got == atDeleteSession && want == atDeleteGate:
		return ": the session was taken before the gate decision"
	}
	return ""
}

// synthetic builds a trace from point names with times: a list of "name" or "name@ms".
func synthetic(base time.Time, spec string) []traceEvent {
	var out []traceEvent
	ms := 0
	for _, f := range strings.Fields(spec) {
		name, at, ok := strings.Cut(f, "@")
		if ok {
			if _, err := fmt.Sscanf(at, "%d", &ms); err != nil {
				panic(err)
			}
		}
		out = append(out, traceEvent{point(name), base.Add(time.Duration(ms) * time.Millisecond)})
		ms++
	}
	return out
}

// TestCheckDeleteOrder is the check's own test, on synthetic timelines: the order D16 requires
// passes, and so does a correct run in which a step between two boundaries was slow; the old
// order (gate, sleep, session, statement), a sleep before a batch's first DELETE, and every other
// out-of-order boundary fail, each with a reason.
func TestCheckDeleteOrder(t *testing.T) {
	base := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	const sleep = 300 * time.Millisecond
	one := "delete-gate delete-gated delete-session delete-issue deleted" // a batch's first DELETE
	// later is each following DELETE, whose sleep starts at ms and lasts the configured 300 ms.
	later := func(ms int) string {
		return fmt.Sprintf("delete-sleep@%d delete-slept@%d delete-gate delete-gated delete-session delete-issue deleted", ms, ms+300)
	}
	next := later(0)

	for _, tc := range []struct {
		name  string
		trace string
		sleep time.Duration
		fail  string // "" passes; else a substring of the reason
	}{
		{"one DELETE", "delete-tree " + one, sleep, ""},
		{"three DELETEs, each after the first behind its sleep", "delete-tree " + one + " " + next + " " + later(1000), sleep, ""},
		{"two batches: the second's first DELETE has no sleep", "delete-tree " + one + " " + next + " delete-tree " + one + " " + later(2000), sleep, ""},
		{"a batch with no DELETE", "delete-tree delete-tree " + one, sleep, ""},
		{"a trace that ends inside a DELETE (a stop, a hold, an error)", "delete-tree " + one + " delete-sleep@0 delete-slept@300 delete-gate", sleep, ""},
		{"no sleep configured: zero-length sleeps", "delete-tree " + one + " delete-sleep@0 delete-slept@0 delete-gate delete-gated delete-session delete-issue deleted", 0, ""},

		// A slow step is not an order violation: a long wait for the session (the pool, a new
		// connection) or a slow gate round, between boundaries that are in order.
		{"a long wait for the session", "delete-tree " + one + " delete-sleep@0 delete-slept@300 delete-gate@301 delete-gated@302 delete-session@5302 delete-issue@8302 deleted@8303", sleep, ""},
		{"a long gate decision", "delete-tree " + one + " delete-sleep@0 delete-slept@300 delete-gate@301 delete-gated@60301 delete-session@60302 delete-issue@60303 deleted@60304", sleep, ""},
		{"a slow statement", "delete-tree " + one + " delete-sleep@0 delete-slept@300 delete-gate@301 delete-gated@302 delete-session@303 delete-issue@304 deleted@90304", sleep, ""},

		{"the old order: gate, then sleep, then session",
			"delete-tree " + one + " delete-gate delete-gated delete-sleep@0 delete-slept@300 delete-session delete-issue deleted", sleep,
			"the gate was asked before the sleep"},
		{"the old order, with no sleep configured",
			"delete-tree " + one + " delete-gate delete-gated delete-session delete-issue deleted", 0,
			"the gate was asked before the sleep"},
		{"a sleep before the first DELETE of a batch",
			"delete-tree delete-sleep@0 delete-slept@300 " + one, sleep,
			"a sleep before the first DELETE of a batch"},
		{"a sleep before the first DELETE of the second batch",
			"delete-tree " + one + " delete-tree delete-sleep@0 delete-slept@300 " + one, sleep,
			"a sleep before the first DELETE of a batch"},
		{"the session before the gate decision",
			"delete-tree " + one + " delete-sleep@0 delete-slept@300 delete-session delete-gate delete-gated delete-issue deleted", sleep,
			"the session was taken before the gate decision"},
		{"the sleep after the gate decision, before the session",
			"delete-tree " + one + " delete-gate delete-gated delete-sleep@0 delete-slept@300 delete-session delete-issue deleted", sleep,
			"the gate was asked before the sleep"},
		{"the statement before the session",
			"delete-tree " + one + " delete-sleep@0 delete-slept@300 delete-gate delete-gated delete-issue delete-session deleted", sleep,
			"delete-issue, want delete-session"},
		{"no sleep between two DELETEs",
			"delete-tree " + one + " " + one, sleep,
			"want delete-sleep"},
		{"a sleep shorter than configured",
			"delete-tree " + one + " delete-sleep@0 delete-slept@100 delete-gate delete-gated delete-session delete-issue deleted", sleep,
			"lasted 100ms, want at least 300ms"},
		{"a new batch inside a DELETE",
			"delete-tree " + one + " delete-sleep@0 delete-slept@300 delete-gate delete-tree " + one, sleep,
			"a new deleteTree call began inside DELETE 2"},
		{"a trace that does not begin with a deleteTree call", one, sleep, "a trace begins with a deleteTree call"},
		{"an empty trace", "", sleep, "empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkDeleteOrder(synthetic(base, tc.trace), tc.sleep)
			switch {
			case tc.fail == "" && err != nil:
				t.Fatalf("a correct trace failed: %v", err)
			case tc.fail != "" && err == nil:
				t.Fatalf("a wrong trace passed; want a failure saying %q", tc.fail)
			case tc.fail != "" && !strings.Contains(err.Error(), tc.fail):
				t.Fatalf("failed with %q, want it to say %q", err, tc.fail)
			}
		})
	}
}
