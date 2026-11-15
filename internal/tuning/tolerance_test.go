package tuning

import (
	"strings"
	"sync"
	"testing"
)

// TEL-1 C4b: D15's step rule over the gate's "round completed under the current revision" input
// (spec §4.6). The store is told of each completed round through RoundCompleted, which the gate
// calls with the gate revision the round was judged under; a raise above the configured
// tolerance needs a round completed under the current gate revision, and goes one step at a time.

func tolerance(s *Store) int { return s.Load().SecondsBehindSourceWithin }

// TestToleranceLowerRestore (spec §7 row 35): any lower value is accepted, down to 0, and a raise
// back to the configured value is accepted in one command from any lower value; neither needs a
// completed round. A value above the configuration that is below the current one is a lowering,
// and is accepted too.
func TestToleranceLowerRestore(t *testing.T) {
	s := newStore() // configured 10, no round completed
	for _, v := range []string{"7", "3", "0"} {
		mustSet(t, s, SecondsBehindSourceWithin, v)
	}
	mustSet(t, s, SecondsBehindSourceWithin, "10") // restored in one command, from 0
	mustSet(t, s, SecondsBehindSourceWithin, "4")
	mustSet(t, s, SecondsBehindSourceWithin, "10") // and again from another lower value

	// From a value above the configuration, a lower value is a lowering whatever the step rule
	// says about raises: 20 is reached by a raise, 15 and 10 are lowerings with no new round.
	s.RoundCompleted(s.Load().GateRevision)
	mustSet(t, s, SecondsBehindSourceWithin, "20")
	mustSet(t, s, SecondsBehindSourceWithin, "15")
	mustSet(t, s, SecondsBehindSourceWithin, "10")
	if got := tolerance(s); got != 10 {
		t.Errorf("tolerance = %d, want the configured 10 restored", got)
	}
}

// TestToleranceOneStep (spec §7 row 36): above the configured value the current value plus one
// step is accepted and one more second is refused, naming the largest value accepted now. The
// step is the configured tolerance, or 1 when that is 0. From below the configuration the limit
// is the higher of the configured value and the current value plus one step.
func TestToleranceOneStep(t *testing.T) {
	step := func(configured int) *Store {
		st := start
		st.SecondsBehindSourceWithin = configured
		return newStore(func(o *Options) { o.Start = st })
	}
	round := func(s *Store) { s.RoundCompleted(s.Load().GateRevision) }

	t.Run("configured 10", func(t *testing.T) {
		s := step(10)
		round(s)
		r := refusalOf(t, s, SecondsBehindSourceWithin, "21")
		if r.Reason != ReasonStep {
			t.Fatalf("21 from 10: %s %q, want step", r.Reason, r.Message)
		}
		for _, want := range []string{"21", "current 10", "largest value accepted now is 20", "configured 10 is the step"} {
			if !strings.Contains(r.Message, want) {
				t.Errorf("message %q does not contain %q", r.Message, want)
			}
		}
		if got := tolerance(s); got != 10 {
			t.Errorf("a refused raise left tolerance %d, want 10", got)
		}
		mustSet(t, s, SecondsBehindSourceWithin, "20")
		round(s)
		if r := refusalOf(t, s, SecondsBehindSourceWithin, "31"); !strings.Contains(r.Message, "largest value accepted now is 30") {
			t.Errorf("31 from 20: %q, want the largest accepted now to be 30", r.Message)
		}
		mustSet(t, s, SecondsBehindSourceWithin, "30")
	})

	t.Run("configured 0, the step is 1", func(t *testing.T) {
		s := step(0)
		round(s)
		r := refusalOf(t, s, SecondsBehindSourceWithin, "2")
		if r.Reason != ReasonStep || !strings.Contains(r.Message, "largest value accepted now is 1") {
			t.Fatalf("2 from 0: %s %q, want step naming 1 as the largest accepted now", r.Reason, r.Message)
		}
		if !strings.Contains(r.Message, "step is 1") {
			t.Errorf("message %q does not say that the step is 1", r.Message)
		}
		mustSet(t, s, SecondsBehindSourceWithin, "1")
		round(s)
		mustSet(t, s, SecondsBehindSourceWithin, "2")
	})

	t.Run("from below the configuration", func(t *testing.T) {
		s := step(10)
		mustSet(t, s, SecondsBehindSourceWithin, "3")
		round(s)
		// The limit is max(configured 10, current 3 + step 10) = 13.
		if r := refusalOf(t, s, SecondsBehindSourceWithin, "14"); !strings.Contains(r.Message, "largest value accepted now is 13") {
			t.Errorf("14 from 3: %q, want the largest accepted now to be 13", r.Message)
		}
		mustSet(t, s, SecondsBehindSourceWithin, "13")

		s = step(10)
		mustSet(t, s, SecondsBehindSourceWithin, "0")
		round(s)
		// From 0 the limit is the configured value itself: nothing above it yet.
		if r := refusalOf(t, s, SecondsBehindSourceWithin, "11"); r.Reason != ReasonStep || !strings.Contains(r.Message, "largest value accepted now is 10") {
			t.Errorf("11 from 0: %s %q, want step naming 10", r.Reason, r.Message)
		}
	})

	t.Run("the current value is unchanged, not a raise", func(t *testing.T) {
		s := step(10)
		round(s)
		mustSet(t, s, SecondsBehindSourceWithin, "20") // no round has completed since
		c, err := s.Set(SecondsBehindSourceWithin, "20", peer)
		if err != nil || !c.Unchanged {
			t.Errorf("Set(20) at 20 = %+v, %v, want unchanged, not a refusal for want of a round", c, err)
		}
	})
}

// TestToleranceStepPerRound (spec §7 row 37): a second raise with no probe round completed since
// the first is refused; after a round it is accepted. Before the gate's first round a raise above
// the configuration is refused. A round judged under an older gate revision cannot supply it, and
// a change that is not a gate setting does not use it up.
func TestToleranceStepPerRound(t *testing.T) {
	s := newStore()
	r := refusalOf(t, s, SecondsBehindSourceWithin, "20")
	if r.Reason != ReasonStep || !strings.Contains(r.Message, "not yet been checked") || !strings.Contains(r.Message, "configured 10") {
		t.Fatalf("a raise before the gate's first round: %s %q, want step saying the replicas have not been checked, naming the configured 10", r.Reason, r.Message)
	}

	s.RoundCompleted(0) // a round judged under gate revision 0
	mustSet(t, s, SecondsBehindSourceWithin, "20")
	if got := s.Load().GateRevision; got != 1 {
		t.Fatalf("gate revision after the raise = %d, want 1", got)
	}
	if r := refusalOf(t, s, SecondsBehindSourceWithin, "30"); r.Reason != ReasonStep || !strings.Contains(r.Message, "not yet been checked") {
		t.Errorf("a second raise with no round since: %s %q, want step saying the replicas have not been checked", r.Reason, r.Message)
	}
	s.RoundCompleted(0) // an older round, or a cached pass of it, cannot supply the next step
	if r := refusalOf(t, s, SecondsBehindSourceWithin, "30"); r.Reason != ReasonStep {
		t.Errorf("a second raise on an older revision's round: %s, want step", r.Reason)
	}
	s.RoundCompleted(1)
	mustSet(t, s, SecondsBehindSourceWithin, "30")

	// A change that is not a gate setting leaves the round valid.
	s.RoundCompleted(2)
	mustSet(t, s, BatchSize, "500")
	mustSet(t, s, SleepSeconds, "2")
	mustSet(t, s, SecondsBehindSourceWithin, "40")

	// check_interval is a gate setting: it uses the round up, as does a lowering.
	s.RoundCompleted(3)
	mustSet(t, s, CheckInterval, "7")
	if r := refusalOf(t, s, SecondsBehindSourceWithin, "50"); r.Reason != ReasonStep {
		t.Errorf("a raise after a check_interval change: %s, want step until a round completes", r.Reason)
	}
	s.RoundCompleted(4)
	mustSet(t, s, SecondsBehindSourceWithin, "35") // a lowering
	if r := refusalOf(t, s, SecondsBehindSourceWithin, "45"); r.Reason != ReasonStep {
		t.Errorf("a raise right after a lowering: %s, want step until a round completes", r.Reason)
	}
	s.RoundCompleted(5)
	mustSet(t, s, SecondsBehindSourceWithin, "45")
}

// TestRoundCompletedKeepsTheNewest: rounds report out of order when two calls overlap; an older
// round arriving late does not take back what a newer one gave.
func TestRoundCompletedKeepsTheNewest(t *testing.T) {
	s := newStore()
	s.RoundCompleted(0)
	mustSet(t, s, SecondsBehindSourceWithin, "20") // gate revision 1
	s.RoundCompleted(1)
	s.RoundCompleted(0) // late
	mustSet(t, s, SecondsBehindSourceWithin, "30")
}

// TestToleranceRefusalChangesNothing (spec §7 row 38, first half): a refused raise changes no
// snapshot, no revision, no summary and no audit event. (The WARNING an accepted raise writes is
// the server's, C5.)
func TestToleranceRefusalChangesNothing(t *testing.T) {
	s := newStore()
	s.RoundCompleted(0)
	mustSet(t, s, SecondsBehindSourceWithin, "20")
	before := s.Load()
	events := len(s.Audit().Events())
	for _, text := range []string{"21", "50", "600"} { // no round since the raise; and over one step
		if r := refusalOf(t, s, SecondsBehindSourceWithin, text); r.Reason != ReasonStep {
			t.Fatalf("raise to %s: %s, want step", text, r.Reason)
		}
	}
	s.RoundCompleted(1)
	for _, text := range []string{"31", "600"} { // a round exists; over one step
		if r := refusalOf(t, s, SecondsBehindSourceWithin, text); r.Reason != ReasonStep {
			t.Fatalf("raise to %s: %s, want step", text, r.Reason)
		}
	}
	if got := s.Load(); got != before {
		t.Errorf("a refused raise replaced the snapshot: revision %d, was %d", got.Revision, before.Revision)
	}
	if got := len(s.Audit().Events()); got != events {
		t.Errorf("a refused raise queued an audit event: %d, was %d", got, events)
	}
	sum := s.Terminal()
	if len(sum.Settings) != 1 || sum.Settings[0].Key != SecondsBehindSourceWithin || sum.Settings[0].Changes != 1 || !sum.Settings[0].AboveConfigured {
		t.Errorf("summary = %+v, want the one accepted raise to 20 counted once and marked above the configuration", sum.Settings)
	}
}

// TestRoundCompletedIsSafeAlongsideSetters: the gate reports a round without the writer lock, and
// a setter reads the report under it; run under -race, with the setter's outcome asserted: after
// the last round reported under the current revision the raise is accepted.
func TestRoundCompletedIsSafeAlongsideSetters(t *testing.T) {
	s := newStore()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				s.RoundCompleted(s.Load().GateRevision)
			}
		}()
	}
	for range 50 {
		_, _ = s.Set(SecondsBehindSourceWithin, "20", peer)
		_, _ = s.Set(SecondsBehindSourceWithin, "10", peer)
	}
	wg.Wait()
	s.RoundCompleted(s.Load().GateRevision)
	if _, err := s.Set(SecondsBehindSourceWithin, "20", peer); err != nil {
		t.Errorf("a raise after a round under the current revision: %v", err)
	}
}
