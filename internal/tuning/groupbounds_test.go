package tuning

import (
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// GR C7s (spec §4.3 *The two bounds as runtime settings*, G6 as amended, G15): group_applier_queue_within and
// group_certifier_queue_within are settings of the store, and gate settings like the tolerance and the interval.
// A raise above the configured value follows D15's rule bound by bound, with each bound's own configured value as
// its step, and the three settings share one gate revision. The fixture's configured values (tolerance 10, applier
// 1000, certifier 300) differ on purpose: a rule that took another setting's step would show.

var groupBounds = []Key{GroupApplierQueueWithin, GroupCertifierQueueWithin}

func configured(k Key) int { return start.whole(k) }

func value(s *Store, k Key) int { return s.Load().whole(k) }

// round completes a probe round under the store's current gate revision, as the gate does.
func round(s *Store) { s.RoundCompleted(s.Load().GateRevision) }

// withConfigured is a store whose run was configured with the bound k at v.
func withConfigured(k Key, v int) *Store {
	st := start.withWhole(k, v)
	return newStore(func(o *Options) { o.Start = st })
}

// TestGroupBoundLoweredAndRestoredFreely: a bound goes to any lower value, down to 0 (which tolerates no queue), and
// back to the configured value in one command from any lower value; neither needs a completed round. From a value
// above the configuration, a lower value is a lowering whatever the step rule says of raises.
func TestGroupBoundLoweredAndRestoredFreely(t *testing.T) {
	for _, k := range groupBounds {
		t.Run(string(k), func(t *testing.T) {
			s := newStore() // no round has completed
			cfg := configured(k)
			for _, v := range []int{cfg - 1, cfg / 2, 1, 0} {
				mustSet(t, s, k, strconv.Itoa(v))
				if got := value(s, k); got != v {
					t.Fatalf("%s = %d after setting %d", k, got, v)
				}
			}
			mustSet(t, s, k, strconv.Itoa(cfg)) // restored in one command, from 0
			mustSet(t, s, k, "3")
			mustSet(t, s, k, strconv.Itoa(cfg))

			round(s)
			mustSet(t, s, k, strconv.Itoa(2*cfg)) // above the configuration, by a raise
			mustSet(t, s, k, strconv.Itoa(cfg+cfg/2))
			mustSet(t, s, k, strconv.Itoa(cfg))
			if got := value(s, k); got != cfg {
				t.Errorf("%s = %d, want the configured %d restored", k, got, cfg)
			}
		})
	}
}

// TestGroupBoundOneStepAboveTheConfiguration: above the configured value the current value plus one step is
// accepted and one more is refused, naming the largest value accepted now. The step is the bound's own configured
// value, and 1 when that is 0. From below the configuration the limit is the higher of the configured value and the
// current value plus a step.
func TestGroupBoundOneStepAboveTheConfiguration(t *testing.T) {
	for _, k := range groupBounds {
		cfg := configured(k)
		t.Run(string(k)+" climbs by its own configured value", func(t *testing.T) {
			s := newStore()
			round(s)
			r := refusalOf(t, s, k, strconv.Itoa(2*cfg+1))
			if r.Reason != ReasonStep {
				t.Fatalf("%d from %d: %s %q, want step", 2*cfg+1, cfg, r.Reason, r.Message)
			}
			for _, want := range []string{string(k), strconv.Itoa(2*cfg + 1), "current " + strconv.Itoa(cfg), "largest value accepted now is " + strconv.Itoa(2*cfg), "configured " + strconv.Itoa(cfg) + " is the step"} {
				if !strings.Contains(r.Message, want) {
					t.Errorf("message %q does not contain %q", r.Message, want)
				}
			}
			if got := value(s, k); got != cfg {
				t.Errorf("a refused raise left %s at %d, want %d", k, got, cfg)
			}
			mustSet(t, s, k, strconv.Itoa(2*cfg))
			round(s)
			if r := refusalOf(t, s, k, strconv.Itoa(3*cfg+1)); !strings.Contains(r.Message, "largest value accepted now is "+strconv.Itoa(3*cfg)) {
				t.Errorf("%d from %d: %q, want the largest accepted now to be %d", 3*cfg+1, 2*cfg, r.Message, 3*cfg)
			}
			mustSet(t, s, k, strconv.Itoa(3*cfg))
		})

		t.Run(string(k)+" configured 0, the step is 1", func(t *testing.T) {
			s := withConfigured(k, 0)
			round(s)
			r := refusalOf(t, s, k, "2")
			if r.Reason != ReasonStep || !strings.Contains(r.Message, "largest value accepted now is 1") || !strings.Contains(r.Message, "step is 1") {
				t.Fatalf("2 from 0: %s %q, want step naming 1 as the largest accepted now, with the step of 1 said", r.Reason, r.Message)
			}
			mustSet(t, s, k, "1")
			round(s)
			mustSet(t, s, k, "2")
		})

		t.Run(string(k)+" from below the configuration", func(t *testing.T) {
			s := newStore()
			mustSet(t, s, k, "3")
			round(s)
			// The limit is max(configured, current 3 + step) = configured + 3.
			if r := refusalOf(t, s, k, strconv.Itoa(cfg+4)); r.Reason != ReasonStep || !strings.Contains(r.Message, "largest value accepted now is "+strconv.Itoa(cfg+3)) {
				t.Errorf("%d from 3: %s %q, want the largest accepted now to be %d", cfg+4, r.Reason, r.Message, cfg+3)
			}
			mustSet(t, s, k, strconv.Itoa(cfg+3))

			s = newStore()
			mustSet(t, s, k, "0")
			round(s)
			// From 0 the limit is the configured value itself when the configured value is the larger.
			if r := refusalOf(t, s, k, strconv.Itoa(cfg+1)); r.Reason != ReasonStep || !strings.Contains(r.Message, "largest value accepted now is "+strconv.Itoa(cfg)) {
				t.Errorf("%d from 0: %s %q, want step naming %d", cfg+1, r.Reason, r.Message, cfg)
			}
		})

		t.Run(string(k)+" the current value is unchanged, not a raise", func(t *testing.T) {
			s := newStore()
			round(s)
			mustSet(t, s, k, strconv.Itoa(2*cfg)) // no round has completed since
			c, err := s.Set(k, strconv.Itoa(2*cfg), peer)
			if err != nil || !c.Unchanged {
				t.Errorf("Set(%d) at %d = %+v, %v, want unchanged, not a refusal for want of a round", 2*cfg, 2*cfg, c, err)
			}
		})
	}
}

// TestGroupBoundRefusalWordsAreTheSpecs: the example of spec §4.3, whole, and the refusal for want of a round, which
// says that the group has not been checked under the current bounds.
func TestGroupBoundRefusalWordsAreTheSpecs(t *testing.T) {
	s := newStore()
	r := refusalOf(t, s, GroupApplierQueueWithin, "5000")
	for _, want := range []string{"group_applier_queue_within 5000", "the configured 1000", "the group has not yet been checked under the current bounds"} {
		if !strings.Contains(r.Message, want) {
			t.Errorf("a raise before the gate's first round: %q does not contain %q", r.Message, want)
		}
	}
	round(s)
	r = refusalOf(t, s, GroupApplierQueueWithin, "5000")
	const want = "group_applier_queue_within 5000 is more than one step above the current 1000; the largest value accepted now is 2000 (the configured 1000 is the step)"
	if r.Reason != ReasonStep || r.Message != want {
		t.Errorf("5000 from 1000 = %s %q, want step %q", r.Reason, r.Message, want)
	}
}

// TestGroupBoundNotRaisedBeforeTheFirstRound: before the gate's first round no raise above the configuration is
// accepted, and a round judged under an older gate revision cannot supply one.
func TestGroupBoundNotRaisedBeforeTheFirstRound(t *testing.T) {
	for _, k := range groupBounds {
		s := newStore()
		cfg := configured(k)
		if r := refusalOf(t, s, k, strconv.Itoa(cfg+1)); r.Reason != ReasonStep || !strings.Contains(r.Message, "not yet been checked") {
			t.Errorf("%s: a raise before the first round: %s %q, want step saying the group has not been checked", k, r.Reason, r.Message)
		}
		s.RoundCompleted(0)
		mustSet(t, s, k, strconv.Itoa(cfg+1)) // revision 1
		s.RoundCompleted(0)                   // an older round, or a cached pass of it, supplies nothing
		if r := refusalOf(t, s, k, strconv.Itoa(cfg+2)); r.Reason != ReasonStep {
			t.Errorf("%s: a raise on an older revision's round: %s, want step", k, r.Reason)
		}
		s.RoundCompleted(1)
		mustSet(t, s, k, strconv.Itoa(cfg+2))
	}
}

// TestTheThreeGateSettingsShareOneRevision: an accepted change of the tolerance, the interval or either bound moves
// the one gate revision, so after one of them a raise above the configuration of any of the three waits for a round
// completed under the new revision, and is then judged by its own configured step. The settings have different
// configured steps (10, 1000 and 300): after each accepted raise, the refusal comes first, then the acceptance after
// a round at the new revision.
func TestTheThreeGateSettingsShareOneRevision(t *testing.T) {
	s := newStore()
	refusedUntilARound := func(k Key, v int) {
		t.Helper()
		if r := refusalOf(t, s, k, strconv.Itoa(v)); r.Reason != ReasonStep || !strings.Contains(r.Message, "not yet been checked") {
			t.Fatalf("%s %d before a round at revision %d: %s %q, want step for want of a round", k, v, s.Load().GateRevision, r.Reason, r.Message)
		}
		round(s)
		mustSet(t, s, k, strconv.Itoa(v))
	}

	round(s)
	mustSet(t, s, SecondsBehindSourceWithin, "20") // the tolerance, one step of 10
	refusedUntilARound(GroupApplierQueueWithin, 2000)
	refusedUntilARound(SecondsBehindSourceWithin, 30) // the reverse: a bound's raise used the round up
	refusedUntilARound(GroupCertifierQueueWithin, 600)
	refusedUntilARound(GroupApplierQueueWithin, 3000) // one bound after the other

	// Each takes its own step in the round that follows: the certifier's is 300, the applier's 1000.
	round(s)
	if r := refusalOf(t, s, GroupCertifierQueueWithin, "901"); !strings.Contains(r.Message, "largest value accepted now is 900") {
		t.Errorf("certifier 901 from 600: %q, want the largest accepted now to be 900", r.Message)
	}
	if r := refusalOf(t, s, GroupApplierQueueWithin, "4001"); !strings.Contains(r.Message, "largest value accepted now is 4000") {
		t.Errorf("applier 4001 from 3000: %q, want the largest accepted now to be 4000", r.Message)
	}
	mustSet(t, s, GroupCertifierQueueWithin, "900")

	// The interval is a gate setting too, and so is a lowering of a bound.
	round(s)
	mustSet(t, s, CheckInterval, "7")
	refusedUntilARound(GroupApplierQueueWithin, 4000)
	round(s)
	mustSet(t, s, GroupCertifierQueueWithin, "100") // a lowering moves the revision
	refusedUntilARound(GroupApplierQueueWithin, 5000)

	// A change that is not a gate setting leaves the round valid.
	round(s)
	mustSet(t, s, BatchSize, "500")
	mustSet(t, s, SleepSeconds, "2")
	mustSet(t, s, GroupApplierQueueWithin, "6000")
}

// TestGroupBoundChangesMoveTheGateRevisionAndAnEqualValueDoesNot: an accepted change of a bound raises the revision
// and the gate revision; an equal value answers `unchanged` and moves neither, nor the audit queue.
func TestGroupBoundChangesMoveTheGateRevisionAndAnEqualValueDoesNot(t *testing.T) {
	for _, k := range groupBounds {
		s := newStore()
		before := s.Load()
		c := mustSet(t, s, k, "5")
		after := s.Load()
		if c.Unchanged || c.Revision != before.Revision+1 || after.GateRevision != before.GateRevision+1 || c.Old != strconv.Itoa(configured(k)) || c.New != "5" || c.Effect != "from the next gate decision" {
			t.Errorf("%s: change = %+v, gate revision %d -> %d", k, c, before.GateRevision, after.GateRevision)
		}
		events := len(s.Audit().Events())
		c, err := s.Set(k, "5", peer)
		if err != nil || !c.Unchanged || c.Revision != 0 {
			t.Fatalf("%s: an equal value = %+v, %v, want unchanged", k, c, err)
		}
		if got := s.Load(); got != after {
			t.Errorf("%s: an equal value replaced the snapshot: revision %d, was %d", k, got.Revision, after.Revision)
		}
		if got := len(s.Audit().Events()); got != events {
			t.Errorf("%s: an equal value queued an audit event: %d, was %d", k, got, events)
		}
	}
}

// TestGroupBoundNumbers: a fraction, a sign, an exponent and a number too large to hold are `invalid`, and the word
// `step` is for a raise only.
func TestGroupBoundNumbers(t *testing.T) {
	for _, k := range groupBounds {
		for _, text := range []string{"12.5", "-1", "+5", "1e3", " 5", "", "abc", "9223372036854775808", "99999999999999999999"} {
			if r := refusalOf(t, newStore(), k, text); r.Reason != ReasonInvalid {
				t.Errorf("%s %q: %s %q, want invalid", k, text, r.Reason, r.Message)
			}
		}
	}
}

// TestGroupBoundStepDoesNotOverflow carries over the tolerance's saturating addition and its range: a configured
// bound of 5e18 with a round completed has a step of 5e18, a sum above the largest int, and a raise to the largest int
// is within one step: the limit does not wrap to a small number.
func TestGroupBoundStepDoesNotOverflow(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("an int of this size has no bound this large")
	}
	for _, k := range groupBounds {
		s := withConfigured(k, asInt(5_000_000_000_000_000_000))
		s.RoundCompleted(0)
		c := mustSet(t, s, k, strconv.Itoa(math.MaxInt))
		if c.Unchanged || value(s, k) != math.MaxInt {
			t.Errorf("%s: raise to the largest int = %+v, value %d, want it accepted", k, c, value(s, k))
		}
	}
}

// TestGroupBoundsApply: the two bounds are used where the run's gate has a server of type group, which depends on
// nothing that changes during the run. A run that does not use the gate gives the gate's words; a gate whose servers
// are all asynchronous says so; the other four gate and size settings are unaffected either way.
func TestGroupBoundsApply(t *testing.T) {
	noGate := newStore(func(o *Options) { o.Applies = Applicability{Delete: true} })
	asyncGate := newStore(func(o *Options) { o.Applies = Applicability{Delete: true, Gate: true} })
	for _, k := range groupBounds {
		for _, tc := range []struct {
			name  string
			store *Store
			why   string
		}{
			{"no gate", noGate, "this run does not use the replication gate"},
			{"a gate with no server of type group", asyncGate, "this run's gate has no server of type group"},
		} {
			r := refusalOf(t, tc.store, k, "5")
			if r.Reason != ReasonInapplicable || !strings.Contains(r.Message, tc.why) {
				t.Errorf("%s with %s: %s %q, want inapplicable saying %q", k, tc.name, r.Reason, r.Message, tc.why)
			}
			if ok, why := tc.store.Applies(k); ok || why != tc.why {
				t.Errorf("%s with %s: Applies() = %v, %q, want the same words", k, tc.name, ok, why)
			}
		}
		if ok, _ := newStore().Applies(k); !ok {
			t.Errorf("%s does not apply to a run whose gate has a group server", k)
		}
	}
	mustSet(t, asyncGate, SecondsBehindSourceWithin, "5")
	mustSet(t, asyncGate, CheckInterval, "3")

	want := []Key{BatchSize, BatchDeleteSize, SleepSeconds, DeleteSleepSeconds, SecondsBehindSourceWithin, CheckInterval}
	if got := asyncGate.ApplicableKeys(); !slices.Equal(got, want) {
		t.Errorf("a gate with no group server uses %v, want the six that existed", got)
	}
	if got := newStore().ApplicableKeys(); !slices.Equal(got, append(want, GroupApplierQueueWithin, GroupCertifierQueueWithin)) {
		t.Errorf("a gate with a group server uses %v, want the eight in listing order", got)
	}

	// The order of reasons holds: inapplicable before ending before invalid.
	asyncGate.Terminal()
	if r := refusalOf(t, asyncGate, GroupApplierQueueWithin, "abc"); r.Reason != ReasonInapplicable {
		t.Errorf("an inapplicable bound with a bad number after the end: %s, want inapplicable", r.Reason)
	}
}

// TestGroupBoundSummary: the closing summary is bounded by eight settings however often they are changed, lists a
// bound changed and restored, and marks a bound that ends above its configured value, as it does the tolerance.
func TestGroupBoundSummary(t *testing.T) {
	s := newStore()
	round(s)
	mustSet(t, s, GroupApplierQueueWithin, "2000") // above the configuration
	round(s)
	mustSet(t, s, GroupCertifierQueueWithin, "100")
	mustSet(t, s, GroupCertifierQueueWithin, "300") // restored
	round(s)
	mustSet(t, s, GroupApplierQueueWithin, "1500") // still above

	sum := s.Terminal()
	want := []SettingSummary{
		{Key: GroupApplierQueueWithin, Configured: "1000", Final: "1500", Changes: 2, AboveConfigured: true},
		{Key: GroupCertifierQueueWithin, Configured: "300", Final: "300", Changes: 2},
	}
	if !slices.Equal(sum.Settings, want) {
		t.Errorf("settings = %+v, want %+v", sum.Settings, want)
	}

	// A bound lowered below its configuration is not above it.
	s = newStore()
	mustSet(t, s, GroupApplierQueueWithin, "0")
	if sum := s.Terminal(); len(sum.Settings) != 1 || sum.Settings[0].AboveConfigured {
		t.Errorf("settings = %+v, want the one lowering, not above the configuration", sum.Settings)
	}

	// Eight settings at most, in listing order.
	s = newStore()
	for _, k := range Keys() {
		mustSet(t, s, k, "2") // every setting's starting value is above or below 2, and each is accepted
	}
	for i, got := range s.Terminal().Settings {
		if got.Key != Keys()[i] {
			t.Errorf("summary entry %d is %s, want %s", i, got.Key, Keys()[i])
		}
	}
	if n := len(s.Terminal().Settings); n != 8 {
		t.Errorf("summary has %d settings, want eight", n)
	}
}

// TestGroupBoundAuditEvent: an accepted change of a bound queues the event every change has, with the value it
// replaced.
func TestGroupBoundAuditEvent(t *testing.T) {
	s := newStore()
	mustSet(t, s, GroupCertifierQueueWithin, "50")
	select {
	case ev := <-s.Audit().Events():
		if ev.Kind != KindSet || ev.Key != GroupCertifierQueueWithin || ev.Old != "300" || ev.New != "50" || ev.Revision != 1 || ev.Peer != peer {
			t.Errorf("event = %+v, want the change of group_certifier_queue_within 300 -> 50 at revision 1", ev)
		}
	default:
		t.Fatal("an accepted change queued no audit event")
	}
}
