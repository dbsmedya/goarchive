package replication

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// TEL-1 C4b: the gate reads the lag tolerance and the check interval from the settings store,
// once per probe round (spec §4.6); a cached pass and a round carry the gate revision they were
// judged under; a pass under a replaced revision is discarded; the gate keeps a last-verdict
// record and tells the store of each completed round. Deterministic like the rest of this
// package: the clock and the sleep are the harness's seams, no goroutine decides an outcome.

var gatePeer = tuning.Peer{UID: 1000, PID: 4242}

func lagSnapshot(lag int64) []dbsrepl.ChannelStatus {
	return []dbsrepl.ChannelStatus{ch("", "Yes", "Yes", lag, true)}
}

// setGate applies one accepted setter to the store, as the control server will.
func setGate(t *testing.T, s *tuning.Store, k tuning.Key, text string) {
	t.Helper()
	if _, err := s.Set(k, text, gatePeer); err != nil {
		t.Fatalf("Set(%s, %s) = %v", k, text, err)
	}
}

// recordSleeps replaces the harness's sleep with one that records every wait the gate asks for
// and advances the fake clock by it, so no wait is slept twice or skipped silently.
func recordSleeps(h *gateHarness) *[]time.Duration {
	var slept []time.Duration
	h.gate.sleep = func(ctx context.Context, _ <-chan struct{}, _ <-chan struct{}, d time.Duration) error {
		slept = append(slept, d)
		h.clock.t = h.clock.t.Add(d)

		return ctx.Err()
	}

	return &slept
}

// TestGateInFlightPass (spec §7 row 57): a round's read is held open at tolerance 60; the
// tolerance is lowered to 10; the read returns lag 40. The round passed at 60, is discarded, no
// pass is cached, the call does not return, and the next round runs at once and holds at 10.
func TestGateInFlightPass(t *testing.T) {
	var h *gateHarness
	var cachedAtRound2 time.Time
	r := &fakeReader{responses: []readerResult{
		{snapshot: lagSnapshot(40), before: func() { setGate(t, h.store, tuning.SecondsBehindSourceWithin, "10") }},
		{snapshot: lagSnapshot(40), before: func() { cachedAtRound2 = h.gate.Record().LastPass }},
		okRes(lagSnapshot(0)),
	}}
	h = newStoreHarness(t, time.Minute, 60, 5, r)
	slept := recordSleeps(h)

	if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
		t.Fatalf("WaitForLag() = %v, want nil once the replica is under the lowered tolerance", err)
	}
	if r.calls != 3 {
		t.Errorf("reads = %d, want 3: the discarded pass, the hold at 10, the pass", r.calls)
	}
	if !slices.Equal(*slept, []time.Duration{5 * time.Second}) {
		t.Errorf("waits = %v, want one interval, after the failing round only: the discarded pass starts the next round at once", *slept)
	}
	if !cachedAtRound2.IsZero() {
		t.Errorf("a pass was recorded at %v before the second round: a pass under a replaced revision must record nothing", cachedAtRound2)
	}
	warns := msgsAt(h.lines(), "warn")
	if countWith(warns, "lag=40s tolerance=10s") != 1 || countWith(warns, "tolerance=60s") != 0 {
		t.Errorf("warn lines = %#v, want one hold at tolerance 10 and none at 60", warns)
	}
	if h.gate.Record().LastPass.IsZero() {
		t.Error("the final pass, under the current revision, was not recorded")
	}
}

// TestGateRoundJudgedByOneSnapshot: a round loads one snapshot and judges every server of the
// round by it; a change that arrives between two servers' probes applies from the next round.
func TestGateRoundJudgedByOneSnapshot(t *testing.T) {
	var h *gateHarness
	first := &fakeReader{responses: []readerResult{
		{snapshot: lagSnapshot(0), before: func() { setGate(t, h.store, tuning.SecondsBehindSourceWithin, "5") }},
		okRes(lagSnapshot(0)),
	}}
	second := &fakeReader{responses: []readerResult{okRes(lagSnapshot(8)), okRes(lagSnapshot(8)), okRes(lagSnapshot(0))}}
	h = newStoreHarness(t, 0, 10, 5, first, second)
	slept := recordSleeps(h)

	if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
		t.Fatalf("WaitForLag() = %v", err)
	}
	warns := msgsAt(h.lines(), "warn")
	if len(warns) != 1 || !strings.Contains(warns[0], "lag=8s tolerance=5s") {
		t.Errorf("warn lines = %#v, want exactly one hold, at the lowered tolerance 5: round 1 judged the second server (lag 8) at 10", warns)
	}
	if first.calls != 3 || second.calls != 3 || len(*slept) != 1 {
		t.Errorf("reads = %d and %d, waits = %v, want 3 rounds of both servers and one wait", first.calls, second.calls, *slept)
	}
}

// TestGateFailingRoundChange (spec §7 row 58): a gate setting changes while a failing round's
// read is in flight: the gate's sleep is never called with the old interval, and the next round
// is judged by the new settings.
func TestGateFailingRoundChange(t *testing.T) {
	t.Run("interval", func(t *testing.T) {
		var h *gateHarness
		r := &fakeReader{responses: []readerResult{
			{snapshot: stoppedSnapshot(), before: func() { setGate(t, h.store, tuning.CheckInterval, "2") }},
			okRes(stoppedSnapshot()),
			okRes(healthySnapshot()),
		}}
		h = newStoreHarness(t, 0, 10, 5, r)
		slept := recordSleeps(h)

		if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
			t.Fatalf("WaitForLag() = %v", err)
		}
		if !slices.Equal(*slept, []time.Duration{2 * time.Second}) {
			t.Errorf("waits = %v, want only the new interval 2s, after the second round: the old 5s is never waited", *slept)
		}
		if r.calls != 3 {
			t.Errorf("reads = %d, want 3", r.calls)
		}
	})

	t.Run("tolerance", func(t *testing.T) {
		var h *gateHarness
		r := &fakeReader{responses: []readerResult{
			{snapshot: stoppedSnapshot(), before: func() { setGate(t, h.store, tuning.SecondsBehindSourceWithin, "5") }},
			okRes(lagSnapshot(8)),
			okRes(lagSnapshot(0)),
		}}
		h = newStoreHarness(t, 0, 10, 5, r)
		slept := recordSleeps(h)

		if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
			t.Fatalf("WaitForLag() = %v", err)
		}
		if !slices.Equal(*slept, []time.Duration{5 * time.Second}) {
			t.Errorf("waits = %v, want one interval, after the second round: the first round's wait is skipped", *slept)
		}
		if warns := msgsAt(h.lines(), "warn"); countWith(warns, "lag=8s tolerance=5s") != 1 {
			t.Errorf("warn lines = %#v, want the second round judged at the new tolerance 5", warns)
		}
	})
}

// TestGateUnrelatedChange (spec §7 row 59): a batch_size change during the interval wait does
// not start a round early: the wait goes on for the time that is left.
func TestGateUnrelatedChange(t *testing.T) {
	r := &fakeReader{responses: []readerResult{okRes(stoppedSnapshot()), okRes(healthySnapshot())}}
	h := newGateHarness(t, 0, r)
	start := h.clock.t
	var slept []time.Duration
	h.gate.sleep = func(ctx context.Context, _ <-chan struct{}, wake <-chan struct{}, d time.Duration) error {
		slept = append(slept, d)
		if len(slept) == 1 {
			setGate(t, h.store, tuning.BatchSize, "500") // closes the snapshot the wait is on
			select {
			case <-wake:
			default:
				t.Error("the wait's signal did not close on a change of the settings")
			}
			if r.calls != 1 {
				t.Errorf("reads = %d when the wait was woken, want 1", r.calls)
			}
			h.clock.t = h.clock.t.Add(time.Second) // woken after one second

			return nil
		}
		h.clock.t = h.clock.t.Add(d)

		return ctx.Err()
	}

	if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
		t.Fatalf("WaitForLag() = %v", err)
	}
	if !slices.Equal(slept, []time.Duration{5 * time.Second, 4 * time.Second}) {
		t.Errorf("waits = %v, want the interval, then the 4s that were left", slept)
	}
	if got := h.gate.Record().Servers[0].CheckedAt; !got.Equal(start.Add(5 * time.Second)) {
		t.Errorf("second round probed at +%v, want +5s: not early", got.Sub(start))
	}
}

// TestGateCachedPassRevision (spec §7 row 60): a pass cached under one gate revision is not a hit
// under the next; under the same revision it is a hit until cache_ttl. A setting that is not a gate
// setting leaves it valid.
func TestGateCachedPassRevision(t *testing.T) {
	r := &fakeReader{responses: []readerResult{okRes(healthySnapshot())}}
	h := newGateHarness(t, 30*time.Second, r)
	call := func(wantReads int, why string) {
		t.Helper()
		if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
			t.Fatalf("WaitForLag() = %v", err)
		}
		if r.calls != wantReads {
			t.Errorf("%s: reads = %d, want %d", why, r.calls, wantReads)
		}
	}

	call(1, "first call probes")
	call(1, "a pass under the same revision, within the ttl, is a hit")
	setGate(t, h.store, tuning.CheckInterval, "4")
	call(2, "the next gate revision cannot hit the older pass")
	call(2, "the new pass is a hit under its own revision")
	setGate(t, h.store, tuning.BatchSize, "500")
	call(2, "a change of another setting leaves the pass valid")
	h.tick(31 * time.Second)
	call(3, "past the ttl the same revision probes again")
	setGate(t, h.store, tuning.SecondsBehindSourceWithin, "5")
	call(4, "a tolerance change is a gate-revision change too")
}

// TestGateStopNeverHeld (spec §7 row 61): a stop during the repeat probe of a discarded pass does
// not end a call that has never held and whose fleet is healthy.
func TestGateStopNeverHeld(t *testing.T) {
	var h *gateHarness
	stop := make(chan struct{})
	r := &fakeReader{responses: []readerResult{
		{snapshot: healthySnapshot(), before: func() { setGate(t, h.store, tuning.SecondsBehindSourceWithin, "9") }},
		{snapshot: healthySnapshot(), before: func() { close(stop) }},
	}}
	h = newGateHarness(t, 0, r)

	if err := h.gate.WaitForLag(context.Background(), stop); err != nil {
		t.Fatalf("WaitForLag() = %v, want nil: the call never held, so a stop does not cut its probe short", err)
	}
	if r.calls != 2 || r.cut != 0 {
		t.Errorf("reads = %d, cut = %d, want 2 reads and none cut", r.calls, r.cut)
	}
	if h.sleepCalls != 0 {
		t.Errorf("waits = %d, want none", h.sleepCalls)
	}
	if h.gate.Record().LastPass.IsZero() {
		t.Error("the healthy repeat round did not record its pass")
	}
}

// TestGateStopAlreadyHolding (spec §7 row 62): round 1 fails; round 2 passes under a revision
// replaced while it probed and is discarded; round 3's read blocks. Stop is closed: the call
// returns ErrStopped at once. With the context cancelled as well, the context's error wins.
func TestGateStopAlreadyHolding(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cancel bool
		want   error
	}{
		{"stop", false, ErrStopped},
		{"stop and a cancelled context", true, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var h *gateHarness
			stop := make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r := &fakeReader{responses: []readerResult{
				okRes(stoppedSnapshot()),
				{snapshot: healthySnapshot(), before: func() { setGate(t, h.store, tuning.SecondsBehindSourceWithin, "9") }},
				{block: true, before: func() {
					if tc.cancel {
						cancel()
					}
					close(stop)
				}},
			}}
			h = newGateHarness(t, 0, r)

			err := h.gate.WaitForLag(ctx, stop)
			if !errors.Is(err, tc.want) {
				t.Fatalf("WaitForLag() = %v, want %v", err, tc.want)
			}
			if r.calls != 3 || r.cut != 1 {
				t.Errorf("reads = %d, cut = %d, want 3 reads and the blocked one cut", r.calls, r.cut)
			}
			if !h.gate.Record().HoldingSince.IsZero() {
				t.Error("the record still says the gate is holding after the call returned")
			}
		})
	}
}

// TestGateVerdictRecord (spec §7 row 63): healthy, lagging, unreachable and unreadable servers
// are recorded with cause, lag, tolerance and time; the lag is the largest among the gated
// channels and absent when a gated channel reports NULL or the read failed; the record also says
// since when the call holds, and the time of the last pass.
func TestGateVerdictRecord(t *testing.T) {
	dial := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	r := []*fakeReader{
		{responses: []readerResult{okRes(lagSnapshot(0))}},
		{responses: []readerResult{okRes(lagSnapshot(40))}},
		{responses: []readerResult{errRes(dial)}},
		{responses: []readerResult{errRes(errors.New("malformed packet"))}},
		{responses: []readerResult{okRes([]dbsrepl.ChannelStatus{ch("", "Yes", "Yes", 0, false)})}},
		{responses: []readerResult{okRes([]dbsrepl.ChannelStatus{ch("", "Yes", "Yes", 3, true), ch("billing", "Yes", "Yes", 12, true)})}},
		{responses: []readerResult{okRes([]dbsrepl.ChannelStatus{ch("", "Yes", "Yes", 40, true), ch("billing", "Yes", "Yes", 2, true)})}},
	}
	h := newGateHarness(t, 0, r...)
	h.gate.servers[6].channels = []string{"billing"} // gates one channel of two
	start := h.clock.t

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var holdingDuring time.Time
	h.gate.sleep = func(ctx context.Context, _ <-chan struct{}, _ <-chan struct{}, d time.Duration) error {
		holdingDuring = h.gate.Record().HoldingSince
		cancel()

		return ctx.Err()
	}
	if err := h.gate.WaitForLag(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitForLag() = %v, want the cancelled context", err)
	}
	if !holdingDuring.Equal(start) {
		t.Errorf("holding since %v during the hold, want %v, when the first server failed", holdingDuring, start)
	}

	rec := h.gate.Record()
	if !rec.HoldingSince.IsZero() || !rec.LastPass.IsZero() {
		t.Errorf("after the call: holding since %v, last pass %v, want neither: the call ended in a cancel with no pass", rec.HoldingSince, rec.LastPass)
	}
	lag := func(n int64) *int64 { return &n }
	for i, want := range []ServerVerdict{
		{Server: "replica1:3306", Healthy: true, Lag: lag(0)},
		{Server: "replica2:3306", Cause: "unhealthy", Lag: lag(40)},
		{Server: "replica3:3306", Cause: "unreachable"},
		{Server: "replica4:3306", Cause: "unreadable"},
		{Server: "replica5:3306", Cause: "unhealthy"},
		{Server: "replica6:3306", Cause: "unhealthy", Lag: lag(12)},
		{Server: "replica7:3306", Healthy: true, Lag: lag(2)},
	} {
		if i >= len(rec.Servers) {
			t.Fatalf("record has %d servers, want 7", len(rec.Servers))
		}
		got := rec.Servers[i]
		if got.Server != want.Server || got.Healthy != want.Healthy || got.Cause != want.Cause {
			t.Errorf("server %d: %s healthy=%v cause=%q, want %s healthy=%v cause=%q", i+1, got.Server, got.Healthy, got.Cause, want.Server, want.Healthy, want.Cause)
		}
		if (got.Lag == nil) != (want.Lag == nil) || got.Lag != nil && *got.Lag != *want.Lag {
			t.Errorf("server %d: lag %v, want %v", i+1, deref(got.Lag), deref(want.Lag))
		}
		if got.Tolerance != 10 || !got.CheckedAt.Equal(start) {
			t.Errorf("server %d: tolerance %d checked at %v, want 10 at %v", i+1, got.Tolerance, got.CheckedAt, start)
		}
		if want.Healthy != (got.Detail == "") {
			t.Errorf("server %d: detail %q, want one exactly when the server is not healthy", i+1, got.Detail)
		}
	}
	for i, sub := range map[int]string{1: "lag=40s tolerance=10s", 2: "connection refused", 3: "malformed packet", 4: "lag=NULL"} {
		if !strings.Contains(rec.Servers[i].Detail, sub) {
			t.Errorf("server %d detail = %q, want it to contain %q", i+1, rec.Servers[i].Detail, sub)
		}
	}
}

func deref(p *int64) any {
	if p == nil {
		return nil
	}

	return *p
}

// TestGateVerdictRecordPass: a pass is recorded with its time, and a later hold with the time it
// began; a server not yet probed is listed with a zero time.
func TestGateVerdictRecordPass(t *testing.T) {
	r := &fakeReader{responses: []readerResult{okRes(healthySnapshot())}}
	h := newGateHarness(t, 0, r)

	before := h.gate.Record()
	if len(before.Servers) != 1 || before.Servers[0].Server != "replica1:3306" || !before.Servers[0].CheckedAt.IsZero() {
		t.Errorf("record before any probe = %+v, want the server listed and never checked", before.Servers)
	}

	h.tick(time.Hour)
	passAt := h.clock.t
	if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	rec := h.gate.Record()
	if !rec.LastPass.Equal(passAt) || !rec.Servers[0].Healthy || !rec.Servers[0].CheckedAt.Equal(passAt) {
		t.Errorf("record after a pass = %+v, want the pass and the check at %v", rec, passAt)
	}
	if !rec.HoldingSince.IsZero() {
		t.Errorf("holding since %v after a pass, want not holding", rec.HoldingSince)
	}

	if rec := (&Gate{enabled: false}).Record(); len(rec.Servers) != 0 {
		t.Errorf("a disabled gate's record = %+v, want no servers", rec)
	}
}

// TestGateRecordMakesNoProbe (spec §7 row 64): reading the record makes no probe: not before a
// call, not during a hold, not after; and what it returns is a copy.
func TestGateRecordMakesNoProbe(t *testing.T) {
	r := &fakeReader{responses: []readerResult{okRes(stoppedSnapshot()), okRes(healthySnapshot())}}
	h := newGateHarness(t, time.Minute, r)

	h.gate.Record()
	if r.calls != 0 {
		t.Fatalf("a record read before any call made %d probes, want 0", r.calls)
	}
	h.gate.sleep = func(ctx context.Context, _ <-chan struct{}, _ <-chan struct{}, d time.Duration) error {
		for range 3 {
			h.gate.Record()
		}
		if r.calls != 1 {
			t.Errorf("reads during the hold = %d after three record reads, want 1: the round's own", r.calls)
		}
		h.clock.t = h.clock.t.Add(d)

		return ctx.Err()
	}
	if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		h.gate.Record()
	}
	if r.calls != 2 {
		t.Errorf("reads = %d, want 2: the two rounds of the call and nothing for the record", r.calls)
	}

	rec := h.gate.Record()
	rec.Servers[0].Cause = "tampered"
	rec.Servers = append(rec.Servers, ServerVerdict{Server: "extra"})
	if again := h.gate.Record(); len(again.Servers) != 1 || again.Servers[0].Cause != "" {
		t.Errorf("record = %+v after a caller changed its copy, want the gate's own record unchanged", again.Servers)
	}
}

// TestGateRecordIsRaceFree: a status read copies the record under the gate's mutex while a hold
// writes it; asserted by the race detector the unit checks run under.
func TestGateRecordIsRaceFree(t *testing.T) {
	r := &fakeReader{responses: []readerResult{okRes(stoppedSnapshot()), okRes(stoppedSnapshot()), okRes(stoppedSnapshot()), okRes(healthySnapshot())}}
	h := newGateHarness(t, 0, r)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				_ = h.gate.Record()
			}
		}
	}()
	err := h.gate.WaitForLag(context.Background(), nil)
	close(done)
	wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
}

// TestGateReportsRoundsToTheStore: the gate tells the store of each completed round, the one
// input of D15's one step per round: a round that passes, one that fails (it counts as
// completed, and bypasses nothing), and one judged under the revision that is current. A cached
// pass is not a round.
func TestGateReportsRoundsToTheStore(t *testing.T) {
	raise := func(h *gateHarness, to string) error {
		_, err := h.store.Set(tuning.SecondsBehindSourceWithin, to, gatePeer)

		return err
	}
	step := func(err error) bool {
		var r *tuning.Refusal

		return errors.As(err, &r) && r.Reason == tuning.ReasonStep
	}

	t.Run("a passing round", func(t *testing.T) {
		r := &fakeReader{responses: []readerResult{okRes(healthySnapshot())}}
		h := newGateHarness(t, time.Minute, r)
		if err := raise(h, "20"); !step(err) {
			t.Fatalf("a raise before the gate's first round = %v, want a step refusal", err)
		}
		if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
		if err := raise(h, "20"); err != nil {
			t.Errorf("a raise after a round under the current revision = %v, want accepted", err)
		}
		// The accepted raise is a new gate revision; the cached pass cannot supply the next
		// round, and no cached pass is a round: the next raise waits for a real one.
		if err := raise(h, "30"); !step(err) {
			t.Errorf("a second raise with no round since = %v, want a step refusal", err)
		}
		if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
		if err := raise(h, "30"); err != nil {
			t.Errorf("a raise after the call probed under the new revision = %v, want accepted", err)
		}
	})

	t.Run("a failing round", func(t *testing.T) {
		r := &fakeReader{responses: []readerResult{okRes(stoppedSnapshot())}}
		h := newGateHarness(t, 0, r)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		h.gate.sleep = func(ctx context.Context, _ <-chan struct{}, _ <-chan struct{}, _ time.Duration) error {
			cancel()

			return ctx.Err()
		}
		if err := h.gate.WaitForLag(ctx, nil); !errors.Is(err, context.Canceled) {
			t.Fatalf("WaitForLag() = %v, want the cancelled context", err)
		}
		if err := raise(h, "20"); err != nil {
			t.Errorf("a raise after a round that failed under the current revision = %v, want accepted: an unhealthy round counts as completed", err)
		}
	})

	t.Run("a discarded pass is a round of the older revision", func(t *testing.T) {
		var h *gateHarness
		r := &fakeReader{responses: []readerResult{
			{snapshot: healthySnapshot(), before: func() { setGate(t, h.store, tuning.CheckInterval, "4") }},
			{snapshot: healthySnapshot(), before: func() {
				if err := raise(h, "20"); !step(err) {
					t.Errorf("a raise while the only round completed was judged under the replaced revision = %v, want a step refusal", err)
				}
			}},
		}}
		h = newGateHarness(t, 0, r)
		if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
		if err := raise(h, "20"); err != nil {
			t.Errorf("a raise after the repeat round under the current revision = %v, want accepted", err)
		}
	})
}

// TestGateNewTakesTheStore: the gate built with a store reads its tolerance and interval from it,
// and one built without gets a store that never changes, at the configured values. A store that
// starts anywhere but at the configuration is refused, as the engine refuses one for the
// processing values.
func TestGateNewTakesTheStore(t *testing.T) {
	log, _ := newTestLogger(t)
	cfg := config.ReplicationConfig{
		Enabled: true, SecondsBehindSourceWithin: 10, CheckInterval: 5, CacheTTL: 15,
		Servers: []config.ReplicationServerConfig{{Host: "10.0.0.1", Port: 3306}},
	}
	store := func(tolerance, interval int) *tuning.Store {
		return tuning.New(tuning.Options{
			Start:   tuning.Settings{BatchSize: 1000, BatchDeleteSize: 500, SecondsBehindSourceWithin: tolerance, CheckInterval: interval},
			Applies: tuning.Applicability{Delete: true, Gate: true},
		})
	}

	own := store(10, 5)
	g, err := New(cfg, []*sql.DB{openIdleDB(t)}, log, own)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	if g.store != own {
		t.Error("the gate did not take the store it was given")
	}

	for _, bad := range []*tuning.Store{store(11, 5), store(10, 6)} {
		if _, err := New(cfg, []*sql.DB{openIdleDB(t)}, log, bad); err == nil {
			t.Errorf("New() accepted a store starting at %+v, want a refusal: the configuration says tolerance 10 and interval 5", bad.Start())
		}
	}

	g, err = New(cfg, []*sql.DB{openIdleDB(t)}, log, nil)
	if err != nil {
		t.Fatalf("New() without a store = %v", err)
	}
	if got := g.store.Start(); got.SecondsBehindSourceWithin != 10 || got.CheckInterval != 5 {
		t.Errorf("a constant store starts at %+v, want the configured tolerance 10 and interval 5", got)
	}
}
