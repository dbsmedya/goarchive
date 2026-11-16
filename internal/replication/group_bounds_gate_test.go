package replication

import (
	"context"
	"database/sql"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// The gate under the two group bounds as runtime settings (GR C7s; spec §4.3 *Rounds and revisions* and *The two
// bounds as runtime settings*, §8). A round reads both bounds from its snapshot with the tolerance, so every group
// server of a round is judged by one pair; the bounds share the tolerance's gate revision, so a pass cached or
// reached under an older pair never stands; and a verdict names the pair it was judged by. Deterministic like the
// rest of the package: scripted readers, the fake clock, no goroutine decides an outcome.

// queueOf is a healthy view whose third member has the given queues.
func queueOf(applier, certifier uint64) groupResult {
	res := okGroup()
	res.stats[2].TransactionsRemoteInApplierQueue = applier
	res.stats[2].TransactionsInQueue = certifier
	return res
}

var boundIn = regexp.MustCompile(`above the bound (\d+)`)

// boundsOf returns the bound each hold line was judged by, in order; a line with none gives "".
func boundsOf(warns []string) []string {
	var out []string
	for _, w := range warns {
		m := boundIn.FindStringSubmatch(w)
		if m == nil {
			out = append(out, "")
			continue
		}
		out = append(out, m[1])
	}
	return out
}

func holdsOnly(h *gateHarness) []string {
	var out []string
	for _, w := range msgsAt(h.lines(), "warn") {
		if strings.HasPrefix(w, "replication hold:") {
			out = append(out, w)
		}
	}
	return out
}

// TestARoundJudgesEveryGroupServerByTheSnapshotsBounds: with a bound lowered before the call, every group server of
// the round is held at the new value, and the line names it; the other bound is the configured one.
func TestARoundJudgesEveryGroupServerByTheSnapshotsBounds(t *testing.T) {
	for _, tc := range []struct {
		name    string
		key     tuning.Key
		lowered string
		queue   groupResult
		check   string
	}{
		{"applier", tuning.GroupApplierQueueWithin, "100", queueOf(300, 0), "GROUP_REPLICATION_APPLIER_QUEUE_WITHIN"},
		{"certifier", tuning.GroupCertifierQueueWithin, "200", queueOf(0, 400), "GROUP_REPLICATION_CERTIFIER_QUEUE_WITHIN"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r1 := &fakeGroupReader{responses: []groupResult{tc.queue, okGroup()}}
			r2 := &fakeGroupReader{responses: []groupResult{tc.queue, okGroup()}}
			h := newFleetHarness(t, 0, 10, 5, groupOnly(r1), groupOnly(r2))
			setGate(t, h.store, tc.key, tc.lowered)
			recordSleeps(h)

			if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
				t.Fatalf("WaitForLag = %v, want a pass once the queues are within the bound", err)
			}
			holds := holdsOnly(h)
			if len(holds) != 2 || !strings.Contains(holds[0], "server gr1:3306") || !strings.Contains(holds[1], "server gr2:3306") {
				t.Fatalf("hold lines = %#v, want one per group server, both held by the lowered bound", holds)
			}
			for i, got := range boundsOf(holds) {
				if got != tc.lowered || !strings.Contains(holds[i], tc.check) {
					t.Errorf("hold %d = %q, want %s at the bound %s", i, holds[i], tc.check, tc.lowered)
				}
			}
		})
	}
}

// TestABoundLoweredWhileARoundsReadsAreInFlight (spec §8): a round starts at bound 1000; while its reads are in
// flight the bound is lowered to 100; the reads return a queue of 300, between the two. The round passed at 1000, is
// discarded, nothing is recorded as a pass and the next round runs at once and holds at 100.
func TestABoundLoweredWhileARoundsReadsAreInFlight(t *testing.T) {
	var h *gateHarness
	var passAtRound2 time.Time
	first := queueOf(300, 0)
	first.before = func() { setGate(t, h.store, tuning.GroupApplierQueueWithin, "100") }
	second := queueOf(300, 0)
	second.before = func() { passAtRound2 = h.gate.Record().LastPass }
	r := &fakeGroupReader{responses: []groupResult{first, second, queueOf(50, 0)}}
	h = newFleetHarness(t, time.Minute, 10, 5, groupOnly(r))
	slept := recordSleeps(h)

	if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
		t.Fatalf("WaitForLag = %v, want a pass once the queue is under the lowered bound", err)
	}
	if r.probes != 3 {
		t.Errorf("probes = %d, want 3: the discarded pass, the hold at 100, the pass", r.probes)
	}
	if !slices.Equal(*slept, []time.Duration{5 * time.Second}) {
		t.Errorf("waits = %v, want one interval, after the failing round only: the discarded pass starts the next round at once", *slept)
	}
	if !passAtRound2.IsZero() {
		t.Errorf("a pass was recorded at %v before the second round: a pass under a replaced pair of bounds must record nothing", passAtRound2)
	}
	if got := boundsOf(holdsOnly(h)); !slices.Equal(got, []string{"100"}) {
		t.Errorf("holds at bounds %v, want one, at the lowered 100 and none at 1000", got)
	}
	if h.gate.Record().LastPass.IsZero() {
		t.Error("the final pass, under the current revision, was not recorded")
	}
}

// TestAPassCachedUnderAnOlderBoundNeverHits: a passing verdict is reused for the cache TTL, but only under the gate
// revision it was judged under: a change of a bound makes the next call probe again, and a change of a sleep does
// not.
func TestAPassCachedUnderAnOlderBoundNeverHits(t *testing.T) {
	r := &fakeGroupReader{responses: []groupResult{okGroup()}}
	h := newFleetHarness(t, time.Minute, 10, 5, groupOnly(r))
	call := func(wantProbes int, why string) {
		t.Helper()
		if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
			t.Fatalf("WaitForLag = %v", err)
		}
		if r.probes != wantProbes {
			t.Fatalf("%s: probes = %d, want %d", why, r.probes, wantProbes)
		}
	}
	call(1, "the first call probes")
	call(1, "a second call inside the TTL is a cache hit")

	rev := h.store.Load().GateRevision
	setGate(t, h.store, tuning.SleepSeconds, "2")
	call(1, "a change of a sleep leaves the pass valid")
	if got := h.store.Load().GateRevision; got != rev {
		t.Errorf("a sleep moved the gate revision: %d, was %d", got, rev)
	}

	setGate(t, h.store, tuning.GroupApplierQueueWithin, "900")
	if got := h.store.Load().GateRevision; got != rev+1 {
		t.Fatalf("a bound moved the gate revision to %d, want %d", got, rev+1)
	}
	call(2, "a pass cached under the older bound never hits")
	call(2, "the new pass is cached")

	setGate(t, h.store, tuning.GroupCertifierQueueWithin, "800")
	call(3, "the other bound moves the same revision")
}

// TestARaiseDuringAHoldEndsTheWaitAndIsJudgedAtOnce: a queue of 1500 holds the job at the configured bound 1000. The
// failing round counts as a probe round completed, so during the wait the operator's raise to 2000, one step above the
// configured value, is accepted; it moves the gate revision, so the wait ends at once and the next round is judged by
// the new bound, with no interval slept for the old one.
func TestARaiseDuringAHoldEndsTheWaitAndIsJudgedAtOnce(t *testing.T) {
	r := &fakeGroupReader{responses: []groupResult{queueOf(1500, 0), queueOf(1500, 0)}}
	h := newFleetHarness(t, 0, 10, 5, groupOnly(r))
	var slept []time.Duration
	h.gate.sleep = func(ctx context.Context, _ <-chan struct{}, _ <-chan struct{}, d time.Duration) error {
		slept = append(slept, d)
		setGate(t, h.store, tuning.GroupApplierQueueWithin, "2000") // accepted: a round completed under this revision
		return ctx.Err()
	}

	if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
		t.Fatalf("WaitForLag = %v, want a pass at the raised bound", err)
	}
	if r.probes != 2 {
		t.Errorf("probes = %d, want 2: the hold at 1000 and the pass at 2000", r.probes)
	}
	if len(slept) != 1 {
		t.Errorf("sleeps = %v, want the one wait the raise ended; no interval is slept for the old bound", slept)
	}
	if got := boundsOf(holdsOnly(h)); !slices.Equal(got, []string{"1000"}) {
		t.Errorf("holds at bounds %v, want one at 1000", got)
	}
	if n := countWith(msgsAt(h.lines(), "info"), "gate settings changed: the fleet is judged again at once"); n != 1 {
		t.Errorf("info lines = %#v, want one line saying that the fleet is judged again at once", msgsAt(h.lines(), "info"))
	}
	v := h.gate.Record().Servers[0]
	if !v.Healthy || v.ApplierQueueWithin == nil || *v.ApplierQueueWithin != 2000 {
		t.Errorf("verdict = %+v, want a pass judged by the raised bound", v)
	}
}

// TestARoundIsJudgedByOnePairOfBounds: a bound lowered while the first server of a round is being probed does not
// reach the second server of that round: both are judged by the snapshot's pair, and both verdicts name it. The
// next round, which the change made necessary, holds both at the new value.
func TestARoundIsJudgedByOnePairOfBounds(t *testing.T) {
	var h *gateHarness
	var afterRound1 []time.Time
	var named [][2]int64
	first := queueOf(300, 0)
	first.before = func() { setGate(t, h.store, tuning.GroupApplierQueueWithin, "100") }
	second := queueOf(300, 0)
	second.before = func() {
		rec := h.gate.Record()
		for _, v := range rec.Servers {
			afterRound1 = append(afterRound1, v.CheckedAt)
			if v.ApplierQueueWithin != nil && v.CertifierQueueWithin != nil {
				named = append(named, [2]int64{*v.ApplierQueueWithin, *v.CertifierQueueWithin})
			}
		}
	}
	r1 := &fakeGroupReader{responses: []groupResult{first, second, queueOf(50, 0)}}
	r2 := &fakeGroupReader{responses: []groupResult{queueOf(300, 0), queueOf(300, 0), queueOf(50, 0)}}
	h = newFleetHarness(t, 0, 10, 5, groupOnly(r1), groupOnly(r2))
	recordSleeps(h)

	if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
		t.Fatalf("WaitForLag = %v", err)
	}
	if want := [][2]int64{{1000, 1000}, {1000, 1000}}; !slices.Equal(named, want) {
		t.Errorf("the verdicts of round 1 name the pairs %v, want both servers judged by the snapshot's 1000 and 1000", named)
	}
	for i, at := range afterRound1 {
		if at.IsZero() {
			t.Errorf("server %d had no verdict after round 1", i)
		}
	}
	if got := boundsOf(holdsOnly(h)); !slices.Equal(got, []string{"100", "100"}) {
		t.Errorf("holds at bounds %v, want both servers held once, by the lowered 100, in the next round", got)
	}
}

// TestAVerdictNamesTheBoundsItWasJudgedBy: a group server's verdict carries the two bounds its last probe was judged
// by, which differ from the values in force between a change and the next probe; both are nil for an asynchronous
// server and for a group server that has not been probed, and a read that failed was judged by a pair too. A reader of
// the record holds nothing of the gate's.
func TestAVerdictNamesTheBoundsItWasJudgedBy(t *testing.T) {
	g := &fakeGroupReader{responses: []groupResult{okGroup(), {membersErr: context.DeadlineExceeded}}}
	a := &fakeReader{responses: []readerResult{okRes(healthySnapshot())}}
	h := newFleetHarness(t, 0, 10, 5, groupOnly(g), asyncOnly(a))

	for i, v := range h.gate.Record().Servers {
		if v.ApplierQueueWithin != nil || v.CertifierQueueWithin != nil {
			t.Errorf("server %d, not probed: bounds %v and %v, want nil", i, v.ApplierQueueWithin, v.CertifierQueueWithin)
		}
	}
	if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
		t.Fatalf("WaitForLag = %v", err)
	}
	rec := h.gate.Record()
	v := rec.Servers[0]
	if v.ApplierQueueWithin == nil || *v.ApplierQueueWithin != 1000 || v.CertifierQueueWithin == nil || *v.CertifierQueueWithin != 1000 {
		t.Fatalf("group verdict bounds = %v and %v, want 1000 and 1000", v.ApplierQueueWithin, v.CertifierQueueWithin)
	}
	if as := rec.Servers[1]; as.ApplierQueueWithin != nil || as.CertifierQueueWithin != nil {
		t.Errorf("asynchronous verdict bounds = %v and %v, want nil", as.ApplierQueueWithin, as.CertifierQueueWithin)
	}

	// The setting changes after the probe: the verdict is older than the setting and still says 1000.
	setGate(t, h.store, tuning.GroupApplierQueueWithin, "700")
	setGate(t, h.store, tuning.GroupCertifierQueueWithin, "250")
	if got := h.gate.Record().Servers[0]; *got.ApplierQueueWithin != 1000 || *got.CertifierQueueWithin != 1000 {
		t.Errorf("verdict bounds after a change = %d and %d, want the 1000 and 1000 it was judged by", *got.ApplierQueueWithin, *got.CertifierQueueWithin)
	}

	// A caller that changes what it was given changes nothing of the gate's.
	*v.ApplierQueueWithin = 1
	if got := h.gate.Record().Servers[0]; *got.ApplierQueueWithin != 1000 {
		t.Errorf("the record's bound = %d after a reader changed its copy, want 1000", *got.ApplierQueueWithin)
	}

	// The next probe judges by the pair now in force, and so does one whose read fails.
	ctx, cancel := context.WithCancel(context.Background())
	h.gate.sleep = func(context.Context, <-chan struct{}, <-chan struct{}, time.Duration) error { cancel(); return nil }
	_ = h.gate.WaitForLag(ctx, nil)
	got := h.gate.Record().Servers[0]
	if got.Healthy || got.ApplierQueueWithin == nil || *got.ApplierQueueWithin != 700 || *got.CertifierQueueWithin != 250 {
		t.Errorf("verdict after a failed read = %+v, want the new pair 700 and 250", got)
	}
}

// TestGateNewChecksTheGroupBoundsOfAStore: a store that starts anywhere but at the configured bounds is refused, as it
// is for the tolerance and the interval, because the configured value is what a raise is measured by; with no group
// server the bounds mean nothing and are not compared.
func TestGateNewChecksTheGroupBoundsOfAStore(t *testing.T) {
	log, _ := newTestLogger(t)
	applier, certifier := 1000, 300
	cfg := config.ReplicationConfig{
		Enabled: true, SecondsBehindSourceWithin: 10, CheckInterval: 5, CacheTTL: 15,
		GroupApplierQueueWithin: &applier, GroupCertifierQueueWithin: &certifier,
		Servers: []config.ReplicationServerConfig{{Host: "gr2", Port: 3306, Type: "group"}},
	}
	store := func(a, c int) *tuning.Store {
		return tuning.New(tuning.Options{
			Start: tuning.Settings{
				BatchSize: 1000, BatchDeleteSize: 500, SecondsBehindSourceWithin: 10, CheckInterval: 5,
				GroupApplierQueueWithin: a, GroupCertifierQueueWithin: c,
			},
			Applies: tuning.Applicability{Delete: true, Gate: true, Group: true},
		})
	}

	own := store(1000, 300)
	g, err := New(cfg, []*sql.DB{openIdleDB(t)}, log, own)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	if g.store != own {
		t.Error("the gate did not take the store it was given")
	}
	for _, tc := range []struct {
		bad  *tuning.Store
		name string
	}{{store(999, 300), "group_applier_queue_within"}, {store(1000, 301), "group_certifier_queue_within"}} {
		if _, err := New(cfg, []*sql.DB{openIdleDB(t)}, log, tc.bad); err == nil || !strings.Contains(err.Error(), tc.name) {
			t.Errorf("New() with a store starting at %+v = %v, want a refusal naming %s", tc.bad.Start(), err, tc.name)
		}
	}

	async := cfg
	async.Servers = []config.ReplicationServerConfig{{Host: "10.0.0.1", Port: 3306}}
	if _, err := New(async, []*sql.DB{openIdleDB(t)}, log, store(1, 2)); err != nil {
		t.Errorf("New() with no group server and other bounds in the store = %v, want it accepted: the bounds mean nothing there", err)
	}
}
