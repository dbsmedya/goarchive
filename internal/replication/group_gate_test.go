package replication

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
	mysql "github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// The gate's group servers (RG-1; spec §4.3, §8; GR C6): the probe is two reads, the judgement is evaluateGroup,
// and the rest of the gate is the one it has: rounds, snapshots, revisions, the verdict record, LAG-1's stop rules.
// Deterministic like the rest of this package: scripted readers, the fake clock, no goroutine decides an outcome.

// groupResult is one scripted probe of a group server: the members read, then the stats read. before runs before
// the members read; block makes a read wait for its context, as a probe of a host that is down does.
type groupResult struct {
	members      []dbsrepl.GroupMember
	membersErr   error
	stats        []dbsrepl.GroupMemberStats
	statsErr     error
	before       func()
	blockMembers bool
	blockStats   bool
}

func okGroup() groupResult {
	m, s := healthyView()
	return groupResult{members: m, stats: s}
}

// fakeGroupReader pops one scripted probe per members read and answers the stats read of the same probe; the last
// response repeats. reads records "members" and "stats" in order; cut counts the reads whose context ended under them.
type fakeGroupReader struct {
	responses []groupResult
	probes    int
	reads     []string
	cut       int
}

func (r *fakeGroupReader) current() groupResult {
	i := r.probes - 1
	if i >= len(r.responses) {
		i = len(r.responses) - 1
	}
	return r.responses[i]
}

func (r *fakeGroupReader) GroupMembers(ctx context.Context) ([]dbsrepl.GroupMember, error) {
	r.probes++
	r.reads = append(r.reads, "members")
	res := r.current()
	if res.before != nil {
		res.before()
	}
	if res.blockMembers {
		<-ctx.Done()
		r.cut++

		return nil, ctx.Err()
	}

	return res.members, res.membersErr
}

func (r *fakeGroupReader) GroupMemberStats(ctx context.Context) ([]dbsrepl.GroupMemberStats, error) {
	r.reads = append(r.reads, "stats")
	res := r.current()
	if res.blockStats {
		<-ctx.Done()
		r.cut++

		return nil, ctx.Err()
	}

	return res.stats, res.statsErr
}

// fleetMember is one server of a harness fleet: an asynchronous replica or a group server.
type fleetMember struct {
	async *fakeReader
	group *fakeGroupReader
}

// newFleetHarness is newStoreHarness over a fleet of both kinds. Group servers are bound at 1000 and 1000 and the
// gate has been told that the source is the member srcID, unless a test says otherwise.
func newFleetHarness(t *testing.T, ttl time.Duration, tolerance, interval int, fleet ...fleetMember) *gateHarness {
	t.Helper()
	h := newStoreHarness(t, ttl, tolerance, interval)
	var servers []server
	for i, m := range fleet {
		if m.group != nil {
			servers = append(servers, server{id: fmt.Sprintf("gr%d:3306", i+1), group: &groupProbe{reader: m.group, applierWithin: 1000, certifierWithin: 1000}})
		} else {
			servers = append(servers, server{id: fmt.Sprintf("replica%d:3306", i+1), reader: m.async})
		}
	}
	h.gate.servers = servers
	h.gate.SetSource(srcID)

	return h
}

func groupOnly(r *fakeGroupReader) fleetMember { return fleetMember{group: r} }
func asyncOnly(r *fakeReader) fleetMember      { return fleetMember{async: r} }

func over(applier uint64) groupResult {
	res := okGroup()
	res.stats[2].TransactionsRemoteInApplierQueue = applier
	return res
}

// TestAGroupProbeIsTwoReadsAndRecordsItsQueues: members then stats, on a pass; the verdict is the group kind's:
// the type, no lag, the two largest queues of the view, and the round's tolerance.
func TestAGroupProbeIsTwoReadsAndRecordsItsQueues(t *testing.T) {
	res := okGroup()
	res.stats[0].TransactionsRemoteInApplierQueue, res.stats[1].TransactionsRemoteInApplierQueue = 7, 12
	res.stats[2].TransactionsInQueue = 3
	r := &fakeGroupReader{responses: []groupResult{res}}
	h := newFleetHarness(t, 0, 10, 5, groupOnly(r))

	if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
		t.Fatalf("WaitForLag = %v, want a pass", err)
	}
	if want := []string{"members", "stats"}; !slices.Equal(r.reads, want) {
		t.Errorf("reads = %v, want %v", r.reads, want)
	}
	v := h.gate.Record().Servers[0]
	if v.Type != "group" || !v.Healthy || v.Lag != nil || v.Tolerance != 10 {
		t.Errorf("verdict = %+v, want a healthy group server with no lag, judged at the round's tolerance 10", v)
	}
	if v.ApplierQueue == nil || *v.ApplierQueue != 12 || v.CertifierQueue == nil || *v.CertifierQueue != 3 {
		t.Errorf("queues = %v and %v, want the largest of the view, 12 and 3", v.ApplierQueue, v.CertifierQueue)
	}
}

// TestAFailedMembersReadSkipsTheStatsRead and the classification of every failed read: as for a replica, an
// error of the network or a connection the driver gave up on is unreachable, and anything else is unreadable.
func TestAFailedReadIsClassifiedAndTheSecondIsSkipped(t *testing.T) {
	netErr := &net.OpError{Op: "dial", Err: errors.New("connection refused")}
	denied := &mysql.MySQLError{Number: 1227, Message: "Access denied"}
	for _, tc := range []struct {
		name  string
		res   groupResult
		cause string
		reads []string
	}{
		{"members: unreachable", groupResult{membersErr: netErr}, "unreachable", []string{"members"}},
		{"members: unreadable", groupResult{membersErr: denied}, "unreadable", []string{"members"}},
		{"members: an error nobody knows", groupResult{membersErr: errors.New("something new")}, "unreadable", []string{"members"}},
		{"stats: unreachable", groupResult{members: okGroup().members, statsErr: netErr}, "unreachable", []string{"members", "stats"}},
		{"stats: unreadable", groupResult{members: okGroup().members, statsErr: denied}, "unreadable", []string{"members", "stats"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &fakeGroupReader{responses: []groupResult{tc.res, okGroup()}}
			h := newFleetHarness(t, 0, 10, 5, groupOnly(r))
			if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
				t.Fatalf("WaitForLag = %v, want a pass once the read works again", err)
			}
			if !slices.Equal(r.reads[:len(tc.reads)], tc.reads) {
				t.Errorf("the failing probe's reads = %v, want %v", r.reads[:len(tc.reads)], tc.reads)
			}
			if !hasMsg(h.lines(), "warn", "") && countWith(msgsAt(h.lines(), "warn"), "replication hold: server gr1:3306 "+tc.cause+" (") != 1 {
				t.Errorf("warn lines = %#v, want one hold line with cause %s", msgsAt(h.lines(), "warn"), tc.cause)
			}
		})
	}

	// the failed probe's verdict carries no queues, even if the first read succeeded
	r := &fakeGroupReader{responses: []groupResult{{members: okGroup().members, statsErr: netErr}, {members: okGroup().members, statsErr: netErr}}}
	h := newFleetHarness(t, 0, 10, 5, groupOnly(r))
	ctx, cancel := context.WithCancel(context.Background())
	h.gate.sleep = func(context.Context, <-chan struct{}, <-chan struct{}, time.Duration) error { cancel(); return nil }
	_ = h.gate.WaitForLag(ctx, nil)
	v := h.gate.Record().Servers[0]
	if v.Healthy || v.Cause != "unreachable" || v.ApplierQueue != nil || v.CertifierQueue != nil || v.Lag != nil {
		t.Errorf("verdict after a failed stats read = %+v, want unreachable with no queues and no lag", v)
	}
}

// TestAMissingGrantNamesTheTableAndTheGrant: a refusal of SELECT on either table is unreadable, and the detail says
// which table and which grant (spec §4.3).
func TestAMissingGrantNamesTheTableAndTheGrant(t *testing.T) {
	refused := &dbsrepl.OpError{Op: "group_members", Err: &mysql.MySQLError{Number: 1142, SQLState: [5]byte{'4', '2', '0', '0', '0'},
		Message: "SELECT command denied to user 'monitor'@'h' for table 'replication_group_members'"}}
	refusedStats := &dbsrepl.OpError{Op: "group_member_stats", Err: &mysql.MySQLError{Number: 1142, SQLState: [5]byte{'4', '2', '0', '0', '0'},
		Message: "SELECT command denied to user 'monitor'@'h' for table 'replication_group_member_stats'"}}
	for _, tc := range []struct {
		name  string
		res   groupResult
		table string
	}{
		{"the view", groupResult{membersErr: refused}, "performance_schema.replication_group_members"},
		{"the stats", groupResult{members: okGroup().members, statsErr: refusedStats}, "performance_schema.replication_group_member_stats"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &fakeGroupReader{responses: []groupResult{tc.res, okGroup()}}
			h := newFleetHarness(t, 0, 10, 5, groupOnly(r))
			if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
				t.Fatalf("WaitForLag = %v", err)
			}
			warns := msgsAt(h.lines(), "warn")
			want := "replication hold: server gr1:3306 unreadable (the account that monitors this server cannot read " + tc.table +
				": grant it SELECT on that table (MySQL error 1142)); job held, retrying in 5s"
			if len(warns) == 0 || warns[0] != want {
				t.Errorf("hold line = %#v, want %q", warns, want)
			}
		})
	}
}

// TestAGroupHoldUsesTheExistingLineShape: the library's finding is the detail, and recovery is logged as for a replica.
func TestAGroupHoldUsesTheExistingLineShape(t *testing.T) {
	r := &fakeGroupReader{responses: []groupResult{over(4210), okGroup()}}
	h := newFleetHarness(t, 0, 10, 5, groupOnly(r))
	if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
		t.Fatalf("WaitForLag = %v", err)
	}
	warns := msgsAt(h.lines(), "warn")
	if len(warns) != 1 || !strings.HasPrefix(warns[0], "replication hold: server gr1:3306 unhealthy (GROUP_REPLICATION_APPLIER_QUEUE_WITHIN: ") ||
		!strings.Contains(warns[0], thirdID) || !strings.Contains(warns[0], "4210") || !strings.HasSuffix(warns[0], "); job held, retrying in 5s") {
		t.Errorf("warn lines = %#v, want the existing line shape with the finding naming the member, the queue and the bound", warns)
	}
	if !hasMsg(h.lines(), "info", "replication recovered: server gr1:3306 healthy; held 5s") {
		t.Errorf("info lines = %#v, want the recovery line", msgsAt(h.lines(), "info"))
	}
	v := h.gate.Record().Servers[0]
	if !v.Healthy || v.ApplierQueue == nil || *v.ApplierQueue != 0 {
		t.Errorf("last verdict = %+v, want the pass, with the queue of the pass", v)
	}
}

// TestTheGateIsToldWhetherTheSourceIsAMember (spec §4.3, plan §2): three states. Not told: every group server
// holds, with its own detail, so none is judged without the id. Told that the source is not a member: the source
// rule is not applied. Told its member id: the view must hold it.
func TestTheGateIsToldWhetherTheSourceIsAMember(t *testing.T) {
	t.Run("not told", func(t *testing.T) {
		r := &fakeGroupReader{responses: []groupResult{okGroup(), okGroup()}}
		h := newFleetHarness(t, 0, 10, 5, groupOnly(r))
		h.gate.source = sourceState{} // newFleetHarness told it; this test is about a gate that was not
		told := make(chan struct{})
		h.gate.sleep = func(ctx context.Context, _ <-chan struct{}, _ <-chan struct{}, d time.Duration) error {
			h.tick(d)
			h.gate.SetSource(srcID)
			close(told)
			return ctx.Err()
		}
		if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
			t.Fatalf("WaitForLag = %v, want a pass once the gate is told", err)
		}
		<-told
		warns := msgsAt(h.lines(), "warn")
		if len(warns) != 1 || !strings.Contains(warns[0], "unhealthy (GROUP_SOURCE_NOT_TOLD: ") || !strings.Contains(warns[0], "has not been told whether the source is a member") {
			t.Errorf("warn lines = %#v, want the hold with the not-told detail", warns)
		}
		if r.probes != 2 {
			t.Errorf("probes = %d, want 2: the hold, and the pass after SetSource", r.probes)
		}
	})
	t.Run("told that the source is not a member", func(t *testing.T) {
		m, s := healthyView()
		m, s = m[1:], s[1:] // the view does not hold the source's id, and need not
		r := &fakeGroupReader{responses: []groupResult{{members: m, stats: s}}}
		h := newFleetHarness(t, 0, 10, 5, groupOnly(r))
		h.gate.SetSource("")
		if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
			t.Errorf("WaitForLag = %v, want a pass: the source rule is not applied", err)
		}
	})
	t.Run("told its member id", func(t *testing.T) {
		m, s := healthyView()
		m, s = m[1:], s[1:]
		r := &fakeGroupReader{responses: []groupResult{{members: m, stats: s}, okGroup()}}
		h := newFleetHarness(t, 0, 10, 5, groupOnly(r))
		if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
			t.Fatalf("WaitForLag = %v", err)
		}
		warns := msgsAt(h.lines(), "warn")
		if len(warns) != 1 || !strings.Contains(warns[0], "GROUP_SOURCE_NOT_IN_VIEW") {
			t.Errorf("warn lines = %#v, want one hold for a view without the source", warns)
		}
	})
	t.Run("an asynchronous fleet needs no telling", func(t *testing.T) {
		r := &fakeReader{responses: []readerResult{okRes(healthySnapshot())}}
		h := newGateHarness(t, 0, r)
		if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
			t.Errorf("WaitForLag = %v, want a pass: only a group server needs the source", err)
		}
	})
}

// TestAMixedFleetIsOneRound: an asynchronous replica and a group server are judged in one round under one snapshot.
func TestAMixedFleetIsOneRound(t *testing.T) {
	t.Run("both pass: one pass is cached", func(t *testing.T) {
		a := &fakeReader{responses: []readerResult{okRes(lagSnapshot(3))}}
		g := &fakeGroupReader{responses: []groupResult{okGroup()}}
		h := newFleetHarness(t, 30*time.Second, 10, 5, asyncOnly(a), groupOnly(g))
		for i := range 2 {
			if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
				t.Fatalf("call %d = %v", i+1, err)
			}
		}
		if a.calls != 1 || g.probes != 1 {
			t.Errorf("probes = %d and %d, want one each: the second call is the cached pass", a.calls, g.probes)
		}
		rec := h.gate.Record()
		if rec.Servers[0].Type != "async" || rec.Servers[0].Lag == nil || *rec.Servers[0].Lag != 3 || rec.Servers[0].ApplierQueue != nil || rec.Servers[0].CertifierQueue != nil {
			t.Errorf("the asynchronous verdict = %+v, want type async, lag 3 and no queue", rec.Servers[0])
		}
		if rec.Servers[1].Type != "group" || rec.Servers[1].Lag != nil || rec.Servers[1].ApplierQueue == nil || rec.Servers[1].CertifierQueue == nil {
			t.Errorf("the group verdict = %+v, want type group, no lag and both queues", rec.Servers[1])
		}
		if rec.Servers[0].Tolerance != 10 || rec.Servers[1].Tolerance != 10 {
			t.Errorf("tolerances = %d and %d, want the round's 10 for both", rec.Servers[0].Tolerance, rec.Servers[1].Tolerance)
		}
	})
	t.Run("the group server fails: it alone holds", func(t *testing.T) {
		a := &fakeReader{responses: []readerResult{okRes(healthySnapshot())}}
		g := &fakeGroupReader{responses: []groupResult{over(5000), okGroup()}}
		h := newFleetHarness(t, 0, 10, 5, asyncOnly(a), groupOnly(g))
		if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
		warns := msgsAt(h.lines(), "warn")
		if countWith(warns, "replication hold: server gr2:3306 unhealthy") != 1 || countWith(warns, "replica1") != 0 {
			t.Errorf("warn lines = %#v, want the group server's hold line only: the healthy replica stays silent", warns)
		}
	})
	t.Run("the replica fails: it alone holds", func(t *testing.T) {
		a := &fakeReader{responses: []readerResult{okRes(stoppedSnapshot()), okRes(healthySnapshot())}}
		g := &fakeGroupReader{responses: []groupResult{okGroup()}}
		h := newFleetHarness(t, 0, 10, 5, asyncOnly(a), groupOnly(g))
		if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
		if countWith(msgsAt(h.lines(), "warn"), "replication hold: server replica1:3306 unhealthy") != 1 {
			t.Errorf("warn lines = %#v, want the replica's hold line", msgsAt(h.lines(), "warn"))
		}
	})
	t.Run("a server not yet probed has its type", func(t *testing.T) {
		a := &fakeReader{responses: []readerResult{okRes(healthySnapshot())}}
		g := &fakeGroupReader{responses: []groupResult{okGroup()}}
		h := newFleetHarness(t, 0, 10, 5, asyncOnly(a), groupOnly(g))
		rec := h.gate.Record()
		if rec.Servers[0].Type != "async" || rec.Servers[1].Type != "group" || !rec.Servers[0].CheckedAt.IsZero() || !rec.Servers[1].CheckedAt.IsZero() {
			t.Errorf("record before a probe = %+v, want each server with its type and no check time", rec.Servers)
		}
	})
}

// The existing revision tests (TEL-1), run with a mixed fleet: a group server that is always healthy beside the
// replica the original test scripts. The judgement of the replica and every claim about the pass are the same.
func TestMixedFleetCachedPassRevision(t *testing.T) {
	r := &fakeReader{responses: []readerResult{okRes(healthySnapshot())}}
	g := &fakeGroupReader{responses: []groupResult{okGroup()}}
	h := newFleetHarness(t, 30*time.Second, 10, 5, asyncOnly(r), groupOnly(g))
	call := func(wantReads int, why string) {
		t.Helper()
		if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
			t.Fatalf("WaitForLag() = %v", err)
		}
		if r.calls != wantReads || g.probes != wantReads {
			t.Errorf("%s: reads = %d and %d, want %d each", why, r.calls, g.probes, wantReads)
		}
	}
	call(1, "first call probes both")
	call(1, "a pass under the same revision, within the ttl, is a hit")
	setGate(t, h.store, tuning.CheckInterval, "4")
	call(2, "the next gate revision cannot hit the older pass")
	setGate(t, h.store, tuning.BatchSize, "500")
	call(2, "a change of another setting leaves the pass valid")
	h.tick(31 * time.Second)
	call(3, "past the ttl both are probed again")
	setGate(t, h.store, tuning.SecondsBehindSourceWithin, "5")
	call(4, "a tolerance change is a gate-revision change for a mixed fleet too")
}

func TestMixedFleetInFlightPass(t *testing.T) {
	var h *gateHarness
	var cachedAtRound2 time.Time
	r := &fakeReader{responses: []readerResult{
		{snapshot: lagSnapshot(40), before: func() { setGate(t, h.store, tuning.SecondsBehindSourceWithin, "10") }},
		{snapshot: lagSnapshot(40), before: func() { cachedAtRound2 = h.gate.Record().LastPass }},
		okRes(lagSnapshot(0)),
	}}
	g := &fakeGroupReader{responses: []groupResult{okGroup()}}
	h = newFleetHarness(t, time.Minute, 60, 5, asyncOnly(r), groupOnly(g))
	slept := recordSleeps(h)

	if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
		t.Fatalf("WaitForLag() = %v, want nil once the replica is under the lowered tolerance", err)
	}
	if r.calls != 3 || g.probes != 3 {
		t.Errorf("reads = %d and %d, want 3 rounds of both: the discarded pass, the hold at 10, the pass", r.calls, g.probes)
	}
	if !slices.Equal(*slept, []time.Duration{5 * time.Second}) {
		t.Errorf("waits = %v, want one interval, after the failing round only", *slept)
	}
	if !cachedAtRound2.IsZero() {
		t.Errorf("a pass was cached at round 2 (%v): the discarded pass must record none", cachedAtRound2)
	}
}

func TestMixedFleetPassRevalidatedAfterTheFinalLog(t *testing.T) {
	r := &fakeReader{responses: []readerResult{
		okRes(lagSnapshot(70)), okRes(lagSnapshot(40)), okRes(lagSnapshot(40)), okRes(lagSnapshot(0)),
	}}
	g := &fakeGroupReader{responses: []groupResult{okGroup()}}
	h := newFleetHarness(t, time.Minute, 60, 5, asyncOnly(r), groupOnly(g))
	slept := recordSleeps(h)
	obs := observeGate(t, h)
	fired := false
	obs.on = func(msg string) {
		if !fired && strings.HasPrefix(msg, "replication gate passed:") {
			fired = true
			setGate(t, h.store, tuning.SecondsBehindSourceWithin, "10")
		}
	}

	if err := h.gate.WaitForLag(context.Background(), nil); err != nil {
		t.Fatalf("WaitForLag() = %v", err)
	}
	if !fired || r.calls != 4 {
		t.Fatalf("setter ran %v, replica reads %d, want the setter during the final entry and 4 reads: hold at 70, a pass at 60 that a setter outdated, a hold at 10, a pass", fired, r.calls)
	}
	if !slices.Equal(*slept, []time.Duration{5 * time.Second, 5 * time.Second}) {
		t.Errorf("waits = %v, want the two intervals after the two failing rounds", *slept)
	}
	if countWith(obs.infos(), "replication gate passed: all 2 servers healthy") != 2 {
		t.Errorf("info lines = %#v, want the pass entry twice, each counting both servers", obs.infos())
	}
	rec := h.gate.Record()
	if rec.Servers[0].Tolerance != 10 || !rec.Servers[0].Healthy || !rec.Servers[1].Healthy {
		t.Errorf("the verdicts the call returned on: %+v, want a pass for both, the replica judged at the lowered tolerance", rec.Servers)
	}
	before := r.calls
	if err := h.gate.WaitForLag(context.Background(), nil); err != nil || r.calls != before {
		t.Errorf("a second call = %v after %d more reads, want a cache hit under the current revision", err, r.calls-before)
	}
}

func TestMixedFleetCacheHitRevalidatedAfterTheMutex(t *testing.T) {
	r := &fakeReader{responses: []readerResult{okRes(lagSnapshot(40)), okRes(lagSnapshot(40))}}
	g := &fakeGroupReader{responses: []groupResult{okGroup()}}
	h := newFleetHarness(t, time.Minute, 60, 5, asyncOnly(r), groupOnly(g))
	if err := h.gate.WaitForLag(context.Background(), nil); err != nil || r.calls != 1 {
		t.Fatalf("the first call = %v after %d reads, want a pass after one", err, r.calls)
	}

	stop := make(chan struct{})
	h.gate.mu.Lock()
	done := make(chan error, 1)
	go func() { done <- h.gate.WaitForLag(context.Background(), stop) }()
	waitBlockedOnGateMutex(t)
	setGate(t, h.store, tuning.SecondsBehindSourceWithin, "10")
	h.gate.mu.Unlock()

	close(stop)
	if err := <-done; !errors.Is(err, ErrStopped) {
		t.Errorf("the call returned %v, want it to have judged at tolerance 10 (a hold ended by a stop)", err)
	}
	if r.calls != 2 || g.probes != 2 {
		t.Errorf("reads = %d and %d, want 2 rounds: the call probed both again instead of taking the old pass", r.calls, g.probes)
	}
}

// LAG-1's three stop rules with a group server as the one that fails (spec §4.3, §8).

// A stop never cuts the first probe: both reads of the first probe of a call run on the run's context.
func TestAStopNeverCutsAGroupServersFirstProbe(t *testing.T) {
	stop := make(chan struct{})
	res := okGroup()
	res.before = func() { close(stop) }
	r := &fakeGroupReader{responses: []groupResult{res}}
	h := newFleetHarness(t, 0, 10, 5, groupOnly(r))
	if err := h.gate.WaitForLag(context.Background(), stop); err != nil {
		t.Fatalf("WaitForLag = %v, want a pass: a stop never cuts a pass short", err)
	}
	if r.cut != 0 || len(r.reads) != 2 {
		t.Errorf("reads = %v, cut %d, want both reads and none cut by the stop", r.reads, r.cut)
	}
}

// A stop ends a hold during the wait, without another probe.
func TestAStopEndsAGroupHoldDuringTheWait(t *testing.T) {
	stop := make(chan struct{})
	r := &fakeGroupReader{responses: []groupResult{over(5000), okGroup()}}
	h := newFleetHarness(t, 0, 10, 5, groupOnly(r))
	h.gate.sleep = func(_ context.Context, _ <-chan struct{}, _ <-chan struct{}, d time.Duration) error {
		h.sleepCalls++
		h.tick(d)
		close(stop)
		return nil
	}
	if err := h.gate.WaitForLag(context.Background(), stop); !errors.Is(err, ErrStopped) {
		t.Fatalf("WaitForLag = %v, want ErrStopped", err)
	}
	if r.probes != 1 {
		t.Errorf("probes = %d, want 1: a stop during the sleep ends the hold without probing again", r.probes)
	}
}

// A stop cancels a later probe in either read, at once, not when the read returns.
func TestAStopCancelsAHeldGroupProbeInEitherRead(t *testing.T) {
	for _, tc := range []struct {
		name  string
		block func(res *groupResult)
		reads []string
	}{
		{"the view read", func(res *groupResult) { res.blockMembers = true }, []string{"members", "stats", "members"}},
		{"the stats read", func(res *groupResult) { res.blockStats = true }, []string{"members", "stats", "members", "stats"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stop := make(chan struct{})
			blocked := okGroup()
			tc.block(&blocked)
			blocked.before = func() { close(stop) }
			r := &fakeGroupReader{responses: []groupResult{over(5000), blocked, okGroup()}}
			h := newFleetHarness(t, 0, 10, 5, groupOnly(r))
			done := make(chan error, 1)
			go func() { done <- h.gate.WaitForLag(context.Background(), stop) }()
			var err error
			select {
			case err = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("WaitForLag still blocked 5 s after a stop during a held group probe")
			}
			if !errors.Is(err, ErrStopped) {
				t.Fatalf("WaitForLag = %v, want ErrStopped", err)
			}
			if !slices.Equal(r.reads, tc.reads) || r.cut != 1 {
				t.Errorf("reads = %v, cut %d, want %v with the one blocked read cut", r.reads, r.cut, tc.reads)
			}
			warns := msgsAt(h.lines(), "warn")
			if countWith(warns, "replication hold: server gr1:3306") != 1 || countWith(warns, stopLine) != 1 {
				t.Errorf("warn lines = %#v, want the first probe's hold line and the stop line, and no cause for the aborted probe", warns)
			}
		})
	}
}

// New builds a group server from its configuration: the reader, the two bounds and a startup line.
func TestNewBuildsAGroupServer(t *testing.T) {
	log, lines := newTestLogger(t)
	applier, certifier := 1000, 500
	cfg := config.ReplicationConfig{
		Enabled: true, SecondsBehindSourceWithin: 10, CheckInterval: 5, CacheTTL: 15,
		GroupApplierQueueWithin: &applier, GroupCertifierQueueWithin: &certifier,
		Servers: []config.ReplicationServerConfig{
			{Host: "10.0.0.1", Port: 3306, Type: "async"},
			{Host: "gr2", Port: 3306, Type: "group"},
		},
	}
	g, err := New(cfg, []*sql.DB{openIdleDB(t), openIdleDB(t)}, log, nil)
	if err != nil {
		t.Fatalf("New = %v", err)
	}
	if g.servers[0].group != nil || g.servers[1].group == nil || g.servers[1].reader != nil {
		t.Fatalf("servers = %+v, want an asynchronous first and a group second", g.servers)
	}
	if p := g.servers[1].group; p.applierWithin != 1000 || p.certifierWithin != 500 || p.reader == nil {
		t.Errorf("group probe = %+v, want the two bounds and a reader", p)
	}
	if g.source.told {
		t.Error("a new gate has been told about the source: it must start in the not-told state")
	}
	for _, want := range []string{
		"Replication monitoring ENABLED: 2 server(s), tolerance 10s, interval 5s, cache TTL 15s",
		"  server 10.0.0.1:3306 channels: all",
		"  server gr2:3306 group: applier queue within 1000, certifier queue within 500",
	} {
		if !hasMsg(lines(), "info", want) {
			t.Errorf("info lines = %#v, want %q", msgsAt(lines(), "info"), want)
		}
	}

	cfg.GroupCertifierQueueWithin = nil
	if _, err := New(cfg, []*sql.DB{openIdleDB(t), openIdleDB(t)}, log, nil); err == nil ||
		!strings.Contains(err.Error(), "group_certifier_queue_within") {
		t.Errorf("New with a missing bound = %v, want a construction error naming it", err)
	}
}

// SetSource is the handover: the gate keeps what it was told, a later telling replaces it, and the engine's
// telling is not a probe.
func TestSetSourceKeepsTheLastTelling(t *testing.T) {
	g := &Gate{}
	if g.source.told {
		t.Fatal("a zero gate is told")
	}
	g.SetSource(srcID)
	if s := g.currentSource(); !s.told || !s.member || s.id != srcID {
		t.Errorf("after SetSource(%q): %+v", srcID, s)
	}
	g.SetSource("")
	if s := g.currentSource(); !s.told || s.member || s.id != "" {
		t.Errorf("after SetSource(\"\"): %+v, want told and not a member", s)
	}
}
