//go:build integration

package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/replication"
	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// TEL-1 C4b: the replication gate on the settings store, driven through the store directly (no
// socket yet). The replica's lag is real: db3 applies the source's events SOURCE_DELAY seconds
// late, as TestMySQLLiveReplicationGate's lagging case does. What C6 completes over the socket is
// plan §4: the status word `held`, the WARNING of each raise and the closing entry; none of it
// is asserted here.

// delayedReplica makes db3 apply every event delay seconds late, and returns it to no delay when
// the test ends. Call it before newEnv, so the scratch schemas are dropped first, and before the
// seed writes, so their events are the delayed ones.
func delayedReplica(t *testing.T, delay int) *sql.DB {
	t.Helper()
	replica := openServer(t, "TEST_REPLICA", "", nil)
	t.Cleanup(func() {
		for _, stmt := range []string{"STOP REPLICA SQL_THREAD", "CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 0", "START REPLICA SQL_THREAD"} {
			_, _ = replica.Exec(stmt)
		}
	})
	execAll(t, replica, "STOP REPLICA SQL_THREAD", fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_DELAY = %d", delay), "START REPLICA SQL_THREAD")
	return replica
}

// lagReaches waits until the replica reports at least n seconds behind.
func lagReaches(t *testing.T, replica *sql.DB, n int64) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for replicaLag(t, replica) < n {
		if time.Now().After(deadline) {
			t.Fatalf("the replica's lag never reached %d s with SOURCE_DELAY set", n)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// gateStore is the store a gated run of cfg reads, with the configured tolerance and a check
// interval of one second, and the live gate over db3 built on it: the engine and the gate read
// the one store, as the command will wire them.
func gateStore(t *testing.T, cfg Config, tolerance int) (*tuning.Store, *replication.Gate) {
	t.Helper()
	store := controlStore(cfg, true, func(o *tuning.Options) { o.Start.SecondsBehindSourceWithin = tolerance })
	gate, _ := liveGateStore(t, tolerance, store)
	return store, gate
}

type runResult struct {
	rep *Report
	err error
}

// goRun runs cfg in the background and returns where its result will arrive.
func goRun(t *testing.T, e env, cfg Config, within time.Duration) <-chan runResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	t.Cleanup(cancel)
	done := make(chan runResult, 1)
	go func() {
		rep, err := e.runContext(t, ctx, cfg)
		done <- runResult{rep, err}
	}()
	return done
}

// waitUntil polls cond every 20 ms until it holds, and fails the test with what after within.
func waitUntil(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", within, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// held reports whether the gate's current call is holding, from its record, which makes no probe.
func held(g *replication.Gate) bool { return !g.Record().HoldingSince.IsZero() }

// stillRunning fails the test if the run has returned, with what it said.
func stillRunning(t *testing.T, done <-chan runResult, why string) {
	t.Helper()
	select {
	case r := <-done:
		t.Fatalf("Run() = %+v, %v %s", r.rep, r.err, why)
	default:
	}
}

// deleteTimes records the time of every DELETE the run issues, beside the seam, so that a test
// can tell when a statement went out without any change to the engine.
type deleteTimes struct {
	mu sync.Mutex
	at []time.Time
}

func (d *deleteTimes) observe(st *statement) {
	if st.op != opDelete {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.at = append(d.at, time.Now())
}

func (d *deleteTimes) times() []time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]time.Time(nil), d.at...)
}

// observe feeds one statement to the seam and the timer both.
func observeBoth(rec *seam, d *deleteTimes) func(*statement) {
	return func(st *statement) {
		rec.observe(st)
		d.observe(st)
	}
}

// TestMySQLControlToleranceLowered (spec §7): the run starts at a configured tolerance above the
// replica's real lag and passes. The tolerance is lowered below the lag: the next DELETE holds,
// and none goes out while it does. It is restored to the configured value: the hold ends without
// waiting for the replica, and every row is deleted exactly once.
func TestMySQLControlToleranceLowered(t *testing.T) {
	replica := delayedReplica(t, 10)
	e := newEnv(t, "tollow")
	d16Tables(t, e, jobspec.Archive)
	seedOrders(t, e, 3)
	cfg := e.config(t, job(t, jobspec.Archive, orderJob))
	cfg.Processing = config.ProcessingConfig{BatchSize: 3, BatchDeleteSize: 1}
	store, gate := gateStore(t, cfg, 30)
	cfg.Tuning, cfg.Lag = store, gate
	rec, times := &seam{}, &deleteTimes{}
	cfg.fault = observeBoth(rec, times)
	lowered := false
	cfg.at = func(p point) {
		if p == atDeleted && !lowered {
			lowered = true
			setStore(t, store, tuning.SecondsBehindSourceWithin, "1") // accepted before the next DELETE's decision
		}
	}
	lagReaches(t, replica, 2)

	done := goRun(t, e, cfg, 90*time.Second)
	waitUntil(t, 30*time.Second, "the lowered tolerance to hold the next DELETE", func() bool { return held(gate) })
	issued := len(times.times())
	if issued != 1 {
		t.Errorf("DELETEs issued when the hold began = %d, want 1: the one that passed at the configured tolerance", issued)
	}
	time.Sleep(3 * time.Second) // three check intervals, the replica still behind
	stillRunning(t, done, "while the tolerance is below the lag, want the run held")
	if got := len(times.times()); got != issued {
		t.Errorf("DELETEs issued during the hold = %d, want %d: none passes the gate below the lag", got, issued)
	}
	if lag := replicaLag(t, replica); lag < 2 {
		t.Fatalf("replica lag %d s: the test needs it above the lowered tolerance throughout the hold", lag)
	}

	restored := time.Now()
	setStore(t, store, tuning.SecondsBehindSourceWithin, "30")
	r := <-done
	if r.err != nil || r.rep == nil {
		t.Fatalf("Run() = %+v, %v after the tolerance was restored", r.rep, r.err)
	}
	if took := time.Since(restored); took > 6*time.Second {
		t.Errorf("the run took %v to finish after the restore, want it to end without waiting for the replica (SOURCE_DELAY is 10 s)", took)
	}
	deletesAreAll(t, e, rec, r.rep, 1)
}

// sleepyRun is the scenario of TestMySQLControlToleranceDuringSleep and its recovery twin: the
// archive deletes one chunk per table, sleeping 4 s between DELETEs; one second into the sleep
// that follows the first DELETE the tolerance is lowered below the lag. When the sleep ends no
// DELETE goes out and the gate holds. The tolerance is restored: the next DELETE is issued at
// once, and the sleep is not slept again before it.
func sleepyRun(t *testing.T, e env, cfg Config, replica *sql.DB, recovered int64) {
	t.Helper()
	const sleep = 4 * time.Second
	cfg.Processing.DeleteSleepSeconds = sleep.Seconds()
	store, gate := gateStore(t, cfg, 30)
	cfg.Tuning, cfg.Lag = store, gate
	rec, times := &seam{}, &deleteTimes{}
	cfg.fault = observeBoth(rec, times)
	firstDelete := make(chan time.Time, 1)
	var lowered sync.WaitGroup
	t.Cleanup(lowered.Wait) // a timer that fires after a failed test must not log into a finished one
	cfg.at = func(p point) {
		if p != atDeleted || len(times.times()) != 1 {
			return
		}
		firstDelete <- time.Now()
		lowered.Add(1)
		time.AfterFunc(time.Second, func() { // during the sleep
			defer lowered.Done()
			if _, err := store.Set(tuning.SecondsBehindSourceWithin, "1", controlPeer); err != nil {
				t.Errorf("lowering the tolerance during the sleep = %v", err)
			}
		})
	}
	lagReaches(t, replica, 2)

	done := goRun(t, e, cfg, 90*time.Second)
	first := <-firstDelete
	waitUntil(t, 30*time.Second, "the lowered tolerance to hold the DELETE the sleep led to", func() bool { return held(gate) })
	began := time.Since(first)
	if began < sleep-300*time.Millisecond || began > sleep+2500*time.Millisecond {
		t.Errorf("the hold began %v after the first DELETE, want about the sleep's %v: the sleep first, then the gate", began, sleep)
	}
	if got := len(times.times()); got != 1 {
		t.Errorf("DELETEs issued when the sleep ended = %d, want 1: the DELETE after the sleep is held, not issued on a pass judged before it", got)
	}
	time.Sleep(2 * time.Second)
	stillRunning(t, done, "while the tolerance is below the lag, want the run held")
	if got := len(times.times()); got != 1 {
		t.Errorf("DELETEs issued during the hold = %d, want 1", got)
	}
	if lag := replicaLag(t, replica); lag < 2 {
		t.Fatalf("replica lag %d s: the test needs it above the lowered tolerance throughout the hold", lag)
	}

	lowered.Wait()
	restored := time.Now()
	setStore(t, store, tuning.SecondsBehindSourceWithin, "30")
	waitUntil(t, 5*time.Second, "the second DELETE after the restore", func() bool { return len(times.times()) >= 2 })
	if gap := times.times()[1].Sub(restored); gap > 2*time.Second {
		t.Errorf("the second DELETE went out %v after the restore, want it at once: the sleep was slept once, before the hold", gap)
	}
	r := <-done
	if r.err != nil || r.rep == nil || r.rep.Recovered != recovered {
		t.Fatalf("Run() = %+v, %v, want it complete with %d roots recovered", r.rep, r.err, recovered)
	}
	if got := len(times.times()); got != 3 {
		t.Errorf("DELETEs = %d, want 3: one per table", got)
	}
	if gap := times.times()[2].Sub(times.times()[1]); gap < sleep-300*time.Millisecond {
		t.Errorf("the third DELETE went out %v after the second, want the sleep between consecutive DELETEs kept", gap)
	}
	if got := fmt.Sprint(r.rep.Deleted); got != "map[line_notes:6 order_lines:6 orders:3]" {
		t.Errorf("deleted = %s, want 3 orders, 6 lines and 6 notes, each once", got)
	}
	if got := count(t, e.src, "SELECT COUNT(*) FROM orders WHERE placed < '2020-01-01'"); got != 0 {
		t.Errorf("old orders left in the source = %d, want 0", got)
	}
	if got := fmt.Sprint(statuses(t, e.dst, e.dstSchema)); got != "map[2:3]" {
		t.Errorf("log = %s, want 3 completed", got)
	}
}

// TestMySQLControlToleranceDuringSleep (spec §7, Codex's example scaled to the estate): the first
// DELETE passes the gate and is issued; during the sleep before the second, the tolerance is
// lowered below the lag. When the sleep ends no DELETE is issued and the gate holds. The
// tolerance is restored to the configured value: the DELETE is issued and the run completes. The
// sleep is slept once.
func TestMySQLControlToleranceDuringSleep(t *testing.T) {
	replica := delayedReplica(t, 12)
	e := newEnv(t, "tolsleep")
	d16Tables(t, e, jobspec.Archive)
	seedOrders(t, e, 3)
	cfg := e.config(t, job(t, jobspec.Archive, orderJob))
	cfg.Processing = config.ProcessingConfig{BatchSize: 3, BatchDeleteSize: 100}
	sleepyRun(t, e, cfg, replica, 0)
}

// TestMySQLControlToleranceDuringSleepRecovery is the same during recovery's deletes: an
// interrupted archive left three logged roots; the rerun's replay sleeps and gates in the same
// order.
func TestMySQLControlToleranceDuringSleepRecovery(t *testing.T) {
	replica := delayedReplica(t, 12)
	e := newEnv(t, "tolsleeprec")
	d16Tables(t, e, jobspec.Archive)
	seedOrders(t, e, 3)
	first := e.config(t, job(t, jobspec.Archive, orderJob))
	first.Processing = config.ProcessingConfig{BatchSize: 3, BatchDeleteSize: 100}
	leaveLogged(t, e, first, 3)
	cfg := e.config(t, job(t, jobspec.Archive, orderJob))
	cfg.Processing = config.ProcessingConfig{BatchSize: 3, BatchDeleteSize: 100}
	sleepyRun(t, e, cfg, replica, 3)
}

// TestMySQLControlToleranceRaised (spec §7): the run starts at a configured tolerance below the
// replica's real lag and holds. A raise straight to a value above the lag is refused as a step,
// the hold continues, and nothing is changed, queued or counted. Raises of one step each, each
// after the gate probed at the previous value, are accepted; the hold continues while the
// tolerance is below the lag and ends at the first step above it, without waiting for the
// replica. (Each raise's WARNING and the closing entry are C6's.)
func TestMySQLControlToleranceRaised(t *testing.T) {
	replica := delayedReplica(t, 20)
	e := newEnv(t, "tolraise")
	d16Tables(t, e, jobspec.Archive)
	seedOrders(t, e, 3)
	cfg := e.config(t, job(t, jobspec.Archive, orderJob))
	cfg.Processing = config.ProcessingConfig{BatchSize: 3, BatchDeleteSize: 100}
	store, gate := gateStore(t, cfg, 1)
	cfg.Tuning, cfg.Lag = store, gate
	rec := &seam{}
	cfg.fault = rec.observe
	lagReaches(t, replica, 5)

	done := goRun(t, e, cfg, 90*time.Second)
	waitUntil(t, 30*time.Second, "the run to hold at the configured tolerance, below the lag", func() bool { return held(gate) })
	checked := gate.Record().Servers[0].CheckedAt
	waitUntil(t, 10*time.Second, "a second probe round, so the first has completed", func() bool {
		return gate.Record().Servers[0].CheckedAt.After(checked)
	})

	// Straight to a value above the lag: refused as a step, naming the largest accepted now.
	_, err := store.Set(tuning.SecondsBehindSourceWithin, "100", controlPeer)
	var refusal *tuning.Refusal
	if !errors.As(err, &refusal) || refusal.Reason != tuning.ReasonStep || !strings.Contains(refusal.Message, "largest value accepted now is 2") {
		t.Fatalf("Set(100) at a configured 1 = %v, want a step refusal naming 2 as the largest value accepted now", err)
	}
	stillRunning(t, done, "after a refused raise, want the hold to continue")
	if !held(gate) || store.Load().Revision != 0 || len(store.Audit().Events()) != 0 {
		t.Errorf("after the refused raise: held %v, revision %d, audit events %d, want held, revision 0, no event", held(gate), store.Load().Revision, len(store.Audit().Events()))
	}

	// One step at a time, each after the gate probed at the previous value.
	raises, current := 0, 1
	for {
		next := current + 1
		waitUntil(t, 10*time.Second, "the raise to "+strconv.Itoa(next)+" to be accepted", func() bool {
			_, err := store.Set(tuning.SecondsBehindSourceWithin, strconv.Itoa(next), controlPeer)
			if err == nil {
				return true
			}
			var r *tuning.Refusal
			if !errors.As(err, &r) || r.Reason != tuning.ReasonStep || !strings.Contains(r.Message, "not yet been checked") {
				t.Fatalf("Set(%d) from %d = %v, want it accepted, or refused only for want of a round under the current tolerance", next, current, err)
			}
			return false
		})
		raises, current = raises+1, next
		var verdict replication.ServerVerdict
		waitUntil(t, 10*time.Second, "the gate to probe at "+strconv.Itoa(current), func() bool {
			verdict = gate.Record().Servers[0]
			return verdict.Tolerance == int64(current) && !verdict.CheckedAt.IsZero()
		})
		if verdict.Healthy {
			break // the first step above the lag: the hold ends here
		}
		if verdict.Lag == nil || *verdict.Lag <= int64(current) {
			t.Fatalf("at tolerance %d the gate found %+v, want a lag above the tolerance while the hold goes on", current, verdict)
		}
		stillRunning(t, done, fmt.Sprintf("at tolerance %d with the lag above it, want the hold to continue", current))
		if raises > 30 {
			t.Fatalf("%d raises and the hold has not ended: the lag runs away from one step per round", raises)
		}
	}
	if raises < 2 {
		t.Errorf("raises = %d, want the hold to have gone on through at least two steps (lag was at least 5)", raises)
	}

	r := <-done
	if r.err != nil || r.rep == nil {
		t.Fatalf("Run() = %+v, %v after the step above the lag", r.rep, r.err)
	}
	if got := fmt.Sprint(r.rep.Deleted); got != "map[line_notes:6 order_lines:6 orders:3]" {
		t.Errorf("deleted = %s, want 3 orders, 6 lines and 6 notes, each once", got)
	}
	if got := count(t, e.src, "SELECT COUNT(*) FROM orders WHERE placed < '2020-01-01'"); got != 0 {
		t.Errorf("old orders left in the source = %d, want 0", got)
	}

	// The summary counts the accepted steps and marks the final value; the refusals are not in it.
	if got := len(store.Audit().Events()); got != raises {
		t.Errorf("audit events = %d, want %d: one per accepted raise, none for the refusal", got, raises)
	}
	sum := store.Terminal()
	if len(sum.Settings) != 1 || sum.Settings[0].Key != tuning.SecondsBehindSourceWithin {
		t.Fatalf("summary = %+v, want the tolerance alone", sum.Settings)
	}
	if s := sum.Settings[0]; s.Configured != "1" || s.Final != strconv.Itoa(current) || s.Changes != raises || !s.AboveConfigured {
		t.Errorf("summary = %+v, want configured 1, final %d, %d changes, above the configuration", s, current, raises)
	}
}
