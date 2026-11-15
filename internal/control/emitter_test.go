package control

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// The audit emitter (spec §4.5, D17): the one place the control server writes output. It takes the
// store's accepted changes from a bounded queue, in revision order, and the best-effort notes that
// share the queue's free places, and it writes them to a sink. No handler, no writer lock and no
// waiter ever waits for it.

type emitterFixture struct {
	store  *tuning.Store
	sink   *recSink
	banner *recBanner
	em     *emitter
}

func newEmitterFixture(t *testing.T, slots int, applies tuning.Applicability) *emitterFixture {
	t.Helper()
	store, _ := newTestStore(t, applies, func(o *tuning.Options) {
		o.Now = func() time.Time { return time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC) }
		if slots > 0 {
			o.AuditSlots = slots
		}
	})
	f := &emitterFixture{store: store, sink: newRecSink(), banner: &recBanner{}}
	f.em = newEmitter(store, f.sink, f.banner.banner)
	f.em.start()
	t.Cleanup(func() { f.sink.release(); f.em.cutoff(); f.em.drain() })
	return f
}

func (f *emitterFixture) set(t *testing.T, k tuning.Key, text string) {
	t.Helper()
	if _, err := f.store.Set(k, text, testPeer); err != nil {
		t.Fatalf("Set(%s, %s) = %v", k, text, err)
	}
}

// waitFor polls until cond holds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestAcceptedChangesAreLogged (spec §4.10): every accepted change is one entry, past the level
// filter and live, with the setting, the old and new values, the revision, the acceptance time,
// the uid and the pid as fields; pause, resume and stop are logged the same way; an unchanged set
// and a refusal are not entries of the store's.
func TestAcceptedChangesAreLogged(t *testing.T) {
	f := newEmitterFixture(t, 0, archiveApplies)
	f.set(t, tuning.BatchDeleteSize, "100")
	f.set(t, tuning.BatchDeleteSize, "100") // unchanged: no event
	if _, err := f.store.Set(tuning.BatchSize, "5000", testPeer); err == nil {
		t.Fatal("a size above the ceiling was accepted")
	}
	if _, err := f.store.Pause(testPeer); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Resume(testPeer); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Stop(testPeer); err != nil {
		t.Fatal(err)
	}
	f.store.Terminal()
	f.em.cutoff()
	f.em.drain()

	got := f.sink.snapshot()
	want := []string{
		"Runtime setting changed: batch_delete_size 500 -> 100 (control socket, uid 1000, pid 4242); from the next DELETE",
		"Runtime control: pause requested (control socket, uid 1000, pid 4242)",
		"Runtime control: resume requested (control socket, uid 1000, pid 4242)",
		"Runtime control: stop requested (control socket, uid 1000, pid 4242)",
	}
	if len(got) != len(want) {
		t.Fatalf("entries = %q, want %d", f.sink.messages(), len(want))
	}
	for i, w := range want {
		if got[i].msg != w || got[i].level != tuning.LevelInfo {
			t.Errorf("entry %d = %v %q, want info %q", i, got[i].level, got[i].msg, w)
		}
	}
	for k, w := range map[string]any{
		"setting": "batch_delete_size", "old": "500", "new": "100", "revision": uint64(1),
		"accepted_at": "2026-11-15T12:00:00Z", "uid": 1000, "pid": 4242,
	} {
		if got[0].kv[k] != w {
			t.Errorf("field %s = %v (%T), want %v", k, got[0].kv[k], got[0].kv[k], w)
		}
	}
}

// TestPeerWithoutCredentials: a platform with no peer credentials logs a change without a uid,
// and one with no pid logs it without a pid (spec §4.1, Windows).
func TestPeerWithoutCredentials(t *testing.T) {
	f := newEmitterFixture(t, 0, archiveApplies)
	if _, err := f.store.Set(tuning.BatchSize, "10", tuning.Peer{UID: -1}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Set(tuning.BatchSize, "20", tuning.Peer{UID: 1000}); err != nil {
		t.Fatal(err)
	}
	f.store.Terminal()
	f.em.cutoff()
	f.em.drain()
	msgs := f.sink.messages()
	if len(msgs) != 2 || !strings.Contains(msgs[0], "(control socket); from") || !strings.Contains(msgs[1], "(control socket, uid 1000); from") {
		t.Errorf("entries = %q", msgs)
	}
	if _, has := f.sink.snapshot()[0].kv["uid"]; has {
		t.Error("an entry for a peer without credentials carries a uid field")
	}
}

// TestToleranceRaiseWarns (spec §7 row 38, second half): each accepted raise of the lag tolerance
// above its configured value is reported as a banner is: one WARNING line through the handed-in
// Banner. A lowering, a restore and a raise up to the configured value are logged as any other
// change and write no WARNING; the summary marks a final value above the configuration.
func TestToleranceRaiseWarns(t *testing.T) {
	f := newEmitterFixture(t, 0, archiveApplies)
	f.set(t, tuning.SecondsBehindSourceWithin, "4")  // a lowering
	f.set(t, tuning.SecondsBehindSourceWithin, "10") // the restore
	f.store.RoundCompleted(f.store.Load().GateRevision)
	f.set(t, tuning.SecondsBehindSourceWithin, "20") // a raise above 10
	f.store.RoundCompleted(f.store.Load().GateRevision)
	f.set(t, tuning.SecondsBehindSourceWithin, "30")
	f.store.RoundCompleted(f.store.Load().GateRevision)
	f.set(t, tuning.SecondsBehindSourceWithin, "25") // a lowering from above the configuration
	sum := f.store.Terminal()
	f.em.cutoff()
	f.em.drain()

	want := []string{
		"WARNING: seconds_behind_source_within raised to 20, above the configured 10, over the control socket (uid 1000): replicas may fall further behind before deletes hold.",
		"WARNING: seconds_behind_source_within raised to 30, above the configured 10, over the control socket (uid 1000): replicas may fall further behind before deletes hold.",
	}
	if got := f.banner.got(); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("banners = %q, want %q", got, want)
	}
	if n := len(f.sink.messages()); n != 5 {
		t.Errorf("entries = %d, want one per accepted change, 5: %q", n, f.sink.messages())
	}
	if len(sum.Settings) != 1 || !sum.Settings[0].AboveConfigured || sum.Settings[0].Final != "25" || sum.Settings[0].Changes != 5 {
		t.Errorf("summary = %+v, want the tolerance, 5 changes, final 25, above the configuration", sum.Settings)
	}
}

// TestEmitterOrder: entries reach the sink in revision order, one per accepted change, whatever
// other entries are queued between them.
func TestEmitterOrder(t *testing.T) {
	f := newEmitterFixture(t, 0, archiveApplies)
	f.sink.block()
	f.set(t, tuning.BatchSize, "900")
	<-f.sink.reached // the emitter is writing revision 1 and holds there
	f.em.note(tuning.LevelWarn, testPeer, "refused: one")
	f.set(t, tuning.BatchSize, "800")
	f.em.note(tuning.LevelWarn, testPeer, "refused: two")
	f.set(t, tuning.BatchSize, "700")
	f.sink.release()
	f.store.Terminal()
	f.em.cutoff()
	f.em.drain()

	var revisions []uint64
	for _, e := range f.sink.snapshot() {
		if r, ok := e.kv["revision"].(uint64); ok {
			revisions = append(revisions, r)
		}
	}
	if len(revisions) != 3 || revisions[0] != 1 || revisions[1] != 2 || revisions[2] != 3 {
		t.Errorf("revisions written = %v, want 1 2 3 in order", revisions)
	}
	if n := len(f.sink.messages()); n != 5 {
		t.Errorf("entries = %q, want the 3 changes and the 2 notes", f.sink.messages())
	}
}

// TestSlowLogStateDoesNotWait (spec §7 row 45): with the log's writer held shut, a setter is
// accepted and answered, the waiters on the old snapshot wake, a second, corrective setter is
// accepted, and the terminal transition returns its frozen summary. None of them waits for the
// writer.
func TestSlowLogStateDoesNotWait(t *testing.T) {
	f := newEmitterFixture(t, 0, archiveApplies)
	f.sink.block()
	old := f.store.Load()
	woke := make(chan struct{})
	go func() { <-old.Changed(); close(woke) }()

	type result struct {
		sum  tuning.Summary
		errs [2]error
	}
	done := make(chan result, 1)
	go func() {
		var r result
		_, r.errs[0] = f.store.Set(tuning.BatchDeleteSize, "100", testPeer)
		<-f.sink.reached // the emitter is stuck writing it
		_, r.errs[1] = f.store.Set(tuning.BatchDeleteSize, "50", testPeer)
		r.sum = f.store.Terminal()
		done <- r
	}()
	select {
	case r := <-done:
		sum := r.sum
		if r.errs[0] != nil || r.errs[1] != nil {
			t.Fatalf("setters = %v, %v, want both accepted", r.errs[0], r.errs[1])
		}
		if len(sum.Settings) != 1 || sum.Settings[0].Changes != 2 || sum.Settings[0].Final != "50" {
			t.Errorf("summary = %+v, want two changes to batch_delete_size, final 50", sum.Settings)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a setter or the terminal transition waited for the log's writer")
	}
	select {
	case <-woke:
	case <-time.After(5 * time.Second):
		t.Error("a waiter on the old snapshot did not wake")
	}
}

// TestSlowLogAuditMatchesSummary (spec §7 row 46): when the writer is released the audit entries
// appear in revision order, one per accepted change, and match the frozen summary.
func TestSlowLogAuditMatchesSummary(t *testing.T) {
	f := newEmitterFixture(t, 0, archiveApplies)
	f.sink.block()
	f.set(t, tuning.BatchSize, "900")
	f.set(t, tuning.SleepSeconds, "2")
	f.set(t, tuning.BatchSize, "800")
	f.set(t, tuning.BatchSize, "1000") // restored: still listed
	sum := f.store.Terminal()
	f.em.cutoff()
	f.sink.release()
	f.em.drain()

	counts := map[string]int{}
	var revisions []uint64
	for _, e := range f.sink.snapshot() {
		counts[e.kv["setting"].(string)]++
		revisions = append(revisions, e.kv["revision"].(uint64))
	}
	for i, r := range revisions {
		if r != uint64(i+1) {
			t.Errorf("revisions = %v, want 1 2 3 4", revisions)
			break
		}
	}
	for _, s := range sum.Settings {
		if counts[string(s.Key)] != s.Changes {
			t.Errorf("%s: %d audit entries, the summary counts %d", s.Key, counts[string(s.Key)], s.Changes)
		}
	}
	if len(counts) != len(sum.Settings) {
		t.Errorf("audit settings %v, summary %+v", counts, sum.Settings)
	}
}

// TestSlowLogBoundedQueue (spec §7 row 47): with the writer shut and the queue full, the next
// mutation is `busy` and the snapshot, revision and summary are unchanged; notes beyond the queue
// are counted, and the count is written after the release.
func TestSlowLogBoundedQueue(t *testing.T) {
	const slots = 3
	f := newEmitterFixture(t, slots, archiveApplies)
	f.sink.block()
	for i := range slots {
		f.set(t, tuning.BatchSize, []string{"900", "800", "700"}[i])
	}
	before, revision := f.store.Load(), f.store.Load().Revision
	_, err := f.store.Set(tuning.BatchSize, "600", testPeer)
	if r, ok := err.(*tuning.Refusal); !ok || r.Reason != tuning.ReasonBusy {
		t.Fatalf("Set with a full queue = %v, want busy", err)
	}
	if f.store.Load() != before || f.store.Load().Revision != revision {
		t.Error("a busy refusal changed the snapshot")
	}
	for range 5 {
		f.em.note(tuning.LevelWarn, testPeer, "refused: a setter")
	}
	f.sink.release()
	waitFor(t, "the queue to drain", func() bool { return len(f.sink.messages()) >= slots+1 })
	f.store.Terminal()
	f.em.cutoff()
	f.em.drain()

	msgs := f.sink.messages()
	if len(msgs) != slots+1 || !strings.Contains(msgs[len(msgs)-1], "5 ") || !strings.Contains(msgs[len(msgs)-1], "not logged") {
		t.Errorf("entries = %q, want the %d changes and one count line of 5", msgs, slots)
	}
	if sum := f.store.Terminal(); sum.Settings[0].Changes != slots {
		t.Errorf("summary counts %d changes, want %d: the busy one is not in it", sum.Settings[0].Changes, slots)
	}
}

// TestNotesShareTheQueue: a note takes a place of the queue and is written like any entry; with
// room it is never counted.
func TestNotesShareTheQueue(t *testing.T) {
	f := newEmitterFixture(t, 0, archiveApplies)
	f.em.note(tuning.LevelWarn, testPeer, "Control request refused: ceiling batch_size 5000")
	f.em.note(tuning.LevelError, tuning.Peer{UID: -1}, "Control socket handler panicked: boom")
	f.store.Terminal()
	f.em.cutoff()
	f.em.drain()
	got := f.sink.snapshot()
	if len(got) != 2 || got[0].level != tuning.LevelWarn || got[1].level != tuning.LevelError ||
		got[0].msg != "Control request refused: ceiling batch_size 5000" || got[0].kv["uid"] != 1000 {
		t.Errorf("entries = %+v, want the warning then the error", got)
	}
}

// TestShutdownDrainIsFinite (spec §7 row 49): while the emitter writes its final list, clients
// keep sending refused setters: the emitter finishes after exactly the entries queued before the
// cutoff plus one count line, and the command's final output can follow.
func TestShutdownDrainIsFinite(t *testing.T) {
	f := newEmitterFixture(t, 0, archiveApplies)
	f.sink.block()
	f.set(t, tuning.BatchSize, "900")
	f.em.note(tuning.LevelWarn, testPeer, "refused: before the cutoff")
	f.store.Terminal()
	f.em.cutoff()

	stop := make(chan struct{})
	var senders sync.WaitGroup
	senders.Add(1)
	go func() {
		defer senders.Done()
		for {
			select {
			case <-stop:
				return
			default:
				f.em.note(tuning.LevelWarn, testPeer, "refused: after the cutoff")
			}
		}
	}()
	drained := make(chan struct{})
	go func() { f.em.drain(); close(drained) }()
	time.Sleep(50 * time.Millisecond) // the drain is waiting on the held writer, with senders at work
	f.sink.release()
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("the emitter did not finish while refused setters kept coming")
	}
	close(stop)
	senders.Wait()

	msgs := f.sink.messages()
	if len(msgs) != 3 || !strings.Contains(msgs[0], "Runtime setting changed") || msgs[1] != "refused: before the cutoff" || !strings.Contains(msgs[2], "not logged") {
		t.Errorf("entries = %q, want the change, the note queued before the cutoff and one count line", msgs)
	}
}

// TestShutdownLateEnqueue (spec §7 row 51): an attempt to queue an entry after the emitter has
// stopped is a counted no-op: no panic, no write after the log closes. The race detector, which
// the unit checks run under, is clean with the attempts racing the end.
func TestShutdownLateEnqueue(t *testing.T) {
	f := newEmitterFixture(t, 0, archiveApplies)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				f.em.note(tuning.LevelWarn, testPeer, "refused: racing the end")
			}
		}()
	}
	f.store.Terminal()
	f.em.cutoff()
	f.em.drain()
	f.sink.closeLog()
	wg.Wait()
	f.em.note(tuning.LevelWarn, testPeer, "refused: after the log closed")
	if f.sink.late != 0 {
		t.Errorf("%d entries were written after the log closed", f.sink.late)
	}
}

// TestEmitterDrainIsIdempotent: drain may be called twice, and a cutoff before it or after it
// changes nothing; the emitter stops once.
func TestEmitterDrainIsIdempotent(t *testing.T) {
	f := newEmitterFixture(t, 0, archiveApplies)
	f.set(t, tuning.BatchSize, "900")
	f.store.Terminal()
	f.em.cutoff()
	f.em.drain()
	f.em.cutoff()
	f.em.drain()
	if n := len(f.sink.messages()); n != 1 {
		t.Errorf("entries = %q, want the one change", f.sink.messages())
	}
}
