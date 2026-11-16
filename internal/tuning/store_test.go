package tuning

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
)

// start is the run's configured values: the ceilings of the two sizes, the configured tolerance
// and the two group bounds, whose steps (10, 1000 and 300) differ on purpose so that a rule that
// borrowed another setting's step would show.
var start = Settings{
	BatchSize: 1000, BatchDeleteSize: 500,
	SleepSeconds: 1, DeleteSleepSeconds: 0,
	SecondsBehindSourceWithin: 10, CheckInterval: 5,
	GroupApplierQueueWithin: 1000, GroupCertifierQueueWithin: 300,
}

// allApply is an archive or purge run with the replication gate and a server of type group in it.
var allApply = Applicability{Delete: true, Gate: true, Group: true}

var peer = Peer{UID: 1000, PID: 4242}

// clock is a controllable time source.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newClock() *clock { return &clock{t: time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)} }

func newStore(opts ...func(*Options)) *Store {
	o := Options{Start: start, Applies: allApply}
	for _, f := range opts {
		f(&o)
	}
	return New(o)
}

func refusal(t *testing.T, err error) *Refusal {
	t.Helper()
	var r *Refusal
	if !errors.As(err, &r) {
		t.Fatalf("error = %v (%T), want a *Refusal", err, err)
	}
	return r
}

func mustSet(t *testing.T, s *Store, k Key, text string) Change {
	t.Helper()
	c, err := s.Set(k, text, peer)
	if err != nil {
		t.Fatalf("Set(%s, %q) = %v", k, text, err)
	}
	return c
}

// TestLoadReturnsTheStart: a new store's first snapshot holds the configured values, at
// revision 0, unpaused and not stopping.
func TestLoadReturnsTheStart(t *testing.T) {
	s := newStore()
	snap := s.Load()
	if snap.Settings != start || snap.Revision != 0 || snap.GateRevision != 0 || snap.Paused || snap.Stopping || !snap.PausedSince.IsZero() {
		t.Fatalf("first snapshot = %+v, want the start at revision 0", snap)
	}
	if s.Start() != start {
		t.Fatalf("Start() = %+v", s.Start())
	}
	select {
	case <-snap.Changed():
		t.Fatal("a snapshot nothing replaced is already changed")
	default:
	}
}

// TestSetChangesOneSetting: a setter replaces the snapshot with a new one that changes only
// that setting; the old snapshot is untouched (immutable), and the reply says what was replaced.
func TestSetChangesOneSetting(t *testing.T) {
	s := newStore()
	old := s.Load()
	c := mustSet(t, s, BatchDeleteSize, "100")
	if c.Unchanged || c.Key != BatchDeleteSize || c.Old != "500" || c.New != "100" || c.Revision != 1 || c.Effect != "from the next DELETE" {
		t.Fatalf("change = %+v", c)
	}
	now := s.Load()
	want := start
	want.BatchDeleteSize = 100
	if now.Settings != want || now.Revision != 1 || now.GateRevision != 0 {
		t.Fatalf("snapshot after = %+v, want %+v at revision 1", now, want)
	}
	if old.Settings != start || old.Revision != 0 {
		t.Fatalf("the old snapshot changed under its readers: %+v", old)
	}
	select {
	case <-old.Changed():
	default:
		t.Fatal("the replaced snapshot's signal did not close")
	}
	select {
	case <-now.Changed():
		t.Fatal("the new snapshot's signal is already closed")
	default:
	}
}

// TestEveryKeySets: each of the eight settings can be set and read back, and a gate setting
// (the tolerance, the interval and the two group bounds) raises the gate revision while the
// others leave it.
func TestEveryKeySets(t *testing.T) {
	s := newStore()
	for i, tc := range []struct {
		key  Key
		text string
		gate bool
	}{
		{BatchSize, "10", false},
		{BatchDeleteSize, "20", false},
		{SleepSeconds, "0.5", false},
		{DeleteSleepSeconds, "0.25", false},
		{SecondsBehindSourceWithin, "3", true},
		{CheckInterval, "2", true},
		{GroupApplierQueueWithin, "700", true},
		{GroupCertifierQueueWithin, "200", true},
	} {
		before := s.Load()
		mustSet(t, s, tc.key, tc.text)
		after := s.Load()
		if got := after.Text(tc.key); got != tc.text {
			t.Errorf("%s reads back %q after setting %q", tc.key, got, tc.text)
		}
		if after.Revision != uint64(i+1) {
			t.Errorf("%s: revision %d, want %d", tc.key, after.Revision, i+1)
		}
		wantGate := before.GateRevision
		if tc.gate {
			wantGate++
		}
		if after.GateRevision != wantGate {
			t.Errorf("%s: gate revision %d, want %d", tc.key, after.GateRevision, wantGate)
		}
	}
}

// TestUnchangedIsNotAChange: setting a value equal to the current one answers "unchanged" and
// does nothing else: no revision, no signal, no audit event, nothing in the summary.
func TestUnchangedIsNotAChange(t *testing.T) {
	s := newStore()
	old := s.Load()
	for _, tc := range []struct {
		key  Key
		text string
	}{{BatchSize, "1000"}, {SleepSeconds, "1.0"}, {DeleteSleepSeconds, "0"}, {CheckInterval, "5"}} {
		c := mustSet(t, s, tc.key, tc.text)
		if !c.Unchanged || c.Revision != 0 || c.Old != c.New {
			t.Errorf("Set(%s, %s) = %+v, want unchanged at revision 0", tc.key, tc.text, c)
		}
	}
	if s.Load() != old {
		t.Error("an unchanged set replaced the snapshot")
	}
	select {
	case <-old.Changed():
		t.Error("an unchanged set closed the signal")
	default:
	}
	select {
	case e := <-s.Audit().Events():
		t.Errorf("an unchanged set queued an event: %+v", e)
	default:
	}
	if sum := s.Terminal(); len(sum.Settings) != 0 || sum.Pauses != 0 {
		t.Errorf("summary = %+v, want empty", sum)
	}
}

// fileMessages are the messages the file's validators give for a value of key: the per-value
// function and the block validator that calls it.
func fileMessage(key Key, v float64) (perValue, inFile string) {
	switch key {
	case BatchSize:
		perValue = errText(config.ValidateBatchSize(int(v)))
		inFile = fieldMessage(config.ValidateProcessingConfig("p", config.ProcessingConfig{BatchSize: int(v), BatchDeleteSize: 1}), "p.batch_size")
	case BatchDeleteSize:
		perValue = errText(config.ValidateBatchDeleteSize(int(v)))
		inFile = fieldMessage(config.ValidateProcessingConfig("p", config.ProcessingConfig{BatchSize: 1, BatchDeleteSize: int(v)}), "p.batch_delete_size")
	case SleepSeconds:
		perValue = errText(config.ValidateSleepSeconds(v))
		inFile = fieldMessage(config.ValidateProcessingConfig("p", config.ProcessingConfig{BatchSize: 1, BatchDeleteSize: 1, SleepSeconds: v}), "p.sleep_seconds")
	case DeleteSleepSeconds:
		perValue = errText(config.ValidateDeleteSleepSeconds(v))
		inFile = fieldMessage(config.ValidateProcessingConfig("p", config.ProcessingConfig{BatchSize: 1, BatchDeleteSize: 1, DeleteSleepSeconds: v}), "p.delete_sleep_seconds")
	case SecondsBehindSourceWithin:
		perValue = errText(config.ValidateSecondsBehindSourceWithin(int(v)))
		inFile = perValue // the file's block needs a whole config; the per-value function is what it calls
	case CheckInterval:
		perValue = errText(config.ValidateCheckInterval(int(v)))
		inFile = perValue
	case GroupApplierQueueWithin:
		perValue = errText(config.ValidateGroupApplierQueueWithin(int(v)))
		inFile = perValue // validateGroupBounds calls it for a value that is set
	case GroupCertifierQueueWithin:
		perValue = errText(config.ValidateGroupCertifierQueueWithin(int(v)))
		inFile = perValue
	}
	return perValue, inFile
}

func fieldMessage(errs config.ValidationErrors, field string) string {
	for _, e := range errs {
		if e.Field == field {
			return e.Message
		}
	}
	return ""
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// TestSettersShareFileValidators (spec §7 row 32): every value the file refuses, the setter
// refuses with the same message and the reason `invalid`; every value the file accepts passes
// the setter's validation (it may still meet a ceiling or a rule of its own).
func TestSettersShareFileValidators(t *testing.T) {
	texts := []string{"0", "1", "2", "10", "999999", "9223372036", "9223372036.854775807", "10000000000", "0.000000001"}
	for _, k := range Keys() {
		isInt := k != SleepSeconds && k != DeleteSleepSeconds
		for _, text := range texts {
			if isInt && strings.Contains(text, ".") {
				continue // a fraction for a whole-number setting is its own refusal
			}
			v, err := strconv.ParseFloat(text, 64)
			if err != nil {
				t.Fatal(err)
			}
			if isInt && v > 1<<31 {
				continue
			}
			perValue, inFile := fileMessage(k, v)
			if perValue != inFile {
				t.Fatalf("%s %s: the file says %q, the per-value function %q", k, text, inFile, perValue)
			}
			s := newStore()
			_, err = s.Set(k, text, peer)
			if perValue != "" {
				r := refusal(t, err)
				if r.Reason != ReasonInvalid || r.Message != perValue {
					t.Errorf("%s %s: refused as %s %q, want invalid %q (the file's message)", k, text, r.Reason, r.Message, perValue)
				}
				continue
			}
			if err != nil {
				if r := refusal(t, err); r.Reason == ReasonInvalid {
					t.Errorf("%s %s: the file accepts it, the setter says invalid: %q", k, text, r.Message)
				}
			}
		}
	}
}

// TestSettersRefuseWhatIsNotANumberOfTheKind: the number's text is judged after the lexical
// form: a setting takes plain digits (and a sleep one point), never a sign, an exponent, a
// space, a fraction on a whole-number setting, or a number too large to hold.
func TestSettersRefuseWhatIsNotANumberOfTheKind(t *testing.T) {
	bad := []string{"", "abc", " 5", "5 ", "+5", "-5", "0x10", "1e3", ".5", "5.", "inf", "nan", "NaN", "1_000", "٣"}
	for _, k := range Keys() {
		for _, text := range bad {
			r := refusalOf(t, newStore(), k, text)
			if r.Reason != ReasonInvalid {
				t.Errorf("%s %q: reason %s, want invalid", k, text, r.Reason)
			}
		}
	}
	for _, k := range []Key{BatchSize, BatchDeleteSize, SecondsBehindSourceWithin, CheckInterval} {
		for _, text := range []string{"1.5", "2.0", "99999999999999999999"} {
			if r := refusalOf(t, newStore(), k, text); r.Reason != ReasonInvalid {
				t.Errorf("%s %q: reason %s, want invalid", k, text, r.Reason)
			}
		}
	}
}

func refusalOf(t *testing.T, s *Store, k Key, text string) *Refusal {
	t.Helper()
	before := s.Load()
	_, err := s.Set(k, text, peer)
	if err == nil {
		t.Fatalf("Set(%s, %q) accepted", k, text)
	}
	if s.Load() != before {
		t.Errorf("a refused Set(%s, %q) replaced the snapshot", k, text)
	}
	return refusal(t, err)
}

// TestSizeCeilings (spec §7 row 34): batch_size and batch_delete_size may be lowered and
// raised back up to the starting value, never above it; check_interval and the sleeps move
// both ways. A refusal changes nothing.
func TestSizeCeilings(t *testing.T) {
	s := newStore()
	for _, tc := range []struct {
		key  Key
		cfg  int
		over string
	}{{BatchSize, 1000, "1001"}, {BatchDeleteSize, 500, "501"}} {
		r := refusalOf(t, s, tc.key, tc.over)
		if r.Reason != ReasonCeiling {
			t.Errorf("%s %s: reason %s, want ceiling", tc.key, tc.over, r.Reason)
		}
		for _, part := range []string{string(tc.key), tc.over, "configured " + strconv.Itoa(tc.cfg), "change the file and run the job again"} {
			if !strings.Contains(r.Message, part) {
				t.Errorf("%s: the ceiling message lacks %q: %s", tc.key, part, r.Message)
			}
		}
		mustSet(t, s, tc.key, "2")                    // lowered
		mustSet(t, s, tc.key, strconv.Itoa(tc.cfg/2)) // raised, below the start
		mustSet(t, s, tc.key, strconv.Itoa(tc.cfg))   // restored to the start
		if r := refusalOf(t, s, tc.key, tc.over); r.Reason != ReasonCeiling {
			t.Errorf("%s: a raise above the start after restoring is %s, want ceiling", tc.key, r.Reason)
		}
	}
	if got := s.Load().Revision; got != 6 {
		t.Errorf("revision %d after six accepted changes and refusals between them, want 6", got)
	}
	// Invalid is judged before the ceiling: zero is not a value for a size at all.
	if r := refusalOf(t, s, BatchSize, "0"); r.Reason != ReasonInvalid {
		t.Errorf("batch_size 0: %s, want invalid", r.Reason)
	}

	for _, tc := range []struct {
		key        Key
		up, down   string
		wantEffect string
	}{
		{SleepSeconds, "30", "0.1", "at once"},
		{DeleteSleepSeconds, "9", "0.25", "at once"},
		{CheckInterval, "60", "1", "from the next gate decision"},
	} {
		if c := mustSet(t, s, tc.key, tc.up); c.Effect != tc.wantEffect {
			t.Errorf("%s effect %q, want %q", tc.key, c.Effect, tc.wantEffect)
		}
		mustSet(t, s, tc.key, tc.down)
	}
}

// TestApplicability: a setting the run does not use is refused as `inapplicable`, with the
// file's own message for a copy-only job's delete settings and one for the gate's; it is
// judged before the value, and before `ending`.
func TestApplicability(t *testing.T) {
	copyOnly := newStore(func(o *Options) { o.Applies = Applicability{} })
	for _, k := range []Key{BatchDeleteSize, DeleteSleepSeconds} {
		r := refusalOf(t, copyOnly, k, "1")
		if r.Reason != ReasonInapplicable || !strings.Contains(r.Message, "a copy-only job deletes nothing") {
			t.Errorf("%s on copy-only: %s %q", k, r.Reason, r.Message)
		}
	}
	for _, k := range []Key{SecondsBehindSourceWithin, CheckInterval, GroupApplierQueueWithin, GroupCertifierQueueWithin} {
		r := refusalOf(t, copyOnly, k, "1")
		if r.Reason != ReasonInapplicable || !strings.Contains(r.Message, "this run does not use the replication gate") {
			t.Errorf("%s on copy-only: %s %q", k, r.Reason, r.Message)
		}
	}
	mustSet(t, copyOnly, BatchSize, "10")
	mustSet(t, copyOnly, SleepSeconds, "2")

	noGate := newStore(func(o *Options) { o.Applies = Applicability{Delete: true} })
	mustSet(t, noGate, BatchDeleteSize, "10")
	if r := refusalOf(t, noGate, CheckInterval, "1"); r.Reason != ReasonInapplicable {
		t.Errorf("a gate setting without replication: %s, want inapplicable", r.Reason)
	}

	for _, tc := range []struct {
		key Key
		ok  bool
	}{
		{BatchSize, true}, {SleepSeconds, true}, {BatchDeleteSize, false}, {DeleteSleepSeconds, false},
		{SecondsBehindSourceWithin, false}, {CheckInterval, false}, {GroupApplierQueueWithin, false}, {GroupCertifierQueueWithin, false},
	} {
		if got, _ := copyOnly.Applies(tc.key); got != tc.ok {
			t.Errorf("copy-only Applies(%s) = %v", tc.key, got)
		}
	}
	if got := copyOnly.ApplicableKeys(); !slices.Equal(got, []Key{BatchSize, SleepSeconds}) {
		t.Errorf("copy-only ApplicableKeys() = %v", got)
	}

	// The order of reasons: inapplicable, then ending, then invalid (spec §4.3).
	copyOnly.Terminal()
	if r := refusalOf(t, copyOnly, BatchDeleteSize, "abc"); r.Reason != ReasonInapplicable {
		t.Errorf("an inapplicable key with a bad number after the end: %s, want inapplicable", r.Reason)
	}
	if r := refusalOf(t, copyOnly, BatchSize, "0"); r.Reason != ReasonEnding {
		t.Errorf("an invalid value after the end: %s, want ending", r.Reason)
	}
}

// TestConcurrentSettersDifferentKeys (spec §7 row 41): handlers that each change another
// setting at the same time all take effect; the final snapshot holds every change, and the
// summary counts each.
func TestConcurrentSettersDifferentKeys(t *testing.T) {
	changes := []struct {
		key  Key
		text string
	}{
		{BatchSize, "10"}, {BatchDeleteSize, "20"}, {SleepSeconds, "0.5"}, {DeleteSleepSeconds, "0.25"},
		{SecondsBehindSourceWithin, "3"}, {CheckInterval, "2"}, {GroupApplierQueueWithin, "700"}, {GroupCertifierQueueWithin, "200"},
	}
	for round := 0; round < 200; round++ {
		s := newStore()
		var wg sync.WaitGroup
		gate := make(chan struct{})
		errs := make(chan error, len(changes))
		for _, c := range changes {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-gate
				_, err := s.Set(c.key, c.text, peer)
				errs <- err
			}()
		}
		close(gate)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("round %d: %v", round, err)
			}
		}
		snap := s.Load()
		want := Settings{
			BatchSize: 10, BatchDeleteSize: 20, SleepSeconds: 0.5, DeleteSleepSeconds: 0.25, SecondsBehindSourceWithin: 3, CheckInterval: 2,
			GroupApplierQueueWithin: 700, GroupCertifierQueueWithin: 200,
		}
		if snap.Settings != want || snap.Revision != uint64(len(changes)) || snap.GateRevision != 4 {
			t.Fatalf("round %d: snapshot %+v, want every change at revision %d and gate revision 4 (the tolerance, the interval and the two bounds)", round, snap, len(changes))
		}
		sum := s.Terminal()
		if len(sum.Settings) != len(changes) {
			t.Fatalf("round %d: summary lists %d settings, want %d", round, len(sum.Settings), len(changes))
		}
		for _, e := range sum.Settings {
			if e.Changes != 1 {
				t.Fatalf("round %d: %s counted %d changes, want 1", round, e.Key, e.Changes)
			}
		}
	}
}

// TestConcurrentSettersSameKey (spec §7 row 42): handlers that change one setting at once are
// ordered by revision; each reply's old value is the previous reply's new value, the final
// value is the last revision's, and the count equals the replies that changed something.
func TestConcurrentSettersSameKey(t *testing.T) {
	const writers = 32
	for round := 0; round < 100; round++ {
		s := newStore()
		var wg sync.WaitGroup
		gate := make(chan struct{})
		replies := make(chan Change, writers)
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-gate
				// Distinct from the start (1) and from each other.
				c, err := s.Set(SleepSeconds, strconv.Itoa(i+2), peer)
				if err != nil {
					t.Errorf("writer %d: %v", i, err)
					return
				}
				replies <- c
			}()
		}
		close(gate)
		wg.Wait()
		close(replies)
		var got []Change
		for c := range replies {
			got = append(got, c)
		}
		if len(got) != writers {
			t.Fatalf("round %d: %d replies, want %d", round, len(got), writers)
		}
		slices.SortFunc(got, func(a, b Change) int { return int(a.Revision) - int(b.Revision) })
		prev := "1"
		for i, c := range got {
			if c.Unchanged || c.Revision != uint64(i+1) {
				t.Fatalf("round %d: reply %d = %+v, want revision %d", round, i, c, i+1)
			}
			if c.Old != prev {
				t.Fatalf("round %d: reply at revision %d replaced %q, but the previous reply set %q", round, c.Revision, c.Old, prev)
			}
			prev = c.New
		}
		if got := s.Load().Text(SleepSeconds); got != prev || s.Load().Revision != writers {
			t.Fatalf("round %d: final value %q at revision %d, want %q at %d", round, got, s.Load().Revision, prev, writers)
		}
		sum := s.Terminal()
		if len(sum.Settings) != 1 || sum.Settings[0].Changes != writers {
			t.Fatalf("round %d: summary %+v, want one setting with %d changes", round, sum.Settings, writers)
		}
	}
}

// TestChangeAtReadToWait (spec §7 row 43): a snapshot is loaded, then a setter is accepted,
// then the wait on the loaded snapshot's signal starts: it returns at once. Two waiters on one
// snapshot both wake.
func TestChangeAtReadToWait(t *testing.T) {
	s := newStore()
	loaded := s.Load()
	mustSet(t, s, SleepSeconds, "2")
	select {
	case <-loaded.Changed():
	case <-time.After(time.Second):
		t.Fatal("a wait on a snapshot replaced before the wait began did not return at once")
	}

	snap := s.Load()
	woke := make(chan int, 2)
	ready := make(chan struct{}, 2)
	for i := 0; i < 2; i++ {
		go func() {
			ready <- struct{}{}
			<-snap.Changed()
			woke <- i
		}()
	}
	<-ready
	<-ready
	mustSet(t, s, SleepSeconds, "3")
	for i := 0; i < 2; i++ {
		select {
		case <-woke:
		case <-time.After(time.Second):
			t.Fatalf("only %d of 2 waiters woke on one change", i)
		}
	}
}

// TestTerminalSetterRace (spec §7 row 54): setters race the terminal transition. Every change
// that got an OK is in the frozen summary, every refused one is not, and the count matches.
func TestTerminalSetterRace(t *testing.T) {
	for round := 0; round < 200; round++ {
		s := newStore()
		var wg sync.WaitGroup
		gate := make(chan struct{})
		var mu sync.Mutex
		accepted := map[Key]int{}
		for i := 0; i < 24; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-gate
				k := []Key{SleepSeconds, DeleteSleepSeconds, CheckInterval}[i%3]
				c, err := s.Set(k, strconv.Itoa(i+2), peer)
				switch {
				case err == nil && !c.Unchanged:
					mu.Lock()
					accepted[k]++
					mu.Unlock()
				case err != nil && refusal(t, err).Reason != ReasonEnding:
					t.Errorf("a setter was refused as %v, want only ending", err)
				}
			}()
		}
		var frozen Summary
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			time.Sleep(time.Duration(round%5) * 20 * time.Microsecond)
			frozen = s.Terminal()
		}()
		close(gate)
		wg.Wait()

		total := 0
		for _, e := range frozen.Settings {
			if e.Changes != accepted[e.Key] {
				t.Fatalf("round %d: the summary counts %d changes of %s, but %d were answered OK", round, e.Changes, e.Key, accepted[e.Key])
			}
			total += e.Changes
		}
		for k, n := range accepted {
			found := false
			for _, e := range frozen.Settings {
				found = found || e.Key == k
			}
			if n > 0 && !found {
				t.Fatalf("round %d: %d OK changes of %s are missing from the summary", round, n, k)
			}
		}
		if got := s.Load().Revision; got != uint64(total) {
			t.Fatalf("round %d: revision %d, want %d accepted changes", round, got, total)
		}
		again := s.Terminal()
		if fmt.Sprint(again) != fmt.Sprint(frozen) {
			t.Fatalf("round %d: the frozen summary changed: %+v then %+v", round, frozen, again)
		}
	}
}

// TestTerminalFreezes: after the terminal transition nothing is accepted, whatever it is.
func TestTerminalFreezes(t *testing.T) {
	stops := 0
	s := newStore(func(o *Options) { o.RequestStop = func() { stops++ } })
	mustSet(t, s, SleepSeconds, "2")
	before := s.Load()
	s.Terminal()
	for name, f := range map[string]func() error{
		"set":    func() error { _, err := s.Set(SleepSeconds, "3", peer); return err },
		"pause":  func() error { _, err := s.Pause(peer); return err },
		"resume": func() error { _, err := s.Resume(peer); return err },
		"stop":   func() error { _, err := s.Stop(peer); return err },
	} {
		if r := refusal(t, f()); r.Reason != ReasonEnding {
			t.Errorf("%s after the end: %s, want ending", name, r.Reason)
		}
	}
	if s.Load() != before || stops != 0 {
		t.Errorf("a mutation after the end changed the store (snapshot replaced: %v, stop requests: %d)", s.Load() != before, stops)
	}
}

// TestSummary: per setting that changed, its configured value, its final value and the number
// of accepted changes; a setting changed and then restored is still listed; a refused or an
// unchanged request is not counted; the pauses are counted with the time spent in them.
func TestSummary(t *testing.T) {
	clk := newClock()
	s := newStore(func(o *Options) { o.Now = clk.Now })
	mustSet(t, s, BatchDeleteSize, "100")
	mustSet(t, s, BatchDeleteSize, "500") // restored
	mustSet(t, s, SleepSeconds, "2")
	mustSet(t, s, SleepSeconds, "2") // unchanged
	_ = refusalOf(t, s, BatchSize, "5000")
	_ = refusalOf(t, s, DeleteSleepSeconds, "abc")

	if _, err := s.Pause(peer); err != nil {
		t.Fatal(err)
	}
	clk.Advance(90 * time.Second)
	if _, err := s.Resume(peer); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Minute)
	if _, err := s.Pause(peer); err != nil {
		t.Fatal(err)
	}
	clk.Advance(30 * time.Second) // still paused when the run ends

	sum := s.Terminal()
	want := []SettingSummary{
		{Key: BatchDeleteSize, Configured: "500", Final: "500", Changes: 2},
		{Key: SleepSeconds, Configured: "1", Final: "2", Changes: 1},
	}
	if !slices.Equal(sum.Settings, want) {
		t.Errorf("settings = %+v, want %+v", sum.Settings, want)
	}
	if sum.Pauses != 2 || sum.PausedFor != 2*time.Minute {
		t.Errorf("pauses = %d for %v, want 2 for 2m0s (the open one to the end of the run)", sum.Pauses, sum.PausedFor)
	}
}

// TestPauseResume: the socket pause is a flag of the snapshot, and each change replaces it.
func TestPauseResume(t *testing.T) {
	clk := newClock()
	s := newStore(func(o *Options) { o.Now = clk.Now })
	first := s.Load()
	res, err := s.Pause(peer)
	if err != nil || !res.Changed || !res.Since.Equal(clk.Now()) {
		t.Fatalf("Pause() = %+v, %v", res, err)
	}
	snap := s.Load()
	if !snap.Paused || !snap.PausedSince.Equal(clk.Now()) || snap.Revision != 1 || snap.GateRevision != 0 {
		t.Fatalf("snapshot after pause = %+v", snap)
	}
	select {
	case <-first.Changed():
	default:
		t.Fatal("pause did not close the previous signal")
	}

	clk.Advance(time.Minute)
	again, err := s.Pause(peer)
	if err != nil || again.Changed || !again.Since.Equal(snap.PausedSince) {
		t.Fatalf("a second Pause() = %+v, %v, want unchanged and the original time", again, err)
	}
	if s.Load() != snap {
		t.Error("a second pause replaced the snapshot")
	}

	res, err = s.Resume(peer)
	if err != nil || !res.Changed || res.Since != snap.PausedSince {
		t.Fatalf("Resume() = %+v, %v", res, err)
	}
	if s.Load().Paused || s.Load().Revision != 2 {
		t.Fatalf("snapshot after resume = %+v", s.Load())
	}
	res, err = s.Resume(peer)
	if err != nil || res.Changed {
		t.Fatalf("Resume() with no pause = %+v, %v, want unchanged", res, err)
	}
}

// TestStop: a stop asks for the first phase once, through the function the store was given; a
// second stop is "already stopping" and does not ask again; a stop never cancels.
func TestStop(t *testing.T) {
	calls := 0
	s := newStore(func(o *Options) { o.RequestStop = func() { calls++ } })
	first := s.Load()
	res, err := s.Stop(peer)
	if err != nil || !res.Changed || res.Already {
		t.Fatalf("Stop() = %+v, %v", res, err)
	}
	if calls != 1 || !s.Load().Stopping || s.Load().Revision != 1 {
		t.Fatalf("after Stop: %d requests, snapshot %+v", calls, s.Load())
	}
	select {
	case <-first.Changed():
	default:
		t.Fatal("stop did not close the previous signal")
	}
	snap := s.Load()
	res, err = s.Stop(peer)
	if err != nil || res.Changed || !res.Already {
		t.Fatalf("a second Stop() = %+v, %v, want already stopping", res, err)
	}
	if calls != 1 || s.Load() != snap {
		t.Fatalf("a second stop asked again (%d requests) or replaced the snapshot", calls)
	}
}

// TestStopNeedsAnAuditPlace: a stop is accepted only with a place in the audit queue, as every
// mutation is; without one it is `busy`, nothing changed and the first phase not requested.
func TestStopNeedsAnAuditPlace(t *testing.T) {
	calls := 0
	s := newStore(func(o *Options) { o.AuditSlots = 1; o.RequestStop = func() { calls++ } })
	mustSet(t, s, SleepSeconds, "2") // fills the one place
	before := s.Load()
	_, err := s.Stop(peer)
	if r := refusal(t, err); r.Reason != ReasonBusy {
		t.Fatalf("Stop() with no place = %v, want busy", err)
	}
	if calls != 0 || s.Load() != before || before.Stopping {
		t.Fatalf("a busy stop changed something (requests %d)", calls)
	}
	// The consumer writes the entry and returns the place: the stop is then accepted.
	<-s.Audit().Events()
	s.Audit().Release()
	if res, err := s.Stop(peer); err != nil || !res.Changed || calls != 1 {
		t.Fatalf("Stop() after a place was freed = %+v, %v (%d requests)", res, err, calls)
	}
}

// TestAuditEvents: every accepted change puts one immutable event in the queue, in revision
// order, with the setting, the value actually replaced, the new value, the peer and the time;
// refused and unchanged requests put none.
func TestAuditEvents(t *testing.T) {
	clk := newClock()
	s := newStore(func(o *Options) { o.Now = clk.Now; o.RequestStop = func() {} })
	mustSet(t, s, BatchDeleteSize, "100")
	clk.Advance(time.Second)
	_ = refusalOf(t, s, BatchSize, "5000")
	mustSet(t, s, SleepSeconds, "1") // unchanged
	if _, err := s.Pause(Peer{UID: -1}); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Second)
	if _, err := s.Resume(peer); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Stop(peer); err != nil {
		t.Fatal(err)
	}

	var got []Event
	for len(got) < 4 {
		select {
		case e := <-s.Audit().Events():
			got = append(got, e)
		case <-time.After(time.Second):
			t.Fatalf("only %d events queued: %+v", len(got), got)
		}
	}
	select {
	case e := <-s.Audit().Events():
		t.Fatalf("an extra event: %+v", e)
	default:
	}
	t0 := newClock().Now()
	want := []Event{
		{Kind: KindSet, Revision: 1, Key: BatchDeleteSize, Old: "500", New: "100", Peer: peer, At: t0},
		{Kind: KindPause, Revision: 2, Peer: Peer{UID: -1}, At: t0.Add(time.Second)},
		{Kind: KindResume, Revision: 3, Peer: peer, At: t0.Add(2 * time.Second)},
		{Kind: KindStop, Revision: 4, Peer: peer, At: t0.Add(2 * time.Second)},
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("event %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestAuditQueueIsBounded (spec §4.5): a mutation is accepted only with a reserved place; when
// the queue is full it is `busy` and nothing changes (no revision, no snapshot, no summary);
// a place freed by the consumer lets the next one in; nothing accepted is ever dropped.
func TestAuditQueueIsBounded(t *testing.T) {
	s := newStore(func(o *Options) { o.AuditSlots = 2 })
	mustSet(t, s, SleepSeconds, "2")
	mustSet(t, s, SleepSeconds, "3")
	before := s.Load()
	_, err := s.Set(SleepSeconds, "4", peer)
	if r := refusal(t, err); r.Reason != ReasonBusy {
		t.Fatalf("a mutation with a full queue = %v, want busy", err)
	}
	if s.Load() != before {
		t.Fatal("a busy mutation replaced the snapshot")
	}
	if sum := s.Terminal(); sum.Settings[0].Changes != 2 {
		t.Fatalf("summary counts %d changes, want the 2 accepted", sum.Settings[0].Changes)
	}

	s = newStore(func(o *Options) { o.AuditSlots = 2 })
	mustSet(t, s, SleepSeconds, "2")
	mustSet(t, s, SleepSeconds, "3")
	e := <-s.Audit().Events()
	if e.Revision != 1 {
		t.Fatalf("first event revision %d", e.Revision)
	}
	s.Audit().Release()
	mustSet(t, s, SleepSeconds, "4") // the freed place
	var revs []uint64
	for len(revs) < 2 {
		e := <-s.Audit().Events()
		revs = append(revs, e.Revision)
		s.Audit().Release()
	}
	if !slices.Equal(revs, []uint64{2, 3}) {
		t.Fatalf("remaining events at revisions %v, want [2 3]", revs)
	}
}

// TestAuditReservation: places are reserved without blocking and put without blocking; the
// control server's best-effort entries share them.
func TestAuditReservation(t *testing.T) {
	a := NewAudit(2)
	first, second := a.Reserve(), a.Reserve()
	if !first || !second {
		t.Fatal("two reservations failed in a queue of two")
	}
	if a.Reserve() {
		t.Fatal("a third place was reserved in a queue of two")
	}
	done := make(chan struct{})
	go func() {
		a.Put(Event{Kind: KindSet, Revision: 1})
		a.Put(Event{Kind: KindSet, Revision: 2})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Put blocked with its place reserved")
	}
	if e := <-a.Events(); e.Revision != 1 {
		t.Fatalf("first event %+v", e)
	}
	if a.Reserve() {
		t.Fatal("a place was free before the consumer released one")
	}
	a.Release()
	if !a.Reserve() {
		t.Fatal("a released place was not reusable")
	}
}

// TestConstantStore: a store that nothing writes: the snapshot is the start for ever, and its
// signal never closes, so a wait on it ends only by its timer.
func TestConstantStore(t *testing.T) {
	s := Constant(start)
	snap := s.Load()
	if snap.Settings != start || snap.Revision != 0 {
		t.Fatalf("snapshot = %+v", snap)
	}
	select {
	case <-snap.Changed():
		t.Fatal("a constant store's snapshot changed")
	case <-time.After(20 * time.Millisecond):
	}
	for _, k := range Keys() {
		if ok, _ := s.Applies(k); !ok {
			t.Errorf("a constant store does not apply %s", k)
		}
	}
}

// TestText: a setting is rendered as the getter's body does: whole numbers in digits, a sleep as
// the shortest plain decimal that reads back to the same value, never with an exponent.
func TestText(t *testing.T) {
	for _, tc := range []struct {
		s    Settings
		k    Key
		want string
	}{
		{Settings{BatchSize: 1000}, BatchSize, "1000"},
		{Settings{BatchDeleteSize: 1}, BatchDeleteSize, "1"},
		{Settings{SecondsBehindSourceWithin: 0}, SecondsBehindSourceWithin, "0"},
		{Settings{CheckInterval: 5}, CheckInterval, "5"},
		{Settings{GroupApplierQueueWithin: 25000}, GroupApplierQueueWithin, "25000"},
		{Settings{GroupCertifierQueueWithin: 0}, GroupCertifierQueueWithin, "0"},
		{Settings{SleepSeconds: 1}, SleepSeconds, "1"},
		{Settings{SleepSeconds: 0.25}, SleepSeconds, "0.25"},
		{Settings{DeleteSleepSeconds: 1e-9}, DeleteSleepSeconds, "0.000000001"},
		{Settings{SleepSeconds: 9223372036}, SleepSeconds, "9223372036"},
	} {
		if got := tc.s.Text(tc.k); got != tc.want {
			t.Errorf("Text(%s) = %q, want %q", tc.k, got, tc.want)
		}
	}
	// Whatever the value, the text reads back to it and has no exponent.
	for _, v := range []float64{0, 1e-9, 0.1, 0.30000000000000004, 1.5, 123456.789, 9223372036.854774} {
		text := Settings{SleepSeconds: v}.Text(SleepSeconds)
		back, err := strconv.ParseFloat(text, 64)
		if err != nil || back != v || strings.ContainsAny(text, "eE") {
			t.Errorf("Text(%v) = %q reads back %v, %v", v, text, back, err)
		}
	}
}

// TestKeys: the eight settings, in the order help and status list them (the two group bounds
// follow check_interval, so the six existing lines keep their places), and each has an effect the
// reply and the log can say.
func TestKeys(t *testing.T) {
	want := []Key{BatchSize, BatchDeleteSize, SleepSeconds, DeleteSleepSeconds, SecondsBehindSourceWithin, CheckInterval, GroupApplierQueueWithin, GroupCertifierQueueWithin}
	if got := Keys(); !slices.Equal(got, want) {
		t.Fatalf("Keys() = %v", got)
	}
	for _, k := range Keys() {
		if k.Effect() == "" {
			t.Errorf("%s has no effect text", k)
		}
	}
	if Key("cache_ttl").Effect() != "" {
		t.Error("cache_ttl is not a setting")
	}
	if _, ok := ParseKey("cache_ttl"); ok {
		t.Error("cache_ttl parsed as a key")
	}
	if k, ok := ParseKey("batch_size"); !ok || k != BatchSize {
		t.Error("batch_size did not parse")
	}
	for _, k := range []Key{GroupApplierQueueWithin, GroupCertifierQueueWithin} {
		if got, ok := ParseKey(string(k)); !ok || got != k {
			t.Errorf("%s did not parse", k)
		}
		if k.Effect() != "from the next gate decision" {
			t.Errorf("%s takes effect %q, want the other gate settings' words", k, k.Effect())
		}
	}
}

// TestEnded: the store says whether the terminal transition has happened, so that status can
// answer `ending` from one truth, without the writer lock (TEL-1 C5, spec §4.8).
func TestEnded(t *testing.T) {
	s := newStore()
	if s.Ended() {
		t.Fatal("a new store is ended")
	}
	mustSet(t, s, BatchSize, "500")
	if s.Ended() {
		t.Error("a setter ended the store")
	}
	s.Terminal()
	if !s.Ended() {
		t.Error("the store is not ended after its terminal transition")
	}
	s.Terminal()
	if !s.Ended() {
		t.Error("a second terminal transition un-ended the store")
	}
}
