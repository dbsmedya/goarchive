package tuning

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
)

// Snapshot is the store's state at one revision. It is immutable: a reader loads one with a
// single atomic read, decides by it alone, and keeps no lock. A change builds the next snapshot
// and closes this one's signal.
type Snapshot struct {
	Settings

	// Revision rises with every accepted change. GateRevision rises only when one of the two
	// gate settings changes: a cached verdict is valid only for the gate revision it was judged
	// under.
	Revision     uint64
	GateRevision uint64

	// Paused is the socket pause; PausedSince is when it was set. Stopping is a stop request.
	Paused      bool
	PausedSince time.Time
	Stopping    bool

	changed chan struct{}
}

// Changed returns a channel that closes when this snapshot is replaced. A waiter loads a
// snapshot, computes its wait from it, and waits on that snapshot's signal beside its timer: if
// the snapshot was replaced between the load and the wait, the signal is already closed and the
// wait returns at once, so no change falls into a gap.
func (s *Snapshot) Changed() <-chan struct{} { return s.changed }

// Options builds a store.
type Options struct {
	// Start is the run's configured values: the ceilings of the two sizes, and the configured
	// tolerance that the step rule is measured by.
	Start Settings
	// Applies says which settings the run uses.
	Applies Applicability
	// AuditSlots is the audit queue's capacity; the default is 256. It is a constant of the
	// design, not a setting.
	AuditSlots int
	// RequestStop asks for the first phase of a stop, exactly as the first signal does. It is
	// called once, under the writer lock, and must return at once.
	RequestStop func()
	// Now is the clock; the default is time.Now.
	Now func() time.Time
}

// defaultAuditSlots is the audit queue's size.
const defaultAuditSlots = 256

// Store holds the current snapshot, the starting values, the bounded summary and the audit
// queue. The engine and the gate read it; only the control server writes it (D13).
type Store struct {
	start       Settings
	applies     Applicability
	audit       *Audit
	requestStop func()
	now         func() time.Time

	cur atomic.Pointer[Snapshot]

	// round is the newest gate revision a completed probe round was judged under, plus one: zero
	// means the gate has completed no round. The gate writes it without the writer lock, and the
	// step rule reads it under that lock.
	round atomic.Uint64

	// ended is set by the terminal transition, under the writer lock, and read without it.
	ended atomic.Bool

	// mu is the writer lock: every mutation and the terminal transition take it.
	mu        sync.Mutex
	terminal  bool
	frozen    *Summary
	changes   map[Key]*changeCount
	pauses    int
	pausedFor time.Duration
}

type changeCount struct {
	configured string
	final      string
	n          int
}

// New returns a store at its starting values, revision 0.
func New(o Options) *Store {
	slots := o.AuditSlots
	if slots < 1 {
		slots = defaultAuditSlots
	}
	s := &Store{
		start:       o.Start,
		applies:     o.Applies,
		audit:       NewAudit(slots),
		requestStop: o.RequestStop,
		now:         o.Now,
		changes:     map[Key]*changeCount{},
	}
	if s.now == nil {
		s.now = time.Now
	}
	s.cur.Store(&Snapshot{Settings: o.Start, changed: make(chan struct{})})
	return s
}

// Constant returns a store that nothing writes: a run without a control socket reads it, and
// its behaviour is the configuration's.
func Constant(start Settings) *Store {
	return New(Options{Start: start, Applies: Applicability{Delete: true, Gate: true}, AuditSlots: 1})
}

// Ended reports whether the terminal transition has happened: from then on no change is accepted
// and status answers `ending`. It takes no lock.
func (s *Store) Ended() bool { return s.ended.Load() }

// Load returns the current snapshot.
func (s *Store) Load() *Snapshot { return s.cur.Load() }

// Start returns the starting values.
func (s *Store) Start() Settings { return s.start }

// RoundCompleted tells the store that the gate completed a probe round judged under gate revision
// rev, whether the round passed or failed. It is the input of D15's one step per probe round:
// a raise above the configured tolerance is accepted only when a round completed under the
// current gate revision. The gate never takes the writer lock; this is one atomic write, and
// only the newest revision is kept, so a round that ends late and was judged under an older
// revision cannot take back what a newer one gave.
func (s *Store) RoundCompleted(rev uint64) {
	for {
		old := s.round.Load()
		if old > rev || s.round.CompareAndSwap(old, rev+1) {
			return
		}
	}
}

// roundUnder reports whether a probe round completed under gate revision rev.
func (s *Store) roundUnder(rev uint64) bool { return s.round.Load() == rev+1 }

// Audit returns the queue of accepted changes' events.
func (s *Store) Audit() *Audit { return s.audit }

// Applies reports whether the run uses a setting and, when it does not, why.
func (s *Store) Applies(k Key) (bool, string) { return s.applies.applies(k) }

// ApplicableKeys returns the settings the run uses, in listing order.
func (s *Store) ApplicableKeys() []Key {
	var out []Key
	for _, k := range Keys() {
		if ok, _ := s.applies.applies(k); ok {
			out = append(out, k)
		}
	}
	return out
}

// Change is the outcome of an accepted setter.
type Change struct {
	// Unchanged: the value equalled the current one; nothing else happened, and Revision is 0.
	Unchanged bool
	Revision  uint64
	Key       Key
	// Old is the value the change actually replaced, New the value now in force.
	Old, New string
	// Effect says when it takes effect.
	Effect string
}

// PauseResult is the outcome of pause and resume. Changed is false when the request found the
// state it asked for; Since is when the socket pause was set.
type PauseResult struct {
	Changed bool
	Since   time.Time
}

// StopResult is the outcome of stop: Already is a stop that was requested before.
type StopResult struct {
	Changed bool
	Already bool
}

// publish builds the next snapshot from the latest one, replaces the current one and closes its
// signal. The caller holds the writer lock.
func (s *Store) publish(change func(*Snapshot), gate bool) *Snapshot {
	old := s.cur.Load()
	next := *old
	change(&next)
	next.Revision = old.Revision + 1
	if gate {
		next.GateRevision++
	}
	next.changed = make(chan struct{})
	s.cur.Store(&next)
	close(old.changed)
	return &next
}

// Set changes one setting to the number text names. In this order: the key's applicability
// (which depends on nothing that changes during a run), then under the writer lock: the end of
// the run, the value (the number's form, the file's own validators, the ceilings, the step rule),
// an equal value, a place in the audit queue; then the next snapshot, the summary and the audit
// event. A refusal changes nothing.
func (s *Store) Set(k Key, text string, peer Peer) (Change, error) {
	if ok, why := s.applies.applies(k); !ok {
		return Change{}, refuse(ReasonInapplicable, why)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminal {
		return Change{}, errEnding
	}
	cur := s.cur.Load()
	next, err := s.candidate(cur, k, text)
	if err != nil {
		return Change{}, err
	}
	old, now := cur.Text(k), next.Text(k)
	if next == cur.Settings {
		return Change{Unchanged: true, Key: k, Old: old, New: old, Effect: k.Effect()}, nil
	}
	if !s.audit.Reserve() {
		return Change{}, errBusy
	}
	at := s.now()
	snap := s.publish(func(n *Snapshot) { n.Settings = next }, k.gate())
	c := s.changes[k]
	if c == nil {
		c = &changeCount{configured: s.start.Text(k)}
		s.changes[k] = c
	}
	c.final, c.n = now, c.n+1
	s.audit.Put(Event{Kind: KindSet, Revision: snap.Revision, Key: k, Old: old, New: now, Peer: peer, At: at})
	return Change{Revision: snap.Revision, Key: k, Old: old, New: now, Effect: k.Effect()}, nil
}

// candidate returns the settings with k set to text, or the refusal. It passes the value
// through the functions the file is validated with, so that the file and the socket cannot
// drift apart.
func (s *Store) candidate(snap *Snapshot, k Key, text string) (Settings, *Refusal) {
	cur := snap.Settings
	if !k.whole() {
		v, bad := parseDecimal(k, text)
		if bad != nil {
			return cur, bad
		}
		validate := config.ValidateSleepSeconds
		if k == DeleteSleepSeconds {
			validate = config.ValidateDeleteSleepSeconds
		}
		if err := validate(v); err != nil {
			return cur, refuse(ReasonInvalid, err.Error())
		}
		return cur.withDecimal(k, v), nil
	}
	v, bad := parseWhole(k, text)
	if bad != nil {
		return cur, bad
	}
	var err error
	switch k {
	case BatchSize:
		err = config.ValidateBatchSize(v)
	case BatchDeleteSize:
		err = config.ValidateBatchDeleteSize(v)
	case SecondsBehindSourceWithin:
		err = config.ValidateSecondsBehindSourceWithin(v)
	case CheckInterval:
		err = config.ValidateCheckInterval(v)
	}
	if err != nil {
		return cur, refuse(ReasonInvalid, err.Error())
	}
	if ceiling := s.start.whole(k); k.size() && v > ceiling {
		return cur, refuse(ReasonCeiling, fmt.Sprintf("%s %d is above this run's configured %d; the control socket never raises this setting above the run's configuration: change the file and run the job again", k, v, ceiling))
	}
	if k == SecondsBehindSourceWithin {
		if r := s.raise(snap, v); r != nil {
			return cur, r
		}
	}
	return cur.withWhole(k, v), nil
}

// raise judges a new lag tolerance by D15's step rule (spec §4.6). Lowering is always accepted,
// down to 0, and so is a raise up to the configured value, which restores the configuration. A
// raise above the configured value needs both: a probe round completed under the current gate
// revision, so the replicas were judged at the present tolerance before the next step; and one
// step, at most the current value plus the configured tolerance (1 when that is 0), or, from a
// value below the configuration, whichever is higher of the configured value and that sum. A
// value equal to the current one is no raise: the caller answers it as unchanged.
func (s *Store) raise(snap *Snapshot, v int) *Refusal {
	cur, configured := snap.SecondsBehindSourceWithin, s.start.SecondsBehindSourceWithin
	if v <= cur || v <= configured {
		return nil
	}
	if !s.roundUnder(snap.GateRevision) {
		return refuse(ReasonStep, fmt.Sprintf("%s %d is above the configured %d; the replicas have not yet been checked under the current tolerance %d, and a raise above the configuration is accepted only after the gate has completed a probe round under it", SecondsBehindSourceWithin, v, configured, cur))
	}
	step, why := configured, fmt.Sprintf("the configured %d is the step", configured)
	if configured == 0 {
		step, why = 1, "the configured tolerance is 0, so the step is 1"
	}
	if limit := max(configured, saturatingAdd(cur, step)); v > limit {
		return refuse(ReasonStep, fmt.Sprintf("%s %d is more than one step above the current %d; the largest value accepted now is %d (%s)", SecondsBehindSourceWithin, v, cur, limit, why))
	}
	return nil
}

// saturatingAdd is a + b for non-negative a and b, at most the largest int: a sum that would
// wrap is the largest int, so a limit built from it is never smaller than its parts.
func saturatingAdd(a, b int) int {
	if a > math.MaxInt-b {
		return math.MaxInt
	}
	return a + b
}

// Pause sets the socket pause. The batch or page in flight finishes first; the engine's pause
// gate waits at the places it waits today.
func (s *Store) Pause(peer Peer) (PauseResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminal {
		return PauseResult{}, errEnding
	}
	cur := s.cur.Load()
	if cur.Paused {
		return PauseResult{Since: cur.PausedSince}, nil
	}
	if !s.audit.Reserve() {
		return PauseResult{}, errBusy
	}
	at := s.now()
	snap := s.publish(func(n *Snapshot) { n.Paused, n.PausedSince = true, at }, false)
	s.pauses++
	s.audit.Put(Event{Kind: KindPause, Revision: snap.Revision, Peer: peer, At: at})
	return PauseResult{Changed: true, Since: at}, nil
}

// Resume clears the socket pause. Whether a sentinel file still holds the run is the engine's
// to say; the store knows only its own flag.
func (s *Store) Resume(peer Peer) (PauseResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminal {
		return PauseResult{}, errEnding
	}
	cur := s.cur.Load()
	if !cur.Paused {
		return PauseResult{}, nil
	}
	if !s.audit.Reserve() {
		return PauseResult{}, errBusy
	}
	at := s.now()
	snap := s.publish(func(n *Snapshot) { n.Paused, n.PausedSince = false, time.Time{} }, false)
	s.pausedFor += at.Sub(cur.PausedSince)
	s.audit.Put(Event{Kind: KindResume, Revision: snap.Revision, Peer: peer, At: at})
	return PauseResult{Changed: true, Since: cur.PausedSince}, nil
}

// Stop requests the first phase of a graceful stop, once, through the function the store was
// given: the same stop as the first signal. A second stop is "already stopping". No socket
// command cancels a run.
func (s *Store) Stop(peer Peer) (StopResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminal {
		return StopResult{}, errEnding
	}
	if s.cur.Load().Stopping {
		return StopResult{Already: true}, nil
	}
	if !s.audit.Reserve() {
		return StopResult{}, errBusy
	}
	at := s.now()
	snap := s.publish(func(n *Snapshot) { n.Stopping = true }, false)
	s.audit.Put(Event{Kind: KindStop, Revision: snap.Revision, Peer: peer, At: at})
	if s.requestStop != nil {
		s.requestStop()
	}
	return StopResult{Changed: true}, nil
}

// SettingSummary is one setting that changed during the run.
type SettingSummary struct {
	Key Key
	// Configured is the value the run started with, Final the value in force at the end.
	Configured, Final string
	// Changes is how many accepted changes the setting had; a setting changed and then restored
	// is still listed.
	Changes int
	// AboveConfigured marks a lag tolerance that ends above its configured value.
	AboveConfigured bool
}

// Summary is the run's runtime changes, bounded by the six settings however often they were
// changed: each change already has its own audit entry.
type Summary struct {
	Settings  []SettingSummary
	Pauses    int
	PausedFor time.Duration
}

// Terminal ends the run's acceptance of changes and returns the frozen summary. It takes the
// writer lock, so every change accepted before it is in the summary and nothing is accepted
// after it; it waits for no output writer. A later call returns the same summary.
func (s *Store) Terminal() Summary {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.terminal {
		s.terminal = true
		s.ended.Store(true)
		cur := s.cur.Load()
		if cur.Paused {
			s.pausedFor += s.now().Sub(cur.PausedSince)
		}
		sum := Summary{Pauses: s.pauses, PausedFor: s.pausedFor}
		for _, k := range keys {
			c := s.changes[k]
			if c == nil {
				continue
			}
			sum.Settings = append(sum.Settings, SettingSummary{
				Key: k, Configured: c.configured, Final: c.final, Changes: c.n,
				AboveConfigured: k == SecondsBehindSourceWithin && cur.SecondsBehindSourceWithin > s.start.SecondsBehindSourceWithin,
			})
		}
		s.frozen = &sum
	}
	out := *s.frozen
	out.Settings = append([]SettingSummary(nil), out.Settings...)
	return out
}
