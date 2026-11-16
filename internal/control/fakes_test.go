package control

import (
	"sync"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// The run's configured values: the ceilings of the two sizes, the configured tolerance and the two group bounds.
var testStart = tuning.Settings{
	BatchSize: 1000, BatchDeleteSize: 500,
	SleepSeconds: 1, DeleteSleepSeconds: 0,
	SecondsBehindSourceWithin: 10, CheckInterval: 5,
	GroupApplierQueueWithin: 1000, GroupCertifierQueueWithin: 300,
}

var (
	// archiveApplies is an archive or purge run with the replication gate and asynchronous servers only.
	archiveApplies = tuning.Applicability{Delete: true, Gate: true}
	// groupApplies is an archive or purge run whose gate also has a server of type group.
	groupApplies = tuning.Applicability{Delete: true, Gate: true, Group: true}
	// noGateApplies is an archive run without replication.
	noGateApplies = tuning.Applicability{Delete: true, Gate: false}
	// copyOnlyApplies is a copy-only run: nothing is deleted, and no gate is used.
	copyOnlyApplies = tuning.Applicability{}
)

var testPeer = tuning.Peer{UID: 1000, PID: 4242}

// stopRecorder is the RequestStop a store is given: it counts its calls.
type stopRecorder struct {
	mu sync.Mutex
	n  int
}

func (r *stopRecorder) request() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.n++
}

func (r *stopRecorder) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n
}

// newTestStore returns a store of a run of the given kind at testStart, with a recording
// RequestStop. A test may change its options.
func newTestStore(t *testing.T, applies tuning.Applicability, opts ...func(*tuning.Options)) (*tuning.Store, *stopRecorder) {
	t.Helper()
	rec := &stopRecorder{}
	o := tuning.Options{Start: testStart, Applies: applies, RequestStop: rec.request}
	for _, f := range opts {
		f(&o)
	}
	return tuning.New(o), rec
}

// fakeView is the status the command table asks for, in place of the real rendering.
type fakeView struct {
	lines []string
	json  string
}

func (v fakeView) text() []string { return v.lines }

func (v fakeView) jsonLine() (string, error) { return v.json, nil }

// fakeTracker, fakeGate and fakeShutdown are the sources a status is read from; each answers what
// the test set.
type fakeTracker struct {
	mu sync.Mutex
	s  TrackerStatus
}

func (f *fakeTracker) Status() TrackerStatus {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.s
}

func (f *fakeTracker) set(s TrackerStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.s = s
}

type fakeGate struct {
	mu sync.Mutex
	r  GateRecord
	n  int
}

func (f *fakeGate) Record() GateRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	return f.r
}

func (f *fakeGate) set(r GateRecord) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.r = r
}

type fakeShutdown struct {
	mu      sync.Mutex
	stopped bool
}

func (f *fakeShutdown) StopRequested() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stopped
}

func (f *fakeShutdown) set(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopped = v
}

// sinkEntry is one entry the emitter wrote.
type sinkEntry struct {
	level tuning.Level
	msg   string
	kv    map[string]any
}

// recSink is the Sink a test gives the emitter: it records what was written and, when told, holds
// its writer shut, so that a test can see what the rest of the server does while the log is not
// keeping up. It is test code: an observer with a gate.
type recSink struct {
	mu      sync.Mutex
	entries []sinkEntry
	hold    chan struct{}
	reached chan struct{}
	closed  bool
	late    int
}

func newRecSink() *recSink { return &recSink{reached: make(chan struct{}, 1024)} }

// block makes the next writes wait for release.
func (s *recSink) block() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hold = make(chan struct{})
}

func (s *recSink) release() {
	s.mu.Lock()
	h := s.hold
	s.hold = nil
	s.mu.Unlock()
	if h != nil {
		close(h)
	}
}

// closeLog marks the log as closed: a write after it is counted as late.
func (s *recSink) closeLog() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
}

func (s *recSink) Entry(level tuning.Level, msg string, kv ...any) {
	s.mu.Lock()
	h := s.hold
	s.mu.Unlock()
	if h != nil {
		s.reached <- struct{}{}
		<-h
	}
	fields := map[string]any{}
	for i := 0; i+1 < len(kv); i += 2 {
		if k, ok := kv[i].(string); ok {
			fields[k] = kv[i+1]
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		s.late++
	}
	s.entries = append(s.entries, sinkEntry{level, msg, fields})
}

func (s *recSink) snapshot() []sinkEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]sinkEntry(nil), s.entries...)
}

func (s *recSink) messages() []string {
	var out []string
	for _, e := range s.snapshot() {
		out = append(out, e.msg)
	}
	return out
}

// recBanner is the Banner a test gives the emitter.
type recBanner struct {
	mu    sync.Mutex
	lines []string
}

func (b *recBanner) banner(line string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lines = append(b.lines, line)
}

func (b *recBanner) got() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.lines...)
}
