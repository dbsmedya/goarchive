package control

import (
	"sync"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// The run's configured values: the ceilings of the two sizes and the configured tolerance.
var testStart = tuning.Settings{
	BatchSize: 1000, BatchDeleteSize: 500,
	SleepSeconds: 1, DeleteSleepSeconds: 0,
	SecondsBehindSourceWithin: 10, CheckInterval: 5,
}

var (
	// archiveApplies is an archive or purge run with the replication gate.
	archiveApplies = tuning.Applicability{Delete: true, Gate: true}
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
