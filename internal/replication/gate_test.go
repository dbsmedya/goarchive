package replication

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
	_ "github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/logger"
)

// newTestLogger returns a real file-backed JSON logger plus a reader that
// decodes what it has written so far. It deliberately uses the existing logger
// API — a test-only export in a production package is reachable by nothing and
// fails the dead-code guard.
func newTestLogger(t *testing.T) (*logger.Logger, func() []map[string]any) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "gate.log")
	log, err := logger.New(&config.LoggingConfig{
		Level:  "debug",
		Format: "json",
		Output: path,
		// FileOnly keeps the assertions off the console stream.
		FileOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })

	lines := func() []map[string]any {
		_ = log.Sync()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var out []map[string]any
		for _, l := range bytes.Split(data, []byte("\n")) {
			if len(l) == 0 {
				continue
			}
			var m map[string]any
			if err := json.Unmarshal(l, &m); err != nil {
				t.Fatalf("non-JSON log line %q: %v", l, err)
			}
			out = append(out, m)
		}

		return out
	}

	return log, lines
}

// msgsAt returns every message logged at one level, in order.
func msgsAt(lines []map[string]any, level string) []string {
	var out []string
	for _, m := range lines {
		if m["level"] != level {
			continue
		}
		if s, ok := m["msg"].(string); ok {
			out = append(out, s)
		}
	}

	return out
}

// hasMsg reports whether an exact message was logged at that level.
func hasMsg(lines []map[string]any, level, msg string) bool {
	for _, s := range msgsAt(lines, level) {
		if s == msg {
			return true
		}
	}

	return false
}

// openIdleDB returns a *sql.DB that has never dialed anything: sql.Open only
// validates the DSN, so no server is involved.
func openIdleDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("mysql", "gate:gate@tcp(127.0.0.1:1)/gate")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return db
}

func TestGateNewDisabled(t *testing.T) {
	log, lines := newTestLogger(t)

	g, err := New(config.ReplicationConfig{Enabled: false}, nil, log)
	if err != nil {
		t.Fatalf("New returned %v, want nil", err)
	}
	if g == nil {
		t.Fatal("New returned a nil Gate")
	}
	if g.enabled {
		t.Error("gate.enabled = true, want false")
	}

	decoded := lines()
	const want = "Replication monitoring is DISABLED (no replication block enabled)"
	if !hasMsg(decoded, "info", want) {
		t.Errorf("info lines = %#v, want one equal to %q", msgsAt(decoded, "info"), want)
	}
	for _, m := range msgsAt(decoded, "info") {
		if strings.Contains(m, "ENABLED:") {
			t.Errorf("logged %q, want no startup summary for a disabled gate", m)
		}
	}
}

func TestGateNewCountMismatch(t *testing.T) {
	log, _ := newTestLogger(t)

	cfg := config.ReplicationConfig{
		Enabled: true,
		Servers: []config.ReplicationServerConfig{
			{Host: "10.0.0.1", Port: 3306},
			{Host: "10.0.0.2", Port: 3306},
		},
	}

	g, err := New(cfg, []*sql.DB{openIdleDB(t)}, log)
	if err == nil {
		t.Fatal("New returned nil error, want a construction error")
	}
	if g != nil {
		t.Error("New returned a non-nil Gate alongside an error")
	}
	const want = "replication: 2 server configs but 1 connections"
	if err.Error() != want {
		t.Errorf("error = %q, want %q", err.Error(), want)
	}
}

func TestGateNewNilHandle(t *testing.T) {
	log, _ := newTestLogger(t)

	cfg := config.ReplicationConfig{
		Enabled: true,
		Servers: []config.ReplicationServerConfig{
			{Host: "10.0.0.1", Port: 3306},
			{Host: "::1", Port: 3307},
		},
	}

	g, err := New(cfg, []*sql.DB{openIdleDB(t), nil}, log)
	if err == nil {
		t.Fatal("New returned nil error, want a construction error")
	}
	if g != nil {
		t.Error("New returned a non-nil Gate alongside an error")
	}
	if !strings.Contains(err.Error(), "[::1]:3307") {
		t.Errorf("error = %q, want it to name %q", err.Error(), "[::1]:3307")
	}
}

func TestGateNewNilLoggerTolerated(t *testing.T) {
	g, err := New(config.ReplicationConfig{Enabled: false}, nil, nil)
	if err != nil {
		t.Fatalf("New returned %v, want nil", err)
	}
	if g.logger == nil {
		t.Error("gate.logger is nil, want the default logger")
	}
}

func TestGateNewStartupSummary(t *testing.T) {
	log, lines := newTestLogger(t)

	cfg := config.ReplicationConfig{
		Enabled:                   true,
		SecondsBehindSourceWithin: 10,
		CheckInterval:             5,
		CacheTTL:                  15,
		Servers: []config.ReplicationServerConfig{
			{Host: "10.0.0.1", Port: 3306},
			{Host: "::1", Port: 3307, Channels: []string{"", "billing"}},
		},
	}

	g, err := New(cfg, []*sql.DB{openIdleDB(t), openIdleDB(t)}, log)
	if err != nil {
		t.Fatalf("New returned %v, want nil", err)
	}

	if !g.enabled {
		t.Error("gate.enabled = false, want true")
	}
	if g.tolerance != 10 {
		t.Errorf("gate.tolerance = %d, want 10", g.tolerance)
	}
	if g.interval != 5*time.Second {
		t.Errorf("gate.interval = %s, want 5s", g.interval)
	}
	if g.ttl != 15*time.Second {
		t.Errorf("gate.ttl = %s, want 15s", g.ttl)
	}
	if g.now == nil || g.sleep == nil {
		t.Error("gate.now/gate.sleep are nil, want defaulted seams")
	}
	if len(g.servers) != 2 {
		t.Fatalf("gate.servers = %d, want 2", len(g.servers))
	}
	if g.servers[0].id != "10.0.0.1:3306" || g.servers[1].id != "[::1]:3307" {
		t.Errorf("server ids = %q/%q, want %q/%q",
			g.servers[0].id, g.servers[1].id, "10.0.0.1:3306", "[::1]:3307")
	}
	if len(g.servers[0].channels) != 0 {
		t.Errorf("server 0 channels = %#v, want none (all channels)", g.servers[0].channels)
	}
	if len(g.servers[1].channels) != 2 {
		t.Errorf("server 1 channels = %#v, want two", g.servers[1].channels)
	}
	if g.servers[0].reader == nil || g.servers[1].reader == nil {
		t.Error("gate.servers carry nil readers, want an inspector each")
	}

	decoded := lines()
	for _, want := range []string{
		"Replication monitoring ENABLED: 2 server(s), tolerance 10s, interval 5s, cache TTL 15s",
		"  server 10.0.0.1:3306 channels: all",
		"  server [::1]:3307 channels: <default>, billing",
	} {
		if !hasMsg(decoded, "info", want) {
			t.Errorf("info lines = %#v, want one equal to %q", msgsAt(decoded, "info"), want)
		}
	}
	if len(msgsAt(decoded, "error")) != 0 {
		t.Errorf("error lines = %#v, want none", msgsAt(decoded, "error"))
	}
}

// --- WaitForLag fixtures -----------------------------------------------------
//
// Everything below is deterministic: no goroutines, no wall clock, no
// time.Sleep. The clock and sleep seams are injected, so a whole hold-and-
// recover scenario runs in microseconds.

// readerResult is one scripted ReplicaStatus response. before, when set, runs
// immediately before the response is returned — the cancellation case uses it
// to kill the context mid-read.
type readerResult struct {
	snapshot []dbsrepl.ChannelStatus
	err      error
	before   func()
}

func okRes(snapshot []dbsrepl.ChannelStatus) readerResult {
	return readerResult{snapshot: snapshot}
}

func errRes(err error) readerResult {
	return readerResult{err: err}
}

// fakeReader pops one scripted response per call and repeats its final
// response once the script is exhausted. Every script below therefore ends in
// a response that lets the loop terminate: a repeating failure under a live
// context is a hang, and a test bug by definition.
type fakeReader struct {
	responses []readerResult
	calls     int
}

func (r *fakeReader) ReplicaStatus(context.Context) ([]dbsrepl.ChannelStatus, error) {
	i := r.calls
	if i >= len(r.responses) {
		i = len(r.responses) - 1
	}
	r.calls++

	res := r.responses[i]
	if res.before != nil {
		res.before()
	}

	return res.snapshot, res.err
}

// fakeClock is the injected now() seam.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

type gateHarness struct {
	gate       *Gate
	clock      *fakeClock
	sleepCalls int
	lines      func() []map[string]any
}

// newGateHarness builds a Gate over scripted readers. Tolerance is 10s and the
// interval 5s throughout, so every held duration in the assertions is an exact
// multiple of one tick.
func newGateHarness(t *testing.T, ttl time.Duration, readers ...*fakeReader) *gateHarness {
	t.Helper()

	log, lines := newTestLogger(t)
	h := &gateHarness{
		clock: &fakeClock{t: time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)},
		lines: lines,
	}

	servers := make([]server, 0, len(readers))
	for i, r := range readers {
		servers = append(servers, server{id: fmt.Sprintf("replica%d:3306", i+1), reader: r})
	}

	h.gate = &Gate{
		enabled:   true,
		servers:   servers,
		tolerance: 10,
		interval:  5 * time.Second,
		ttl:       ttl,
		now:       h.clock.now,
		sleep:     h.advance,
		logger:    log,
	}

	// Case 11, enforced for every harness-based scenario: zap attaches a stack
	// trace at Error level, so no line in the gate may be logged at one.
	t.Cleanup(func() {
		if errs := msgsAt(h.lines(), "error"); len(errs) != 0 {
			t.Errorf("error-level lines = %#v, want none anywhere in the gate", errs)
		}
	})

	return h
}

// advance is the default injected sleep: it advances the fake clock, counts
// the call, and reports whether the context died. Nothing blocks.
func (h *gateHarness) advance(ctx context.Context, d time.Duration) error {
	h.sleepCalls++
	h.clock.t = h.clock.t.Add(d)

	return ctx.Err()
}

func (h *gateHarness) tick(d time.Duration) { h.clock.t = h.clock.t.Add(d) }

func healthySnapshot() []dbsrepl.ChannelStatus {
	return []dbsrepl.ChannelStatus{ch("", "Yes", "Yes", 0, true)}
}

func stoppedSnapshot() []dbsrepl.ChannelStatus {
	return []dbsrepl.ChannelStatus{ch("", "Yes", "No", 0, true)}
}

// countWith counts the messages containing sub.
func countWith(msgs []string, sub string) int {
	n := 0
	for _, m := range msgs {
		if strings.Contains(m, sub) {
			n++
		}
	}

	return n
}

// Task 8's disabled-gate contract, completed here: an inert gate answers
// without reading anything.
func TestGateWaitDisabled(t *testing.T) {
	log, lines := newTestLogger(t)

	g, err := New(config.ReplicationConfig{Enabled: false}, nil, log)
	if err != nil {
		t.Fatalf("New returned %v, want nil", err)
	}
	if err := g.WaitForLag(context.Background()); err != nil {
		t.Errorf("WaitForLag on a disabled gate = %v, want nil", err)
	}
	if n := len(msgsAt(lines(), "warn")); n != 0 {
		t.Errorf("warn lines = %d, want none", n)
	}
}

// Case 1: a passing verdict inside the TTL costs zero round trips.
func TestGateWaitCacheHit(t *testing.T) {
	reader := &fakeReader{responses: []readerResult{okRes(healthySnapshot())}}
	h := newGateHarness(t, 15*time.Second, reader)

	if err := h.gate.WaitForLag(context.Background()); err != nil {
		t.Fatalf("first WaitForLag = %v, want nil", err)
	}
	if reader.calls != 1 {
		t.Fatalf("reader calls = %d after the first pass, want 1", reader.calls)
	}

	h.tick(time.Second)
	if err := h.gate.WaitForLag(context.Background()); err != nil {
		t.Fatalf("second WaitForLag = %v, want nil", err)
	}
	if reader.calls != 1 {
		t.Errorf("reader calls = %d within the TTL, want 1", reader.calls)
	}
	if h.sleepCalls != 0 {
		t.Errorf("sleep calls = %d, want 0", h.sleepCalls)
	}
}

// Case 2: past the TTL the gate reads again.
func TestGateWaitCacheExpiry(t *testing.T) {
	reader := &fakeReader{responses: []readerResult{okRes(healthySnapshot())}}
	h := newGateHarness(t, 15*time.Second, reader)

	if err := h.gate.WaitForLag(context.Background()); err != nil {
		t.Fatalf("first WaitForLag = %v, want nil", err)
	}

	h.tick(16 * time.Second)
	if err := h.gate.WaitForLag(context.Background()); err != nil {
		t.Fatalf("second WaitForLag = %v, want nil", err)
	}
	if reader.calls != 2 {
		t.Errorf("reader calls = %d after the TTL expired, want 2", reader.calls)
	}
}

// Case 3: cache_ttl 0 restores check-every-time behavior.
func TestGateWaitCacheDisabled(t *testing.T) {
	reader := &fakeReader{responses: []readerResult{okRes(healthySnapshot())}}
	h := newGateHarness(t, 0, reader)

	for i := range 2 {
		if err := h.gate.WaitForLag(context.Background()); err != nil {
			t.Fatalf("WaitForLag %d = %v, want nil", i+1, err)
		}
	}
	if reader.calls != 2 {
		t.Errorf("reader calls = %d with cache_ttl 0, want 2", reader.calls)
	}
}

// Case 4: a failure is never cached, and the documented bounded detection
// delay is exactly one TTL — the cached pass conceals the new failure until it
// expires, after which the failure is observed and then recovers.
func TestGateWaitFailureNeverCached(t *testing.T) {
	reader := &fakeReader{responses: []readerResult{
		okRes(healthySnapshot()), // call A
		okRes(stoppedSnapshot()), // call C, tick 1
		okRes(healthySnapshot()), // call C, tick 2
	}}
	h := newGateHarness(t, 15*time.Second, reader)

	if err := h.gate.WaitForLag(context.Background()); err != nil {
		t.Fatalf("call A = %v, want nil", err)
	}
	if reader.calls != 1 {
		t.Fatalf("reader calls = %d after call A, want 1", reader.calls)
	}

	h.tick(time.Second)
	if err := h.gate.WaitForLag(context.Background()); err != nil {
		t.Fatalf("call B = %v, want nil", err)
	}
	if reader.calls != 1 {
		t.Errorf("reader calls = %d for the cached call B, want 1", reader.calls)
	}
	if n := len(msgsAt(h.lines(), "warn")); n != 0 {
		t.Errorf("warn lines = %d after the cached call, want none", n)
	}

	h.tick(15 * time.Second)
	if err := h.gate.WaitForLag(context.Background()); err != nil {
		t.Fatalf("call C = %v, want nil", err)
	}
	if reader.calls != 3 {
		t.Errorf("reader calls = %d after call C, want 3", reader.calls)
	}

	decoded := h.lines()
	if n := countWith(msgsAt(decoded, "warn"), "replication hold:"); n != 1 {
		t.Errorf("hold lines = %d, want 1", n)
	}
	if n := countWith(msgsAt(decoded, "info"), "replication recovered:"); n != 1 {
		t.Errorf("recovery lines = %d, want 1", n)
	}
	if n := countWith(msgsAt(decoded, "info"), "replication gate passed:"); n != 1 {
		t.Errorf("fleet resume lines = %d, want 1", n)
	}
}

// Case 5: hold then recover on a single-server fleet — the exact line shapes,
// and BOTH recovery lines.
func TestGateWaitHoldThenRecover(t *testing.T) {
	reader := &fakeReader{responses: []readerResult{
		okRes(stoppedSnapshot()),
		okRes(stoppedSnapshot()),
		okRes(healthySnapshot()),
	}}
	h := newGateHarness(t, 15*time.Second, reader)

	if err := h.gate.WaitForLag(context.Background()); err != nil {
		t.Fatalf("WaitForLag = %v, want nil", err)
	}

	decoded := h.lines()
	wantWarns := []string{
		"replication hold: server replica1:3306 unhealthy (channel <default>: io=Yes sql=No); job held, retrying in 5s",
		"replication hold: server replica1:3306 unhealthy (channel <default>: io=Yes sql=No); job held, retrying in 5s (held 5s)",
	}
	if got := msgsAt(decoded, "warn"); !reflect.DeepEqual(got, wantWarns) {
		t.Errorf("warn lines =\n%#v\nwant\n%#v", got, wantWarns)
	}
	wantInfos := []string{
		"replication recovered: server replica1:3306 healthy; held 10s",
		"replication gate passed: all 1 servers healthy; job resuming after 10s held",
	}
	if got := msgsAt(decoded, "info"); !reflect.DeepEqual(got, wantInfos) {
		t.Errorf("info lines =\n%#v\nwant\n%#v", got, wantInfos)
	}
}

// Case 6: a healthy server is silent while its sick peer holds the fleet.
func TestGateWaitTwoServersOneSick(t *testing.T) {
	a := &fakeReader{responses: []readerResult{okRes(healthySnapshot())}}
	b := &fakeReader{responses: []readerResult{okRes(stoppedSnapshot()), okRes(healthySnapshot())}}
	h := newGateHarness(t, 15*time.Second, a, b)

	if err := h.gate.WaitForLag(context.Background()); err != nil {
		t.Fatalf("WaitForLag = %v, want nil", err)
	}

	decoded := h.lines()
	all := append(msgsAt(decoded, "warn"), msgsAt(decoded, "info")...)
	if n := countWith(all, "replica1:3306"); n != 0 {
		t.Errorf("lines naming the healthy server = %d, want 0 (%#v)", n, all)
	}
	if got := msgsAt(decoded, "warn"); len(got) != 1 {
		t.Errorf("warn lines = %#v, want exactly one", got)
	}
	wantInfos := []string{
		"replication recovered: server replica2:3306 healthy; held 5s",
		"replication gate passed: all 2 servers healthy; job resuming after 5s held",
	}
	if got := msgsAt(decoded, "info"); !reflect.DeepEqual(got, wantInfos) {
		t.Errorf("info lines =\n%#v\nwant\n%#v", got, wantInfos)
	}
}

// Case 7: one server recovers while another still holds; the gate returns only
// when the whole fleet is healthy.
func TestGateWaitRecoveryWhileAnotherHolds(t *testing.T) {
	a := &fakeReader{responses: []readerResult{
		okRes(stoppedSnapshot()), okRes(stoppedSnapshot()), okRes(healthySnapshot()),
	}}
	b := &fakeReader{responses: []readerResult{
		okRes(stoppedSnapshot()), okRes(healthySnapshot()), okRes(healthySnapshot()),
	}}
	h := newGateHarness(t, 15*time.Second, a, b)

	if err := h.gate.WaitForLag(context.Background()); err != nil {
		t.Fatalf("WaitForLag = %v, want nil", err)
	}
	if a.calls != 3 || b.calls != 3 {
		t.Errorf("reader calls = %d/%d, want 3/3", a.calls, b.calls)
	}

	decoded := h.lines()
	warns := msgsAt(decoded, "warn")
	if len(warns) != 3 {
		t.Fatalf("warn lines = %#v, want three", warns)
	}
	if !strings.Contains(warns[2], "replica1:3306") || !strings.Contains(warns[2], "(held 5s)") {
		t.Errorf("third warn = %q, want the ongoing hold for replica1", warns[2])
	}
	wantInfos := []string{
		"replication recovered: server replica2:3306 healthy; held 5s",
		"replication recovered: server replica1:3306 healthy; held 10s",
		"replication gate passed: all 2 servers healthy; job resuming after 10s held",
	}
	if got := msgsAt(decoded, "info"); !reflect.DeepEqual(got, wantInfos) {
		t.Errorf("info lines =\n%#v\nwant\n%#v", got, wantInfos)
	}
}

// Case 8: a cause change reports the new cause and the previous one, and does
// NOT reset the held duration.
func TestGateWaitCauseChange(t *testing.T) {
	dial := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	reader := &fakeReader{responses: []readerResult{
		okRes(stoppedSnapshot()),
		errRes(dial),
		okRes(healthySnapshot()),
	}}
	h := newGateHarness(t, 15*time.Second, reader)

	if err := h.gate.WaitForLag(context.Background()); err != nil {
		t.Fatalf("WaitForLag = %v, want nil", err)
	}

	decoded := h.lines()
	warns := msgsAt(decoded, "warn")
	if len(warns) != 2 {
		t.Fatalf("warn lines = %#v, want two", warns)
	}
	if !strings.Contains(warns[1], "unreachable") {
		t.Errorf("second warn = %q, want the new cause", warns[1])
	}
	if !strings.Contains(warns[1], "(was: unhealthy; held 5s)") {
		t.Errorf("second warn = %q, want %q", warns[1], "(was: unhealthy; held 5s)")
	}
	wantInfos := []string{
		"replication recovered: server replica1:3306 healthy; held 10s",
		"replication gate passed: all 1 servers healthy; job resuming after 10s held",
	}
	if got := msgsAt(decoded, "info"); !reflect.DeepEqual(got, wantInfos) {
		t.Errorf("info lines =\n%#v\nwant\n%#v", got, wantInfos)
	}
}

// Case 9: a relapse inside one call is a fresh hold — new entry line, and the
// held duration restarts from it.
func TestGateWaitRelapseWithinOneCall(t *testing.T) {
	a := &fakeReader{responses: []readerResult{
		okRes(stoppedSnapshot()),
		okRes(healthySnapshot()),
		okRes(stoppedSnapshot()),
		okRes(healthySnapshot()),
	}}
	b := &fakeReader{responses: []readerResult{
		okRes(stoppedSnapshot()),
		okRes(stoppedSnapshot()),
		okRes(stoppedSnapshot()),
		okRes(healthySnapshot()),
	}}
	h := newGateHarness(t, 15*time.Second, a, b)

	if err := h.gate.WaitForLag(context.Background()); err != nil {
		t.Fatalf("WaitForLag = %v, want nil", err)
	}
	if a.calls != 4 || b.calls != 4 {
		t.Errorf("reader calls = %d/%d, want 4/4", a.calls, b.calls)
	}

	decoded := h.lines()
	warns := msgsAt(decoded, "warn")
	infos := msgsAt(decoded, "info")

	// An entry line carries no "(held ...)" suffix; an ongoing one does.
	entryA, ongoingA := 0, 0
	for _, m := range warns {
		if !strings.Contains(m, "replica1:3306") {
			continue
		}
		if strings.Contains(m, "held ") {
			ongoingA++
		} else {
			entryA++
		}
	}
	if entryA != 2 {
		t.Errorf("entry holds for replica1 = %d, want 2 (%#v)", entryA, warns)
	}
	if ongoingA != 0 {
		t.Errorf("ongoing holds for replica1 = %d, want 0 (%#v)", ongoingA, warns)
	}

	entryB := 0
	for _, m := range warns {
		if strings.Contains(m, "replica2:3306") && !strings.Contains(m, "held ") {
			entryB++
		}
	}
	if entryB != 1 {
		t.Errorf("entry holds for replica2 = %d, want 1 (%#v)", entryB, warns)
	}

	wantInfos := []string{
		"replication recovered: server replica1:3306 healthy; held 5s",
		"replication recovered: server replica1:3306 healthy; held 5s",
		"replication recovered: server replica2:3306 healthy; held 15s",
		"replication gate passed: all 2 servers healthy; job resuming after 15s held",
	}
	if !reflect.DeepEqual(infos, wantInfos) {
		t.Errorf("info lines =\n%#v\nwant\n%#v", infos, wantInfos)
	}
}

// Case 10: cancellation observed right after a read returns — no
// classification, no hold line, and sleep is never reached.
func TestGateWaitCancelAfterRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := &fakeReader{responses: []readerResult{{
		snapshot: stoppedSnapshot(),
		before:   cancel,
	}}}
	h := newGateHarness(t, 15*time.Second, reader)

	err := h.gate.WaitForLag(ctx)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitForLag = %v, want context.Canceled", err)
	}
	if reader.calls != 1 {
		t.Errorf("reader calls = %d, want 1", reader.calls)
	}
	decoded := h.lines()
	if n := len(msgsAt(decoded, "warn")); n != 0 {
		t.Errorf("warn lines = %#v, want none — the read was aborted, not classified",
			msgsAt(decoded, "warn"))
	}
	if h.sleepCalls != 0 {
		t.Errorf("sleep calls = %d, want 0", h.sleepCalls)
	}
}

// Case 10b: cancellation during the inter-tick sleep leaves the loop through
// the sleep seam, which is genuinely exercised.
func TestGateWaitCancelDuringSleep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := &fakeReader{responses: []readerResult{okRes(stoppedSnapshot())}}
	h := newGateHarness(t, 15*time.Second, reader)
	h.gate.sleep = func(context.Context, time.Duration) error {
		h.sleepCalls++
		cancel()

		return context.Canceled
	}

	err := h.gate.WaitForLag(ctx)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitForLag = %v, want context.Canceled", err)
	}
	if h.sleepCalls != 1 {
		t.Errorf("sleep calls = %d, want 1", h.sleepCalls)
	}
	if n := len(msgsAt(h.lines(), "warn")); n != 1 {
		t.Errorf("warn lines = %d, want the single entry hold", n)
	}
}

// Case 11, stated non-vacuously: a scenario that logs plenty still logs
// nothing at Error level, whatever the cause.
func TestGateWaitNeverLogsErrorLevel(t *testing.T) {
	reader := &fakeReader{responses: []readerResult{
		errRes(&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}),
		errRes(errors.New("malformed packet")),
		okRes(stoppedSnapshot()),
		okRes(healthySnapshot()),
	}}
	h := newGateHarness(t, 15*time.Second, reader)

	if err := h.gate.WaitForLag(context.Background()); err != nil {
		t.Fatalf("WaitForLag = %v, want nil", err)
	}

	decoded := h.lines()
	if n := len(msgsAt(decoded, "warn")); n != 3 {
		t.Errorf("warn lines = %#v, want three", msgsAt(decoded, "warn"))
	}
	if n := len(msgsAt(decoded, "error")); n != 0 {
		t.Errorf("error lines = %#v, want none", msgsAt(decoded, "error"))
	}
	for _, m := range decoded {
		if _, ok := m["stacktrace"]; ok {
			t.Errorf("log line %v carries a stack trace, want none", m)
		}
	}
}
