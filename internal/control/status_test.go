package control

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// The status (spec §4.8): the state word by its precedence, the counters, the fields of
// status-json and the labelled lines of status, read from the four sources without a write.

var (
	statusStart = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	statusNow   = statusStart.Add(6*time.Minute + 2*time.Second)
)

// statusFixture is a run of 6m02s that is held for 2m10s by a replica 42 s behind a tolerance of
// 10, as the spec's example is, with batch_delete_size lowered from 500 to 100.
type statusFixture struct {
	store    *tuning.Store
	tracker  *fakeTracker
	gate     *fakeGate
	shutdown *fakeShutdown
	view     *view
}

func newStatusFixture(t *testing.T, applies tuning.Applicability) *statusFixture {
	t.Helper()
	store, _ := newTestStore(t, applies)
	f := &statusFixture{store: store, tracker: &fakeTracker{}, gate: &fakeGate{}, shutdown: &fakeShutdown{}}
	f.tracker.set(TrackerStatus{
		Phase: PhaseRunning, Step: "delete", Table: "order_details",
		Batches: 13, Roots: 12000, RowsCopied: 64920, RowsDeleted: 61500,
		Tables: []TableCounts{{Name: "orders", Copied: 12000, Deleted: 11000}, {Name: "order_details", Copied: 52920, Deleted: 50500}},
	})
	lag := int64(42)
	f.gate.set(GateRecord{
		HoldingSince: statusNow.Add(-2*time.Minute - 10*time.Second),
		LastPass:     statusNow.Add(-5 * time.Minute),
		Servers: []ServerVerdict{{
			Server: "replica1.internal:3306", Cause: "unhealthy", Detail: "channel <default>: lag=42s tolerance=10s",
			Lag: &lag, Tolerance: 10, CheckedAt: statusNow.Add(-3 * time.Second),
		}},
	})
	f.view = &view{
		info: Info{
			Version: "3.1.0", Job: "archive_old_orders", JobType: "archive",
			ConfigFile: "/etc/goarchive/archiver.yaml", PID: 4242, StartedAt: statusStart,
			ReplicationEnabled: true, CacheTTL: 15,
		},
		socket:   "/home/archiver/.goarchive/run/archive_old_orders.sock",
		store:    store,
		tracker:  f.tracker,
		gate:     f.gate,
		shutdown: f.shutdown,
		now:      func() time.Time { return statusNow },
	}
	return f
}

func (f *statusFixture) json(t *testing.T) map[string]any {
	t.Helper()
	line, err := f.view.jsonLine()
	if err != nil {
		t.Fatal(err)
	}
	if strings.ContainsAny(line, "\n\r") {
		t.Fatalf("status-json is not one line: %q", line)
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(line), &out); err != nil {
		t.Fatalf("status-json is not JSON: %v\n%s", err, line)
	}
	return out
}

// TestStatusStates (spec §4.8): one state word, by this precedence: ending, stopping, held,
// paused, recovering, running, starting. A pause requested while a batch is in flight shows
// `running` with the pause listed as requested; `paused` means the run is actually waiting.
func TestStatusStates(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(f *statusFixture)
		want  string
	}{
		{"starting", func(f *statusFixture) { f.tracker.set(TrackerStatus{Phase: PhaseStarting}); f.gate.set(GateRecord{}) }, "starting"},
		{"running", func(f *statusFixture) { f.gate.set(GateRecord{}) }, "running"},
		{"recovering", func(f *statusFixture) {
			f.tracker.set(TrackerStatus{Phase: PhaseRecovering})
			f.gate.set(GateRecord{})
		}, "recovering"},
		{"paused", func(f *statusFixture) {
			f.tracker.set(TrackerStatus{Phase: PhaseRunning, WaitingSince: statusNow.Add(-time.Minute)})
			f.gate.set(GateRecord{})
		}, "paused"},
		{"paused beats recovering", func(f *statusFixture) {
			f.tracker.set(TrackerStatus{Phase: PhaseRecovering, WaitingSince: statusNow.Add(-time.Minute)})
			f.gate.set(GateRecord{})
		}, "paused"},
		{"held", func(f *statusFixture) {}, "held"},
		{"held beats paused", func(f *statusFixture) {
			f.tracker.set(TrackerStatus{Phase: PhaseRunning, WaitingSince: statusNow.Add(-time.Minute)})
		}, "held"},
		{"stopping by a socket stop", func(f *statusFixture) {
			if _, err := f.store.Stop(testPeer); err != nil {
				t.Fatal(err)
			}
		}, "stopping"},
		{"stopping by a signal", func(f *statusFixture) { f.shutdown.set(true) }, "stopping"},
		{"ending", func(f *statusFixture) { f.shutdown.set(true); f.store.Terminal() }, "ending"},
	} {
		f := newStatusFixture(t, archiveApplies)
		tc.setup(f)
		if got := f.json(t)["state"]; got != tc.want {
			t.Errorf("%s: state = %v, want %s", tc.name, got, tc.want)
		}
		if first := f.view.text()[0]; !strings.Contains(first, ": "+tc.want) {
			t.Errorf("%s: the summary line %q does not carry the state word", tc.name, first)
		}
	}
}

// TestStatusPauseRequested: a socket pause set while a batch is in flight is listed as requested
// and the state stays `running`; once the loop is waiting the state is `paused` and since when.
func TestStatusPauseRequested(t *testing.T) {
	f := newStatusFixture(t, archiveApplies)
	f.gate.set(GateRecord{})
	if _, err := f.store.Pause(testPeer); err != nil {
		t.Fatal(err)
	}
	j := f.json(t)
	pause := j["pause"].(map[string]any)
	if j["state"] != "running" || !sameStrings(pause["requested"], "socket") || pause["waiting_since"] != nil {
		t.Errorf("a pause requested in flight: state %v, pause %v, want running, [socket], waiting_since null", j["state"], pause)
	}
	f.tracker.set(TrackerStatus{Phase: PhaseRunning, WaitingSince: statusNow.Add(-90 * time.Second), Sentinel: true})
	j = f.json(t)
	pause = j["pause"].(map[string]any)
	if j["state"] != "paused" || !sameStrings(pause["requested"], "socket", "sentinel") || pause["waiting_since"] != statusNow.Add(-90*time.Second).Format(time.RFC3339) {
		t.Errorf("a waiting loop: state %v, pause %v, want paused, [socket sentinel], waiting_since", j["state"], pause)
	}
}

func sameStrings(v any, want ...string) bool {
	list, ok := v.([]any)
	if !ok || len(list) != len(want) {
		return false
	}
	for i := range want {
		if list[i] != want[i] {
			return false
		}
	}
	return true
}

// TestStatusJSON (spec §4.8, §7 row 8's JSON half): one object with every field the spec names,
// the protocol version among them, times in RFC 3339 UTC and durations in seconds.
func TestStatusJSON(t *testing.T) {
	f := newStatusFixture(t, archiveApplies)
	f.view.info.SentinelFile = "/var/run/goarchive/pause"
	if _, err := f.store.Set(tuning.BatchDeleteSize, "100", testPeer); err != nil {
		t.Fatal(err)
	}
	f.tracker.set(TrackerStatus{
		Phase: PhaseRunning, Step: "delete", Table: "order_details",
		Batches: 13, Roots: 12000, RootsSkipped: 2, RootsRecovered: 7, RowsCopied: 64920, RowsDeleted: 61500,
		Tables:   []TableCounts{{Name: "orders", Copied: 12000, Deleted: 11000}},
		Estimate: &Estimate{Roots: 20000, Percent: 60, ETA: 4 * time.Minute, ETAKnown: true},
	})
	j := f.json(t)

	for key, want := range map[string]any{
		"protocol": float64(1), "version": "3.1.0", "job": "archive_old_orders", "job_type": "archive",
		"config_file": "/etc/goarchive/archiver.yaml", "pid": float64(4242),
		"socket":     "/home/archiver/.goarchive/run/archive_old_orders.sock",
		"started_at": "2026-11-15T12:00:00Z", "now": "2026-11-15T12:06:02Z", "elapsed_seconds": float64(362),
		"state": "held", "stop_requested": false, "changes": float64(1),
	} {
		if j[key] != want {
			t.Errorf("%s = %v (%T), want %v", key, j[key], j[key], want)
		}
	}
	if got := j["activity"]; !equalJSON(got, map[string]any{"step": "delete", "table": "order_details"}) {
		t.Errorf("activity = %v", got)
	}
	if got := j["counters"]; !equalJSON(got, map[string]any{"batches": 13.0, "roots": 12000.0, "roots_skipped": 2.0, "roots_recovered": 7.0, "rows_copied": 64920.0, "rows_deleted": 61500.0}) {
		t.Errorf("counters = %v", got)
	}
	if got := j["tables"]; !equalJSON(got, map[string]any{"orders": map[string]any{"copied": 12000.0, "deleted": 11000.0}}) {
		t.Errorf("tables = %v", got)
	}
	if got := j["progress"]; !equalJSON(got, map[string]any{"roots_estimate": 20000.0, "percent": 60.0, "eta": 240.0}) {
		t.Errorf("progress = %v", got)
	}
	if got := j["pause"]; !equalJSON(got, map[string]any{"requested": []any{}, "waiting_since": nil, "sentinel_file": "/var/run/goarchive/pause"}) {
		t.Errorf("pause = %v", got)
	}
	hold := j["hold"].(map[string]any)
	if hold["since"] != "2026-11-15T12:03:52Z" {
		t.Errorf("hold.since = %v", hold["since"])
	}
	if servers := hold["servers"].([]any); len(servers) != 1 || servers[0].(map[string]any)["server"] != "replica1.internal:3306" {
		t.Errorf("hold.servers = %v", hold["servers"])
	}

	settings := j["settings"].(map[string]any)
	if len(settings) != 6 {
		t.Errorf("settings has %d entries, want the six a gated archive run accepts: %v", len(settings), settings)
	}
	if got := settings["batch_delete_size"]; !equalJSON(got, map[string]any{"value": 100.0, "configured": 500.0}) {
		t.Errorf("batch_delete_size = %v", got)
	}
	if got := settings["sleep_seconds"]; !equalJSON(got, map[string]any{"value": 1.0, "configured": 1.0}) {
		t.Errorf("sleep_seconds = %v", got)
	}
	if _, has := settings["cache_ttl"]; has {
		t.Error("settings lists cache_ttl, which is no setting")
	}

	repl := j["replication"].(map[string]any)
	if repl["enabled"] != true || repl["used"] != true || repl["cache_ttl"] != 15.0 || repl["last_pass_at"] != "2026-11-15T12:01:02Z" {
		t.Errorf("replication = %v", repl)
	}
	server := repl["servers"].([]any)[0].(map[string]any)
	for key, want := range map[string]any{
		"server": "replica1.internal:3306", "healthy": false, "cause": "unhealthy",
		"detail": "channel <default>: lag=42s tolerance=10s", "lag_seconds": 42.0, "tolerance": 10.0,
		"checked_at": "2026-11-15T12:05:59Z",
	} {
		if server[key] != want {
			t.Errorf("replication.servers[0].%s = %v, want %v", key, server[key], want)
		}
	}
}

func equalJSON(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

// TestStatusJSONNulls: what a run does not have is null, not absent: no hold, no estimate, no wait,
// a server never probed, a lag that is NULL.
func TestStatusJSONNulls(t *testing.T) {
	f := newStatusFixture(t, archiveApplies)
	f.tracker.set(TrackerStatus{Phase: PhaseStarting})
	f.gate.set(GateRecord{Servers: []ServerVerdict{{Server: "replica1:3306"}}})
	j := f.json(t)
	for _, key := range []string{"hold", "progress", "activity"} {
		if v, has := j[key]; !has || v != nil {
			t.Errorf("%s = %v (present %v), want null", key, v, has)
		}
	}
	repl := j["replication"].(map[string]any)
	if repl["last_pass_at"] != nil {
		t.Errorf("last_pass_at = %v, want null", repl["last_pass_at"])
	}
	server := repl["servers"].([]any)[0].(map[string]any)
	if server["checked_at"] != nil || server["lag_seconds"] != nil {
		t.Errorf("a server never probed: %v, want checked_at and lag_seconds null", server)
	}
	if pause := j["pause"].(map[string]any); pause["waiting_since"] != nil {
		t.Errorf("waiting_since = %v, want null", pause["waiting_since"])
	} else if _, has := pause["sentinel_file"]; has {
		t.Error("sentinel_file is present for a run without one")
	}
}

// TestStatusSettingsFollowApplicability (spec §4.4): the snapshot's settings hold only the settings
// the run accepts, and the replication object says whether the gate is used.
func TestStatusSettingsFollowApplicability(t *testing.T) {
	for _, tc := range []struct {
		name    string
		applies tuning.Applicability
		want    int
		used    bool
	}{
		{"all", archiveApplies, 6, true},
		{"no gate", noGateApplies, 4, false},
		{"copy-only", copyOnlyApplies, 2, false},
	} {
		f := newStatusFixture(t, tc.applies)
		if !tc.used {
			f.view.gate = nil
		}
		j := f.json(t)
		if got := len(j["settings"].(map[string]any)); got != tc.want {
			t.Errorf("%s: %d settings, want %d", tc.name, got, tc.want)
		}
		if got := j["replication"].(map[string]any)["used"]; got != tc.used {
			t.Errorf("%s: replication.used = %v, want %v", tc.name, got, tc.used)
		}
	}
}

// TestStatusText (spec §4.8): the first body line is the summary and the rest is the same content
// in labelled lines. The spec's example lines come first, as it shows them and in its order; the
// content its JSON has that the example does not show follows them: the replication gate, the
// changes accepted, and the per-table counts. (progress is one more line, when there is an
// estimate; TestStatusTextOtherStates shows it.)
func TestStatusText(t *testing.T) {
	f := newStatusFixture(t, archiveApplies)
	if _, err := f.store.Set(tuning.BatchDeleteSize, "100", testPeer); err != nil {
		t.Fatal(err)
	}
	got := strings.Join(f.view.text(), "\n")
	want := strings.Join([]string{
		"archive_old_orders (archive): held 2m10s, batch 13, roots 12000, copied 64920, deleted 61500, elapsed 6m02s",
		"  run:       pid 4242, config /etc/goarchive/archiver.yaml",
		"  socket:    /home/archiver/.goarchive/run/archive_old_orders.sock",
		"  activity:  delete on order_details",
		"  hold:      replica1.internal:3306 unhealthy (channel <default>: lag=42s tolerance=10s), checked 3s ago",
		"  pause:     none",
		"  settings:  batch_size 1000   batch_delete_size 100 (configured 500)   sleep_seconds 1   delete_sleep_seconds 0",
		"             seconds_behind_source_within 10   check_interval 5",
		"  replication: enabled, used, cache_ttl 15s, last pass 5m00s ago (2026-11-15T12:01:02Z)",
		"               replica1.internal:3306 unhealthy (channel <default>: lag=42s tolerance=10s), checked 3s ago (2026-11-15T12:05:59Z)",
		"  changes:   1 accepted runtime change",
		"  tables:    orders copied 12000, deleted 11000",
		"             order_details copied 52920, deleted 50500",
	}, "\n")
	if got != want {
		t.Errorf("status text:\n%s\nwant:\n%s", got, want)
	}
}

// TestStatusTextReplication: the replication line says whether the replication block is enabled and
// whether this run uses the gate; for a run that does, the configured cache_ttl, when the last pass
// was, and one line per server with its last verdict and when it was checked.
func TestStatusTextReplication(t *testing.T) {
	f := newStatusFixture(t, archiveApplies)
	lag := int64(0)
	checked := statusNow.Add(-3 * time.Second)
	f.gate.set(GateRecord{Servers: []ServerVerdict{
		{Server: "replica2:3306", Healthy: true, Lag: &lag, Tolerance: 10, CheckedAt: checked},
		{Server: "replica3:3306", Cause: "unreachable", Detail: "dial tcp: connection refused", Tolerance: 10, CheckedAt: checked},
		{Server: "replica4:3306"},
	}})
	text := strings.Join(f.view.text(), "\n")
	for _, want := range []string{
		"  replication: enabled, used, cache_ttl 15s, last pass never",
		"               replica2:3306 healthy, lag 0s, tolerance 10s, checked 3s ago (2026-11-15T12:05:59Z)",
		"               replica3:3306 unreachable (dial tcp: connection refused), checked 3s ago (2026-11-15T12:05:59Z)",
		"               replica4:3306 not checked yet",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("status text lacks %q:\n%s", want, text)
		}
	}

	// Enabled in the file, but this run does not use the gate (a copy-only job): said so.
	f = newStatusFixture(t, copyOnlyApplies)
	f.view.gate = nil
	if got := strings.Join(f.view.text(), "\n"); !strings.Contains(got, "  replication: enabled, not used by this run") {
		t.Errorf("a run that does not use the gate:\n%s", got)
	}
	f.view.info.ReplicationEnabled = false
	if got := strings.Join(f.view.text(), "\n"); !strings.Contains(got, "  replication: not enabled") {
		t.Errorf("a run without replication:\n%s", got)
	}
}

// TestStatusTextTail: after the spec's example lines come replication, changes, progress (only when
// there is an estimate) and tables (only when there is a table), in that order; a run that has
// accepted nothing says `none`.
func TestStatusTextTail(t *testing.T) {
	f := newStatusFixture(t, noGateApplies)
	f.view.gate = nil
	f.tracker.set(TrackerStatus{Phase: PhaseRunning, Estimate: &Estimate{Roots: 20000, Percent: 60, ETA: 4 * time.Minute, ETAKnown: true},
		Tables: []TableCounts{{Name: "orders", Copied: 1, Deleted: 2}}})
	text := strings.Join(f.view.text(), "\n")
	last := -1
	for _, label := range []string{"  settings:", "  replication:", "  changes:   none", "  progress:  about 20000 roots, 60%, ETA 4m00s", "  tables:    orders copied 1, deleted 2"} {
		at := strings.Index(text, label)
		if at < 0 || at < last {
			t.Fatalf("%q is not after the lines before it (at %d, after %d):\n%s", label, at, last, text)
		}
		last = at
	}

	// Without an estimate and without a table those two lines are absent.
	f.tracker.set(TrackerStatus{Phase: PhaseRunning})
	text = strings.Join(f.view.text(), "\n")
	for _, label := range []string{"  progress:", "  tables:"} {
		if strings.Contains(text, label) {
			t.Errorf("status text has %q with nothing to say:\n%s", label, text)
		}
	}
	if !strings.Contains(text, "  changes:   none") {
		t.Errorf("a run with no change:\n%s", text)
	}
	if _, err := f.store.Set(tuning.BatchSize, "10", testPeer); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Pause(testPeer); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(f.view.text(), "\n"); !strings.Contains(got, "  changes:   2 accepted runtime changes") {
		t.Errorf("two accepted changes:\n%s", got)
	}
}

// TestStatusTextOtherStates: a run that is not held says so in its words, and the optional lines
// appear only when there is something to say.
func TestStatusTextOtherStates(t *testing.T) {
	f := newStatusFixture(t, noGateApplies)
	f.view.gate = nil
	f.tracker.set(TrackerStatus{
		Phase: PhaseRunning, WaitingSince: statusNow.Add(-4 * time.Minute), Sentinel: true,
		Estimate: &Estimate{Roots: 20000, Percent: 60, ETA: 4 * time.Minute, ETAKnown: true},
	})
	if _, err := f.store.Pause(testPeer); err != nil {
		t.Fatal(err)
	}
	text := strings.Join(f.view.text(), "\n")
	for _, want := range []string{
		"paused 4m00s,",
		"  activity:  none",
		"  hold:      none",
		"  pause:     waiting for 4m00s (control socket, sentinel file)",
		"  progress:  about 20000 roots, 60%, ETA 4m00s",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("status text lacks %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "seconds_behind_source_within") {
		t.Errorf("status text lists a gate setting for a run without the gate:\n%s", text)
	}

	f = newStatusFixture(t, noGateApplies)
	f.view.gate = nil
	if _, err := f.store.Pause(testPeer); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(f.view.text(), "\n"), "  pause:     requested (control socket)") {
		t.Errorf("a pause not yet waited on: %s", strings.Join(f.view.text(), "\n"))
	}
}

// TestStatusIsPure (spec §4.8): a status read changes nothing and asks each source once.
func TestStatusIsPure(t *testing.T) {
	f := newStatusFixture(t, archiveApplies)
	before := f.store.Load()
	f.json(t)
	f.view.text()
	if f.store.Load() != before {
		t.Error("a status read replaced the store's snapshot")
	}
	if f.gate.n != 2 {
		t.Errorf("the gate's record was read %d times for two renderings, want 2", f.gate.n)
	}
}

// TestFormatDuration: the status shows whole seconds, as 6m02s and 2m10s; under a minute just the
// seconds, from an hour the hours too.
func TestFormatDuration(t *testing.T) {
	for _, tc := range []struct {
		d    time.Duration
		want string
	}{
		{0, "0s"}, {900 * time.Millisecond, "0s"}, {5 * time.Second, "5s"}, {59 * time.Second, "59s"},
		{time.Minute, "1m00s"}, {2*time.Minute + 10*time.Second, "2m10s"}, {6*time.Minute + 2*time.Second, "6m02s"},
		{time.Hour, "1h00m00s"}, {3*time.Hour + 4*time.Minute + 5*time.Second, "3h04m05s"}, {-time.Second, "0s"},
	} {
		if got := formatDuration(tc.d); got != tc.want {
			t.Errorf("formatDuration(%v) = %q, want %q", tc.d, got, tc.want)
		}
	}
}

// TestStatusEstimateWithoutAnETA: an estimate whose ETA cannot be told yet (no root done, or past the
// estimate) says so, not 0s: status-json has `eta` null and the text says the ETA is unknown.
func TestStatusEstimateWithoutAnETA(t *testing.T) {
	f := newStatusFixture(t, noGateApplies)
	f.view.gate = nil
	f.tracker.set(TrackerStatus{Phase: PhaseRunning, Estimate: &Estimate{Roots: 20000, Percent: 0}})
	progress := f.json(t)["progress"].(map[string]any)
	if progress["roots_estimate"] != 20000.0 || progress["percent"] != 0.0 {
		t.Errorf("progress = %v", progress)
	}
	if v, has := progress["eta"]; !has || v != nil {
		t.Errorf("eta = %v (present %v), want null", v, has)
	}
	if text := strings.Join(f.view.text(), "\n"); !strings.Contains(text, "  progress:  about 20000 roots, 0%, ETA unknown") {
		t.Errorf("status text:\n%s\nwant the ETA said to be unknown", text)
	}
}
