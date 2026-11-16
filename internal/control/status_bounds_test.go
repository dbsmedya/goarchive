package control

import (
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// What `status` and `status-json` say of the two group bounds as settings (GR C7s; spec §4.7, §8 *Status of the
// bounds*). All additions, so protocol stays 1: `settings` holds the two with value and configured in a run where they
// apply; `replication.group_*_within` are the values in force, read from the same snapshot as `settings.*.value`, and
// the configured values where the gate is unused; each group verdict carries the bounds it was judged by, which differ
// from the values in force between a change and the next probe.

// boundsFixture is a gated run with a group server: the store applies the bounds at the fixture's configured 1000 and
// 300, and the gate's record is one replica, a healthy group server, a group server whose read failed and one never
// probed. Their bounds are what each was judged by.
func boundsFixture(t *testing.T) *statusFixture {
	t.Helper()
	f := newStatusFixture(t, groupApplies)
	f.view.info.GroupApplierQueueWithin, f.view.info.GroupCertifierQueueWithin = ip(1000), ip(300)
	checked := statusNow.Add(-3 * time.Second)
	f.gate.set(GateRecord{
		HoldingSince: statusNow.Add(-90 * time.Second), LastPass: statusNow.Add(-time.Minute),
		Servers: []ServerVerdict{
			{Server: "replica1:3306", Type: "async", Healthy: true, Lag: i64(0), Tolerance: 10, CheckedAt: checked},
			{Server: "gr2:3306", Type: "group", Healthy: true, Tolerance: 10, CheckedAt: checked, ApplierQueue: i64(12), CertifierQueue: i64(3),
				ApplierQueueWithin: i64(1000), CertifierQueueWithin: i64(300)},
			{Server: "gr3:3306", Type: "group", Cause: "unreadable", Detail: "denied", Tolerance: 10, CheckedAt: checked,
				ApplierQueueWithin: i64(1000), CertifierQueueWithin: i64(300)},
			{Server: "gr4:3306", Type: "group"},
		},
	})
	return f
}

func settingOf(t *testing.T, j map[string]any, key string) map[string]any {
	t.Helper()
	s, ok := j["settings"].(map[string]any)[key].(map[string]any)
	if !ok {
		t.Fatalf("settings has no %s: %v", key, j["settings"])
	}
	return s
}

// TestStatusJSONSaysTheBoundsInForce: the settings entries and the replication values are the values in force, from one
// snapshot, after every change; settings also say the configured value.
func TestStatusJSONSaysTheBoundsInForce(t *testing.T) {
	f := boundsFixture(t)
	check := func(when string, applier, certifier float64) {
		t.Helper()
		j := f.json(t)
		repl := j["replication"].(map[string]any)
		if repl["group_applier_queue_within"] != applier || repl["group_certifier_queue_within"] != certifier {
			t.Errorf("%s: replication bounds = %v and %v, want the values in force %v and %v", when, repl["group_applier_queue_within"], repl["group_certifier_queue_within"], applier, certifier)
		}
		a, c := settingOf(t, j, "group_applier_queue_within"), settingOf(t, j, "group_certifier_queue_within")
		if a["value"] != applier || a["configured"] != 1000.0 || c["value"] != certifier || c["configured"] != 300.0 {
			t.Errorf("%s: settings = %v and %v, want value %v and %v against the configured 1000 and 300", when, a, c, applier, certifier)
		}
		if a["value"] != repl["group_applier_queue_within"] || c["value"] != repl["group_certifier_queue_within"] {
			t.Errorf("%s: replication and settings disagree: %v and %v against %v and %v", when, repl["group_applier_queue_within"], repl["group_certifier_queue_within"], a["value"], c["value"])
		}
	}
	check("at the start", 1000, 300)
	for _, step := range []struct {
		key    tuning.Key
		text   string
		a, c   float64
		change string
	}{
		{tuning.GroupApplierQueueWithin, "400", 400, 300, "a lowering"},
		{tuning.GroupCertifierQueueWithin, "0", 400, 0, "a lowering to 0"},
		{tuning.GroupApplierQueueWithin, "1000", 1000, 0, "a restore"},
	} {
		if _, err := f.store.Set(step.key, step.text, testPeer); err != nil {
			t.Fatalf("%s: %v", step.change, err)
		}
		check("after "+step.change, step.a, step.c)
	}
}

// TestStatusJSONNamesTheBoundsEachVerdictWasJudgedBy: a group server's verdict carries applier_queue_within and
// certifier_queue_within, which stay what the probe used when the setting changes after it; an asynchronous server and a
// group server not probed have null for both; a group server whose read failed has the pair its probe was judged by.
func TestStatusJSONNamesTheBoundsEachVerdictWasJudgedBy(t *testing.T) {
	f := boundsFixture(t)
	if _, err := f.store.Set(tuning.GroupApplierQueueWithin, "400", testPeer); err != nil {
		t.Fatal(err)
	}
	servers := f.json(t)["replication"].(map[string]any)["servers"].([]any)
	for i, want := range []struct{ applier, certifier any }{{nil, nil}, {1000.0, 300.0}, {1000.0, 300.0}, {nil, nil}} {
		s := servers[i].(map[string]any)
		a, aHas := s["applier_queue_within"]
		c, cHas := s["certifier_queue_within"]
		if !aHas || !cHas || a != want.applier || c != want.certifier {
			t.Errorf("replication.servers[%d] bounds = %v (present %v) and %v (present %v), want %v and %v", i, a, aHas, c, cHas, want.applier, want.certifier)
		}
	}
	// the value in force moved on; the verdict still says what it was judged by
	if got := f.json(t)["replication"].(map[string]any)["group_applier_queue_within"]; got != 400.0 {
		t.Errorf("the bound in force = %v, want 400", got)
	}

	// the hold carries the same fields
	f.gate.set(GateRecord{HoldingSince: statusNow.Add(-time.Minute), Servers: []ServerVerdict{
		{Server: "gr2:3306", Type: "group", Cause: "unhealthy", Detail: "queue", Tolerance: 10, CheckedAt: statusNow.Add(-time.Second),
			ApplierQueue: i64(500), CertifierQueue: i64(0), ApplierQueueWithin: i64(400), CertifierQueueWithin: i64(300)},
	}})
	hold := f.json(t)["hold"].(map[string]any)["servers"].([]any)[0].(map[string]any)
	if hold["applier_queue_within"] != 400.0 || hold["certifier_queue_within"] != 300.0 {
		t.Errorf("hold.servers[0] = %v, want the bounds it was judged by", hold)
	}
}

// TestStatusBoundsFollowTheGateUse: where the gate is used and lists a group server the bounds are settings and the
// values in force; where the gate lists none they are null and no setting; where the gate is unused (a copy-only job, or
// replication not enabled) but a group server is listed, `replication` keeps the configured values and no setting says
// them.
func TestStatusBoundsFollowTheGateUse(t *testing.T) {
	for _, tc := range []struct {
		name      string
		applies   tuning.Applicability
		setup     func(f *statusFixture)
		settings  int
		bounds    any // replication.group_applier_queue_within
		certifier any
	}{
		{"a gate with a group server", groupApplies, func(f *statusFixture) {
			f.view.info.GroupApplierQueueWithin, f.view.info.GroupCertifierQueueWithin = ip(1000), ip(300)
		}, 8, 1000.0, 300.0},
		{"a gate with asynchronous servers only", archiveApplies, func(f *statusFixture) {}, 6, nil, nil},
		{"a copy-only job with a group server listed", copyOnlyApplies, func(f *statusFixture) {
			f.view.gate = nil
			f.view.info.GroupApplierQueueWithin, f.view.info.GroupCertifierQueueWithin = ip(1000), ip(300)
		}, 2, 1000.0, 300.0},
		{"replication not enabled with a group server listed", noGateApplies, func(f *statusFixture) {
			f.view.gate = nil
			f.view.info.ReplicationEnabled = false
			f.view.info.GroupApplierQueueWithin, f.view.info.GroupCertifierQueueWithin = ip(1000), ip(300)
		}, 4, 1000.0, 300.0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newStatusFixture(t, tc.applies)
			tc.setup(f)
			j := f.json(t)
			repl := j["replication"].(map[string]any)
			if repl["group_applier_queue_within"] != tc.bounds || repl["group_certifier_queue_within"] != tc.certifier {
				t.Errorf("replication bounds = %v and %v, want %v and %v", repl["group_applier_queue_within"], repl["group_certifier_queue_within"], tc.bounds, tc.certifier)
			}
			settings := j["settings"].(map[string]any)
			if len(settings) != tc.settings {
				t.Errorf("settings has %d entries, want %d: %v", len(settings), tc.settings, settings)
			}
			_, has := settings["group_applier_queue_within"]
			if want := tc.applies.Gate && tc.applies.Group; has != want {
				t.Errorf("settings lists group_applier_queue_within = %v, want %v", has, want)
			}
		})
	}
}

// TestStatusTextSaysTheBoundsAndTheVerdictsBounds: the text says the bounds in force at the end of the replication line,
// the two settings after check_interval with the configured value where it differs, and each queue of a group server's
// line with the bound it was judged by; an asynchronous line and an unprobed one are as they were.
func TestStatusTextSaysTheBoundsAndTheVerdictsBounds(t *testing.T) {
	f := boundsFixture(t)
	if _, err := f.store.Set(tuning.GroupApplierQueueWithin, "400", testPeer); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Set(tuning.GroupCertifierQueueWithin, "0", testPeer); err != nil {
		t.Fatal(err)
	}
	text := strings.Join(f.view.text(), "\n")
	for _, want := range []string{
		"  replication: enabled, used, cache_ttl 15s, last pass 1m00s ago (2026-11-15T12:05:02Z), group queue bounds: applier 400, certifier 0",
		"seconds_behind_source_within 10   check_interval 5   group_applier_queue_within 400 (configured 1000)   group_certifier_queue_within 0 (configured 300)",
		"               replica1:3306 healthy, lag 0s, tolerance 10s, checked 3s ago (2026-11-15T12:05:59Z)",
		"               gr2:3306 healthy, group, applier queue 12 (bound 1000), certifier queue 3 (bound 300), checked 3s ago (2026-11-15T12:05:59Z)",
		"               gr3:3306 unreadable (denied), group, applier queue unknown (bound 1000), certifier queue unknown (bound 300), checked 3s ago (2026-11-15T12:05:59Z)",
		"               gr4:3306 not checked yet",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("status text lacks %q:\n%s", want, text)
		}
	}
}

// TestStatusTextCoversTheBoundsInTheJSON is the parity rule over a run that has the new fields: every field of its
// status-json is in its text.
func TestStatusTextCoversTheBoundsInTheJSON(t *testing.T) {
	f := boundsFixture(t)
	f.tracker.set(TrackerStatus{Phase: PhaseRunning, Step: "delete", Table: "orders", Batches: 3, Roots: 30, RowsCopied: 60, RowsDeleted: 60,
		Tables: []TableCounts{{Name: "orders", Copied: 30, Deleted: 30}}})
	if _, err := f.store.Set(tuning.GroupApplierQueueWithin, "400", testPeer); err != nil {
		t.Fatal(err)
	}
	j, text := f.json(t), strings.Join(f.view.text(), "\n")
	for field, needles := range statusParityNeedles(t, f, j) {
		for _, n := range needles {
			if !textHas(text, n) {
				t.Errorf("status-json's %q is not in the text: no %q in:\n%s", field, strings.ReplaceAll(n, "\x00", " ... "), text)
			}
		}
	}
}
