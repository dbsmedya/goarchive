package control

import (
	"strings"
	"testing"
	"time"
)

// What `status` and `status-json` say of a group server (spec §4.7, §8 "Status parity"; GR C6). All additions, so
// protocol stays 1: the replication object gains the two configured bounds, each server gains type and the two queues,
// and `lag_seconds` keeps its key and is null for a group server. Nothing of an asynchronous server changes.

func i64(n int64) *int64 { return &n }
func ip(n int) *int      { return &n }

// groupFleetFixture is a gated run whose fleet is one replica and three group verdicts in the states of the spec's
// parity row: healthy, unhealthy, a failed read and never probed.
func groupFleetFixture(t *testing.T) *statusFixture {
	t.Helper()
	f := newStatusFixture(t, archiveApplies)
	f.view.info.GroupApplierQueueWithin, f.view.info.GroupCertifierQueueWithin = ip(1000), ip(500)
	checked := statusNow.Add(-3 * time.Second)
	f.gate.set(GateRecord{
		HoldingSince: statusNow.Add(-90 * time.Second), LastPass: statusNow.Add(-time.Hour),
		Servers: []ServerVerdict{
			{Server: "replica1:3306", Type: "async", Healthy: true, Lag: i64(0), Tolerance: 10, CheckedAt: checked},
			{Server: "gr2:3306", Type: "group", Healthy: true, Tolerance: 10, CheckedAt: checked, ApplierQueue: i64(12), CertifierQueue: i64(3)},
			{Server: "gr3:3306", Type: "group", Cause: "unhealthy", Detail: "GROUP_REPLICATION_APPLIER_QUEUE_WITHIN: applier queue of 4210, above the bound 1000",
				Tolerance: 10, CheckedAt: checked, ApplierQueue: i64(4210), CertifierQueue: i64(0)},
			{Server: "gr4:3306", Type: "group", Cause: "unreadable", Detail: "the account cannot read the view", Tolerance: 10, CheckedAt: checked},
			{Server: "gr5:3306", Type: "group"},
		},
	})
	return f
}

func TestStatusJSONForAGroupFleet(t *testing.T) {
	j := groupFleetFixture(t).json(t)
	repl := j["replication"].(map[string]any)
	if repl["group_applier_queue_within"] != 1000.0 || repl["group_certifier_queue_within"] != 500.0 {
		t.Errorf("replication bounds = %v and %v, want the configured 1000 and 500", repl["group_applier_queue_within"], repl["group_certifier_queue_within"])
	}
	servers := repl["servers"].([]any)
	want := []map[string]any{
		{"server": "replica1:3306", "type": "async", "healthy": true, "lag_seconds": 0.0, "tolerance": 10.0, "applier_queue": nil, "certifier_queue": nil},
		{"server": "gr2:3306", "type": "group", "healthy": true, "lag_seconds": nil, "tolerance": 10.0, "applier_queue": 12.0, "certifier_queue": 3.0},
		{"server": "gr3:3306", "type": "group", "healthy": false, "cause": "unhealthy", "lag_seconds": nil, "tolerance": 10.0, "applier_queue": 4210.0, "certifier_queue": 0.0},
		{"server": "gr4:3306", "type": "group", "healthy": false, "cause": "unreadable", "lag_seconds": nil, "tolerance": 10.0, "applier_queue": nil, "certifier_queue": nil},
		{"server": "gr5:3306", "type": "group", "healthy": nil, "lag_seconds": nil, "checked_at": nil, "applier_queue": nil, "certifier_queue": nil},
	}
	for i, w := range want {
		got := servers[i].(map[string]any)
		for key, v := range w {
			if gotv, has := got[key]; !has || gotv != v {
				t.Errorf("replication.servers[%d].%s = %v (present %v), want %v", i, key, gotv, has, v)
			}
		}
	}
	// an asynchronous server keeps every key it had, with the same types
	for _, key := range []string{"server", "healthy", "cause", "detail", "lag_seconds", "tolerance", "checked_at"} {
		if _, has := servers[0].(map[string]any)[key]; !has {
			t.Errorf("an asynchronous server lost the key %q", key)
		}
	}
	// the hold carries the same fields
	hold := j["hold"].(map[string]any)["servers"].([]any)
	if len(hold) != 2 {
		t.Fatalf("hold.servers = %v, want the two servers with a cause", hold)
	}
	if first := hold[0].(map[string]any); first["type"] != "group" || first["applier_queue"] != 4210.0 {
		t.Errorf("hold.servers[0] = %v, want the group server with its queue", first)
	}
}

func TestStatusJSONBoundsAreNullWithoutAGroupServer(t *testing.T) {
	f := newStatusFixture(t, archiveApplies)
	repl := f.json(t)["replication"].(map[string]any)
	for _, key := range []string{"group_applier_queue_within", "group_certifier_queue_within"} {
		if v, has := repl[key]; !has || v != nil {
			t.Errorf("replication.%s = %v (present %v), want null when no group server is listed", key, v, has)
		}
	}
	server := repl["servers"].([]any)[0].(map[string]any)
	if v, has := server["type"]; !has || v != "async" {
		t.Errorf("the fixture's asynchronous server has type %v (present %v), want async: a verdict without a type is an async one", v, has)
	}
	for _, key := range []string{"applier_queue", "certifier_queue"} {
		if v, has := server[key]; !has || v != nil {
			t.Errorf("an asynchronous server's %s = %v (present %v), want null", key, v, has)
		}
	}
}

func TestStatusTextForAGroupFleet(t *testing.T) {
	text := strings.Join(groupFleetFixture(t).view.text(), "\n")
	for _, want := range []string{
		"  replication: enabled, used, cache_ttl 15s, last pass 1h00m00s ago (2026-11-15T11:06:02Z), group queue bounds: applier 1000, certifier 500",
		"               replica1:3306 healthy, lag 0s, tolerance 10s, checked 3s ago (2026-11-15T12:05:59Z)",
		"               gr2:3306 healthy, group, applier queue 12, certifier queue 3, checked 3s ago (2026-11-15T12:05:59Z)",
		"               gr3:3306 unhealthy (GROUP_REPLICATION_APPLIER_QUEUE_WITHIN: applier queue of 4210, above the bound 1000), group, applier queue 4210, certifier queue 0, checked 3s ago (2026-11-15T12:05:59Z)",
		"               gr4:3306 unreadable (the account cannot read the view), group, applier queue unknown, certifier queue unknown, checked 3s ago (2026-11-15T12:05:59Z)",
		"               gr5:3306 not checked yet",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("status text lacks %q:\n%s", want, text)
		}
	}
	// no group server, no bounds on the line
	f := newStatusFixture(t, archiveApplies)
	if got := strings.Join(f.view.text(), "\n"); strings.Contains(got, "queue bounds") {
		t.Errorf("a run with no group server mentions queue bounds:\n%s", got)
	}
}

// TestStatusTextSaysTheGroupBoundsWhereTheGateIsUnused (Codex batch 1, finding 2): the bounds are on the replication line
// of a used gate, of a block that is enabled but not used by this run (a copy-only job with a group server listed) and of a
// block that is not enabled; and on none of the three when no group server is listed.
func TestStatusTextSaysTheGroupBoundsWhereTheGateIsUnused(t *testing.T) {
	const bounds = ", group queue bounds: applier 1000, certifier 500"
	for _, tc := range []struct {
		name  string
		setup func(f *statusFixture)
		want  string
	}{
		{"enabled, not used by this run", func(f *statusFixture) { f.view.gate = nil }, "  replication: enabled, not used by this run, cache_ttl 15s" + bounds},
		{"not enabled", func(f *statusFixture) { f.view.gate = nil; f.view.info.ReplicationEnabled = false }, "  replication: not enabled, cache_ttl 15s" + bounds},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newStatusFixture(t, archiveApplies)
			tc.setup(f)
			f.view.info.GroupApplierQueueWithin, f.view.info.GroupCertifierQueueWithin = ip(1000), ip(500)
			if text := strings.Join(f.view.text(), "\n"); !strings.Contains(text, tc.want) {
				t.Errorf("status text lacks %q:\n%s", tc.want, text)
			}
			f.view.info.GroupApplierQueueWithin, f.view.info.GroupCertifierQueueWithin = nil, nil
			if text := strings.Join(f.view.text(), "\n"); strings.Contains(text, "queue bounds") {
				t.Errorf("with no group server listed the text mentions bounds:\n%s", text)
			}
		})
	}
}
