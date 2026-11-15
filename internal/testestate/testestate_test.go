package testestate

import (
	"database/sql"
	"slices"
	"strings"
	"testing"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
)

// What the integration suites of different packages share on one estate: the exclusive use of the
// replica (db3, or gr-replica on the group estate) and the proof that a server is in its base state
// before a test relies on it. These are the pure parts; the lock and the reads are in
// testestate_integration_test.go.

func TestTopLevelNamesTheTestThatOwnsASubtest(t *testing.T) {
	for in, want := range map[string]string{
		"TestMySQLControlStatus":                 "TestMySQLControlStatus",
		"TestMySQLRecurringRoot/purge":           "TestMySQLRecurringRoot",
		"TestMySQLDestination/archive/read_only": "TestMySQLDestination",
	} {
		if got := topLevel(in); got != want {
			t.Errorf("topLevel(%q) = %q, want %q", in, got, want)
		}
	}
}

func running() []dbsrepl.ChannelStatus {
	return []dbsrepl.ChannelStatus{{ChannelName: "", IORunning: "Yes", SQLRunning: "Yes", SecondsBehindSource: sql.NullInt64{Int64: 0, Valid: true}}}
}

// TestReplicaProblemsNameEachChangedState: the base state is read_only and super_read_only off, the
// default channel's two threads running and no SOURCE_DELAY; whatever differs is named, each in its own
// words, so a message tells what a killed test left behind.
func TestReplicaProblemsNameEachChangedState(t *testing.T) {
	if got := replicaProblems(dbsrepl.Config{}, running(), 0); len(got) != 0 {
		t.Fatalf("the base state has problems: %v", got)
	}
	stopped := running()
	stopped[0].SQLRunning = "No"
	connecting := running()
	connecting[0].IORunning = "Connecting"
	for _, tc := range []struct {
		name     string
		cfg      dbsrepl.Config
		channels []dbsrepl.ChannelStatus
		delay    int64
		want     []string
	}{
		{"read_only", dbsrepl.Config{ReadOnly: true}, running(), 0, []string{"read_only is ON"}},
		{"super_read_only", dbsrepl.Config{ReadOnly: true, SuperReadOnly: true}, running(), 0, []string{"read_only is ON", "super_read_only is ON"}},
		{"the applier stopped", dbsrepl.Config{}, stopped, 0, []string{"the SQL thread is No"}},
		{"the receiver connecting", dbsrepl.Config{}, connecting, 0, []string{"the I/O thread is Connecting"}},
		{"a delay", dbsrepl.Config{}, running(), 20, []string{"SOURCE_DELAY is 20"}},
		{"no channel", dbsrepl.Config{}, nil, 0, []string{"the replica has no default channel"}},
		{"several", dbsrepl.Config{ReadOnly: true}, stopped, 5, []string{"read_only is ON", "the SQL thread is No", "SOURCE_DELAY is 5"}},
	} {
		got := replicaProblems(tc.cfg, tc.channels, tc.delay)
		if !slices.Equal(got, tc.want) {
			t.Errorf("%s: problems = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func onlineView(self string) []dbsrepl.GroupMember {
	view := []dbsrepl.GroupMember{
		{MemberID: "u1", Host: "gr1", State: "ONLINE", Role: "PRIMARY"},
		{MemberID: "u2", Host: "gr2", State: "ONLINE", Role: "SECONDARY"},
		{MemberID: "u3", Host: "gr3", State: "ONLINE", Role: "SECONDARY"},
	}
	for i := range view {
		view[i].Self = view[i].Host == self
	}
	return view
}

func baseMembers() []memberFacts {
	gr := dbsrepl.GroupReplication{PluginStatus: "ACTIVE", GroupName: "g", TransactionSizeLimit: 150000000, TransactionSizeLimitKnown: true}
	return []memberFacts{{"gr1", gr, "BEFORE_ON_PRIMARY_FAILOVER"}, {"gr2", gr, "BEFORE_ON_PRIMARY_FAILOVER"}, {"gr3", gr, "BEFORE_ON_PRIMARY_FAILOVER"}}
}

// TestGroupProblemsNameEachChangedState: gr1 is the PRIMARY with all three members ONLINE, the size
// limit at the estate's value and group_replication_consistency at the series' own on every member.
func TestGroupProblemsNameEachChangedState(t *testing.T) {
	const limit, consistency = 150000000, "BEFORE_ON_PRIMARY_FAILOVER"
	if got := groupProblems(onlineView("gr1"), baseMembers(), limit, consistency); len(got) != 0 {
		t.Fatalf("the base state has problems: %v", got)
	}

	switched := onlineView("gr1")
	switched[0].Role, switched[1].Role = "SECONDARY", "PRIMARY"
	recovering := onlineView("gr1")
	recovering[2].State = "RECOVERING"
	lowered := baseMembers()
	lowered[1].limit.TransactionSizeLimit = 4096
	unknown := baseMembers()
	unknown[2].limit.TransactionSizeLimitKnown = false
	raised := baseMembers()
	raised[1].consistency = "EVENTUAL"

	for _, tc := range []struct {
		name    string
		view    []dbsrepl.GroupMember
		members []memberFacts
		want    string
	}{
		{"the primary was switched", switched, baseMembers(), "gr1 is not the PRIMARY of its group (its view: gr1 SECONDARY ONLINE, gr2 PRIMARY ONLINE, gr3 SECONDARY ONLINE)"},
		{"a member is RECOVERING", recovering, baseMembers(), "gr3 is RECOVERING, not ONLINE"},
		{"a member left", onlineView("gr1")[:2], baseMembers(), "the group has 2 members in gr1's view, want 3"},
		{"a lowered limit", onlineView("gr1"), lowered, "gr2: group_replication_transaction_size_limit is 4096, want 150000000"},
		{"an unknown limit", onlineView("gr1"), unknown, "gr3: group_replication_transaction_size_limit is not reported"},
		{"another consistency", onlineView("gr1"), raised, "gr2: group_replication_consistency is EVENTUAL, want BEFORE_ON_PRIMARY_FAILOVER"},
	} {
		got := groupProblems(tc.view, tc.members, limit, consistency)
		if len(got) != 1 || got[0] != tc.want {
			t.Errorf("%s: problems = %q, want [%q]", tc.name, got, tc.want)
		}
	}
}

// TestEstateConsistencyIsTheSeriesOwnDefault: tests/README.md's group estate: BEFORE_ON_PRIMARY_FAILOVER on
// 8.4 and 9.7, EVENTUAL on 8.0, the AdminAPI persisting each series' own.
func TestEstateConsistencyIsTheSeriesOwnDefault(t *testing.T) {
	for version, want := range map[string]string{
		"8.0.46":     "EVENTUAL",
		"8.0.46-37":  "EVENTUAL",
		"8.4.13":     "BEFORE_ON_PRIMARY_FAILOVER",
		"8.4.11-12":  "BEFORE_ON_PRIMARY_FAILOVER",
		"9.7.4":      "BEFORE_ON_PRIMARY_FAILOVER",
		"10.0.0-rc1": "BEFORE_ON_PRIMARY_FAILOVER",
	} {
		if got := estateConsistency(version); got != want {
			t.Errorf("estateConsistency(%q) = %q, want %q", version, got, want)
		}
	}
	if strings.TrimSpace(estateConsistency("8.0.1")) == "" {
		t.Error("no value for 8.0")
	}
}
