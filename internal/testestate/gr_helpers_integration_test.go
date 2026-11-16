//go:build integration

package testestate

import (
	"context"
	"database/sql"
	"maps"
	"slices"
	"testing"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"

	"github.com/dbsmedya/goarchive/v3/internal/database"
)

// These tests drive the fence harness's and the mode helper's restores (groupfence.go, groupmode.go,
// copied from dbsgomysql v1.6.0's own, internal/testsupport) from the states a failed test would leave
// behind. They are the library's three self-tests on goarchive's estate. They live in this package, the
// one that owns the helpers, and run on the group estate, one package at a time (tests/README.md, The
// group estate): two packages switching and fencing the same group at once would race. Each test calls its
// restore twice: the second call must be a no-op, as it is when the registered cleanup runs after an
// explicit restore.

// requireGroupEstate skips the test unless TEST_SOURCE is a Group Replication member: the plugin is ACTIVE
// and a group name is set. The group estate is.
func requireGroupEstate(t *testing.T) {
	t.Helper()
	cfg, err := envConfig("TEST_SOURCE")
	if err != nil {
		t.Fatal(err)
	}
	db, err := database.OpenPool(t.Context(), &cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	g, err := dbsrepl.NewInspector(db).GroupReplication(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if g.PluginStatus != "ACTIVE" || g.GroupName == "" {
		t.Skip("the source is not a Group Replication member: this test runs on the group estate (TOPOLOGY=group)")
	}
}

// execOn runs a statement that changes a member's state.
func execOn(t *testing.T, db *sql.DB, statement string) {
	t.Helper()
	if _, err := db.ExecContext(context.WithoutCancel(t.Context()), statement); err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
}

// TestMySQLGroupModeChangeRestoresFromStoppedMember leaves the group multi-primary with gr3 stopped before
// the switch, which is where a failing mode test stops, and requires Restore to bring back single-primary
// with gr1 primary, every member ONLINE, and every member's runtime values and persisted rows as captured.
func TestMySQLGroupModeChangeRestoresFromStoppedMember(t *testing.T) {
	requireGroupEstate(t)
	topology := OpenGroupTopology(t)
	change := StartGroupModeChange(t, topology)

	execOn(t, topology.Rejoiner, "STOP GROUP_REPLICATION")
	WaitMemberState(t, topology.Rejoiner, MemberOffline)
	change.SwitchToMultiPrimary(t)

	change.Restore(t)
	change.Restore(t)

	assertModeChangeRestored(t, topology, change)
}

// TestMySQLGroupModeChangeRestoresStoppedMultiPrimaryMember leaves the group multi-primary with gr3 stopped
// after the switch, so gr3 keeps the multi-primary mode and checks at runtime and in its persisted rows.
// Restore must then set gr3's checks and mode with SET GLOBAL before gr3 can rejoin, and restore its
// persisted rows on the running member: SET PERSIST_ONLY where the baseline has a row, RESET PERSIST IF
// EXISTS where it has none. The test predicts gr3's statements from the stopped member, then requires the
// record Restore keeps of what it issued (RestoredPersisted) to hold exactly those for gr3, each issued
// with gr3 ONLINE. The second Restore must issue nothing.
func TestMySQLGroupModeChangeRestoresStoppedMultiPrimaryMember(t *testing.T) {
	requireGroupEstate(t)
	topology := OpenGroupTopology(t)
	change := StartGroupModeChange(t, topology)

	change.SwitchToMultiPrimary(t)
	execOn(t, topology.Rejoiner, "STOP GROUP_REPLICATION")
	WaitMemberState(t, topology.Rejoiner, MemberOffline)

	// The preconditions of the two repair paths, from the server.
	baseline := change.Baseline(t, topology.Rejoiner)
	stopped := ReadMemberModeState(t, topology.Rejoiner)
	for _, name := range []string{VarSinglePrimaryMode, VarEnforceUpdateChecks} {
		if stopped.Runtime[name] == baseline.Runtime[name] {
			t.Fatalf("the stopped gr3's %s is %q, as captured: the SET GLOBAL repair would not run", name, stopped.Runtime[name])
		}
	}
	predicted := PersistedRestoreStatements(baseline.Persisted, stopped.Persisted)
	if len(predicted) != 2 {
		t.Fatalf("gr3's persisted restore, predicted while it is stopped, is %q, want two statements: "+
			"the switch persists the mode and the checks", predicted)
	}

	change.Restore(t)
	restored := change.RestoredPersisted()
	t.Logf("the persisted restore Restore issued: %+v", restored)
	var onGR3 []string
	for _, issued := range restored {
		if issued.Member != "rejoiner" {
			continue
		}
		if issued.State != MemberOnline {
			t.Errorf("Restore issued %q with gr3 %q, want %s", issued.Statement, issued.State, MemberOnline)
		}
		onGR3 = append(onGR3, issued.Statement)
	}
	if !slices.Equal(onGR3, predicted) {
		t.Errorf("Restore issued %q on gr3, want the statements predicted while it was stopped, %q", onGR3, predicted)
	}

	change.Restore(t)
	if again := change.RestoredPersisted(); !slices.Equal(again, restored) {
		t.Errorf("after the second Restore the record is %+v, want it unchanged: %+v", again, restored)
	}

	assertModeChangeRestored(t, topology, change)
}

// TestMySQLGroupFenceReleaseRestores builds the fence and releases it at once, the unwind a failing fence
// test gets from its cleanup, and requires gr1 primary and writable, every member ONLINE, and the
// harness's schema gone from every member.
func TestMySQLGroupFenceReleaseRestores(t *testing.T) {
	requireGroupEstate(t)
	topology := OpenGroupTopology(t)
	fence := BuildGroupFence(t, topology)

	if role := MemberRole(t, fence.Primary); role != MemberPrimary {
		t.Fatalf("the fenced member's role is %q, want %s", role, MemberPrimary)
	}
	if !SuperReadOnly(t, fence.Primary) {
		t.Fatal("the fenced primary's super_read_only is off, want on")
	}

	fence.Release(t)
	fence.Release(t)

	if role := MemberRole(t, topology.Primary); role != MemberPrimary {
		t.Errorf("gr1's role after the release is %q, want %s", role, MemberPrimary)
	}
	if SuperReadOnly(t, topology.Primary) {
		t.Error("gr1's super_read_only after the release is on, want off")
	}
	for role, db := range map[string]*sql.DB{"gr1": topology.Primary, "gr2": topology.Secondary, "gr3": topology.Rejoiner} {
		if state := MemberState(t, db); state != MemberOnline {
			t.Errorf("%s is %s after the release, want %s", role, state, MemberOnline)
		}
		if has := hasSchema(t, db, fence.Schema); has {
			t.Errorf("the harness schema %s is still on %s after the release", fence.Schema, role)
		}
	}
}

// hasSchema reports whether the server has a schema of that name. Harness read: the library has no fact
// for the list of schemas (the SHOW statement is the plain way to ask), and the question is the test's.
func hasSchema(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	rows, err := db.QueryContext(context.WithoutCancel(t.Context()), "SHOW DATABASES LIKE '"+name+"'")
	if err != nil {
		t.Fatalf("look for the schema %s: %v", name, err)
	}
	defer func() { _ = rows.Close() }()
	found := rows.Next()
	if err := rows.Err(); err != nil {
		t.Fatalf("look for the schema %s: %v", name, err)
	}
	return found
}

// assertModeChangeRestored requires gr1 primary, every member ONLINE, and each member's runtime values and
// persisted rows equal to what the helper captured.
func assertModeChangeRestored(t *testing.T, topology *GroupTopology, change *GroupModeChange) {
	t.Helper()

	if role := MemberRole(t, topology.Primary); role != MemberPrimary {
		t.Errorf("gr1's role after the restore is %q, want %s", role, MemberPrimary)
	}
	for role, db := range map[string]*sql.DB{"gr1": topology.Primary, "gr2": topology.Secondary, "gr3": topology.Rejoiner} {
		if state := MemberState(t, db); state != MemberOnline {
			t.Errorf("%s is %s after the restore, want %s", role, state, MemberOnline)
		}
		got, want := ReadMemberModeState(t, db), change.Baseline(t, db)
		if !maps.Equal(got.Runtime, want.Runtime) || !maps.Equal(got.Persisted, want.Persisted) {
			t.Errorf("%s after the restore = %+v, want the captured %+v", role, got, want)
		}
	}
	RequireGroupBase(t) // the estate itself is back: the members' limit, consistency and mode
}
