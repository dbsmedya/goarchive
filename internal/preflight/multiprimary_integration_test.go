//go:build integration

package preflight

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"

	"github.com/dbsmedya/goarchive/v3/internal/database"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/testestate"
)

// GROUP_MULTI_PRIMARY_CHECK against real servers (spec §4.6 and §8's *Multi-primary, integration*, GR C7). The
// group is switched to multi-primary mode with the library's own switch function (testestate's copy of its
// helper), and the helper's cleanup switches it back and fails the test if it cannot. Members are stopped after
// the switch, not before it: a member that is down during a switch cannot rejoin until its values match
// (dbsgomysql's COMPAT entry 37). These run on the group estate, one package at a time.

// multiPrimaryGroup switches the group to multi-primary mode and returns its members.
func multiPrimaryGroup(t *testing.T) *testestate.GroupTopology {
	t.Helper()
	requireMemberSource(t)
	topology := testestate.OpenGroupTopology(t)
	testestate.StartGroupModeChange(t, topology).SwitchToMultiPrimary(t)
	return topology
}

// preflightOf runs preflight on a pair as a command does and returns what it said, with the banners it gave.
func preflightOf(t *testing.T, src, dst *sql.DB, spec *jobspec.Spec, allow bool) (res *Result, banners []string, err error) {
	t.Helper()
	id, err := database.ReadServerIdentity(context.Background(), src)
	if err != nil {
		t.Fatal(err)
	}
	res, err = Run(context.Background(), Config{
		Source: src, SourceDatabase: schemaOf(t, src), SourceServerUUID: id.ServerUUID,
		Destination: dst, DestinationDatabase: schemaOf(t, dst), JobSchema: schemaOf(t, dst),
		Spec: spec, Verification: jobspec.VerifyCount, Processing: defaultProcessing, AllowMultiPrimary: allow,
		Banner: func(b string) { banners = append(banners, b) },
	})
	return res, banners, err
}

// memberDestinationPair is a plain source (the estate's destination server) and a destination that is a member of
// the group (gr1), with the order tables on both.
func memberDestinationPair(t *testing.T, name string, typ jobspec.Type) (src, dst *sql.DB, spec *jobspec.Spec) {
	t.Helper()
	const cs = "CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"
	src, dst = scratch(t, "TEST_DEST", name+"src"), scratch(t, "TEST_SOURCE", name+"dst")
	execAll(t, src, orderTables(cs)...)
	execAll(t, dst, orderTables(cs)...)
	return src, dst, job(t, typ, orderJob)
}

// refusedAs requires err to be the multi-primary refusal and its message to contain each of want.
func refusedAs(t *testing.T, err error, want ...string) {
	t.Helper()
	e := refusedWith(t, err, "GROUP_MULTI_PRIMARY_CHECK")
	for _, w := range want {
		if !strings.Contains(e.Message, w) {
			t.Errorf("the refusal %q does not contain %q", e.Message, w)
		}
	}
}

// bypassed requires the run to have passed with one banner that names the case, and returns the result.
func bypassed(t *testing.T, res *Result, banners []string, err error, want string) *Result {
	t.Helper()
	if err != nil {
		t.Fatalf("Run() with --allow-multi-primary = %v, want success", err)
	}
	if len(banners) != 1 || !strings.HasPrefix(banners[0], "WARNING: --allow-multi-primary: ") || !strings.Contains(banners[0], want) {
		t.Errorf("banners = %q, want one that begins with the flag and contains %q", banners, want)
	}
	return res
}

// TestMySQLMultiPrimaryRefusesEveryJobTypeOnAGroupSource: with the group in multi-primary mode, an archive, a purge
// and a copy-only run are each refused on the group's source, and the bypass starts each with a banner that says the
// source is a running member of a group in multi-primary mode; the report's line carries "mode multi-primary".
func TestMySQLMultiPrimaryRefusesEveryJobTypeOnAGroupSource(t *testing.T) {
	multiPrimaryGroup(t)
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
		t.Run(string(typ), func(t *testing.T) {
			s, d, spec := orderPair(t, "mpsrc"+string(typ), typ)
			_, _, err := preflightOf(t, s, d, spec, false)
			refusedAs(t, err, "the source is a member of Group Replication group ", "the group is in multi-primary mode (group_replication_single_primary_mode is OFF)")
			res, banners, err := preflightOf(t, s, d, spec, true)
			res = bypassed(t, res, banners, err, "the source is a member of Group Replication group ")
			if block := res.GroupReport(defaultProcessing); !strings.Contains(block, ", ONLINE, PRIMARY, mode multi-primary; ") {
				t.Errorf("the report = %q, want the member line with the mode word", block)
			}
		})
	}
}

// TestMySQLMultiPrimaryRefusesAGroupDestination: a destination that is a member of the multi-primary group is refused
// for every job type, and the words say that the server is configured for multi-primary mode (its view is not read).
func TestMySQLMultiPrimaryRefusesAGroupDestination(t *testing.T) {
	multiPrimaryGroup(t)
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
		t.Run(string(typ), func(t *testing.T) {
			s, d, spec := memberDestinationPair(t, "mpdst"+string(typ), typ)
			_, _, err := preflightOf(t, s, d, spec, false)
			refusedAs(t, err, "the destination is configured for multi-primary mode (group_replication_single_primary_mode is OFF)")
			res, banners, err := preflightOf(t, s, d, spec, true)
			res = bypassed(t, res, banners, err, "the destination is configured for multi-primary mode")
			if block := res.GroupReport(defaultProcessing); !strings.Contains(block, "destination: configured for group ") || !strings.Contains(block, ", configured multi-primary; ") {
				t.Errorf("the report = %q, want the destination line with the configured mode", block)
			}
		})
	}
}

// TestMySQLASingleMemberOnlineIsStillRefused: in multi-primary mode, with the other two members stopped after the
// switch, the one member left is PRIMARY and ONLINE in its own view, as it is in single-primary mode. The mode fact
// refuses it; a count of primaries could not have.
func TestMySQLASingleMemberOnlineIsStillRefused(t *testing.T) {
	topology := multiPrimaryGroup(t)
	s, d, spec := orderPair(t, "mplone", jobspec.Archive)
	testestate.StopMember(t, topology.Secondary)
	testestate.StopMember(t, topology.Rejoiner)
	_, _, err := preflightOf(t, s, d, spec, false)
	refusedAs(t, err, "the source is a member of Group Replication group ", "multi-primary mode")
	res, banners, err := preflightOf(t, s, d, spec, true)
	bypassed(t, res, banners, err, "the source is a member of Group Replication group ")
}

// TestMySQLAPeerRecoveringDoesNotHideTheMode: in multi-primary mode with a peer RECOVERING (its applier held, as M9
// held it), the source is refused, and the bypass covers it.
func TestMySQLAPeerRecoveringDoesNotHideTheMode(t *testing.T) {
	topology := multiPrimaryGroup(t)
	s, d, spec := orderPair(t, "mprec", jobspec.Archive)
	testestate.HoldPeerRecovering(t, topology)
	_, _, err := preflightOf(t, s, d, spec, false)
	refusedAs(t, err, "the source is a member of Group Replication group ", "multi-primary mode")
	res, banners, err := preflightOf(t, s, d, spec, true)
	bypassed(t, res, banners, err, "the source is a member of Group Replication group ")
}

// TestMySQLASingleGroupWithAPeerRecoveringPasses: in single-primary mode a peer that is RECOVERING, or stopped, refuses
// nothing: the check judges the mode and never the members' roles or states. The report names the mode in force.
func TestMySQLASingleGroupWithAPeerRecoveringPasses(t *testing.T) {
	requireMemberSource(t)
	topology := testestate.OpenGroupTopology(t)
	testestate.StartGroupModeChange(t, topology) // the cleanup rejoins a member a failing test leaves stopped
	s, d, spec := orderPair(t, "sprec", jobspec.Archive)

	testestate.HoldPeerRecovering(t, topology)
	res, banners, err := preflightOf(t, s, d, spec, false)
	if err != nil || len(banners) != 0 {
		t.Fatalf("Run() with a peer RECOVERING = %v (banners %q), want success and no banner", err, banners)
	}
	if block := res.GroupReport(defaultProcessing); !strings.Contains(block, ", ONLINE, PRIMARY, mode single-primary; ") {
		t.Errorf("the report = %q, want the member line with mode single-primary", block)
	}
}

// groupFacts reads each member's GroupReplication fact, in the order gr1, gr2, gr3.
func groupFacts(t *testing.T, topology *testestate.GroupTopology) [3]dbsrepl.GroupReplication {
	t.Helper()
	var facts [3]dbsrepl.GroupReplication
	for i, db := range []*sql.DB{topology.Primary, topology.Secondary, topology.Rejoiner} {
		g, err := dbsrepl.NewInspector(db).GroupReplication(context.Background())
		if err != nil {
			t.Fatalf("gr%d: %v", i+1, err)
		}
		facts[i] = g
	}
	return facts
}

// TestMySQLTheModeFactByState is M9's mode-fact part with the library's answers (spec §7): what each member's
// GroupReplication fact says in the states M9 measured, so that the check's rule (the configured value, judged
// wherever the plugin is active with a name) rests on observation. The value is the server's own setting: a member
// that is not running keeps what it had when it left, which differs from its group's after a switch it was down for.
func TestMySQLTheModeFactByState(t *testing.T) {
	requireMemberSource(t)
	topology := testestate.OpenGroupTopology(t)
	change := testestate.StartGroupModeChange(t, topology)

	type want [3]string // the mode each of gr1, gr2, gr3 reports; every one is known
	check := func(state string, w want) {
		t.Helper()
		facts := groupFacts(t, topology)
		var got want
		for i, g := range facts {
			if !g.SinglePrimaryModeKnown {
				t.Errorf("%s: gr%d did not report its mode (plugin %s)", state, i+1, g.PluginStatus)
			}
			got[i] = g.SinglePrimaryMode
		}
		t.Logf("%-52s gr1=%s gr2=%s gr3=%s", state, got[0], got[1], got[2])
		if got != w {
			t.Errorf("%s: the members report %v, want %v", state, got, w)
		}
	}

	check("single-primary, three ONLINE", want{"ON", "ON", "ON"})

	testestate.StopMember(t, topology.Secondary)
	testestate.StopMember(t, topology.Rejoiner)
	check("single-primary, one ONLINE and two stopped", want{"ON", "ON", "ON"})
	testestate.StartMember(t, topology.Secondary)
	testestate.StartMember(t, topology.Rejoiner)

	recovering := testestate.HoldPeerRecovering(t, topology)
	check("single-primary, one ONLINE and a peer RECOVERING", want{"ON", "ON", "ON"})
	recovering.Release(t)

	change.SwitchToMultiPrimary(t)
	check("multi-primary, three ONLINE", want{"OFF", "OFF", "OFF"})

	testestate.StopMember(t, topology.Secondary)
	testestate.StopMember(t, topology.Rejoiner)
	check("multi-primary, one ONLINE and two stopped after the switch", want{"OFF", "OFF", "OFF"})
	testestate.StartMember(t, topology.Secondary)
	testestate.StartMember(t, topology.Rejoiner)

	recovering = testestate.HoldPeerRecovering(t, topology)
	check("multi-primary, one ONLINE and a peer RECOVERING", want{"OFF", "OFF", "OFF"})
	recovering.Release(t)

	testestate.StopMember(t, topology.Rejoiner)
	change.SwitchToSinglePrimary(t)
	check("a member down during the switch back to single-primary", want{"ON", "ON", "OFF"})
}
