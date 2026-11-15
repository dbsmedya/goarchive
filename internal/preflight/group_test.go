package preflight

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// These tests pin what preflight reads about Group Replication, in which order, and what it
// returns (spec §4.2 *Where the facts are read* and *In which order*, §4.7, §8; GR C2).

const (
	groupName  = "8a94f357-0000-4000-8000-0000000000aa"
	sourceUUID = "11111111-1111-4111-8111-111111111111"
	otherUUID  = "22222222-2222-4222-8222-222222222222"
)

// asMember makes the world's source a member of a group whose own row has the given state and
// role, with one more member in its view.
func (w *world) asMember(state, role string) {
	w.cfg.SourceServerUUID = sourceUUID
	w.src.group = dbsrepl.GroupReplication{PluginStatus: "ACTIVE", GroupName: groupName,
		TransactionSizeLimit: 150000000, TransactionSizeLimitKnown: true}
	w.src.members = []dbsrepl.GroupMember{
		{ChannelName: "group_replication_applier", MemberID: sourceUUID, Host: "gr1", State: state, Role: role, Self: true},
		{ChannelName: "group_replication_applier", MemberID: otherUUID, Host: "gr2", State: "ONLINE", Role: "SECONDARY"},
	}
}

// ordered records every fact either server is asked for, in one list, as "src.Tables".
func (w *world) ordered() *[]string {
	order := new([]string)
	w.src.name, w.src.shared = "src", order
	w.dst.name, w.dst.shared = "dst", order
	return order
}

// groupCalls are the facts of the group set, as the shared list spells them.
var groupCalls = []string{"src.ReplicationConfig", "src.GroupReplication", "dst.GroupReplication", "src.GroupMembers"}

// deniedView is the library's error for a source account without SELECT on the view (M7).
func deniedView() error {
	return &dbsrepl.OpError{Op: "group_members", Err: &mysql.MySQLError{Number: 1142, SQLState: [5]byte{'4', '2', '0', '0', '0'},
		Message: "SELECT command denied to user 'ga_src'@'192.168.65.1' for table 'replication_group_members'"}}
}

// TestGroupFactsComeBeforeEveryOtherRead is spec §4.2's order: the source's ReplicationConfig is
// preflight's first statement, then each side's plugin state, then the source's view, and only
// for a member. No table or catalog read of any stage comes before them.
func TestGroupFactsComeBeforeEveryOtherRead(t *testing.T) {
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
		for _, member := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/member=%v", typ, member), func(t *testing.T) {
				w := healthy(t, typ)
				if member {
					w.asMember("ONLINE", "PRIMARY")
				}
				order := w.ordered()
				w.mustPass()
				want := []string{"src.ReplicationConfig", "src.GroupReplication", "dst.GroupReplication"}
				if member {
					want = append(want, "src.GroupMembers")
				}
				if len(*order) < len(want) || !slices.Equal((*order)[:len(want)], want) {
					t.Fatalf("the first reads were %v, want %v", (*order)[:min(len(*order), len(want)+2)], want)
				}
				for _, c := range (*order)[len(want):] {
					if slices.Contains(groupCalls, c) {
						t.Errorf("%s was read after the group facts (reads: %v)", c, *order)
					}
				}
			})
		}
	}
}

// TestGroupFactsAreReadWhenEveryCheckIsSkipped: a flag that skips the policy checks never
// skips the facts, because the identity and the report depend on them (Codex round 4.2).
func TestGroupFactsAreReadWhenEveryCheckIsSkipped(t *testing.T) {
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
		t.Run(string(typ), func(t *testing.T) {
			w := healthy(t, typ)
			w.cfg.SkipPolicy = true
			w.asMember("ONLINE", "PRIMARY")
			order := w.ordered()
			res := w.mustPass()
			if len(*order) < 4 || !slices.Equal((*order)[:4], groupCalls) {
				t.Fatalf("under SkipPolicy the first reads were %v, want %v", *order, groupCalls)
			}
			if res.Group.Source.GroupName != groupName || len(res.Group.Members) != 2 || res.Group.Identity != groupName {
				t.Errorf("under SkipPolicy the result holds %+v, want the member's facts and the group name as identity", res.Group)
			}
		})
	}
}

// TestTheViewIsReadOnlyForAnActiveNamedPlugin: GroupMembers is read when the plugin is ACTIVE
// with a name and never otherwise, so a server without Group Replication needs no new grant and
// a plain server cannot be refused for one. The view's read is made to fail where it must not
// be made.
func TestTheViewIsReadOnlyForAnActiveNamedPlugin(t *testing.T) {
	for _, tc := range []struct {
		name string
		gr   dbsrepl.GroupReplication
		read bool
	}{
		{"no plugin", dbsrepl.GroupReplication{}, false},
		{"disabled", dbsrepl.GroupReplication{PluginStatus: "DISABLED"}, false},
		{"inactive with a name", dbsrepl.GroupReplication{PluginStatus: "INACTIVE", GroupName: groupName}, false},
		{"active with no name", dbsrepl.GroupReplication{PluginStatus: "ACTIVE"}, false},
		{"active with a name", dbsrepl.GroupReplication{PluginStatus: "ACTIVE", GroupName: groupName}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := healthy(t, jobspec.Archive)
			w.src.group = tc.gr
			if tc.read {
				w.src.members = []dbsrepl.GroupMember{{MemberID: sourceUUID, State: "ONLINE", Role: "PRIMARY", Self: true}}
			} else {
				w.src.membersErr = deniedView()
			}
			res := w.mustPass()
			if got := slices.Contains(w.src.calls, "GroupMembers"); got != tc.read {
				t.Errorf("GroupMembers read = %v, want %v (calls %v)", got, tc.read, w.src.calls)
			}
			if !tc.read && res.Group.Members != nil {
				t.Errorf("members = %v, want none for a server that is not asked", res.Group.Members)
			}
		})
	}
}

// TestAMissingGrantRefusesBeforeAnyTableRead: a refused view (1142, M7's text) is a preflight
// refusal that names the grant, and nothing else of the source is read after it, so no lock is
// ever taken.
func TestAMissingGrantRefusesBeforeAnyTableRead(t *testing.T) {
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
		t.Run(string(typ), func(t *testing.T) {
			w := healthy(t, typ)
			w.asMember("ONLINE", "PRIMARY")
			w.src.membersErr = deniedView()
			order := w.ordered()
			e := w.mustFail("SOURCE_GROUP_VIEW_PERMISSION_CHECK")
			for _, want := range []string{
				"performance_schema.replication_group_members",
				"GRANT SELECT ON performance_schema.replication_group_members",
				"SELECT command denied to user 'ga_src'@'192.168.65.1' for table 'replication_group_members'",
			} {
				if !strings.Contains(e.Message, want) {
					t.Errorf("the refusal %q does not contain %q", e.Message, want)
				}
			}
			if !slices.Equal(*order, groupCalls) {
				t.Errorf("reads before and after the refusal = %v, want only the group set %v", *order, groupCalls)
			}
		})
	}
}

// TestAFailedGroupReadIsNotAnAnswer: a group fact that cannot be read fails preflight as "could
// not find out" (a plain error, not a check), naming the read, and nothing after it runs.
func TestAFailedGroupReadIsNotAnAnswer(t *testing.T) {
	boom := errors.New("connection reset")
	for _, tc := range []struct {
		name   string
		break_ func(w *world)
		want   string
		after  []string // the reads that must not have been made
	}{
		{"the source's configuration", func(w *world) { w.src.configErr = boom }, "the source's replication configuration",
			[]string{"src.GroupReplication", "dst.GroupReplication", "src.GroupMembers", "src.Tables"}},
		{"the source's plugin state", func(w *world) { w.src.groupErr = boom }, "the source's Group Replication state",
			[]string{"dst.GroupReplication", "src.GroupMembers", "src.Tables"}},
		{"the destination's plugin state", func(w *world) { w.dst.groupErr = boom }, "the destination's Group Replication state",
			[]string{"src.GroupMembers", "src.Tables"}},
		{"the source's view, for another reason than the grant", func(w *world) { w.src.membersErr = &dbsrepl.OpError{Op: "group_members", Err: boom} }, "the source's group view",
			[]string{"src.Tables"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := healthy(t, jobspec.Archive)
			w.asMember("ONLINE", "PRIMARY")
			tc.break_(w)
			order := w.ordered()
			_, err := w.run()
			var pe *Error
			if err == nil || errors.As(err, &pe) {
				t.Fatalf("run() = %v, want a plain error: preflight could not find out", err)
			}
			if !strings.Contains(err.Error(), tc.want) || !errors.Is(err, boom) {
				t.Errorf("run() = %v, want it to name %q and wrap the cause", err, tc.want)
			}
			for _, c := range tc.after {
				if slices.Contains(*order, c) {
					t.Errorf("%s was read after the failed read (reads: %v)", c, *order)
				}
			}
		})
	}
}

// TestIdentity is the source's data identity (spec §4.2): the group name only for a plugin that
// is ACTIVE, with a name and an own row that is ONLINE; the server's uuid for every other case.
func TestIdentity(t *testing.T) {
	active := dbsrepl.GroupReplication{PluginStatus: "ACTIVE", GroupName: groupName}
	own := func(state string) dbsrepl.GroupMember {
		return dbsrepl.GroupMember{MemberID: sourceUUID, State: state, Role: "PRIMARY", Self: true}
	}
	peer := dbsrepl.GroupMember{MemberID: otherUUID, State: "ONLINE", Role: "SECONDARY"}
	anonymous := dbsrepl.GroupMember{State: "OFFLINE"} // the row a server that never started the plugin reports
	for _, tc := range []struct {
		name    string
		gr      dbsrepl.GroupReplication
		members []dbsrepl.GroupMember
		want    string
	}{
		{"an ONLINE member", active, []dbsrepl.GroupMember{peer, own("ONLINE")}, groupName},
		{"an ONLINE member that is alone", active, []dbsrepl.GroupMember{own("ONLINE")}, groupName},
		{"no plugin", dbsrepl.GroupReplication{}, nil, sourceUUID},
		{"no plugin, a view anyway", dbsrepl.GroupReplication{}, []dbsrepl.GroupMember{own("ONLINE")}, sourceUUID},
		{"a disabled plugin", dbsrepl.GroupReplication{PluginStatus: "DISABLED"}, nil, sourceUUID},
		{"an inactive plugin with a name", dbsrepl.GroupReplication{PluginStatus: "INACTIVE", GroupName: groupName}, []dbsrepl.GroupMember{own("ONLINE")}, sourceUUID},
		{"an active plugin with no name", dbsrepl.GroupReplication{PluginStatus: "ACTIVE"}, []dbsrepl.GroupMember{own("ONLINE")}, sourceUUID},
		{"a name and no own row", active, []dbsrepl.GroupMember{peer}, sourceUUID},
		{"a name and an empty view", active, nil, sourceUUID},
		{"an own row RECOVERING", active, []dbsrepl.GroupMember{peer, own("RECOVERING")}, sourceUUID},
		{"an own row OFFLINE", active, []dbsrepl.GroupMember{own("OFFLINE")}, sourceUUID},
		{"an own row in ERROR", active, []dbsrepl.GroupMember{own("ERROR")}, sourceUUID},
		{"an own row UNREACHABLE", active, []dbsrepl.GroupMember{own("UNREACHABLE")}, sourceUUID},
		{"an anonymous row", active, []dbsrepl.GroupMember{anonymous}, sourceUUID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Identity(tc.gr, tc.members, sourceUUID); got != tc.want {
				t.Errorf("Identity = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestTheResultCarriesTheGroupFacts: what preflight read is returned as read, with the
// identity, for a member and for a plain pair.
func TestTheResultCarriesTheGroupFacts(t *testing.T) {
	t.Run("a member", func(t *testing.T) {
		w := healthy(t, jobspec.Archive)
		w.asMember("ONLINE", "PRIMARY")
		w.src.config = dbsrepl.Config{ReadOnly: false, SuperReadOnly: false, ServerID: 1}
		w.dst.group = dbsrepl.GroupReplication{PluginStatus: "ACTIVE", GroupName: groupName, TransactionSizeLimit: 64000000, TransactionSizeLimitKnown: true}
		res := w.mustPass()
		g := res.Group
		if g.SourceConfig != w.src.config || g.Source != w.src.group || g.Destination != w.dst.group || !slices.Equal(g.Members, w.src.members) {
			t.Errorf("the result holds %+v, want what the servers returned", g)
		}
		if g.Identity != groupName {
			t.Errorf("identity = %q, want the group name %q", g.Identity, groupName)
		}
	})
	t.Run("a plain pair", func(t *testing.T) {
		w := healthy(t, jobspec.Archive)
		w.cfg.SourceServerUUID = sourceUUID
		res := w.mustPass()
		if res.Group.Source.PluginStatus != "" || res.Group.Members != nil || res.Group.Identity != sourceUUID {
			t.Errorf("the result holds %+v, want no plugin, no view and the server's uuid as identity", res.Group)
		}
	})
	t.Run("a member that is not ONLINE keeps its uuid", func(t *testing.T) {
		w := healthy(t, jobspec.Archive)
		w.asMember("RECOVERING", "SECONDARY")
		if got := w.mustPass().Group.Identity; got != sourceUUID {
			t.Errorf("identity = %q, want the server's uuid %q", got, sourceUUID)
		}
	})
}

// TestTheGroupReport is spec §4.7's block, without the mode word: one block when the source or
// the destination is configured for a group, and nothing for a plain pair.
func TestTheGroupReport(t *testing.T) {
	sizes := "  sizes:       batch_size 1000, batch_delete_size 500 (each transaction must fit the limit of its side)"
	member := func(w *world) { w.asMember("ONLINE", "PRIMARY") }
	for _, tc := range []struct {
		name  string
		setup func(w *world)
		want  string
	}{
		{"a plain pair", func(*world) {}, ""},
		{"an ONLINE primary, a plain destination", member, strings.Join([]string{
			"Group Replication:",
			"  source:      member of group " + groupName + ", ONLINE, PRIMARY; transaction size limit 150000000 bytes",
			"  destination: Group Replication plugin not active",
			sizes}, "\n")},
		{"an ONLINE secondary", func(w *world) { w.asMember("ONLINE", "SECONDARY") }, strings.Join([]string{
			"Group Replication:",
			"  source:      member of group " + groupName + ", ONLINE, SECONDARY; transaction size limit 150000000 bytes",
			"  destination: Group Replication plugin not active",
			sizes}, "\n")},
		{"configured and not a running member", func(w *world) { w.asMember("OFFLINE", "") }, strings.Join([]string{
			"Group Replication:",
			"  source:      configured for group " + groupName + ", not a running member (state OFFLINE); transaction size limit 150000000 bytes",
			"  destination: Group Replication plugin not active",
			sizes}, "\n")},
		{"configured with no row of its own", func(w *world) { member(w); w.src.members = w.src.members[1:] }, strings.Join([]string{
			"Group Replication:",
			"  source:      configured for group " + groupName + ", not a running member (no row of its own in its view); transaction size limit 150000000 bytes",
			"  destination: Group Replication plugin not active",
			sizes}, "\n")},
		{"a limit that is not known", func(w *world) {
			member(w)
			w.src.group.TransactionSizeLimitKnown, w.src.group.TransactionSizeLimit = false, 0
		}, strings.Join([]string{
			"Group Replication:",
			"  source:      member of group " + groupName + ", ONLINE, PRIMARY; transaction size limit unknown",
			"  destination: Group Replication plugin not active",
			sizes}, "\n")},
		{"a known limit of 0", func(w *world) { member(w); w.src.group.TransactionSizeLimit = 0 }, strings.Join([]string{
			"Group Replication:",
			"  source:      member of group " + groupName + ", ONLINE, PRIMARY; transaction size limit 0 (the server's own \"no limit\")",
			"  destination: Group Replication plugin not active",
			sizes}, "\n")},
		{"a plain source and a member destination", func(w *world) {
			w.dst.group = dbsrepl.GroupReplication{PluginStatus: "ACTIVE", GroupName: groupName, TransactionSizeLimit: 64000000, TransactionSizeLimitKnown: true}
		}, strings.Join([]string{
			"Group Replication:",
			"  source:      Group Replication plugin not active",
			"  destination: configured for group " + groupName + "; transaction size limit 64000000 bytes",
			sizes}, "\n")},
		{"a plugin that is active with no name", func(w *world) { w.src.group = dbsrepl.GroupReplication{PluginStatus: "ACTIVE"} }, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := healthy(t, jobspec.Archive)
			tc.setup(w)
			res := w.mustPass()
			if got := res.GroupReport(w.cfg.Processing); got != tc.want {
				t.Errorf("GroupReport:\n got %q\nwant %q", got, tc.want)
			}
		})
	}
	t.Run("the sizes are the job's", func(t *testing.T) {
		w := healthy(t, jobspec.Archive)
		member(w)
		got := w.mustPass().GroupReport(config.ProcessingConfig{BatchSize: 20, BatchDeleteSize: 7})
		if !strings.Contains(got, "sizes:       batch_size 20, batch_delete_size 7 (") {
			t.Errorf("GroupReport = %q, want the sizes it was given", got)
		}
	})
}
