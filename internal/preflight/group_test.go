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
		TransactionSizeLimit: 150000000, TransactionSizeLimitKnown: true, SinglePrimaryMode: "ON", SinglePrimaryModeKnown: true}
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
var groupCalls = []string{"src.ReplicationConfig", "dst.ReplicationConfig", "src.GroupReplication", "dst.GroupReplication", "src.GroupMembers"}

// deniedView is the library's error for a source account without SELECT on the view (M7).
func deniedView() error {
	return &dbsrepl.OpError{Op: "group_members", Err: &mysql.MySQLError{Number: 1142, SQLState: [5]byte{'4', '2', '0', '0', '0'},
		Message: "SELECT command denied to user 'ga_src'@'192.168.65.1' for table 'replication_group_members'"}}
}

// TestGroupFactsComeBeforeEveryOtherRead is spec §4.2's order: each side's ReplicationConfig is that
// side's first statement (the source's first, GR C4), then each side's plugin state, then the
// source's view, and only for a member. No table or catalog read of any stage comes before them.
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
				want := []string{"src.ReplicationConfig", "dst.ReplicationConfig", "src.GroupReplication", "dst.GroupReplication"}
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
			if len(*order) < len(groupCalls) || !slices.Equal((*order)[:len(groupCalls)], groupCalls) {
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
		{"active with a name", dbsrepl.GroupReplication{PluginStatus: "ACTIVE", GroupName: groupName, SinglePrimaryMode: "ON", SinglePrimaryModeKnown: true}, true},
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

// TestAMissingGrantRefusesUnderSkipPolicy (pin): the skip flag skips policy checks, never the
// acquisition of the facts, and a view that cannot be read leaves the identity unknown, so the
// refusal stands under it. It passes when written: it pins what the first stage does with the
// flag, which the docs state.
func TestAMissingGrantRefusesUnderSkipPolicy(t *testing.T) {
	w := healthy(t, jobspec.Archive)
	w.cfg.SkipPolicy = true
	w.asMember("ONLINE", "PRIMARY")
	w.src.membersErr = deniedView()
	w.refused("SOURCE_GROUP_VIEW_PERMISSION_CHECK")
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
			[]string{"dst.ReplicationConfig", "src.GroupReplication", "dst.GroupReplication", "src.GroupMembers", "src.Tables"}},
		{"the destination's configuration", func(w *world) { w.dst.configErr = boom }, "the destination's replication configuration",
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
		w.dst.config = dbsrepl.Config{ReadOnly: false, SuperReadOnly: false, ServerID: 4}
		w.dst.group = dbsrepl.GroupReplication{PluginStatus: "ACTIVE", GroupName: groupName, TransactionSizeLimit: 64000000, TransactionSizeLimitKnown: true,
			SinglePrimaryMode: "ON", SinglePrimaryModeKnown: true}
		res := w.mustPass()
		g := res.Group
		if g.DestinationConfig != w.dst.config {
			t.Errorf("destination configuration = %+v, want %+v", g.DestinationConfig, w.dst.config)
		}
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

// TestTheGroupReport is spec §4.7's block, with the mode word (rev 5.3): one block when the source or
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
			"  source:      member of group " + groupName + ", ONLINE, PRIMARY, mode single-primary; transaction size limit 150000000 bytes",
			"  destination: Group Replication plugin not active",
			sizes}, "\n")},
		{"an ONLINE secondary", func(w *world) { w.asMember("ONLINE", "SECONDARY") }, strings.Join([]string{
			"Group Replication:",
			"  source:      member of group " + groupName + ", ONLINE, SECONDARY, mode single-primary; transaction size limit 150000000 bytes",
			"  destination: Group Replication plugin not active",
			sizes}, "\n")},
		{"configured and not a running member", func(w *world) { w.asMember("OFFLINE", "") }, strings.Join([]string{
			"Group Replication:",
			"  source:      configured for group " + groupName + ", not a running member (state OFFLINE), configured single-primary; transaction size limit 150000000 bytes",
			"  destination: Group Replication plugin not active",
			sizes}, "\n")},
		{"configured with no row of its own", func(w *world) { member(w); w.src.members = w.src.members[1:] }, strings.Join([]string{
			"Group Replication:",
			"  source:      configured for group " + groupName + ", not a running member (no row of its own in its view), configured single-primary; transaction size limit 150000000 bytes",
			"  destination: Group Replication plugin not active",
			sizes}, "\n")},
		{"a limit that is not known", func(w *world) {
			member(w)
			w.src.group.TransactionSizeLimitKnown, w.src.group.TransactionSizeLimit = false, 0
		}, strings.Join([]string{
			"Group Replication:",
			"  source:      member of group " + groupName + ", ONLINE, PRIMARY, mode single-primary; transaction size limit unknown",
			"  destination: Group Replication plugin not active",
			sizes}, "\n")},
		{"a known limit of 0", func(w *world) { member(w); w.src.group.TransactionSizeLimit = 0 }, strings.Join([]string{
			"Group Replication:",
			"  source:      member of group " + groupName + ", ONLINE, PRIMARY, mode single-primary; transaction size limit 0 (the server's own \"no limit\")",
			"  destination: Group Replication plugin not active",
			sizes}, "\n")},
		{"a plain source and a member destination", func(w *world) {
			w.dst.group = dbsrepl.GroupReplication{PluginStatus: "ACTIVE", GroupName: groupName, TransactionSizeLimit: 64000000, TransactionSizeLimitKnown: true,
				SinglePrimaryMode: "ON", SinglePrimaryModeKnown: true}
		}, strings.Join([]string{
			"Group Replication:",
			"  source:      Group Replication plugin not active",
			"  destination: configured for group " + groupName + ", configured single-primary; transaction size limit 64000000 bytes",
			sizes}, "\n")},
		{"a plugin that is active with no name", func(w *world) { w.src.group = dbsrepl.GroupReplication{PluginStatus: "ACTIVE"} }, ""},
		// The mode word is the library's fact: a running member says "mode", a server whose view is not
		// read, or that is not running, says "configured"; a fact that is not known or is another text says
		// "mode unknown" on either (a refusal of the run names the text; the report does not).
		{"an ONLINE member of a multi-primary group", func(w *world) { member(w); w.src.group.SinglePrimaryMode = "OFF"; w.cfg.AllowMultiPrimary = true }, strings.Join([]string{
			"Group Replication:",
			"  source:      member of group " + groupName + ", ONLINE, PRIMARY, mode multi-primary; transaction size limit 150000000 bytes",
			"  destination: Group Replication plugin not active",
			sizes}, "\n")},
		{"a member whose mode is not known", func(w *world) {
			member(w)
			w.cfg.AllowMultiPrimary = true
			w.src.group.SinglePrimaryMode, w.src.group.SinglePrimaryModeKnown = "", false
		}, strings.Join([]string{
			"Group Replication:",
			"  source:      member of group " + groupName + ", ONLINE, PRIMARY, mode unknown; transaction size limit 150000000 bytes",
			"  destination: Group Replication plugin not active",
			sizes}, "\n")},
		{"a member whose mode is another text", func(w *world) { member(w); w.src.group.SinglePrimaryMode = "1"; w.cfg.AllowMultiPrimary = true }, strings.Join([]string{
			"Group Replication:",
			"  source:      member of group " + groupName + ", ONLINE, PRIMARY, mode unknown; transaction size limit 150000000 bytes",
			"  destination: Group Replication plugin not active",
			sizes}, "\n")},
		{"configured multi-primary and not running", func(w *world) {
			w.asMember("OFFLINE", "")
			w.src.group.SinglePrimaryMode, w.cfg.AllowMultiPrimary = "OFF", true
		}, strings.Join([]string{
			"Group Replication:",
			"  source:      configured for group " + groupName + ", not a running member (state OFFLINE), configured multi-primary; transaction size limit 150000000 bytes",
			"  destination: Group Replication plugin not active",
			sizes}, "\n")},
		{"a destination configured multi-primary", func(w *world) {
			w.cfg.AllowMultiPrimary = true
			w.dst.group = dbsrepl.GroupReplication{PluginStatus: "ACTIVE", GroupName: groupName, TransactionSizeLimit: 64000000, TransactionSizeLimitKnown: true,
				SinglePrimaryMode: "OFF", SinglePrimaryModeKnown: true}
		}, strings.Join([]string{
			"Group Replication:",
			"  source:      Group Replication plugin not active",
			"  destination: configured for group " + groupName + ", configured multi-primary; transaction size limit 64000000 bytes",
			sizes}, "\n")},
		{"a destination whose mode is not known", func(w *world) {
			w.cfg.AllowMultiPrimary = true
			w.dst.group = dbsrepl.GroupReplication{PluginStatus: "ACTIVE", GroupName: groupName, TransactionSizeLimit: 64000000, TransactionSizeLimitKnown: true}
		}, strings.Join([]string{
			"Group Replication:",
			"  source:      Group Replication plugin not active",
			"  destination: configured for group " + groupName + ", mode unknown; transaction size limit 64000000 bytes",
			sizes}, "\n")},
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

// TestSizeLimits: what a failed commit's message may say of a side's transaction size limit, from the
// facts read at the start (spec §4.4, GR C3). A known limit proves the plugin is ACTIVE, not that the
// server is a running member, so the membership is told apart: an ONLINE member, a server whose plugin
// is active and that was not a running member, and a destination, whose view is not read.
func TestSizeLimits(t *testing.T) {
	active := dbsrepl.GroupReplication{PluginStatus: "ACTIVE", GroupName: groupName, TransactionSizeLimit: 150000000, TransactionSizeLimitKnown: true}
	own := func(state string) []dbsrepl.GroupMember {
		return []dbsrepl.GroupMember{{MemberID: sourceUUID, State: state, Role: "PRIMARY", Self: true}}
	}
	for _, tc := range []struct {
		name string
		g    Group
		src  SizeLimit
		dst  SizeLimit
	}{
		{"a plain pair", Group{}, SizeLimit{}, SizeLimit{}},
		{"an ONLINE member and a plain destination", Group{Source: active, Members: own("ONLINE")},
			SizeLimit{Known: true, Bytes: 150000000, Group: groupName, Member: MemberOnline}, SizeLimit{}},
		{"a member that is RECOVERING", Group{Source: active, Members: own("RECOVERING")},
			SizeLimit{Known: true, Bytes: 150000000, Group: groupName, Member: MemberNotRunning}, SizeLimit{}},
		{"a name and no row of its own", Group{Source: active},
			SizeLimit{Known: true, Bytes: 150000000, Group: groupName, Member: MemberNotRunning}, SizeLimit{}},
		{"an active plugin with no name", Group{Source: dbsrepl.GroupReplication{PluginStatus: "ACTIVE", TransactionSizeLimit: 9, TransactionSizeLimitKnown: true}},
			SizeLimit{Known: true, Bytes: 9, Member: MemberNotRunning}, SizeLimit{}},
		{"a limit of 0 is known", Group{Source: dbsrepl.GroupReplication{PluginStatus: "ACTIVE", GroupName: groupName, TransactionSizeLimitKnown: true}, Members: own("ONLINE")},
			SizeLimit{Known: true, Bytes: 0, Group: groupName, Member: MemberOnline}, SizeLimit{}},
		{"a limit the server did not report", Group{Source: dbsrepl.GroupReplication{PluginStatus: "ACTIVE", GroupName: groupName}, Members: own("ONLINE")},
			SizeLimit{}, SizeLimit{}},
		{"a member destination", Group{Destination: active},
			SizeLimit{}, SizeLimit{Known: true, Bytes: 150000000, Group: groupName, Member: MemberUnread}},
		{"a destination with a disabled plugin", Group{Destination: dbsrepl.GroupReplication{PluginStatus: "DISABLED"}},
			SizeLimit{}, SizeLimit{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.g.SourceSizeLimit(); got != tc.src {
				t.Errorf("SourceSizeLimit = %+v, want %+v", got, tc.src)
			}
			if got := tc.g.DestinationSizeLimit(); got != tc.dst {
				t.Errorf("DestinationSizeLimit = %+v, want %+v", got, tc.dst)
			}
		})
	}
}

// TestSourceMemberID (GR C6, spec §4.3): what the engine hands the replication gate when preflight returns. The
// source's own member id when it is an ONLINE member, and "" for every other source: a plain server, a member that
// is not ONLINE, a plugin that is active with no row of its own, and the anonymous row a server reports before Group
// Replication ever ran on it. "" is the telling that the source is no member, so the gate's source rule is not applied.
func TestSourceMemberID(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(w *world)
		want  string
	}{
		{"a plain server", func(*world) {}, ""},
		{"an ONLINE primary", func(w *world) { w.asMember("ONLINE", "PRIMARY") }, sourceUUID},
		{"an ONLINE secondary", func(w *world) { w.asMember("ONLINE", "SECONDARY") }, sourceUUID},
		{"a member that is RECOVERING", func(w *world) { w.asMember("RECOVERING", "SECONDARY") }, ""},
		{"a member whose view has no row of its own", func(w *world) {
			w.asMember("ONLINE", "PRIMARY")
			w.src.members[0].Self = false
		}, ""},
		{"the anonymous row", func(w *world) {
			w.asMember("ONLINE", "PRIMARY")
			w.src.members = []dbsrepl.GroupMember{{ChannelName: "group_replication_applier", State: "OFFLINE", Self: true}}
		}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := healthy(t, jobspec.Archive)
			tc.setup(w)
			if got := w.mustPass().Group.SourceMemberID(); got != tc.want {
				t.Errorf("SourceMemberID() = %q, want %q", got, tc.want)
			}
		})
	}
}
