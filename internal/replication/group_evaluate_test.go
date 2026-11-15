package replication

import (
	"slices"
	"strings"
	"testing"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
)

// RG-1's judgement of a group server (spec §4.3, §8; GR C6): a pure function beside evaluate, in the library's
// own order, then goarchive's two problems (coverage and the source rule) only when the four checks found nothing.

const (
	srcID   = "11111111-1111-4111-8111-111111111111"
	peerID  = "22222222-2222-4222-8222-222222222222"
	thirdID = "33333333-3333-4333-8333-333333333333"
)

func gm(id, host, state string, self bool) dbsrepl.GroupMember {
	role := "SECONDARY"
	if id == srcID {
		role = "PRIMARY"
	}
	return dbsrepl.GroupMember{ChannelName: "group_replication_applier", MemberID: id, Host: host, State: state, Role: role, Self: self}
}

func gs(id, view string, applier, certifier uint64) dbsrepl.GroupMemberStats {
	return dbsrepl.GroupMemberStats{ChannelName: "group_replication_applier", ViewID: view, MemberID: id,
		TransactionsRemoteInApplierQueue: applier, TransactionsInQueue: certifier}
}

// healthyView is three ONLINE members and the queried one among them, with a stats row each.
func healthyView() ([]dbsrepl.GroupMember, []dbsrepl.GroupMemberStats) {
	return []dbsrepl.GroupMember{gm(srcID, "gr1", "ONLINE", false), gm(peerID, "gr2", "ONLINE", true), gm(thirdID, "gr3", "ONLINE", false)},
		[]dbsrepl.GroupMemberStats{gs(srcID, "v1", 0, 0), gs(peerID, "v1", 0, 0), gs(thirdID, "v1", 0, 0)}
}

// told is the source's state once the engine has handed it over: a member with this id.
func told(id string) sourceState { return sourceState{told: true, member: id != "", id: id} }

func checks(ps []problem) []string {
	var out []string
	for _, p := range ps {
		out = append(out, p.check)
	}
	return out
}

func TestAHealthyGroupServerHasNoProblem(t *testing.T) {
	members, stats := healthyView()
	if ps := evaluateGroup(members, stats, 1000, 1000, told(srcID)); len(ps) != 0 {
		t.Errorf("a healthy view = %v, want no problem", checks(ps))
	}
}

// TestGroupJudgementOrder: each of the four library checks alone; the findings come in the library's order;
// and coverage and the source rule are only judged when the four found nothing.
func TestGroupJudgementOrder(t *testing.T) {
	const (
		local     = dbsrepl.IDGroupReplicationLocalMemberOnline
		online    = dbsrepl.IDGroupReplicationMembersOnline
		applier   = dbsrepl.IDGroupReplicationApplierQueueWithin
		certifier = dbsrepl.IDGroupReplicationCertifierQueueWithin
	)
	tests := []struct {
		name  string
		build func(m []dbsrepl.GroupMember, s []dbsrepl.GroupMemberStats) ([]dbsrepl.GroupMember, []dbsrepl.GroupMemberStats)
		want  []string
	}{
		{"the queried member is not in its own view", func(m []dbsrepl.GroupMember, s []dbsrepl.GroupMemberStats) ([]dbsrepl.GroupMember, []dbsrepl.GroupMemberStats) {
			m[1].Self = false
			return m, s
		}, []string{local}},
		{"a peer is UNREACHABLE", func(m []dbsrepl.GroupMember, s []dbsrepl.GroupMemberStats) ([]dbsrepl.GroupMember, []dbsrepl.GroupMemberStats) {
			m[2].State = "UNREACHABLE"
			return m, s
		}, []string{online}},
		{"an applier queue over its bound", func(m []dbsrepl.GroupMember, s []dbsrepl.GroupMemberStats) ([]dbsrepl.GroupMember, []dbsrepl.GroupMemberStats) {
			s[2].TransactionsRemoteInApplierQueue = 4210
			return m, s
		}, []string{applier}},
		{"a certifier queue over its bound", func(m []dbsrepl.GroupMember, s []dbsrepl.GroupMemberStats) ([]dbsrepl.GroupMember, []dbsrepl.GroupMemberStats) {
			s[0].TransactionsInQueue = 1001
			return m, s
		}, []string{certifier}},
		{"the library's order: local, members, applier, certifier", func(m []dbsrepl.GroupMember, s []dbsrepl.GroupMemberStats) ([]dbsrepl.GroupMember, []dbsrepl.GroupMemberStats) {
			m[1].State = "RECOVERING" // its own row, so the local check fires too
			m[2].State = "UNREACHABLE"
			s[0].TransactionsRemoteInApplierQueue = 5000
			s[1].TransactionsInQueue = 5000
			return m, s
		}, []string{local, online, online, applier, certifier}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, s := healthyView()
			m, s = tc.build(m, s)
			if got := checks(evaluateGroup(m, s, 1000, 1000, told(srcID))); !slices.Equal(got, tc.want) {
				t.Errorf("problems = %v, want %v", got, tc.want)
			}
		})
	}

	t.Run("findings come before coverage and the source rule, which are then not judged", func(t *testing.T) {
		m, s := healthyView()
		s = s[:2] // the third member has no stats row: a coverage problem on its own
		m[2].State = "UNREACHABLE"
		got := checks(evaluateGroup(m, s, 1000, 1000, told("someone-else")))
		if want := []string{online}; !slices.Equal(got, want) {
			t.Errorf("problems = %v, want only the library's finding %v: coverage and the source rule are judged when the four found nothing", got, want)
		}
	})
	t.Run("a queue equal to its bound passes, a bound of 0 tolerates none", func(t *testing.T) {
		m, s := healthyView()
		s[0].TransactionsRemoteInApplierQueue = 1000
		if ps := evaluateGroup(m, s, 1000, 1000, told(srcID)); len(ps) != 0 {
			t.Errorf("a queue equal to its bound = %v, want it to pass", checks(ps))
		}
		if got := checks(evaluateGroup(m, s, 0, 1000, told(srcID))); !slices.Equal(got, []string{applier}) {
			t.Errorf("a queue of 1000 against a bound of 0 = %v, want %v", got, []string{applier})
		}
	})
}

// TestGroupCoverage: the library leaves it to the consumer to require, for every member the view lists, a stats
// row with its member id and a non-empty view id: counters of zero are not evidence of a healthy backlog.
func TestGroupCoverage(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func(m []dbsrepl.GroupMember, s []dbsrepl.GroupMemberStats) ([]dbsrepl.GroupMember, []dbsrepl.GroupMemberStats)
		names []string // the members the problem names
	}{
		{"a member with no stats row", func(m []dbsrepl.GroupMember, s []dbsrepl.GroupMemberStats) ([]dbsrepl.GroupMember, []dbsrepl.GroupMemberStats) {
			return m, s[:2]
		}, []string{thirdID}},
		{"a stats row with an empty view id", func(m []dbsrepl.GroupMember, s []dbsrepl.GroupMemberStats) ([]dbsrepl.GroupMember, []dbsrepl.GroupMemberStats) {
			s[1].ViewID = ""
			return m, s
		}, []string{peerID}},
		{"no stats at all", func(m []dbsrepl.GroupMember, s []dbsrepl.GroupMemberStats) ([]dbsrepl.GroupMember, []dbsrepl.GroupMemberStats) {
			return m, nil
		}, []string{srcID, peerID, thirdID}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, s := healthyView()
			m, s = tc.build(m, s)
			ps := evaluateGroup(m, s, 1000, 1000, told(srcID))
			if len(ps) != len(tc.names) {
				t.Fatalf("problems = %v, want %d of %s", checks(ps), len(tc.names), checkGroupStatsNotRunning)
			}
			for i, p := range ps {
				if p.check != checkGroupStatsNotRunning || !strings.Contains(renderProblem(p, 10), tc.names[i]) {
					t.Errorf("problem %d = %s %q, want %s naming member %s", i, p.check, renderProblem(p, 10), checkGroupStatsNotRunning, tc.names[i])
				}
			}
		})
	}
	t.Run("an empty view is the local-member finding, not coverage", func(t *testing.T) {
		got := checks(evaluateGroup(nil, nil, 1000, 1000, told(srcID)))
		if want := []string{dbsrepl.IDGroupReplicationLocalMemberOnline}; !slices.Equal(got, want) {
			t.Errorf("problems = %v, want %v", got, want)
		}
	})
	t.Run("a row of another member's counters does not cover a member", func(t *testing.T) {
		m, s := healthyView()
		s[2].MemberID = "44444444-4444-4444-8444-444444444444"
		ps := evaluateGroup(m, s, 1000, 1000, told(srcID))
		if len(ps) != 1 || ps[0].check != checkGroupStatsNotRunning || !strings.Contains(renderProblem(ps[0], 10), thirdID) {
			t.Errorf("problems = %v, want one coverage problem naming %s", checks(ps), thirdID)
		}
	})
}

// TestGroupSourceRule: a source that is an ONLINE member must be in the listed server's view; a source that is not a member
// is not judged by it; a gate that has not been told holds.
func TestGroupSourceRule(t *testing.T) {
	m, s := healthyView()

	if ps := evaluateGroup(m, s, 1000, 1000, told(srcID)); len(ps) != 0 {
		t.Errorf("a view that holds the source = %v, want it to pass", checks(ps))
	}

	other := "99999999-9999-4999-8999-999999999999"
	ps := evaluateGroup(m, s, 1000, 1000, told(other))
	if len(ps) != 1 || ps[0].check != checkGroupSourceNotInView {
		t.Fatalf("a view without the source = %v, want %s", checks(ps), checkGroupSourceNotInView)
	}
	if got := renderProblem(ps[0], 10); !strings.Contains(got, other) || !strings.Contains(got, "not in this server's view of the group") {
		t.Errorf("the detail = %q, want it to name the source's member id and say it is not in the view", got)
	}

	if ps := evaluateGroup(m, s, 1000, 1000, told("")); len(ps) != 0 {
		t.Errorf("a source that is told not to be a member = %v, want the rule not applied", checks(ps))
	}

	ps = evaluateGroup(m, s, 1000, 1000, sourceState{})
	if len(ps) != 1 || ps[0].check != checkGroupSourceNotTold {
		t.Fatalf("a gate that was not told = %v, want %s", checks(ps), checkGroupSourceNotTold)
	}
	if got := renderProblem(ps[0], 10); !strings.Contains(got, "has not been told") {
		t.Errorf("the detail = %q, want it to say the gate has not been told whether the source is a member", got)
	}
}

// TestLargestQueues: the two queue depths a verdict carries are the largest in the view's stats; none without rows.
func TestLargestQueues(t *testing.T) {
	_, s := healthyView()
	s[0].TransactionsRemoteInApplierQueue, s[2].TransactionsRemoteInApplierQueue = 7, 12
	s[1].TransactionsInQueue = 3
	a, c := largestQueues(s)
	if a == nil || *a != 12 || c == nil || *c != 3 {
		t.Errorf("largestQueues = %v, %v, want 12 and 3", a, c)
	}
	if a, c := largestQueues(nil); a != nil || c != nil {
		t.Errorf("largestQueues of no rows = %v, %v, want none", a, c)
	}
}
