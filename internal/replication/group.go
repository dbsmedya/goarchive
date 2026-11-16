package replication

import (
	"context"
	"errors"
	"fmt"
	"strings"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// A `type: group` server in the gate (RG-1; spec §4.3, GR C6). The fleet is one list, one round, one snapshot and one
// verdict per server; what differs per server is the probe and the judgement, and they are here.

// The two problems goarchive raises itself for a group server. Like checkChannelNotFound they are goarchive-local
// IDs, never minted as a library Finding.
const (
	// checkGroupStatsNotRunning: a member of the view has no counters in the stats, so a queue of zero says nothing
	// of it. The library leaves this rule to the consumer.
	checkGroupStatsNotRunning = "GROUP_STATS_NOT_RUNNING"
	// checkGroupSourceNotInView: the source is an ONLINE member and the listed server's view does not hold it, so the
	// server is a member of another group, or is partitioned away from the source.
	checkGroupSourceNotInView = "GROUP_SOURCE_NOT_IN_VIEW"
	// checkGroupSourceNotTold: the gate has not been told whether the source is a member, so it cannot apply the
	// source rule, and no group server is judged without it.
	checkGroupSourceNotTold = "GROUP_SOURCE_NOT_TOLD"
)

// The two tables a group server's probe reads, as an operator names them in a refusal.
const (
	tableMembers = "performance_schema.replication_group_members"
	tableStats   = "performance_schema.replication_group_member_stats"
)

// groupReader is the two reads of a group server's probe, which the library's Inspector provides. They are named
// locally so the policy can be tested without a connection.
type groupReader interface {
	GroupMembers(ctx context.Context) ([]dbsrepl.GroupMember, error)
	GroupMemberStats(ctx context.Context) ([]dbsrepl.GroupMemberStats, error)
}

// groupProbe is what a `type: group` server adds to a server: its reader. The two bounds it is judged by are the
// store's, read once per round from the round's snapshot with the tolerance, so every group server of a round is judged
// by one pair and a change of either moves the gate revision (GR C7s).
type groupProbe struct {
	reader groupReader
}

// groupSnapshot is what one probe of a group server read.
type groupSnapshot struct {
	members []dbsrepl.GroupMember
	stats   []dbsrepl.GroupMemberStats
}

// read is the probe: the view, then the stats, both on one context. A failure of the first skips the second. The
// table names the read that failed, for the message of a refusal.
func (p *groupProbe) read(ctx context.Context) (snap groupSnapshot, table string, err error) {
	if snap.members, err = p.reader.GroupMembers(ctx); err != nil {
		return groupSnapshot{}, tableMembers, err
	}
	if snap.stats, err = p.reader.GroupMemberStats(ctx); err != nil {
		return groupSnapshot{}, tableStats, err
	}

	return snap, "", nil
}

// sourceState is what the gate knows of the source: whether it has been told, whether the source is an ONLINE member
// of a group, and its member id. "Not told" and "told: not a member" are two states: in the first the gate holds every
// group server, in the second the source rule is not applied.
type sourceState struct {
	told   bool
	member bool
	id     string
}

// SetSource tells the gate whether the source is an ONLINE member of a group and its member id: memberID is that id,
// and "" says the source is no member. The engine makes the telling once, when preflight returns, which is after the
// gate was built and before its first call (spec §4.3). Until it is made, the gate holds any group server.
func (g *Gate) SetSource(memberID string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.source = sourceState{told: true, member: memberID != "", id: memberID}
}

// currentSource is what the gate was last told.
func (g *Gate) currentSource() sourceState {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.source
}

// evaluateGroup judges one group server's snapshot and returns every reason the job must hold. It is pure. The order is
// the library's own composition, and goarchive's two problems follow only when the four checks found nothing:
//
//  1. GROUP_REPLICATION_LOCAL_MEMBER_ONLINE over the view;
//  2. GROUP_REPLICATION_MEMBERS_ONLINE over the view;
//  3. GROUP_REPLICATION_APPLIER_QUEUE_WITHIN at applierWithin;
//  4. GROUP_REPLICATION_CERTIFIER_QUEUE_WITHIN at certifierWithin;
//  5. coverage, then the source rule.
//
// Every row of the listed server's view is judged, not only its own: a member reports its peers from periodic copies, so
// one listed member covers the present view. Nothing is clamped or defaulted, as in evaluate.
func evaluateGroup(members []dbsrepl.GroupMember, stats []dbsrepl.GroupMemberStats, applierWithin, certifierWithin int64, src sourceState) []problem {
	var ps []problem
	for _, batch := range [][]dbsrepl.Finding{
		dbsrepl.CheckGroupReplicationLocalMemberOnline(members),
		dbsrepl.CheckGroupReplicationMembersOnline(members),
		dbsrepl.CheckGroupReplicationApplierQueueWithin(stats, applierWithin),
		dbsrepl.CheckGroupReplicationCertifierQueueWithin(stats, certifierWithin),
	} {
		for _, f := range batch {
			ps = append(ps, fromFinding(f))
		}
	}
	if len(ps) > 0 {
		return ps
	}

	if ps = uncovered(members, stats); len(ps) > 0 {
		return ps
	}

	return sourceProblems(members, src)
}

// uncovered is coverage: every member of the view has a stats row with its member id and a non-empty view id. The
// library's queue checks pass a member that has no row, and a stopped member's own row of zeros, so a gate must
// require the rows itself.
func uncovered(members []dbsrepl.GroupMember, stats []dbsrepl.GroupMemberStats) []problem {
	var ps []problem
	for i := range members {
		covered := false
		for j := range stats {
			if stats[j].MemberID == members[i].MemberID && stats[j].ViewID != "" {
				covered = true
				break
			}
		}
		if covered {
			continue
		}
		ps = append(ps, problem{
			check: checkGroupStatsNotRunning,
			message: fmt.Sprintf("Group member %q on host %q has no counters in this server's stats: no row with its member id and a view id, "+
				"so a queue of zero says nothing of it", members[i].MemberID, members[i].Host),
			facts: members[i],
		})
	}

	return ps
}

// sourceProblems is the source rule: when the source is an ONLINE member, the view contains a row whose member id is
// the source's. It costs no read. It catches a member of another group listed by mistake, and a member partitioned
// away from the source. A source that is no member is not judged by it, and a gate that has not been told holds.
func sourceProblems(members []dbsrepl.GroupMember, src sourceState) []problem {
	switch {
	case !src.told:
		return []problem{{
			check: checkGroupSourceNotTold,
			message: "The gate has not been told whether the source is a member of a group yet, so it cannot tell whether this server's view " +
				"contains the source; a group server is not judged without it",
		}}
	case !src.member:
		return nil
	}
	for i := range members {
		if members[i].MemberID == src.id {
			return nil
		}
	}

	return []problem{{
		check: checkGroupSourceNotInView,
		message: fmt.Sprintf("The source's member id %q is not in this server's view of the group: it is a member of another group, "+
			"or it is cut off from the source", src.id),
		facts: members,
	}}
}

// largestQueues is the largest applier queue and the largest certifier queue among the rows of a view's stats, or nil for
// each when there are no rows: the two depths a verdict carries.
func largestQueues(stats []dbsrepl.GroupMemberStats) (applier, certifier *int64) {
	if len(stats) == 0 {
		return nil, nil
	}
	var a, c int64
	for i := range stats {
		a = max(a, int64(stats[i].TransactionsRemoteInApplierQueue))
		c = max(c, int64(stats[i].TransactionsInQueue))
	}

	return &a, &c
}

// groupReadDetail is the line a failed read of a group server's probe shows. A refusal of SELECT (MySQL error 1142)
// names the table and the grant; any other failure is the error's own text, as for a replica.
func groupReadDetail(table string, err error) string {
	var me *mysql.MySQLError
	if errors.As(err, &me) && me.Number == 1142 {
		return fmt.Sprintf("the account that monitors this server cannot read %s: grant it SELECT on that table (MySQL error 1142)", table)
	}

	return sanitizeLine(err.Error())
}

// groupBounds renders the two group bounds in force for the startup line.
func groupBounds(st tuning.Settings) string {
	return strings.Join([]string{
		fmt.Sprintf("applier queue within %d", st.GroupApplierQueueWithin),
		fmt.Sprintf("certifier queue within %d", st.GroupCertifierQueueWithin),
	}, ", ")
}
