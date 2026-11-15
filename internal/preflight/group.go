package preflight

import (
	"context"
	"errors"
	"fmt"
	"strings"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/config"
)

// Group is what preflight read about Group Replication on the two servers (spec §4.2, GR C2).
// For a pair with no group in it every field is the zero value, apart from Identity, which is
// then the source's server uuid.
//
// The facts are read once, in the first two stages of every preflight, so validate, dry-run and a run
// all see them, and they are read whatever checks are skipped: the identity and the report depend
// on them. The engine's guard still reads GTIDStatus itself, under the source table locks.
type Group struct {
	// SourceConfig and DestinationConfig are each side's ReplicationConfig, the first statement
	// preflight sends to that side: the two read-only checks judge them before any plugin state is read.
	SourceConfig, DestinationConfig dbsrepl.Config
	// Source and Destination are each side's GroupReplication: the plugin's state, the configured
	// group name and the transaction size limit.
	Source, Destination dbsrepl.GroupReplication
	// Members is the source's view of its group. It is read only when Source's plugin is ACTIVE
	// with a group name, and is nil otherwise.
	Members []dbsrepl.GroupMember
	// Identity is the source's data identity: the group name when the source is an ONLINE member,
	// else its server uuid (Identity the function). Nothing binds a job to it yet.
	Identity string
}

// configured reports whether a server's Group Replication plugin is ACTIVE with a group name.
// That is the test for reading the view: a server without it is not asked, so it needs no grant.
func configured(g dbsrepl.GroupReplication) bool {
	return g.PluginStatus == "ACTIVE" && g.GroupName != ""
}

// ownRow returns the view's row that describes the queried server, or nil.
func ownRow(members []dbsrepl.GroupMember) *dbsrepl.GroupMember {
	for i := range members {
		if members[i].Self {
			return &members[i]
		}
	}
	return nil
}

// onlineOwn reports whether the view's row of the queried server is ONLINE. An anonymous row (the
// one a server reports before the plugin ever started) is never the own row.
func onlineOwn(members []dbsrepl.GroupMember) bool {
	own := ownRow(members)
	return own != nil && own.MemberID != "" && own.State == "ONLINE"
}

// Identity is the source's data identity (spec §4.2): the group name only when the plugin is
// ACTIVE, a group name is set and the server's own row in its view is ONLINE; the server's uuid
// in every other case. A server with a name that is not running in a group logs under its own
// uuid, so recording the name for it would make the guard on its replicas miss its jobs.
func Identity(g dbsrepl.GroupReplication, members []dbsrepl.GroupMember, serverUUID string) string {
	if configured(g) && onlineOwn(members) {
		return g.GroupName
	}
	return serverUUID
}

// Membership is how a side was seen at the start, for the words of an error about it (spec §4.4):
// a known limit proves that the plugin is ACTIVE, not that the server is a running member.
type Membership int

const (
	_ Membership = iota // the zero value: no limit is known, nothing is said
	// MemberOnline is a source whose own row in its view was ONLINE.
	MemberOnline
	// MemberNotRunning is a source whose plugin is ACTIVE and whose own row was not ONLINE.
	MemberNotRunning
	// MemberUnread is a destination: its view is not read, so its membership is not claimed.
	MemberUnread
)

// SizeLimit is one side's group_replication_transaction_size_limit as the start read it, with what
// is known of the side's membership. Known is false for a server that did not report it (no plugin,
// or not ACTIVE), and the zero SizeLimit then says nothing: a commit error on that side is left as
// MySQL sent it. A known Bytes of 0 is the server's own "no limit" for that variable.
type SizeLimit struct {
	Known  bool
	Bytes  uint64
	Group  string // the configured group name, "" when none is set
	Member Membership
}

// SourceSizeLimit is the source's limit and membership at the start.
func (g Group) SourceSizeLimit() SizeLimit {
	if !g.Source.TransactionSizeLimitKnown {
		return SizeLimit{}
	}
	l := SizeLimit{Known: true, Bytes: g.Source.TransactionSizeLimit, Member: MemberNotRunning}
	if configured(g.Source) {
		l.Group = g.Source.GroupName
		if onlineOwn(g.Members) {
			l.Member = MemberOnline
		}
	}
	return l
}

// DestinationSizeLimit is the destination's limit at the start. Its view is not read, so its
// membership is MemberUnread.
func (g Group) DestinationSizeLimit() SizeLimit {
	if !g.Destination.TransactionSizeLimitKnown {
		return SizeLimit{}
	}
	return SizeLimit{Known: true, Bytes: g.Destination.TransactionSizeLimit, Group: g.Destination.GroupName, Member: MemberUnread}
}

// readOnlyGeneral is spec §4.5's sentence, the only thing either read-only message says about why
// a server may be read-only: the cause by role is the group report's, not these checks'.
const readOnlyGeneral = "On a Group Replication group a secondary is read-only, and so is a newly elected primary until it has applied its backlog."

// readOnlyVariables names which of the two variables is on, for a message.
func readOnlyVariables(c dbsrepl.Config) string {
	switch {
	case c.ReadOnly && c.SuperReadOnly:
		return "read_only and super_read_only are ON"
	case c.SuperReadOnly:
		return "super_read_only is ON"
	default:
		return "read_only is ON"
	}
}

// checkReadOnly is preflight's first stage (spec §4.2, In which order; §4.5): each side's
// ReplicationConfig before any other statement, judged as it is read, so a server that is read-only
// is refused before the plugin-state read that a fenced new primary holds. It runs under every skip
// policy, and no flag skips either check.
//
// SOURCE_READ_ONLY_CHECK (an archive and a purge, which delete from the source and whose recovery
// replays deletes there): read_only or super_read_only on the source. A copy-only run never writes
// the source and is exempt. DEST_READ_ONLY_CHECK (every job type: each writes its data or its
// tracking tables to the destination): super_read_only on the destination only, so read_only alone
// stays as it is, an account that may write through it still can. Neither message names a cause.
func checkReadOnly(ctx context.Context, f *facts) error {
	g := &f.group
	var err error
	if g.SourceConfig, err = f.src.ReplicationConfig(ctx); err != nil {
		return fmt.Errorf("read the source's replication configuration: %w", err)
	}
	if ProfileFor(f.cfg.Spec.Type) != ProfileNonDestructive && (g.SourceConfig.ReadOnly || g.SourceConfig.SuperReadOnly) {
		return &Error{Check: "SOURCE_READ_ONLY_CHECK", Message: fmt.Sprintf(
			"the source is read-only (%s). An archive or a purge deletes from the source, and its recovery replays deletes there, "+
				"so it is refused before anything is copied or replayed. %s", readOnlyVariables(g.SourceConfig), readOnlyGeneral)}
	}
	if g.DestinationConfig, err = f.dst.ReplicationConfig(ctx); err != nil {
		return fmt.Errorf("read the destination's replication configuration: %w", err)
	}
	if g.DestinationConfig.SuperReadOnly {
		return &Error{Check: "DEST_READ_ONLY_CHECK", Message: fmt.Sprintf(
			"the destination is read-only (%s). Every job writes to the destination: its data, or its tracking tables, "+
				"so it is refused before anything else is asked of it. %s", readOnlyVariables(g.DestinationConfig), readOnlyGeneral)}
	}
	return nil
}

// readGroupFacts is preflight's second stage (spec §4.2, In which order): each side's plugin state,
// then the source's view and only for a plugin that is ACTIVE with a name. Both ReplicationConfig
// reads are already done (checkReadOnly). A fenced new primary answers the configuration reads and
// holds these, so the order is what lets the read-only checks refuse it before it waits.
func readGroupFacts(ctx context.Context, f *facts) error {
	g := &f.group
	var err error
	if g.Source, err = f.src.GroupReplication(ctx); err != nil {
		return fmt.Errorf("read the source's Group Replication state: %w", err)
	}
	if g.Destination, err = f.dst.GroupReplication(ctx); err != nil {
		return fmt.Errorf("read the destination's Group Replication state: %w", err)
	}
	if configured(g.Source) {
		if g.Members, err = f.src.GroupMembers(ctx); err != nil {
			var me *mysql.MySQLError
			if errors.As(err, &me) && me.Number == 1142 {
				return &Error{Check: "SOURCE_GROUP_VIEW_PERMISSION_CHECK", Message: fmt.Sprintf(
					"The source is configured for Group Replication group %s, and its account cannot read the group's view (%v). "+
						"GoArchive reads the view to tell whether the source is a running member of the group; grant it "+
						"directly to the account (GRANT SELECT ON performance_schema.replication_group_members TO <user>)",
					g.Source.GroupName, err)}
			}
			return fmt.Errorf("read the source's group view: %w", err)
		}
	}
	g.Identity = Identity(g.Source, g.Members, f.cfg.SourceServerUUID)
	return nil
}

// GroupReport is spec §4.7's block, as printed by validate and dry-run and logged by a run: one
// block when the source or the destination is configured for Group Replication, and "" for a
// pair with no group in it. The sizes are the job's. The mode word is not in it yet.
func (r *Result) GroupReport(p config.ProcessingConfig) string {
	g := r.Group
	if !configured(g.Source) && !configured(g.Destination) {
		return ""
	}
	return strings.Join([]string{
		"Group Replication:",
		"  source:      " + sourceLine(g),
		"  destination: " + destinationLine(g.Destination),
		fmt.Sprintf("  sizes:       batch_size %d, batch_delete_size %d (each transaction must fit the limit of its side)",
			p.BatchSize, p.BatchDeleteSize),
	}, "\n")
}

func sourceLine(g Group) string {
	if line, plain := notConfigured(g.Source); plain {
		return line
	}
	own := ownRow(g.Members)
	limit := limitText(g.Source)
	if onlineOwn(g.Members) {
		line := "member of group " + g.Source.GroupName + ", ONLINE"
		if own.Role != "" {
			line += ", " + own.Role
		}
		return line + "; " + limit
	}
	state := "no row of its own in its view"
	if own != nil {
		state = "state " + own.State
	}
	return "configured for group " + g.Source.GroupName + ", not a running member (" + state + "); " + limit
}

// destinationLine has no state or role: the destination's view is not read.
func destinationLine(g dbsrepl.GroupReplication) string {
	if line, plain := notConfigured(g); plain {
		return line
	}
	return "configured for group " + g.GroupName + "; " + limitText(g)
}

// notConfigured is the line of a server that is not configured for a group.
func notConfigured(g dbsrepl.GroupReplication) (string, bool) {
	switch {
	case g.PluginStatus != "ACTIVE":
		return "Group Replication plugin not active", true
	case g.GroupName == "":
		return "Group Replication plugin active, no group name set", true
	}
	return "", false
}

// limitText is a server's group_replication_transaction_size_limit as the report spells it. A
// known 0 is the server's own "no limit" for that variable, and rules nothing else out.
func limitText(g dbsrepl.GroupReplication) string {
	switch {
	case !g.TransactionSizeLimitKnown:
		return "transaction size limit unknown"
	case g.TransactionSizeLimit == 0:
		return "transaction size limit 0 (the server's own \"no limit\")"
	}
	return fmt.Sprintf("transaction size limit %d bytes", g.TransactionSizeLimit)
}
