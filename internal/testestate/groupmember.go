package testestate

// The group estate's members as test support, copied from dbsgomysql v1.6.0's own harness
// (internal/testsupport/group_replication.go, gr_mode.go and gr_fence.go, the commit the library
// tagged as v1.6.0) and adapted to goarchive's estate. The adaptations:
//
//   - the estate is the one tests/README.md describes (gr1 is TEST_SOURCE, gr2 and gr3 are
//     TEST_GROUP_MEMBER2 and TEST_GROUP_MEMBER3), and a topology starts with the base-state check
//     (RequireGroupBase), so a test never builds on a group another test left changed;
//   - a read uses the library's fact where one exists: a member's role and state from GroupMembers,
//     super_read_only from ReplicationConfig, the mode from GroupReplication, gtid_executed from
//     GTIDStatus, the server's uuid from its own row of the view;
//   - the reads that remain are marked "harness read": no library fact answers them (the persisted
//     rows of the mode's variables, the update-everywhere checks, a held applier worker, a connection
//     id).
//
// The helpers change the group's global state. They run only in the group stage, one package at a time
// (`-p 1`, tests/README.md, The group estate), restore what they change, and fail the test when they
// cannot.

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/config"
)

const (
	// GroupReplicationWaitDeadline bounds every Group Replication poll here.
	GroupReplicationWaitDeadline = 120 * time.Second

	// MemberOnline and its siblings are the MEMBER_STATE spellings the helpers wait for, and
	// MemberPrimary and MemberSecondary the MEMBER_ROLE ones.
	MemberOnline     = "ONLINE"
	MemberOffline    = "OFFLINE"
	MemberRecovering = "RECOVERING"
	MemberError      = "ERROR"
	MemberPrimary    = "PRIMARY"
	MemberSecondary  = "SECONDARY"

	sqlStartGroupReplication = "START GROUP_REPLICATION"
	sqlStopGroupReplication  = "STOP GROUP_REPLICATION"

	// grStatementTimeout bounds each statement the helpers issue. They run from t.Cleanup too, where
	// t.Context() is already canceled, so every statement derives from context.WithoutCancel(t.Context()).
	grStatementTimeout = 30 * time.Second

	pollInterval = 250 * time.Millisecond

	// teardownInstruction is how a test that cannot repair the group says so.
	teardownInstruction = "restore the group estate by hand, or run `make test-down TOPOLOGY=group` and `make test-up TOPOLOGY=group`"
)

// cleanupSafeContext is t.Context() without its cancellation, bounded by timeout: usable from a t.Cleanup
// function, where t.Context() is canceled.
func cleanupSafeContext(t testing.TB, timeout time.Duration) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.WithoutCancel(t.Context()), timeout)
}

// GroupTopology holds the three members of the group estate: Primary, Secondary and Rejoiner are gr1, gr2
// and gr3. The DSN fields reach each as root with no session settings, so a session opened from one
// inherits the server's global group_replication_consistency; the fence harness's own sessions derive from
// them.
//
// The zero value is unusable; build one with OpenGroupTopology.
type GroupTopology struct {
	Primary, Secondary, Rejoiner          *sql.DB
	PrimaryDSN, SecondaryDSN, RejoinerDSN string
}

// plainDSN is the DSN of a root session on cfg's server with no session settings.
func plainDSN(cfg config.DatabaseConfig) string {
	c := mysql.NewConfig()
	c.User, c.Passwd = cfg.User, cfg.Password
	c.Net = "tcp"
	c.Addr = net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	c.TLSConfig = "false"
	return c.FormatDSN()
}

// openDSN opens a pool on dsn, pings it and registers its Close.
func openDSN(t testing.TB, role, dsn string) *sql.DB {
	t.Helper()
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse the %s DSN: %v", role, err)
	}
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		t.Fatalf("connector for the %s: %v", role, err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close the %s: %v", role, err)
		}
	})
	ctx, cancel := cleanupSafeContext(t, grStatementTimeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("reach the %s: %v", role, err)
	}
	return db
}

// OpenGroupTopology opens the group estate's three members from the environment (TEST_SOURCE,
// TEST_GROUP_MEMBER2, TEST_GROUP_MEMBER3) and registers their Close. It first requires the group's base
// state (RequireGroupBase): a test that took the group already changed would build on state it did not
// make. A member the environment does not name fails the test; the caller has found the source to be a
// member, which only the group estate is.
func OpenGroupTopology(t testing.TB) *GroupTopology {
	t.Helper()
	RequireGroupBase(t)

	var dsns [3]string
	for i, m := range groupMembers {
		cfg, err := envConfig(m.prefix)
		if err != nil {
			t.Fatalf("%s: %v", m.name, err)
		}
		dsns[i] = plainDSN(cfg)
	}
	return &GroupTopology{
		Primary:    openDSN(t, "group primary (gr1)", dsns[0]),
		Secondary:  openDSN(t, "group secondary (gr2)", dsns[1]),
		Rejoiner:   openDSN(t, "group rejoiner (gr3)", dsns[2]),
		PrimaryDSN: dsns[0], SecondaryDSN: dsns[1], RejoinerDSN: dsns[2],
	}
}

// ownMember is the row of db's view that describes db's own server, or nil when the view has none.
func ownMember(t testing.TB, db *sql.DB) *dbsrepl.GroupMember {
	t.Helper()
	ctx, cancel := cleanupSafeContext(t, grStatementTimeout)
	defer cancel()
	members, err := dbsrepl.NewInspector(db).GroupMembers(ctx)
	if err != nil {
		t.Fatalf("read the group view: %v", err)
	}
	for i := range members {
		if members[i].Self {
			return &members[i]
		}
	}
	return nil
}

// MemberState reads this server's own MEMBER_STATE from its view of the group, or "" when the view has no
// row of its own.
func MemberState(t testing.TB, db *sql.DB) string {
	t.Helper()
	if own := ownMember(t, db); own != nil {
		return own.State
	}
	return ""
}

// MemberRole reads this server's own MEMBER_ROLE, or "" when it has no row of its own.
func MemberRole(t testing.TB, db *sql.DB) string {
	t.Helper()
	if own := ownMember(t, db); own != nil {
		return own.Role
	}
	return ""
}

// ServerUUID is this server's uuid, from its own row of the view (a member's MEMBER_ID is its server_uuid,
// and a stopped member reports its own OFFLINE row). It fails the test when the view has no row of its own.
func ServerUUID(t testing.TB, db *sql.DB) string {
	t.Helper()
	own := ownMember(t, db)
	if own == nil || own.MemberID == "" {
		t.Fatal("read the server's uuid: the group view has no row of its own")
	}
	return own.MemberID
}

// SuperReadOnly reads super_read_only through the library's ReplicationConfig.
func SuperReadOnly(t testing.TB, db *sql.DB) bool {
	t.Helper()
	ctx, cancel := cleanupSafeContext(t, grStatementTimeout)
	defer cancel()
	cfg, err := dbsrepl.NewInspector(db).ReplicationConfig(ctx)
	if err != nil {
		t.Fatalf("read super_read_only: %v", err)
	}
	return cfg.SuperReadOnly
}

// groupMode is db's group_replication_single_primary_mode as the library reports it (GroupReplication),
// with whether the server returned it.
func groupMode(t testing.TB, db *sql.DB) (mode string, known bool) {
	t.Helper()
	ctx, cancel := cleanupSafeContext(t, grStatementTimeout)
	defer cancel()
	g, err := dbsrepl.NewInspector(db).GroupReplication(ctx)
	if err != nil {
		t.Fatalf("read the group's mode: %v", err)
	}
	return g.SinglePrimaryMode, g.SinglePrimaryModeKnown
}

// EnsureGroupOnline is convergent: a secondary or rejoiner left OFFLINE by an earlier test is started again,
// one in ERROR is stopped and started again, and every path ends by proving all three members ONLINE. The
// primary is never restarted (re-forming a group is the bootstrap's job), so a primary that is not ONLINE
// fails the test with the teardown instruction, as does a member the restart does not bring back ONLINE.
func EnsureGroupOnline(t testing.TB, topology *GroupTopology) {
	t.Helper()
	if state := MemberState(t, topology.Primary); state != MemberOnline {
		t.Fatalf("group primary is %q, not ONLINE: %s", state, teardownInstruction)
	}
	for _, member := range []struct {
		role string
		db   *sql.DB
	}{{"group secondary", topology.Secondary}, {"group rejoiner", topology.Rejoiner}} {
		var restart []string
		switch MemberState(t, member.db) {
		case MemberOffline:
			restart = []string{sqlStartGroupReplication}
		case MemberError:
			restart = []string{sqlStopGroupReplication, sqlStartGroupReplication}
		}
		for _, statement := range restart {
			ctx, cancel := cleanupSafeContext(t, GroupReplicationWaitDeadline)
			_, err := member.db.ExecContext(ctx, statement)
			cancel()
			if err != nil {
				t.Fatalf("%s on the %s: %v: %s", statement, member.role, err, teardownInstruction)
			}
		}
		waitMemberState(t, member.db, MemberOnline, "the "+member.role+" did not return: "+teardownInstruction)
	}
}

// WaitMemberState polls until this server's own MEMBER_STATE is want, failing with the last state seen once
// GroupReplicationWaitDeadline passes.
func WaitMemberState(t testing.TB, db *sql.DB, want string) {
	t.Helper()
	waitMemberState(t, db, want, "")
}

// waitMemberState is WaitMemberState with hint appended to its failure, and it stops early on ERROR when
// waiting for ONLINE: a member in ERROR does not leave it on its own, so waiting out the deadline would only
// delay the hint.
func waitMemberState(t testing.TB, db *sql.DB, want, hint string) {
	t.Helper()
	suffix := ""
	if hint != "" {
		suffix = "; " + hint
	}
	pollUntil(t, "member state "+want, func() (bool, string) {
		last := MemberState(t, db)
		if want == MemberOnline && last == MemberError {
			t.Fatalf("member state is %q, want %q%s", last, want, suffix)
		}
		return last == want, "member state " + last
	})
}

// pollUntil re-runs observe until it reports done, failing with what observe last described once
// GroupReplicationWaitDeadline passes. It runs on a context of its own, so it works from t.Cleanup, where
// t.Context() is already canceled. There are no fixed sleeps.
func pollUntil(t testing.TB, what string, observe func() (done bool, last string)) {
	t.Helper()
	ctx, cancel := cleanupSafeContext(t, GroupReplicationWaitDeadline)
	defer cancel()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		done, last := observe()
		if done {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s: not reached after %s; last observation: %s", what, GroupReplicationWaitDeadline, last)
		case <-ticker.C:
		}
	}
}

// execGR runs statement on a context of its own, bounded by the Group Replication wait deadline: a switch
// function or START GROUP_REPLICATION can take that long.
func execGR(t testing.TB, db *sql.DB, statement string) {
	t.Helper()
	ctx, cancel := cleanupSafeContext(t, GroupReplicationWaitDeadline)
	defer cancel()
	if _, err := db.ExecContext(ctx, statement); err != nil {
		var serverErr *mysql.MySQLError
		if errors.As(err, &serverErr) {
			t.Fatalf("%s: server error %d: %v", statement, serverErr.Number, err)
		}
		t.Fatalf("%s: %v", statement, err)
	}
}

// quoteSQLString quotes a value as a string literal under the sql_mode the estate runs, where backslash is
// the escape character and a quote is doubled. NO_BACKSLASH_ESCAPES is not supported: under it the
// backslash doubling would be wrong.
func quoteSQLString(value string) string {
	escaped := strings.ReplaceAll(value, `\`, `\\`)
	return "'" + strings.ReplaceAll(escaped, "'", "''") + "'"
}

// setAsPrimary is the election statement for the member with uuid.
func setAsPrimary(uuid string) string {
	return "SELECT group_replication_set_as_primary(" + quoteSQLString(uuid) + ")"
}
