package testestate

// The mode helper, copied from dbsgomysql v1.6.0 (internal/testsupport/gr_mode.go) and adapted (see
// groupmember.go).

import (
	"database/sql"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// The variables the mode helper captures and restores. The switch functions set and persist the first two
// on every ONLINE member (dbsgomysql's docs/COMPAT.md entry 37); the third is captured so a restore that
// touched it would be seen.
const (
	VarSinglePrimaryMode    = "group_replication_single_primary_mode"
	VarEnforceUpdateChecks  = "group_replication_enforce_update_everywhere_checks"
	VarGroupConsistency     = "group_replication_consistency"
	sqlSwitchToMultiPrimary = "SELECT group_replication_switch_to_multi_primary_mode()"
	// Harness reads: the library has no fact for the update-everywhere checks or for the persisted rows
	// of these variables.
	sqlEnforceChecks = "SELECT VARIABLE_NAME, VARIABLE_VALUE FROM performance_schema.global_variables WHERE VARIABLE_NAME = '" +
		VarEnforceUpdateChecks + "' LIMIT 18446744073709551615"
	sqlModePersisted = "SELECT VARIABLE_NAME, VARIABLE_VALUE FROM performance_schema.persisted_variables WHERE VARIABLE_NAME IN ('" +
		VarSinglePrimaryMode + "', '" + VarEnforceUpdateChecks + "', '" + VarGroupConsistency + "') LIMIT 18446744073709551615"
	modeSinglePrimaryValue   = "ON"
	modeMultiPrimaryValue    = "OFF"
	checksSinglePrimaryValue = "OFF"
)

// persistedVariables lists the variables whose persisted rows are captured.
var persistedVariables = []string{VarGroupConsistency, VarEnforceUpdateChecks, VarSinglePrimaryMode}

// PersistedRow is one variable's row in performance_schema.persisted_variables: Present false means the
// variable has no row.
type PersistedRow struct {
	Present bool
	Value   string
}

// MemberModeState is what the mode helper captures on one member: the runtime values of the mode and the
// update-everywhere checks, and the persisted rows of those two and of group_replication_consistency.
type MemberModeState struct {
	Runtime   map[string]string
	Persisted map[string]PersistedRow
}

// GroupModeChange switches the group's mode with group_replication_switch_to_multi_primary_mode() and
// group_replication_switch_to_single_primary_mode(), and restores the topology afterwards: single-primary
// with gr1 primary, every member ONLINE, and every member's persisted rows exactly as captured.
//
// The baseline is captured per member and restored as found, never flattened: it differs by series (on 8.0
// every member persists the mode ON and the checks OFF; on 8.4 and 9.7 no member persists either), and the
// switch functions persist both on every member they reach.
//
// A GroupModeChange belongs to one test and is not safe for concurrent use.
type GroupModeChange struct {
	topology *GroupTopology
	members  []modeMember
	gr1UUID  string
	restored bool

	// persisted is what Restore issued to restore persisted rows.
	persisted []PersistedRestore
}

// PersistedRestore is one statement Restore issued to put a member's persisted rows back: the member as the
// helper names it ("primary" is gr1, "secondary" gr2, "rejoiner" gr3), the member's own MEMBER_STATE read
// immediately before the statement, and the statement. Restore records it once the statement has succeeded.
type PersistedRestore struct {
	Member    string
	State     string
	Statement string
}

type modeMember struct {
	role     string
	db       *sql.DB
	baseline MemberModeState
}

// StartGroupModeChange proves the group single-primary with gr1 primary and every member ONLINE, captures
// every member's state, and registers Restore as a cleanup, all before anything changes.
func StartGroupModeChange(t testing.TB, topology *GroupTopology) *GroupModeChange {
	t.Helper()

	EnsureGroupOnline(t, topology)
	if role := MemberRole(t, topology.Primary); role != MemberPrimary {
		t.Fatalf("the group primary's own role is %q, not %s: %s", role, MemberPrimary, teardownInstruction)
	}

	change := &GroupModeChange{topology: topology, gr1UUID: ServerUUID(t, topology.Primary)}
	for _, member := range []struct {
		role string
		db   *sql.DB
	}{{"primary", topology.Primary}, {"secondary", topology.Secondary}, {"rejoiner", topology.Rejoiner}} {
		state := ReadMemberModeState(t, member.db)
		if state.Runtime[VarSinglePrimaryMode] != modeSinglePrimaryValue {
			t.Fatalf("the %s's %s is %q before the change, want %s: %s",
				member.role, VarSinglePrimaryMode, state.Runtime[VarSinglePrimaryMode], modeSinglePrimaryValue, teardownInstruction)
		}
		change.members = append(change.members, modeMember{role: member.role, db: member.db, baseline: state})
	}
	t.Cleanup(func() { change.Restore(t) })

	return change
}

// Baseline returns the state captured on db, one of the topology's three members, before the first change.
func (c *GroupModeChange) Baseline(t testing.TB, db *sql.DB) MemberModeState {
	t.Helper()

	for _, member := range c.members {
		if member.db == db {
			return member.baseline
		}
	}
	t.Fatal("Baseline: not one of the topology's three members")

	return MemberModeState{}
}

// SwitchToMultiPrimary runs the multi-primary switch on gr1 and waits until every ONLINE member reports the
// mode OFF.
func (c *GroupModeChange) SwitchToMultiPrimary(t testing.TB) {
	t.Helper()

	execGR(t, c.topology.Primary, sqlSwitchToMultiPrimary)
	c.waitOnlineMode(t, modeMultiPrimaryValue)
}

// SwitchToSinglePrimary runs the single-primary switch, naming gr1, on gr1 and waits until every ONLINE
// member reports the mode ON and gr1 is PRIMARY.
func (c *GroupModeChange) SwitchToSinglePrimary(t testing.TB) {
	t.Helper()

	execGR(t, c.topology.Primary, c.switchToSinglePrimary())
	c.waitOnlineMode(t, modeSinglePrimaryValue)
	pollUntil(t, "gr1 is PRIMARY after the single-primary switch", func() (bool, string) {
		role := MemberRole(t, c.topology.Primary)

		return role == MemberPrimary, "role " + role
	})
}

func (c *GroupModeChange) switchToSinglePrimary() string {
	return "SELECT group_replication_switch_to_single_primary_mode(" + quoteSQLString(c.gr1UUID) + ")"
}

// waitOnlineMode waits until every member that is ONLINE reports want.
func (c *GroupModeChange) waitOnlineMode(t testing.TB, want string) {
	t.Helper()

	pollUntil(t, "every ONLINE member reports "+VarSinglePrimaryMode+" = "+want, func() (bool, string) {
		var seen []string
		done := true
		for _, member := range c.members {
			if MemberState(t, member.db) != MemberOnline {
				continue
			}
			value, _ := groupMode(t, member.db)
			seen = append(seen, member.role+"="+value)
			if value != want {
				done = false
			}
		}

		return done, strings.Join(seen, ", ")
	})
}

// Restore is the helper's cleanup, and is safe to call more than once and from any state the helper's own
// steps or a test's STOP GROUP_REPLICATION leave: it switches the group back to single-primary with gr1
// primary, repairs and restarts any member that is not ONLINE, then restores each member's persisted rows as
// captured, recording each statement (RestoredPersisted), and verifies the runtime values and the rows. A
// call after the first issues nothing.
func (c *GroupModeChange) Restore(t testing.TB) {
	t.Helper()

	if c.restored {
		return
	}
	c.restored = true

	primary := c.members[0]
	if state := MemberState(t, primary.db); state != MemberOnline {
		t.Fatalf("the group primary is %q, not ONLINE: %s", state, teardownInstruction)
	}
	if mode, _ := groupMode(t, primary.db); mode != modeSinglePrimaryValue {
		execGR(t, primary.db, c.switchToSinglePrimary())
		c.waitOnlineMode(t, modeSinglePrimaryValue)
	}
	if role := MemberRole(t, primary.db); role != MemberPrimary {
		execGR(t, primary.db, setAsPrimary(c.gr1UUID))
	}

	for _, member := range c.members[1:] {
		c.rejoin(t, member)
	}
	c.waitOnlineMode(t, modeSinglePrimaryValue)
	EnsureGroupOnline(t, c.topology)

	for _, member := range c.members {
		current := ReadMemberModeState(t, member.db)
		for _, statement := range PersistedRestoreStatements(member.baseline.Persisted, current.Persisted) {
			state := MemberState(t, member.db)
			execGR(t, member.db, statement)
			c.persisted = append(c.persisted, PersistedRestore{Member: member.role, State: state, Statement: statement})
		}
	}

	c.verify(t)
}

// RestoredPersisted returns the statements Restore issued to restore persisted rows, in the order it issued
// them: none before Restore has run, or when no member's rows differed from its baseline.
func (c *GroupModeChange) RestoredPersisted() []PersistedRestore {
	return slices.Clone(c.persisted)
}

// rejoin brings a member that is not ONLINE back: one in ERROR is stopped first, and a stopped member whose
// own mode or checks differ from the single-primary group's is set to match, checks first (a member cannot
// join a group whose configuration differs from its own, dbsgomysql's docs/COMPAT.md entry 37), then
// started.
func (c *GroupModeChange) rejoin(t testing.TB, member modeMember) {
	t.Helper()

	switch MemberState(t, member.db) {
	case MemberOnline, MemberRecovering:
		return
	case MemberError:
		execGR(t, member.db, sqlStopGroupReplication)
	}

	runtime := ReadMemberModeState(t, member.db).Runtime
	if runtime[VarEnforceUpdateChecks] != checksSinglePrimaryValue {
		execGR(t, member.db, "SET GLOBAL "+VarEnforceUpdateChecks+" = "+checksSinglePrimaryValue)
	}
	if runtime[VarSinglePrimaryMode] != modeSinglePrimaryValue {
		execGR(t, member.db, "SET GLOBAL "+VarSinglePrimaryMode+" = "+modeSinglePrimaryValue)
	}
	execGR(t, member.db, sqlStartGroupReplication)
	waitMemberState(t, member.db, MemberOnline, "the "+member.role+" did not rejoin: "+teardownInstruction)
}

// verify fails the test unless every member is ONLINE, gr1 is PRIMARY, and each member's runtime values and
// persisted rows equal its baseline.
func (c *GroupModeChange) verify(t testing.TB) {
	t.Helper()

	if role := MemberRole(t, c.members[0].db); role != MemberPrimary {
		t.Errorf("after the restore the group primary's role is %q, want %s", role, MemberPrimary)
	}
	for _, member := range c.members {
		if state := MemberState(t, member.db); state != MemberOnline {
			t.Errorf("after the restore the %s is %q, want %s", member.role, state, MemberOnline)
		}
		current := ReadMemberModeState(t, member.db)
		if !mapsEqual(current.Runtime, member.baseline.Runtime) {
			t.Errorf("after the restore the %s's runtime values are %v, want %v", member.role, current.Runtime, member.baseline.Runtime)
		}
		if !mapsEqual(current.Persisted, member.baseline.Persisted) {
			t.Errorf("after the restore the %s's persisted rows are %v, want %v", member.role, current.Persisted, member.baseline.Persisted)
		}
	}
}

// ReadMemberModeState reads db's runtime mode (the library's GroupReplication fact) and checks, and its
// persisted rows of the mode, the checks and group_replication_consistency. Only performance_schema is read
// beyond the fact, so a fence does not hold it.
func ReadMemberModeState(t testing.TB, db *sql.DB) MemberModeState {
	t.Helper()

	state := MemberModeState{Runtime: map[string]string{}, Persisted: map[string]PersistedRow{}}
	if mode, known := groupMode(t, db); known {
		state.Runtime[VarSinglePrimaryMode] = mode
	}
	for name, value := range readVariableRows(t, db, sqlEnforceChecks) { // harness read: no fact
		state.Runtime[name] = value
	}
	persisted := readVariableRows(t, db, sqlModePersisted) // harness read: no fact
	for _, name := range persistedVariables {
		value, present := persisted[name]
		state.Persisted[name] = PersistedRow{Present: present, Value: value}
	}

	return state
}

// PersistedRestoreStatements returns, in variable-name order, the statements that take current to baseline:
// RESET PERSIST IF EXISTS for a row the baseline does not have, SET PERSIST_ONLY for a row it has with
// another value or not at all, a decimal value unquoted, any other as a string. Neither statement changes a
// runtime value. A variable in neither map is left alone.
func PersistedRestoreStatements(baseline, current map[string]PersistedRow) []string {
	names := make([]string, 0, len(baseline)+len(current))
	for name := range baseline {
		names = append(names, name)
	}
	for name := range current {
		if _, ok := baseline[name]; !ok {
			names = append(names, name)
		}
	}
	slices.Sort(names)

	var statements []string
	for _, name := range names {
		want, have := baseline[name], current[name]
		switch {
		case want == have:
		case !want.Present:
			statements = append(statements, "RESET PERSIST IF EXISTS "+name)
		default:
			literal := quoteSQLString(want.Value)
			if _, err := strconv.ParseUint(want.Value, 10, 64); err == nil {
				literal = want.Value
			}
			statements = append(statements, "SET PERSIST_ONLY "+name+" = "+literal)
		}
	}

	return statements
}

// readVariableRows maps VARIABLE_NAME to VARIABLE_VALUE for query's rows.
func readVariableRows(t testing.TB, db *sql.DB, query string) map[string]string {
	t.Helper()

	ctx, cancel := cleanupSafeContext(t, grStatementTimeout)
	defer cancel()

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer func() { _ = rows.Close() }()

	values := map[string]string{}
	for rows.Next() {
		var name string
		var value sql.NullString
		if err := rows.Scan(&name, &value); err != nil {
			t.Fatalf("scan %s: %v", query, err)
		}
		values[name] = value.String
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %s: %v", query, err)
	}

	return values
}

func mapsEqual[V comparable](a, b map[string]V) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if other, ok := b[key]; !ok || other != value {
			return false
		}
	}

	return true
}
