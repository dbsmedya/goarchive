package testestate

// The fence harness, copied from dbsgomysql v1.6.0 (internal/testsupport/gr_fence.go) and adapted (see
// groupmember.go). C8 uses it; C7 carries it with its self-test so the copy is proven once, on this estate.

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// The fence harness's statements. Every session the harness opens on a member sets its own
// group_replication_consistency to EVENTUAL first: on 8.4 and 9.7 the members' global value is
// BEFORE_ON_PRIMARY_FAILOVER, and a session that inherited it on the fenced primary would be held by the
// fence it is there to manage.
//
// Harness reads: the library has no fact for a held applier worker or for a connection's id.
const (
	sqlConnectionID   = "SELECT CONNECTION_ID()"
	sqlWaitForGTIDSet = "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 60)"
	sqlHeldApplier    = "SELECT COUNT(*) FROM performance_schema.replication_applier_status_by_worker " +
		"WHERE CHANNEL_NAME = 'group_replication_applier' AND APPLYING_TRANSACTION <> ''"

	// fenceElectionTimeout bounds the background group_replication_set_as_primary on a member whose backlog
	// is held: the call returns only once the backlog is applied, which is after Release unlocks the
	// applier. Release itself waits for it only GroupReplicationWaitDeadline after the unlock, and then
	// cancels it.
	fenceElectionTimeout = 10 * time.Minute
)

// GroupFence is a newly elected primary under a fence, built as M5 builds it: the secondary's applier is
// held behind a table lock, the primary writes a transaction that needs the lock, and the secondary is
// elected. It then reports itself PRIMARY with super_read_only = 1 and its backlog unapplied, and a session
// there whose group_replication_consistency is BEFORE_ON_PRIMARY_FAILOVER is held unless its statement is
// one the manual permits (dbsgomysql's docs/COMPAT.md entry 36).
//
// A GroupFence belongs to one test and is not safe for concurrent use.
type GroupFence struct {
	// Primary is the fenced primary, the topology's Secondary (gr2), as a pool whose sessions run EVENTUAL.
	// Use it to observe the fenced primary without being held; open your own sessions to be held.
	Primary *sql.DB
	// PrimaryDSN reaches the fenced primary as root, with no session settings: a session opened from it
	// inherits the server's global group_replication_consistency.
	PrimaryDSN string
	// Schema is the harness's own schema on the group. HeldTable is a table in it whose newest row the
	// fenced primary has not applied: a SELECT from it in a fencing session is held.
	Schema, HeldTable string

	topology    *GroupTopology
	oldPrimary  *sql.DB
	rejoiner    *sql.DB
	newUUID     string
	oldUUID     string
	lock        *sql.Conn
	lockID      int64
	election    chan error
	electionCtx func()
	released    bool
}

// FenceOptions shape the fence a test builds on top of the harness's own schema (GR C8). The zero value is the harness
// as dbsgomysql builds it.
type FenceOptions struct {
	// Prepare are statements run on the group's primary once the harness's schema exists and every member has applied
	// it, before the secondary's applier is held, so the rows and tables they make are already on the secondary.
	// "{schema}" in a statement is the harness's schema.
	Prepare []string
	// Backlog are statements run, in order, in the one transaction that the secondary's applier cannot apply past the
	// lock, so their rows are the ones a fenced read must wait for. "{schema}" is the harness's schema too.
	Backlog []string
	// NoElection holds the secondary's applier and writes the backlog, and elects nobody: gr2 stays an ordinary
	// secondary that is behind, which is where a session's BEFORE_ON_PRIMARY_FAILOVER adds no barrier.
	NoElection bool
}

// BuildGroupFence fences the topology's Secondary (gr2) as a newly elected primary and returns once gr2
// reports itself PRIMARY with super_read_only = 1; it fails the test if super_read_only is anything else
// there. Before it changes anything it registers a cleanup that calls Release, so a failure at any later
// step still restores the topology.
func BuildGroupFence(t testing.TB, topology *GroupTopology) *GroupFence {
	t.Helper()
	return BuildGroupFenceWith(t, topology, FenceOptions{})
}

// BuildGroupFenceWith is BuildGroupFence with the options' tables, late rows and election.
func BuildGroupFenceWith(t testing.TB, topology *GroupTopology, options FenceOptions) *GroupFence {
	t.Helper()

	EnsureGroupOnline(t, topology)
	if role := MemberRole(t, topology.Primary); role != MemberPrimary {
		t.Fatalf("the group primary's own role is %q, not %s: %s", role, MemberPrimary, teardownInstruction)
	}

	fence := &GroupFence{
		Primary:    openEventualDB(t, "fenced primary (EVENTUAL)", topology.SecondaryDSN),
		PrimaryDSN: topology.SecondaryDSN,
		topology:   topology,
		oldPrimary: openEventualDB(t, "group primary (EVENTUAL)", topology.PrimaryDSN),
		rejoiner:   openEventualDB(t, "group rejoiner (EVENTUAL)", topology.RejoinerDSN),
		newUUID:    ServerUUID(t, topology.Secondary),
		oldUUID:    ServerUUID(t, topology.Primary),
	}
	fence.Schema = "grfence_" + fenceSuffix(t)
	fence.HeldTable = fence.Schema + ".t"

	// Registered before the first change: Release drops the schema with IF EXISTS, so it also undoes a
	// CREATE that reached the server but failed on the client.
	t.Cleanup(func() { fence.Release(t) })
	fence.exec(t, fence.oldPrimary, "CREATE DATABASE "+fence.Schema)
	for _, statement := range []string{
		"CREATE TABLE " + fence.Schema + ".blocker (id INT PRIMARY KEY, v INT NOT NULL)",
		"CREATE TABLE " + fence.HeldTable + " (id INT PRIMARY KEY)",
		"INSERT INTO " + fence.Schema + ".blocker VALUES (1, 0)",
	} {
		fence.exec(t, fence.oldPrimary, statement)
	}
	fence.waitApplied(t)
	for _, statement := range options.Prepare {
		fence.exec(t, fence.oldPrimary, fence.expand(statement))
	}
	if len(options.Prepare) > 0 {
		fence.waitApplied(t)
	}

	fence.holdApplier(t)
	fence.writeBacklog(t, options.Backlog)
	if !options.NoElection {
		fence.elect(t)
	}

	return fence
}

// expand puts the harness's schema in a statement written with "{schema}".
func (f *GroupFence) expand(statement string) string {
	return strings.ReplaceAll(statement, "{schema}", f.Schema)
}

// holdApplier takes LOCK TABLES ... READ on the blocker table on gr2, in a session of its own.
func (f *GroupFence) holdApplier(t testing.TB) {
	t.Helper()

	ctx, cancel := cleanupSafeContext(t, grStatementTimeout)
	defer cancel()

	lock, err := f.Primary.Conn(ctx)
	if err != nil {
		t.Fatalf("pin the lock session on the secondary: %v", err)
	}
	f.lock = lock
	if err := lock.QueryRowContext(ctx, sqlConnectionID).Scan(&f.lockID); err != nil { // harness read: no fact
		t.Fatalf("read the lock session's id: %v", err)
	}
	if _, err := lock.ExecContext(ctx, "LOCK TABLES "+f.Schema+".blocker READ"); err != nil {
		t.Fatalf("lock the blocker table on the secondary: %v", err)
	}
}

// writeBacklog commits one transaction on gr1 that gr2's applier cannot apply past the lock, and waits until
// a gr2 worker is applying it.
func (f *GroupFence) writeBacklog(t testing.TB, extra []string) {
	t.Helper()

	ctx, cancel := cleanupSafeContext(t, grStatementTimeout)
	defer cancel()

	tx, err := f.oldPrimary.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin the backlog transaction on the primary: %v", err)
	}
	statements := []string{
		"UPDATE " + f.Schema + ".blocker SET v = v + 1 WHERE id = 1",
		"INSERT INTO " + f.HeldTable + " VALUES (1)",
	}
	for _, statement := range extra {
		statements = append(statements, f.expand(statement))
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				t.Errorf("roll back the backlog transaction on the primary: %v", rollbackErr)
			}
			t.Fatalf("%s on the primary: %v", statement, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit the backlog transaction on the primary: %v", err)
	}

	pollUntil(t, "a worker on the secondary is held applying the backlog", func() (bool, string) {
		var held int
		if err := f.Primary.QueryRowContext(ctx, sqlHeldApplier).Scan(&held); err != nil { // harness read: no fact
			return false, err.Error()
		}

		return held > 0, fmt.Sprintf("%d workers applying", held)
	})
}

// elect calls group_replication_set_as_primary(gr2) on gr1 in the background, since it returns only after
// gr2's backlog is applied, and waits for gr2 to report itself PRIMARY.
func (f *GroupFence) elect(t testing.TB) {
	t.Helper()

	ctx, cancel := cleanupSafeContext(t, fenceElectionTimeout)
	f.electionCtx = cancel
	f.election = make(chan error, 1)
	go func() {
		_, err := f.oldPrimary.ExecContext(ctx, setAsPrimary(f.newUUID))
		f.election <- err
	}()

	pollUntil(t, "the secondary reports itself PRIMARY", func() (bool, string) {
		role := MemberRole(t, f.Primary)
		if role == MemberPrimary {
			return true, "own role " + role
		}
		select {
		case err := <-f.election:
			// Put it back for Release, which waits on it. The election has ended without gr2 PRIMARY, so
			// waiting longer cannot help.
			f.election <- err
			t.Fatalf("group_replication_set_as_primary returned before the secondary reported itself PRIMARY "+
				"(own role %q): %v", role, err)
		default:
		}

		return false, "own role " + role
	})

	if !SuperReadOnly(t, f.Primary) {
		t.Fatal("the newly elected primary has super_read_only off, want on: the fence is not in force")
	}
}

// Release is the harness's cleanup, and is safe to call more than once: it unlocks gr2's applier, waits for
// the election to finish and for gr2 to become writable, elects gr1 again, brings every member ONLINE, and
// drops the harness's schema. It runs on contexts of its own, so it works from t.Cleanup.
func (f *GroupFence) Release(t testing.TB) {
	t.Helper()

	if f.released {
		return
	}
	f.released = true

	f.unlock(t)

	if f.election != nil {
		select {
		case err := <-f.election:
			if err != nil {
				t.Errorf("group_replication_set_as_primary on the secondary: %v", err)
			}
		case <-time.After(GroupReplicationWaitDeadline):
			// The lock is gone, so the backlog applies at once; a call still running after this is canceled
			// below. The wait is bounded well inside go test's own timeout, so the rest of Release still runs.
			t.Errorf("group_replication_set_as_primary on the secondary did not return within %s of the unlock",
				GroupReplicationWaitDeadline)
		}
		f.electionCtx()
		pollUntil(t, "the elected secondary becomes writable", func() (bool, string) {
			readOnly := SuperReadOnly(t, f.Primary)

			return !readOnly, fmt.Sprintf("super_read_only = %t", readOnly)
		})
	}

	if role := MemberRole(t, f.oldPrimary); role != MemberPrimary {
		ctx, cancel := cleanupSafeContext(t, GroupReplicationWaitDeadline)
		_, err := f.Primary.ExecContext(ctx, setAsPrimary(f.oldUUID))
		cancel()
		if err != nil {
			t.Fatalf("elect the group primary again: %v: %s", err, teardownInstruction)
		}
	}
	pollUntil(t, "the group primary is PRIMARY and writable again", func() (bool, string) {
		role := MemberRole(t, f.oldPrimary)
		readOnly := SuperReadOnly(t, f.oldPrimary)

		return role == MemberPrimary && !readOnly, fmt.Sprintf("role %s, super_read_only = %t", role, readOnly)
	})
	EnsureGroupOnline(t, f.topology)

	f.exec(t, f.oldPrimary, "DROP DATABASE IF EXISTS "+f.Schema)
	f.waitApplied(t)
}

// UnlockApplier ends the lock that holds the secondary's applier, so the backlog applies and a fenced statement is let
// go, and leaves the rest of the teardown to Release: the election's end, the group's roles and the harness's schema. A
// test that must see what a held statement or run does after the backlog is applied calls it, waits for them, and lets
// the cleanup's Release drop the schema afterwards (GR C8).
func (f *GroupFence) UnlockApplier(t testing.TB) {
	t.Helper()
	f.unlock(t)
}

// unlock ends the lock session: UNLOCK TABLES on it, or KILL from another session when that fails.
func (f *GroupFence) unlock(t testing.TB) {
	t.Helper()

	if f.lock == nil {
		return
	}

	ctx, cancel := cleanupSafeContext(t, grStatementTimeout)
	defer cancel()

	if _, err := f.lock.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
		if _, killErr := f.Primary.ExecContext(ctx, fmt.Sprintf("KILL %d", f.lockID)); killErr != nil {
			t.Errorf("release the secondary's lock: UNLOCK TABLES: %v; KILL %d: %v", err, f.lockID, killErr)
		}
	}
	if err := f.lock.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
		t.Errorf("close the lock session: %v", err)
	}
	f.lock = nil
}

// waitApplied waits until gr2 and gr3 have applied every transaction gr1 has.
func (f *GroupFence) waitApplied(t testing.TB) {
	t.Helper()
	waitAppliedOn(t, f.oldPrimary, f.Primary, f.rejoiner)
}

func (f *GroupFence) exec(t testing.TB, db *sql.DB, statement string) {
	t.Helper()

	ctx, cancel := cleanupSafeContext(t, grStatementTimeout)
	defer cancel()

	if _, err := db.ExecContext(ctx, statement); err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
}

// openEventualDB opens a pool on dsn whose every session sets group_replication_consistency to EVENTUAL as it
// connects, and registers its Close.
func openEventualDB(t testing.TB, role, dsn string) *sql.DB {
	t.Helper()

	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse the %s DSN: %v", role, err)
	}
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params["group_replication_consistency"] = "'EVENTUAL'"
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		t.Fatalf("connector for the %s: %v", role, err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Errorf("close the %s: %v", role, closeErr)
		}
	})

	return db
}

// ElectPrimary makes member the group's primary with no backlog held (group_replication_set_as_primary), and returns
// once it reports itself PRIMARY and writable. A cleanup puts the group's primary back to gr1 when it is not, so a test
// that moves the primary needs no teardown of its own for the plain election (GR C8).
func ElectPrimary(t testing.TB, topology *GroupTopology, member *sql.DB) {
	t.Helper()

	EnsureGroupOnline(t, topology)
	original := ServerUUID(t, topology.Primary)
	t.Cleanup(func() {
		if MemberRole(t, topology.Primary) != MemberPrimary {
			execGR(t, member, setAsPrimary(original))
			pollUntil(t, "the group primary is PRIMARY and writable again", func() (bool, string) {
				role, readOnly := MemberRole(t, topology.Primary), SuperReadOnly(t, topology.Primary)

				return role == MemberPrimary && !readOnly, fmt.Sprintf("role %s, super_read_only = %t", role, readOnly)
			})
		}
	})
	execGR(t, member, setAsPrimary(ServerUUID(t, member)))
	pollUntil(t, "the elected member becomes PRIMARY and writable", func() (bool, string) {
		role, readOnly := MemberRole(t, member), SuperReadOnly(t, member)

		return role == MemberPrimary && !readOnly, fmt.Sprintf("role %s, super_read_only = %t", role, readOnly)
	})
}

// OpenUnfenced opens a pool on the estate's server named by prefix (TEST_SOURCE, TEST_GROUP_MEMBER2, ...) as root, whose
// every session sets its own group_replication_consistency to EVENTUAL as it connects, through the driver alone: no
// goarchive connector, so nothing puts a fence behind it (GR C8). It is for a test's reads and writes of a server state a
// fence holds, which must not be held by what they observe or undo, and for a source a test wants read without the fence.
// schema is the default schema, or "".
func OpenUnfenced(t testing.TB, prefix, schema string) *sql.DB {
	t.Helper()
	cfg, err := envConfig(prefix)
	if err != nil {
		t.Fatal(err)
	}
	dsn, err := mysql.ParseDSN(plainDSN(cfg))
	if err != nil {
		t.Fatalf("parse the DSN of %s: %v", prefix, err)
	}
	dsn.DBName = schema

	return openEventualDB(t, prefix+" (unfenced)", dsn.FormatDSN())
}

func fenceSuffix(t testing.TB) string {
	t.Helper()

	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatalf("generate a schema suffix: %v", err)
	}

	return hex.EncodeToString(suffix)
}
