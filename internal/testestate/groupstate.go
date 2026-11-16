package testestate

// Group states the integration tests put the estate in and take it back from: a member stopped, a member
// started, and a peer held RECOVERING. Like the mode helper and the fence harness they change the group's
// state and run only in the group stage; each registers its own restore, and the mode helper's Restore is the
// fallback for a member a failed test left stopped (it rejoins it).

import (
	"database/sql"
	"testing"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
)

// StopMember runs STOP GROUP_REPLICATION on db and waits until the member reports itself OFFLINE.
func StopMember(t testing.TB, db *sql.DB) {
	t.Helper()
	execGR(t, db, sqlStopGroupReplication)
	WaitMemberState(t, db, MemberOffline)
}

// StartMember runs START GROUP_REPLICATION on db and waits until the member reports itself ONLINE.
func StartMember(t testing.TB, db *sql.DB) {
	t.Helper()
	execGR(t, db, sqlStartGroupReplication)
	waitMemberState(t, db, MemberOnline, "the member did not return: "+teardownInstruction)
}

// StartMemberInBackground begins START GROUP_REPLICATION on db without waiting for it: it returns a function
// that waits for the statement to return and for the member to report itself ONLINE, once. It is how a test has
// a peer return while something else runs.
func StartMemberInBackground(t testing.TB, db *sql.DB) (wait func()) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		ctx, cancel := cleanupSafeContext(t, GroupReplicationWaitDeadline)
		defer cancel()
		_, err := db.ExecContext(ctx, sqlStartGroupReplication)
		done <- err
	}()
	var once bool
	return func() {
		t.Helper()
		if once {
			return
		}
		once = true
		if err := <-done; err != nil {
			t.Fatalf("%s: %v", sqlStartGroupReplication, err)
		}
		waitMemberState(t, db, MemberOnline, "the member did not return: "+teardownInstruction)
	}
}

// peerState is the state the view of db gives the member with uuid, or "" when the view has no row for it.
func peerState(t testing.TB, db *sql.DB, uuid string) string {
	t.Helper()
	ctx, cancel := cleanupSafeContext(t, grStatementTimeout)
	defer cancel()
	members, err := dbsrepl.NewInspector(db).GroupMembers(ctx)
	if err != nil {
		t.Fatalf("read the group view: %v", err)
	}
	for _, m := range members {
		if m.MemberID == uuid {
			return m.State
		}
	}
	return ""
}

// RecoveringPeer is the group's third member held RECOVERING, as M9 made it: the member is stopped, its applier
// is held behind a table lock, the primary writes a transaction that needs the lock, and the member is started.
// It joins and then waits in distributed recovery, which the primary's view shows as RECOVERING, until Release
// lets its applier finish.
//
// A RecoveringPeer belongs to one test and is not safe for concurrent use.
type RecoveringPeer struct {
	topology *GroupTopology
	// Schema is the harness's own schema on the group.
	Schema   string
	uuid     string
	lock     *sql.Conn
	started  func()
	released bool
}

// HoldPeerRecovering stops gr3, holds its applier, and starts it, and returns once the primary's view lists gr3
// RECOVERING. It registers Release as a cleanup before it changes anything.
func HoldPeerRecovering(t testing.TB, topology *GroupTopology) *RecoveringPeer {
	t.Helper()
	EnsureGroupOnline(t, topology)
	p := &RecoveringPeer{topology: topology, Schema: "grrec_" + fenceSuffix(t), uuid: ServerUUID(t, topology.Rejoiner)}
	t.Cleanup(func() { p.Release(t) })

	for _, statement := range []string{
		"CREATE DATABASE " + p.Schema,
		"CREATE TABLE " + p.Schema + ".blocker (id INT PRIMARY KEY, v INT NOT NULL)",
		"INSERT INTO " + p.Schema + ".blocker VALUES (1, 0)",
	} {
		execGR(t, topology.Primary, statement)
	}
	waitAppliedOn(t, topology.Primary, topology.Secondary, topology.Rejoiner)

	StopMember(t, topology.Rejoiner)
	ctx, cancel := cleanupSafeContext(t, grStatementTimeout)
	defer cancel()
	lock, err := topology.Rejoiner.Conn(ctx)
	if err != nil {
		t.Fatalf("pin the lock session on the peer: %v", err)
	}
	p.lock = lock
	if _, err := lock.ExecContext(ctx, "LOCK TABLES "+p.Schema+".blocker READ"); err != nil {
		t.Fatalf("lock the blocker table on the peer: %v", err)
	}
	execGR(t, topology.Primary, "UPDATE "+p.Schema+".blocker SET v = v + 1 WHERE id = 1")

	p.started = StartMemberInBackground(t, topology.Rejoiner)
	pollUntil(t, "the primary's view lists the peer RECOVERING", func() (bool, string) {
		state := peerState(t, topology.Primary, p.uuid)
		return state == MemberRecovering, "the peer is " + state
	})
	return p
}

// Release lets the peer's applier finish, waits for the peer to be ONLINE, and drops the harness's schema. It is
// safe to call more than once.
func (p *RecoveringPeer) Release(t testing.TB) {
	t.Helper()
	if p.released {
		return
	}
	p.released = true

	if p.lock != nil {
		ctx, cancel := cleanupSafeContext(t, grStatementTimeout)
		if _, err := p.lock.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
			t.Errorf("release the peer's lock: %v", err)
		}
		cancel()
		_ = p.lock.Close()
		p.lock = nil
	}
	if p.started != nil {
		p.started()
	}
	execGR(t, p.topology.Primary, "DROP DATABASE IF EXISTS "+p.Schema)
	waitAppliedOn(t, p.topology.Primary, p.topology.Secondary, p.topology.Rejoiner)
}

// waitAppliedOn waits until every pool of others has applied every transaction primary has.
func waitAppliedOn(t testing.TB, primary *sql.DB, others ...*sql.DB) {
	t.Helper()
	ctx, cancel := cleanupSafeContext(t, GroupReplicationWaitDeadline)
	defer cancel()
	gtid, err := dbsrepl.NewInspector(primary).GTIDStatus(ctx)
	if err != nil {
		t.Fatalf("read the group primary's gtid_executed: %v", err)
	}
	for i, db := range others {
		var timedOut int
		// harness read: waiting for a GTID set is a function call, not a fact
		if err := db.QueryRowContext(ctx, sqlWaitForGTIDSet, gtid.Executed).Scan(&timedOut); err != nil {
			t.Fatalf("wait for member %d to apply %q: %v", i+1, gtid.Executed, err)
		}
		if timedOut != 0 {
			t.Fatalf("member %d did not apply %q within 60 s", i+1, gtid.Executed)
		}
	}
}
