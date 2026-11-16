//go:build integration

package engine

import (
	"context"
	"database/sql"
	"strconv"
	"testing"
	"time"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"

	"github.com/dbsmedya/goarchive/v3/internal/testestate"
)

// The connector's fence on real servers (spec §4.2, §8: *The four plugin states*, *Opposing values*, *Every connection*;
// GR C8). Each pool is goarchive's own (database.OpenPool, through openServer), so the connector that runs is the one a run
// uses; a session's starting value is emulated with database.Global, which reaches the session first, as a server's
// init_connect or global would. The session's value is read through the library's fact on the connection.

const fenceValue = "BEFORE_ON_PRIMARY_FAILOVER"

func eventualSession() map[string]string { return map[string]string{consistencyVar: "'EVENTUAL'"} }

func sessionOf(value string) map[string]string {
	return map[string]string{consistencyVar: "'" + value + "'"}
}

// sessionConsistency reads the session group_replication_consistency of one connection of db, and the connection's id.
func sessionConsistency(t *testing.T, db *sql.DB) (value string, id int64) {
	t.Helper()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("a connection was not handed out: %v", err)
	}
	defer func() { _ = conn.Close() }()
	got, err := dbsrepl.NewInspector(conn).GroupConsistency(ctx)
	if err != nil || !got.SessionConsistencyKnown {
		t.Fatalf("read the session's group_replication_consistency: %v (known %v)", err, got.SessionConsistencyKnown)
	}
	if err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&id); err != nil { // harness read: no fact
		t.Fatal(err)
	}
	return got.SessionConsistency, id
}

// TestMySQLTheFenceOnTheStatesOfThePlugin: a server without the plugin is left as it is (the session stays EVENTUAL and a
// read works); on a server whose plugin is ACTIVE the EVENTUAL session is raised to BEFORE_ON_PRIMARY_FAILOVER, on a running
// member and on one that was stopped (the plugin stays ACTIVE there), and a read works on each. DISABLED is not a server of
// this estate: the connector's table row for it is a unit test, and a probe against the library's own DISABLED server is
// the evidence of the 2026-11 measurements (spec §8, round 3).
func TestMySQLTheFenceOnTheStatesOfThePlugin(t *testing.T) {
	t.Run("the plugin absent", func(t *testing.T) {
		// the plain destination: db2 on the plain estate, gr-dest on the group estate
		db := openServer(t, "TEST_DEST", "", eventualSession())
		if got, _ := sessionConsistency(t, db); got != "EVENTUAL" {
			t.Errorf("a session on a server without the plugin reads %q, want it left EVENTUAL", got)
		}
		var one int
		if err := db.QueryRowContext(context.Background(), "SELECT 1").Scan(&one); err != nil || one != 1 {
			t.Errorf("a read on it = %d, %v", one, err)
		}
	})

	t.Run("the plugin ACTIVE and the member running", func(t *testing.T) {
		requireGroupMemberSource(t)
		for _, prefix := range []string{"TEST_SOURCE", "TEST_GROUP_MEMBER2"} {
			db := openServer(t, prefix, "", eventualSession())
			if got, _ := sessionConsistency(t, db); got != fenceValue {
				t.Errorf("%s: an EVENTUAL session reads %q, want it raised to %s", prefix, got, fenceValue)
			}
		}
	})

	t.Run("the plugin ACTIVE and not running in a group", func(t *testing.T) {
		requireGroupMemberSource(t)
		topology := testestate.OpenGroupTopology(t)
		testestate.StartGroupModeChange(t, topology) // its cleanup rejoins a member a failing test leaves stopped
		testestate.StopMember(t, topology.Rejoiner)
		db := openServer(t, "TEST_GROUP_MEMBER3", "", eventualSession())
		if got, _ := sessionConsistency(t, db); got != fenceValue {
			t.Errorf("a stopped member's EVENTUAL session reads %q, want it raised to %s: the plugin is ACTIVE there", got, fenceValue)
		}
		var one int
		if err := db.QueryRowContext(context.Background(), "SELECT 1").Scan(&one); err != nil || one != 1 {
			t.Errorf("a fenced session on a member that is not running could not read: %d, %v", one, err)
		}
		testestate.StartMember(t, topology.Rejoiner)
	})
}

// TestMySQLTheFenceNeverWeakensASessionAndTheGlobalDecidesNothing (spec §8, round 2 finding 2): a session that connection
// initialization made stronger than the server's global is not weakened, and one it made EVENTUAL on a server whose
// global is stronger is fenced.
func TestMySQLTheFenceNeverWeakensASessionAndTheGlobalDecidesNothing(t *testing.T) {
	requireGroupMemberSource(t)
	topology := testestate.OpenGroupTopology(t)

	t.Run("a stronger session on a server whose global is EVENTUAL", func(t *testing.T) {
		testestate.SetGlobalConsistency(t, topology.Secondary, "EVENTUAL")
		for _, stronger := range []string{"BEFORE", "AFTER", "BEFORE_AND_AFTER", fenceValue} {
			db := openServer(t, "TEST_GROUP_MEMBER2", "", sessionOf(stronger))
			if got, _ := sessionConsistency(t, db); got != stronger {
				t.Errorf("a session started at %s reads %q, want it kept", stronger, got)
			}
		}
	})

	t.Run("an EVENTUAL session on a server whose global is stronger", func(t *testing.T) {
		testestate.SetGlobalConsistency(t, topology.Secondary, fenceValue)
		db := openServer(t, "TEST_GROUP_MEMBER2", "", eventualSession())
		if got, _ := sessionConsistency(t, db); got != fenceValue {
			t.Errorf("an EVENTUAL session reads %q on a server whose global is %s, want it fenced: the global decides nothing", got, fenceValue)
		}
	})
}

// TestMySQLAReplacementConnectionIsFencedBeforeItsFirstRead (spec §8, *Every connection*, review finding 10): the session of
// a pooled connection is killed on the server, which is what makes the pool open a replacement in the middle of a run, and
// the replacement's session is fenced before the pool hands it out, on a server whose global is EVENTUAL.
func TestMySQLAReplacementConnectionIsFencedBeforeItsFirstRead(t *testing.T) {
	requireGroupMemberSource(t)
	topology := testestate.OpenGroupTopology(t)
	testestate.SetGlobalConsistency(t, topology.Secondary, "EVENTUAL")
	db := openServer(t, "TEST_GROUP_MEMBER2", "", eventualSession())
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	first, firstID := sessionConsistency(t, db)
	if first != fenceValue {
		t.Fatalf("the first connection's session reads %q, want %s", first, fenceValue)
	}
	killer := openServer(t, "TEST_GROUP_MEMBER2", "", nil)
	if _, err := killer.ExecContext(context.Background(), "KILL "+strconv.FormatInt(firstID, 10)); err != nil {
		t.Fatalf("kill the pooled session: %v", err)
	}
	// the server removes the killed session at once; the pool's next use finds it gone and opens another
	deadline := time.Now().Add(30 * time.Second)
	for {
		second, secondID := sessionConsistency(t, db)
		if secondID != firstID {
			if second != fenceValue {
				t.Errorf("the replacement connection (%d, after %d) reads %q before its first use, want %s", secondID, firstID, second, fenceValue)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the pool still hands out connection %d after it was killed", firstID)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
