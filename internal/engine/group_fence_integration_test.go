//go:build integration

package engine

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
	"github.com/dbsmedya/goarchive/v3/internal/testestate"
)

// The failover fence, G12 (spec §4.2, §8 *Prevention*; GR C8), on a real group. gr2 is made a newly elected primary
// whose backlog is held (the harness copied from dbsgomysql), on a group whose global group_replication_consistency is
// EVENTUAL, so that nothing but goarchive's own SET puts its sessions behind the fence. The tables are two parents applied
// before the lock and five children that are the backlog itself: a read that is not held sees the parents and none of the
// children, which is the stale read the fence is there to prevent. The test needs the group estate and runs in its serial stage.

// fenceFixture is two eligible parents and one that is not, applied before the lock, and five children that are the backlog:
// four of them belong to the eligible parents.
var fenceFixture = testestate.FenceOptions{
	Prepare: []string{
		"CREATE TABLE {schema}.r (k INT NOT NULL, eligible TINYINT NOT NULL, PRIMARY KEY (k)) ENGINE=InnoDB",
		"CREATE TABLE {schema}.c (id INT NOT NULL, rk INT NULL, PRIMARY KEY (id), KEY ix_rk (rk), CONSTRAINT fk_c_r FOREIGN KEY (rk) REFERENCES r (k)) ENGINE=InnoDB",
		"INSERT INTO {schema}.r VALUES (1, 1), (2, 1), (3, 0)",
	},
	Backlog: []string{"INSERT INTO {schema}.c VALUES (1, 1), (2, 1), (3, 2), (4, 2), (5, 3)"},
}

// stillWaiting reports (true) when no result has come back within d.
func stillWaiting[T any](results <-chan T, d time.Duration) (T, bool) {
	select {
	case r := <-results:
		return r, false
	case <-time.After(d):
		var zero T
		return zero, true
	}
}

// awaitResult waits for a result for d and fails the test when it does not come.
func awaitResult[T any](t *testing.T, what string, results <-chan T, d time.Duration) T {
	t.Helper()
	select {
	case r := <-results:
		return r
	case <-time.After(d):
		t.Fatalf("%s did not finish within %s of the backlog being applied", what, d)
		panic("unreachable")
	}
}

type countResult struct {
	n   int
	err error
}

type bannerLog struct {
	mu   sync.Mutex
	list []string
}

func (b *bannerLog) add(s string) { b.mu.Lock(); b.list = append(b.list, s); b.mu.Unlock() }
func (b *bannerLog) all() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.list...)
}

// TestMySQLAFencedSessionIsHeldOnANewlyElectedPrimaryAndTheLateRowsAreRead is spec §8's *Prevention* (a), (b) and (c) on one
// newly elected primary with its backlog held: (c) an archive and a purge are refused at the start, at once, by G9, with
// the cause named; (a) a table read through goarchive's pool is held, and returns the late rows once the backlog is
// applied; (b) a copy-only run is warned, held, and then copies every row, the late children included.
func TestMySQLAFencedSessionIsHeldOnANewlyElectedPrimaryAndTheLateRowsAreRead(t *testing.T) {
	requireGroupMemberSource(t)
	topology := testestate.OpenGroupTopology(t)
	testestate.SetGlobalConsistency(t, topology.Secondary, "EVENTUAL")
	fence := testestate.BuildGroupFenceWith(t, topology, fenceFixture)

	// the source is the fenced primary, through goarchive's own pool: its sessions are behind the fence by goarchive's SET
	src := openServer(t, "TEST_GROUP_MEMBER2", fence.Schema, nil)
	conn, err := src.Conn(context.Background())
	if err != nil {
		t.Fatalf("a connection on the newly elected primary was not handed out (the fence's reads must pass it): %v", err)
	}
	var session string
	if err := conn.QueryRowContext(context.Background(), "SELECT @@SESSION.group_replication_consistency").Scan(&session); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if session != "BEFORE_ON_PRIMARY_FAILOVER" {
		t.Errorf("a session of goarchive's pool reads group_replication_consistency %q on a group whose global is EVENTUAL, want BEFORE_ON_PRIMARY_FAILOVER", session)
	}

	// what a session that is not fenced sees on the same server: the parents, and none of the late children
	var unfenced int
	if err := fence.Primary.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+fence.Schema+".c").Scan(&unfenced); err != nil {
		t.Fatal(err)
	}
	if unfenced != 0 {
		t.Fatalf("an unfenced session counts %d children, want 0: the backlog is not held", unfenced)
	}

	dstSchema, dst := scratch(t, "TEST_DEST", "fencecopy")
	execAll(t, dst, "CREATE TABLE r (k INT NOT NULL, eligible TINYINT NOT NULL, PRIMARY KEY (k)) ENGINE=InnoDB",
		"CREATE TABLE c (id INT NOT NULL, rk INT NULL, PRIMARY KEY (id), KEY ix_rk (rk), CONSTRAINT fk_c_r FOREIGN KEY (rk) REFERENCES r (k)) ENGINE=InnoDB")
	e := env{src: src, dst: dst, srcSchema: fence.Schema, dstSchema: dstSchema}

	// (c) an archive and a purge are refused at the start, at once, with the cause: nothing is held, nothing is replayed
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge} {
		began := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		_, err := e.runContext(t, ctx, e.config(t, job(t, typ, keyJob)))
		cancel()
		pe := wantErr[*preflight.Error](t, err)
		if pe.Check != "SOURCE_READ_ONLY_CHECK" || !strings.Contains(pe.Message, "The source is the PRIMARY of group ") || !strings.Contains(pe.Message, "newly elected") {
			t.Errorf("%s: %v, want SOURCE_READ_ONLY_CHECK naming the newly elected primary", typ, err)
		}
		if took := time.Since(began); took > 30*time.Second {
			t.Errorf("%s: refused after %s, want at once: nothing may hold it", typ, took)
		}
		if got := count(t, dst, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = 'archiver_job'", dstSchema); got != 0 {
			t.Errorf("%s: the destination's tracking tables exist after a refusal at the start", typ)
		}
	}

	// (a) a table read through the pool is held, and then returns the late rows
	reads := make(chan countResult, 1)
	go func() {
		var n int
		err := src.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM c").Scan(&n)
		reads <- countResult{n, err}
	}()

	// (b) a copy-only run: warned, then held at its first table read
	var banners bannerLog
	cfg := e.config(t, job(t, jobspec.CopyOnly, keyJob))
	cfg.Banner = banners.add
	type runResult struct {
		rep *Report
		err error
	}
	runs := make(chan runResult, 1)
	go func() {
		rep, err := e.runContext(t, context.Background(), cfg)
		runs <- runResult{rep, err}
	}()

	if r, isHeld := stillWaiting(reads, 4*time.Second); !isHeld {
		t.Fatalf("a table read through goarchive's pool returned %d (%v) while the backlog was unapplied: the session is not fenced", r.n, r.err)
	}
	if r, isHeld := stillWaiting(runs, time.Second); !isHeld {
		t.Fatalf("a copy-only run finished (%v) while the backlog was unapplied: it read around the fence", r.err)
	}
	if b := banners.all(); len(b) != 1 || !strings.Contains(b[0], "PRIMARY") || !strings.Contains(b[0], "read-only") {
		t.Errorf("banners while the run was held = %q, want copy-only's one warning of a read-only primary", b)
	}

	fence.UnlockApplier(t) // the cleanup's Release drops the schema, after the run has read it

	if r := awaitResult(t, "the held table read", reads, 2*time.Minute); r.err != nil || r.n != 5 {
		t.Errorf("the held read returned %d rows (%v), want the 5 late children", r.n, r.err)
	}
	r := awaitResult(t, "the held copy-only run", runs, 3*time.Minute)
	if r.err != nil {
		t.Fatalf("the copy-only run ended with %v, want it to copy once the backlog was applied", r.err)
	}
	if got := count(t, dst, "SELECT COUNT(*) FROM r"); got != 2 {
		t.Errorf("destination roots = %d, want the 2 eligible parents", got)
	}
	if got := count(t, dst, "SELECT COUNT(*) FROM c"); got != 4 {
		t.Errorf("destination children = %d, want the 4 late children of the eligible parents: the run read around the fence", got)
	}
}

// TestMySQLAFencedSessionOnAnOrdinaryLaggingSecondaryIsNotHeld (spec §8, *Prevention*, rev 5.1): goarchive's own value is
// BEFORE_ON_PRIMARY_FAILOVER, which adds a barrier on a newly elected primary and none on a secondary. A secondary whose
// applier is behind (nobody elected) is read at once, and shows what it has applied: reading a replica means that, and the
// fence does not change it. The backlog is then applied and the late rows are there.
func TestMySQLAFencedSessionOnAnOrdinaryLaggingSecondaryIsNotHeld(t *testing.T) {
	requireGroupMemberSource(t)
	topology := testestate.OpenGroupTopology(t)
	testestate.SetGlobalConsistency(t, topology.Secondary, "EVENTUAL")
	opts := fenceFixture
	opts.NoElection = true
	fence := testestate.BuildGroupFenceWith(t, topology, opts)

	src := openServer(t, "TEST_GROUP_MEMBER2", fence.Schema, nil)
	reads := make(chan countResult, 1)
	go func() {
		var n int
		var session string
		conn, err := src.Conn(context.Background())
		if err == nil {
			err = conn.QueryRowContext(context.Background(), "SELECT @@SESSION.group_replication_consistency").Scan(&session)
		}
		if err == nil {
			err = conn.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM c").Scan(&n)
		}
		if conn != nil {
			_ = conn.Close()
		}
		if err == nil && session != "BEFORE_ON_PRIMARY_FAILOVER" {
			n = -1
		}
		reads <- countResult{n, err}
	}()
	r, isHeld := stillWaiting(reads, 60*time.Second)
	if isHeld {
		t.Fatal("a fenced session was held on an ordinary secondary: BEFORE_ON_PRIMARY_FAILOVER adds no barrier there")
	}
	if r.err != nil || r.n != 0 {
		t.Errorf("the read on the lagging secondary = %d (%v), want 0: what the secondary has applied, with the session at BEFORE_ON_PRIMARY_FAILOVER", r.n, r.err)
	}
	fence.UnlockApplier(t)
	var after int
	for deadline := time.Now().Add(time.Minute); ; time.Sleep(250 * time.Millisecond) {
		if err := src.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM c").Scan(&after); err != nil {
			t.Fatalf("read the secondary after the backlog was released: %v", err)
		}
		if after == 5 || time.Now().After(deadline) {
			break
		}
	}
	if after != 5 {
		t.Errorf("after the backlog was applied the secondary counts %d, want the 5 children", after)
	}
}
