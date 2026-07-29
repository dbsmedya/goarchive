//go:build integration

package archiver

import (
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/internal/graph"
)

// TestGrantsReadAndReleaseUnderSingleConnection is the deadlock proof required by spec
// §2. With SetMaxOpenConns(1) — a configuration goarchive supports today — a preflight
// run that held the grants Conn while issuing any other query would block forever.
//
// The test runs a FULL preflight (not just the grants stage) against a one-connection
// pool, under a timeout. A hang is the failure mode being guarded against, so the
// timeout IS the assertion.
func TestGrantsReadAndReleaseUnderSingleConnection(t *testing.T) {
	_, runCtx := SetupIntegrationTest(t)

	f := newChrFixture(t, runCtx)
	const ddl = "CREATE TABLE orders (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB"
	f.ExecSource(t, runCtx, ddl)
	f.ExecDest(t, runCtx, ddl)

	dstAcct := chrCreateAccount(t, runCtx, f.DestDB, "destination",
		"GRANT CREATE, SELECT, INSERT, UPDATE ON `"+f.DestSchema+"`.* TO {{ACCOUNT}}")

	dstDB := chrOpenAs(t, dstAcct, f.DestSchema)
	dstDB.SetMaxOpenConns(1)

	// CheckerAs may call t.Fatalf, so construct it on the test goroutine. The source
	// stays on the fixture's proven admin pool: phase 019 consumes destination grants
	// only. Phase 021 adds the source-side one-connection proof.
	c := f.CheckerAs(t, graph.NewGraph("orders", "id"), f.SourceDB, dstDB)

	done := make(chan error, 1)
	go func() {
		done <- chrRun(t, runCtx, c, chrCommands[0], false)
	}()

	watchdog := time.NewTimer(60 * time.Second)
	defer watchdog.Stop()
	select {
	case err := <-done:
		chrAssertPasses(t, err)
	case <-watchdog.C:
		t.Fatal("preflight deadlocked with max_connections=1: the grants Conn is being " +
			"held across another query. It must be released immediately after Inspector.Grants returns")
	}
}

// TestSourceGrantsReadAndReleaseUnderSingleConnection is the source-side counterpart
// to TestGrantsReadAndReleaseUnderSingleConnection. It belongs here because
// sourceGrants first exists in this phase (021).
func TestSourceGrantsReadAndReleaseUnderSingleConnection(t *testing.T) {
	_, runCtx := SetupIntegrationTest(t)
	f := newChrFixture(t, runCtx)
	const ddl = "CREATE TABLE orders (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB"
	f.ExecSource(t, runCtx, ddl)
	f.ExecDest(t, runCtx, ddl)

	// TWO global grants, for two different checks — see the note under this test.
	srcAcct := chrCreateAccount(t, runCtx, f.SourceDB, "source",
		"GRANT PROCESS ON *.* TO {{ACCOUNT}}",
		"GRANT SELECT ON *.* TO {{ACCOUNT}}",
		"GRANT DELETE ON `"+f.SourceSchema+"`.* TO {{ACCOUNT}}")
	srcDB := chrOpenAs(t, srcAcct, f.SourceSchema)
	srcDB.SetMaxOpenConns(1)

	// Construct on the test goroutine: CheckerAs may call t.Fatalf. Keep f.DestDB as
	// the fixture's unconstrained admin pool; cleanup uses it to drop the account and
	// schema. Only the restricted source pool is the one-slot subject of this proof.
	c := f.CheckerAs(t, graph.NewGraph("orders", "id"), srcDB, f.DestDB)

	done := make(chan error, 1)
	go func() {
		done <- chrRun(t, runCtx, c, chrCommands[0], false)
	}()

	watchdog := time.NewTimer(60 * time.Second)
	defer watchdog.Stop()
	select {
	case err := <-done:
		chrAssertPasses(t, err)
	case <-watchdog.C:
		t.Fatal("preflight deadlocked with a one-connection source pool: the grants " +
			"Conn must be released before the next source query")
	}
}
