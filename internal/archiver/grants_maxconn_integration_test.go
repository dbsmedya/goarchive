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
