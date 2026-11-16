//go:build integration

package engine

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
	"github.com/dbsmedya/goarchive/v3/internal/testestate"
)

// Runs on a group in multi-primary mode (spec §4.6 and §8's *Multi-primary, integration*, GR C7). The group is
// switched with the library's switch function (testestate's copy of its helper), which restores it in its cleanup and
// fails the test if it cannot. Members are stopped after the switch, never before it.

// returningGate is a LagGate that, on its first call, starts the stopped peer in the background and returns once it
// is ONLINE: the peer returns while the run holds its locks, after preflight read its facts and before the first
// batch.
type returningGate struct {
	mu    sync.Mutex
	calls int
	start func()
}

func (g *returningGate) WaitForLag(context.Context, <-chan struct{}) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.calls++
	if g.calls == 1 {
		g.start()
	}
	return nil
}

// startPeer is the function a returningGate runs: START GROUP_REPLICATION on the stopped peer, then a wait until its own
// row says ONLINE. It reports with t.Errorf only: the engine calls the gate from whatever goroutine runs the batch.
func startPeer(t *testing.T, db *sql.DB) func() {
	return func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), testestate.GroupReplicationWaitDeadline)
		defer cancel()
		if _, err := db.ExecContext(ctx, "START GROUP_REPLICATION"); err != nil {
			t.Errorf("start the returning peer: %v", err)
			return
		}
		for ctx.Err() == nil {
			if members, err := dbsrepl.NewInspector(db).GroupMembers(ctx); err == nil {
				for _, m := range members {
					if m.Self && m.State == testestate.MemberOnline {
						return
					}
				}
			}
			time.Sleep(250 * time.Millisecond)
		}
		t.Errorf("the returning peer was not ONLINE within %s", testestate.GroupReplicationWaitDeadline)
	}
}

// seededEnv is a source on the group's primary and a plain destination with the key family's tables and rows.
func seededEnv(t *testing.T, name string) (env, keyFamily) {
	t.Helper()
	e := newEnv(t, name)
	f := keyFamilies[0]
	e.both(t, f.tables()...)
	f.seed(t, e)
	return e, f
}

// multiPrimaryRefusedWithNoSideEffect requires err to be the multi-primary refusal, and the run to have left nothing:
// no tracking table and no row moved.
func multiPrimaryRefusedWithNoSideEffect(t *testing.T, e env, err error, before int) {
	t.Helper()
	var pe *preflight.Error
	if !errors.As(err, &pe) || pe.Check != "GROUP_MULTI_PRIMARY_CHECK" {
		t.Fatalf("Run() = %v, want GROUP_MULTI_PRIMARY_CHECK", err)
	}
	if !strings.Contains(pe.Message, "--allow-multi-primary") {
		t.Errorf("the refusal %q does not name the bypass", pe.Message)
	}
	var tables, roots int
	if err := e.dst.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = 'archiver_job'",
		e.dstSchema).Scan(&tables); err != nil || tables != 0 {
		t.Errorf("tracking tables = %d (%v) after a refusal before any lock, want none", tables, err)
	}
	if err := e.src.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM r").Scan(&roots); err != nil || roots != before {
		t.Errorf("source roots = %d (%v), want the %d seeded rows untouched", roots, err, before)
	}
}

// TestMySQLAMultiPrimaryGroupRefusesEveryJobTypeBeforeAnyLock: with the group in multi-primary mode an archive, a purge
// and a copy-only run are refused at the start, with the bypass named, and nothing of the run exists afterwards.
func TestMySQLAMultiPrimaryGroupRefusesEveryJobTypeBeforeAnyLock(t *testing.T) {
	requireGroupMemberSource(t)
	topology := testestate.OpenGroupTopology(t)
	testestate.StartGroupModeChange(t, topology).SwitchToMultiPrimary(t)
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
		t.Run(string(typ), func(t *testing.T) {
			e, _ := seededEnv(t, "mprefuse"+string(typ))
			var before int
			if err := e.src.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM r").Scan(&before); err != nil || before == 0 {
				t.Fatalf("seeded roots = %d (%v), want some", before, err)
			}
			cfg := e.config(t, job(t, typ, keyJob))
			_, err := e.run(t, cfg)
			multiPrimaryRefusedWithNoSideEffect(t, e, err, before)
		})
	}
}

// TestMySQLAPeerReturningDuringAMultiPrimaryRunChangesNothingItReports: with --allow-multi-primary an archive starts on a
// multi-primary group with a banner that names the case, and a stopped peer that returns while the run holds its locks
// changes nothing the run reports: one banner, one report block, the same facts as at the start, and the run completes.
func TestMySQLAPeerReturningDuringAMultiPrimaryRunChangesNothingItReports(t *testing.T) {
	requireGroupMemberSource(t)
	topology := testestate.OpenGroupTopology(t)
	testestate.StartGroupModeChange(t, topology).SwitchToMultiPrimary(t)
	testestate.StopMember(t, topology.Rejoiner)

	e, f := seededEnv(t, "mpbypass")
	core, logs := observer.New(zapcore.DebugLevel)
	var banners []string
	cfg := e.config(t, job(t, jobspec.Archive, keyJob))
	cfg.Logger = &logger.Logger{SugaredLogger: zap.New(core).Sugar()}
	cfg.AllowMultiPrimary = true
	cfg.Banner = func(b string) { banners = append(banners, b) }
	cfg.Lag = &returningGate{start: startPeer(t, topology.Rejoiner)}
	rep := e.mustRun(t, cfg)

	if rep.Roots != int64(len(f.keys)) {
		t.Errorf("roots = %d, want the %d eligible roots archived", rep.Roots, len(f.keys))
	}
	if len(banners) != 1 || !strings.HasPrefix(banners[0], "WARNING: --allow-multi-primary: the source is a member of Group Replication group ") {
		t.Errorf("banners = %q, want one that names the case: the source is a running member of a group in multi-primary mode", banners)
	}
	var blocks []string
	for _, le := range logs.All() {
		if strings.HasPrefix(le.Message, "Group Replication:") {
			blocks = append(blocks, le.Message)
		}
	}
	if len(blocks) != 1 || !strings.Contains(blocks[0], ", ONLINE, PRIMARY, mode multi-primary; ") {
		t.Errorf("group reports = %q, want one, with the facts as they were at the start", blocks)
	}
	if state := testestate.MemberState(t, topology.Rejoiner); state != testestate.MemberOnline {
		t.Errorf("gr3 is %s after the run, want ONLINE: the peer returned during it", state)
	}
}

// TestMySQLAPeerReturningDuringASinglePrimaryRunDoesNotStopIt: in single-primary mode a stopped peer that returns while
// a run holds its locks does not refuse or stop it: the run completes with no banner, and its report names the mode.
func TestMySQLAPeerReturningDuringASinglePrimaryRunDoesNotStopIt(t *testing.T) {
	requireGroupMemberSource(t)
	topology := testestate.OpenGroupTopology(t)
	testestate.StartGroupModeChange(t, topology) // the cleanup rejoins a member a failing test leaves stopped
	testestate.StopMember(t, topology.Rejoiner)

	e, f := seededEnv(t, "spreturn")
	core, logs := observer.New(zapcore.DebugLevel)
	var banners []string
	cfg := e.config(t, job(t, jobspec.Archive, keyJob))
	cfg.Logger = &logger.Logger{SugaredLogger: zap.New(core).Sugar()}
	cfg.Banner = func(b string) { banners = append(banners, b) }
	cfg.Lag = &returningGate{start: startPeer(t, topology.Rejoiner)}
	rep := e.mustRun(t, cfg)

	if rep.Roots != int64(len(f.keys)) {
		t.Errorf("roots = %d, want the %d eligible roots archived", rep.Roots, len(f.keys))
	}
	if len(banners) != 0 {
		t.Errorf("banners = %q, want none: nothing was bypassed", banners)
	}
	var blocks []string
	for _, le := range logs.All() {
		if strings.HasPrefix(le.Message, "Group Replication:") {
			blocks = append(blocks, le.Message)
		}
	}
	if len(blocks) != 1 || !strings.Contains(blocks[0], ", ONLINE, PRIMARY, mode single-primary; ") {
		t.Errorf("group reports = %q, want one that names single-primary mode", blocks)
	}
}
