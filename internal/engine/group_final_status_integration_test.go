//go:build integration

package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/lock"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
	"github.com/dbsmedya/goarchive/v3/internal/testestate"
)

// TestMySQLTheFinalStatusOnAMemberDestinationAfterAFailoverIsBoundedByTheCleanupTimeout (spec §8, *Final status on a member
// destination*; M8 passed, so the group is a supported destination; GR C8). The destination is gr2, the group's primary and
// writable, and the run's work is done and committed. As the run's last act before its final status the group fails over:
// gr1 becomes the primary, and gr2 is elected again with a backlog it has not applied, so its sessions are behind the fence.
// The final status is an UPDATE on the lock session, which the fence holds: the run must still exit within the 5 s cleanup
// bound, the failure is logged as it is today and is not the run's error, and the lock session's server thread keeps the job
// lock until the hold ends (M14). Releasing the backlog lets the thread end: the late write is then applied, and the lock is free.
func TestMySQLTheFinalStatusOnAMemberDestinationAfterAFailoverIsBoundedByTheCleanupTimeout(t *testing.T) {
	requireGroupMemberSource(t)
	topology := testestate.OpenGroupTopology(t)
	testestate.SetGlobalConsistency(t, topology.Secondary, "EVENTUAL")
	testestate.ElectPrimary(t, topology, topology.Secondary) // gr2 is the primary and writable: a destination G13 lets through

	e := newEnvOn(t, "grfinal", "TEST_DEST", "TEST_GROUP_MEMBER2")
	e.both(t, numsDDL)
	seedNums(t, e.src, 20)
	core, logs := observer.New(zapcore.DebugLevel)
	cfg := e.config(t, job(t, jobspec.Archive, numsJob))
	cfg.Logger = &logger.Logger{SugaredLogger: zap.New(core).Sugar()}

	var fence *testestate.GroupFence
	var hookEnded time.Time
	cfg.at = func(p point) {
		if p != atFinished {
			return
		}
		testestate.ElectPrimary(t, topology, topology.Primary) // gr1 is the primary again: gr2 is a secondary
		fence = testestate.BuildGroupFence(t, topology)        // gr2 is elected with its backlog held: a newly elected primary
		hookEnded = time.Now()
	}

	rep, err := e.run(t, cfg)
	took := time.Since(hookEnded)
	if fence == nil {
		t.Fatalf("the run ended (%v) without reaching its final status: the failover was never made", err)
	}
	if err != nil {
		t.Errorf("the run ended with %v, want nil: the work was done, and a final status that could not be written is logged and is not the run's error", err)
	}
	if rep == nil || rep.Roots != 20 {
		t.Errorf("report = %+v, want the 20 roots archived before the failover", rep)
	}
	if took > 25*time.Second {
		t.Errorf("the run took %s to end after the failover, want it bounded by the %s cleanup timeout", took.Round(time.Millisecond), cleanupTimeout)
	}
	failed := false
	for _, le := range logs.All() {
		if strings.Contains(le.Message, "failed to write the final job status") {
			failed = true
		}
	}
	if !failed {
		t.Errorf("log lines = %v, want the final-status failure logged as it is today", messagesOf(logs.All()))
	}

	// the lock session's thread is held, with the job lock, until the backlog is applied
	jobLock := lock.JobLockName(cfg.JobSchema, cfg.Name)
	var holder *int64
	if err := fence.Primary.QueryRowContext(context.Background(), "SELECT IS_USED_LOCK(?)", jobLock).Scan(&holder); err != nil || holder == nil {
		t.Errorf("the job lock is held by %v (%v) after the run ended, want it kept by the held thread of the lock session (M14)", holder, err)
	}
	fence.UnlockApplier(t)
	for deadline := time.Now().Add(time.Minute); ; time.Sleep(250 * time.Millisecond) {
		if err := fence.Primary.QueryRowContext(context.Background(), "SELECT IS_USED_LOCK(?)", jobLock).Scan(&holder); err != nil {
			t.Fatal(err)
		}
		if holder == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the job lock is still held by connection %d a minute after the backlog was applied", *holder)
		}
	}
}

func messagesOf(entries []observer.LoggedEntry) []string {
	var out []string
	for _, e := range entries {
		out = append(out, e.Message)
	}
	return out
}
