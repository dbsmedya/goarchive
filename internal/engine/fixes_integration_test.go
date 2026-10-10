//go:build integration

package engine

import (
	"context"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
)

// TestMySQLFailedFirstHeartbeat (M5-F1): once the job is marked running, every way out of the
// run writes its final status. A first heartbeat that fails leaves the job failed, not
// running, so other jobs on the root table are not refused as if it were alive.
func TestMySQLFailedFirstHeartbeat(t *testing.T) {
	e := newEnv(t, "beat")
	e.both(t, intTables)
	execAll(t, e.src, "INSERT INTO r VALUES (1, 1)")
	cfg := e.config(t, job(t, jobspec.Archive, decimalJob))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg.at = func(p point) {
		if p == atMarked {
			cancel() // the heartbeat that follows fails on the cancelled context
		}
	}
	if _, err := Run(ctx, cfg); err == nil {
		t.Fatal("Run() passed a failed first heartbeat")
	}
	if got := count(t, e.dst, "SELECT job_status FROM archiver_job WHERE job_name = 'j'"); got != int64(tracking.JobFailed) {
		t.Errorf("job_status = %d, want failed (%d)", got, tracking.JobFailed)
	}
}

// TestMySQLRecoveryHonoursStop (M5-F3): a first signal during a long recovery takes effect
// at the next page boundary, as it does at a batch boundary. The page in flight finishes,
// the run reports Stopped with the job idle, and the next run recovers the rest.
func TestMySQLRecoveryHonoursStop(t *testing.T) {
	e := newEnv(t, "recstop")
	e.both(t, orderTables("CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")...)
	seedOrders(t, e, 3)
	whole := e.config(t, job(t, jobspec.Archive, orderJob))
	whole.Processing.BatchSize = 3
	e.interrupted(t, whole, atCommitted) // three roots copied, none deleted

	stop := make(chan struct{})
	cfg := e.config(t, job(t, jobspec.Archive, orderJob))
	cfg.Processing.BatchSize = 1 // recovery pages of one root
	cfg.Stop = stop
	cfg.at = func(p point) {
		if p == atDeleted {
			select {
			case <-stop:
			default:
				close(stop)
			}
		}
	}
	rep := e.mustRun(t, cfg)
	if !rep.Stopped || rep.Recovered != 1 || rep.Batches != 0 {
		t.Fatalf("report = %+v, want one recovered page, then a stop", rep)
	}
	if got := statuses(t, e.dst, e.dstSchema); got[int(tracking.StatusCopied)] != 2 || got[int(tracking.StatusCompleted)] != 1 {
		t.Errorf("log = %v, want 2 copied and 1 completed", got)
	}
	if got := count(t, e.dst, "SELECT job_status FROM archiver_job WHERE job_name = 'j'"); got != int64(tracking.JobIdle) {
		t.Errorf("job_status = %d, want idle", got)
	}
	if rep := e.mustRun(t, e.config(t, job(t, jobspec.Archive, orderJob))); rep.Recovered != 2 {
		t.Errorf("rerun = %+v, want the 2 remaining roots recovered", rep)
	}
}
