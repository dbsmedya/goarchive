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
