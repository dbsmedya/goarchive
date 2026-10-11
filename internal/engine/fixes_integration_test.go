//go:build integration

package engine

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

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
	if _, err := Run(ctx, cfg); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() = %v, want the run stopped by its failed first heartbeat (context.Canceled)", err)
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

// leastInUse is the fewest connections db had in use over ten samples a tenth of a second
// apart.
func leastInUse(db *sql.DB) int {
	least := db.Stats().InUse
	for range 9 {
		time.Sleep(100 * time.Millisecond)
		least = min(least, db.Stats().InUse)
	}
	return least
}

// TestMySQLRecoveryHonoursTheSentinel (M6-F1): the sentinel pause takes effect at a recovery
// page boundary, as it does at a batch boundary. While the file exists the next page waits;
// once it is removed, recovery and the run finish.
func TestMySQLRecoveryHonoursTheSentinel(t *testing.T) {
	e := newEnv(t, "recpause")
	e.both(t, orderTables("CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")...)
	seedOrders(t, e, 3)
	whole := e.config(t, job(t, jobspec.Archive, orderJob))
	whole.Processing.BatchSize = 3
	e.interrupted(t, whole, atCommitted) // three roots copied, none deleted

	sentinel := filepath.Join(t.TempDir(), "pause")
	paused := make(chan struct{})
	var once sync.Once
	cfg := e.config(t, job(t, jobspec.Archive, orderJob))
	cfg.Processing.BatchSize = 1 // recovery pages of one root
	cfg.Processing.SentinelFile = sentinel
	cfg.at = func(p point) {
		if p == atDeleted { // the first page is under way
			once.Do(func() {
				if err := os.WriteFile(sentinel, nil, 0o600); err != nil {
					panic(err)
				}
				close(paused)
			})
		}
	}
	type result struct {
		rep *Report
		err error
	}
	done := make(chan result, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	go func() {
		rep, err := Run(ctx, cfg)
		done <- result{rep, err}
	}()
	// A run that returns before the pause (refused at startup, say) fails the test here
	// instead of leaving it blocked until the package's timeout (M7c T1).
	select {
	case <-paused:
	case res := <-done:
		t.Fatalf("Run() returned before the sentinel paused it: %+v, %v", res.rep, res.err)
	}
	time.Sleep(2*sentinelPollInterval + 500*time.Millisecond)
	// While paused the run holds only its job lock's session: nothing else is left idle to
	// outlive the server's wait_timeout (Fable M7a plan review, I1). A heartbeat can borrow
	// a pooled connection for a moment, so the least of several samples counts.
	if got := leastInUse(cfg.Destination); got != 1 {
		t.Errorf("destination connections in use while paused = %d, want 1 (the job lock): recovery must not hold one across the pause", got)
	}
	if got := statuses(t, e.dst, e.dstSchema); got[int(tracking.StatusCompleted)] != 1 || got[int(tracking.StatusCopied)] != 2 {
		t.Errorf("log while the sentinel exists = %v, want 1 completed and 2 copied: recovery must wait before its next page", got)
	}
	if err := os.Remove(sentinel); err != nil {
		t.Fatal(err)
	}
	res := <-done
	if res.err != nil {
		t.Fatalf("Run() = %v", res.err)
	}
	if res.rep.Recovered != 3 || res.rep.Stopped {
		t.Errorf("report = %+v, want 3 recovered and no stop", res.rep)
	}
	if got := statuses(t, e.dst, e.dstSchema); got[int(tracking.StatusCompleted)] != 3 {
		t.Errorf("log after the sentinel is removed = %v, want 3 completed", got)
	}
}

// TestMySQLRecoveryWaitsAtStartup (M6-F1): a sentinel file already present when a run with
// logged work starts holds recovery before its first page; nothing is replayed until the
// file is removed.
func TestMySQLRecoveryWaitsAtStartup(t *testing.T) {
	e := newEnv(t, "recstart")
	e.both(t, orderTables("CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")...)
	seedOrders(t, e, 3)
	whole := e.config(t, job(t, jobspec.Archive, orderJob))
	whole.Processing.BatchSize = 3
	e.interrupted(t, whole, atCommitted) // three roots copied, none deleted

	sentinel := filepath.Join(t.TempDir(), "pause")
	if err := os.WriteFile(sentinel, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := e.config(t, job(t, jobspec.Archive, orderJob))
	cfg.Processing.SentinelFile = sentinel
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	go func() {
		_, err := Run(ctx, cfg)
		done <- err
	}()
	time.Sleep(2*sentinelPollInterval + 500*time.Millisecond)
	if got := statuses(t, e.dst, e.dstSchema); got[int(tracking.StatusCopied)] != 3 {
		t.Errorf("log while the sentinel exists = %v, want 3 copied: recovery must not start", got)
	}
	if err := os.Remove(sentinel); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if got := statuses(t, e.dst, e.dstSchema); got[int(tracking.StatusCompleted)] != 3 {
		t.Errorf("log after the sentinel is removed = %v, want 3 completed", got)
	}
}

// TestMySQLRecoveryStopsWhilePaused (M6-F1): a first signal while recovery waits at the
// sentinel ends the pause and stops the run, as it does at a batch boundary: nothing is
// replayed, the job is idle, and the logged roots stay for the next run.
func TestMySQLRecoveryStopsWhilePaused(t *testing.T) {
	e := newEnv(t, "recpstop")
	e.both(t, orderTables("CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")...)
	seedOrders(t, e, 3)
	whole := e.config(t, job(t, jobspec.Archive, orderJob))
	whole.Processing.BatchSize = 3
	e.interrupted(t, whole, atCommitted) // three roots copied, none deleted

	sentinel := filepath.Join(t.TempDir(), "pause")
	if err := os.WriteFile(sentinel, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	cfg := e.config(t, job(t, jobspec.Archive, orderJob))
	cfg.Processing.SentinelFile = sentinel
	cfg.Stop = stop
	time.AfterFunc(2*sentinelPollInterval, func() { close(stop) })
	rep := e.mustRun(t, cfg)
	if !rep.Stopped || rep.Recovered != 0 || rep.Batches != 0 {
		t.Errorf("report = %+v, want a stop with nothing recovered", rep)
	}
	if got := statuses(t, e.dst, e.dstSchema); got[int(tracking.StatusCopied)] != 3 {
		t.Errorf("log = %v, want the 3 copied roots left for the next run", got)
	}
	if got := count(t, e.dst, "SELECT job_status FROM archiver_job WHERE job_name = 'j'"); got != int64(tracking.JobIdle) {
		t.Errorf("job_status = %d, want idle", got)
	}
}

// TestMySQLSecondRunWhilePaused: a run paused at the sentinel keeps its job lock, so a
// second run of the same job (the next cron tick) is refused at startup instead of waiting
// at the same file; removing the file resumes only the first. Pair (R56): first run paused
// → the second is refused, "already running", and changes nothing; first run finished →
// the same second run starts and finds nothing to do.
func TestMySQLSecondRunWhilePaused(t *testing.T) {
	e := newEnv(t, "cron")
	e.both(t, orderTables("CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")...)
	seedOrders(t, e, 3)
	sentinel := filepath.Join(t.TempDir(), "pause")
	if err := os.WriteFile(sentinel, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	first := e.config(t, job(t, jobspec.Archive, orderJob))
	first.Processing.SentinelFile = sentinel
	type result struct {
		rep *Report
		err error
	}
	done := make(chan result, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	go func() {
		rep, err := Run(ctx, first)
		done <- result{rep, err}
	}()
	time.Sleep(2*sentinelPollInterval + 500*time.Millisecond) // the first run is paused before its first batch

	second := e.config(t, job(t, jobspec.Archive, orderJob))
	second.Processing.SentinelFile = sentinel
	if _, err := e.run(t, second); err == nil || !strings.Contains(err.Error(), `job "j" is already running`) {
		t.Fatalf("second run while the first is paused = %v, want it refused as already running", err)
	}
	if got := count(t, e.src, "SELECT COUNT(*) FROM orders"); got != 4 {
		t.Errorf("source orders after the refused run = %d, want 4", got)
	}

	if err := os.Remove(sentinel); err != nil {
		t.Fatal(err)
	}
	res := <-done
	if res.err != nil || res.rep.Roots != 3 {
		t.Fatalf("first run = %+v, %v, want the 3 old orders archived", res.rep, res.err)
	}
	if rep := e.mustRun(t, second); rep.Roots != 0 || rep.Recovered != 0 {
		t.Errorf("second run after the first finished = %+v, want nothing to do", rep)
	}
}

// TestMySQLSmallDestinationPool (M5-F6): startup holds the root-table and job locks, each on
// its own destination connection, while it reads the registry on a third. A pool limited
// below three is refused before any lock, naming the setting, instead of waiting forever.
// Three is enough for a whole run, recovery included.
func TestMySQLSmallDestinationPool(t *testing.T) {
	e := newEnv(t, "pool")
	e.both(t, orderTables("CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")...)
	seedOrders(t, e, 3)
	e.interrupted(t, e.config(t, job(t, jobspec.Archive, orderJob)), atCommitted)

	small := openServer(t, "TEST_DEST", e.dstSchema, nil)
	small.SetMaxOpenConns(2)
	cfg := e.config(t, job(t, jobspec.Archive, orderJob))
	cfg.Destination = small
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err := Run(ctx, cfg)
	if err == nil || !strings.Contains(err.Error(), "destination.max_connections") {
		t.Fatalf("Run() with a 2-connection destination pool = %v, want a refusal naming destination.max_connections", err)
	}
	if got := count(t, e.dst, "SELECT job_status FROM archiver_job WHERE job_name = 'j'"); got != int64(tracking.JobFailed) {
		t.Errorf("job_status = %d, want the interrupted run's failed status untouched", got)
	}

	enough := openServer(t, "TEST_DEST", e.dstSchema, nil)
	enough.SetMaxOpenConns(3)
	cfg.Destination = enough
	ctx3, cancel3 := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel3()
	rep, err := Run(ctx3, cfg)
	if err != nil || rep.Recovered != 2 || rep.Roots != 1 {
		t.Fatalf("Run() with a 3-connection pool = %+v, %v, want 2 recovered and 1 archived", rep, err)
	}
}
