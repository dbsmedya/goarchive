//go:build integration

package engine

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/lock"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
)

// The plan's Review Focus: conditions an operator meets that the spec implies but the
// phase and binding tests do not reach.

// TestMySQLGracefulStop (Review Focus 1): the first signal during a batch lets that batch
// finish, ends the run at the boundary with the job idle, and a rerun goes on from where the
// where range still matches.
func TestMySQLGracefulStop(t *testing.T) {
	e := newEnv(t, "stop")
	e.both(t, orderTables("CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")...)
	seedOrders(t, e, 5)
	stop := make(chan struct{})
	cfg := e.config(t, job(t, jobspec.Archive, orderJob))
	cfg.Stop = stop
	cfg.at = func(p point) {
		if p == atCommitted {
			select {
			case <-stop:
			default:
				close(stop)
			}
		}
	}
	rep := e.mustRun(t, cfg)
	if !rep.Stopped || rep.Batches != 1 || rep.Roots != 2 {
		t.Fatalf("report = %+v, want one finished batch, then a stop", rep)
	}
	if got := statuses(t, e.dst, e.dstSchema); got[int(tracking.StatusCompleted)] != 2 || len(got) != 1 {
		t.Errorf("log = %v, want the batch completed", got)
	}
	if got := count(t, e.dst, "SELECT job_status FROM archiver_job WHERE job_name = 'j'"); got != int64(tracking.JobIdle) {
		t.Errorf("job_status = %d, want idle", got)
	}
	rep = e.mustRun(t, e.config(t, job(t, jobspec.Archive, orderJob)))
	if rep.Roots != 3 || count(t, e.src, "SELECT COUNT(*) FROM orders") != 1 {
		t.Errorf("rerun = %+v", rep)
	}
}

// TestMySQLLostJobLock (Review Focus 2): another session kills the connection holding the
// job's advisory lock. The keep-alive notices and cancels the run, which fails without
// starting another batch; what it logged is recovered by the next run.
func TestMySQLLostJobLock(t *testing.T) {
	saved := keepAliveEvery
	keepAliveEvery = 100 * time.Millisecond
	t.Cleanup(func() { keepAliveEvery = saved })
	e := newEnv(t, "lock")
	e.both(t, orderTables("CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")...)
	seedOrders(t, e, 5)
	admin := openServer(t, "TEST_DEST", "", nil)
	cfg := e.config(t, job(t, jobspec.Archive, orderJob))
	cfg.at = func(p point) {
		if p != atCommitted {
			return
		}
		holder, err := lockHolder(context.Background(), admin, lock.JobLockName(e.dstSchema, "j"))
		if err != nil {
			t.Error(err)
			return
		}
		execAll(t, admin, "KILL "+itoa(holder))
		time.Sleep(500 * time.Millisecond) // past several keep-alive checks
	}
	_, err := e.run(t, cfg)
	if err == nil || !strings.Contains(err.Error(), "job lock was lost") {
		t.Fatalf("Run() = %v, want the lost lock", err)
	}
	if got := count(t, e.dst, "SELECT job_status FROM archiver_job WHERE job_name = 'j'"); got != int64(tracking.JobFailed) {
		t.Errorf("job_status = %d, want failed", got)
	}
	rep := e.mustRun(t, e.config(t, job(t, jobspec.Archive, orderJob)))
	if count(t, e.src, "SELECT COUNT(*) FROM orders") != 1 || rep.Recovered+rep.Roots != 5 {
		t.Errorf("rerun = %+v", rep)
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

// TestMySQLForeignKeyChecksReset (Review Focus 3): safety.disable_foreign_key_checks turns
// the checks off for the copy's session only. The session is reset before it returns to the
// pool, so when the batch has committed, every idle destination session checks foreign keys.
func TestMySQLForeignKeyChecksReset(t *testing.T) {
	e := newEnv(t, "fkreset")
	e.both(t, orderTables("CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")...)
	seedOrders(t, e, 1)
	cfg := e.config(t, job(t, jobspec.CopyOnly, orderJob))
	cfg.DisableForeignKeyChecks = true
	checked := 0
	cfg.at = func(p point) {
		if p != atCommitted {
			return
		}
		ctx := context.Background()
		idle := e.dst.Stats().Idle
		var held []*sql.Conn
		defer func() {
			for _, c := range held {
				_ = c.Close()
			}
		}()
		for range idle {
			c, err := e.dst.Conn(ctx) // idle sessions are handed out first
			if err != nil {
				t.Error(err)
				return
			}
			held = append(held, c)
			var on int
			if err := c.QueryRowContext(ctx, "SELECT @@SESSION.foreign_key_checks").Scan(&on); err != nil {
				t.Error(err)
				return
			}
			if on != 1 {
				t.Errorf("an idle destination session has foreign_key_checks = %d", on)
			}
			checked++
		}
	}
	e.mustRun(t, cfg)
	if checked == 0 {
		t.Error("no idle destination session was checked")
	}
}

// failingGate refuses every wait, as a gate does while a replica lags past its limit and
// the context ends.
type failingGate struct{}

func (failingGate) WaitForLag(context.Context, <-chan struct{}) error {
	return errors.New("replica lag 900s exceeds 10s")
}

// TestMySQLRecoveryWaitsAtTheGate (Review Focus 4): recovery replays no delete while the lag
// gate refuses; the logged roots stay logged for the next run.
func TestMySQLRecoveryWaitsAtTheGate(t *testing.T) {
	e := newEnv(t, "gate")
	e.both(t, orderTables("CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")...)
	seedOrders(t, e, 3)
	e.interrupted(t, e.config(t, job(t, jobspec.Archive, orderJob)), atCommitted)
	before := rows(t, e.src, "line_notes", "note_id")
	cfg := e.config(t, job(t, jobspec.Archive, orderJob))
	cfg.Lag = failingGate{}
	if _, err := e.run(t, cfg); err == nil || !strings.Contains(err.Error(), "replication lag gate") {
		t.Fatalf("Run() = %v, want the lag gate's refusal", err)
	}
	if got := rows(t, e.src, "line_notes", "note_id"); !equal(got, before) {
		t.Error("recovery deleted past a refusing gate")
	}
	if got := statuses(t, e.dst, e.dstSchema); got[int(tracking.StatusCopied)] != 2 {
		t.Errorf("log = %v, want the 2 roots still copied", got)
	}
}

// TestMySQLContractViolation (Review Focus 5, spec §7.1a): a child row deleted by someone else
// between the copy and the delete breaks the cold-data contract. The delete chunk that meets
// it reaches fewer rows than its keys, rolls back and fails the run; the restart re-discovers
// what is left and finishes.
func TestMySQLContractViolation(t *testing.T) {
	e := newEnv(t, "contract")
	e.both(t, orderTables("CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")...)
	seedOrders(t, e, 2)
	cfg := e.config(t, job(t, jobspec.Archive, orderJob))
	cfg.at = func(p point) {
		if p == atCommitted {
			execAll(t, e.src, "DELETE FROM line_notes WHERE note_id = 1")
		}
	}
	_, err := e.run(t, cfg)
	if ke := wantErr[*KeySetError](t, err); ke.Statement != string(opDelete) || ke.Table != "line_notes" || ke.Matched != 1 {
		t.Fatalf("refusal = %+v", ke)
	}
	if got := count(t, e.src, "SELECT COUNT(*) FROM line_notes"); got != 5 {
		t.Errorf("source notes = %d, want 5: six seeded, one deleted outside, the chunk rolled back", got)
	}
	rep := e.mustRun(t, e.config(t, job(t, jobspec.Archive, orderJob)))
	if rep.Recovered != 2 || count(t, e.src, "SELECT COUNT(*) FROM line_notes") != 2 {
		t.Errorf("restart = %+v, source notes %d", rep, count(t, e.src, "SELECT COUNT(*) FROM line_notes"))
	}
}
