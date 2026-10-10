//go:build integration

package engine

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
)

// The SIGKILL tests of spec §10: a child process runs the engine and stops dead at a crash
// point; the parent kills it with SIGKILL, so nothing of the child's cleanup runs, and then
// runs the job again in-process. The job's advisory lock dies with the child's session.

// TestEngineHelperProcess is the child. It runs only under GA_ENGINE_HELPER.
func TestEngineHelperProcess(t *testing.T) {
	if os.Getenv("GA_ENGINE_HELPER") == "" {
		t.Skip("helper process")
	}
	e := env{
		srcSchema: os.Getenv("GA_SRC_SCHEMA"), dstSchema: os.Getenv("GA_DST_SCHEMA"),
	}
	e.src = openServer(t, "TEST_SOURCE", e.srcSchema, nil)
	e.dst = openServer(t, "TEST_DEST", e.dstSchema, nil)
	cfg := e.config(t, job(t, jobspec.Type(os.Getenv("GA_TYPE")), os.Getenv("GA_JOB")))
	stopAt, nth := point(os.Getenv("GA_KILL_AT")), atoi(os.Getenv("GA_KILL_NTH"))
	seen := 0
	cfg.at = func(p point) {
		if p != stopAt {
			return
		}
		if seen++; seen == nth {
			fmt.Println("REACHED")
			select {} // wait for SIGKILL
		}
	}
	_, err := Run(context.Background(), cfg)
	fmt.Printf("FINISHED %v\n", err)
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

// killAt runs typ in a child process until the nth time it reaches p, then kills it.
func (e env) killAt(t *testing.T, typ jobspec.Type, body string, p point, nth int) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestEngineHelperProcess$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(), "GA_ENGINE_HELPER=1", "GA_SRC_SCHEMA="+e.srcSchema, "GA_DST_SCHEMA="+e.dstSchema,
		"GA_TYPE="+string(typ), "GA_JOB="+body, "GA_KILL_AT="+string(p), "GA_KILL_NTH="+strconv.Itoa(nth))
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	reached := make(chan string, 1)
	go func() {
		var log []string
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			log = append(log, sc.Text())
			if strings.Contains(sc.Text(), "REACHED") {
				reached <- ""
				return
			}
		}
		reached <- strings.Join(log, "\n")
	}()
	select {
	case log := <-reached:
		if log != "" {
			_ = cmd.Wait()
			t.Fatalf("the child never reached %s %d:\n%s", p, nth, log)
		}
	case <-time.After(60 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("the child did not reach %s %d in time", p, nth)
	}
	if err := cmd.Process.Kill(); err != nil { // SIGKILL
		t.Fatal(err)
	}
	_ = cmd.Wait()
}

// countingGate is a lag gate that counts its waits.
type countingGate struct{ waits atomic.Int64 }

func (g *countingGate) WaitForLag(context.Context) error { g.waits.Add(1); return nil }

// TestMySQLKillRecovery: each crash point of each type, then a restart. Archive and purge
// finish the logged roots by delete-only replay, each replay chunk behind the lag gate,
// and the result equals an uninterrupted run. Copy-only restarted with resume: false
// refuses the duplicate and suggests resume: true; with resume: true it finishes.
func TestMySQLKillRecovery(t *testing.T) {
	cs := "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
	for _, tc := range []struct {
		name  string
		typ   jobspec.Type
		at    point
		nth   int
		state map[int]int64 // the log after the kill
	}{
		{"archive after the destination commit", jobspec.Archive, atCommitted, 2, map[int]int64{1: 2, 2: 2}},
		{"archive mid-delete", jobspec.Archive, atDeleted, 3, map[int]int64{1: 2}}, // batch 1 has five delete chunks: notes 2, lines 2, orders 1
		// The fifth chunk of batch 1 deletes its two roots: they are gone from the source but
		// still logged copied, so the replay deletes only their (already deleted) trees.
		{"archive after its last delete chunk", jobspec.Archive, atDeleted, 5, map[int]int64{1: 2}},
		{"purge after deleting is logged", jobspec.Purge, atLogged, 1, map[int]int64{4: 2}},
		{"purge mid-delete", jobspec.Purge, atDeleted, 7, map[int]int64{2: 2, 4: 2}}, // the second chunk of batch 2
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, "kill")
			if tc.typ == jobspec.Purge {
				execAll(t, e.src, orderTables(cs)...)
			} else {
				e.both(t, orderTables(cs)...)
			}
			seedOrders(t, e, 5)
			old := oldRows(t, e)
			e.killAt(t, tc.typ, orderJob, tc.at, tc.nth)
			if got := statuses(t, e.dst, e.dstSchema); fmt.Sprint(got) != fmt.Sprint(tc.state) {
				t.Fatalf("log after the kill = %v, want %v", got, tc.state)
			}
			if got := count(t, e.dst, "SELECT job_status FROM archiver_job WHERE job_name = 'j'"); got != int64(tracking.JobRunning) {
				t.Errorf("job_status after SIGKILL = %d, want running (no cleanup ran)", got)
			}

			gate := &countingGate{}
			cfg := e.config(t, job(t, tc.typ, orderJob))
			cfg.Lag = gate
			rep := e.mustRun(t, cfg)
			if rep.Recovered != 2 || rep.Roots != 5-2-int64(tc.state[2]) {
				t.Errorf("report = %+v", rep)
			}
			if got := count(t, e.src, "SELECT COUNT(*) FROM orders"); got != 1 {
				t.Errorf("source orders = %d, want only the new one", got)
			}
			if tc.typ == jobspec.Archive {
				for table, order := range orderOrder {
					if got := rows(t, e.dst, table, order); !equal(got, old[table]) {
						t.Errorf("destination %s =\n%v\nwant\n%v", table, got, old[table])
					}
				}
			}
			if got := statuses(t, e.dst, e.dstSchema); fmt.Sprint(got) != "map[2:5]" {
				t.Errorf("log = %v, want 5 completed", got)
			}
			// Every source delete chunk, in recovery and in the loop, waited at the gate;
			// purge also waits once per batch before logging deleting.
			chunks := int64(0)
			for _, n := range rep.Deleted {
				chunks += (n + 1) / 2
			}
			if gate.waits.Load() < chunks {
				t.Errorf("lag gate waits = %d, want at least one per delete chunk (%d)", gate.waits.Load(), chunks)
			}
		})
	}

	t.Run("copy-only mid-run", func(t *testing.T) {
		e := newEnv(t, "killco")
		e.both(t, orderTables(cs)...)
		seedOrders(t, e, 5)
		old := oldRows(t, e)
		e.killAt(t, jobspec.CopyOnly, orderJob, atCommitted, 1)
		_, err := e.run(t, e.config(t, job(t, jobspec.CopyOnly, orderJob)))
		de := wantErr[*DuplicateError](t, err)
		if !de.CopyOnly || !strings.Contains(err.Error(), "resume: true") {
			t.Errorf("resume: false rerun: %v", err)
		}
		rep := e.mustRun(t, e.config(t, job(t, jobspec.CopyOnly, orderJob+"    resume: true\n")))
		if rep.Skipped != 2 || rep.Roots != 3 {
			t.Errorf("resume: true report = %+v, want 2 skipped, 3 copied", rep)
		}
		for table, order := range orderOrder {
			if got := rows(t, e.dst, table, order); !equal(got, old[table]) {
				t.Errorf("destination %s =\n%v\nwant\n%v", table, got, old[table])
			}
		}
	})
}
