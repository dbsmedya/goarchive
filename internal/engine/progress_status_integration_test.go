//go:build integration

package engine

import (
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// TestMySQLProgressForEveryRun (TEL-1 C6, spec §4.8, §7): the tracker the status reads exists for every
// run, without --progress: it is starting before the run, and as the run goes its activity, its
// counters (also per table) and its state word move, the counters only up, and at the end they equal the
// report's. No estimate is read without --progress.
func TestMySQLProgressForEveryRun(t *testing.T) {
	e := newEnv(t, "progevery")
	d16Tables(t, e, jobspec.Archive)
	seedOrders(t, e, 6)
	cfg := e.config(t, job(t, jobspec.Archive, orderJob))
	cfg.Processing = config.ProcessingConfig{BatchSize: 3, BatchDeleteSize: 100}
	tr := NewProgress(time.Now())
	cfg.Tracker = tr
	if st := tr.Status(); st.Phase != PhaseStarting || st.RowsCopied != 0 {
		t.Fatalf("before the run: %+v, want starting with nothing counted", st)
	}
	var seen []ProgressStatus
	var points []point
	cfg.at = func(p point) {
		seen = append(seen, tr.Status())
		points = append(points, p)
	}
	rep := e.mustRun(t, cfg)

	// The first batch's copy has committed and nothing was deleted yet.
	first := -1
	for i, p := range points {
		if p == atCommitted {
			first = i
			break
		}
	}
	if first < 0 {
		t.Fatal("the run never reached a committed copy")
	}
	if st := seen[first]; st.Phase != PhaseRunning || st.RowsCopied != 15 || st.RowsDeleted != 0 || st.Batches != 0 || st.Step != "copy" {
		t.Errorf("at the first committed copy: %+v, want running, 15 rows copied (3 orders, 6 lines, 6 notes), none deleted, no batch done, copying", st)
	}
	// A delete is under way: the step is delete, on a table, with rows deleted.
	var sawDelete bool
	for i, p := range points {
		if p == atDeleted && seen[i].Step == "delete" && seen[i].Table != "" && seen[i].RowsDeleted > 0 {
			sawDelete = true
		}
	}
	if !sawDelete {
		t.Error("no delete was seen with its table and a count of rows deleted")
	}
	// The counters never go down within a run.
	for i := 1; i < len(seen); i++ {
		a, b := seen[i-1], seen[i]
		if b.RowsCopied < a.RowsCopied || b.RowsDeleted < a.RowsDeleted || b.Roots < a.Roots || b.Batches < a.Batches {
			t.Errorf("a counter went down between %v and %v: %+v then %+v", points[i-1], points[i], a, b)
		}
	}

	st := tr.Status()
	var copied, deleted int64
	for _, n := range rep.Copied {
		copied += n
	}
	for _, n := range rep.Deleted {
		deleted += n
	}
	if st.Batches != int64(rep.Batches) || st.Roots != rep.Roots || st.RowsCopied != copied || st.RowsDeleted != deleted || st.Phase != PhaseRunning {
		t.Errorf("final status = %+v, want it equal to the report %+v", st, rep)
	}
	for _, tc := range st.Tables {
		if tc.Copied != rep.Copied[tc.Name] || tc.Deleted != rep.Deleted[tc.Name] {
			t.Errorf("table %s: status %+v, report copied %d deleted %d", tc.Name, tc, rep.Copied[tc.Name], rep.Deleted[tc.Name])
		}
	}
	if len(st.Tables) != 3 {
		t.Errorf("tables = %+v, want orders, order_lines and line_notes", st.Tables)
	}
	if st.Estimate != nil {
		t.Errorf("estimate = %+v without --progress, want none: the optimizer estimate stays a --progress cost", st.Estimate)
	}
}

// TestMySQLProgressDuringRecovery: a rerun that replays logged roots is recovering while it does, counts
// the roots it recovered and the rows it deleted, and is running afterwards.
func TestMySQLProgressDuringRecovery(t *testing.T) {
	e := newEnv(t, "progrec")
	d16Tables(t, e, jobspec.Archive)
	seedOrders(t, e, 3)
	first := e.config(t, job(t, jobspec.Archive, orderJob))
	first.Processing = config.ProcessingConfig{BatchSize: 3, BatchDeleteSize: 100}
	leaveLogged(t, e, first, 3)

	cfg := e.config(t, job(t, jobspec.Archive, orderJob))
	cfg.Processing = config.ProcessingConfig{BatchSize: 3, BatchDeleteSize: 100}
	tr := NewProgress(time.Now())
	cfg.Tracker = tr
	var during []ProgressStatus
	cfg.at = func(p point) {
		if p == atDeleted {
			during = append(during, tr.Status())
		}
	}
	rep := e.mustRun(t, cfg)
	if len(during) == 0 {
		t.Fatal("no delete was seen during recovery")
	}
	for _, st := range during {
		if st.Phase != PhaseRecovering || st.Step != "delete" {
			t.Errorf("during recovery's deletes: %+v, want recovering, on a delete", st)
		}
	}
	st := tr.Status()
	if st.RootsRecovered != 3 || rep.Recovered != 3 || st.Phase != PhaseRunning || st.RowsDeleted != 15 {
		t.Errorf("after the run: %+v (report recovered %d), want 3 roots recovered, 15 rows deleted, running", st, rep.Recovered)
	}
}
