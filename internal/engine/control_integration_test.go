//go:build integration

package engine

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// TEL-1 C4a: the engine's readers of the settings store, driven through the store directly (no
// socket yet). What C6 completes over the socket is spec's plan §4; none of it is asserted here.

var controlPeer = tuning.Peer{UID: 1000, PID: 4242}

// controlStore is the store a run of cfg reads, as the command will build it: it starts at the
// job's processing values, which are the ceilings.
func controlStore(cfg Config, gate bool, opts ...func(*tuning.Options)) *tuning.Store {
	p := cfg.Processing
	o := tuning.Options{
		Start: tuning.Settings{
			BatchSize: p.BatchSize, BatchDeleteSize: p.BatchDeleteSize,
			SleepSeconds: p.SleepSeconds, DeleteSleepSeconds: p.DeleteSleepSeconds,
			SecondsBehindSourceWithin: 30, CheckInterval: 1,
		},
		Applies: tuning.Applicability{Delete: cfg.Spec.Type != jobspec.CopyOnly, Gate: gate && cfg.Spec.Type != jobspec.CopyOnly},
	}
	for _, f := range opts {
		f(&o)
	}
	return tuning.New(o)
}

func setStore(t *testing.T, s *tuning.Store, k tuning.Key, text string) {
	t.Helper()
	if _, err := s.Set(k, text, controlPeer); err != nil {
		t.Fatalf("Set(%s, %s) = %v", k, text, err)
	}
}

// seam records the key-addressed statements of a run through Config.fault, which it does not
// change, and counts a batch's commit (Config.at), so a statement is told by the batch it ran in:
// a batch's reads and discovery all happen before its copy commits.
type seam struct {
	mu      sync.Mutex
	batches int
	seen    []seenStatement
}

type seenStatement struct {
	batch  int
	op     op
	table  string
	tuples int
}

var (
	keyWidth    = map[string]int{"orders": 1, "order_lines": 2, "line_notes": 1}
	parentWidth = map[string]int{"order_lines": 1, "line_notes": 2}
)

func (s *seam) observe(st *statement) {
	w := keyWidth[st.table]
	if st.op == opDiscover {
		w = parentWidth[st.table]
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen = append(s.seen, seenStatement{s.batches, st.op, st.table, len(st.args) / w})
}

func (s *seam) committed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches++
}

// tuples returns the tuple counts of the statements of op on table, in order, within batch (a
// negative batch means every batch).
func (s *seam) tuples(o op, table string, batch int) []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []int
	for _, st := range s.seen {
		if st.op == o && st.table == table && (batch < 0 || st.batch == batch) {
			out = append(out, st.tuples)
		}
	}
	return out
}

func sum(ns []int) (total int) {
	for _, n := range ns {
		total += n
	}
	return total
}

func maxOf(ns []int) (m int) {
	for _, n := range ns {
		m = max(m, n)
	}
	return m
}

// leaveLogged runs archive for cfg in this process and cancels it right after its first batch's
// copy committed, before any delete: the batch's roots stay logged copied, as a crash there
// leaves them (killAt does that with a child process; this leaves a batch of a size the child
// cannot be given).
func leaveLogged(t *testing.T, e env, cfg Config, roots int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg.at = func(p point) {
		if p == atCommitted {
			cancel()
		}
	}
	if _, err := e.runContext(t, ctx, cfg); err == nil {
		t.Fatal("the run that was to be cancelled after its copy finished without an error")
	}
	if got := fmt.Sprint(statuses(t, e.dst, e.dstSchema)); got != fmt.Sprintf("map[1:%d]", roots) {
		t.Fatalf("log after the cancelled run = %s, want %d roots copied and not deleted", got, roots)
	}
}

// TestMySQLControlBatchSize (spec §7): batch_size is read once per batch and that value is the
// fetch limit, the end test and the chunk size of every read and discovery statement of the batch.
// Lower it to 2 and restore it to 4 while a batch that fetched 2 roots is in flight: the run does
// not end early (today's code would read `2 < 4` at the end test and stop with rows left in the
// range) and every root is archived. The statement seam shows each batch's statements at that
// batch's size.
func TestMySQLControlBatchSize(t *testing.T) {
	e := newEnv(t, "ctlsize")
	d16Tables(t, e, jobspec.Archive)
	seedOrders(t, e, 6)
	cfg := e.config(t, job(t, jobspec.Archive, orderJob))
	cfg.Processing = config.ProcessingConfig{BatchSize: 4, BatchDeleteSize: 4}
	store := controlStore(cfg, false)
	setStore(t, store, tuning.BatchSize, "2") // lowered before the first fetch
	cfg.Tuning = store
	rec := &seam{}
	cfg.fault = rec.observe
	first := true
	cfg.at = func(p point) {
		if p != atCommitted {
			return
		}
		rec.committed()
		if first {
			first = false
			setStore(t, store, tuning.BatchSize, "4") // restored while batch 1 (2 roots) is in flight
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rep, err := e.runContext(t, ctx, cfg)
	if err != nil || rep == nil || rep.Stopped || rep.Roots != 6 || rep.Batches != 2 {
		t.Fatalf("Run() = %+v, %v, want 2 batches (2 roots, then 4) and every root archived", rep, err)
	}
	if got := count(t, e.src, "SELECT COUNT(*) FROM orders WHERE placed < '2020-01-01'"); got != 0 {
		t.Errorf("old orders left in the source = %d, want 0", got)
	}
	if got := fmt.Sprint(statuses(t, e.dst, e.dstSchema)); got != "map[2:6]" {
		t.Errorf("log = %s, want 6 completed", got)
	}
	for batch, want := range []int{2, 4} {
		reads := rec.tuples(opRead, "orders", batch)
		if sum(reads) != want || maxOf(reads) > want {
			t.Errorf("batch %d read its %d roots in statements of %v, want %d in all, none above the batch's size", batch+1, want, reads, want)
		}
		if disc := rec.tuples(opDiscover, "order_lines", batch); maxOf(disc) > want || sum(disc) != want {
			t.Errorf("batch %d discovery on order_lines used %v parent tuples, want %d in all, none above the batch's size", batch+1, disc, want)
		}
	}
}

// TestMySQLControlRecoveryPages: recovery reads batch_size once per page, the same way. An
// interrupted archive left six logged roots; the rerun starts at 4, is lowered to 2 before the
// first page and restored to 4 while that page is in flight. Page 1 is fetched at 2 and its end
// test uses 2 (today's `2 < 4` would end recovery with four roots pending); page 2 is fetched at 4;
// every logged root is replayed.
func TestMySQLControlRecoveryPages(t *testing.T) {
	e := newEnv(t, "ctlpages")
	d16Tables(t, e, jobspec.Archive)
	seedOrders(t, e, 6)
	first := e.config(t, job(t, jobspec.Archive, orderJob))
	first.Processing = config.ProcessingConfig{BatchSize: 6, BatchDeleteSize: 6}
	leaveLogged(t, e, first, 6)

	cfg := e.config(t, job(t, jobspec.Archive, orderJob))
	cfg.Processing = config.ProcessingConfig{BatchSize: 4, BatchDeleteSize: 4}
	store := controlStore(cfg, false)
	setStore(t, store, tuning.BatchSize, "2")
	cfg.Tuning = store
	rec := &seam{}
	cfg.fault = rec.observe
	restored := false
	cfg.at = func(p point) {
		if p == atDeleted && !restored {
			restored = true
			setStore(t, store, tuning.BatchSize, "4") // while page 1 is in flight
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rep, err := e.runContext(t, ctx, cfg)
	if err != nil || rep == nil || rep.Recovered != 6 || rep.Roots != 0 {
		t.Fatalf("Run() = %+v, %v, want the 6 logged roots recovered and nothing more", rep, err)
	}
	if got := fmt.Sprint(statuses(t, e.dst, e.dstSchema)); got != "map[2:6]" {
		t.Errorf("log = %s, want 6 completed and none pending", got)
	}
	if got := count(t, e.src, "SELECT COUNT(*) FROM orders WHERE placed < '2020-01-01'"); got != 0 {
		t.Errorf("old orders left in the source = %d, want 0", got)
	}
	if pages := rec.tuples(opPresent, "orders", -1); !slices.Equal(pages, []int{2, 4}) {
		t.Errorf("recovery's root reads were %v, want a page of 2 then a page of 4", pages)
	}
}

// heldRun starts cfg on the estate with the replica's applier stopped, so that the first DELETE
// of the run (the batch's or recovery's) holds at the gate, waits three check intervals, applies
// lower to the store during the hold, and starts the applier: the run then finishes.
func heldRun(t *testing.T, e env, cfg Config, store *tuning.Store, lower func()) (*Report, error) {
	t.Helper()
	gate, exec := liveGate(t, 30)
	exec("STOP REPLICA SQL_THREAD")
	cfg.Lag, cfg.Tuning = gate, store
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	type result struct {
		rep *Report
		err error
	}
	done := make(chan result, 1)
	go func() {
		rep, err := e.runContext(t, ctx, cfg)
		done <- result{rep, err}
	}()
	select {
	case r := <-done:
		t.Fatalf("Run() = %+v, %v before the applier started, want it held at the gate", r.rep, r.err)
	case <-time.After(3 * time.Second): // three check intervals
	}
	lower()
	exec("START REPLICA SQL_THREAD")
	r := <-done
	return r.rep, r.err
}

// deletesAreAll checks every DELETE of the run carried n keys, and that their number and the
// report's counts are exact: three old orders, six lines, six notes.
func deletesAreAll(t *testing.T, e env, rec *seam, rep *Report, n int) {
	t.Helper()
	for _, table := range []string{"line_notes", "order_lines", "orders"} {
		sizes := rec.tuples(opDelete, table, -1)
		for _, s := range sizes {
			if s != n {
				t.Errorf("a DELETE on %s carried %d keys, want %d: %v", table, s, n, sizes)
				break
			}
		}
	}
	if got := fmt.Sprint(rep.Deleted); got != "map[line_notes:6 order_lines:6 orders:3]" {
		t.Errorf("deleted = %s, want 3 orders, 6 lines and 6 notes, each once", got)
	}
	if got := count(t, e.src, "SELECT COUNT(*) FROM orders WHERE placed < '2020-01-01'"); got != 0 {
		t.Errorf("old orders left in the source = %d, want 0", got)
	}
	if got := fmt.Sprint(statuses(t, e.dst, e.dstSchema)); got != "map[2:3]" {
		t.Errorf("log = %s, want 3 completed", got)
	}
}

// TestMySQLControlDeleteSizeDuringHold: with the replica's applier stopped a batch holds before
// its first DELETE; batch_delete_size is lowered during the hold; the applier is started. The very
// first DELETE after the release carries the new size (a chunk built at 4 before the hold would
// have been sent at 4), every row is deleted and the counts are exact.
func TestMySQLControlDeleteSizeDuringHold(t *testing.T) {
	e := newEnv(t, "ctlholdsize")
	d16Tables(t, e, jobspec.Archive)
	seedOrders(t, e, 3)
	cfg := e.config(t, job(t, jobspec.Archive, orderJob))
	cfg.Processing = config.ProcessingConfig{BatchSize: 3, BatchDeleteSize: 4}
	store := controlStore(cfg, true)
	rec := &seam{}
	cfg.fault = rec.observe
	rep, err := heldRun(t, e, cfg, store, func() { setStore(t, store, tuning.BatchDeleteSize, "1") })
	if err != nil || rep == nil {
		t.Fatalf("Run() = %+v, %v", rep, err)
	}
	deletesAreAll(t, e, rec, rep, 1)
	if first := rec.tuples(opDelete, "line_notes", -1); len(first) != 6 {
		t.Errorf("the notes were deleted in %d statements, want 6 of one key", len(first))
	}
}

// TestMySQLControlDeleteSizeRecovery is the same during recovery's deletes: an interrupted
// archive left three logged roots; the rerun holds before the first DELETE of the replay.
func TestMySQLControlDeleteSizeRecovery(t *testing.T) {
	e := newEnv(t, "ctlholdrec")
	d16Tables(t, e, jobspec.Archive)
	seedOrders(t, e, 3)
	first := e.config(t, job(t, jobspec.Archive, orderJob))
	first.Processing = config.ProcessingConfig{BatchSize: 3, BatchDeleteSize: 4}
	leaveLogged(t, e, first, 3)

	cfg := e.config(t, job(t, jobspec.Archive, orderJob))
	cfg.Processing = config.ProcessingConfig{BatchSize: 3, BatchDeleteSize: 4}
	store := controlStore(cfg, true)
	rec := &seam{}
	cfg.fault = rec.observe
	rep, err := heldRun(t, e, cfg, store, func() { setStore(t, store, tuning.BatchDeleteSize, "1") })
	if err != nil || rep == nil || rep.Recovered != 3 {
		t.Fatalf("Run() = %+v, %v, want the 3 logged roots recovered", rep, err)
	}
	deletesAreAll(t, e, rec, rep, 1)
}

// TestMySQLControlDeleteSizeMidTable: a lower size sent between two DELETEs of one table applies
// to the next one, and the key offset is exact: no key deleted twice, none skipped (a DELETE whose
// rows affected differ from its keys is refused, and the counts must add up).
func TestMySQLControlDeleteSizeMidTable(t *testing.T) {
	e := newEnv(t, "ctlmid")
	d16Tables(t, e, jobspec.Archive)
	seedOrders(t, e, 3)
	cfg := e.config(t, job(t, jobspec.Archive, orderJob))
	cfg.Processing = config.ProcessingConfig{BatchSize: 3, BatchDeleteSize: 4}
	store := controlStore(cfg, false)
	cfg.Tuning = store
	rec := &seam{}
	cfg.fault = rec.observe
	lowered := false
	cfg.at = func(p point) {
		if p == atDeleted && !lowered {
			lowered = true
			setStore(t, store, tuning.BatchDeleteSize, "1") // between the first and second DELETE of line_notes
		}
	}
	rep := e.mustRun(t, cfg)
	if got := rec.tuples(opDelete, "line_notes", -1); !slices.Equal(got, []int{4, 1, 1}) {
		t.Errorf("the notes were deleted in chunks of %v, want [4 1 1]: the first at the old size, the rest at the new", got)
	}
	for _, table := range []string{"order_lines", "orders"} {
		for _, n := range rec.tuples(opDelete, table, -1) {
			if n != 1 {
				t.Errorf("a DELETE on %s carried %d keys, want 1", table, n)
			}
		}
	}
	if got := fmt.Sprint(rep.Deleted); got != "map[line_notes:6 order_lines:6 orders:3]" {
		t.Errorf("deleted = %s, want 3 orders, 6 lines and 6 notes, each once", got)
	}
	if got := count(t, e.src, "SELECT COUNT(*) FROM orders WHERE placed < '2020-01-01'"); got != 0 {
		t.Errorf("old orders left = %d, want 0", got)
	}
	if got := fmt.Sprint(statuses(t, e.dst, e.dstSchema)); got != "map[2:3]" {
		t.Errorf("log = %s, want 3 completed", got)
	}
}

// TestMySQLControlRaiseRefused: a size above the starting value is refused, the run's statements
// keep their sizes, and nothing refused is in the summary. (That the refusal is logged is the
// socket's, C6.)
func TestMySQLControlRaiseRefused(t *testing.T) {
	e := newEnv(t, "ctlraise")
	d16Tables(t, e, jobspec.Archive)
	seedOrders(t, e, 6)
	cfg := e.config(t, job(t, jobspec.Archive, orderJob))
	cfg.Processing = config.ProcessingConfig{BatchSize: 4, BatchDeleteSize: 4}
	store := controlStore(cfg, false)
	cfg.Tuning = store
	rec := &seam{}
	cfg.fault = rec.observe
	var refused []tuning.Reason
	tried := false
	cfg.at = func(p point) {
		rec.committedIf(p)
		if p != atCommitted || tried {
			return
		}
		tried = true
		for _, try := range []struct {
			k    tuning.Key
			text string
		}{{tuning.BatchSize, "5000"}, {tuning.BatchDeleteSize, "5"}} {
			_, err := store.Set(try.k, try.text, controlPeer)
			r, ok := err.(*tuning.Refusal)
			if !ok {
				t.Fatalf("Set(%s, %s) = %v, want a refusal", try.k, try.text, err)
			}
			refused = append(refused, r.Reason)
		}
	}
	rep := e.mustRun(t, cfg)
	if !slices.Equal(refused, []tuning.Reason{tuning.ReasonCeiling, tuning.ReasonCeiling}) {
		t.Errorf("refusals = %v, want two ceilings", refused)
	}
	if rep.Roots != 6 || rep.Batches != 2 {
		t.Errorf("report = %+v, want batches of 4 and 2 roots", rep)
	}
	for _, table := range []string{"orders", "order_lines", "line_notes"} {
		for _, o := range []op{opRead, opDelete} {
			if m := maxOf(rec.tuples(o, table, -1)); m > 4 {
				t.Errorf("a %s on %s carried %d keys, above the starting 4", o, table, m)
			}
		}
	}
	if sum := store.Terminal(); len(sum.Settings) != 0 || sum.Pauses != 0 {
		t.Errorf("summary = %+v, want empty: a refusal is not a change", sum)
	}
	if got := store.Load().Revision; got != 0 {
		t.Errorf("revision = %d, want 0", got)
	}
}

// committedIf counts a batch commit seen at point p (for a test that also does its own work at
// the point).
func (s *seam) committedIf(p point) {
	if p == atCommitted {
		s.committed()
	}
}

// TestMySQLControlCopyOnly (engine half): on a copy-only run batch_size and sleep_seconds work.
// Lowered to 2 and slept 0.3 s before the first batch, five roots run as batches of 2, 2 and 1, with
// two sleeps. (The delete and gate setters' refusal over the socket is C6's.)
func TestMySQLControlCopyOnly(t *testing.T) {
	e := newEnv(t, "ctlcopy")
	d16Tables(t, e, jobspec.CopyOnly)
	seedOrders(t, e, 5)
	cfg := e.config(t, job(t, jobspec.CopyOnly, orderJob))
	cfg.Processing = config.ProcessingConfig{BatchSize: 4, BatchDeleteSize: 4}
	store := controlStore(cfg, false)
	setStore(t, store, tuning.BatchSize, "2")
	setStore(t, store, tuning.SleepSeconds, "0.3")
	cfg.Tuning = store
	rec := &seam{}
	cfg.fault = rec.observe
	cfg.at = rec.committedIf
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	began := time.Now()
	rep, err := e.runContext(t, ctx, cfg)
	took := time.Since(began)
	if err != nil || rep == nil || rep.Roots != 5 || rep.Batches != 3 {
		t.Fatalf("Run() = %+v, %v, want 3 batches of 2, 2 and 1 roots", rep, err)
	}
	if took < 550*time.Millisecond {
		t.Errorf("Run() took %v: the two sleeps of 0.3 s between the full batches did not happen", took)
	}
	for batch, want := range []int{2, 2, 1} {
		if got := sum(rec.tuples(opRead, "orders", batch)); got != want {
			t.Errorf("batch %d read %d roots, want %d", batch+1, got, want)
		}
	}
	if got := count(t, e.dst, "SELECT COUNT(*) FROM orders WHERE placed < '2020-01-01'"); got != 5 {
		t.Errorf("the destination holds %d old orders, want 5", got)
	}
}
