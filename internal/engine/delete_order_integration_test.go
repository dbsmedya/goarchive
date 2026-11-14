//go:build integration

package engine

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// D16 (TEL-1 C3, spec §4.5, §9 item 8): for each DELETE the inter-delete sleep comes first and
// the replication gate's decision last, immediately before the statement. Before the change the
// gate came first and the sleep after it, so a gate that passed a second before the DELETE
// judged the replicas by a moment that was no longer current.

// timeline records the gate's waits and the DELETE statements of a run, each with the time it
// happened, from one clock. It stands in for Config.Lag (a recording wrapper: it passes every
// wait at once) and observes statements through Config.fault without changing them. Both are
// test code, not faults.
type timeline struct {
	mu     sync.Mutex
	events []timelineEvent
}

type timelineEvent struct {
	kind  string // "gate" or "delete"
	table string // the DELETE's table
	at    time.Time
}

func (tl *timeline) add(kind, table string) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	tl.events = append(tl.events, timelineEvent{kind, table, time.Now()})
}

func (tl *timeline) WaitForLag(context.Context, <-chan struct{}) error {
	tl.add("gate", "")
	return nil
}

func (tl *timeline) observe(st *statement) {
	if st.op == opDelete {
		tl.add("delete", st.table)
	}
}

// d16Tables creates the order tables where the job type needs them.
func d16Tables(t *testing.T, e env, typ jobspec.Type) {
	t.Helper()
	cs := "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
	if typ == jobspec.Purge {
		execAll(t, e.src, orderTables(cs)...)
		return
	}
	e.both(t, orderTables(cs)...)
}

// TestMySQLDeleteSleepThenGate (spec §7): between two DELETEs of a batch the sleep runs, and
// then the gate is asked, and then the statement is issued; the first DELETE of a batch has no
// sleep before it. Three old orders in batches of two roots and chunks of two keys make eight
// DELETEs: batch 1 has five (two chunks of notes, two of lines, one of orders), batch 2 three.
// The same order holds in recovery, which shares the path, and for purge, whose pre-log gate
// stays. The run's rows and counts equal those of the same job before the change.
func TestMySQLDeleteSleepThenGate(t *testing.T) {
	const sleep = 300 * time.Millisecond
	for _, tc := range []struct {
		name  string
		typ   jobspec.Type
		crash point // "" for none; else a first run is killed here, leaving batch 1 for recovery
	}{
		{"archive", jobspec.Archive, ""},
		{"purge", jobspec.Purge, ""},
		{"archive recovery", jobspec.Archive, atCommitted},
		{"purge recovery", jobspec.Purge, atLogged},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, "d16order")
			d16Tables(t, e, tc.typ)
			seedOrders(t, e, 3)
			if tc.crash != "" {
				e.killAt(t, tc.typ, orderJob, tc.crash, 1)
			}
			tl := &timeline{}
			cfg := e.config(t, job(t, tc.typ, orderJob))
			cfg.Processing.DeleteSleepSeconds = sleep.Seconds()
			cfg.Lag, cfg.fault = tl, tl.observe
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			rep, err := e.runContext(t, ctx, cfg)
			if err != nil {
				t.Fatalf("Run() = %v", err)
			}

			// The job's result is what it was before the change.
			wantRecovered := int64(0)
			if tc.crash != "" {
				wantRecovered = 2
			}
			if rep.Recovered != wantRecovered || rep.Stopped {
				t.Errorf("report = %+v, want %d recovered and not stopped", rep, wantRecovered)
			}
			if got := fmt.Sprint(rep.Deleted); got != "map[line_notes:6 order_lines:6 orders:3]" {
				t.Errorf("deleted = %s, want 3 orders, 6 lines and 6 notes", got)
			}
			if got := count(t, e.src, "SELECT COUNT(*) FROM orders WHERE placed < '2020-01-01'"); got != 0 {
				t.Errorf("old orders left in the source = %d, want 0", got)
			}
			if got := fmt.Sprint(statuses(t, e.dst, e.dstSchema)); got != "map[2:3]" {
				t.Errorf("log = %s, want 3 completed", got)
			}

			// The order of the DELETEs: children first, in chunks of two, batch 1 then batch 2.
			var deletes []int // indices into events
			tl.mu.Lock()
			events := slices.Clone(tl.events)
			tl.mu.Unlock()
			var tables []string
			for i, ev := range events {
				if ev.kind == "delete" {
					deletes = append(deletes, i)
					tables = append(tables, ev.table)
				}
			}
			wantTables := []string{"line_notes", "line_notes", "order_lines", "order_lines", "orders", "line_notes", "order_lines", "orders"}
			if !slices.Equal(tables, wantTables) {
				t.Fatalf("DELETEs on %v, want %v", tables, wantTables)
			}

			// A DELETE's gate is the last wait before it.
			gateOf := func(del int) timelineEvent {
				for i := deletes[del] - 1; i >= 0; i-- {
					if events[i].kind == "gate" {
						return events[i]
					}
				}
				t.Fatalf("no gate wait before DELETE %d", del+1)
				return timelineEvent{}
			}
			firstOfBatch := map[int]bool{0: true, 5: true}
			for k := range deletes {
				gate, del := gateOf(k), events[deletes[k]]
				if gap := del.at.Sub(gate.at); gap >= sleep {
					t.Errorf("DELETE %d (%s) came %v after its gate wait: the sleep was between them, and the gate decision must be the last step before the statement",
						k+1, del.table, gap)
				}
				if firstOfBatch[k] {
					continue // no DELETE was issued before it in its batch, so no sleep precedes it
				}
				prev := events[deletes[k-1]]
				if gap := gate.at.Sub(prev.at); gap < sleep {
					t.Errorf("DELETE %d (%s): the gate was asked %v after DELETE %d, before the %v sleep had passed: the sleep comes first",
						k+1, del.table, gap, k, sleep)
				}
			}
		})
	}
}

// TestMySQLDeleteSleepStop (spec §7): a first stop during the inter-delete sleep. The sleep is
// not ended by it (the batch in flight always finishes, spec fact 8), and then the gate is asked.
// With healthy replicas the gate passes, the batch finishes and the run ends stopped at the
// boundary: a pin, true before the change too. With the replica's applier stopped during the
// sleep, the gate sees it when the sleep ends and the hold ends at once through LAG-1's rule,
// before the next DELETE; the next run's recovery finishes the batch.
func TestMySQLDeleteSleepStop(t *testing.T) {
	const sleep = time.Second
	oldOrders := "SELECT COUNT(*) FROM orders WHERE placed < '2020-01-01'"

	t.Run("healthy replicas", func(t *testing.T) {
		e := newEnv(t, "d16stopok")
		d16Tables(t, e, jobspec.Archive)
		seedOrders(t, e, 3)
		gate, _ := liveGate(t, 30)
		stop := make(chan struct{})
		var once sync.Once
		cfg := e.config(t, job(t, jobspec.Archive, orderJob))
		cfg.Processing.DeleteSleepSeconds = sleep.Seconds()
		cfg.Lag, cfg.Stop = gate, stop
		cfg.at = func(p point) {
			if p == atDeleted {
				once.Do(func() { go func() { time.Sleep(sleep / 4); close(stop) }() }) // during the first sleep
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		start := time.Now()
		rep, err := e.runContext(t, ctx, cfg)
		took := time.Since(start)
		if err != nil || rep == nil || !rep.Stopped || rep.Roots != 2 {
			t.Fatalf("Run() = %+v, %v, want batch 1 finished (2 roots) and Stopped", rep, err)
		}
		if want := 4 * sleep; took < want {
			t.Errorf("Run() took %v: a stop shortened a sleep, but batch 1's five DELETEs have four sleeps of %v between them", took, sleep)
		}
		if got := count(t, e.src, oldOrders); got != 1 {
			t.Errorf("old orders left = %d, want 1: batch 1 deleted both its roots", got)
		}
		if got := fmt.Sprint(statuses(t, e.dst, e.dstSchema)); got != "map[2:2]" {
			t.Errorf("log = %s, want batch 1's 2 roots completed", got)
		}
	})

	t.Run("applier stopped during the sleep", func(t *testing.T) {
		e := newEnv(t, "d16stopheld")
		d16Tables(t, e, jobspec.Archive)
		seedOrders(t, e, 3)
		gate, exec := liveGate(t, 30)
		replica := openServer(t, "TEST_REPLICA", "", nil) // for the goroutine below: exec fails the test with FailNow
		stop := make(chan struct{})
		var once sync.Once
		cfg := e.config(t, job(t, jobspec.Archive, orderJob))
		cfg.Processing.DeleteSleepSeconds = sleep.Seconds()
		cfg.Lag, cfg.Stop = gate, stop
		applier := make(chan struct{})
		cfg.at = func(p point) {
			if p == atDeleted {
				// The first DELETE is committed and its gate has passed; the applier stops, and a
				// stop is requested, while the sleep is running.
				once.Do(func() {
					go func() {
						time.Sleep(sleep / 4)
						if _, err := replica.ExecContext(context.Background(), "STOP REPLICA SQL_THREAD"); err != nil {
							t.Errorf("STOP REPLICA SQL_THREAD: %v", err)
						}
						close(applier)
						close(stop)
					}()
				})
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		rep, err := e.runContext(t, ctx, cfg)
		if err != nil || rep == nil || !rep.Stopped {
			t.Fatalf("Run() = %+v, %v, want Stopped and no error", rep, err)
		}
		select {
		case <-applier:
		default:
			t.Fatal("the run ended before the applier was stopped: the test proved nothing")
		}
		// Exactly one DELETE ran: two of the batch's four notes. The gate was asked after the
		// sleep, found the replica not applying and the stop requested, and ended the hold
		// before the second DELETE.
		if got := count(t, e.src, "SELECT COUNT(*) FROM line_notes"); got != 6 {
			t.Errorf("notes in the source = %d, want 6: one chunk of two deleted of eight, and none after the sleep", got)
		}
		if got := count(t, e.src, oldOrders); got != 3 {
			t.Errorf("old orders after the stop = %d, want 3", got)
		}
		if got := fmt.Sprint(statuses(t, e.dst, e.dstSchema)); got != "map[1:2]" {
			t.Errorf("log after the stop = %s, want batch 1's 2 roots still copied", got)
		}

		exec("START REPLICA SQL_THREAD")
		again := e.config(t, job(t, jobspec.Archive, orderJob))
		again.Lag = gate
		ctx2, cancel2 := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel2()
		rep, err = e.runContext(t, ctx2, again)
		if err != nil {
			t.Fatalf("second Run() = %v", err)
		}
		if rep.Recovered != 2 {
			t.Errorf("second run recovered %d, want 2", rep.Recovered)
		}
		if got := count(t, e.src, oldOrders); got != 0 {
			t.Errorf("old orders after the second run = %d, want 0", got)
		}
		if got := fmt.Sprint(statuses(t, e.dst, e.dstSchema)); got != "map[2:3]" {
			t.Errorf("log after the second run = %s, want 3 completed", got)
		}
	})
}
