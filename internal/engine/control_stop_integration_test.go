//go:build integration

package engine

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// The engine's half of the control socket's stop (TEL-1 C4a, R2-1): the store's Stop asks for the
// first phase through the function it was given, which closes the run's stop channel, exactly as
// the first signal does. The engine sees only its stop channel, so these tests pass against the
// engine as it is: they pin what a stop from code does, and the store's own tests (red first)
// prove the store's part. The command halves, over the socket, are C6's.

// stopper is a Config.Stop and the RequestStop that closes it, once.
type stopper struct {
	ch   chan struct{}
	once sync.Once
	n    int
	mu   sync.Mutex
}

func newStopper() *stopper { return &stopper{ch: make(chan struct{})} }

func (s *stopper) request() {
	s.mu.Lock()
	s.n++
	s.mu.Unlock()
	s.once.Do(func() { close(s.ch) })
}

func (s *stopper) requests() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}

func stopConfig(t *testing.T, e env, size int) (Config, *tuning.Store, *stopper) {
	t.Helper()
	cfg := e.config(t, job(t, jobspec.Archive, orderJob))
	cfg.Processing = config.ProcessingConfig{BatchSize: size, BatchDeleteSize: size}
	st := newStopper()
	store := controlStore(cfg, false, func(o *tuning.Options) { o.RequestStop = st.request })
	cfg.Tuning, cfg.Stop = store, st.ch
	return cfg, store, st
}

// TestMySQLControlStopFinalBatch (engine half, pin): a stop inside the final short batch leaves
// the report completed, not stopped, as a first signal does there (spec fact 6): the batch ends
// the where range, so there is nothing to stop before.
func TestMySQLControlStopFinalBatch(t *testing.T) {
	e := newEnv(t, "ctlstopfinal")
	d16Tables(t, e, jobspec.Archive)
	seedOrders(t, e, 3) // batches of 2 and 1 roots: the second is short and final
	cfg, store, st := stopConfig(t, e, 2)
	commits := 0
	cfg.at = func(p point) {
		if p != atCommitted {
			return
		}
		if commits++; commits == 2 {
			if res, err := store.Stop(controlPeer); err != nil || !res.Changed {
				t.Fatalf("Stop() = %+v, %v", res, err)
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rep, err := e.runContext(t, ctx, cfg)
	if err != nil || rep == nil || rep.Stopped || rep.Roots != 3 {
		t.Fatalf("Run() = %+v, %v, want every root archived and the report completed, not stopped", rep, err)
	}
	if st.requests() != 1 {
		t.Errorf("the first phase was requested %d times, want 1", st.requests())
	}
	if got := count(t, e.src, "SELECT COUNT(*) FROM orders WHERE placed < '2020-01-01'"); got != 0 {
		t.Errorf("old orders left = %d, want 0", got)
	}
}

// TestMySQLControlStop (engine half): a stop from the store at a point inside a batch lets the
// batch complete and the run end stopped at the boundary; a second stop is "already stopping" and
// does not cancel anything.
func TestMySQLControlStop(t *testing.T) {
	e := newEnv(t, "ctlstop")
	d16Tables(t, e, jobspec.Archive)
	seedOrders(t, e, 5) // batches of 2, 2 and 1
	cfg, store, st := stopConfig(t, e, 2)
	var second tuning.StopResult
	cfg.at = func(p point) {
		switch p {
		case atCommitted:
			if res, err := store.Stop(controlPeer); err != nil || !res.Changed {
				t.Fatalf("Stop() = %+v, %v", res, err)
			}
		case atDeleted:
			if st.requests() == 1 && !second.Already {
				res, err := store.Stop(controlPeer)
				if err != nil {
					t.Fatalf("the second Stop() = %v", err)
				}
				second = res
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rep, err := e.runContext(t, ctx, cfg)
	if err != nil || rep == nil || !rep.Stopped || rep.Roots != 2 || rep.Batches != 1 {
		t.Fatalf("Run() = %+v, %v, want batch 1 finished (2 roots) and Stopped", rep, err)
	}
	if !second.Already || second.Changed {
		t.Errorf("the second Stop() = %+v, want already stopping", second)
	}
	if st.requests() != 1 {
		t.Errorf("the first phase was requested %d times, want 1: a second stop asks for nothing", st.requests())
	}
	if got := count(t, e.src, "SELECT COUNT(*) FROM orders WHERE placed < '2020-01-01'"); got != 3 {
		t.Errorf("old orders left = %d, want 3", got)
	}
	if got := fmt.Sprint(statuses(t, e.dst, e.dstSchema)); got != "map[2:2]" {
		t.Errorf("log = %s, want batch 1's 2 roots completed", got)
	}
}

// TestMySQLControlStopWithError (engine half): a stop, then the batch's error: the error is the
// run's result, not swallowed by the stop, and the report is not "stopped". The error here is a
// DELETE that hits fewer rows than its keys (a key set error), made by dropping the last key of
// the first DELETE on the notes through Config.fault, the seam the key-set tests use.
func TestMySQLControlStopWithError(t *testing.T) {
	e := newEnv(t, "ctlstoperr")
	d16Tables(t, e, jobspec.Archive)
	seedOrders(t, e, 5)
	cfg, store, _ := stopConfig(t, e, 2)
	cfg.at = func(p point) {
		if p == atCommitted {
			if _, err := store.Stop(controlPeer); err != nil {
				t.Fatalf("Stop() = %v", err)
			}
		}
	}
	cfg.fault = func(st *statement) {
		if st.op != opDelete || st.table != "line_notes" || len(st.args) < 2 {
			return
		}
		i := strings.LastIndex(st.query, ",?)")
		st.query = st.query[:i] + ")" + st.query[i+3:]
		st.args = st.args[:len(st.args)-1]
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rep, err := e.runContext(t, ctx, cfg)
	wantErr[*KeySetError](t, err)
	if rep != nil && rep.Stopped {
		t.Errorf("the report says Stopped after an error: %+v", rep)
	}
	if got := fmt.Sprint(statuses(t, e.dst, e.dstSchema)); got != "map[1:2]" {
		t.Errorf("log = %s, want batch 1's 2 roots still copied for the next run's recovery", got)
	}
}
