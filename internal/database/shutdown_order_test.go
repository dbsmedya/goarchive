package database

import (
	"os"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"
)

// The order of the shutdown's two phases (TEL-1 R2, Codex batch 2 finding 2; spec §4.7, D17). A stop
// from code is accepted when RequestFirst returns, and that acceptance is the first phase: a real signal
// after it is the second phase and cancels the work context, whether the dispatcher had run by then or
// not. These tests queue both inputs before the dispatcher runs, through newShutdown, which takes the
// signal channel as an argument; nothing is injected into the dispatch loop, and no real signal is sent.

// phaseLog records what onFirst and onSecond were called with, in order.
type phaseLog struct {
	mu     sync.Mutex
	events []string
}

func (p *phaseLog) add(e string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, e)
}

func (p *phaseLog) first(sig os.Signal)  { p.add("first: " + sig.String()) }
func (p *phaseLog) second(sig os.Signal) { p.add("second: " + sig.String()) }

func (p *phaseLog) got() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return slices.Clone(p.events)
}

const (
	fromCode = "control socket stop request"
	fromTerm = "terminated" // syscall.SIGTERM's text
)

// TestStopOrderBothPendingBeforeDispatch: a request from code and a signal are both waiting when the
// dispatcher starts, in either order of arrival. The request was accepted, so it is the first phase and
// the signal the second: the work context is cancelled, onFirst ran for the request and onSecond for the
// signal. The select that reads two ready inputs picks one at random, so the check repeats.
func TestStopOrderBothPendingBeforeDispatch(t *testing.T) {
	for _, order := range []struct {
		name         string
		requestFirst bool // the request is made before the signal is queued
	}{{"the request, then the signal", true}, {"the signal, then the request", false}} {
		t.Run(order.name, func(t *testing.T) {
			for i := range 300 {
				sigs := make(chan os.Signal, 2)
				var log phaseLog
				s, dispatch := newShutdown(log.first, log.second, sigs)
				if order.requestFirst {
					s.RequestFirst()
					sigs <- syscall.SIGTERM
				} else {
					sigs <- syscall.SIGTERM
					s.RequestFirst()
				}
				go dispatch()

				select {
				case <-s.Context().Done():
				case <-time.After(500 * time.Millisecond):
					t.Fatalf("iteration %d: the signal after an accepted stop did not cancel the work context; calls so far: %q", i, log.got())
				}
				if want := []string{"first: " + fromCode, "second: " + fromTerm}; !slices.Equal(log.got(), want) {
					t.Fatalf("iteration %d: calls = %q, want %q", i, log.got(), want)
				}
				if !s.StopRequested() {
					t.Fatalf("iteration %d: the stop channel is not closed", i)
				}
			}
		})
	}
}

// TestStopOrderRepeatedRequests: many requests from code before the dispatcher runs are one first
// phase, run once; a signal after them is the second phase; none of the requests is left waiting to be
// taken for a phase later.
func TestStopOrderRepeatedRequests(t *testing.T) {
	sigs := make(chan os.Signal, 2)
	var log phaseLog
	s, dispatch := newShutdown(log.first, log.second, sigs)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() { defer wg.Done(); s.RequestFirst() }()
	}
	wg.Wait()
	s.RequestFirst()
	go dispatch()

	select {
	case <-s.Stop():
	case <-time.After(2 * time.Second):
		t.Fatal("the stop channel did not close")
	}
	sigs <- syscall.SIGTERM
	select {
	case <-s.Context().Done():
	case <-time.After(2 * time.Second):
		t.Fatalf("the signal after the requests did not cancel the work context; calls: %q", log.got())
	}
	if want := []string{"first: " + fromCode, "second: " + fromTerm}; !slices.Equal(log.got(), want) {
		t.Errorf("calls = %q, want %q", log.got(), want)
	}
}

// TestStopOrderRequestAfterASignal: a signal took the first phase; a request from code afterwards does
// nothing (it cannot be a second phase: no socket command cancels a run), and the next signal is the
// second phase.
func TestStopOrderRequestAfterASignal(t *testing.T) {
	sigs := make(chan os.Signal, 2)
	var log phaseLog
	s, dispatch := newShutdown(log.first, log.second, sigs)
	go dispatch()
	sigs <- syscall.SIGTERM
	select {
	case <-s.Stop():
	case <-time.After(2 * time.Second):
		t.Fatal("the signal did not take the first phase")
	}

	s.RequestFirst()
	select {
	case <-s.Context().Done():
		t.Fatal("a request from code cancelled the work context: no socket command cancels a run")
	case <-time.After(50 * time.Millisecond):
	}

	sigs <- syscall.SIGINT
	select {
	case <-s.Context().Done():
	case <-time.After(2 * time.Second):
		t.Fatalf("the second signal did not cancel the work context; calls: %q", log.got())
	}
	if want := []string{"first: " + fromTerm, "second: interrupt"}; !slices.Equal(log.got(), want) {
		t.Errorf("calls = %q, want %q: the request after the signal is not a phase", log.got(), want)
	}
}
