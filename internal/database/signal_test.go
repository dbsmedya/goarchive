//go:build !windows

// Unix only: these tests send the process real signals with syscall.Kill, which Windows lacks.

package database

import (
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestSetupGracefulShutdown_TwoPhase(t *testing.T) {
	if os.Getenv("CI") == "true" {
		t.Skip("Skipping signal test in CI environment")
	}

	var firstCalled, secondCalled bool
	sh := SetupGracefulShutdown(
		func(os.Signal) { firstCalled = true },
		func(os.Signal) { secondCalled = true },
	)
	ctx, stop := sh.Context(), sh.Stop()
	time.Sleep(10 * time.Millisecond) // let the handler goroutine register

	// Phase 1: first signal closes stop but MUST leave the work context alive so
	// the in-flight batch can finish.
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	select {
	case <-stop:
		// expected
	case <-time.After(500 * time.Millisecond):
		t.Fatal("first signal did not close the stop channel")
	}
	if !firstCalled {
		t.Error("onFirst callback was not invoked")
	}
	select {
	case <-ctx.Done():
		t.Fatal("work context must stay alive after the FIRST signal")
	default:
	}

	// Phase 2: second signal cancels the work context (hard abort).
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	select {
	case <-ctx.Done():
		// expected
	case <-time.After(500 * time.Millisecond):
		t.Fatal("second signal did not cancel the work context")
	}
	if !secondCalled {
		t.Error("onSecond callback was not invoked")
	}
}

// TestRequestFirstHandsOffToTheSignalGoroutine (TEL-1 C6, spec §4.7, D17): a stop requested from
// code returns at once, even while the first phase's log line is slow, because the request is handed
// to the signal goroutine, which runs onFirst and then closes the stop channel. A handler of the
// control socket therefore never writes that line itself, nor waits for it.
func TestRequestFirstHandsOffToTheSignalGoroutine(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int32
	var inGoroutine atomic.Bool
	sh := SetupGracefulShutdown(func(sig os.Signal) {
		calls.Add(1)
		inGoroutine.Store(sig != nil)
		<-release // the log line is slow
	}, nil)

	if sh.StopRequested() {
		t.Fatal("a stop is requested before anything asked for one")
	}
	began := time.Now()
	sh.RequestFirst()
	if took := time.Since(began); took > 200*time.Millisecond {
		t.Errorf("RequestFirst took %v while onFirst was blocked, want it at once", took)
	}
	select {
	case <-sh.Stop():
		t.Fatal("the stop channel closed before onFirst returned: the line is written first, as for a signal")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-sh.Stop():
	case <-time.After(2 * time.Second):
		t.Fatal("the stop channel did not close once onFirst returned")
	}
	if !sh.StopRequested() {
		t.Error("StopRequested is false after the stop channel closed")
	}
	if calls.Load() != 1 || !inGoroutine.Load() {
		t.Errorf("onFirst ran %d times, with a signal value %v; want once, with one (a stand-in for a request from code)", calls.Load(), inGoroutine.Load())
	}
	select {
	case <-sh.Context().Done():
		t.Error("the work context was cancelled by the first phase")
	default:
	}
}

// TestStopPhases (spec §7 row 66): a stop from code closes the stop channel once; asking again does
// not close it twice, run onFirst again or block; a real signal after it is the second phase and
// cancels the work context.
func TestStopPhases(t *testing.T) {
	var first, second atomic.Int32
	sh := SetupGracefulShutdown(func(os.Signal) { first.Add(1) }, func(os.Signal) { second.Add(1) })
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() { defer wg.Done(); sh.RequestFirst() }()
	}
	wg.Wait()
	select {
	case <-sh.Stop():
	case <-time.After(2 * time.Second):
		t.Fatal("the stop channel did not close")
	}
	sh.RequestFirst() // once more, after the phase is over: nothing happens and nothing blocks
	time.Sleep(50 * time.Millisecond)
	if first.Load() != 1 {
		t.Errorf("onFirst ran %d times for nine requests, want once", first.Load())
	}
	select {
	case <-sh.Context().Done():
		t.Fatal("a request from code cancelled the work context: no socket command cancels a run")
	default:
	}

	if os.Getenv("CI") == "true" {
		t.Skip("the second phase needs a real signal, which the CI environment does not take")
	}
	time.Sleep(10 * time.Millisecond) // the handler is registered
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	select {
	case <-sh.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("a signal after a stop from code did not cancel the work context")
	}
	if second.Load() != 1 {
		t.Errorf("onSecond ran %d times, want once", second.Load())
	}
}
