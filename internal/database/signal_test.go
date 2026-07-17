package database

import (
	"os"
	"syscall"
	"testing"
	"time"
)

func TestSetupGracefulShutdown_TwoPhase(t *testing.T) {
	if os.Getenv("CI") == "true" {
		t.Skip("Skipping signal test in CI environment")
	}

	var firstCalled, secondCalled bool
	ctx, stop := SetupGracefulShutdown(
		func(os.Signal) { firstCalled = true },
		func(os.Signal) { secondCalled = true },
	)
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
