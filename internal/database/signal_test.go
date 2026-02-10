package database

import (
	"os"
	"syscall"
	"testing"
	"time"
)

func TestSetupSignalHandler(t *testing.T) {
	ctx := SetupSignalHandler()

	// Context should not be cancelled initially
	select {
	case <-ctx.Done():
		t.Error("Context should not be cancelled immediately")
	default:
		// Expected - context is still active
	}

	// Clean up by sending a signal to ourselves
	// Note: In a real test scenario with t.Parallel(), this could affect other tests
	// But for this simple case, we'll verify the mechanism works
}

func TestSignalCancelsContext(t *testing.T) {
	if os.Getenv("CI") == "true" {
		t.Skip("Skipping signal test in CI environment")
	}

	ctx := SetupSignalHandler()

	// Send SIGTERM to ourselves
	time.Sleep(10 * time.Millisecond) // Let the goroutine start
	syscall.Kill(syscall.Getpid(), syscall.SIGTERM)

	// Wait for context to be cancelled
	select {
	case <-ctx.Done():
		// Success - context was cancelled
	case <-time.After(100 * time.Millisecond):
		t.Error("Context was not cancelled after receiving signal")
	}
}

func TestSetupSignalHandlerWithCallback(t *testing.T) {
	if os.Getenv("CI") == "true" {
		t.Skip("Skipping signal test in CI environment")
	}

	callbackCalled := false
	var receivedSignal os.Signal

	callback := func(sig os.Signal) {
		callbackCalled = true
		receivedSignal = sig
	}

	ctx := SetupSignalHandlerWithCallback(callback)

	// Send SIGINT to ourselves
	time.Sleep(10 * time.Millisecond)
	syscall.Kill(syscall.Getpid(), syscall.SIGINT)

	// Wait for callback and context cancellation
	select {
	case <-ctx.Done():
		if !callbackCalled {
			t.Error("Callback was not called")
		}
		if receivedSignal != syscall.SIGINT {
			t.Errorf("Expected signal SIGINT, got %v", receivedSignal)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Context was not cancelled after receiving signal")
	}
}

func TestContextNotCancelledWithoutSignal(t *testing.T) {
	ctx := SetupSignalHandler()

	// Wait a short time
	time.Sleep(50 * time.Millisecond)

	// Context should still not be cancelled
	select {
	case <-ctx.Done():
		t.Error("Context should not be cancelled without signal")
	default:
		// Expected
	}
}
