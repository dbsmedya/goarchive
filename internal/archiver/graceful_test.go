package archiver

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestStopRequested(t *testing.T) {
	if stopRequested(nil) {
		t.Error("nil channel must never report stop")
	}

	open := make(chan struct{})
	if stopRequested(open) {
		t.Error("open channel must not report stop")
	}

	closed := make(chan struct{})
	close(closed)
	if !stopRequested(closed) {
		t.Error("closed channel must report stop")
	}
}

func TestIsCancellation(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"canceled", context.Canceled, true},
		{"deadline", context.DeadlineExceeded, true},
		{"wrapped canceled", fmt.Errorf("copy failed: %w", context.Canceled), true},
		{"other", errors.New("duplicate entry"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isCancellation(tc.err); got != tc.want {
				t.Errorf("isCancellation(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestInterruptibleSleep_Timeout(t *testing.T) {
	// A short sleep with no interruption returns nil after elapsing.
	start := time.Now()
	if err := interruptibleSleep(context.Background(), nil, 20*time.Millisecond); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if elapsed := time.Since(start); elapsed < 15*time.Millisecond {
		t.Errorf("returned too early (%v) - did not wait for the timer", elapsed)
	}
}

func TestInterruptibleSleep_ZeroDuration(t *testing.T) {
	if err := interruptibleSleep(context.Background(), nil, 0); err != nil {
		t.Fatalf("zero duration must return nil immediately, got %v", err)
	}
}

func TestInterruptibleSleep_StopReturnsNil(t *testing.T) {
	// A cooperative stop ends the sleep early with nil (not an error), so the
	// caller falls through to its boundary stop check.
	stop := make(chan struct{})
	close(stop)
	start := time.Now()
	if err := interruptibleSleep(context.Background(), stop, 10*time.Second); err != nil {
		t.Fatalf("stop must return nil, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("stop did not short-circuit the sleep (took %v)", elapsed)
	}
}

func TestInterruptibleSleep_ContextCancelReturnsErr(t *testing.T) {
	// A canceled work context (second Ctrl-C) ends the sleep with ctx.Err().
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := interruptibleSleep(ctx, nil, 10*time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
