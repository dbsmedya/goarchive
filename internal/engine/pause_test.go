package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/logger"
)

func TestStopRequested(t *testing.T) {
	if stopRequested(nil) {
		t.Error("a nil channel stopped")
	}
	stop := make(chan struct{})
	if stopRequested(stop) {
		t.Error("an open channel stopped")
	}
	close(stop)
	if !stopRequested(stop) {
		t.Error("a closed channel did not stop")
	}
}

func TestInterruptibleSleep(t *testing.T) {
	stop := make(chan struct{})
	close(stop)
	start := time.Now()
	if err := interruptibleSleep(context.Background(), stop, 3*time.Second); err != nil || time.Since(start) > time.Second {
		t.Errorf("a stop did not end the sleep: %v after %v", err, time.Since(start))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := interruptibleSleep(ctx, nil, time.Hour); !errors.Is(err, context.Canceled) {
		t.Errorf("a cancelled context = %v, want context.Canceled", err)
	}
	if err := interruptibleSleep(context.Background(), nil, 0); err != nil {
		t.Errorf("a zero sleep = %v", err)
	}
}

func TestSentinelGate(t *testing.T) {
	present := 3
	g := newSentinelGate("pause", logger.NewDefault())
	polls := 0
	g.presentFn = func(string) bool { present--; return present >= 0 }
	g.sleepFn = func(context.Context, time.Duration) error { polls++; return nil }
	if err := g.wait(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if polls != 3 {
		t.Errorf("waited %d polls while the file was present 3 more checks, want 3", polls)
	}

	g = newSentinelGate("pause", logger.NewDefault())
	g.presentFn = func(string) bool { return true }
	ctx, cancel := context.WithCancel(context.Background())
	g.sleepFn = func(context.Context, time.Duration) error { cancel(); return ctx.Err() }
	if err := g.wait(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Errorf("a cancelled pause = %v", err)
	}

	stop := make(chan struct{})
	g = newSentinelGate("pause", logger.NewDefault())
	g.presentFn = func(string) bool { return true }
	g.sleepFn = func(context.Context, time.Duration) error { close(stop); return nil }
	if err := g.wait(context.Background(), stop); err != nil {
		t.Errorf("a stop during a pause = %v, want nil", err)
	}

	if err := newSentinelGate("", logger.NewDefault()).wait(context.Background(), nil); err != nil {
		t.Errorf("no sentinel file = %v", err)
	}
}
