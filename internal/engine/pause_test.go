package engine

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
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

// TestSentinelPauseShowsOnProgress (R50): while the sentinel pauses the run, the --progress
// line says so and its ETA reads paused; the mark clears when the run resumes.
func TestSentinelPauseShowsOnProgress(t *testing.T) {
	tr := newProgressTracker(10, time.Now())
	g := newSentinelGate("/var/run/goarchive.pause", logger.NewDefault())
	g.progress = tr
	present := 2
	var during string
	g.presentFn = func(string) bool { present--; return present >= 0 }
	g.sleepFn = func(context.Context, time.Duration) error { during = tr.snapshot().pausedReason; return nil }
	if err := g.wait(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if want := `sentinel file "/var/run/goarchive.pause" present`; during != want {
		t.Errorf("paused reason while paused = %q, want %q", during, want)
	}
	if got := tr.snapshot().pausedReason; got != "" {
		t.Errorf("paused reason after resume = %q", got)
	}
}

// TestSentinelResumeIsLive (spec §3.3): with a file log's tee, the pause's end reaches stderr
// like its start, so the console never ends on "Paused".
func TestSentinelResumeIsLive(t *testing.T) {
	var errb bytes.Buffer
	// At level error: the pause and its end are live, so the level hides neither (Codex MR R1).
	log, err := logger.New(&config.LoggingConfig{Level: "error", Format: "json", Output: filepath.Join(t.TempDir(), "run.log")},
		logger.Options{Stderr: &errb, Tee: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	present := 1
	g := newSentinelGate("pause", log)
	g.presentFn = func(string) bool { present--; return present >= 0 }
	g.sleepFn = func(context.Context, time.Duration) error { return nil }
	if err := g.wait(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if got := errb.String(); !strings.Contains(got, "WARN  Paused: sentinel file") || !strings.Contains(got, "INFO  Resumed: sentinel file") {
		t.Errorf("tee = %q, want the pause and its end", got)
	}
}
