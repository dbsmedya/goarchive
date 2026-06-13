package archiver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/internal/logger"
)

func TestSentinelGate_DisabledWhenEmptyPath(t *testing.T) {
	g := newSentinelGate("", logger.NewDefault())
	if err := g.wait(context.Background(), nil); err != nil {
		t.Fatalf("expected nil for empty path, got %v", err)
	}
}

func TestSentinelGate_NoPauseWhenAbsent(t *testing.T) {
	g := newSentinelGate("/no/such/sentinel", logger.NewDefault())
	g.presentFn = func(string) bool { return false }
	slept := 0
	g.sleepFn = func(context.Context, time.Duration) error { slept++; return nil }

	if err := g.wait(context.Background(), nil); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if slept != 0 {
		t.Fatalf("expected no poll sleeps when sentinel absent, got %d", slept)
	}
}

func TestSentinelGate_PausesUntilRemoved(t *testing.T) {
	g := newSentinelGate("/sentinel", logger.NewDefault())
	// Present for the first 3 checks (initial + 2 re-checks), then removed.
	calls := 0
	g.presentFn = func(string) bool {
		calls++
		return calls <= 3
	}
	sleeps := 0
	g.sleepFn = func(_ context.Context, d time.Duration) error {
		if d != sentinelPollInterval {
			t.Errorf("poll sleep = %v, want %v", d, sentinelPollInterval)
		}
		sleeps++
		return nil
	}

	if err := g.wait(context.Background(), nil); err != nil {
		t.Fatalf("wait: %v", err)
	}
	// 1 initial present()==true, then sleep+recheck until the 4th check is false:
	// 3 poll sleeps total.
	if sleeps != 3 {
		t.Fatalf("expected 3 poll sleeps before resume, got %d", sleeps)
	}
}

func TestSentinelGate_ContextCancelDuringPause(t *testing.T) {
	g := newSentinelGate("/sentinel", logger.NewDefault())
	g.presentFn = func(string) bool { return true } // never removed
	g.sleepFn = func(context.Context, time.Duration) error { return context.Canceled }

	err := g.wait(context.Background(), nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled from interrupted pause, got %v", err)
	}
}

func TestSentinelGate_StopRequestedBeforePause(t *testing.T) {
	g := newSentinelGate("/sentinel", logger.NewDefault())
	g.presentFn = func(string) bool { return true } // would pause forever
	sleeps := 0
	g.sleepFn = func(context.Context, time.Duration) error { sleeps++; return nil }

	stop := make(chan struct{})
	close(stop) // cooperative stop already requested

	if err := g.wait(context.Background(), stop); err != nil {
		t.Fatalf("expected nil when stop already requested, got %v", err)
	}
	if sleeps != 0 {
		t.Fatalf("expected no poll sleeps when stop precedes the pause, got %d", sleeps)
	}
}

func TestSentinelGate_StopRequestedDuringPause(t *testing.T) {
	g := newSentinelGate("/sentinel", logger.NewDefault())
	g.presentFn = func(string) bool { return true } // never removed
	stop := make(chan struct{})
	// First poll: close the stop channel and return nil (as the real sleep does on
	// stop). The wait loop's post-sleep stopRequested check must then exit with nil.
	g.sleepFn = func(context.Context, time.Duration) error {
		close(stop)
		return nil
	}

	if err := g.wait(context.Background(), stop); err != nil {
		t.Fatalf("expected nil when stop requested mid-pause, got %v", err)
	}
}

// TestSentinelGate_RealFilePresence exercises the real os.Stat-based presence
// check (no seam) to confirm presence-means-pause and absence-means-proceed.
func TestSentinelGate_RealFilePresence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pause.flag")

	// Absent -> returns immediately.
	g := newSentinelGate(path, logger.NewDefault())
	if err := g.wait(context.Background(), nil); err != nil {
		t.Fatalf("wait (absent): %v", err)
	}

	// Present -> would block; use a sleepFn that removes the file on first poll so
	// the real present() check then reports absent and the gate resumes.
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}
	g2 := newSentinelGate(path, logger.NewDefault())
	g2.sleepFn = func(context.Context, time.Duration) error {
		_ = os.Remove(path)
		return nil
	}
	if err := g2.wait(context.Background(), nil); err != nil {
		t.Fatalf("wait (present then removed): %v", err)
	}
}
