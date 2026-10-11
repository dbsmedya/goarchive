package engine

import (
	"context"
	"testing"
	"time"
)

// TestCleanupContext: cleanup (a lock release, the final job status, the FOREIGN_KEY_CHECKS
// reset) runs on a context the run's cancellation cannot end, bounded so a wedged server
// cannot hang the exit (2.x issue #79, M5-F2).
func TestCleanupContext(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	ctx, done := cleanupContext(parent)
	defer done()
	if err := ctx.Err(); err != nil {
		t.Fatalf("a cancelled run cancels its cleanup: %v", err)
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("cleanup is unbounded")
	}
	if left := time.Until(deadline); left <= 0 || left > 5*time.Second {
		t.Errorf("cleanup bound = %s, want at most 5s", left)
	}
}
