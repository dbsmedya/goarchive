//go:build integration

package cmd

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestMySQLCountWarnings (JSN-1): a run under --verification=count warns before it starts, and
// names a JSON column, on stderr and in the log, even with the log on a file and file_only set:
// the operator chose a weaker check, and the run says what it gives up. validate and dry-run,
// which run the same preflight, print both once too (Fable's final review F5; a control).
func TestMySQLCountWarnings(t *testing.T) {
	f := orderFixture(t, "jsn_count")
	mustExec(t, f.src, "ALTER TABLE orders ADD COLUMN doc JSON")
	mustExec(t, f.dst, "ALTER TABLE orders ADD COLUMN doc JSON")
	logPath := filepath.Join(t.TempDir(), "run.log")
	setLogging(t, f, "logging:\n  level: error\n  output: "+logPath+"\n  file_only: true\n")
	want := []string{
		"WARNING: verification: count: copied rows are counted by key, not compared, so a value the copy changes goes unseen. The source rows are then deleted.",
		"WARNING: verification: count: JSON columns orders.doc: a JSON value is copied as MySQL's printed text",
	}
	// validate and dry-run first: they change nothing, so archive still has rows to move.
	for _, command := range []string{"validate", "dry-run", "archive"} {
		t.Run(command, func(t *testing.T) {
			_, errOut, code := mainStreams(t, command, "-c", f.config, "--job", "archive_orders", "--verification=count")
			if code != 0 {
				t.Fatalf("exit %d, stderr %q", code, errOut)
			}
			log := readAll(t, logPath)
			for _, w := range want {
				if strings.Count(errOut, w) != 1 {
					t.Errorf("stderr holds %q %d times, want once:\n%s", w, strings.Count(errOut, w), errOut)
				}
				if !strings.Contains(log, w) {
					t.Errorf("log file lacks %q:\n%s", w, log)
				}
			}
		})
	}
}
