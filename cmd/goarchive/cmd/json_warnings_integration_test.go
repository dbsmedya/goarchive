//go:build integration

package cmd

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestMySQLCountWarnings (JSN-1): a run under --verification=count warns before it starts, and
// names a JSON column, on stderr and in the log, even with the log on a file and file_only set:
// the operator chose a weaker check, and the run says what it gives up.
func TestMySQLCountWarnings(t *testing.T) {
	f := orderFixture(t, "jsn_count")
	mustExec(t, f.src, "ALTER TABLE orders ADD COLUMN doc JSON")
	mustExec(t, f.dst, "ALTER TABLE orders ADD COLUMN doc JSON")
	logPath := filepath.Join(t.TempDir(), "run.log")
	setLogging(t, f, "logging:\n  level: error\n  output: "+logPath+"\n  file_only: true\n")
	_, errOut, code := mainStreams(t, "archive", "-c", f.config, "--job", "archive_orders", "--verification=count")
	if code != 0 {
		t.Fatalf("exit %d, stderr %q", code, errOut)
	}
	log := readAll(t, logPath)
	for _, w := range []string{
		"WARNING: verification: count: copied rows are counted by key, not compared, so a value the copy changes goes unseen. The source rows are then deleted.",
		"WARNING: verification: count: JSON columns orders.doc: a JSON value is copied as MySQL's printed text",
	} {
		if strings.Count(errOut, w) != 1 {
			t.Errorf("stderr holds %q %d times, want once:\n%s", w, strings.Count(errOut, w), errOut)
		}
		if !strings.Contains(log, w) {
			t.Errorf("log file lacks %q:\n%s", w, log)
		}
	}
}
