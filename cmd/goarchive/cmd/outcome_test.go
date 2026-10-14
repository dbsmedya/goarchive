package cmd

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/engine"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
)

// lastLogEntry logs one run outcome to a fresh file-only JSON log and returns its last entry.
func lastLogEntry(t *testing.T, rep *engine.Report, err error) map[string]any {
	t.Helper()
	path := filepath.Join(t.TempDir(), "run.log")
	log, lerr := logger.New(&config.LoggingConfig{Level: "info", Format: "json", Output: path, FileOnly: true})
	if lerr != nil {
		t.Fatal(lerr)
	}
	log = log.WithJob("archive_docs")
	log.Info("Batch 1: 2 roots of docs (archive)")
	logOutcome(log, jobspec.Archive, rep, err)
	_ = log.Close()
	data, rerr := os.ReadFile(path)
	if rerr != nil {
		t.Fatal(rerr)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	var entry map[string]any
	if jerr := json.Unmarshal([]byte(lines[len(lines)-1]), &entry); jerr != nil {
		t.Fatalf("last line %q: %v", lines[len(lines)-1], jerr)
	}
	return entry
}

// TestLogOutcome (GH #5): every run that reached its logger ends its log with one entry
// saying how it ended, with the report's counts, as 2.x's "Archive complete" did, so a log
// file shows the end of the run even under file_only.
func TestLogOutcome(t *testing.T) {
	rep := &engine.Report{Batches: 1, Roots: 2, Copied: map[string]int64{"docs": 2, "pages": 4},
		Deleted: map[string]int64{"docs": 2, "pages": 4}, Verification: jobspec.VerifySHA256,
		Verified: map[string]int64{"docs": 2, "pages": 4}, Elapsed: 1500 * time.Millisecond}
	stopped := *rep
	stopped.Stopped = true

	for _, tc := range []struct {
		name, level, msg string
		rep              *engine.Report
		err              error
		fields           map[string]any
	}{
		{"completed", "info", "Run completed", rep, nil,
			map[string]any{"roots": 2.0, "batches": 1.0, "elapsed_seconds": 1.5, "verification": "sha256"}},
		{"stopped", "info", "Run stopped before the end of the where range: run the job again to continue", &stopped, nil,
			map[string]any{"roots": 2.0}},
		{"failed with a report", "error", "Run failed", rep, errors.New("engine: batch 2: boom"),
			map[string]any{"roots": 2.0, "error": "engine: batch 2: boom"}},
		{"failed before the engine", "error", "Run failed", nil, errors.New("failed to connect to databases"),
			map[string]any{"error": "failed to connect to databases"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := lastLogEntry(t, tc.rep, tc.err)
			if e["level"] != tc.level || e["msg"] != tc.msg || e["job"] != "archive_docs" || e["job_type"] != "archive" {
				t.Fatalf("last entry = %v, want level %q, msg %q, job archive_docs, job_type archive", e, tc.level, tc.msg)
			}
			for k, v := range tc.fields {
				if e[k] != v {
					t.Errorf("%s = %v, want %v", k, e[k], v)
				}
			}
			if _, ok := e["stacktrace"]; ok {
				t.Errorf("the outcome entry carries a stack trace: %v", e["stacktrace"])
			}
			if tc.rep != nil {
				if copied, _ := e["copied"].(map[string]any); copied["pages"] != 4.0 {
					t.Errorf("copied = %v, want pages=4", e["copied"])
				}
			}
		})
	}
}
