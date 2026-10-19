//go:build integration

package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMySQLRunLogEndsWithOutcome (GH #5): a run's log file ends with how the run ended, under
// file_only too, where the printed report reaches no file: "Run completed" with the counts,
// then, for a run that fails, "Run failed" with the error.
func TestMySQLRunLogEndsWithOutcome(t *testing.T) {
	f := newFixture(t, "outcome", pageJobs)
	ddl := []string{
		"CREATE TABLE docs (id INT NOT NULL, placed DATE NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB",
		"CREATE TABLE pages (doc_id INT NOT NULL, page_no INT NOT NULL, body LONGBLOB, scan LONGBLOB, " +
			"PRIMARY KEY (doc_id, page_no), CONSTRAINT fk_pages_doc FOREIGN KEY (doc_id) REFERENCES docs (id)) ENGINE=InnoDB",
	}
	mustExec(t, f.src, ddl...)
	mustExec(t, f.dst, ddl...)
	mustExec(t, f.src, "INSERT INTO docs VALUES (1, '2019-01-01'), (2, '2024-01-01')",
		"INSERT INTO pages VALUES (1, 0, 'a', NULL), (1, 1, 'b', NULL), (2, 0, 'c', NULL)")

	logPath := filepath.Join(t.TempDir(), "run.log")
	cfg, err := os.ReadFile(f.config)
	if err != nil {
		t.Fatal(err)
	}
	logging := "logging:\n  level: info\n  format: json\n  output: " + logPath + "\n  file_only: true\n"
	body := strings.Replace(string(cfg), "logging:\n  level: error\n  output: stderr\n", logging, 1)
	if body == string(cfg) {
		t.Fatal("fixture config has no logging block to replace")
	}
	if err := os.WriteFile(f.config, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	if out, err := execute(t, "archive", "-c", f.config, "--job", "archive_docs"); err != nil {
		t.Fatalf("archive: %v\n%s", err, out)
	}
	e := lastEntry(t, logPath)
	if e["msg"] != "Run completed" || e["job"] != "archive_docs" || e["roots"] != 1.0 {
		t.Fatalf("last entry after the archive = %v, want Run completed for archive_docs with roots 1", e)
	}
	if copied, _ := e["copied"].(map[string]any); copied["pages"] != 2.0 {
		t.Errorf("copied = %v, want pages=2", e["copied"])
	}

	mustExec(t, f.dst, "DROP TABLE pages")
	if _, err := execute(t, "archive", "-c", f.config, "--job", "archive_docs"); err == nil {
		t.Fatal("archive into a destination without pages succeeded")
	}
	e = lastEntry(t, logPath)
	if e["msg"] != "Run failed" || e["level"] != "error" || !strings.Contains(e["error"].(string), "pages") {
		t.Fatalf("last entry after the failed archive = %v, want Run failed naming pages", e)
	}
}

func lastEntry(t *testing.T, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	var e map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &e); err != nil {
		t.Fatalf("last line %q: %v", lines[len(lines)-1], err)
	}
	return e
}
