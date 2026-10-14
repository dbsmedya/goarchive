package harness

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const summarySHA = "0123456789abcdef0123456789abcdef01234567"

// summaryGate writes one gate artifact as the release-verify gate job uploads it: the gate
// run's directory (run.tsv, summary.txt, complete) and the server version beside it.
func summaryGate(t *testing.T, dir string, n int) string {
	t.Helper()
	runID := fmt.Sprintf("20261014T16590%dZ-100%d", n, n)
	run := filepath.Join(dir, fmt.Sprintf("gate-%d", n), runID)
	writeFixture(t, filepath.Join(run, "run.tsv"),
		"run_id\tsource_sha\tdirty\tstarted_utc\tended_utc\toutcome\tcommand_exit\trecording_exit\n"+
			runID+"\t"+summarySHA+"\tclean\t2026-10-14T16:59:07Z\t2026-10-14T17:15:50Z\tPASS\t0\t0\n")
	writeFixture(t, filepath.Join(run, "summary.txt"),
		"GATE SUMMARY ("+runID+")\n  integration        PASS    PASS=1239 FAIL=0 SKIP=5\n")
	writeFixture(t, filepath.Join(run, "complete"),
		"GATE COMPLETE - every stage above exited 0\nrun_id="+runID+"\nsha="+summarySHA+"\n")
	writeFixture(t, filepath.Join(dir, fmt.Sprintf("gate-%d", n), "server-version.txt"), "8.4.11\n")
	return run
}

// summaryRow writes one matrix row artifact with its verdict.
func summaryRow(t *testing.T, dir, row, verdict string) {
	t.Helper()
	writeFixture(t, filepath.Join(dir, "matrix-"+row, row, "verdict.txt"),
		"row: "+row+"  kind: version  setup: mysql:8.4\nimage: mysql:8.4  pair: 0  code: 0123456  tree: clean  go test exit: 0  seconds: 100\n"+
			"verdict: "+verdict+"  expect: pass  pass: 1199  fail: 0  skip: 4\n")
}

// replaceIn rewrites one field of a fixture file.
func replaceIn(t *testing.T, path, old, new string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), old) {
		t.Fatalf("%s does not contain %q", path, old)
	}
	writeFixture(t, path, strings.Replace(string(b), old, new, 1))
}

// TestGHSummaryFailsClosed (beta.1 PR 4, Codex R1 and R2): the release-verify summary is the
// run's certificate, so it accepts a gate only on the evidence tests/AGENTS.md requires (one
// run record for the target SHA, clean, PASS, both exits 0, its completion marker for the same
// run and SHA, and its summary), and it accepts the run only when every job it depends on
// succeeded, whatever the artifacts say.
func TestGHSummaryFailsClosed(t *testing.T) {
	script, err := filepath.Abs(filepath.Join("..", "envmatrix", "gh-summary.sh"))
	if err != nil {
		t.Fatal(err)
	}
	ok := []string{"target=success", "gate=success", "matrix=success"}
	for _, tc := range []struct {
		name  string
		mut   func(t *testing.T, dir string, gate0 string)
		needs []string
		want  int
		says  string
	}{
		{"every gate and row certified", nil, ok, 0, "Gates: 6 (want 6). Matrix rows: 2 (want 2)."},
		{"a missing gate", func(t *testing.T, dir, _ string) { _ = os.RemoveAll(filepath.Join(dir, "gate-5")) }, ok, 1, ""},
		{"a missing row", func(t *testing.T, dir, _ string) { _ = os.RemoveAll(filepath.Join(dir, "matrix-v97")) }, ok, 1, ""},
		{"a failed row", func(t *testing.T, dir, _ string) {
			replaceIn(t, filepath.Join(dir, "matrix-v97", "v97", "verdict.txt"), "verdict: OK", "verdict: FAIL")
		}, ok, 1, ""},
		{"a failed gate", func(t *testing.T, _, g string) { replaceIn(t, filepath.Join(g, "run.tsv"), "\tPASS\t", "\tFAIL\t") }, ok, 1, ""},
		{"no completion marker", func(t *testing.T, _, g string) { _ = os.Remove(filepath.Join(g, "complete")) }, ok, 1, "| gate-0 | NOT CERTIFIED"},
		{"a completion marker for another SHA", func(t *testing.T, _, g string) {
			replaceIn(t, filepath.Join(g, "complete"), "sha="+summarySHA, "sha=fedcba9876543210fedcba9876543210fedcba98")
		}, ok, 1, "| gate-0 | NOT CERTIFIED"},
		{"a completion marker for another run", func(t *testing.T, _, g string) {
			replaceIn(t, filepath.Join(g, "complete"), "run_id=", "run_id=x")
		}, ok, 1, "| gate-0 | NOT CERTIFIED"},
		{"a run of another SHA", func(t *testing.T, _, g string) {
			replaceIn(t, filepath.Join(g, "run.tsv"), "\t"+summarySHA+"\t", "\tfedcba9876543210fedcba9876543210fedcba98\t")
		}, ok, 1, "| gate-0 | NOT CERTIFIED"},
		{"a dirty tree", func(t *testing.T, _, g string) { replaceIn(t, filepath.Join(g, "run.tsv"), "\tclean\t", "\tdirty\t") }, ok, 1, "| gate-0 | NOT CERTIFIED"},
		{"a nonzero command exit", func(t *testing.T, _, g string) { replaceIn(t, filepath.Join(g, "run.tsv"), "PASS\t0\t0", "PASS\t1\t0") }, ok, 1, "| gate-0 | NOT CERTIFIED"},
		{"a nonzero recording exit", func(t *testing.T, _, g string) { replaceIn(t, filepath.Join(g, "run.tsv"), "PASS\t0\t0", "PASS\t0\t1") }, ok, 1, "| gate-0 | NOT CERTIFIED"},
		{"no gate summary", func(t *testing.T, _, g string) { _ = os.Remove(filepath.Join(g, "summary.txt")) }, ok, 1, "| gate-0 | NOT CERTIFIED"},
		{"two run records in one gate", func(t *testing.T, dir, _ string) { summaryGate(t, filepath.Join(dir, "gate-0", "again"), 0) }, ok, 1, "| gate-0 | NOT CERTIFIED"},
		{"a failed gate job", nil, []string{"target=success", "gate=failure", "matrix=success"}, 1, "| job gate | failure |"},
		{"a cancelled matrix job", nil, []string{"target=success", "gate=success", "matrix=cancelled"}, 1, "| job matrix | cancelled |"},
		{"a skipped target job", nil, []string{"target=skipped", "gate=success", "matrix=success"}, 1, "| job target | skipped |"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			gate0 := summaryGate(t, dir, 0)
			for n := 1; n < 6; n++ {
				summaryGate(t, dir, n)
			}
			summaryRow(t, dir, "v84", "OK")
			summaryRow(t, dir, "v97", "OK")
			if tc.mut != nil {
				tc.mut(t, dir, gate0)
			}
			cmd := exec.Command("bash", append([]string{script, dir, summarySHA, "2"}, tc.needs...)...)
			out, err := cmd.CombinedOutput()
			code := 0
			if ee, ok := errors.AsType[*exec.ExitError](err); ok {
				code = ee.ExitCode()
			} else if err != nil {
				t.Fatal(err)
			}
			if code != tc.want {
				t.Errorf("exit %d, want %d; output:\n%s", code, tc.want, out)
			}
			if tc.says != "" && !strings.Contains(string(out), tc.says) {
				t.Errorf("output does not name %q:\n%s", tc.says, out)
			}
		})
	}
}
