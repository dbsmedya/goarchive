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

// summaryPart writes one part artifact as the Gate verify workflow's integration or e2e job
// uploads it (RVG-3): the part run's directory (run.tsv, summary.txt, complete) and the server
// version beside it.
func summaryPart(t *testing.T, dir string, n int, part string) string {
	t.Helper()
	runID := fmt.Sprintf("20261014T16590%dZ-%d%d", n, len(part), n)
	art := filepath.Join(dir, fmt.Sprintf("gate-%s-%d", part, n))
	run := filepath.Join(art, runID)
	writeFixture(t, filepath.Join(run, "run.tsv"),
		"run_id\tsource_sha\tdirty\tstarted_utc\tended_utc\toutcome\tcommand_exit\trecording_exit\tpart\n"+
			runID+"\t"+summarySHA+"\tclean\t2026-10-14T16:59:07Z\t2026-10-14T17:05:50Z\tPASS\t0\t0\t"+part+"\n")
	stages := "  integration        PASS    PASS=1239 FAIL=0 SKIP=5\n  e2e                NOT RUN -\n  e2e-examples       NOT RUN -\n"
	if part == "e2e" {
		stages = "  integration        NOT RUN -\n  e2e                PASS    Passed: 13  Failed: 0\n  e2e-examples       PASS    Passed: 6  Failed: 0\n"
	}
	writeFixture(t, filepath.Join(run, "summary.txt"), "GATE SUMMARY ("+runID+")\n"+stages)
	writeFixture(t, filepath.Join(run, "complete"),
		"GATE PART COMPLETE - every stage of part "+part+" exited 0\nrun_id="+runID+"\nsha="+summarySHA+"\npart="+part+"\n")
	writeFixture(t, filepath.Join(art, "server-version.txt"), "8.4.11\n")
	return run
}

// summaryGate writes both parts of image n and returns the integration part's run directory.
func summaryGate(t *testing.T, dir string, n int) string {
	t.Helper()
	summaryPart(t, dir, n, "e2e")
	return summaryPart(t, dir, n, "integration")
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

// TestGHSummaryFailsClosed (beta.1 PR 4, Codex R1 and R2; beta.2 RVG-1a+b, Sonnet F1, F2 and F5):
// the summary is a Matrix verify or Gate verify run's certificate, so it accepts a gate only on
// the evidence tests/AGENTS.md requires (one run record for the target SHA, clean, PASS, both
// exits 0, its completion marker for the same run and SHA, and its summary), it accepts the run
// only when every job it depends on succeeded, whatever the artifacts say, and only when the
// run holds exactly the gates and rows it wants: a gate in a matrix run, or a row in a gate run,
// fails it. Counts that are missing or not numbers, or a summary that wants nothing, exit 2
// before anything is read. Since RVG-3 a Gate verify image is two part artifacts, integration
// and e2e, and counts as a gate only when both are certified, each for its own part.
func TestGHSummaryFailsClosed(t *testing.T) {
	script, err := filepath.Abs(filepath.Join("..", "envmatrix", "gh-summary.sh"))
	if err != nil {
		t.Fatal(err)
	}
	ok := []string{"target=success", "integration=success", "e2e=success", "matrix=success"}
	matrixOK := []string{"target=success", "matrix=success"}
	gateOK := []string{"target=success", "integration=success", "e2e=success"}
	notCertified := []string{"| gate-0 integration | NOT CERTIFIED"}
	for _, tc := range []struct {
		name  string
		build string // the artifacts: "" six gates and two rows, "matrix" the rows only, "gate" the gates only
		mut   func(t *testing.T, dir string, gate0 string)
		args  []string // <rows-wanted> <gates-wanted>; nil means 2 6
		needs []string
		want  int
		says  []string // every one must appear in the output
	}{
		{name: "every gate and row certified", needs: ok,
			says: []string{"## Gate verify: `0123456789ab`", "Gates: 6 (want 6). Matrix rows: 2 (want 2).",
				"| gate-0 integration | PASS | PASS=1239 FAIL=0 SKIP=5 | 8.4.11 |", "| gate-0 e2e | PASS | E2E 13/0, examples 6/0 | 8.4.11 |"}},
		{name: "a missing gate", mut: func(t *testing.T, dir, _ string) {
			_ = os.RemoveAll(filepath.Join(dir, "gate-integration-5"))
			_ = os.RemoveAll(filepath.Join(dir, "gate-e2e-5"))
		}, needs: ok, want: 1, says: []string{"Gates: 5 (want 6)."}},

		// The two parts of an image (RVG-3): an image counts as a gate only with both certified.
		{name: "an image without its e2e part", mut: func(t *testing.T, dir, _ string) { _ = os.RemoveAll(filepath.Join(dir, "gate-e2e-3")) }, needs: ok, want: 1,
			says: []string{"| gate-3 e2e | MISSING |", "Gates: 5 (want 6)."}},
		{name: "an image without its integration part", mut: func(t *testing.T, dir, _ string) { _ = os.RemoveAll(filepath.Join(dir, "gate-integration-3")) }, needs: ok, want: 1,
			says: []string{"| gate-3 integration | MISSING |", "Gates: 5 (want 6)."}},
		{name: "a part whose run.tsv names the other part", mut: func(t *testing.T, _, g string) {
			replaceIn(t, filepath.Join(g, "run.tsv"), "\tintegration\n", "\te2e\n")
		}, needs: ok, want: 1, says: append(notCertified, "Gates: 5 (want 6).")},
		{name: "a part whose completion marker names another part", mut: func(t *testing.T, _, g string) {
			replaceIn(t, filepath.Join(g, "complete"), "part=integration", "part=e2e")
		}, needs: ok, want: 1, says: notCertified},
		{name: "a whole gate in a part artifact", mut: func(t *testing.T, _, g string) {
			replaceIn(t, filepath.Join(g, "run.tsv"), "\tintegration\n", "\tall\n")
			replaceIn(t, filepath.Join(g, "complete"), "GATE PART COMPLETE - every stage of part integration exited 0", "GATE COMPLETE - every stage above exited 0")
			replaceIn(t, filepath.Join(g, "complete"), "part=integration", "part=all")
		}, needs: ok, want: 1, says: notCertified},
		{name: "an old whole-gate artifact", mut: func(t *testing.T, dir, _ string) {
			writeFixture(t, filepath.Join(dir, "gate-6", "server-version.txt"), "8.4.11\n")
		}, needs: ok, want: 1, says: []string{"| gate-6 | NOT CERTIFIED: not a part artifact |"}},
		{name: "a part artifact in a matrix run", build: "matrix", mut: func(t *testing.T, dir, _ string) { summaryPart(t, dir, 0, "integration") },
			args: []string{"2", "0"}, needs: matrixOK, want: 1, says: []string{"| gate-0 integration | NOT CERTIFIED: unexpected image |"}},
		// MR review R1: the wanted images are 0..5, each named once, canonically.
		{name: "an unexpected image in place of a missing one", mut: func(t *testing.T, dir, _ string) {
			_ = os.RemoveAll(filepath.Join(dir, "gate-integration-5"))
			_ = os.RemoveAll(filepath.Join(dir, "gate-e2e-5"))
			summaryGate(t, dir, 6)
		}, needs: ok, want: 1, says: []string{"| gate-6 integration | NOT CERTIFIED: unexpected image |", "| gate-5 integration | MISSING |", "Gates: 5 (want 6)."}},
		{name: "a part named with a leading zero", mut: func(t *testing.T, dir, _ string) {
			src := summaryPart(t, filepath.Join(dir, "alias"), 0, "integration")
			if err := os.Rename(filepath.Dir(src), filepath.Join(dir, "gate-integration-00")); err != nil {
				t.Fatal(err)
			}
			_ = os.RemoveAll(filepath.Join(dir, "alias"))
		}, needs: ok, want: 1, says: []string{"| gate-integration-00 | NOT CERTIFIED: not a part artifact |", "Gates: 6 (want 6)."}},
		{name: "a failed e2e job", needs: []string{"target=success", "integration=success", "e2e=failure", "matrix=success"}, want: 1,
			says: []string{"| job e2e | failure |"}},
		{name: "a missing row", mut: func(t *testing.T, dir, _ string) { _ = os.RemoveAll(filepath.Join(dir, "matrix-v97")) }, needs: ok, want: 1,
			says: []string{"Matrix rows: 1 (want 2)."}},
		{name: "a failed row", mut: func(t *testing.T, dir, _ string) {
			replaceIn(t, filepath.Join(dir, "matrix-v97", "v97", "verdict.txt"), "verdict: OK", "verdict: FAIL")
		}, needs: ok, want: 1, says: []string{"| matrix v97 | FAIL |"}},
		{name: "a failed gate", mut: func(t *testing.T, _, g string) { replaceIn(t, filepath.Join(g, "run.tsv"), "\tPASS\t", "\tFAIL\t") }, needs: ok, want: 1,
			says: notCertified},
		{name: "no completion marker", mut: func(t *testing.T, _, g string) { _ = os.Remove(filepath.Join(g, "complete")) }, needs: ok, want: 1,
			says: notCertified},
		{name: "a completion marker for another SHA", mut: func(t *testing.T, _, g string) {
			replaceIn(t, filepath.Join(g, "complete"), "sha="+summarySHA, "sha=fedcba9876543210fedcba9876543210fedcba98")
		}, needs: ok, want: 1, says: notCertified},
		{name: "a completion marker for another run", mut: func(t *testing.T, _, g string) {
			replaceIn(t, filepath.Join(g, "complete"), "run_id=", "run_id=x")
		}, needs: ok, want: 1, says: notCertified},
		{name: "a run of another SHA", mut: func(t *testing.T, _, g string) {
			replaceIn(t, filepath.Join(g, "run.tsv"), "\t"+summarySHA+"\t", "\tfedcba9876543210fedcba9876543210fedcba98\t")
		}, needs: ok, want: 1, says: notCertified},
		{name: "a dirty tree", mut: func(t *testing.T, _, g string) { replaceIn(t, filepath.Join(g, "run.tsv"), "\tclean\t", "\tdirty\t") }, needs: ok, want: 1,
			says: notCertified},
		{name: "a nonzero command exit", mut: func(t *testing.T, _, g string) { replaceIn(t, filepath.Join(g, "run.tsv"), "PASS\t0\t0", "PASS\t1\t0") }, needs: ok, want: 1,
			says: notCertified},
		{name: "a nonzero recording exit", mut: func(t *testing.T, _, g string) { replaceIn(t, filepath.Join(g, "run.tsv"), "PASS\t0\t0", "PASS\t0\t1") }, needs: ok, want: 1,
			says: notCertified},
		{name: "no gate summary", mut: func(t *testing.T, _, g string) { _ = os.Remove(filepath.Join(g, "summary.txt")) }, needs: ok, want: 1,
			says: notCertified},
		{name: "two run records in one part", mut: func(t *testing.T, dir, _ string) {
			summaryPart(t, filepath.Join(dir, "gate-integration-0", "again"), 0, "integration")
		},
			needs: ok, want: 1, says: notCertified},
		{name: "a failed integration job", needs: []string{"target=success", "integration=failure", "e2e=success", "matrix=success"}, want: 1,
			says: []string{"| job integration | failure |"}},
		{name: "a cancelled matrix job", needs: []string{"target=success", "integration=success", "e2e=success", "matrix=cancelled"}, want: 1, says: []string{"| job matrix | cancelled |"}},
		{name: "a skipped target job", needs: []string{"target=skipped", "integration=success", "e2e=success", "matrix=success"}, want: 1, says: []string{"| job target | skipped |"}},

		// The two workflows (RVG-1a+b): each run holds only its own jobs.
		{name: "a matrix-only run", build: "matrix", args: []string{"2", "0"}, needs: matrixOK,
			says: []string{"## Matrix verify: `0123456789ab`", "Gates: 0 (want 0). Matrix rows: 2 (want 2)."}},
		{name: "a gate-only run", build: "gate", args: []string{"0", "6"}, needs: gateOK,
			says: []string{"## Gate verify: `0123456789ab`", "Gates: 6 (want 6). Matrix rows: 0 (want 0)."}},
		{name: "a gate artifact in a matrix run", args: []string{"2", "0"}, needs: matrixOK, want: 1,
			says: []string{"## Matrix verify: ", "| gate-0 integration | NOT CERTIFIED: unexpected image |", "Gates: 0 (want 0)."}},
		{name: "a row in a gate run", args: []string{"0", "6"}, needs: gateOK, want: 1,
			says: []string{"## Gate verify: ", "Matrix rows: 2 (want 0)."}},
		// A re-run summary job downloads the previous attempt's summary artifact too (F5): its
		// name must match neither gate-* nor matrix-*.
		{name: "an earlier summary artifact", mut: func(t *testing.T, dir, _ string) {
			writeFixture(t, filepath.Join(dir, "summary-gate", "gate-verify.md"), "## Gate verify\n")
			writeFixture(t, filepath.Join(dir, "summary-matrix", "matrix-verify.md"), "## Matrix verify\n")
		}, needs: ok, says: []string{"Gates: 6 (want 6). Matrix rows: 2 (want 2)."}},

		// The counts are checked before anything is read (F1): exit 2, never a pass.
		{name: "a missing gate count", args: []string{"2"}, needs: ok, want: 2, says: []string{"usage: gh-summary.sh"}},
		// Each guard has a case that fails without it (RVG-2): with no job results $4 is unset, and
		// without $# -ge 4 bash stops on it with exit 1; without the $3 check, 0+2 reads as 2 and passes.
		{name: "a missing gate count with no job results", args: []string{"2"}, want: 2, says: []string{"usage: gh-summary.sh"}},
		{name: "a rows count that is not a number", args: []string{"0+2", "6"}, needs: ok, want: 2, says: []string{"usage: gh-summary.sh"}},
		{name: "a gate count that is not a number", args: []string{"2", "six"}, needs: ok, want: 2, says: []string{"usage: gh-summary.sh"}},
		{name: "a summary that wants nothing", args: []string{"0", "0"}, needs: ok, want: 2, says: []string{"verifies nothing"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			gate0 := ""
			if tc.build != "matrix" {
				gate0 = summaryGate(t, dir, 0)
				for n := 1; n < 6; n++ {
					summaryGate(t, dir, n)
				}
			}
			if tc.build != "gate" {
				summaryRow(t, dir, "v84", "OK")
				summaryRow(t, dir, "v97", "OK")
			}
			if tc.mut != nil {
				tc.mut(t, dir, gate0)
			}
			args := tc.args
			if args == nil {
				args = []string{"2", "6"}
			}
			cmd := exec.Command("bash", append(append([]string{script, dir, summarySHA}, args...), tc.needs...)...)
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
			for _, s := range tc.says {
				if !strings.Contains(string(out), s) {
					t.Errorf("output does not name %q:\n%s", s, out)
				}
			}
		})
	}
}
