package harness

import (
	"errors"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// actionlintVersion pins actionlint, run through go run like make lint runs golangci-lint.
const actionlintVersion = "v1.7.12"

// actionlint runs the pinned actionlint from dir on files and returns its output and exit
// code. A failure to run it at all (no module proxy and no cache) fails the test; it is never
// a skip.
func actionlint(t *testing.T, dir string, files ...string) (out []byte, code int) {
	t.Helper()
	args := append([]string{"run", "github.com/rhysd/actionlint/cmd/actionlint@" + actionlintVersion,
		"-shellcheck=", "-pyflakes=", "-no-color"}, files...)
	cmd := exec.Command("go", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if ee, ok := errors.AsType[*exec.ExitError](err); ok {
		return out, ee.ExitCode()
	} else if err != nil {
		t.Fatalf("go run actionlint: %v\n%s", err, out)
	}
	return out, 0
}

// TestWorkflowsPassActionlint (RVG-2): make check lints .github/workflows through this test,
// which test-unit runs, so the lint needs no make check stage of its own (one would have to be
// a gate stage too, TestMakeCheckMatchesGate). ShellCheck and pyflakes are off: actionlint runs
// them when they are on PATH, which would make the result depend on the machine.
func TestWorkflowsPassActionlint(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("a broken workflow fails", func(t *testing.T) {
		dir := t.TempDir()
		writeFixture(t, filepath.Join(dir, "broken.yml"), "name: broken\non: push\njobs:\n  x:\n    runs-on: ubuntu-latest\n"+
			"    steps:\n      - run: echo \"${{ github.no_such_property }}\"\n")
		out, code := actionlint(t, root, filepath.Join(dir, "broken.yml"))
		if code != 1 || !strings.Contains(string(out), `property "no_such_property" is not defined`) {
			t.Errorf("exit %d, want 1 naming the undefined property:\n%s", code, out)
		}
	})

	t.Run("the repository's workflows", func(t *testing.T) {
		list, err := exec.Command("git", "-C", root, "ls-files", ".github/workflows/*.yml").Output()
		if err != nil {
			t.Fatalf("git ls-files: %v", err)
		}
		files := strings.Fields(string(list))
		if len(files) == 0 {
			t.Fatal("no tracked workflows")
		}
		if out, code := actionlint(t, root, files...); code != 0 {
			t.Errorf("actionlint exit %d:\n%s", code, out)
		}
	})
}
