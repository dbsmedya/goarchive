package harness

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// callsParallel reports whether src calls the testing package's Parallel method. The match is textual, so a
// comment that spells the call counts too, which keeps the rule simple to hold to.
func callsParallel(src string) bool {
	return strings.Contains(src, ".Parallel()")
}

// TestIntegrationTestsDoNotRunInParallel: the integration suites share the estate's servers with each other, and
// some of them change a server's state for the length of the test (a stopped applier, a delayed or read-only
// replica, a switched primary, a lowered size limit, tests/README.md). A test that ran beside another would see
// that state, so no integration test calls t.Parallel. Packages already run side by side, which the replica's
// lock (internal/testestate) and, on the group estate, one-package-at-a-time (-p 1) answer.
func TestIntegrationTestsDoNotRunInParallel(t *testing.T) {
	for name, want := range map[string]bool{
		"t.Parallel()":                true,
		"  sub.Parallel()":            true,
		"t.Run(\"x\", func() {})":     false,
		"// parallel is not a call":   false,
		"t.Parallel (not a call yet)": false,
	} {
		if got := callsParallel(name); got != want {
			t.Errorf("callsParallel(%q) = %v, want %v", name, got, want)
		}
	}

	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	list, err := exec.Command("git", "-C", root, "ls-files", "*_integration_test.go").Output()
	if err != nil {
		t.Fatalf("git ls-files: %v", err)
	}
	files := strings.Fields(string(list))
	if len(files) == 0 {
		t.Fatal("no tracked integration test files")
	}
	for _, f := range files {
		body, err := os.ReadFile(filepath.Join(root, f))
		if err != nil {
			t.Fatal(err)
		}
		if callsParallel(string(body)) {
			t.Errorf("%s calls t.Parallel: integration tests share servers and change their state, so none runs beside another", f)
		}
	}
}
