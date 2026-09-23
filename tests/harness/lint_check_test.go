package harness

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// run_lint_check must lint through make lint, so every lint the gate runs uses the version
// the Makefile pins, and a failing make lint must still fail the call.
func TestRunLintCheckUsesMakeLint(t *testing.T) {
	libDir, err := filepath.Abs(filepath.Join("..", "e2e", "lib"))
	if err != nil {
		t.Fatal(err)
	}
	run := func(t *testing.T, makeExit string) (calls []string, root, out string, runErr error) {
		t.Helper()
		var err error
		root, err = filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		record := filepath.Join(root, "record")
		writeFixture(t, filepath.Join(root, "fakebin/make"),
			"#!/bin/bash\necho \"make $PWD $*\" >> \"$RECORD\"\nexit \"$FAKE_MAKE_EXIT\"\n")
		writeFixture(t, filepath.Join(root, "fakebin/golangci-lint"),
			"#!/bin/bash\necho \"golangci-lint $*\" >> \"$RECORD\"\nexit 0\n")
		cmd := exec.Command("/bin/bash", "-c",
			`. "$LIB/log.sh" && . "$LIB/golayer.sh" && run_lint_check`)
		cmd.Env = []string{
			"PATH=" + filepath.Join(root, "fakebin") + ":/usr/bin:/bin",
			"PROJECT_ROOT=" + root,
			"LIB=" + libDir,
			"RECORD=" + record,
			"FAKE_MAKE_EXIT=" + makeExit,
		}
		b, runErr := cmd.CombinedOutput()
		out = string(b)
		b, err = os.ReadFile(record)
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		return strings.FieldsFunc(string(b), func(r rune) bool { return r == '\n' }), root, out, runErr
	}

	t.Run("delegates", func(t *testing.T) {
		calls, root, out, runErr := run(t, "0")
		if want := []string{"make " + root + " lint"}; !reflect.DeepEqual(calls, want) {
			t.Fatalf("run_lint_check ran %v, want %v", calls, want)
		}
		if runErr != nil {
			t.Fatalf("run_lint_check failed when make lint passed: %v\n%s", runErr, out)
		}
	})

	t.Run("propagates_failure", func(t *testing.T) {
		calls, root, _, runErr := run(t, "2")
		if runErr == nil {
			t.Fatal("run_lint_check exited 0 when make lint failed")
		}
		if want := []string{"make " + root + " lint"}; !reflect.DeepEqual(calls, want) {
			t.Fatalf("run_lint_check ran %v, want %v", calls, want)
		}
	})
}
