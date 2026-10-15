package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/engine"
)

// TestExitCode: each outcome's code and their precedence (spec §3.4).
func TestExitCode(t *testing.T) {
	defer func() { started = false }()
	for _, tc := range []struct {
		name    string
		started bool
		err     error
		want    int
	}{
		{"ok", true, nil, 0},
		{"stopped", true, errStopped, 3},
		{"job running", true, fmt.Errorf("x: %w", &engine.JobRunningError{Job: "j"}), 4},
		{"job running, holder known", true, &engine.JobRunningError{Job: "j", Holder: 7}, 4},
		{"table locked", true, fmt.Errorf("x: %w", &engine.TableLockedError{Table: "t"}), 4},
		{"overlap", true, &engine.OverlapError{Job: "j"}, 4},
		{"usage", true, usage(errors.New("bad")), 2},
		{"cobra before RunE", false, errors.New("unknown flag: --x"), 2},
		{"contention beats usage", true, usage(&engine.JobRunningError{Job: "j"}), 4},
		{"other", true, errors.New("connect: refused"), 1},
		{"wrapped file error after start", true, fmt.Errorf("x: %w", os.ErrNotExist), 1},
		{"reported failure", true, &reportedError{errors.New("boom")}, 1},
	} {
		started = tc.started
		if got := exitCode(tc.err); got != tc.want {
			t.Errorf("%s: exitCode = %d, want %d", tc.name, got, tc.want)
		}
	}
}

// mainStreams runs Main in process with the two streams kept apart.
func mainStreams(t *testing.T, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	resetFlags(rootCmd)
	defer func() {
		resetFlags(rootCmd)
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
		rootCmd.SetArgs(nil)
	}()
	var out, errb bytes.Buffer
	code = Main(args, &out, &errb)
	return out.String(), errb.String(), code
}

// TestMainStreams: usage errors exit 2 with exactly one Error: line on stderr and nothing on
// stdout; a refused connection exits 1; no arguments prints help and exits 0.
func TestMainStreams(t *testing.T) {
	cfg := writeConfig(t, unreachable)
	for _, tc := range []struct {
		args []string
		want int
	}{
		{[]string{"archive", "--nope"}, 2},
		{[]string{"archive"}, 2},
		{[]string{"bogus"}, 2},
		{[]string{"archive", "-c", "/nonexistent/goarchive.yaml", "--job", "x"}, 2},
		{[]string{"purge", "-c", cfg, "--job", "nojob"}, 2},
		{[]string{"purge", "-c", cfg, "--job", "purge_old", "--progress=500ms"}, 2},
		{[]string{"copy-only", "-c", cfg, "--job", "copy_old", "--verification=md5"}, 2},
		{[]string{"validate", "-c", cfg}, 1},
	} {
		out, errOut, code := mainStreams(t, tc.args...)
		if code != tc.want || out != "" || strings.Count(errOut, "Error:") != 1 {
			t.Errorf("%v: code %d (want %d), stdout %q, stderr %q", tc.args, code, tc.want, out, errOut)
		}
	}
	out, errOut, code := mainStreams(t)
	if code != 0 || !strings.Contains(out, "Usage:") || errOut != "" {
		t.Errorf("no arguments: code %d, stdout %q, stderr %q; want help on stdout and 0", code, out, errOut)
	}
}
