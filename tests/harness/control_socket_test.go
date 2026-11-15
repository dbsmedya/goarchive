package harness

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestHarnessControlSocketDeclaration runs the real engine against simulated estate functions, as the
// replication declaration's test does. `control_socket` selects the multi-run procedure of
// lib/control.sh, and a declaration that cannot mean that must be refused before any fixture reset,
// never degraded to an ordinary working test: an unknown value, a command other than archive, a
// resume declaration beside it, and an example-mode test. A valid declaration reaches the
// procedure with the config, the test log and the file the final run's output goes to, and the
// assertions that read one run's closing entry read that file.
func TestHarnessControlSocketDeclaration(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name, mode, command, control, interrupt string
		valid                                   bool
	}{
		{"unknown-value", "working", "archive", "unknown", "", false},
		{"purge", "working", "purge", "driven", "", false},
		{"copy-only", "working", "copy-only", "driven", "", false},
		{"interrupt-conflict", "working", "archive", "driven", "graceful", false},
		{"non-working", "example", "archive", "driven", "", false},
		{"declared", "working", "archive", "driven", "", true},
		{"ordinary-working-control", "working", "archive", "", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			decl := fmt.Sprintf("test_name=TestDeclaration\nmode=%s\ncommand=%s\ncontrol_socket=%s\ninterrupt=%s\nconfig_file=test.yaml\ntables=payment\nverify_method=none\nmin_duration=0.0\nexpected_rows=7\ninterrupt_after_batches=1\ninterrupt_expect_dest=1\n",
				tc.mode, tc.command, tc.control, tc.interrupt)
			if err := os.WriteFile(filepath.Join(dir, "declaration.sh"), []byte(decl), 0o600); err != nil {
				t.Fatal(err)
			}
			script := `source "$CONTROL_TEST_ROOT/tests/e2e/lib/engine.sh"
TESTS_DIR="$CONTROL_TEST_DIR"
SOURCE_HOST=source SOURCE_PORT=3305 SOURCE_USER=root SOURCE_DB=sakila
ARCHIVE_HOST=dest ARCHIVE_PORT=3307 ARCHIVE_USER=root ARCHIVE_DB=sakila_archive
registry_file_for() { printf '%s\n' "$TESTS_DIR/declaration.sh"; }
registry_category_for_file() { echo archive; }
log_error() { printf '%s\n' "$*" >&2; }
log_info() { :; }
log_header() { :; }
reset_source_database() { touch "$TESTS_DIR/reset"; }
get_row_count() {
    if [ "$1" = dest ]; then echo 0
    elif [ -f "$TESTS_DIR/executed" ]; then echo 3
    else echo 10; fi
}
ensure_destination_schema() { :; }
run_archive_job() { touch "$TESTS_DIR/executed"; }
run_replication_hold_job() { touch "$TESTS_DIR/executed"; }
run_control_socket_job() { touch "$TESTS_DIR/executed"; touch "$TESTS_DIR/control"; printf '%s\n' "$@" > "$TESTS_DIR/control.args"; }
run_interrupted_job() { return 1; }
ensure_goarchive_bin() { return 1; }
assert_min_duration() { return 0; }
assert_tracking_settled() { return 0; }
assert_verified() { printf '%s\n' "$1" > "$TESTS_DIR/verified.log"; return 0; }
extract_job_name() { echo declaration-job; }
assert_postcondition() { [ "$3" = 10 ] && [ "$4" = 3 ] && [ "$5" = 0 ] && [ "$6" = 7 ]; }
run_e2e_test 20
rc=$?
if [ -n "${control_socket:-}" ]; then echo HARNESS_CONTROL_VARIABLE_LEAK >&2; exit 91; fi
exit "$rc"
`
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", "-c", script)
			cmd.Env = append(os.Environ(), "CONTROL_TEST_ROOT="+root, "CONTROL_TEST_DIR="+dir)
			var out, stderr bytes.Buffer
			cmd.Stdout = &out
			cmd.Stderr = &stderr
			err := cmd.Run()
			if ctx.Err() != nil {
				t.Fatalf("declaration harness timed out: %v", ctx.Err())
			}
			_, resetErr := os.Stat(filepath.Join(dir, "reset"))
			_, controlErr := os.Stat(filepath.Join(dir, "control"))
			if strings.Contains(stderr.String(), "HARNESS_CONTROL_VARIABLE_LEAK") {
				t.Fatal(stderr.String())
			}
			if !tc.valid {
				if err == nil || !os.IsNotExist(resetErr) {
					t.Fatalf("HARNESS_CONTROL_DECLARATION_ACCEPTED: %s reached estate reset", tc.name)
				}
				if !strings.Contains(stderr.String(), "control_socket") {
					t.Fatalf("wrong refusal: %s", stderr.String())
				}
				return
			}
			if err != nil || resetErr != nil {
				t.Fatalf("working declaration failed: %v; %s", err, stderr.String())
			}
			if tc.control == "" {
				if controlErr == nil {
					t.Fatal("an ordinary working test ran the control socket procedure")
				}
				return
			}
			if controlErr != nil {
				t.Fatalf("a declared control_socket never reached its procedure: %v", controlErr)
			}
			args, _ := os.ReadFile(filepath.Join(dir, "control.args"))
			got := strings.Split(strings.TrimSpace(string(args)), "\n")
			wantLog := filepath.Join(dir, "results", "test_20.log")
			wantFinal := filepath.Join(dir, "results", "test_20.final.log")
			if len(got) != 3 || got[0] != "test.yaml" || got[1] != wantLog || got[2] != wantFinal {
				t.Errorf("the procedure was called with %q, want the config, %s and %s", got, wantLog, wantFinal)
			}
			verified, _ := os.ReadFile(filepath.Join(dir, "verified.log"))
			if strings.TrimSpace(string(verified)) != wantFinal {
				t.Errorf("the closing entry was read from %q, want the final run's own file %s", strings.TrimSpace(string(verified)), wantFinal)
			}
		})
	}
}
