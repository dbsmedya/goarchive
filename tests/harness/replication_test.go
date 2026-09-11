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

// The real engine runs against simulated estate functions. A reset marker is
// observable even when invalid declarations would otherwise fail later.
func TestHarnessReplicationDeclaration(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name, mode, command, hold, interrupt string
		valid                                bool
	}{
		{"unknown-hold", "working", "purge", "unknown", "", false},
		{"non-purge", "working", "archive", "stopped-applier", "", false},
		{"interrupt-conflict", "working", "purge", "stopped-applier", "graceful", false},
		{"non-working", "example", "purge", "stopped-applier", "", false},
		{"ordinary-working-control", "working", "purge", "", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			decl := fmt.Sprintf("test_name=TestDeclaration\nmode=%s\ncommand=%s\nreplication_hold=%s\ninterrupt=%s\nconfig_file=test.yaml\ntables=payment\nverify_method=none\nmin_duration=0.0\nexpected_rows=7\ninterrupt_after_batches=1\ninterrupt_expect_dest=1\nroot_pk=payment_id\n", tc.mode, tc.command, tc.hold, tc.interrupt)
			if err := os.WriteFile(filepath.Join(dir, "declaration.sh"), []byte(decl), 0600); err != nil {
				t.Fatal(err)
			}
			script := `source "$REPLICATION_TEST_ROOT/tests/e2e/lib/engine.sh"
TESTS_DIR="$REPLICATION_TEST_DIR"
SOURCE_HOST=source SOURCE_PORT=3305 SOURCE_USER=root SOURCE_DB=sakila
ARCHIVE_HOST=dest ARCHIVE_PORT=3307 ARCHIVE_USER=root ARCHIVE_DB=sakila_archive
registry_file_for() { printf '%s\n' "$TESTS_DIR/declaration.sh"; }
registry_category_for_file() { echo purge; }
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
run_interrupted_job() { return 1; }
ensure_goarchive_bin() { return 1; }
assert_min_duration() { return 0; }
assert_postcondition() { [ "$3" = 10 ] && [ "$4" = 3 ] && [ "$5" = 0 ] && [ "$6" = 7 ]; }
run_e2e_test 13
rc=$?
if [ -n "${replication_hold:-}" ]; then echo HARNESS_REPLICATION_VARIABLE_LEAK >&2; exit 91; fi
exit "$rc"
`
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", "-c", script)
			cmd.Env = append(os.Environ(), "REPLICATION_TEST_ROOT="+root, "REPLICATION_TEST_DIR="+dir)
			var out, stderr bytes.Buffer
			cmd.Stdout = &out
			cmd.Stderr = &stderr
			err := cmd.Run()
			if ctx.Err() != nil {
				t.Fatalf("declaration harness timed out: %v", ctx.Err())
			}
			_, resetErr := os.Stat(filepath.Join(dir, "reset"))
			if tc.valid {
				if err != nil || resetErr != nil {
					t.Fatalf("ordinary working declaration failed: %v; %s", err, stderr.String())
				}
			} else {
				if err == nil || !os.IsNotExist(resetErr) {
					t.Fatalf("HARNESS_REPLICATION_DECLARATION_ACCEPTED: %s reached estate reset", tc.name)
				}
				if !strings.Contains(stderr.String(), "replication_hold") {
					t.Fatalf("wrong refusal: %s", stderr.String())
				}
			}
			if strings.Contains(stderr.String(), "HARNESS_REPLICATION_VARIABLE_LEAK") {
				t.Fatal(stderr.String())
			}
		})
	}
}
