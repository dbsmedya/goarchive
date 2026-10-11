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

// These tests run the E2E harness's shell functions (tests/e2e/lib) against the 3.0
// binary's output and tracking tables, with the database replaced by a fake
// mysql-query.sh. Each case is a pair: the passing input, and the same input with one
// fact changed, which must fail (R56).

// e2eLibs is every harness library, in run-tests.sh's order.
var e2eLibs = []string{"log", "query", "assert", "estate", "runner", "tracking", "interrupt", "replication", "golayer", "registry", "engine"}

type shellResult struct {
	stdout, stderr string
	rc             int
}

// runLib sources the harness libraries in a bash with SCRIPT_DIR at dir, then runs script.
// dir holds the fake mysql-query.sh when a test writes one.
func runLib(t *testing.T, dir, script string) shellResult {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	var src strings.Builder
	for _, l := range e2eLibs {
		src.WriteString(". \"$E2E_ROOT/tests/e2e/lib/" + l + ".sh\"\n")
	}
	src.WriteString("SCRIPT_DIR=\"$E2E_DIR\" TESTS_DIR=\"$E2E_DIR\"\n")
	src.WriteString("ARCHIVE_HOST=dest ARCHIVE_PORT=3307 ARCHIVE_USER=root ARCHIVE_DB=sakila_archive\n")
	src.WriteString("SOURCE_HOST=source SOURCE_PORT=3305 SOURCE_USER=root SOURCE_DB=sakila\n")
	src.WriteString(script)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", "-c", src.String())
	cmd.Env = append(os.Environ(), "E2E_ROOT="+root, "E2E_DIR="+dir)
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	err = cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("harness script timed out: %v\n%s", ctx.Err(), stderr.String())
	}
	rc := 0
	if e, ok := err.(*exec.ExitError); ok {
		rc = e.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	return shellResult{out.String(), stderr.String(), rc}
}

// fakeQuery writes a mysql-query.sh into dir that appends each SQL statement to dir/sql
// and answers with a header line and then the next line of dir/answers (one per call).
func fakeQuery(t *testing.T, dir string, answers ...string) {
	t.Helper()
	script := `#!/bin/bash
printf '%s\n' "$2" >> "$(dirname "$0")/sql"
n=$(( $(cat "$(dirname "$0")/calls" 2>/dev/null || echo 0) + 1 ))
echo "$n" > "$(dirname "$0")/calls"
a=$(sed -n "${n}p" "$(dirname "$0")/answers")
[[ "$a" == FAIL ]] && exit 1
[[ -z "$a" ]] && exit 0
echo value
echo "$a"
`
	writeFixture(t, filepath.Join(dir, "mysql-query.sh"), script)
	if err := os.WriteFile(filepath.Join(dir, "answers"), []byte(strings.Join(answers, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeLog(t *testing.T, dir, name, text string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// The 3.0 report of a 5-root archive of payment, verified by sha256.
const report30 = `{"level":"info","msg":"Batch 1: 5 roots of payment (archive)","job":"j"}
Job j (archive): 5 roots in 1 batches
  copied:  payment=5
  deleted: payment=5
  verified (sha256): payment=5
  elapsed: 1.500s
`

// TestE2EVerifiedLine: STEP 3c reads the report's verified line, which must name the
// declared method and cover exactly the copied rows; verify_method none needs no verified
// line at all.
func TestE2EVerifiedLine(t *testing.T) {
	for _, tc := range []struct {
		name, log, method string
		pass              bool
	}{
		{"sha256", report30, "sha256", true},
		{"other_method", report30, "count", false},
		{"no_verified_line", strings.Replace(report30, "  verified (sha256): payment=5\n", "", 1), "sha256", false},
		{"verified_fewer_rows", strings.Replace(report30, "verified (sha256): payment=5", "verified (sha256): payment=4", 1), "sha256", false},
		{"none_without_line", strings.Replace(report30, "  verified (sha256): payment=5\n", "", 1), "none", true},
		{"none_but_verified", report30, "none", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			p := writeLog(t, dir, "run.log", tc.log)
			r := runLib(t, dir, "assert_verified \""+p+"\" "+tc.method)
			if (r.rc == 0) != tc.pass {
				t.Fatalf("assert_verified %s on %s: rc=%d, want pass=%t\n%s%s", tc.method, tc.name, r.rc, tc.pass, r.stdout, r.stderr)
			}
		})
	}
}

// TestE2EPacingReadsElapsed: STEP 3d compares the floor with the report's elapsed time.
func TestE2EPacingReadsElapsed(t *testing.T) {
	for _, tc := range []struct {
		name, log, floor string
		pass             bool
	}{
		{"above_floor", report30, "1.0", true},
		{"below_floor", report30, "2.0", false},
		{"no_elapsed_line", strings.Replace(report30, "  elapsed: 1.500s\n", "", 1), "0.0", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			p := writeLog(t, dir, "run.log", tc.log)
			r := runLib(t, dir, "assert_min_duration \""+p+"\" "+tc.floor)
			if (r.rc == 0) != tc.pass {
				t.Fatalf("assert_min_duration %s: rc=%d, want pass=%t\n%s%s", tc.floor, r.rc, tc.pass, r.stdout, r.stderr)
			}
		})
	}
}

// TestE2ETrackingReads30: the tracking reads use the 3.0 log's ga_status, where copied (1)
// and deleting (4) are the statuses recovery replays.
func TestE2ETrackingReads30(t *testing.T) {
	dir := t.TempDir()
	fakeQuery(t, dir, "7", "3", "0")
	r := runLib(t, dir, `tracking_job_id payment-job && tracking_nonterminal_count 7 && tracking_status_count 7 2`)
	if r.rc != 0 || r.stdout != "7\n3\n0\n" {
		t.Fatalf("tracking reads: rc=%d stdout=%q\n%s", r.rc, r.stdout, r.stderr)
	}
	sql := readFixture(t, filepath.Join(dir, "sql"))
	for _, w := range []string{"WHERE job_name = 'payment-job'", "archiver_job_log_7 WHERE ga_status IN (1, 4)", "archiver_job_log_7 WHERE ga_status = 2"} {
		if !strings.Contains(sql, w) {
			t.Errorf("tracking SQL lacks %q:\n%s", w, sql)
		}
	}
	if strings.Contains(sql, "log_status") || strings.Contains(sql, "last_processed_root_pk_id") {
		t.Errorf("tracking SQL reads 2.x columns:\n%s", sql)
	}
}

// TestE2ECompositeOrphans: an orphan check names composite columns joined by "+", paired
// by position.
func TestE2ECompositeOrphans(t *testing.T) {
	dir := t.TempDir()
	fakeQuery(t, dir, "0")
	r := runLib(t, dir, `count_orphans dest 3307 root sakila_archive km_item order_id+line_no km_line order_id+line_no`)
	if r.rc != 0 || r.stdout != "0\n" {
		t.Fatalf("count_orphans: rc=%d stdout=%q\n%s", r.rc, r.stdout, r.stderr)
	}
	sql := readFixture(t, filepath.Join(dir, "sql"))
	if !strings.Contains(sql, "ON c.`order_id` = p.`order_id` AND c.`line_no` = p.`line_no`") ||
		!strings.Contains(sql, "WHERE c.`order_id` IS NOT NULL AND c.`line_no` IS NOT NULL AND p.`order_id` IS NULL") {
		t.Fatalf("composite orphan SQL:\n%s", sql)
	}
	// A column count that differs between the two sides is refused, not truncated.
	if r := runLib(t, dir, `count_orphans dest 3307 root sakila_archive km_item order_id km_line order_id+line_no`); r.rc == 0 {
		t.Fatalf("mismatched column lists were accepted: %q", r.stdout)
	}
}

// TestE2EGracefulMarker: the graceful interrupt waits for the engine's "Batch N:" line,
// and batch 1 is not batch 10.
func TestE2EGracefulMarker(t *testing.T) {
	dir := t.TempDir()
	p := writeLog(t, dir, "run.log", `{"level":"info","msg":"Batch 10: 100 roots of payment (archive)"}`+"\n")
	script := `pat=$(interrupt_marker_pattern graceful 1) || exit 9
grep -q "$pat" "` + p + `" && exit 1
echo '{"level":"info","msg":"Batch 1: 100 roots of payment (archive)"}' >> "` + p + `"
grep -q "$pat" "` + p + `" || exit 2
`
	if r := runLib(t, dir, script); r.rc != 0 {
		t.Fatalf("graceful marker: rc=%d\n%s%s", r.rc, r.stdout, r.stderr)
	}
}

// TestE2ECrashWaitsForCopiedRows: the crash interrupt fires once the log holds the declared
// copied roots, polled from the destination; the 3.0 engine logs nothing between the
// copy's commit and the deletes. A run that exits first fails the wait.
func TestE2ECrashWaitsForCopiedRows(t *testing.T) {
	dir := t.TempDir()
	// No job row yet, then the job id, then copied 0, completed 0, then id, copied 100, completed 0.
	fakeQuery(t, dir, "", "7", "0", "0", "7", "100", "0")
	script := `sleep 30 & pid=$!
INTERRUPT_POLL_SECONDS=0
_interrupt_wait_for_copied payment-job 100 "$pid"; rc=$?
kill "$pid"
exit $rc`
	if r := runLib(t, dir, script); r.rc != 0 {
		t.Fatalf("wait for copied rows: rc=%d\n%s%s", r.rc, r.stdout, r.stderr)
	}
	dir = t.TempDir()
	fakeQuery(t, dir, "7", "0", "0", "7", "0", "0", "7", "0", "0")
	script = `( exit 0 ) & pid=$!; wait "$pid"
INTERRUPT_POLL_SECONDS=0
_interrupt_wait_for_copied payment-job 100 "$pid"`
	if r := runLib(t, dir, script); r.rc == 0 || !strings.Contains(r.stdout+r.stderr, "EXITED") {
		t.Fatalf("a run that exited first passed the wait: rc=%d\n%s%s", r.rc, r.stdout, r.stderr)
	}
}

// fakeTracking writes a mysql-query.sh into dir that answers the crash preconditions'
// reads by their SQL, so the answers do not depend on the order of the reads.
func fakeTracking(t *testing.T, dir string, dest, copied, completed, deleting, status int) {
	t.Helper()
	script := fmt.Sprintf(`#!/bin/bash
echo value
case "$2" in
  *"SELECT id FROM"*) echo 7 ;;
  *"SELECT job_status"*) echo %d ;;
  *"ga_status IN (1, 4)"*) echo %d ;;
  *"ga_status = 1;"*) echo %d ;;
  *"ga_status = 2;"*) echo %d ;;
  *"ga_status = 4;"*) echo %d ;;
  *"COUNT(*) FROM "*"payment"*) echo %d ;;
  *) exit 1 ;;
esac
`, status, copied+deleting, copied, completed, deleting, dest)
	writeFixture(t, filepath.Join(dir, "mysql-query.sh"), script)
}

// TestE2ECrashPreconditions: a crash after batch N leaves the earlier batches completed and
// only the last batch at 'copied'. The destination holds every copied root, so it is
// checked against copied + completed, and only the 'copied' roots are left for recovery,
// which the preconditions hand to the run-2 accounting as INTERRUPT_PENDING.
func TestE2ECrashPreconditions(t *testing.T) {
	for _, tc := range []struct {
		name                                      string
		expect, dest, copied, completed, deleting int
		status                                    int
		pending                                   string // "" when the preconditions must refuse
	}{
		{"batch1", 100, 100, 100, 0, 0, 1, "100"},
		{"batch2", 200, 200, 100, 100, 0, 1, "100"},
		{"batch_completed", 200, 200, 0, 200, 0, 1, ""},
		{"log_short_of_destination", 200, 200, 100, 50, 0, 1, ""},
		{"deleting", 200, 200, 100, 100, 3, 1, ""},
		{"exited_normally", 200, 200, 100, 100, 0, 0, ""},
		{"destination_late", 200, 300, 100, 200, 0, 1, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			fakeTracking(t, dir, tc.dest, tc.copied, tc.completed, tc.deleting, tc.status)
			r := runLib(t, dir, fmt.Sprintf(`assert_interrupt_preconditions crash payment-job payment %d /dev/null || exit 1
echo "pending=$INTERRUPT_PENDING"`, tc.expect))
			if tc.pending == "" {
				if r.rc == 0 {
					t.Fatalf("preconditions passed: %s%s", r.stdout, r.stderr)
				}
				// The refusal must be the assertion's, not a read that failed.
				if strings.Contains(r.stdout+r.stderr, "tracking:") || strings.Contains(r.stdout+r.stderr, "could not count") {
					t.Fatalf("preconditions refused on a failed read:\n%s%s", r.stdout, r.stderr)
				}
				return
			}
			if r.rc != 0 || !strings.Contains(r.stdout, "pending="+tc.pending+"\n") {
				t.Fatalf("preconditions: rc=%d, want pending=%s\n%s%s", r.rc, tc.pending, r.stdout, r.stderr)
			}
		})
	}
}

// TestE2EResumeAccounting: run 2 of a crash test reports the recovery of exactly the
// roots left at 'copied' and copies only the rest; a graceful run 2 recovers nothing.
// After a crash in batch 2 the destination already held two batches, but only the
// second was left for recovery.
func TestE2EResumeAccounting(t *testing.T) {
	crash := `{"level":"info","msg":"Recovery: replaying 100 copied roots of payment"}
Job j (archive): 399 roots in 4 batches
  recovered: 100 logged roots finished by replay
  copied:  payment=399
`
	crash2 := strings.Replace(crash, "payment=399", "payment=299", 1)
	graceful := "Job j (archive): 1899 roots in 19 batches\n  copied:  payment=1899\n"
	for _, tc := range []struct {
		name, kind, log    string
		total, at, pending string
		pass               bool
	}{
		{"crash", "crash", crash, "499", "100", "100", true},
		{"crash_recopied", "crash", strings.Replace(crash, "payment=399", "payment=499", 1), "499", "100", "100", false},
		{"crash_no_recovery", "crash", strings.Replace(crash, "Recovery: replaying 100", "Recovery: replaying 80", 1), "499", "100", "100", false},
		{"crash_batch2", "crash", crash2, "499", "200", "100", true},
		{"crash_batch2_cumulative", "crash", strings.NewReplacer("replaying 100", "replaying 200", "recovered: 100", "recovered: 200").Replace(crash2), "499", "200", "100", false},
		{"crash_without_pending", "crash", crash, "499", "100", "", false},
		{"graceful", "graceful", graceful, "1999", "100", "0", true},
		{"graceful_recovered", "graceful", crash, "499", "100", "0", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			p := writeLog(t, dir, "run2.log", tc.log)
			r := runLib(t, dir, "assert_resume_copy_accounting "+tc.kind+" \""+p+"\" payment "+tc.total+" "+tc.at+" "+tc.pending)
			if (r.rc == 0) != tc.pass {
				t.Fatalf("accounting %s: rc=%d, want pass=%t\n%s%s", tc.name, r.rc, tc.pass, r.stdout, r.stderr)
			}
		})
	}
}

// TestE2EGoLayerPackages: the gate's integration layer runs the 3.0 packages, as
// make test-integration does; internal/archiver is gone.
func TestE2EGoLayerPackages(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, filepath.Join(dir, "bin/go"), "#!/bin/bash\nprintf '%s\\n' \"$*\" >> \""+filepath.Join(dir, "go-args")+"\"\necho '--- PASS: TestX'\n")
	r := runLib(t, dir, `PATH="$E2E_DIR/bin:$PATH" PROJECT_ROOT="$E2E_DIR" GO_TEST_ARGS="" VERBOSE=""
run_integration_tests`)
	if r.rc != 0 {
		t.Fatalf("run_integration_tests: rc=%d\n%s%s", r.rc, r.stdout, r.stderr)
	}
	if got := readFixture(t, filepath.Join(dir, "go-args")); !strings.Contains(got, "-tags=integration") ||
		!strings.Contains(got, "./internal/... ./cmd/...") || strings.Contains(got, "archiver") {
		t.Fatalf("integration layer ran %q", got)
	}
}
