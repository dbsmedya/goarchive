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

// The 3.0 log of a 5-root archive of payment, verified by sha256: a batch entry and the run's
// closing entry (logOutcome), which the harness reads in place of the human report.
const report30 = `{"level":"info","msg":"Batch 1: 5 roots of payment (archive)","job":"j"}
{"level":"info","msg":"Run completed","job":"j","job_type":"archive","roots":5,"batches":1,"copied":{"payment":5},"deleted":{"payment":5},"verified":{"payment":5},"verification":"sha256","elapsed_seconds":1.5}
`

// dryRunOnly is validate's and dry-run's output: no closing entry, so no run to read.
const dryRunOnly = `{"level":"info","msg":"Statement cap: source by key on payment(payment_id): 16000 key tuples per statement","job":"j"}
Dry-run of j (archive). No data is changed.
`

// requireJQ: the harness reads the closing entry with jq (tests/README.md, Prerequisites).
func requireJQ(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("jq"); err != nil {
		t.Fatal("jq is a prerequisite of the E2E harness (tests/README.md, Prerequisites)")
	}
}

// TestE2EVerifiedLine: STEP 3c reads the closing entry's verified counts, which must name the
// declared method and cover exactly the copied rows; verify_method none needs no verified
// counts at all. Only the job's single closing entry counts.
func TestE2EVerifiedLine(t *testing.T) {
	requireJQ(t)
	noVerified := strings.Replace(report30, `"verified":{"payment":5},`, "", 1)
	for _, tc := range []struct {
		name, log, method string
		pass              bool
	}{
		{"sha256", report30, "sha256", true},
		{"other_method", report30, "count", false},
		{"no_verified_counts", noVerified, "sha256", false},
		{"verified_fewer_rows", strings.Replace(report30, `"verified":{"payment":5}`, `"verified":{"payment":4}`, 1), "sha256", false},
		{"none_without_counts", noVerified, "none", true},
		{"none_but_verified", report30, "none", false},
		{"two_closing_entries", report30 + report30, "sha256", false},
		{"another_jobs_entry", strings.Replace(report30, `"msg":"Run completed","job":"j"`, `"msg":"Run completed","job":"k"`, 1), "sha256", false},
		// Codex MR R3, R4: malformed input fails closed; counts are tables of nonnegative
		// integers, compared as objects.
		{"malformed_after_valid", report30 + "{broken json\n", "sha256", false},
		{"malformed_before_valid", "{broken json\n" + report30, "sha256", false},
		{"msg_not_a_string", `{"level":"info","msg":5,"job":"j"}` + "\n" + report30, "sha256", false},
		{"negative_counts", strings.NewReplacer(`"copied":{"payment":5}`, `"copied":{"payment":-5}`, `"verified":{"payment":5}`, `"verified":{"payment":-5}`).Replace(report30), "sha256", false},
		{"string_counts", strings.NewReplacer(`"copied":{"payment":5}`, `"copied":{"payment":"5"}`, `"verified":{"payment":5}`, `"verified":{"payment":"5"}`).Replace(report30), "sha256", false},
		{"counts_not_objects", strings.NewReplacer(`"copied":{"payment":5}`, `"copied":[5]`, `"verified":{"payment":5}`, `"verified":[5]`).Replace(report30), "sha256", false},
		{"reordered_equal_objects", strings.NewReplacer(`"copied":{"payment":5}`, `"copied":{"payment":5,"rental":2}`, `"verified":{"payment":5}`, `"verified":{"rental":2,"payment":5}`).Replace(report30), "sha256", true},
		{"none_with_malformed_verified", strings.Replace(report30, `"verified":{"payment":5},`, `"verified":"x",`, 1), "none", false},
		// Codex MR re-review R4: a present false or null is not an absent field.
		{"none_with_verified_false", strings.Replace(report30, `"verified":{"payment":5},`, `"verified":false,`, 1), "none", false},
		{"none_with_verified_null", strings.Replace(report30, `"verified":{"payment":5},`, `"verified":null,`, 1), "none", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			p := writeLog(t, dir, "run.log", tc.log)
			r := runLib(t, dir, "assert_verified \""+p+"\" "+tc.method+" j")
			if (r.rc == 0) != tc.pass {
				t.Fatalf("assert_verified %s on %s: rc=%d, want pass=%t\n%s%s", tc.method, tc.name, r.rc, tc.pass, r.stdout, r.stderr)
			}
		})
	}
}

// TestE2EPacingReadsElapsed: STEP 3d compares the floor with the closing entry's
// elapsed_seconds, the run's own wall time; a missing or malformed one fails closed.
func TestE2EPacingReadsElapsed(t *testing.T) {
	requireJQ(t)
	for _, tc := range []struct {
		name, log, floor string
		pass             bool
	}{
		{"above_floor", report30, "1.0", true},
		{"below_floor", report30, "2.0", false},
		{"no_elapsed", strings.Replace(report30, `,"elapsed_seconds":1.5`, "", 1), "0.0", false},
		{"elapsed_not_a_number", strings.Replace(report30, `"elapsed_seconds":1.5`, `"elapsed_seconds":"x"`, 1), "0.0", false},
		{"elapsed_negative", strings.Replace(report30, `"elapsed_seconds":1.5`, `"elapsed_seconds":-1`, 1), "0.0", false},
		{"validate_and_dry_run_only", dryRunOnly, "0.0", false},
		{"malformed_after_valid", report30 + "{broken json\n", "0.0", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			p := writeLog(t, dir, "run.log", tc.log)
			r := runLib(t, dir, "assert_min_duration \""+p+"\" "+tc.floor+" j")
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

// TestE2EResumeAccounting: run 2 of a crash test recovers exactly the roots left at
// 'copied', by the Recovery entry and by the closing entry's recovered count, and copies only
// the rest; a graceful run 2 recovers nothing. After a crash in batch 2 the destination
// already held two batches, but only the second was left for recovery.
func TestE2EResumeAccounting(t *testing.T) {
	requireJQ(t)
	crash := `{"level":"info","msg":"Recovery: replaying 100 copied roots of payment","job":"j"}
{"level":"info","msg":"Run completed","job":"j","job_type":"archive","roots":399,"batches":4,"recovered":100,"copied":{"payment":399},"elapsed_seconds":2}
`
	crash2 := strings.Replace(crash, `"payment":399`, `"payment":299`, 1)
	graceful := `{"level":"info","msg":"Run completed","job":"j","job_type":"archive","roots":1899,"batches":19,"copied":{"payment":1899},"elapsed_seconds":9}` + "\n"
	for _, tc := range []struct {
		name, kind, log    string
		total, at, pending string
		pass               bool
	}{
		{"crash", "crash", crash, "499", "100", "100", true},
		{"crash_recopied", "crash", strings.Replace(crash, `"payment":399`, `"payment":499`, 1), "499", "100", "100", false},
		{"crash_no_recovery", "crash", strings.Replace(crash, "Recovery: replaying 100", "Recovery: replaying 80", 1), "499", "100", "100", false},
		{"crash_recovered_field_mismatch", "crash", strings.Replace(crash, `"recovered":100`, `"recovered":80`, 1), "499", "100", "100", false},
		{"crash_batch2", "crash", crash2, "499", "200", "100", true},
		{"crash_batch2_cumulative", "crash", strings.NewReplacer("replaying 100", "replaying 200", `"recovered":100`, `"recovered":200`).Replace(crash2), "499", "200", "100", false},
		{"crash_without_pending", "crash", crash, "499", "100", "", false},
		{"graceful", "graceful", graceful, "1999", "100", "0", true},
		{"graceful_recovered", "graceful", crash, "499", "100", "0", false},
		{"crash_recovered_not_a_number", "crash", strings.Replace(crash, `"recovered":100`, `"recovered":"100"`, 1), "499", "100", "100", false},
		{"graceful_recovered_negative", "graceful", strings.Replace(graceful, `"batches":19,`, `"batches":19,"recovered":-1,`, 1), "1999", "100", "0", false},
		{"crash_copied_not_an_object", "crash", strings.Replace(crash, `"copied":{"payment":399}`, `"copied":399`, 1), "499", "100", "100", false},
		{"graceful_recovered_false", "graceful", strings.Replace(graceful, `"batches":19,`, `"batches":19,"recovered":false,`, 1), "1999", "100", "0", false},
		{"graceful_recovered_null", "graceful", strings.Replace(graceful, `"batches":19,`, `"batches":19,"recovered":null,`, 1), "1999", "100", "0", false},
		{"graceful_recovered_zero", "graceful", strings.Replace(graceful, `"batches":19,`, `"batches":19,"recovered":0,`, 1), "1999", "100", "0", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			p := writeLog(t, dir, "run2.log", tc.log)
			r := runLib(t, dir, "assert_resume_copy_accounting "+tc.kind+" \""+p+"\" payment "+tc.total+" "+tc.at+" "+tc.pending+" j")
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

// TestE2ESourceGTIDSet: Test13's replica check reads the source's gtid_executed on one line.
// A server separates the uuids of a set with a newline (probe L11), and mysql-query.sh prints
// a value as it gets it, so the fake answers with the newline unless the statement removes
// it on the server; a set with one uuid has none.
func TestE2ESourceGTIDSet(t *testing.T) {
	const a, b = "7fcbfd08-c636-11f1-9db5-5ee215af872f:1-1657", "f11ed000-57a1-4c0d-9e5e-000000000001:1"
	for _, tc := range []struct{ name, set, want string }{
		{"one uuid", a, a},
		{"two uuids", a + ",\n" + b, a + "," + b},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeFixture(t, filepath.Join(dir, "mysql-query.sh"), `#!/bin/bash
set=$(printf '`+strings.ReplaceAll(tc.set, "\n", `\n`)+`')
echo value
if [[ "$2" == *"REPLACE(@@GLOBAL.gtid_executed, CHAR(10), '')"* ]]; then
  printf '%s\n' "${set//$'\n'/}"
else
  printf '%s\n' "$set"
fi
`)
			r := runLib(t, dir, "source_gtid_set")
			if r.rc != 0 || strings.TrimSpace(r.stdout) != tc.want {
				t.Errorf("source_gtid_set = %q (rc %d, stderr %q), want %q", r.stdout, r.rc, r.stderr, tc.want)
			}
		})
	}
}
