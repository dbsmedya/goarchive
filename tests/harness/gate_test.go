package harness

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

var gateStages = []string{"estate", "fmt-check", "vet", "lint", "consumer-policy", "deadcode", "unit", "integration", "characterization", "e2e", "e2e-examples"}

func writeFixture(t *testing.T, path, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0o755); err != nil {
		t.Fatal(err)
	}
}

// The subject is copied unchanged. Only the synthetic repository's commands and
// PATH are controlled, and no ambient credentials or shell startup files leak in.
func gateFixture(t *testing.T, failingStage, teeExit string) (string, []string) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	src, err := os.ReadFile(filepath.Join("..", "scripts", "run-gate.sh"))
	if err != nil {
		t.Fatal(err)
	}
	writeFixture(t, filepath.Join(root, "tests/scripts/run-gate.sh"), string(src))
	stage := `#!/bin/bash
name="$1"
[[ "$name" == test-unit ]] && name=unit
printf '%s\n' "$name" >> "$PROJECT_ROOT/commands"
printf 'marker:%s\n' "$name"
case "$name" in
 estate) echo 'test estate reachable on fixture' ;;
 lint) echo '0 issues' ;;
 consumer-policy|deadcode) echo "$name: clean" ;;
 integration) echo 'PASS=7 FAIL=0 SKIP=2' ;;
 characterization) echo 'baseline: OK (fixture)' ;;
 e2e|e2e-examples) echo 'Passed: 3 / Failed: 0' ;;
esac
if [[ "${FIXTURE_BLOCK:-}" == 1 && "$name" == estate ]]; then
 /bin/sleep 30 & child=$!
 trap 'kill "$child" 2>/dev/null; wait "$child" 2>/dev/null; exit 143' TERM
 trap 'kill "$child" 2>/dev/null; wait "$child" 2>/dev/null; exit 130' INT
 echo "$$ $child" > "$PROJECT_ROOT/pids"
 echo READY > "$PROJECT_ROOT/ready"
 wait "$child"
fi
[[ "$name" == "$FIXTURE_FAIL" ]] && exit 23
exit 0
`
	writeFixture(t, filepath.Join(root, "tests/scripts/stage.sh"), stage)
	for script, name := range map[string]string{"require-containers-up.sh": "estate", "run-tests.sh": "integration", "check-characterization-baseline.sh": "characterization"} {
		writeFixture(t, filepath.Join(root, "tests/scripts", script), "#!/bin/bash\nexec /bin/bash tests/scripts/stage.sh "+name+"\n")
	}
	writeFixture(t, filepath.Join(root, "fakebin/make"), "#!/bin/bash\nexec /bin/bash tests/scripts/stage.sh \"$1\"\n")
	if teeExit != "" {
		writeFixture(t, filepath.Join(root, "fakebin/tee"), "#!/bin/bash\n/bin/cat >/dev/null\nexit "+teeExit+"\n")
	}
	env := []string{"HOME=" + root, "PATH=" + filepath.Join(root, "fakebin") + ":/usr/bin:/bin:/usr/sbin:/sbin", "PROJECT_ROOT=" + root, "MYSQL_ROOT_PASSWORD=fixture", "FIXTURE_FAIL=" + failingStage}
	for _, args := range [][]string{{"init", "-q"}, {"add", "tests", "fakebin"}, {"-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "fixture"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir, cmd.Env = root, env
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("fixture git: %v %s", err, out)
		}
	}
	return root, env
}

type gateResult struct {
	stdout, stderr string
	exit           int
}
type gateProcess struct {
	cmd            *exec.Cmd
	stdout, stderr bytes.Buffer
	done           chan error
}

func startGate(t *testing.T, root string, env []string) *gateProcess {
	t.Helper()
	p := &gateProcess{cmd: exec.Command("/bin/bash", "tests/scripts/run-gate.sh"), done: make(chan error, 1)}
	p.cmd.Dir, p.cmd.Env = root, env
	p.cmd.Stdout, p.cmd.Stderr = &p.stdout, &p.stderr
	p.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.done <- p.cmd.Wait() }()
	t.Cleanup(func() { _ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL) })
	return p
}
func (p *gateProcess) reap(t *testing.T) gateResult {
	t.Helper()
	select {
	case err := <-p.done:
		rc := 0
		if err != nil {
			var e *exec.ExitError
			if !errors.As(err, &e) {
				t.Fatal(err)
			}
			rc = e.ExitCode()
		}
		return gateResult{p.stdout.String(), p.stderr.String(), rc}
	case <-time.After(10 * time.Second):
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
		select {
		case <-p.done:
		case <-time.After(2 * time.Second):
			t.Fatal("gate process group did not reap after KILL")
		}
		t.Fatal("gate exceeded bounded deadline")
	}
	return gateResult{}
}
func runGate(t *testing.T, root string, env []string) gateResult {
	t.Helper()
	return startGate(t, root, env).reap(t)
}
func fixtureEnv(env []string, key, value string) []string {
	result := make([]string, 0, len(env)+1)
	for _, entry := range env {
		if !strings.HasPrefix(entry, key+"=") {
			result = append(result, entry)
		}
	}
	return append(result, key+"="+value)
}
func gateRunDir(t *testing.T, root string, result gateResult, failure string) string {
	t.Helper()
	const prefix = "Gate evidence directory: "
	for _, line := range strings.Split(result.stdout, "\n") {
		if strings.HasPrefix(line, prefix) {
			dir := strings.TrimPrefix(line, prefix)
			if filepath.Dir(dir) != filepath.Join(root, "tests/results/gate") {
				t.Fatalf("%s: evidence path outside fixture: %q", failure, dir)
			}
			return dir
		}
	}
	t.Fatalf("%s: current invocation did not identify its evidence directory; stdout=%s stderr=%s", failure, result.stdout, result.stderr)
	return ""
}
func readFixture(t *testing.T, path string) string {
	t.Helper()
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	return string(b)
}
func readTSV(t *testing.T, path, header string, rows int) [][]string {
	t.Helper()
	data := readFixture(t, path)
	if !strings.HasSuffix(data, "\n") {
		t.Fatalf("record has no final newline: %q", data)
	}
	lines := strings.Split(strings.TrimSuffix(data, "\n"), "\n")
	if len(lines) != rows+1 || lines[0] != header {
		t.Fatalf("invalid TSV %s: %q", path, data)
	}
	out := make([][]string, rows)
	for i, line := range lines[1:] {
		out[i] = strings.Split(line, "\t")
		if len(out[i]) != len(strings.Split(header, "\t")) {
			t.Fatalf("invalid TSV row: %q", line)
		}
	}
	return out
}
func manifest(t *testing.T, dir string) [][]string {
	t.Helper()
	return readTSV(t, filepath.Join(dir, "summary.tsv"), "stage\tstatus\tcommand_exit\tcapture_exit\tlog\theadline", 11)
}
func runMetadata(t *testing.T, dir string) []string {
	t.Helper()
	row := readTSV(t, filepath.Join(dir, "run.tsv"), "run_id\tsource_sha\tdirty\tstarted_utc\tended_utc\toutcome\tcommand_exit\trecording_exit", 1)[0]
	if row[0] != filepath.Base(dir) {
		t.Fatalf("run ID %q differs from directory %q", row[0], dir)
	}
	for _, stamp := range []string{row[3], row[4]} {
		if _, e := time.Parse(time.RFC3339, stamp); e != nil {
			t.Fatalf("invalid UTC timestamp %q", stamp)
		}
	}
	if row[4] < row[3] {
		t.Fatalf("run ended before start: %v", row)
	}
	return row
}
func noCompletion(t *testing.T, dir string, result gateResult) {
	t.Helper()
	if _, e := os.Stat(filepath.Join(dir, "complete")); !os.IsNotExist(e) {
		t.Fatalf("failure retained completion record: %v", e)
	}
	if strings.Contains(result.stdout, "GATE COMPLETE") || strings.Contains(result.stderr, "GATE COMPLETE") {
		t.Fatalf("failure certified completion: %+v", result)
	}
}
func assertStages(t *testing.T, dir string, failed int, commandExit, captureExit string) {
	t.Helper()
	rows := manifest(t, dir)
	for i, row := range rows {
		status, rc, cap := "PASS", "0", "0"
		if i == failed {
			status, rc, cap = "FAIL", commandExit, captureExit
		} else if failed >= 0 && i > failed {
			status, rc, cap = "NOT RUN", "-", "-"
		}
		if row[0] != gateStages[i] || row[1] != status || row[2] != rc || row[3] != cap {
			t.Fatalf("stage %d = %v, want %s/%s/%s/%s", i, row, gateStages[i], status, rc, cap)
		}
		if status == "NOT RUN" {
			if row[4] != "-" {
				t.Fatalf("unstarted stage has log: %v", row)
			}
			if _, e := os.Stat(filepath.Join(dir, gateStages[i]+".log")); !os.IsNotExist(e) {
				t.Fatalf("unstarted stage has current log: %v", e)
			}
		} else {
			if row[4] != filepath.Join(dir, gateStages[i]+".log") {
				t.Fatalf("foreign log: %v", row)
			}
			if cap == "0" && !strings.Contains(readFixture(t, row[4]), "marker:"+gateStages[i]+"\n") {
				t.Fatalf("missing current stage marker: %v", row)
			}
		}
	}
}
func commandMarkers(t *testing.T, root string) []string {
	t.Helper()
	b, e := os.ReadFile(filepath.Join(root, "commands"))
	if os.IsNotExist(e) {
		return nil
	}
	if e != nil {
		t.Fatal(e)
	}
	return strings.Fields(string(b))
}

func TestGateRunEvidence(t *testing.T) {
	root, env := gateFixture(t, "integration", "")
	writeFixture(t, filepath.Join(root, "tests/results/gate/e2e.log"), "OLD_PASS\n")
	first := runGate(t, root, fixtureEnv(env, "FIXTURE_FAIL", ""))
	if first.exit != 0 {
		t.Fatalf("fixture success failed: %+v", first)
	}
	second := runGate(t, root, env)
	const failure = "GATE_STALE_EVIDENCE"
	firstDir := gateRunDir(t, root, first, failure)
	secondDir := gateRunDir(t, root, second, failure)
	if firstDir == secondDir {
		t.Fatalf("%s: two invocations reused %s", failure, firstDir)
	}
	if second.exit != 23 {
		t.Fatalf("integration exit=%d want23: %+v", second.exit, second)
	}
	assertStages(t, firstDir, -1, "", "")
	assertStages(t, secondDir, 7, "23", "0")
	if !strings.Contains(readFixture(t, filepath.Join(firstDir, "complete")), "GATE COMPLETE") {
		t.Fatal("old successful completion lost")
	}
	if readFixture(t, filepath.Join(root, "tests/results/gate/e2e.log")) != "OLD_PASS\n" {
		t.Fatal("old shared log changed")
	}
	noCompletion(t, secondDir, second)
	meta := runMetadata(t, secondDir)
	if meta[5] != "FAIL" || meta[6] != "23" || meta[7] != "0" {
		t.Fatalf("failed metadata=%v", meta)
	}
}

func TestGatePersistsSuccess(t *testing.T) {
	for _, dirty := range []bool{false, true} {
		t.Run(fmt.Sprintf("staged-dirty-%t", dirty), func(t *testing.T) {
			root, env := gateFixture(t, "", "")
			if dirty {
				writeFixture(t, filepath.Join(root, "tracked"), "staged change\n")
				cmd := exec.Command("git", "add", "tracked")
				cmd.Dir, cmd.Env = root, env
				if e := cmd.Run(); e != nil {
					t.Fatal(e)
				}
			}
			result := runGate(t, root, env)
			if result.exit != 0 {
				t.Fatalf("success failed: %+v", result)
			}
			dir := gateRunDir(t, root, result, "GATE_SUCCESS_NOT_PERSISTED")
			assertStages(t, dir, -1, "", "")
			meta := runMetadata(t, dir)
			cmd := exec.Command("git", "rev-parse", "HEAD")
			cmd.Dir, cmd.Env = root, env
			sha, e := cmd.Output()
			if e != nil {
				t.Fatal(e)
			}
			wantDirty := "clean"
			if dirty {
				wantDirty = "dirty"
			}
			if meta[1] != strings.TrimSpace(string(sha)) || meta[2] != wantDirty || meta[5] != "PASS" || meta[6] != "0" || meta[7] != "0" {
				t.Fatalf("success metadata=%v", meta)
			}
			complete := readFixture(t, filepath.Join(dir, "complete"))
			want := "GATE COMPLETE - every stage above exited 0\nrun_id=" + meta[0] + "\nsha=" + meta[1] + "\n"
			if complete != want || strings.Count(result.stdout, "GATE COMPLETE - every stage above exited 0") != 1 {
				t.Fatalf("completion disagrees: %q stdout=%s", complete, result.stdout)
			}
			human := readFixture(t, filepath.Join(dir, "summary.txt"))
			if len(strings.Split(strings.TrimSuffix(human, "\n"), "\n")) != 12 {
				t.Fatalf("human summary not multiline: %q", human)
			}
			rows := manifest(t, dir)
			if rows[7][5] != "PASS=7 FAIL=0 SKIP=2" || rows[8][5] != "OK (fixture)" || rows[9][5] != "Passed: 3  Failed: 0" {
				t.Fatalf("stage headlines lost: %v %v %v", rows[7], rows[8], rows[9])
			}
			if !reflect.DeepEqual(commandMarkers(t, root), gateStages) {
				t.Fatalf("command order=%v", commandMarkers(t, root))
			}
		})
	}
}

func TestGatePreservesCommandFailure(t *testing.T) {
	root, env := gateFixture(t, "lint", "0")
	result := runGate(t, root, env)
	if result.exit != 23 {
		t.Fatalf("command failure not preserved as exit23: %+v", result)
	}
	if !reflect.DeepEqual(commandMarkers(t, root), gateStages[:4]) {
		t.Fatalf("later command ran: %v", commandMarkers(t, root))
	}
	if strings.Contains(result.stdout, "GATE COMPLETE") {
		t.Fatal("command failure certified success")
	}
}
func TestGateCaptureFailure(t *testing.T) {
	root, env := gateFixture(t, "", "73")
	result := runGate(t, root, env)
	if result.exit == 0 {
		t.Fatal("GATE_CAPTURE_FAILURE_IGNORED: failed log capture certified success")
	}
	dir := gateRunDir(t, root, result, "GATE_CAPTURE_FAILURE_IGNORED")
	if result.exit != 73 {
		t.Fatalf("capture exit=%d want73", result.exit)
	}
	assertStages(t, dir, 0, "0", "73")
	noCompletion(t, dir, result)
	meta := runMetadata(t, dir)
	if meta[5] != "FAIL" || meta[6] != "0" || meta[7] != "73" {
		t.Fatalf("capture metadata=%v", meta)
	}
	if !reflect.DeepEqual(commandMarkers(t, root), gateStages[:1]) {
		t.Fatalf("commands=%v", commandMarkers(t, root))
	}
}
func TestGatePrerequisiteFailure(t *testing.T) {
	root, env := gateFixture(t, "", "")
	result := runGate(t, root, fixtureEnv(env, "MYSQL_ROOT_PASSWORD", ""))
	const failure = "GATE_PREREQUISITE_UNRECORDED"
	if result.exit == 0 {
		t.Fatalf("%s: missing credentials succeeded", failure)
	}
	dir := gateRunDir(t, root, result, failure)
	for _, row := range manifest(t, dir) {
		if row[1] != "NOT RUN" || row[2] != "-" || row[3] != "-" || row[4] != "-" {
			t.Fatalf("%s: started stage %v", failure, row)
		}
	}
	meta := runMetadata(t, dir)
	if meta[5] != "FAIL" || meta[6] != "1" || meta[7] != "0" {
		t.Fatalf("%s: missing prerequisite verdict: %v", failure, meta)
	}
	if !strings.Contains(result.stderr, "MYSQL_ROOT_PASSWORD") || len(commandMarkers(t, root)) != 0 {
		t.Fatalf("prerequisite not enforced: %+v", result)
	}
	noCompletion(t, dir, result)
}
func TestGatePersistenceFailure(t *testing.T) {
	for _, fault := range []string{"summary.tsv", "complete", "failed-stage-summary"} {
		t.Run(fault, func(t *testing.T) {
			stage := ""
			if fault == "failed-stage-summary" {
				stage = "lint"
			}
			root, env := gateFixture(t, stage, "")
			shim := `#!/bin/bash
destination="${2##*/}"
if [[ "$FIXTURE_PUBLISH_FAULT" == failed-stage-summary ]]; then
 if [[ "$destination" == summary.tsv ]] && /usr/bin/grep -q $'lint\tFAIL\t23\t0' "$1"; then exit 74; fi
elif [[ "$destination" == "$FIXTURE_PUBLISH_FAULT" ]]; then
 exit 74
fi
exec /bin/mv "$@"
`
			writeFixture(t, filepath.Join(root, "fakebin/mv"), shim)
			result := runGate(t, root, fixtureEnv(env, "FIXTURE_PUBLISH_FAULT", fault))
			if result.exit == 0 || !strings.Contains(strings.ToLower(result.stderr), "record") {
				t.Fatalf("GATE_PERSISTENCE_FAILURE_IGNORED: fault=%s %+v", fault, result)
			}
			if stage != "" && result.exit != 23 {
				t.Fatalf("publication failure overrode command23: %+v", result)
			}
			dir := gateRunDir(t, root, result, "GATE_PERSISTENCE_FAILURE_IGNORED")
			noCompletion(t, dir, result)
			meta := runMetadata(t, dir)
			if meta[5] != "FAIL" || meta[7] != "74" {
				t.Fatalf("recording failure metadata=%v", meta)
			}
			if fault == "complete" {
				assertStages(t, dir, -1, "", "")
				if !reflect.DeepEqual(commandMarkers(t, root), gateStages) {
					t.Fatal("completion publication not reached")
				}
			}
		})
	}
}
func TestGateAllocationFailure(t *testing.T) {
	root, env := gateFixture(t, "", "")
	writeFixture(t, filepath.Join(root, "fakebin/mkdir"), "#!/bin/bash\n[[ \"$1\" == -p ]] && exec /bin/mkdir \"$@\"\nexit 74\n")
	result := runGate(t, root, env)
	if result.exit == 0 || !strings.Contains(strings.ToLower(result.stderr), "record") {
		t.Fatalf("allocation failure not diagnosed: %+v", result)
	}
	if len(commandMarkers(t, root)) != 0 || strings.Contains(result.stdout, "GATE COMPLETE") {
		t.Fatalf("allocation failure ran commands: %+v", result)
	}
}
func TestGateRunIDCollision(t *testing.T) {
	root, env := gateFixture(t, "", "")
	writeFixture(t, filepath.Join(root, "fakebin/mkdir"), `#!/bin/bash
if [[ "$1" != -p && ! -e "$PROJECT_ROOT/collided" ]]; then
 /bin/mkdir "$@" || exit $?
 touch "$PROJECT_ROOT/collided"
 exit 1
fi
exec /bin/mkdir "$@"
`)
	result := runGate(t, root, env)
	if result.exit != 0 {
		t.Fatalf("collision did not recover: %+v", result)
	}
	dir := gateRunDir(t, root, result, "collision")
	runMetadata(t, dir)
}
func TestGateSignalEvidence(t *testing.T) {
	root, env := gateFixture(t, "", "")
	p := startGate(t, root, fixtureEnv(env, "FIXTURE_BLOCK", "1"))
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(root, "ready")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
			p.reap(t)
			t.Fatal("blocking stage never announced READY")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := syscall.Kill(-p.cmd.Process.Pid, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	result := p.reap(t)
	if result.exit != 143 {
		t.Fatalf("TERM exit=%d want143: %+v", result.exit, result)
	}
	dir := gateRunDir(t, root, result, "GATE_SIGNAL_UNRECORDED")
	assertStages(t, dir, 0, "143", "-")
	noCompletion(t, dir, result)
	meta := runMetadata(t, dir)
	if meta[5] != "FAIL" || meta[6] != "143" || meta[7] != "0" {
		t.Fatalf("signal metadata=%v", meta)
	}
	for _, s := range strings.Fields(readFixture(t, filepath.Join(root, "pids"))) {
		pid, e := strconv.Atoi(s)
		if e != nil {
			t.Fatal(e)
		}
		deadline := time.Now().Add(time.Second)
		for {
			err := syscall.Kill(pid, 0)
			if errors.Is(err, syscall.ESRCH) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("signal fixture leaked child %d: %v", pid, err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}
