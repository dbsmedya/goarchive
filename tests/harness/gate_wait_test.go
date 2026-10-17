package harness

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// waitFixture is a repository root holding the two scripts under test, unchanged, and an empty
// evidence root.
func waitFixture(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"gate-wait.sh", "gate-start.sh"} {
		src, err := os.ReadFile(filepath.Join("..", "scripts", s))
		if err != nil {
			t.Fatal(err)
		}
		writeFixture(t, filepath.Join(root, "tests/scripts", s), string(src))
	}
	if err := os.MkdirAll(filepath.Join(root, "tests/results/gate"), 0o755); err != nil {
		t.Fatal(err)
	}
	return root
}

// waitRun writes a run directory as run-gate.sh leaves it, with run_id <UTC>-<pid>.
func waitRun(t *testing.T, root string, pid int, outcome, started, ended string) string {
	t.Helper()
	runID := fmt.Sprintf("20261017T140000Z-%d", pid)
	dir := filepath.Join(root, "tests/results/gate", runID)
	writeFixture(t, filepath.Join(dir, "run.tsv"),
		"run_id\tsource_sha\tdirty\tstarted_utc\tended_utc\toutcome\tcommand_exit\trecording_exit\tpart\n"+
			runID+"\t0123456789abcdef0123456789abcdef01234567\tclean\t"+started+"\t"+ended+"\t"+outcome+"\t0\t0\tall\n")
	return dir
}

// standIn starts a long-running process whose command line is a gate's, as ps shows it.
func standIn(t *testing.T, root, script string) int {
	t.Helper()
	writeFixture(t, filepath.Join(root, "tests/scripts", script), "#!/bin/bash\n/bin/sleep 30\n")
	cmd := exec.Command("/bin/bash", "tests/scripts/"+script)
	cmd.Dir = root
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); _ = cmd.Wait() })
	return cmd.Process.Pid
}

func runScript(t *testing.T, root string, env []string, script string, args ...string) (string, int) {
	t.Helper()
	cmd := exec.Command("/bin/bash", append([]string{"tests/scripts/" + script}, args...)...)
	cmd.Dir = root
	cmd.Env = append([]string{"HOME=" + root, "PATH=/usr/bin:/bin:/usr/sbin:/sbin"}, env...)
	done := make(chan struct{})
	var out []byte
	var err error
	go func() { out, err = cmd.CombinedOutput(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		_ = cmd.Process.Kill()
		<-done
		t.Fatalf("%s did not return", script)
	}
	if ee, ok := errors.AsType[*exec.ExitError](err); ok {
		return string(out), ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	return string(out), 0
}

// TestGateWait (RVG-3): gate-wait.sh reports a finished run with its duration (exit 0), a run
// still going (exit 3, after its wait), a RUNNING record whose gate process is gone (exit 4),
// and a directory that is not a gate run of this checkout (exit 2).
func TestGateWait(t *testing.T) {
	t.Run("finished, PASS", func(t *testing.T) {
		root := waitFixture(t)
		dir := waitRun(t, root, 1, "PASS", "2026-10-17T14:00:00Z", "2026-10-17T14:09:08Z")
		out, code := runScript(t, root, nil, "gate-wait.sh", dir)
		if code != 0 || !strings.Contains(out, "outcome: PASS") || !strings.Contains(out, "duration: 548 s (ceiling 900 s)") {
			t.Errorf("exit %d:\n%s", code, out)
		}
	})
	t.Run("finished, FAIL, over the ceiling", func(t *testing.T) {
		root := waitFixture(t)
		dir := waitRun(t, root, 1, "FAIL", "2026-10-17T14:00:00Z", "2026-10-17T14:15:01Z")
		out, code := runScript(t, root, nil, "gate-wait.sh", dir)
		if code != 0 || !strings.Contains(out, "outcome: FAIL") || !strings.Contains(out, "duration: 901 s (ceiling 900 s): over the ceiling") {
			t.Errorf("exit %d:\n%s", code, out)
		}
	})
	t.Run("running", func(t *testing.T) {
		root := waitFixture(t)
		pid := standIn(t, root, "run-gate.sh")
		dir := waitRun(t, root, pid, "RUNNING", time.Now().UTC().Add(-time.Minute).Format(time.RFC3339), "-")
		start := time.Now()
		out, code := runScript(t, root, []string{"GATE_WAIT_SECONDS=1"}, "gate-wait.sh", dir)
		if code != 3 || !strings.Contains(out, "still running") || !strings.Contains(out, "elapsed: ") {
			t.Errorf("exit %d:\n%s", code, out)
		}
		if time.Since(start) < time.Second {
			t.Errorf("returned before its wait: %v", time.Since(start))
		}
	})
	t.Run("running, then finished", func(t *testing.T) {
		root := waitFixture(t)
		pid := standIn(t, root, "run-gate.sh")
		dir := waitRun(t, root, pid, "RUNNING", "2026-10-17T14:00:00Z", "-")
		running := readFixture(t, filepath.Join(dir, "run.tsv"))
		finished := strings.Replace(running, "\t-\tRUNNING\t", "\t2026-10-17T14:08:00Z\tPASS\t", 1)
		go func() {
			// As run-gate.sh publishes it: a whole file renamed into place.
			time.Sleep(time.Second)
			tmp := filepath.Join(dir, ".run.tsv.tmp")
			if os.WriteFile(tmp, []byte(finished), 0o644) == nil {
				_ = os.Rename(tmp, filepath.Join(dir, "run.tsv"))
			}
		}()
		out, code := runScript(t, root, []string{"GATE_WAIT_SECONDS=15"}, "gate-wait.sh", dir)
		if code != 0 || !strings.Contains(out, "outcome: PASS") || !strings.Contains(out, "duration: 480 s") {
			t.Errorf("exit %d:\n%s", code, out)
		}
	})
	t.Run("running, no gate process", func(t *testing.T) {
		root := waitFixture(t)
		gone := exec.Command("/bin/true")
		if err := gone.Run(); err != nil {
			t.Fatal(err)
		}
		dir := waitRun(t, root, gone.Process.Pid, "RUNNING", "2026-10-17T14:00:00Z", "-")
		out, code := runScript(t, root, []string{"GATE_WAIT_SECONDS=1"}, "gate-wait.sh", dir)
		if code != 4 || !strings.Contains(out, "no gate process") {
			t.Errorf("exit %d:\n%s", code, out)
		}
	})
	t.Run("running, the PID is another program", func(t *testing.T) {
		root := waitFixture(t)
		pid := standIn(t, root, "something-else.sh")
		dir := waitRun(t, root, pid, "RUNNING", "2026-10-17T14:00:00Z", "-")
		out, code := runScript(t, root, []string{"GATE_WAIT_SECONDS=1"}, "gate-wait.sh", dir)
		if code != 4 || !strings.Contains(out, "no gate process") {
			t.Errorf("exit %d:\n%s", code, out)
		}
	})
	t.Run("bad directories", func(t *testing.T) {
		root := waitFixture(t)
		foreign := filepath.Join(t.TempDir(), "20261017T140000Z-1")
		writeFixture(t, filepath.Join(foreign, "run.tsv"), "x\n")
		for _, args := range [][]string{nil, {filepath.Join(root, "tests/results/gate/nope")}, {foreign}, {"a", "b"}} {
			out, code := runScript(t, root, nil, "gate-wait.sh", args...)
			if code != 2 {
				t.Errorf("%v: exit %d, want 2:\n%s", args, code, out)
			}
		}
	})
}

// TestGateStart (RVG-3): gate-start.sh starts make gate detached, prints the run's evidence
// directory and its console file, and returns while the gate still runs; without tests/.env it
// starts nothing.
func TestGateStart(t *testing.T) {
	root := waitFixture(t)
	writeFixture(t, filepath.Join(root, "fakebin/make"), `#!/bin/bash
[[ "$1" == gate ]] || exit 9
echo $$ > "$PWD/standin.pid"
mkdir -p "$PWD/tests/results/gate/20261017T140000Z-$$"
echo "Gate evidence directory: $PWD/tests/results/gate/20261017T140000Z-$$"
echo "credentials: ${FIXTURE_CREDENTIAL:-none}"
exec /bin/sleep 30
`)
	env := []string{"PATH=" + filepath.Join(root, "fakebin") + ":/usr/bin:/bin:/usr/sbin:/sbin"}
	if out, code := runScript(t, root, env, "gate-start.sh"); code != 1 || !strings.Contains(out, "tests/.env") {
		t.Fatalf("without tests/.env: exit %d:\n%s", code, out)
	}
	if _, err := os.Stat(filepath.Join(root, "standin.pid")); !os.IsNotExist(err) {
		t.Fatal("started a gate without tests/.env")
	}
	writeFixture(t, filepath.Join(root, "tests/.env"), "FIXTURE_CREDENTIAL=loaded\n")
	start := time.Now()
	out, code := runScript(t, root, env, "gate-start.sh")
	if code != 0 || !strings.Contains(out, "Gate evidence directory: "+filepath.Join(root, "tests/results/gate/20261017T140000Z-")) {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	if time.Since(start) > 10*time.Second {
		t.Errorf("waited for the gate: %v", time.Since(start))
	}
	pidText := strings.TrimSpace(readFixture(t, filepath.Join(root, "standin.pid")))
	var pid int
	if _, err := fmt.Sscan(pidText, &pid); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
	if err := syscall.Kill(pid, 0); err != nil {
		t.Fatalf("the gate did not outlive gate-start.sh: %v", err)
	}
	var console string
	for line := range strings.SplitSeq(out, "\n") {
		if c, ok := strings.CutPrefix(line, "Gate console: "); ok {
			console = c
		}
	}
	if filepath.Dir(console) != filepath.Join(root, "tests/results/gate") || !strings.Contains(readFixture(t, console), "credentials: loaded") {
		t.Errorf("console %q does not hold the gate's output with tests/.env loaded", console)
	}
}
