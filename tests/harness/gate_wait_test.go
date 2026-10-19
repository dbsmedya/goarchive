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
		dir := waitRun(t, root, pid, "RUNNING", time.Now().UTC().Format(time.RFC3339), "-")
		// A 2 s budget: the deadline is whole seconds, so a 1 s budget can end at once near a
		// second's boundary (Gate verify run 39045038492).
		start := time.Now()
		out, code := runScript(t, root, []string{"GATE_WAIT_SECONDS=2"}, "gate-wait.sh", dir)
		if code != 3 || !strings.Contains(out, "still running") || !strings.Contains(out, "elapsed: ") {
			t.Errorf("exit %d:\n%s", code, out)
		}
		if time.Since(start) < time.Second {
			t.Errorf("returned before its wait: %v", time.Since(start))
		}
	})
	// MR review R3: a final outcome is final only once the gate's process has exited, after its
	// last publication (the completion marker, or a failure record that replaces PASS).
	t.Run("PASS published, process still finishing, then exited", func(t *testing.T) {
		root := waitFixture(t)
		pid := standIn(t, root, "run-gate.sh")
		now := time.Now().UTC()
		dir := waitRun(t, root, pid, "PASS", now.Format(time.RFC3339), now.Add(480*time.Second).Format(time.RFC3339))
		out, code := runScript(t, root, []string{"GATE_WAIT_SECONDS=1"}, "gate-wait.sh", dir)
		if code != 3 || !strings.Contains(out, "still running") {
			t.Errorf("a live gate's PASS was final: exit %d:\n%s", code, out)
		}
		go func() {
			time.Sleep(time.Second)
			_ = syscall.Kill(-pid, syscall.SIGKILL)
		}()
		out, code = runScript(t, root, []string{"GATE_WAIT_SECONDS=15"}, "gate-wait.sh", dir)
		if code != 0 || !strings.Contains(out, "outcome: PASS") || !strings.Contains(out, "duration: 480 s") {
			t.Errorf("exit %d:\n%s", code, out)
		}
	})
	// MR re-review R3: the gate publishes its outcome and exits between the waiter's read of
	// RUNNING and its process check. A ps stand-in makes that order certain: it publishes PASS,
	// then reports no process. The waiter must read the record again, not call the run hung.
	t.Run("outcome published as the process exits", func(t *testing.T) {
		root := waitFixture(t)
		dir := waitRun(t, root, 4242, "RUNNING", "2026-10-17T14:00:00Z", "-")
		finished := strings.Replace(readFixture(t, filepath.Join(dir, "run.tsv")), "\t-\tRUNNING\t", "\t2026-10-17T14:08:00Z\tPASS\t", 1)
		writeFixture(t, filepath.Join(dir, ".finished"), finished)
		writeFixture(t, filepath.Join(root, "fakebin/ps"), "#!/bin/bash\n/bin/mv \"$FIXTURE_RUN/.finished\" \"$FIXTURE_RUN/run.tsv\" 2>/dev/null\nexit 1\n")
		env := []string{"PATH=" + filepath.Join(root, "fakebin") + ":/usr/bin:/bin:/usr/sbin:/sbin", "FIXTURE_RUN=" + dir, "GATE_WAIT_SECONDS=1"}
		out, code := runScript(t, root, env, "gate-wait.sh", dir)
		if code != 0 || !strings.Contains(out, "outcome: PASS") || !strings.Contains(out, "duration: 480 s") {
			t.Errorf("exit %d, want the published PASS:\n%s", code, out)
		}
	})
	// MR review R4: no wait runs past started_utc + 1800 s.
	t.Run("running, at the 1800 s stop", func(t *testing.T) {
		root := waitFixture(t)
		pid := standIn(t, root, "run-gate.sh")
		dir := waitRun(t, root, pid, "RUNNING", time.Now().UTC().Add(-1799*time.Second).Format(time.RFC3339), "-")
		start := time.Now()
		out, code := runScript(t, root, []string{"GATE_WAIT_SECONDS=10", "GATE_PROCESS_SLACK=2000"}, "gate-wait.sh", dir)
		if code != 3 || time.Since(start) > 4*time.Second {
			t.Errorf("exit %d after %v, want 3 at the stop:\n%s", code, time.Since(start), out)
		}
		var elapsed int
		for line := range strings.SplitSeq(out, "\n") {
			if e, ok := strings.CutPrefix(line, "elapsed: "); ok {
				_, _ = fmt.Sscanf(e, "%d", &elapsed)
			}
		}
		if elapsed < 1800 || elapsed > 1802 {
			t.Errorf("elapsed %d, want 1800–1802:\n%s", elapsed, out)
		}
	})
	t.Run("running, no gate process", func(t *testing.T) {
		root := waitFixture(t)
		gone := exec.Command("/bin/sh", "-c", "exit 0")
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
	// MR review R5: a live run-gate.sh that started after this run did is another invocation.
	t.Run("running, the PID is another gate run", func(t *testing.T) {
		root := waitFixture(t)
		other := waitFixture(t)
		pid := standIn(t, other, "run-gate.sh")
		dir := waitRun(t, root, pid, "RUNNING", time.Now().UTC().Add(-10*time.Minute).Format(time.RFC3339), "-")
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
	// The stand-in announces its directory, then takes 2 s to publish run.tsv (MR review R2), or,
	// with FIXTURE_DIE, dies before it does.
	writeFixture(t, filepath.Join(root, "fakebin/make"), `#!/bin/bash
[[ "$1" == gate ]] || exit 9
echo $$ > "$PWD/standin.pid"
d="$PWD/tests/results/gate/20261017T140000Z-$$"
mkdir -p "$d"
echo "Gate evidence directory: $d"
echo "credentials: ${FIXTURE_CREDENTIAL:-none}"
/bin/sleep 2
[[ -f "$PWD/die" ]] && exit 1
printf 'run_id\n20261017T140000Z-%s\n' $$ > "$d/run.tsv"
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
	if _, err := os.Stat(filepath.Join(root, "tests/results/gate", "20261017T140000Z-"+strings.TrimSpace(readFixture(t, filepath.Join(root, "standin.pid"))), "run.tsv")); err != nil {
		t.Errorf("gate-start.sh returned before run.tsv was published: %v", err)
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

// TestGateStartEarlyDeath (MR review R2): a gate that names its directory and dies before it
// publishes run.tsv is a failed start, reported with its console.
func TestGateStartEarlyDeath(t *testing.T) {
	root := waitFixture(t)
	writeFixture(t, filepath.Join(root, "fakebin/make"), `#!/bin/bash
d="$PWD/tests/results/gate/20261017T140000Z-$$"
mkdir -p "$d"
echo "Gate evidence directory: $d"
echo "dying early"
exit 1
`)
	writeFixture(t, filepath.Join(root, "tests/.env"), "X=1\n")
	env := []string{"PATH=" + filepath.Join(root, "fakebin") + ":/usr/bin:/bin:/usr/sbin:/sbin"}
	out, code := runScript(t, root, env, "gate-start.sh")
	if code != 1 || !strings.Contains(out, "dying early") || !strings.Contains(out, "Gate evidence directory: ") {
		t.Errorf("exit %d, want 1 with the directory and the console:\n%s", code, out)
	}
}
