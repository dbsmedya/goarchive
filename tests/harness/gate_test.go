package harness

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func gateFixture(t *testing.T, failingStage string, teeExit string) (string, []string) {
	t.Helper()
	root := t.TempDir()
	for _, dir := range []string{"tests/scripts", "tests/results", "fakebin"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil { t.Fatal(err) }
	}
	src, err := os.ReadFile(filepath.Join("..", "scripts", "run-gate.sh"))
	if err != nil { t.Fatal(err) }
	if err := os.WriteFile(filepath.Join(root, "tests/scripts/run-gate.sh"), src, 0o755); err != nil { t.Fatal(err) }
	stage := "#!/usr/bin/env bash\necho stage-$1\nif [[ \"$FIXTURE_BLOCK\" == 1 && \"$1\" == estate ]]; then sleep 20; fi\nif [[ \"$1\" == \"" + failingStage + "\" ]]; then exit 23; fi\nexit 0\n"
	estate := "#!/usr/bin/env bash\nif [[ \"$FIXTURE_BLOCK\" == 1 ]]; then echo READY; sleep 20; fi\necho 'test estate reachable on fixture'\n"
	if err := os.WriteFile(filepath.Join(root, "tests/scripts/require-containers-up.sh"), []byte(estate), 0o755); err != nil { t.Fatal(err) }
	integration := "#!/usr/bin/env bash\necho 'PASS=1 FAIL=0 SKIP=0'\nif [[ \"$FIXTURE_FAIL\" == integration ]]; then exit 23; fi\n"
	if err := os.WriteFile(filepath.Join(root, "tests/scripts/run-tests.sh"), []byte(integration), 0o755); err != nil { t.Fatal(err) }
	if err := os.WriteFile(filepath.Join(root, "tests/scripts/check-characterization-baseline.sh"), []byte("#!/usr/bin/env bash\necho 'baseline: OK (fixture)'\n"), 0o755); err != nil { t.Fatal(err) }
	makefile := ".DEFAULT:\n\t@true\nfmt-check vet lint consumer-policy deadcode test-unit e2e e2e-examples:\n\t@bash tests/scripts/stage.sh $@\n"
	if err := os.WriteFile(filepath.Join(root, "Makefile"), []byte(makefile), 0o644); err != nil { t.Fatal(err) }
	if err := os.WriteFile(filepath.Join(root, "tests/scripts/stage.sh"), []byte(stage), 0o755); err != nil { t.Fatal(err) }
	make := "#!/usr/bin/env bash\nexec bash tests/scripts/stage.sh \"$1\"\n"
	if err := os.WriteFile(filepath.Join(root, "fakebin/make"), []byte(make), 0o755); err != nil { t.Fatal(err) }
	if teeExit != "" {
		tee := "#!/usr/bin/env bash\n/bin/cat >/dev/null\nexit " + teeExit + "\n"
		if err := os.WriteFile(filepath.Join(root, "fakebin/tee"), []byte(tee), 0o755); err != nil { t.Fatal(err) }
	}
	return root, []string{"MYSQL_ROOT_PASSWORD=fixture", "FIXTURE_FAIL=" + failingStage, "PATH=" + filepath.Join(root, "fakebin") + ":" + os.Getenv("PATH")}
}

func runGate(t *testing.T, root string, env []string) (string, error) {
	t.Helper()
	cmd := exec.Command("/bin/bash", "tests/scripts/run-gate.sh")
	cmd.Dir, cmd.Env = root, append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func TestGateRunEvidence(t *testing.T) {
	root, env := gateFixture(t, "integration", "")
	successEnv := append([]string(nil), env...)
	successEnv[1] = "FIXTURE_FAIL="
	if out, err := runGate(t, root, successEnv); err != nil { t.Fatalf("success run: %v output=%s", err, out) }
	if _, err := runGate(t, root, env); err == nil { t.Fatal("failing run succeeded") }
	runs, err := filepath.Glob(filepath.Join(root, "tests/results/gate/*/summary.tsv"))
	if err != nil { t.Fatal(err) }
	if len(runs) != 2 { t.Fatalf("GATE_STALE_EVIDENCE: got %d evidence paths, want two isolated runs", len(runs)) }
}

func TestGatePersistsSuccess(t *testing.T) {
	root, env := gateFixture(t, "", "")
	if _, err := runGate(t, root, env); err != nil { t.Fatalf("success run: %v", err) }
	runs, err := filepath.Glob(filepath.Join(root, "tests/results/gate/*/complete"))
	if err != nil { t.Fatal(err) }
	if len(runs) != 1 { t.Fatalf("GATE_SUCCESS_NOT_PERSISTED: complete records = %d, want 1", len(runs)) }
	summary, err := os.ReadFile(filepath.Join(root, "tests/results/gate", filepath.Base(filepath.Dir(runs[0])), "summary.tsv"))
	if err != nil { t.Fatal(err) }
	lines := strings.Split(strings.TrimSpace(string(summary)), "\n")
	if len(lines) != 12 || lines[0] != "stage\tstatus\tcommand_exit\tcapture_exit\tlog\theadline" { t.Fatalf("invalid persisted summary.tsv: %q", summary) }
}

func TestGateSignalEvidence(t *testing.T) {
	root, env := gateFixture(t, "", "")
	env = append(env, "FIXTURE_BLOCK=1")
	cmd := exec.Command("/bin/bash", "tests/scripts/run-gate.sh")
	cmd.Dir, cmd.Env = root, append(os.Environ(), env...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := cmd.StdoutPipe(); if err != nil { t.Fatal(err) }
	if err := cmd.Start(); err != nil { t.Fatal(err) }
	ready := make(chan bool, 1)
	go func() { scanner := bufio.NewScanner(stdout); for scanner.Scan() { if scanner.Text() == "READY" { ready <- true; return } }; ready <- false }()
	select { case ok := <-ready: if !ok { t.Fatal("blocking stage never announced readiness") }; case <-time.After(2 * time.Second): t.Fatal("timed out awaiting blocking stage readiness") }
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM); err != nil { t.Fatal(err) }
	done := make(chan error, 1); go func() { done <- cmd.Wait() }()
	select { case <-done: case <-time.After(2 * time.Second): _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); t.Fatal("gate process group did not reap after TERM") }
	files, err := filepath.Glob(filepath.Join(root, "tests/results/gate/*/summary.tsv")); if err != nil || len(files) != 1 { t.Fatalf("signal summary = %v, %v", files, err) }
	summary, err := os.ReadFile(files[0]); if err != nil { t.Fatal(err) }
	if !strings.Contains(string(summary), "estate\tFAIL\t143\t") && !strings.Contains(string(summary), "estate\tFAIL\t130\t") { t.Fatalf("signal was not persisted as stage failure: %s", summary) }
	if complete, _ := filepath.Glob(filepath.Join(root, "tests/results/gate/*/complete")); len(complete) != 0 { t.Fatalf("signal run wrote completion: %v", complete) }
}

func TestGatePreservesCommandFailure(t *testing.T) {
	root, env := gateFixture(t, "lint", "")
	out, err := runGate(t, root, env)
	exitErr, ok := err.(*exec.ExitError)
	if !ok || exitErr.ExitCode() != 23 || !strings.Contains(out, "GATE FAILED") { t.Fatalf("command failure was not preserved as exit 23: err=%v output=%s", err, out) }
}

func TestGateCaptureFailure(t *testing.T) {
	root, env := gateFixture(t, "", "73")
	if _, err := runGate(t, root, env); err == nil { t.Fatal("GATE_CAPTURE_FAILURE_IGNORED: capture failure certified success") }
}

func TestGatePrerequisiteFailure(t *testing.T) {
	root, env := gateFixture(t, "", "")
	env = []string{env[1]}
	cmd := exec.Command("/bin/bash", "tests/scripts/run-gate.sh")
	cmd.Dir = root
	for _, entry := range os.Environ() { if !strings.HasPrefix(entry, "MYSQL_ROOT_PASSWORD=") { cmd.Env = append(cmd.Env, entry) } }
	cmd.Env = append(cmd.Env, env...)
	if _, err := cmd.CombinedOutput(); err == nil { t.Fatal("GATE_PREREQUISITE_UNRECORDED: missing credentials succeeded") }
	files, _ := filepath.Glob(filepath.Join(root, "tests/results/gate/*/summary.tsv")); if len(files) != 1 { t.Fatalf("prerequisite summary files = %v", files) }
	summary, err := os.ReadFile(files[0]); if err != nil { t.Fatal(err) }
	if strings.Count(string(summary), "\tNOT RUN\t") != 11 { t.Fatalf("prerequisite did not persist all NOT RUN: %s", summary) }
}
