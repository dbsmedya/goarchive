package harness

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
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
	stage := "#!/usr/bin/env bash\necho stage-$1\nif [[ \"$1\" == \"" + failingStage + "\" ]]; then exit 23; fi\nexit 0\n"
	if err := os.WriteFile(filepath.Join(root, "tests/scripts/require-containers-up.sh"), []byte("#!/usr/bin/env bash\necho 'test estate reachable on fixture'\n"), 0o755); err != nil { t.Fatal(err) }
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
	runs, err := filepath.Glob(filepath.Join(root, "tests/results/gate/*"))
	if err != nil { t.Fatal(err) }
	if len(runs) != 2 { t.Fatalf("GATE_STALE_EVIDENCE: got %d evidence paths, want two isolated runs", len(runs)) }
}

func TestGatePersistsSuccess(t *testing.T) {
	root, env := gateFixture(t, "", "")
	if _, err := runGate(t, root, env); err != nil { t.Fatalf("success run: %v", err) }
	runs, err := filepath.Glob(filepath.Join(root, "tests/results/gate/*/complete"))
	if err != nil { t.Fatal(err) }
	if len(runs) != 1 { t.Fatalf("GATE_SUCCESS_NOT_PERSISTED: complete records = %d, want 1", len(runs)) }
}

func TestGatePreservesCommandFailure(t *testing.T) {
	root, env := gateFixture(t, "lint", "")
	out, err := runGate(t, root, env)
	if err == nil || !strings.Contains(out, "exit 23") { t.Fatalf("command failure was not preserved: err=%v output=%s", err, out) }
}

func TestGateCaptureFailure(t *testing.T) {
	root, env := gateFixture(t, "", "73")
	if _, err := runGate(t, root, env); err == nil { t.Fatal("GATE_CAPTURE_FAILURE_IGNORED: capture failure certified success") }
}

func TestGatePrerequisiteFailure(t *testing.T) {
	root, env := gateFixture(t, "", "")
	env = []string{env[1]}
	if _, err := runGate(t, root, env); err == nil { t.Fatal("GATE_PREREQUISITE_UNRECORDED: missing credentials succeeded") }
}
