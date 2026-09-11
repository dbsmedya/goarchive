package harness

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func runnerScript(t *testing.T, explicit, goExit string) (string, []string, string) {
	t.Helper()
	root := t.TempDir()
	bin := filepath.Join(root, "bin", "goarchive")
	for _, dir := range []string{"bin", "fakebin"} { if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil { t.Fatal(err) } }
	if err := os.WriteFile(bin, []byte("stale"), 0o755); err != nil { t.Fatal(err) }
	goScript := "#!/usr/bin/env bash\necho build >> \"$BUILD_LOG\"\nif [[ \"" + goExit + "\" == 0 && \"$1\" == build && \"$2\" == -o && \"$3\" != \"\" ]]; then printf current > \"$3\"; fi\nexit " + goExit + "\n"
	if err := os.WriteFile(filepath.Join(root, "fakebin", "go"), []byte(goScript), 0o755); err != nil { t.Fatal(err) }
	log := filepath.Join(root, "build.log")
	env := []string{"PROJECT_ROOT=" + root, "GOARCHIVE_BIN=" + bin, "GOARCHIVE_BIN_EXPLICIT=" + explicit, "BUILD_LOG=" + log, "PATH=" + filepath.Join(root, "fakebin") + ":" + os.Getenv("PATH")}
	return root, env, bin
}

func runEnsure(t *testing.T, root string, env []string) error {
	t.Helper()
	cmd := exec.Command("/bin/bash", "-c", "log_info(){ :; }; log_error(){ :; }; source tests/e2e/lib/runner.sh; ensure_goarchive_bin")
	cmd.Dir, cmd.Env = root, append(os.Environ(), env...)
	_, err := cmd.CombinedOutput()
	return err
}

func runEnsureTwice(t *testing.T, root string, env []string) error {
	t.Helper()
	cmd := exec.Command("/bin/bash", "-c", "log_info(){ :; }; log_error(){ :; }; source tests/e2e/lib/runner.sh; ensure_goarchive_bin; ensure_goarchive_bin")
	cmd.Dir, cmd.Env = root, append(os.Environ(), env...)
	_, err := cmd.CombinedOutput()
	return err
}

func copyRunner(t *testing.T, root string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, "tests/e2e/lib"), 0o755); err != nil { t.Fatal(err) }
	src, err := os.ReadFile(filepath.Join("..", "e2e/lib/runner.sh")); if err != nil { t.Fatal(err) }
	if err := os.WriteFile(filepath.Join(root, "tests/e2e/lib/runner.sh"), src, 0o644); err != nil { t.Fatal(err) }
}

func TestHarnessOwnedBinaryFresh(t *testing.T) {
	root, env, bin := runnerScript(t, "false", "0")
	copyRunner(t, root)
	if err := runEnsureTwice(t, root, env); err != nil { t.Fatal(err) }
	if err := runEnsure(t, root, env); err != nil { t.Fatal(err) }
	data, err := os.ReadFile(bin); if err != nil { t.Fatal(err) }
	if string(data) != "current" { t.Fatalf("HARNESS_STALE_BINARY: got %q", data) }
	lines, _ := os.ReadFile(filepath.Join(root, "build.log"))
	if got := strings.Count(string(lines), "build"); got != 2 { t.Fatalf("owned build count = %d, want 2 (one per shell)", got) }
}

func TestHarnessExplicitBinary(t *testing.T) {
	root, env, bin := runnerScript(t, "true", "37")
	copyRunner(t, root)
	if err := runEnsure(t, root, env); err != nil { t.Fatal(err) }
	data, err := os.ReadFile(bin); if err != nil { t.Fatal(err) }
	if string(data) != "stale" { t.Fatalf("explicit binary changed to %q", data) }
	if _, err := os.Stat(filepath.Join(root, "build.log")); !os.IsNotExist(err) { t.Fatalf("explicit binary invoked builder: %v", err) }
}

func TestHarnessMissingExplicitBinary(t *testing.T) {
	root, env, bin := runnerScript(t, "true", "0")
	copyRunner(t, root)
	if err := os.Remove(bin); err != nil { t.Fatal(err) }
	if err := runEnsure(t, root, env); err == nil { t.Fatal("missing explicit binary succeeded") }
	if _, err := os.Stat(filepath.Join(root, "build.log")); !os.IsNotExist(err) { t.Fatalf("missing explicit binary invoked builder: %v", err) }
}

func TestHarnessBuildFailure(t *testing.T) {
	root, env, bin := runnerScript(t, "false", "37")
	copyRunner(t, root)
	if err := runEnsureTwice(t, root, env); err == nil { t.Fatal("HARNESS_FAILED_BUILD_REUSED: failed build succeeded") }
	data, err := os.ReadFile(bin); if err != nil { t.Fatal(err) }
	if string(data) != "stale" { t.Fatalf("failed build replaced stale sentinel: %q", data) }
	lines, _ := os.ReadFile(filepath.Join(root, "build.log"))
	if got := strings.Count(string(lines), "build"); got != 2 { t.Fatalf("failed build attempts = %d, want 2", got) }
}
