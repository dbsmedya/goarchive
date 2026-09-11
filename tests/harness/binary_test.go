package harness

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const staleBinary = "#!/bin/bash\necho stale-executed >> \"$EXECUTION_LOG\"\n"

func runnerScript(t *testing.T, explicit, goExit string) (string, []string, string) {
	t.Helper()
	root := t.TempDir()
	bin := filepath.Join(root, "bin/goarchive")
	writeFixture(t, bin, staleBinary)
	writeFixture(t, filepath.Join(root, "fakebin/go"), `#!/bin/bash
echo build >> "$BUILD_LOG"
if [[ "$FIXTURE_GO_EXIT" == 0 && "$1" == build && "$2" == -o && -n "$3" ]]; then
 printf '#!/bin/bash\necho current\n' > "$3"
fi
exit "$FIXTURE_GO_EXIT"
`)
	env := []string{"HOME=" + root, "PROJECT_ROOT=" + root, "GOARCHIVE_BIN=" + bin, "GOARCHIVE_BIN_EXPLICIT=" + explicit, "BUILD_LOG=" + filepath.Join(root, "build.log"), "EXECUTION_LOG=" + filepath.Join(root, "executed"), "FIXTURE_GO_EXIT=" + goExit, "GOARCHIVE_BIN_READY=true", "PATH=" + filepath.Join(root, "fakebin") + ":/usr/bin:/bin"}
	for _, name := range []string{"runner.sh", "log.sh"} {
		src, e := os.ReadFile(filepath.Join("..", "e2e/lib", name))
		if e != nil {
			t.Fatal(e)
		}
		writeFixture(t, filepath.Join(root, "tests/e2e/lib", name), string(src))
	}
	return root, env, bin
}
func runEnsure(t *testing.T, root string, env []string, body string) (string, error) {
	t.Helper()
	cmd := exec.Command("/bin/bash", "-c", "source tests/e2e/lib/log.sh; source tests/e2e/lib/runner.sh; "+body)
	cmd.Dir, cmd.Env = root, env
	out, err := cmd.Output()
	return string(out), err
}
func buildCount(t *testing.T, root string) int {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, "build.log"))
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Count(string(data), "build\n")
}
func TestHarnessOwnedBinaryFresh(t *testing.T) {
	root, env, bin := runnerScript(t, "false", "0")
	for i := 1; i <= 2; i++ {
		if out, err := runEnsure(t, root, env, "ensure_goarchive_bin && ensure_goarchive_bin"); err != nil {
			t.Fatalf("ensure: %v %s", err, out)
		}
		if got := readFixture(t, bin); got == staleBinary {
			t.Fatalf("HARNESS_STALE_BINARY: owned build was not rebuilt")
		} else if got != "#!/bin/bash\necho current\n" {
			t.Fatalf("unexpected binary content: %q", got)
		}
		if got := buildCount(t, root); got != i {
			t.Fatalf("owned builds=%d after shell%d, want%d", got, i, i)
		}
	}
	t.Logf("owned build attempts=%d across two shells; executable marker=current", buildCount(t, root))
}
func TestHarnessExplicitBinary(t *testing.T) {
	root, env, bin := runnerScript(t, "true", "37")
	if out, err := runEnsure(t, root, env, "ensure_goarchive_bin && [[ \"$GOARCHIVE_BIN\" == \"$PROJECT_ROOT/bin/goarchive\" ]]"); err != nil {
		t.Fatalf("explicit ensure: %v %s", err, out)
	}
	if got := readFixture(t, bin); got != staleBinary {
		t.Fatalf("explicit binary replaced: %q", got)
	}
	if got := buildCount(t, root); got != 0 {
		t.Fatalf("explicit builds=%d", got)
	}
}
func TestHarnessMissingExplicitBinary(t *testing.T) {
	root, env, bin := runnerScript(t, "true", "0")
	if err := os.Remove(bin); err != nil {
		t.Fatal(err)
	}
	out, err := runEnsure(t, root, env, "ensure_goarchive_bin")
	if err == nil || !strings.Contains(out, "GOARCHIVE_BIN was set explicitly, but no binary exists at:") || !strings.Contains(out, bin) {
		t.Fatalf("missing-path refusal lost: %v %s", err, out)
	}
	if got := buildCount(t, root); got != 0 {
		t.Fatalf("missing explicit builds=%d", got)
	}
}
func TestHarnessBuildFailure(t *testing.T) {
	root, env, bin := runnerScript(t, "false", "37")
	out, err := runEnsure(t, root, env, `first=0; second=0
ensure_goarchive_bin && "$GOARCHIVE_BIN" || first=$?
ensure_goarchive_bin && "$GOARCHIVE_BIN" || second=$?
printf 'ensure exits: %s %s\n' "$first" "$second"
[[ "$first" -ne 0 && "$second" -ne 0 ]]
`)
	if err != nil || !strings.Contains(out, "ensure exits: 1 1") {
		t.Fatalf("HARNESS_FAILED_BUILD_REUSED: stale executable remained eligible: %v %s", err, out)
	}
	if _, err := os.Stat(filepath.Join(root, "executed")); !os.IsNotExist(err) {
		t.Fatalf("HARNESS_FAILED_BUILD_REUSED: stale sentinel executed: %v", err)
	}
	if got := readFixture(t, bin); got != staleBinary {
		t.Fatalf("failed build replaced stale sentinel: %q", got)
	}
	if got := buildCount(t, root); got != 2 {
		t.Fatalf("failed build attempts=%d, want2 in same shell", got)
	}
	t.Logf("failed build attempts=%d in one shell; stale sentinel executions=0", buildCount(t, root))
}
