package harness

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestImageRunTestIgnoresDockerWarnings (TEL1-M6): tests/scripts/image-run-test.sh reads the image's user
// id from `docker run`'s standard output only. Docker prints a platform warning on standard error when an
// amd64 image runs on an arm64 host, and that warning is not the uid. The script runs against a fake
// `docker` on the PATH that answers each call it makes as the real image would, and prints that warning on
// standard error for the uid call; the script must still pass.
func TestImageRunTestIgnoresDockerWarnings(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("no bash")
	}
	bin := t.TempDir()
	fake := `#!/usr/bin/env bash
args="$*"
case "$args" in
  *"-c id -u"*)
    echo "WARNING: The requested image's platform (linux/amd64) does not match the detected host platform (linux/arm64/v8) and no specific platform was requested" >&2
    echo 65532 ;;
  *"--version"*) echo "goarchive version 9.9.9-test" ;;
  *"validate"*) echo "Error: validation failed:"; echo "  - x: unknown key (line 1)"; exit 2 ;;
  *"touch ./write-probe"*) exit 0 ;;
  *"archive --job j"*)
    echo "Error: no default control socket directory for uid 65532: the user database has no entry for it; set control.socket_dir"
    exit 1 ;;
  *) echo "fake docker: unexpected call: $args" >&2; exit 99 ;;
esac
`
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte(fake), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bash, filepath.Join(root, "tests/scripts/image-run-test.sh"), "goarchive:test", "9.9.9-test")
	cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "RUNNER_TEMP="+t.TempDir())
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil || !strings.Contains(out.String(), "image-run-test: OK") {
		t.Fatalf("image-run-test.sh = %v, want it to pass: a docker warning on stderr is not the uid\n%s", err, out.String())
	}
}
