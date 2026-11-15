package control

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"
)

// heldFilesRemovable is false on Windows, which will not remove a lock file that the run holds open
// ("The process cannot access the file because it is being used by another process") and will not
// rename a directory that has open handles ("Access is denied"). Measured in the Windows CI job (run
// 43669762242). A case that needs the run's lock name or directory to be taken from under it cannot
// arise there; a test whose point needs it skips, and one with a part that does not keeps that part.
const heldFilesRemovable = runtime.GOOS != "windows"

func skipUnlessHeldFilesRemovable(t *testing.T) {
	t.Helper()
	if !heldFilesRemovable {
		t.Skip("Windows will not remove a lock file or rename a directory that the run holds open: this case cannot arise there")
	}
}

// The platform primitives are about other processes: a lock held elsewhere, a peer of
// another uid, a dialer in another process. Those tests start the test binary again as a
// helper (the pattern os/exec's own tests use). The helper is test code, not a fault.

const (
	helperRoleEnv = "GC_CONTROL_HELPER"
	helperPathEnv = "GC_CONTROL_PATH"
)

// TestHelperProcess is the helper's entry point: it does nothing in a normal run, and returns at
// once. It does not skip: a skip would show as SKIP in every run of the package, among the skips
// that mean a host cannot run a test (TEL1-M1).
func TestHelperProcess(t *testing.T) {
	role := os.Getenv(helperRoleEnv)
	if role == "" {
		return
	}
	path := os.Getenv(helperPathEnv)
	switch role {
	case "lock": // hold the lock on path until stdin ends
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
		if err != nil {
			fmt.Println("error", err)
			os.Exit(2)
		}
		if err := lockFile(f); err != nil {
			fmt.Println("error", err)
			os.Exit(3)
		}
		fmt.Println("locked")
		_, _ = io.Copy(io.Discard, os.Stdin)
	case "dial": // send one line, print the reply
		c, err := net.Dial("unix", path)
		if err != nil {
			fmt.Println("error", err)
			os.Exit(2)
		}
		_, _ = c.Write([]byte("ping\n"))
		line, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			fmt.Println("error", err)
			os.Exit(3)
		}
		fmt.Print(line)
	case "peer": // listen, serve one connection, report who connected
		l, err := net.Listen("unix", path)
		if err != nil {
			fmt.Println("error", err)
			os.Exit(2)
		}
		fmt.Printf("ready uid=%d\n", os.Geteuid())
		c, err := l.Accept()
		if err != nil {
			fmt.Println("error", err)
			os.Exit(3)
		}
		uid, pid, err := peerUID(c)
		if err != nil {
			fmt.Println("error", err)
			os.Exit(4)
		}
		fmt.Printf("uid=%d pid=%d\n", uid, pid)
	default:
		fmt.Println("error unknown role", role)
		os.Exit(2)
	}
	os.Exit(0)
}

// helper starts the helper role against path and returns its stdout reader. The process is
// killed and reaped when the test ends.
func helper(t *testing.T, exe, role, path string, attr *syscall.SysProcAttr) (*exec.Cmd, *bufio.Reader) {
	t.Helper()
	cmd := exec.Command(exe, "-test.run=^TestHelperProcess$")
	cmd.Env = append(os.Environ(), helperRoleEnv+"="+role, helperPathEnv+"="+path)
	cmd.SysProcAttr = attr
	// The helper's stdin is a pipe that stays open until the helper is waited for: the lock
	// role holds its lock until stdin ends, and Wait closes the pipe itself.
	if _, err := cmd.StdinPipe(); err != nil {
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	return cmd, bufio.NewReader(out)
}

// expect reads one line of the helper's output and fails the test unless it is want.
func expect(t *testing.T, r *bufio.Reader, want string) {
	t.Helper()
	line, err := readLineWithin(r, 20*time.Second)
	if err != nil || line != want {
		t.Fatalf("helper said %q, %v; want %q", line, err, want)
	}
}

func readLineWithin(r *bufio.Reader, d time.Duration) (string, error) {
	type result struct {
		line string
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		line, err := r.ReadString('\n')
		ch <- result{trimNL(line), err}
	}()
	select {
	case res := <-ch:
		return res.line, res.err
	case <-time.After(d):
		return "", fmt.Errorf("no line within %v", d)
	}
}

func trimNL(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}

// shortDir makes a directory with a short path for sockets (t.TempDir is too long on macOS).
func shortDir(t *testing.T) string {
	t.Helper()
	base := "/tmp"
	if runtime.GOOS == "windows" {
		base = os.TempDir()
	}
	dir, err := os.MkdirTemp(base, "gc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// staleSocket binds a socket at name in dir and closes the listener without unlinking, which
// leaves the socket file as a crashed run does.
func staleSocket(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	l.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}
