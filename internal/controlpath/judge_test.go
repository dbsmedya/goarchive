package controlpath

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// staleSocket binds a socket at a new path inside dir and closes the listener without
// unlinking, which leaves the socket file as a crashed run does. It returns the path.
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

func lstat(t *testing.T, path string) os.FileInfo {
	t.Helper()
	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi
}

// symlinkOrSkip makes a symbolic link; Windows needs a privilege that a runner may lack.
func symlinkOrSkip(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("cannot make a symbolic link here: %v", err)
	}
}

// TestJudgeSocket: only an object that Lstat identifies as a socket is one (spec §4.2 Start
// step 4, and the client's endpoint checks, §4.9); everything else is refused, saying what it is.
func TestJudgeSocket(t *testing.T) {
	dir := shortDir(t)
	sock := staleSocket(t, dir, "s.sock")
	if err := JudgeSocket(lstat(t, sock)); err != nil {
		t.Fatalf("a socket file: %v", err)
	}

	regular := filepath.Join(dir, "regular")
	if err := os.WriteFile(regular, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	subdir := filepath.Join(dir, "sub")
	if err := os.Mkdir(subdir, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link.sock")
	symlinkOrSkip(t, sock, link)

	for _, c := range []struct{ name, path, found string }{
		{"regular file", regular, "regular file"},
		{"directory", subdir, "directory"},
		{"symbolic link to a socket", link, "symbolic link"},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := JudgeSocket(lstat(t, c.path))
			if err == nil || !strings.Contains(err.Error(), c.found) {
				t.Fatalf("JudgeSocket = %v, want a refusal saying %q", err, c.found)
			}
		})
	}
}

// TestJudgeDirKind: a socket directory is a real directory, not a link or a file, on every
// platform (spec §4.1 rule 2, and Windows's "not enforced" list leaves this rule in).
func TestJudgeDirKind(t *testing.T) {
	dir := shortDir(t)
	if err := JudgeDir(lstat(t, dir), os.Geteuid()); err != nil {
		t.Fatalf("a directory the test made: %v", err)
	}
	regular := filepath.Join(dir, "regular")
	if err := os.WriteFile(regular, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	symlinkOrSkip(t, dir, link)
	for _, c := range []struct{ name, path, found string }{
		{"regular file", regular, "regular file"},
		{"symbolic link to a directory", link, "symbolic link"},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := JudgeDir(lstat(t, c.path), os.Geteuid())
			if err == nil || !strings.Contains(err.Error(), c.found) {
				t.Fatalf("JudgeDir = %v, want a refusal saying %q", err, c.found)
			}
		})
	}
}
