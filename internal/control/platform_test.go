package control

import (
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// TestLockFileExcludes: an exclusive, non-blocking lock on an open file excludes every other
// open of the file, in this process or another (spec §4.2 Start step 2); closing releases it.
func TestLockFileExcludes(t *testing.T) {
	path := filepath.Join(shortDir(t), "job.lock")
	open := func() *os.File {
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	first, second := open(), open()
	defer second.Close()
	if err := lockFile(first); err != nil {
		t.Fatalf("the first lock: %v", err)
	}
	if err := lockFile(second); !errors.Is(err, errLockHeld) {
		t.Fatalf("the second lock = %v, want errLockHeld", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lockFile(second); err != nil {
		t.Fatalf("after the holder closed: %v", err)
	}
}

// TestLockFileHeldAcrossProcesses: another process's lock excludes this one, and the system
// releases it when that process is killed, however it dies (spec §4.2 Start step 2).
func TestLockFileHeldAcrossProcesses(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(shortDir(t), "job.lock")
	cmd, out := helper(t, exe, "lock", path, nil)
	expect(t, out, "locked")

	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := lockFile(f); !errors.Is(err, errLockHeld) {
		t.Fatalf("lock held by another process: %v, want errLockHeld", err)
	}
	if err := cmd.Process.Kill(); err != nil { // SIGKILL; TerminateProcess on Windows
		t.Fatal(err)
	}
	_ = cmd.Wait()
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := lockFile(f)
		if err == nil {
			return
		}
		if !errors.Is(err, errLockHeld) || time.Now().After(deadline) {
			t.Fatalf("the lock of a killed process was not released: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestIsSocket: Lstat identifies a stale socket file as a socket, and nothing else is one: a
// regular file, a directory and a symbolic link to a socket are not (spec §4.2 Start step 4).
func TestIsSocket(t *testing.T) {
	dir := shortDir(t)
	sock := staleSocket(t, dir, "s.sock")
	fi, err := os.Lstat(sock)
	if err != nil {
		t.Fatal(err)
	}
	if !isSocket(fi) {
		t.Fatalf("a stale socket file is not identified as a socket: mode %v", fi.Mode())
	}

	regular := filepath.Join(dir, "regular")
	if err := os.WriteFile(regular, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link.sock")
	linkErr := os.Symlink(sock, link)
	for name, path := range map[string]string{"regular file": regular, "directory": dir, "symbolic link": link} {
		if name == "symbolic link" && linkErr != nil {
			t.Logf("no symbolic link here: %v", linkErr)
			continue
		}
		fi, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		if isSocket(fi) {
			t.Errorf("a %s is identified as a socket: mode %v", name, fi.Mode())
		}
	}
}

// TestListenDialAcrossProcesses: a Unix listener and a dialer in two processes talk (spec §9
// item 2 on Windows; the same test everywhere).
func TestListenDialAcrossProcesses(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(shortDir(t), "x.sock")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	served := make(chan string, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			served <- "accept: " + err.Error()
			return
		}
		defer c.Close()
		buf := make([]byte, 5)
		if _, err := io.ReadFull(c, buf); err != nil {
			served <- "read: " + err.Error()
			return
		}
		_, _ = c.Write([]byte("pong\n"))
		served <- string(buf)
	}()
	_, out := helper(t, exe, "dial", path, nil)
	expect(t, out, "pong")
	select {
	case got := <-served:
		if got != "ping\n" {
			t.Fatalf("the server read %q", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the server did not finish")
	}
}

// TestPeerUIDSameUser: a connection made by this very process reports this process's
// effective uid, and its pid where the platform gives one. Windows has no peer credentials.
func TestPeerUIDSameUser(t *testing.T) {
	path := filepath.Join(shortDir(t), "p.sock")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- c
	}()
	client, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server := <-accepted
	if server == nil {
		t.Fatal("no connection accepted")
	}
	defer server.Close()

	uid, pid, err := peerUID(server)
	if runtime.GOOS == "windows" {
		if !errors.Is(err, errNoPeerCredentials) {
			t.Fatalf("peerUID on Windows = %d, %d, %v; want errNoPeerCredentials", uid, pid, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("peerUID: %v", err)
	}
	if uid != os.Geteuid() {
		t.Errorf("peer uid %d, want %d", uid, os.Geteuid())
	}
	if pid != os.Getpid() {
		t.Errorf("peer pid %d, want %d", pid, os.Getpid())
	}
}

// TestPeerUIDNotASocket: credentials are read from a Unix socket only; anything else is an
// error, never a zero uid that would read as root.
func TestPeerUIDNotASocket(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	if uid, pid, err := peerUID(a); err == nil {
		t.Fatalf("peerUID of a pipe = %d, %d, nil", uid, pid)
	}
}
