package control

import (
	"errors"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"
)

// These tests pin what the platforms do to a connect and to a close, which the design relies
// on (spec §4.2 Start step 4, §4.3, §4.9; §9 items 7 and 9). They are **pins**: they observe
// the platform and pass before and after any goarchive code. A platform that answers
// differently fails here on purpose, and the evidence files (c1-item7.md, c1-item9.md) say
// what each answer means for the design. Windows was measured in the Windows CI job (run
// 43669762242, c-windows-1.md): its cases pin what was seen, and log it besides.

// atOnce is how long a connect may take and still be "at once". Every connect measured on
// Linux and macOS ends in microseconds.
const atOnce = 2 * time.Second

func dialTimed(path string) (net.Conn, time.Duration, error) {
	start := time.Now()
	c, err := net.DialTimeout("unix", path, 10*time.Second)
	return c, time.Since(start), err
}

// TestConnectToStaleSocketIsRefusedAtOnce: a socket file with no listener is refused at once.
func TestConnectToStaleSocketIsRefusedAtOnce(t *testing.T) {
	path := staleSocket(t, shortDir(t), "stale.sock")
	c, took, err := dialTimed(path)
	if err == nil {
		c.Close()
		t.Fatal("connected to a socket with no listener")
	}
	t.Logf("%s: stale socket: %v after %v", runtime.GOOS, err, took)
	if took > atOnce {
		t.Fatalf("not at once: %v", took)
	}
	switch runtime.GOOS {
	case "linux", "darwin":
		if !errors.Is(err, syscall.ECONNREFUSED) {
			t.Fatalf("got %v, want connection refused", err)
		}
	case "windows":
		pinWindowsStale(t, err)
	}
}

// TestConnectPermissionError: a socket the connecting user may not write to is refused with a
// permission error at once, on Linux and macOS. It is the deterministic "undecided" case of
// spec §4.2 step 4, and the client's exit 9 (spec §7). A root process is not refused by
// modes, so the test needs a non-root uid (plan L3).
func TestConnectPermissionError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("not available: a Windows socket file has no mode bits to deny")
	}
	if os.Geteuid() == 0 {
		t.Skip("needs a non-root uid: root is not refused by file modes (plan L3)")
	}
	dir := shortDir(t)
	path := filepath.Join(dir, "deny.sock")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}
	c, took, err := dialTimed(path)
	if err == nil {
		c.Close()
		t.Fatal("connected to a socket with mode 0")
	}
	if !errors.Is(err, fs.ErrPermission) || took > atOnce {
		t.Fatalf("got %v after %v, want a permission error at once", err, took)
	}
}

// TestSaturatedListenerConnect pins what a connect to a live listener whose queue is full
// does: it never stalls, on any platform measured; it fails at once, and the error differs.
// Linux says EAGAIN (which net.Error calls a timeout, though nothing waited); macOS says
// connection refused, the answer for a socket with no listener, so there "refused" does not
// prove that nobody listens. The classifier of spec §4.2 step 4 and the client depend on both
// facts; c1-item7.md has the measurement.
func TestSaturatedListenerConnect(t *testing.T) {
	path := filepath.Join(shortDir(t), "full.sock")
	listenNoAccept(t, path, 1)
	var held []net.Conn
	t.Cleanup(func() {
		for _, c := range held {
			c.Close()
		}
	})
	for i := 1; i <= 300; i++ {
		c, took, err := dialTimed(path)
		if err == nil {
			held = append(held, c)
			continue
		}
		t.Logf("%s: connection %d to a saturated listener: %v after %v (timeout=%v)", runtime.GOOS, i, err, took, isTimeout(err))
		if took > atOnce {
			t.Fatalf("a connect stalled for %v: %v", took, err)
		}
		switch runtime.GOOS {
		case "linux":
			if !errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.ECONNREFUSED) {
				t.Fatalf("got %v, want EAGAIN", err)
			}
			if !isTimeout(err) {
				t.Fatalf("net.Error.Timeout() is false for %v: a classifier may now rely on it", err)
			}
		case "darwin":
			if !errors.Is(err, syscall.ECONNREFUSED) {
				t.Fatalf("got %v, want connection refused", err)
			}
		case "windows":
			pinWindowsSaturated(t, err)
		}
		return
	}
	t.Fatalf("%d connections to a listener with a queue of 1 were all made", len(held))
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// TestCloseWithInputUnread is spec §9 item 9: a client sends two lines, the server reads one,
// replies and closes with the second unread. The client always gets the whole reply; what
// follows is a reset on Linux and a clean end of stream on macOS.
func TestCloseWithInputUnread(t *testing.T) {
	path := filepath.Join(shortDir(t), "close.sock")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	const request = "status\n"
	go func() {
		c, err := l.Accept()
		if err != nil {
			return
		}
		buf := make([]byte, len(request))
		_, _ = io.ReadFull(c, buf)
		_, _ = c.Write([]byte("OK\nbody\n"))
		_ = c.Close()
	}()
	c, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Write([]byte(request + request)); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	got, err := io.ReadAll(c)
	if string(got) != "OK\nbody\n" {
		t.Fatalf("the reply was %q (%v)", got, err)
	}
	t.Logf("%s: after the reply: %v", runtime.GOOS, err)
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatalf("the close was never seen: %v", err)
	}
	switch runtime.GOOS {
	case "linux":
		if !errors.Is(err, syscall.ECONNRESET) {
			t.Fatalf("got %v, want a reset", err)
		}
	case "darwin":
		if err != nil {
			t.Fatalf("got %v, want a clean end of stream", err)
		}
	case "windows":
		pinWindowsReset(t, err)
	}
}
