//go:build windows

package cmd

import (
	"net"
	"os"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

// listenNoAccept binds an AF_UNIX stream socket at path with a listen queue of backlog and never
// accepts: a live listener that cannot take another connection once its queue is full.
func listenNoAccept(t *testing.T, path string, backlog int) {
	t.Helper()
	fd, err := windows.Socket(windows.AF_UNIX, windows.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = windows.Closesocket(fd) })
	if err := windows.Bind(fd, &windows.SockaddrUnix{Name: path}); err != nil {
		t.Fatal(err)
	}
	if err := windows.Listen(fd, backlog); err != nil {
		t.Fatal(err)
	}
}

// TestControlClassifyWindowsFullQueue (spec §7 row 77a, Windows): a connect to a full queue is
// WSAENOBUFS, not EAGAIN. It is not a timeout and not a refusal, and it is the same fact: the run is
// busy. So it is exit 9 with the busy message, as EAGAIN is on Linux, and not the generic failure.
// WSAECONNREFUSED is the stale socket (exit 5), recognised by the server's own predicate.
func TestControlClassifyWindowsFullQueue(t *testing.T) {
	dial := func(errno windows.Errno) error {
		return &net.OpError{Op: "dial", Net: "unix", Addr: &net.UnixAddr{Name: `C:\s`, Net: "unix"}, Err: os.NewSyscallError("connect", errno)}
	}
	got := classifyConnect(`C:\s`, dial(windows.WSAENOBUFS))
	if got == nil || got.code != 9 || !strings.Contains(got.Error(), "busy") || !strings.Contains(got.Error(), strconv.Quote(`C:\s`)) {
		t.Errorf("classifyConnect(WSAENOBUFS) = %v (code %d), want exit 9, the busy message, naming the path", got, codeOf(got))
	}
	got = classifyConnect(`C:\s`, dial(windows.WSAECONNREFUSED))
	if got == nil || got.code != 5 || !strings.Contains(got.Error(), "stale") {
		t.Errorf("classifyConnect(WSAECONNREFUSED) = %v (code %d), want exit 5, stale", got, codeOf(got))
	}
}
