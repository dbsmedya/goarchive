//go:build windows

package control

import (
	"testing"

	"golang.org/x/sys/windows"
)

// listenNoAccept binds an AF_UNIX stream socket at path with a listen queue of backlog and
// never accepts (see the Unix version).
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
