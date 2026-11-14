//go:build !windows

package control

import (
	"syscall"
	"testing"
)

// listenNoAccept binds a stream socket at path with a listen queue of backlog and never
// accepts: a live listener that cannot take another connection once its queue is full. Go's
// own listener asks for the system's maximum queue, which a test cannot fill.
func listenNoAccept(t *testing.T, path string, backlog int) {
	t.Helper()
	fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Close(fd) })
	if err := syscall.Bind(fd, &syscall.SockaddrUnix{Name: path}); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Listen(fd, backlog); err != nil {
		t.Fatal(err)
	}
}
