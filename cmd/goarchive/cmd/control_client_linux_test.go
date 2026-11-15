//go:build linux

package cmd

import (
	"net"
	"syscall"
	"testing"
)

// listenNoAccept binds a stream socket at path with a listen queue of backlog and never accepts: a live
// listener that cannot take another connection once its queue is full. Go's own listener asks for the
// system's maximum queue, which a test cannot fill.
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

// closing accepts and closes at once without reading: with the request unread, Linux resets the
// connection under the client (spec §4.3).
func closing(_ *testListener, c net.Conn) { _ = c.Close() }

// TestControlResetBeforeTheReply (spec §7 row 76, Linux): a listener that closes with the request
// unread resets the connection, and the client exits 9 with nothing on stdout. On macOS the same close
// ends the stream cleanly (spec §4.3, item 9), which is an empty reply (exit 7), so the case is
// Linux's.
func TestControlResetBeforeTheReply(t *testing.T) {
	l := newListener(t, closing)
	code, out, errs := runMain(t, "control", l.Path, "-s", "batch_size=5")
	failing(t, "a connection reset", code, out, errs, 9, "may or may not have been applied")
}
