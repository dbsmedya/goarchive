//go:build linux

package cmd

import (
	"errors"
	"net"
	"path/filepath"
	"syscall"
	"testing"
	"time"
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

// TestControlFullQueueIsExit9AtOnce (spec §7 row 77a, Linux): against a live listener whose queue is
// full, connect answers EAGAIN at once, which net.Error calls a timeout though nothing waited. The
// client exits 9 with the busy message, immediately, and prints nothing on stdout. On macOS a full
// queue is refused instead (spec §4.2); there is no equivalent test there.
func TestControlFullQueueIsExit9AtOnce(t *testing.T) {
	path := filepath.Join(shortTemp(t), "j.sock")
	listenNoAccept(t, path, 1)
	var held []net.Conn
	t.Cleanup(func() {
		for _, c := range held {
			c.Close()
		}
	})
	for i := 0; ; i++ {
		c, err := net.Dial("unix", path)
		if err == nil {
			held = append(held, c)
			if i > 300 {
				t.Fatal("a listener with a queue of 1 never filled")
			}
			continue
		}
		if !errors.Is(err, syscall.EAGAIN) {
			t.Fatalf("the queue's full answer = %v, want EAGAIN", err)
		}
		break
	}
	began := time.Now()
	code, out, errs := runMain(t, "control", path, "status")
	failing(t, "a full queue", code, out, errs, 9, "busy")
	if took := time.Since(began); took > time.Second {
		t.Errorf("the client took %v on a full queue, want it at once", took)
	}
}

// TestControlResetBeforeTheReply (spec §7 row 76, Linux): a listener that closes with the request
// unread resets the connection, and the client exits 9 with nothing on stdout. On macOS the same close
// ends the stream cleanly (spec §4.3, item 9), which is an empty reply (exit 7), so the case is
// Linux's.
func TestControlResetBeforeTheReply(t *testing.T) {
	l := newListener(t, closing)
	code, out, errs := runMain(t, "control", l.Path, "-s", "batch_size=5")
	failing(t, "a connection reset", code, out, errs, 9, "may or may not have been applied")
}
