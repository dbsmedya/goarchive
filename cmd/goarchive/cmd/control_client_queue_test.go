//go:build linux || windows

package cmd

import (
	"net"
	"path/filepath"
	"testing"
	"time"
)

// TestControlFullQueueIsExit9AtOnce (spec §7 row 77a, Linux and Windows): against a live listener whose
// queue is full, connect fails at once: Linux answers EAGAIN, which net.Error calls a timeout though
// nothing waited, and Windows answers WSAENOBUFS (c-windows-1.md). The client exits 9 with the busy
// message, immediately, and prints nothing on stdout. On macOS a full queue is refused instead (spec
// §4.2); there is no equivalent test there. The fill loop ends on the platform's own full-queue error,
// as isQueueFull recognises it, so the test also pins that the predicate knows the real error.
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
		if !isQueueFull(err) {
			t.Fatalf("the queue's full answer = %v, want the platform's full-queue error", err)
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
