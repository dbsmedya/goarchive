//go:build linux

package control

import (
	"errors"
	"net"
	"path/filepath"
	"syscall"
	"testing"
)

// TestStartLeavesAFullQueueSocket (spec §7 row 20, Linux): a live listener whose queue is full
// answers a connection attempt with EAGAIN, at once. The start refuses (exit 1, not an InUseError)
// and leaves the socket: nothing but ECONNREFUSED removes one. On macOS a full queue is refused,
// which the design accepts (spec §4.2); there is no equivalent test there.
func TestStartLeavesAFullQueueSocket(t *testing.T) {
	f := &fixture{t: t}
	o := baseOptions(t, f, archiveApplies)
	path := filepath.Join(f.dir, "job.sock")
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
	before := captured(t, path)
	_, err := Start(o)
	var inUse *InUseError
	if err == nil || errors.As(err, &inUse) {
		t.Fatalf("Start() = %v, want an undecided refusal that is not an InUseError", err)
	}
	if !sameFile(before, captured(t, path)) {
		t.Error("a socket whose queue was full was removed or replaced")
	}
}
