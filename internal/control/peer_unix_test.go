//go:build linux || darwin

package control

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// otherUID is the uid the helper server runs as in TestPeerUIDOtherUser: a uid that needs
// no user-database entry.
const otherUID = 12345

// TestPeerUIDOtherUser is spec §9 item 3 and plan L4: the server runs as another user and
// the client as uid 0; the server's peerUID reports 0 and the client's pid. It needs a root
// test process (a container, L4); elsewhere it skips, and C5's judging-function test stands
// for it. The macOS run is not made as root.
func TestPeerUIDOtherUser(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skipf("needs a process running as uid 0 to be the client (plan L4); this is uid %d", os.Geteuid())
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	// The other user must be able to run the test binary and to enter the socket directory.
	dir := shortDir(t)
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(dir, "other")
	if err := os.Mkdir(other, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(other, otherUID, otherUID); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(dir, "helper.test")
	src, err := os.Open(exe)
	if err != nil {
		t.Fatal(err)
	}
	dst, err := os.OpenFile(bin, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o755)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(dst, src); err != nil {
		t.Fatal(err)
	}
	_ = src.Close()
	if err := dst.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(other, "peer.sock")
	attr := &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: otherUID, Gid: otherUID}}
	_, out := helper(t, bin, "peer", path, attr)
	expect(t, out, "ready")

	c, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial as uid 0: %v", err)
	}
	defer c.Close()
	expect(t, out, fmt.Sprintf("uid=0 pid=%d", os.Getpid()))
}
