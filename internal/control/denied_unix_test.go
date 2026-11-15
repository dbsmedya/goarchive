//go:build linux || darwin

package control

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// TestSameUserOnlyOtherUID (spec §7 row 29, plan L4): over a real socket, a connection from
// another uid gets ERR denied, the refusal is logged with its uid, and the snapshot, revision and
// summary are unchanged. It needs a test process running as uid 0, which makes the server and then
// runs the client as another user; elsewhere it skips, and TestJudgePeer stands for it.
//
// The directory rules keep any other user out of the socket directory, which is their point; so
// the test, which wants the server to see the other user, opens the directory and the socket to
// it after the server has started. That is the test's own change to its own temporary files.
func TestSameUserOnlyOtherUID(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skipf("needs a process running as uid 0 to run the client as another user (plan L4); this is uid %d", os.Geteuid())
	}
	f := startFixture(t, archiveApplies)
	if err := os.Chmod(f.dir, 0o711); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(f.path(), 0o666); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(f.dir, "helper.test")
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

	attr := &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: otherUID, Gid: otherUID}}
	_, out := helper(t, bin, "dial", f.path(), attr)
	line, err := readLineWithin(out, 20e9)
	if err != nil || !strings.HasPrefix(line, "ERR denied ") {
		t.Fatalf("a connection from uid %d got %q, %v, want ERR denied", otherUID, line, err)
	}
	if f.store.Load().Revision != 0 {
		t.Error("a denied connection changed the store")
	}
	waitFor(t, "the refusal to be logged", func() bool { return countEntries(f.sink, fmt.Sprintf("uid %d", otherUID)) >= 1 })
	if sum := f.store.Terminal(); len(sum.Settings) != 0 {
		t.Errorf("summary = %+v, want none", sum.Settings)
	}
}
