package control

import (
	"errors"
	"net"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// TestJudgePeer (spec §7 row 29, the judging function): the peer's uid must equal the run's
// effective uid; every other uid is refused, uid 0 among them, and so are unreadable credentials.
// Where the platform has none (Windows) the rule is not enforced and a change is logged without a
// uid.
func TestJudgePeer(t *testing.T) {
	boom := errors.New("getsockopt failed")
	for _, tc := range []struct {
		name     string
		uid, pid int
		err      error
		euid     int
		enforced bool
		wantUID  int
		denied   bool
	}{
		{"the same user", 1000, 4242, nil, 1000, true, 1000, false},
		{"another user", 1001, 7, nil, 1000, true, 0, true},
		{"uid 0 against an unprivileged run", 0, 7, nil, 1000, true, 0, true},
		{"another user against a run of uid 0", 1000, 7, nil, 0, true, 0, true},
		{"uid 0 against a run of uid 0", 0, 7, nil, 0, true, 0, false},
		{"unreadable credentials", 0, 0, boom, 1000, true, 0, true},
		{"no credentials on the platform", 0, 0, errNoPeerCredentials, 1000, true, 0, true},
		{"no credentials where it is not enforced", 0, 0, errNoPeerCredentials, -1, false, -1, false},
		{"another error where it is not enforced", 0, 0, boom, -1, false, 0, true},
	} {
		peer, err := judgePeer(tc.uid, tc.pid, tc.err, tc.euid, tc.enforced)
		if (err != nil) != tc.denied {
			t.Errorf("%s: err = %v, want denied %v", tc.name, err, tc.denied)
			continue
		}
		if !tc.denied && (peer.UID != tc.wantUID || (tc.err == nil && peer.PID != tc.pid)) {
			t.Errorf("%s: peer = %+v, want uid %d", tc.name, peer, tc.wantUID)
		}
		if tc.denied && !strings.Contains(err.Error(), "uid") && tc.err == nil {
			t.Errorf("%s: message %q does not name the uids", tc.name, err)
		}
	}
}

// TestSameUserOnly (spec §7 row 29): a same-user connection is accepted and its uid logged; where
// the platform has no credentials the entry carries none. The other-uid connection over a real
// socket is plan L4's (TestSameUserOnlyOtherUID).
func TestSameUserOnly(t *testing.T) {
	f := startFixture(t, archiveApplies)
	if got := f.ask("batch_delete_size=100"); !strings.HasPrefix(got, "OK\n") {
		t.Fatalf("a same-user setter = %q, want it accepted", got)
	}
	_ = f.srv.Close()
	got := f.sink.snapshot()
	if len(got) != 1 {
		t.Fatalf("entries = %q, want the one change", f.sink.messages())
	}
	if runtime.GOOS == "windows" {
		if _, has := got[0].kv["uid"]; has {
			t.Error("an entry on Windows carries a uid")
		}
		return
	}
	if got[0].kv["uid"] != os.Geteuid() || got[0].kv["pid"] != os.Getpid() {
		t.Errorf("entry fields = %v, want uid %d and pid %d: the kernel's record of the peer", got[0].kv, os.Geteuid(), os.Getpid())
	}
	if !strings.Contains(got[0].msg, "uid") {
		t.Errorf("entry = %q, want the uid in it", got[0].msg)
	}
}

// TestDeniedPeerIsRefusedAndLogged: a connection judged `denied` gets ERR denied, changes nothing,
// and is logged with its uid. The judgement here is made by a store-less path: the server's own peer
// check is replaced by the test's, through the one seam the check has.
func TestDeniedPeerIsRefusedAndLogged(t *testing.T) {
	f := startFixture(t, archiveApplies, func(o *Options) {
		o.tune.peer = func(net.Conn) (tuning.Peer, error) { return tuning.Peer{}, &peerDeniedError{uid: 4321, euid: 1000} }
	})
	got := f.ask("batch_size=10")
	if !strings.HasPrefix(got, "ERR denied ") || strings.Count(got, "\n") != 1 {
		t.Errorf("reply = %q, want one ERR denied line", got)
	}
	if f.store.Load().Revision != 0 {
		t.Error("a denied request changed the store")
	}
	_ = f.srv.Close()
	var found bool
	for _, m := range f.sink.messages() {
		if strings.Contains(m, "denied") && strings.Contains(m, "4321") {
			found = true
		}
	}
	if !found {
		t.Errorf("entries = %q, want the refusal with the uid", f.sink.messages())
	}
	if sum := f.store.Terminal(); len(sum.Settings) != 0 {
		t.Errorf("summary = %+v", sum.Settings)
	}
}
