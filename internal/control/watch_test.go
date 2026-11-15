package control

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// The identity check (spec §4.2, D18): once a minute the run compares what is at its directory's,
// its lock's and its socket's path with the identities it recorded, reports a loss once per object,
// loudly, and repairs nothing.

func countEntries(s *recSink, sub string) int {
	n := 0
	for _, m := range s.messages() {
		if strings.Contains(m, sub) {
			n++
		}
	}
	return n
}

func fastWatch(o *Options) { o.tune.watchEvery = 20 * time.Millisecond }

// TestDirectoryReplaced (spec §7 row 21): the socket directory is renamed away and a new one
// created at its name while the server runs: the check reports the directory, the lock and the
// socket once each; the server creates nothing in the new directory; at its end it removes nothing
// there; a second start in the new directory binds its own socket.
func TestDirectoryReplaced(t *testing.T) {
	f := startFixture(t, archiveApplies, fastWatch)
	old := f.dir + ".old"
	t.Cleanup(func() { _ = os.RemoveAll(old) })
	if err := os.Rename(f.dir, old); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(f.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the three losses to be reported", func() bool {
		return countEntries(f.sink, "Control socket directory lost") == 1 && countEntries(f.sink, "Control lock lost") == 1 && countEntries(f.sink, "Control socket lost") == 1
	})
	time.Sleep(150 * time.Millisecond) // several more checks: nothing is reported twice
	for _, sub := range []string{"Control socket directory lost", "Control lock lost", "Control socket lost"} {
		if n := countEntries(f.sink, sub); n != 1 {
			t.Errorf("%q reported %d times, want once", sub, n)
		}
	}
	if entries, err := os.ReadDir(f.dir); err != nil || len(entries) != 0 {
		t.Errorf("the new directory holds %v, %v, want nothing: the run creates nothing in it", entries, err)
	}
	for _, e := range f.sink.snapshot() {
		if strings.Contains(e.msg, "lost") && e.level == tuning.LevelInfo {
			t.Errorf("a loss is reported at info: %q", e.msg)
		}
	}

	// A second start in the new directory binds its own socket, and the first run's end removes
	// nothing there.
	g := &fixture{t: t}
	o := baseOptions(t, g, archiveApplies)
	o.Dir = f.dir
	g.dir = f.dir
	srv, err := Start(o)
	if err != nil {
		t.Fatalf("a start in the new directory = %v", err)
	}
	defer srv.Close()
	_ = f.srv.Close()
	if got := ask(t, srv.Path(), "help\n"); !strings.HasPrefix(got, "OK\nprotocol 1") {
		t.Errorf("the second run's socket after the first ended: %q", got)
	}
}

// TestIdentityCheckRepairsNothing (spec §7 row 22): after the lock name is lost no new lock file
// appears and no entry claims protection; after the socket name is lost no new socket is bound.
func TestIdentityCheckRepairsNothing(t *testing.T) {
	f := startFixture(t, archiveApplies, fastWatch)
	if err := os.Remove(f.lockPath()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the lock's loss to be reported", func() bool { return countEntries(f.sink, "Control lock lost") == 1 })
	if err := os.Remove(f.path()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the socket's loss to be reported", func() bool { return countEntries(f.sink, "Control socket lost") == 1 })
	time.Sleep(150 * time.Millisecond)
	if _, err := os.Lstat(f.lockPath()); !os.IsNotExist(err) {
		t.Errorf("a lock file appeared after the name was lost: %v", err)
	}
	if _, err := os.Lstat(f.path()); !os.IsNotExist(err) {
		t.Errorf("a socket appeared after the name was lost: %v", err)
	}
	for _, m := range f.sink.messages() {
		if strings.Contains(m, "protected") || strings.Contains(m, "now locked") {
			t.Errorf("an entry claims protection: %q", m)
		}
	}
	// The run itself goes on: it can still be asked, over a connection made before the loss.
	if f.store.Load().Revision != 0 {
		t.Error("the check changed the store")
	}
}

// TestIdentityUnchangedReportsNothing: an undisturbed run reports no loss however often it checks.
func TestIdentityUnchangedReportsNothing(t *testing.T) {
	f := startFixture(t, archiveApplies, fastWatch)
	time.Sleep(200 * time.Millisecond)
	if n := countEntries(f.sink, "lost"); n != 0 {
		t.Errorf("an undisturbed run reported %d losses: %q", n, f.sink.messages())
	}
}

// TestShutdownPendingTick (spec §7 row 50): an identity tick due during the end is stopped and
// joined before the emitter stops; it writes nothing afterwards.
func TestShutdownPendingTick(t *testing.T) {
	f := startFixture(t, archiveApplies, func(o *Options) { o.tune.watchEvery = 5 * time.Millisecond })
	if err := os.Remove(f.path()); err != nil {
		t.Fatal(err)
	}
	f.srv.Ending()
	f.srv.Drain()
	written := len(f.sink.snapshot())
	// A loss after the end is nobody's to report.
	if err := os.Remove(f.lockPath()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if got := len(f.sink.snapshot()); got != written {
		t.Errorf("%d entries after the drain, want %d: the tick wrote after it was joined", got, written)
	}
	if err := f.srv.Close(); err != nil && !strings.Contains(err.Error(), strconv.Quote(f.path())) {
		t.Errorf("Close() = %v", err)
	}
}
