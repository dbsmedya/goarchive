package control

import (
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// The server: serving requests on a real Unix socket, the request boundary, the deadlines, the
// handler limit and the end (spec §4.2 Serve and End, §4.3). The listener is real, in a short
// temporary directory the test owns; nothing is faked but the sources of a status and the sink.

type fixture struct {
	t       *testing.T
	srv     *Server
	dir     string
	store   *tuning.Store
	stops   *stopRecorder
	sink    *recSink
	banner  *recBanner
	tracker *fakeTracker
}

// baseOptions are the options of a server of a run of the given kind, in its own short directory.
func baseOptions(t *testing.T, f *fixture, applies tuning.Applicability) Options {
	t.Helper()
	f.store, f.stops = newTestStore(t, applies)
	f.sink, f.banner, f.tracker = newRecSink(), &recBanner{}, &fakeTracker{}
	f.tracker.set(TrackerStatus{Phase: PhaseRunning})
	f.dir = shortDir(t)
	o := Options{
		Job: "job", Dir: f.dir, Euid: os.Geteuid(), Store: f.store,
		Info: Info{
			Version: "3.1.0", Job: "job", JobType: "archive", ConfigFile: "/etc/job.yaml",
			PID: os.Getpid(), StartedAt: time.Now(),
		},
		Tracker: f.tracker, Shutdown: &fakeShutdown{}, Sink: f.sink, Banner: f.banner.banner,
		tune: timings{watchEvery: time.Hour},
	}
	if applies.Gate {
		o.Gate = &fakeGate{}
	}
	return o
}

// startFixture starts a server of a run of the given kind; mod may change its options first.
func startFixture(t *testing.T, applies tuning.Applicability, mod ...func(*Options)) *fixture {
	t.Helper()
	f := &fixture{t: t}
	o := baseOptions(t, f, applies)
	for _, m := range mod {
		m(&o)
	}
	srv, err := Start(o)
	if err != nil {
		t.Fatalf("Start() = %v", err)
	}
	f.srv = srv
	t.Cleanup(func() { f.sink.release(); _ = srv.Close() })
	return f
}

func (f *fixture) path() string { return filepath.Join(f.dir, "job.sock") }

func (f *fixture) lockPath() string { return filepath.Join(f.dir, "job.lock") }

// ask makes one connection, sends payload as it is, and returns everything the server answers up
// to the end of the stream. A reset after the reply (the server closes with input unread, spec §9
// item 9) is not an error: what was read is the reply.
func ask(t *testing.T, path, payload string) string {
	t.Helper()
	c, err := net.DialTimeout("unix", path, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", path, err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	_, _ = c.Write([]byte(payload))
	b, _ := io.ReadAll(c)
	return string(b)
}

func (f *fixture) ask(request string) string { return ask(f.t, f.path(), request+"\n") }

// refusals counts the entries the sink has that are about a refused request.
func refusals(s *recSink) int {
	n := 0
	for _, m := range s.messages() {
		if strings.Contains(m, "refused") {
			n++
		}
	}
	return n
}

// TestServerAnswersEveryCommand: a request on the socket gets the command table's reply, framed.
func TestServerAnswersEveryCommand(t *testing.T) {
	f := startFixture(t, archiveApplies)
	if got := f.ask("help"); !strings.HasPrefix(got, "OK\nprotocol 1\n") {
		t.Errorf("help = %q", got)
	}
	if got := f.ask("batch_delete_size=100"); got != "OK\nbatch_delete_size: 500 -> 100, from the next DELETE\n" {
		t.Errorf("setter = %q", got)
	}
	if got := f.ask("batch_size=5000"); !strings.HasPrefix(got, "ERR ceiling batch_size 5000 is above this run's configured 1000;") {
		t.Errorf("a size above the ceiling = %q", got)
	}
	if got := f.ask("frobnicate"); !strings.HasPrefix(got, "ERR unknown ") || !strings.HasSuffix(got, "\n") {
		t.Errorf("unknown = %q", got)
	}
	if got := f.ask("status-json"); !strings.HasPrefix(got, "OK\n{") || strings.Count(got, "\n") != 2 || !strings.Contains(got, `"protocol":1`) {
		t.Errorf("status-json = %q", got)
	}
	if got := f.ask("status"); !strings.HasPrefix(got, "OK\njob (archive): running,") {
		t.Errorf("status = %q", got)
	}
}

// TestRequestBoundary (spec §7 rows 2 to 5): the request ends at its first LF, whatever the
// client's writes and half-close do.
func TestRequestBoundary(t *testing.T) {
	t.Run("one write", func(t *testing.T) {
		f := startFixture(t, archiveApplies)
		got := ask(t, f.path(), "batch_delete_size=100\nbatch_size=1\n")
		if got != "OK\nbatch_delete_size: 500 -> 100, from the next DELETE\n" {
			t.Errorf("reply = %q, want the first line's answer alone", got)
		}
		snap := f.store.Load()
		if snap.BatchDeleteSize != 100 || snap.BatchSize != 1000 || snap.Revision != 1 {
			t.Errorf("snapshot = delete %d, size %d, revision %d, want 100, 1000, 1: the second line is neither applied nor answered", snap.BatchDeleteSize, snap.BatchSize, snap.Revision)
		}
		_ = f.srv.Close()
		if n := refusals(f.sink); n != 0 {
			t.Errorf("%d refusal entries, want none: the second line is not a request", n)
		}
		if got := len(f.sink.messages()); got != 1 {
			t.Errorf("entries = %q, want the one change", f.sink.messages())
		}
	})

	t.Run("split writes", func(t *testing.T) {
		f := startFixture(t, archiveApplies)
		c, err := net.Dial("unix", f.path())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(10 * time.Second))
		if _, err := c.Write([]byte("batch_delete_size=100\n")); err != nil {
			t.Fatal(err)
		}
		waitFor(t, "the first line to be accepted", func() bool { return f.store.Load().Revision == 1 })
		// The server may already have replied and closed: this write, or the read, may fail as
		// the platform does (spec §9 item 9). That is allowed; no wait for further input is added.
		_, _ = c.Write([]byte("batch_size=1\n"))
		b, _ := io.ReadAll(c)
		if reply := string(b); reply != "" && reply != "OK\nbatch_delete_size: 500 -> 100, from the next DELETE\n" {
			t.Errorf("reply = %q, want only the first request's", reply)
		}
		snap := f.store.Load()
		if snap.BatchDeleteSize != 100 || snap.BatchSize != 1000 || snap.Revision != 1 {
			t.Errorf("snapshot = delete %d, size %d, revision %d, want exactly the first change", snap.BatchDeleteSize, snap.BatchSize, snap.Revision)
		}
		_ = f.srv.Close()
		if n := refusals(f.sink); n != 0 || len(f.sink.messages()) != 1 {
			t.Errorf("entries = %q, want the one change and no refusal", f.sink.messages())
		}
		if sum := f.store.Terminal(); len(sum.Settings) != 1 || sum.Settings[0].Changes != 1 {
			t.Errorf("summary = %+v, want the one change", sum.Settings)
		}
	})

	t.Run("half close", func(t *testing.T) {
		f := startFixture(t, archiveApplies)
		for _, closeWrite := range []bool{true, false} {
			c, err := net.Dial("unix", f.path())
			if err != nil {
				t.Fatal(err)
			}
			_ = c.SetDeadline(time.Now().Add(10 * time.Second))
			began := time.Now()
			if _, err := c.Write([]byte("batch_size=?\n")); err != nil {
				t.Fatal(err)
			}
			if closeWrite {
				if err := c.(*net.UnixConn).CloseWrite(); err != nil {
					t.Fatal(err)
				}
			}
			b, _ := io.ReadAll(c)
			_ = c.Close()
			if string(b) != "OK\nbatch_size=1000\n" || time.Since(began) > time.Second {
				t.Errorf("close write %v: reply %q after %v, want the reply at once", closeWrite, b, time.Since(began))
			}
		}
	})

	t.Run("no LF", func(t *testing.T) {
		f := startFixture(t, archiveApplies, func(o *Options) { o.tune.connDeadline = 400 * time.Millisecond })
		c, err := net.Dial("unix", f.path())
		if err != nil {
			t.Fatal(err)
		}
		_, _ = c.Write([]byte("batch_size=10"))
		_ = c.(*net.UnixConn).CloseWrite()
		b, _ := io.ReadAll(c)
		_ = c.Close()
		if !strings.HasPrefix(string(b), "ERR malformed ") {
			t.Errorf("a line with no LF and the end of input = %q, want ERR malformed", b)
		}
		if got := ask(t, f.path(), strings.Repeat("a", 4096)); !strings.HasPrefix(got, "ERR malformed ") {
			t.Errorf("4096 bytes with no LF = %q, want ERR malformed", got)
		}
		// A line with no LF and no end of input is closed at the deadline, with no reply.
		c, err = net.Dial("unix", f.path())
		if err != nil {
			t.Fatal(err)
		}
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		began := time.Now()
		_, _ = c.Write([]byte("batch_size=10"))
		b, _ = io.ReadAll(c)
		_ = c.Close()
		if len(b) != 0 || time.Since(began) < 300*time.Millisecond {
			t.Errorf("a line with no LF held open: reply %q after %v, want it closed at the deadline with no reply", b, time.Since(began))
		}
		if f.store.Load().Revision != 0 {
			t.Error("a request without an LF changed the store")
		}
	})
}

// TestOverLongLine (spec §7 row 11): a line above 4096 bytes is `ERR malformed`, never truncated
// and run.
func TestOverLongLine(t *testing.T) {
	f := startFixture(t, archiveApplies)
	got := ask(t, f.path(), "batch_size="+strings.Repeat("1", 5000)+"\n")
	if !strings.HasPrefix(got, "ERR malformed ") || strings.Count(got, "\n") != 1 || len(got) > 400 {
		t.Errorf("reply = %q, want one short ERR malformed line", got)
	}
	if f.store.Load().Revision != 0 {
		t.Error("an over-long line changed the store")
	}
}

// TestRequestLimitIgnoresTailOverAConnection (Codex r5, the connection half of TestRequestLimitIgnoresTail): a valid first line with a tail that
// takes the total above 4096 bytes is accepted and processed once.
func TestRequestLimitIgnoresTailOverAConnection(t *testing.T) {
	f := startFixture(t, archiveApplies)
	got := ask(t, f.path(), "pause\n"+strings.Repeat("x", 6000))
	if !strings.HasPrefix(got, "OK\n") {
		t.Errorf("reply = %q, want OK", got)
	}
	if snap := f.store.Load(); !snap.Paused || snap.Revision != 1 {
		t.Errorf("snapshot = paused %v, revision %d, want one pause", snap.Paused, snap.Revision)
	}
}

// TestSilentClient (spec §7 row 12): a connection that sends nothing is closed at the deadline.
func TestSilentClient(t *testing.T) {
	f := startFixture(t, archiveApplies, func(o *Options) { o.tune.connDeadline = 300 * time.Millisecond })
	c, err := net.Dial("unix", f.path())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	began := time.Now()
	b, _ := io.ReadAll(c)
	if took := time.Since(began); len(b) != 0 || took < 250*time.Millisecond || took > 3*time.Second {
		t.Errorf("a silent client: %q after %v, want it closed with no reply at about 300 ms", b, took)
	}
}

// TestHandlerLimit: at most the configured number of connections are handled at once; the rest wait
// in the listen queue and are served when a handler is free.
func TestHandlerLimit(t *testing.T) {
	f := startFixture(t, archiveApplies, func(o *Options) {
		o.tune.handlers = 2
		o.tune.connDeadline = 800 * time.Millisecond
	})
	var silent []net.Conn
	for range 2 {
		c, err := net.Dial("unix", f.path())
		if err != nil {
			t.Fatal(err)
		}
		silent = append(silent, c)
	}
	defer func() {
		for _, c := range silent {
			c.Close()
		}
	}()
	time.Sleep(100 * time.Millisecond) // both are in handlers by now
	began := time.Now()
	got := ask(t, f.path(), "batch_size=?\n")
	if took := time.Since(began); got != "OK\nbatch_size=1000\n" || took < 400*time.Millisecond {
		t.Errorf("a third request: %q after %v, want it to wait for a free handler (about 700 ms) and then be served", got, took)
	}
}

// panicTracker is a source whose read panics: a fault inside a handler, as the design calls for
// being survived.
type panicTracker struct{}

func (panicTracker) Status() TrackerStatus { panic("tracker read failed") }

// TestHandlerPanicIsRecovered (spec §4.2 Serve): a panic in a handler is recovered, reported through
// the emitter as an error, and closes that connection only: the server goes on serving.
func TestHandlerPanicIsRecovered(t *testing.T) {
	f := startFixture(t, archiveApplies, func(o *Options) { o.Tracker = panicTracker{} })
	if got := f.ask("status"); got != "" {
		t.Errorf("a request whose handler panicked got %q, want the connection closed with no reply", got)
	}
	if got := f.ask("help"); !strings.HasPrefix(got, "OK\nprotocol 1") {
		t.Errorf("the server did not go on serving after a panic: %q", got)
	}
	_ = f.srv.Close()
	var found bool
	for _, e := range f.sink.snapshot() {
		if e.level == tuning.LevelError && strings.Contains(e.msg, "panicked") && strings.Contains(e.msg, "tracker read failed") {
			found = true
		}
	}
	if !found {
		t.Errorf("entries = %q, want an error entry for the panic", f.sink.messages())
	}
}

// TestNextBackoff: an accept error is retried after a few milliseconds, doubling to one second.
func TestNextBackoff(t *testing.T) {
	d := time.Duration(0)
	var got []time.Duration
	for range 10 {
		d = nextBackoff(d)
		got = append(got, d)
	}
	want := []time.Duration{5, 10, 20, 40, 80, 160, 320, 640, 1000, 1000}
	for i := range want {
		if got[i] != want[i]*time.Millisecond {
			t.Fatalf("backoff = %v, want %v", got, want)
		}
	}
}

// TestHandlersDoNoOutput (spec §7 row 48): a handler serving a refused setter returns within the
// network deadline while the log's writer is held shut: it writes to the socket only.
func TestHandlersDoNoOutput(t *testing.T) {
	f := startFixture(t, archiveApplies)
	f.sink.block()
	began := time.Now()
	got := f.ask("batch_size=0")
	if !strings.HasPrefix(got, "ERR invalid ") || time.Since(began) > 2*time.Second {
		t.Errorf("a refused setter with the log held shut: %q after %v, want ERR invalid at once", got, time.Since(began))
	}
	for range 20 {
		_ = f.ask("batch_size=0")
	}
	if got := f.ask("status"); !strings.HasPrefix(got, "OK\n") {
		t.Errorf("status = %q", got)
	}
}

// TestShutdown (spec §7 row 14): Close ends the accept loop without spinning, waits for the
// handlers, removes the socket file and releases the lock.
func TestShutdown(t *testing.T) {
	f := startFixture(t, archiveApplies, func(o *Options) { o.tune.connDeadline = 500 * time.Millisecond })
	idle, err := net.Dial("unix", f.path())
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	time.Sleep(50 * time.Millisecond) // the handler is waiting for the idle connection's line
	began := time.Now()
	if err := f.srv.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	if took := time.Since(began); took > 2*time.Second {
		t.Errorf("Close took %v with one idle connection, want it within the deadline", took)
	}
	select {
	case <-f.srv.acceptDone:
	default:
		t.Error("the accept loop is still running after Close")
	}
	if _, err := os.Lstat(f.path()); !os.IsNotExist(err) {
		t.Errorf("the socket file is still there after Close: %v", err)
	}
	if _, err := os.Lstat(f.lockPath()); err != nil {
		t.Errorf("the lock file is gone after Close, want it kept: %v", err)
	}
	lock, err := os.OpenFile(f.lockPath(), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := lockFile(lock); err != nil {
		t.Errorf("the lock is still held after Close: %v", err)
	}
	if err := f.srv.Close(); err != nil {
		t.Errorf("a second Close = %v, want nil", err)
	}
	if _, err := net.DialTimeout("unix", f.path(), time.Second); err == nil {
		t.Error("a connection was made after Close")
	}
}

// TestEndRemovesOwnSocket (spec §7 row 15): closing the listener by itself removes nothing
// (auto-unlink is off); Close removes this run's socket; and when the socket path was replaced by a
// regular file the file is left in place and reported.
func TestEndRemovesOwnSocket(t *testing.T) {
	t.Run("listener close alone", func(t *testing.T) {
		f := startFixture(t, archiveApplies)
		if err := f.srv.listener.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(f.path()); err != nil {
			t.Errorf("closing the listener removed the socket: %v", err)
		}
		if err := f.srv.Close(); err != nil {
			t.Errorf("Close() = %v", err)
		}
		if _, err := os.Lstat(f.path()); !os.IsNotExist(err) {
			t.Errorf("Close left this run's own socket: %v", err)
		}
	})

	t.Run("replaced by a regular file", func(t *testing.T) {
		f := startFixture(t, archiveApplies)
		if err := os.Remove(f.path()); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(f.path(), []byte("someone else's"), 0o600); err != nil {
			t.Fatal(err)
		}
		err := f.srv.Close()
		if err == nil || !strings.Contains(err.Error(), strconv.Quote(f.path())) {
			t.Errorf("Close() = %v, want an error naming the file it left (quoted: %s)", err, strconv.Quote(f.path()))
		}
		if b, rerr := os.ReadFile(f.path()); rerr != nil || string(b) != "someone else's" {
			t.Errorf("the file is %q, %v after Close, want it left as it was", b, rerr)
		}
	})
}

// TestEndAfterTakeover (spec §7 row 16): instance A's lock name and socket name are removed;
// instance B starts and binds the same path; A ends: B's socket is still there and still answers,
// because A removes by identity.
func TestEndAfterTakeover(t *testing.T) {
	skipUnlessHeldFilesRemovable(t)
	a := startFixture(t, archiveApplies)
	if err := os.Remove(a.lockPath()); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(a.path()); err != nil {
		t.Fatal(err)
	}
	b := startFixture(t, archiveApplies, func(o *Options) { o.Dir = a.dir })
	b.dir = a.dir
	_ = a.srv.Close()
	if got := ask(t, b.path(), "help\n"); !strings.HasPrefix(got, "OK\nprotocol 1") {
		t.Errorf("B's socket after A ended: %q, want it still answering", got)
	}
	// B's own end removes it.
	if err := b.srv.Close(); err != nil {
		t.Errorf("B's Close() = %v", err)
	}
	if _, err := os.Lstat(b.path()); !os.IsNotExist(err) {
		t.Errorf("B's socket is left after B ended: %v", err)
	}
}

// TestPartialStart (spec §7 row 17): a start that fails after its bind removes the socket it
// created and releases the lock.
func TestPartialStart(t *testing.T) {
	f := &fixture{t: t}
	o := baseOptions(t, f, archiveApplies)
	o.tune.afterBind = func() error { return errors.New("a step after the bind failed") }
	if _, err := Start(o); err == nil || !strings.Contains(err.Error(), "a step after the bind failed") {
		t.Fatalf("Start() = %v, want the step's error", err)
	}
	if _, err := os.Lstat(f.path()); !os.IsNotExist(err) {
		t.Errorf("the socket it created is left: %v", err)
	}
	lock, err := os.OpenFile(f.lockPath(), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("the lock file: %v", err)
	}
	if err := lockFile(lock); err != nil {
		t.Errorf("the lock is still held after the failed start: %v", err)
	}
	_ = lock.Close() // the test's own hold, released before the next start
	// And the same directory is usable at once.
	o.tune.afterBind = nil
	srv, err := Start(o)
	if err != nil {
		t.Fatalf("a start after the failed one: %v", err)
	}
	_ = srv.Close()
}

// TestShutdownStatusServed (spec §7 row 52) and TestTerminalStatus (row 55): between the terminal
// transition and the listener's close, status answers `ending` and a setter gets `ERR ending`; and
// status goes on answering `ending` until the socket closes.
func TestShutdownStatusServed(t *testing.T) {
	f := startFixture(t, archiveApplies)
	sum := f.srv.Ending()
	if len(sum.Settings) != 0 {
		t.Errorf("summary = %+v, want none", sum.Settings)
	}
	for range 2 {
		if got := f.ask("status-json"); !strings.Contains(got, `"state":"ending"`) {
			t.Errorf("status-json after the terminal transition = %q, want state ending", got)
		}
	}
	if got := f.ask("status"); !strings.Contains(got, "ending") {
		t.Errorf("status = %q", got)
	}
	for _, line := range []string{"batch_size=10", "pause", "resume", "stop"} {
		if got := f.ask(line); !strings.HasPrefix(got, "ERR ending ") {
			t.Errorf("%s after the terminal transition = %q, want ERR ending", line, got)
		}
	}
	if got := f.ask("batch_size=?"); got != "OK\nbatch_size=1000\n" {
		t.Errorf("a getter after the terminal transition = %q, want it answered", got)
	}
	f.srv.Drain() // the emitter has finished; the socket still answers until Close
	if got := f.ask("status-json"); !strings.Contains(got, `"state":"ending"`) {
		t.Errorf("status-json after the drain = %q, want state ending", got)
	}
	if err := f.srv.Close(); err != nil {
		t.Errorf("Close() = %v", err)
	}
	if _, err := net.DialTimeout("unix", f.path(), time.Second); err == nil {
		t.Error("the socket still accepts after Close")
	}
}

// TestTerminalUnsentRequest (spec §7 row 53): a connection accepted before the terminal
// transition sends its setter after it: `ERR ending`, not in the summary, and Close returns within
// the deadline.
func TestTerminalUnsentRequest(t *testing.T) {
	f := startFixture(t, archiveApplies, func(o *Options) { o.tune.connDeadline = time.Second })
	c, err := net.Dial("unix", f.path())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	time.Sleep(50 * time.Millisecond) // accepted: its handler is waiting for the line
	sum := f.srv.Ending()
	if _, err := c.Write([]byte("batch_size=10\n")); err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(c)
	if !strings.HasPrefix(string(b), "ERR ending ") {
		t.Errorf("a setter sent after the terminal transition = %q, want ERR ending", b)
	}
	if len(sum.Settings) != 0 || f.store.Load().Revision != 0 {
		t.Errorf("summary %+v, revision %d: the refused setter must not be in them", sum.Settings, f.store.Load().Revision)
	}
	began := time.Now()
	if err := f.srv.Close(); err != nil {
		t.Errorf("Close() = %v", err)
	}
	if took := time.Since(began); took > 2*time.Second {
		t.Errorf("Close took %v, want it within the deadline", took)
	}
}

// TestStopOverTheSocket: a stop over the socket calls the store's RequestStop once, and status says
// stopping.
func TestStopOverTheSocket(t *testing.T) {
	f := startFixture(t, archiveApplies)
	if got := f.ask("stop"); !strings.HasPrefix(got, "OK\nstop requested") {
		t.Errorf("stop = %q", got)
	}
	if got := f.ask("stop"); !strings.Contains(got, "already stopping") {
		t.Errorf("second stop = %q", got)
	}
	if f.stops.calls() != 1 {
		t.Errorf("RequestStop calls = %d, want 1", f.stops.calls())
	}
	if got := f.ask("status-json"); !strings.Contains(got, `"state":"stopping"`) || !strings.Contains(got, `"stop_requested":true`) {
		t.Errorf("status-json = %q, want stopping", got)
	}
}

// TestRefusalsAreLoggedBestEffort: a refused setter is a warning entry, through the emitter, with
// the request quoted and cut to 64 bytes; a status read and an unchanged set are not entries.
func TestRefusalsAreLoggedBestEffort(t *testing.T) {
	f := startFixture(t, archiveApplies)
	_ = f.ask("batch_size=5000")
	_ = f.ask("batch_size=1000") // unchanged
	_ = f.ask("status")
	_ = f.ask("status-json")
	_ = f.ask("batch_size=?")
	long := "x" + strings.Repeat("y", 200)
	_ = ask(t, f.path(), long+"\n")
	_ = f.srv.Close()

	var refused []sinkEntry
	for _, e := range f.sink.snapshot() {
		if strings.Contains(e.msg, "refused") {
			refused = append(refused, e)
		}
	}
	if len(refused) != 2 {
		t.Fatalf("entries = %q, want two refusals and nothing for the reads or the unchanged set", f.sink.messages())
	}
	for _, e := range refused {
		if e.level != tuning.LevelWarn {
			t.Errorf("a refusal is logged at %v, want warn", e.level)
		}
	}
	if !strings.Contains(refused[0].msg, "ceiling") || !strings.Contains(refused[0].msg, `"batch_size=5000"`) {
		t.Errorf("first refusal = %q, want the reason and the quoted request", refused[0].msg)
	}
	if strings.Contains(refused[1].msg, strings.Repeat("y", 100)) || !strings.Contains(refused[1].msg, "...") {
		t.Errorf("a long request is logged as %q, want it cut to 64 bytes", refused[1].msg)
	}
}

// TestSourcesAreRead: the status the server answers is built from the sources the options carry.
func TestSourcesAreRead(t *testing.T) {
	f := startFixture(t, archiveApplies)
	f.tracker.set(TrackerStatus{Phase: PhaseRunning, Step: "copy", Table: "orders", Batches: 3, RowsCopied: 99})
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				if got := f.ask("status"); !strings.Contains(got, "copied 99") || !strings.Contains(got, "copy on orders") {
					t.Errorf("status = %q", got)
				}
			}
		}()
	}
	wg.Wait()
}

// TestInUseErrorQuotesPaths (Codex batch 2 finding 5): the contention message names the lock or socket
// path and the directory with %q, as the startup messages do: a Windows path with spaces and backslashes
// is unambiguous, and a path with a control character cannot split the message into lines. The job's name
// was already quoted.
func TestInUseErrorQuotesPaths(t *testing.T) {
	for _, tc := range []struct{ path, dir string }{
		{`C:\Users\run ner\job.lock`, `C:\Users\run ner`},
		{"/tmp/a\nb/job.lock", "/tmp/a\nb"},
	} {
		for _, lock := range []bool{true, false} {
			msg := (&InUseError{Path: tc.path, Lock: lock, Job: "j", Dir: tc.dir}).Error()
			for _, want := range []string{strconv.Quote(tc.path), strconv.Quote(tc.dir), `the job "j"`, "already uses this socket directory"} {
				if !strings.Contains(msg, want) {
					t.Errorf("lock=%v: %q lacks %s", lock, msg, want)
				}
			}
			if strings.ContainsAny(msg, "\n\r") {
				t.Errorf("lock=%v: the message holds a raw line break: %q", lock, msg)
			}
		}
	}
}
