package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/control"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// The client's tests run through Main, as the CLI's tests do, so they prove the exit code and both
// output streams, not an inner function (spec §7). "A test listener" is a listener the test itself
// opens on a socket to answer as a run, a broken run or a hostile one would; it is test code, not a
// fault put into GoArchive.

// runMain runs the CLI on args and returns its exit code and what it wrote to each stream.
func runMain(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	resetFlags(rootCmd)
	t.Cleanup(func() {
		resetFlags(rootCmd)
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
		rootCmd.SetArgs(nil)
	})
	var out, errb bytes.Buffer
	code = Main(args, &out, &errb)
	return code, out.String(), errb.String()
}

// testListener is a listener on a socket in a directory the client accepts: made by the test's own
// user, mode 0700. It counts the connections it gets and records what each sent.
type testListener struct {
	t    *testing.T
	Dir  string
	Path string
	l    *net.UnixListener

	mu       sync.Mutex
	conns    []net.Conn
	requests []string // each connection's bytes up to and including its first LF
	extra    []string // anything a connection sent after that, within a short grace
}

// newListenerAt binds path with handler serving every connection.
func newListenerAt(t *testing.T, path string, handler func(*testListener, net.Conn)) *testListener {
	t.Helper()
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	tl := &testListener{t: t, Dir: filepath.Dir(path), Path: path, l: l}
	var wg sync.WaitGroup
	t.Cleanup(func() {
		_ = l.Close()
		tl.mu.Lock()
		for _, c := range tl.conns {
			_ = c.Close()
		}
		tl.mu.Unlock()
		wg.Wait()
	})
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			tl.mu.Lock()
			tl.conns = append(tl.conns, c)
			tl.mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				handler(tl, c)
			}()
		}
	}()
	return tl
}

// newListener binds <a new short private directory>/j.sock.
func newListener(t *testing.T, handler func(*testListener, net.Conn)) *testListener {
	t.Helper()
	return newListenerAt(t, filepath.Join(shortTemp(t), "j.sock"), handler)
}

func (tl *testListener) connections() int {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	return len(tl.conns)
}

func (tl *testListener) sent() (requests, extra []string) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	return append([]string(nil), tl.requests...), append([]string(nil), tl.extra...)
}

// readRequest reads c to its first LF, as a run does, and records it.
func (tl *testListener) readRequest(c net.Conn) {
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	var line []byte
	buf := make([]byte, 1)
	for {
		n, err := c.Read(buf)
		if n > 0 {
			line = append(line, buf[0])
			if buf[0] == '\n' {
				break
			}
		}
		if err != nil {
			break
		}
	}
	tl.mu.Lock()
	tl.requests = append(tl.requests, string(line))
	tl.mu.Unlock()
}

// drainExtra records what the connection sends after its first line, for a short grace.
func (tl *testListener) drainExtra(c net.Conn) {
	_ = c.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	rest, _ := readConn(c)
	if len(rest) > 0 {
		tl.mu.Lock()
		tl.extra = append(tl.extra, string(rest))
		tl.mu.Unlock()
	}
}

func readConn(c net.Conn) ([]byte, error) {
	var out []byte
	buf := make([]byte, 512)
	for {
		n, err := c.Read(buf)
		out = append(out, buf[:n]...)
		if err != nil {
			return out, err
		}
	}
}

// replying reads the request line, writes reply, notes anything more and closes: a run's way.
func replying(reply string) func(*testListener, net.Conn) {
	return func(tl *testListener, c net.Conn) {
		tl.readRequest(c)
		_, _ = c.Write([]byte(reply))
		tl.drainExtra(c)
		_ = c.Close()
	}
}

// silent accepts and says nothing, keeping the connection open until the test ends.
func silent(tl *testListener, c net.Conn) {
	_, _ = readConn(c)
}

// withLimits sets the client's bounds for one test.
func withLimits(t *testing.T, l controlLimits) {
	t.Helper()
	old := clientLimits
	clientLimits = l
	t.Cleanup(func() { clientLimits = old })
}

// forPath replaces the {path} marker in args.
func forPath(args []string, path string) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = strings.ReplaceAll(a, "{path}", path)
	}
	return out
}

// TestControlForms (spec §7 row 72): with the socket path first, an action reaches the server as
// written; `-s k=v` and `--set k=v` as `k=v`; `-g k` and `--get k` as `k=?`; byte for byte, one line,
// one connection. A flag may also come before the path. The longest line a run accepts goes through.
func TestControlForms(t *testing.T) {
	longest := strings.Repeat("a", control.MaxRequest-1)
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"help", []string{"{path}", "help"}, "help\n"},
		{"status", []string{"{path}", "status"}, "status\n"},
		{"status-json", []string{"{path}", "status-json"}, "status-json\n"},
		{"pause", []string{"{path}", "pause"}, "pause\n"},
		{"resume", []string{"{path}", "resume"}, "resume\n"},
		{"stop", []string{"{path}", "stop"}, "stop\n"},
		{"an unknown action is sent as it is", []string{"{path}", "frobnicate"}, "frobnicate\n"},
		{"set, short", []string{"{path}", "-s", "batch_delete_size=200"}, "batch_delete_size=200\n"},
		{"set, long", []string{"{path}", "--set", "sleep_seconds=0.5"}, "sleep_seconds=0.5\n"},
		{"set, long with =", []string{"{path}", "--set=batch_size=5"}, "batch_size=5\n"},
		{"set, flag first", []string{"-s", "batch_size=5", "{path}"}, "batch_size=5\n"},
		{"get, short", []string{"{path}", "-g", "batch_size"}, "batch_size=?\n"},
		{"get, long", []string{"{path}", "--get", "batch_size"}, "batch_size=?\n"},
		{"an unknown key is sent as it is", []string{"{path}", "-g", "nonesuch"}, "nonesuch=?\n"},
		{"the longest line", []string{"{path}", longest}, longest + "\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := newListener(t, replying("OK\nfine\n"))
			code, out, errs := runMain(t, append([]string{"control"}, forPath(tc.args, l.Path)...)...)
			if code != 0 || out != "fine\n" || errs != "" {
				t.Fatalf("exit %d, stdout %q, stderr %q, want 0, the body, nothing", code, out, errs)
			}
			reqs, extra := l.sent()
			if l.connections() != 1 || len(reqs) != 1 || reqs[0] != tc.want || len(extra) != 0 {
				t.Errorf("the listener saw %d connection(s), requests %q, more %q; want one connection and exactly %q",
					l.connections(), reqs, extra, tc.want)
			}
		})
	}
}

// TestControlUsageErrors (spec §7 row 73): each is exit 2, found before connecting: nothing is on
// stdout and the listener at the path saw no connection.
func TestControlUsageErrors(t *testing.T) {
	farPath := filepath.Join("/tmp", strings.Repeat("a", 200), "j.sock")
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"no socket path", nil, "socket path"},
		{"an empty socket path", []string{"", "status"}, "socket path"},
		{"no action and no flag", []string{"{path}"}, "needs an action"},
		{"an action with -s", []string{"{path}", "status", "-s", "a=1"}, "exactly one"},
		{"an action with -g", []string{"{path}", "status", "-g", "a"}, "exactly one"},
		{"-s with -g", []string{"{path}", "-s", "a=1", "-g", "b"}, "exactly one"},
		{"two -s", []string{"{path}", "-s", "a=1", "-s", "b=2"}, "once"},
		{"two --set", []string{"{path}", "--set", "a=1", "--set", "b=2"}, "once"},
		{"-s and --set", []string{"{path}", "-s", "a=1", "--set", "b=2"}, "once"},
		{"two -g", []string{"{path}", "-g", "a", "-g", "b"}, "once"},
		{"two --get", []string{"{path}", "--get", "a", "--get", "b"}, "once"},
		{"-g and --get", []string{"{path}", "-g", "a", "--get", "b"}, "once"},
		{"three positionals", []string{"{path}", "status", "extra"}, "too many"},
		{"a second positional with =", []string{"{path}", "batch_size=5"}, "use -s <key>=<value>"},
		{"-s without =", []string{"{path}", "-s", "k"}, "<key>=<value>"},
		{"-s with an empty key", []string{"{path}", "-s", "=v"}, "<key>=<value>"},
		{"-s with an empty value", []string{"{path}", "-s", "k="}, "<key>=<value>"},
		{"-g with an empty key", []string{"{path}", "-g", ""}, "-g needs"},
		{"-g with =", []string{"{path}", "-g", "a=b"}, "-g needs"},
		{"a socket path over the limit", []string{farPath, "status"}, "limit"},
		{"a line over 4096 bytes", []string{"{path}", strings.Repeat("a", control.MaxRequest)}, "4096"},
		{"a newline in the line", []string{"{path}", "sta\ntus"}, "newline"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := newListener(t, replying("OK\nnever\n"))
			code, out, errs := runMain(t, append([]string{"control"}, forPath(tc.args, l.Path)...)...)
			if code != 2 || out != "" || !strings.Contains(errs, tc.want) {
				t.Errorf("exit %d, stdout %q, stderr %q, want 2, nothing on stdout, and %q on stderr", code, out, errs, tc.want)
			}
			if n := l.connections(); n != 0 {
				t.Errorf("%d connection(s) made, want none: a usage error is found before connecting", n)
			}
		})
	}
}

// startRealServer starts the run's own control server on a socket in a short private directory, as a
// run does before it connects, and returns its path.
func startRealServer(t *testing.T) (string, *controlRun) {
	t.Helper()
	dir := filepath.Join(shortTemp(t), "run")
	ctl, err := startControl(testControlParams(t, "", controlYAML(dir)))
	if err != nil || ctl.srv == nil {
		t.Fatalf("startControl = %v, %v", ctl, err)
	}
	t.Cleanup(func() { _ = ctl.close() })
	return ctl.srv.Path(), ctl
}

// TestControlNamesAreTheRuns (spec §7 row 74): an unknown action and an unknown key are sent and come
// back as the run's ERR (exit 1); the client's help lists the actions and keys of the server's table.
func TestControlNamesAreTheRuns(t *testing.T) {
	path, _ := startRealServer(t)
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"frobnicate"}, "Error: unknown: command \"frobnicate\" is not part of this protocol"},
		{[]string{"-g", "nonesuch"}, "Error: unknown: setting \"nonesuch\" is not part of this protocol"},
		{[]string{"-s", "nonesuch=5"}, "Error: unknown: setting \"nonesuch\" is not part of this protocol"},
		{[]string{"-s", "cache_ttl=5"}, "Error: unknown: setting \"cache_ttl\""},
	} {
		code, out, errs := runMain(t, append([]string{"control", path}, tc.args...)...)
		if code != 1 || out != "" || !strings.Contains(errs, tc.want) {
			t.Errorf("%v: exit %d, stdout %q, stderr %q, want 1, nothing on stdout and %q", tc.args, code, out, errs, tc.want)
		}
	}

	// The client's help is the server's table: every line of the run's own help, and every setting
	// the protocol has, is in it.
	code, help, errs := runMain(t, "control", "--help")
	if code != 0 || errs != "" {
		t.Fatalf("control --help: exit %d, stderr %q", code, errs)
	}
	_, served, _ := runMain(t, "control", path, "help")
	for _, line := range strings.Split(strings.TrimSpace(served), "\n")[1:] {
		if !strings.Contains(help, strings.TrimRight(line, " ")) {
			t.Errorf("the run's help line %q is not in the client's help:\n%s", line, help)
		}
	}
	for _, k := range tuning.Keys() {
		if !strings.Contains(help, string(k)+"=") {
			t.Errorf("the client's help does not name the setting %s:\n%s", k, help)
		}
	}
}

// TestControlRefusesConfig (spec §7 row 75): -c and --config, before or after control, are a usage
// error that connects to nothing; without the flag the default config file is neither read nor
// needed, and a broken archiver.yaml in the working directory changes nothing.
func TestControlRefusesConfig(t *testing.T) {
	l := newListener(t, replying("OK\nfine\n"))
	const msg = "control finds the run by its socket path; --config has no effect"
	for _, args := range [][]string{
		{"-c", "x", "control", "{path}", "status"},
		{"--config", "x", "control", "{path}", "status"},
		{"--config=x", "control", "{path}", "status"},
		{"control", "-c", "x", "{path}", "status"},
		{"control", "--config", "x", "{path}", "status"},
		{"control", "{path}", "status", "--config=x"},
		{"control", "{path}", "-g", "batch_size", "-c", "x"},
	} {
		code, out, errs := runMain(t, forPath(args, l.Path)...)
		if code != 2 || out != "" || !strings.Contains(errs, msg) {
			t.Errorf("%v: exit %d, stdout %q, stderr %q, want 2, nothing on stdout and %q", args, code, out, errs, msg)
		}
	}
	if n := l.connections(); n != 0 {
		t.Errorf("%d connection(s) made by a refused --config, want none", n)
	}

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "archiver.yaml"), []byte("this: [is: not yaml"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	code, out, errs := runMain(t, "control", l.Path, "status")
	if code != 0 || out != "fine\n" || errs != "" {
		t.Errorf("with a broken archiver.yaml in the working directory: exit %d, stdout %q, stderr %q, want 0, the body, nothing", code, out, errs)
	}
}

// failing asserts that a client call failed with code, an empty stdout and want on stderr.
func failing(t *testing.T, what string, code int, out, errs string, wantCode int, want string) {
	t.Helper()
	if code != wantCode || out != "" || !strings.Contains(errs, want) {
		t.Errorf("%s: exit %d, stdout %q, stderr %q; want %d, nothing on stdout, and %q on stderr", what, code, out, errs, wantCode, want)
	}
}

// TestControlExitCodes (spec §7 row 76, §4.11): through Main. In every failing case stdout is empty.
func TestControlExitCodes(t *testing.T) {
	t.Run("0 with the body, without the OK line", func(t *testing.T) {
		l := newListener(t, replying("OK\nline one\nline two\n"))
		code, out, errs := runMain(t, "control", l.Path, "status")
		if code != 0 || out != "line one\nline two\n" || errs != "" {
			t.Errorf("exit %d, stdout %q, stderr %q", code, out, errs)
		}
	})
	t.Run("0 for an OK with an empty body prints nothing", func(t *testing.T) {
		l := newListener(t, replying("OK\n"))
		code, out, errs := runMain(t, "control", l.Path, "status")
		if code != 0 || out != "" || errs != "" {
			t.Errorf("exit %d, stdout %q, stderr %q", code, out, errs)
		}
	})
	t.Run("1 for an ERR, with the reason and the message", func(t *testing.T) {
		for _, reply := range []string{
			"ERR ceiling batch_size 5000 is above the run's starting value 1000\n",
			"ERR frobnicate a reason word the client does not know\n", // well formed: still a refusal
		} {
			l := newListener(t, replying(reply))
			code, out, errs := runMain(t, "control", l.Path, "-s", "batch_size=5000")
			want := "Error: " + strings.Replace(strings.TrimSuffix(strings.TrimPrefix(reply, "ERR "), "\n"), " ", ": ", 1)
			failing(t, reply, code, out, errs, 1, want)
		}
	})
	t.Run("5 when nothing is at the path", func(t *testing.T) {
		dir := shortTemp(t)
		path := filepath.Join(dir, "j.sock")
		code, out, errs := runMain(t, "control", path, "status")
		failing(t, "nothing at the path", code, out, errs, 5, "no control socket at "+path)
		code, out, errs = runMain(t, "control", filepath.Join(dir, "nodir", "j.sock"), "status")
		failing(t, "no directory", code, out, errs, 5, "no control socket at ")
		if !strings.Contains(errs, "not running") || !strings.Contains(errs, "not enabled") || !strings.Contains(errs, "path is wrong") {
			t.Errorf("the message does not say what it can mean: %q", errs)
		}
	})
	t.Run("5 when a stale socket refuses the connection", func(t *testing.T) {
		l := newListener(t, replying("OK\nnever\n"))
		l.l.SetUnlinkOnClose(false)
		if err := l.l.Close(); err != nil { // the listener is gone, its socket file is left: a killed run
			t.Fatal(err)
		}
		if _, err := os.Lstat(l.Path); err != nil {
			t.Fatalf("the stale socket file is not there: %v", err)
		}
		code, out, errs := runMain(t, "control", l.Path, "status")
		failing(t, "a stale socket", code, out, errs, 5, "stale")
		if !strings.Contains(errs, l.Path) {
			t.Errorf("the message does not name the path: %q", errs)
		}
	})
	t.Run("7 for a malformed reply", func(t *testing.T) {
		for _, tc := range []struct{ name, reply string }{
			{"OK without LF", "OK"},
			{"ok in lower case", "ok\nbody\n"},
			{"OK with more on its line", "OK now\nbody\n"},
			{"anything else", "garbage\n"},
			{"ERR without a reason", "ERR\n"},
			{"ERR with an empty reason", "ERR \n"},
			{"ERR without a message", "ERR busy\n"},
			{"ERR with an upper-case reason", "ERR Busy the log is behind\n"},
			{"ERR with a second line", "ERR busy the log is behind\nmore\n"},
			{"an empty reply", ""},
			{"a body that does not end with LF", "OK\nline one\nline tw"},
			{"an OK without LF at all", "OKbody"},
		} {
			l := newListener(t, replying(tc.reply))
			code, out, errs := runMain(t, "control", l.Path, "status")
			failing(t, tc.name, code, out, errs, 7, "malformed")
		}
	})
	t.Run("7 for a reply of 1 MiB + 1 byte, and 0 for exactly 1 MiB", func(t *testing.T) {
		const mib = 1 << 20
		over := "OK\n" + strings.Repeat("x", mib-3) + "\n" // 1 MiB + 1 byte, ending with LF
		l := newListener(t, replying(over))
		code, out, errs := runMain(t, "control", l.Path, "status")
		failing(t, "1 MiB + 1", code, out, errs, 7, "malformed")

		exact := "OK\n" + strings.Repeat("x", mib-4) + "\n" // exactly 1 MiB
		l = newListener(t, replying(exact))
		code, out, errs = runMain(t, "control", l.Path, "status")
		if code != 0 || len(out) != mib-3 || errs != "" {
			t.Errorf("exactly 1 MiB: exit %d, %d bytes on stdout, stderr %q, want 0 and %d bytes", code, len(out), errs, mib-3)
		}
	})
	t.Run("9 for a permission error on connect", func(t *testing.T) {
		if runtime.GOOS == "windows" || os.Geteuid() == 0 {
			t.Skip("needs a socket that the user may not connect to: a user other than root, on Linux or macOS")
		}
		l := newListener(t, replying("OK\nnever\n"))
		if err := os.Chmod(l.Path, 0); err != nil {
			t.Fatal(err)
		}
		code, out, errs := runMain(t, "control", l.Path, "status")
		failing(t, "EACCES", code, out, errs, 9, "permission")
		if l.connections() != 0 {
			t.Errorf("a connection was made through a socket the user may not connect to")
		}
	})
}

// TestControlTimeouts (spec §7 row 77): exit 6 in the exchange, with the listener holding the
// connection and the client giving up at its own deadline, which is told from an immediate refusal
// or another transport error by the time it took.
func TestControlTimeouts(t *testing.T) {
	t.Run("exchange", func(t *testing.T) {
		withLimits(t, controlLimits{dial: 5 * time.Second, exchange: 300 * time.Millisecond, maxReply: 1 << 20})
		l := newListener(t, silent)
		began := time.Now()
		code, out, errs := runMain(t, "control", l.Path, "status")
		took := time.Since(began)
		failing(t, "no reply", code, out, errs, 6, "300ms")
		if took < 250*time.Millisecond || took > 5*time.Second {
			t.Errorf("the client gave up after %v, want its 300ms deadline", took)
		}
		if l.connections() != 1 {
			t.Errorf("%d connection(s), want the one that was held", l.connections())
		}
	})
	t.Run("dial", func(t *testing.T) {
		t.Skip("no connection attempt stalls on Linux or macOS (spec §9 item 7) and no platform has been measured to stall: " +
			"a passed dial deadline is classified by TestControlDialDeadlinePassed and TestControlClassifyDialError")
	})
}

// TestControlDialDeadlinePassed (spec §7 rows 77 and 77a): a dial deadline that has passed is exit
// 6 and no connection was made, which tells it from a refusal (5) and from another failure (9).
func TestControlDialDeadlinePassed(t *testing.T) {
	withLimits(t, controlLimits{dial: -time.Second, exchange: 10 * time.Second, maxReply: 1 << 20})
	l := newListener(t, replying("OK\nnever\n"))
	code, out, errs := runMain(t, "control", l.Path, "status")
	failing(t, "an expired dial deadline", code, out, errs, 6, l.Path)
	if n := l.connections(); n != 0 {
		t.Errorf("%d connection(s) made after the dial deadline, want none", n)
	}
}

// TestControlClassifyDialError (spec §7 row 77a): the classifying function, error values in. A
// refusal or a missing path is exit 5; the client's own passed deadline is exit 6; EAGAIN, though it
// reports itself as a timeout, is exit 9 with the busy message; EACCES is exit 9.
func TestControlClassifyDialError(t *testing.T) {
	dial := func(err error) error {
		return &net.OpError{Op: "dial", Net: "unix", Addr: &net.UnixAddr{Name: "/s", Net: "unix"}, Err: err}
	}
	sys := func(e syscall.Errno) error { return dial(os.NewSyscallError("connect", e)) }
	eagain := sys(syscall.EAGAIN)
	var ne net.Error
	if !errors.As(eagain, &ne) || !ne.Timeout() {
		t.Fatalf("precondition: EAGAIN is expected to report itself as a timeout, got %v", eagain)
	}
	type dialCase struct {
		name string
		err  error
		code int
		msg  string
	}
	cases := []dialCase{
		{"ECONNREFUSED", sys(syscall.ECONNREFUSED), 5, "stale"},
		{"ENOENT", sys(syscall.ENOENT), 5, "no control socket"},
		{"a missing path", dial(fs.ErrNotExist), 5, "no control socket"},
		{"the dial deadline passed", dial(context.DeadlineExceeded), 6, "within"},
		{"a deadline error", dial(os.ErrDeadlineExceeded), 6, "within"},
		{"ETIMEDOUT", sys(syscall.ETIMEDOUT), 9, "/s"},
		{"EACCES", sys(syscall.EACCES), 9, "permission"},
		{"an error of no known kind", errors.New("boom"), 9, "boom"},
	}
	// EAGAIN is the full-queue answer where isQueueFull knows it: Linux and macOS. Windows answers a full
	// queue with WSAENOBUFS and never with EAGAIN, so a synthetic EAGAIN there is an error of no known
	// kind (exit 9, not busy); TestControlClassifyWindowsFullQueue is the Windows case.
	if runtime.GOOS != "windows" {
		cases = append(cases, dialCase{"EAGAIN", eagain, 9, "busy"})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyConnect("/s", tc.err)
			if got == nil || got.code != tc.code || !strings.Contains(got.Error(), tc.msg) || !strings.Contains(got.Error(), "/s") {
				t.Errorf("classifyConnect(%v) = %v (code %d), want code %d and %q naming the path", tc.err, got, codeOf(got), tc.code, tc.msg)
			}
		})
	}
}

func codeOf(e *controlError) int {
	if e == nil {
		return 0
	}
	return e.code
}

// TestControlCutAtLineBoundaryIsALimit (spec §7 row 78) pins a LIMIT of the framing, not a guarantee
// (spec §4.3): a reply cut exactly at a line boundary cannot be told from a complete one. A listener
// sends `OK` LF and the first lines of a longer body, then closes cleanly; the client prints those
// lines and exits 0. Its exit 0 means "a well-formed OK reply", not "a complete snapshot".
func TestControlCutAtLineBoundaryIsALimit(t *testing.T) {
	l := newListener(t, replying("OK\nfirst line of a longer body\nsecond line of a longer body\n"))
	code, out, errs := runMain(t, "control", l.Path, "status")
	if code != 0 || out != "first line of a longer body\nsecond line of a longer body\n" || errs != "" {
		t.Errorf("exit %d, stdout %q, stderr %q, want the cut reply printed with exit 0", code, out, errs)
	}
}

// TestControlEndpointChecks (spec §7 row 79): exit 8, with nothing connected and nothing sent, and
// modes, owners and files left as they were.
func TestControlEndpointChecks(t *testing.T) {
	// Windows makes no owner or mode check (best effort, spec §4.9) and a symbolic link needs a
	// privilege there, so those cases skip; the regular file runs everywhere.
	notOnWindows := func(t *testing.T, why string) {
		t.Helper()
		if runtime.GOOS == "windows" {
			t.Skip(why)
		}
	}
	refused := func(t *testing.T, l *testListener, path, want string) {
		t.Helper()
		code, out, errs := runMain(t, "control", path, "status")
		failing(t, want, code, out, errs, 8, want)
		if !strings.Contains(errs, path) {
			t.Errorf("the message does not name the path %s: %q", path, errs)
		}
		if l != nil {
			if reqs, _ := l.sent(); l.connections() != 0 || len(reqs) != 0 {
				t.Errorf("%d connection(s) and requests %q at the endpoint, want nothing connected and nothing sent", l.connections(), reqs)
			}
		}
	}
	t.Run("the directory is a symbolic link", func(t *testing.T) {
		notOnWindows(t, "creating a symbolic link needs a privilege on Windows")
		real := filepath.Join(shortTemp(t), "real")
		if err := os.Mkdir(real, 0o700); err != nil {
			t.Fatal(err)
		}
		l := newListenerAt(t, filepath.Join(real, "j.sock"), replying("OK\nnever\n"))
		link := filepath.Join(shortTemp(t), "link")
		if err := os.Symlink(real, link); err != nil {
			t.Fatal(err)
		}
		refused(t, l, filepath.Join(link, "j.sock"), "symbolic link")
		if fi, err := os.Lstat(link); err != nil || fi.Mode()&os.ModeSymlink == 0 {
			t.Errorf("the link was changed: %v, %v", fi, err)
		}
	})
	t.Run("the directory has group or other bits", func(t *testing.T) {
		notOnWindows(t, "directory modes are not enforced on Windows")
		l := newListener(t, replying("OK\nnever\n"))
		if err := os.Chmod(l.Dir, 0o750); err != nil {
			t.Fatal(err)
		}
		refused(t, l, l.Path, "group or other")
		if fi, err := os.Stat(l.Dir); err != nil || fi.Mode().Perm() != 0o750 {
			t.Errorf("the directory's mode was changed: %v, %v", fi, err)
		}
	})
	t.Run("the owner rule is the shared judging function", func(t *testing.T) {
		notOnWindows(t, "directory owners are not enforced on Windows")
		l := newListener(t, replying("OK\nnever\n"))
		err := checkEndpoint(l.Path, os.Geteuid()+1)
		var ce *controlError
		if !errors.As(err, &ce) || ce.code != 8 || !strings.Contains(err.Error(), "owned by uid") || !strings.Contains(err.Error(), l.Path) {
			t.Errorf("checkEndpoint with another uid = %v, want exit 8 naming the owner and the path", err)
		}
		if checkEndpoint(l.Path, os.Geteuid()) != nil {
			t.Errorf("checkEndpoint refused our own directory")
		}
	})
	t.Run("the directory belongs to another user", func(t *testing.T) {
		notOnWindows(t, "directory owners are not enforced on Windows")
		if os.Geteuid() != 0 {
			t.Skip("needs root to give a directory to another uid (the L1 container)")
		}
		l := newListener(t, replying("OK\nnever\n"))
		if err := os.Chmod(filepath.Dir(l.Dir), 0o755); err != nil { // let the test reach it
			t.Fatal(err)
		}
		if err := os.Chown(l.Dir, 12345, 12345); err != nil {
			t.Fatal(err)
		}
		refused(t, l, l.Path, "owned by uid 12345")
	})
	t.Run("the socket path is a symbolic link to a socket", func(t *testing.T) {
		notOnWindows(t, "creating a symbolic link needs a privilege on Windows")
		dir := shortTemp(t)
		l := newListenerAt(t, filepath.Join(dir, "real.sock"), replying("OK\nnever\n"))
		link := filepath.Join(dir, "j.sock")
		if err := os.Symlink(l.Path, link); err != nil {
			t.Fatal(err)
		}
		refused(t, l, link, "symbolic link")
		if target, err := os.Readlink(link); err != nil || target != l.Path {
			t.Errorf("the link was changed: %q, %v", target, err)
		}
	})
	t.Run("the socket path is a regular file", func(t *testing.T) {
		path := filepath.Join(shortTemp(t), "j.sock")
		if err := os.WriteFile(path, []byte("not a socket"), 0o600); err != nil {
			t.Fatal(err)
		}
		refused(t, nil, path, "regular file")
		if b, err := os.ReadFile(path); err != nil || string(b) != "not a socket" {
			t.Errorf("the file was changed: %q, %v", b, err)
		}
	})
}

// TestControlRoundTrip is the client against the run's own server: an action, a setter and a getter
// in the three forms, each as one request.
func TestControlRoundTrip(t *testing.T) {
	path, _ := startRealServer(t)

	code, out, errs := runMain(t, "control", path, "status-json")
	if code != 0 || errs != "" {
		t.Fatalf("status-json: exit %d, stderr %q", code, errs)
	}
	var st map[string]any
	if err := json.Unmarshal([]byte(out), &st); err != nil || st["protocol"] != float64(1) || st["socket"] != path {
		t.Errorf("status-json body %q (%v), want protocol 1 and socket %s", out, err, path)
	}
	if code, out, _ := runMain(t, "control", path, "status"); code != 0 || !strings.Contains(out, "socket:") {
		t.Errorf("status: exit %d, %q", code, out)
	}
	if code, out, errs := runMain(t, "control", path, "-s", "batch_delete_size=2"); code != 0 || !strings.HasPrefix(out, "batch_delete_size: ") || errs != "" {
		t.Errorf("set: exit %d, stdout %q, stderr %q", code, out, errs)
	}
	if code, out, _ := runMain(t, "control", path, "--get", "batch_delete_size"); code != 0 || out != "batch_delete_size=2\n" {
		t.Errorf("get: exit %d, %q", code, out)
	}
	code, out, errs = runMain(t, "control", path, "-s", "batch_delete_size=999999")
	failing(t, "above the ceiling", code, out, errs, 1, "Error: ceiling:")
	if code, out, _ := runMain(t, "control", path, "pause"); code != 0 || !strings.HasPrefix(out, "paused") {
		t.Errorf("pause: exit %d, %q", code, out)
	}
	if code, out, _ := runMain(t, "control", path, "resume"); code != 0 || !strings.Contains(out, "resumed") {
		t.Errorf("resume: exit %d, %q", code, out)
	}
	if code, out, _ := runMain(t, "control", path, "stop"); code != 0 || !strings.HasPrefix(out, "stop requested") {
		t.Errorf("stop: exit %d, %q", code, out)
	}
}

// TestControlPrintedPath (spec §7 row 80): the path that dry-run and validate print for a job, given to
// the client unchanged, reaches that job's run, and it is the path in status-json's `socket`. (The run's
// log carries the same path: TestMySQLControlPrintedPath.)
func TestControlPrintedPath(t *testing.T) {
	dir := filepath.Join(shortTemp(t), "run")
	doc := parseDoc(t, "", controlYAML(dir), "job", jobspec.Archive)
	line, err := controlReport(doc, doc.Jobs["job"], "job")
	if err != nil {
		t.Fatal(err)
	}
	printed, ok := strings.CutPrefix(line, "control socket: ")
	if !ok {
		t.Fatalf("the report line is %q", line)
	}
	ctl, err := startControl(testControlParams(t, "", controlYAML(dir)))
	if err != nil || ctl.srv == nil {
		t.Fatalf("startControl = %v, %v", ctl, err)
	}
	t.Cleanup(func() { _ = ctl.close() })

	code, out, errs := runMain(t, "control", printed, "status-json")
	if code != 0 || errs != "" {
		t.Fatalf("exit %d, stderr %q: the printed path %s did not reach the run", code, errs, printed)
	}
	var st map[string]any
	if err := json.Unmarshal([]byte(out), &st); err != nil || st["socket"] != printed || ctl.srv.Path() != printed {
		t.Errorf("status-json socket = %v (%v), the server's path %s, the printed path %s: want all three equal", st["socket"], err, ctl.srv.Path(), printed)
	}
}
