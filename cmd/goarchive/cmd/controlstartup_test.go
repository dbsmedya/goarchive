package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/controlpath"
)

// stuckServer is a TCP server that accepts connections and says nothing, so a MySQL client waits for its
// handshake: a run that connects to it is held in its startup, with its control socket already served.
// release closes every connection, now and later, which fails the run's connect.
type stuckServer struct {
	l        net.Listener
	mu       sync.Mutex
	conns    []net.Conn
	released bool
}

func newStuckServer(t *testing.T) *stuckServer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &stuckServer{l: l}
	t.Cleanup(func() { s.release(); _ = l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			if s.released {
				_ = c.Close()
			} else {
				s.conns = append(s.conns, c)
			}
			s.mu.Unlock()
		}
	}()
	return s
}

func (s *stuckServer) port() int { return s.l.Addr().(*net.TCPAddr).Port }

func (s *stuckServer) accepted() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

func (s *stuckServer) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.released = true
	for _, c := range s.conns {
		_ = c.Close()
	}
	s.conns = nil
}

// TestTerminalStartupFailure (spec §7 row 56): a change is accepted, then the run fails before the engine
// returns a report (its connect fails): the closing entry carries the runtime-changes summary, and the
// socket file is gone and the lock released.
func TestTerminalStartupFailure(t *testing.T) {
	stuck := newStuckServer(t)
	dir := filepath.Join(shortTemp(t), "run")
	cfg := writeConfig(t, fmt.Sprintf(`
source:
  host: 127.0.0.1
  port: %[1]d
  user: u
  password: p
  database: sakila
  tls: disable
destination:
  host: 127.0.0.1
  port: %[1]d
  user: u
  password: p
  database: archive
  tls: disable
logging:
  level: info
  format: json
  output: stderr
control:
  enabled: true
  socket_dir: %[2]s
jobs:
  j:
    job_type: archive
    root_table: orders
    primary_key: id
    where: "1=1"
`, stuck.port(), dir))
	sock := filepath.Join(dir, "j.sock")

	var out, errb safeBuffer
	resetFlags(rootCmd)
	defer func() {
		resetFlags(rootCmd)
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
		rootCmd.SetArgs(nil)
	}()
	done := make(chan int, 1)
	go func() { done <- Main([]string{"archive", "-c", cfg, "--job", "j"}, &out, &errb) }()

	// The run is in its connect, held by the server that says nothing, and its socket answers.
	deadline := time.Now().Add(30 * time.Second)
	for stuck.accepted() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the run never reached its connect:\n%s", errb.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := askSocket(t, sock, "status-json"); !strings.Contains(got, `"state":"starting"`) {
		t.Fatalf("status during the held connect = %q, want starting", got)
	}
	if got := askSocket(t, sock, "batch_size=1"); !strings.HasPrefix(got, "OK\nbatch_size: 1000 -> 1,") {
		t.Fatalf("batch_size=1 = %q", got)
	}

	stuck.release() // the connect fails: the run fails before the engine returns a report
	var code int
	select {
	case code = <-done:
	case <-time.After(60 * time.Second):
		t.Fatalf("the run did not end:\n%s", errb.String())
	}
	if code != 1 {
		t.Errorf("exit %d, want 1 (a failed connect)", code)
	}
	var closing map[string]any
	for _, line := range strings.Split(strings.TrimSpace(errb.String()), "\n") {
		var e map[string]any
		if json.Unmarshal([]byte(line), &e) == nil && e["msg"] == "Run failed" {
			closing = e
		}
	}
	if closing == nil || closing["runtime_changes"] != "batch_size 1000 -> 1 (1 change)" {
		t.Errorf("closing entry = %v\nstderr:\n%s\nwant it to carry runtime_changes", closing, errb.String())
	}
	if _, err := os.Lstat(sock); !os.IsNotExist(err) {
		t.Errorf("the socket file is left after the run failed: %v", err)
	}
	// The lock is released: a new start in the same directory binds at once.
	p := testControlParams(t, "", controlYAML(dir))
	again, err := startControl(p)
	if err != nil || again.srv == nil {
		t.Fatalf("a start after the failed run = %v, %v", again, err)
	}
	if err := again.close(); err != nil {
		t.Error(err)
	}
}

// TestNoEntryNoDefault (spec §7 row 26, plan L5): in a process whose effective uid has no user-database
// entry, with USER and HOME both set, a job with control enabled and no socket_dir is refused with the
// no-default message and no directory is created under that $HOME; the same job with socket_dir set runs;
// a job with control off makes no lookup and creates nothing. The test needs such a user, which a
// container gives it (`docker run --user 12345:12345 -e USER=ghost -e HOME=/tmp/ghost`); elsewhere it
// skips.
func TestNoEntryNoDefault(t *testing.T) {
	if _, err := controlpath.HomeOf(os.Geteuid()); err == nil {
		t.Skipf("uid %d has a user-database entry: run the test as a uid with none (plan L5)", os.Geteuid())
	}
	home := os.Getenv("HOME")
	if home == "" || os.Getenv("USER") == "" {
		t.Skip("the test is about a populated environment: HOME and USER must both be set (plan L5)")
	}
	if err := os.MkdirAll(home, 0o700); err != nil {
		t.Skipf("cannot make the home the environment names: %v", err)
	}
	defer func() { _ = os.Remove(home) }()
	untouched := func(what string) {
		t.Helper()
		if entries, err := os.ReadDir(home); err != nil || len(entries) != 0 {
			t.Errorf("%s: %s holds %v, %v, want nothing created under $HOME", what, home, entries, err)
		}
	}

	// Enabled, no socket_dir: refused with the remedy, naming the uid.
	p := testControlParams(t, "control:\n  enabled: true\n", "")
	_, err := startControl(p)
	var noDefault *controlpath.NoDefaultError
	if !errors.As(err, &noDefault) || !strings.Contains(err.Error(), "control.socket_dir") || !strings.Contains(err.Error(), fmt.Sprint(os.Geteuid())) {
		t.Fatalf("startControl with no default = %v, want the no-default refusal naming the uid and control.socket_dir", err)
	}
	untouched("enabled without socket_dir")

	// The report says the same, and validate fails the job.
	if line, rerr := controlReport(p.doc, p.spec, "job"); rerr == nil || !strings.Contains(line, "control socket: none") {
		t.Errorf("controlReport = %q, %v, want the reason and an error", line, rerr)
	}

	// With socket_dir set the run needs no lookup and starts.
	dir := filepath.Join(shortTemp(t), "run")
	set := testControlParams(t, "", controlYAML(dir))
	ctl, err := startControl(set)
	if err != nil || ctl.srv == nil {
		t.Fatalf("startControl with socket_dir = %v, %v, want it to start", ctl, err)
	}
	if got := askSocket(t, filepath.Join(dir, "job.sock"), "help"); !strings.HasPrefix(got, "OK\nprotocol 1") {
		t.Errorf("help = %q", got)
	}
	if err := ctl.close(); err != nil {
		t.Error(err)
	}
	untouched("enabled with socket_dir")

	// A job with control off looks nothing up and creates nothing.
	off := testControlParams(t, "", "")
	if ctl, err := startControl(off); err != nil || ctl.srv != nil {
		t.Errorf("startControl with control off = %v, %v, want a controlRun with no server", ctl, err)
	}
	untouched("control off")
}
