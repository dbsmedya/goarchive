//go:build integration

package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"

	"github.com/dbsmedya/goarchive/v3/internal/engine"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
)

// TEL-1 C6: what a run does over its control socket, end to end, on the estate. The command has no
// engine seam, so every test times the run with the socket itself: `pause` before the first batch (or
// after one) and a wait for the state `paused`, or a wait for a step in `status-json`. Each test owns a
// short socket directory, since t.TempDir is too long for a socket path on macOS.

// controlRunFixture is the order tree on both servers, seeded with three old orders, and a config that
// enables control in a socket directory of its own. processing replaces the fixture's processing block.
func controlRunFixture(t *testing.T, name, processing string) (fixture, string) {
	t.Helper()
	f := orderFixture(t, name)
	if processing != "" {
		setConfig(t, f, "processing:\n  batch_size: 2\n  batch_delete_size: 2\n", processing)
	}
	return f, enableControl(t, f)
}

// enableControl appends a control block with a short socket directory to the fixture's config.
func enableControl(t *testing.T, f fixture) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "gc")
	if err != nil {
		t.Skipf("no short directory for a socket: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	appendConfig(t, f, "control:\n  enabled: true\n  socket_dir: "+dir+"\n")
	return dir
}

func appendConfig(t *testing.T, f fixture, block string) {
	t.Helper()
	cfg, err := os.ReadFile(f.config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.config, append(cfg, []byte("\n"+block)...), 0o600); err != nil {
		t.Fatal(err)
	}
}

// liveRun is a command running in the background, and its socket.
type liveRun struct {
	t         *testing.T
	sock      string
	out, errb syncBuffer
	done      chan int
}

// startLive runs the command with args in the background; sock is the socket it will serve.
func startLive(t *testing.T, sock string, args ...string) *liveRun {
	t.Helper()
	resetFlags(rootCmd)
	r := &liveRun{t: t, sock: sock, done: make(chan int, 1)}
	go func() { r.done <- Main(args, &r.out, &r.errb) }()
	t.Cleanup(func() {
		resetFlags(rootCmd)
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
		rootCmd.SetArgs(nil)
	})
	return r
}

// ask waits for the socket and makes one request.
func (r *liveRun) ask(request string) string {
	r.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := os.Lstat(r.sock); err == nil {
			break
		}
		select {
		case code := <-r.done:
			r.t.Fatalf("the run ended (code %d) before its socket was served:\nstderr:\n%s", code, r.errb.String())
		default:
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("no socket at %s after 30 s:\n%s", r.sock, r.errb.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	c, err := net.DialTimeout("unix", r.sock, 5*time.Second)
	if err != nil {
		r.t.Fatalf("dial %s: %v", r.sock, err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	_, _ = c.Write([]byte(request + "\n"))
	b, _ := io.ReadAll(c)
	return string(b)
}

// status is status-json parsed.
func (r *liveRun) status() map[string]any {
	r.t.Helper()
	reply := r.ask("status-json")
	_, body, ok := strings.Cut(reply, "OK\n")
	if !ok {
		r.t.Fatalf("status-json = %q", reply)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(body)), &m); err != nil {
		r.t.Fatalf("status-json %q: %v", body, err)
	}
	return m
}

// waitFor polls status-json until cond holds, and fails the test with what if the run ends or the time
// runs out. It returns the status that satisfied cond.
func (r *liveRun) waitFor(what string, within time.Duration, cond func(map[string]any) bool) map[string]any {
	r.t.Helper()
	deadline := time.Now().Add(within)
	for {
		st := r.status()
		if cond(st) {
			return st
		}
		select {
		case code := <-r.done:
			r.done <- code
			r.t.Fatalf("the run ended (code %d) while waiting for %s; last status %v\nstderr:\n%s", code, what, st, r.errb.String())
		default:
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("timed out after %v waiting for %s; last status %v\nstderr:\n%s", within, what, st, r.errb.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (r *liveRun) waitState(want string) map[string]any {
	r.t.Helper()
	return r.waitFor("state "+want, 60*time.Second, func(st map[string]any) bool { return st["state"] == want })
}

// finish waits for the command to end and returns its exit code.
func (r *liveRun) finish() int {
	r.t.Helper()
	select {
	case code := <-r.done:
		return code
	case <-time.After(90 * time.Second):
		r.t.Fatalf("the run did not end within 90 s:\n%s", r.errb.String())
		return -1
	}
}

func counter(st map[string]any, name string) float64 {
	c, _ := st["counters"].(map[string]any)
	v, _ := c[name].(float64)
	return v
}

func step(st map[string]any) string {
	a, _ := st["activity"].(map[string]any)
	s, _ := a["step"].(string)
	return s
}

func expectOK(t *testing.T, reply, what string) {
	t.Helper()
	if !strings.HasPrefix(reply, "OK\n") {
		t.Fatalf("%s = %q, want OK", what, reply)
	}
}

const oldOrders = "SELECT COUNT(*) FROM orders WHERE placed < '2020-01-01'"

// TestMySQLControlPause (spec §7): pause holds the run at the next boundary with no further writes;
// resume continues it to the end. A batch finishes, the run sleeps, a pause arrives, and the run waits
// at the boundary after the sleep with the second batch not begun.
func TestMySQLControlPause(t *testing.T) {
	f, dir := controlRunFixture(t, "ctlpause", "processing:\n  batch_size: 2\n  batch_delete_size: 2\n  sleep_seconds: 2\n")
	r := startLive(t, filepath.Join(dir, "archive_orders.sock"), "archive", "-c", f.config, "--job", "archive_orders")
	r.waitFor("the first batch", 60*time.Second, func(st map[string]any) bool { return counter(st, "batches") >= 1 })
	expectOK(t, r.ask("pause"), "pause")
	st := r.waitState("paused")
	if got := scalar(t, f.dst, "SELECT COUNT(*) FROM orders"); got != 2 {
		t.Errorf("destination orders when paused = %d, want the first batch's 2", got)
	}
	if got := scalar(t, f.src, oldOrders); got != 1 {
		t.Errorf("old orders left in the source when paused = %d, want 1", got)
	}
	pause := st["pause"].(map[string]any)
	if pause["waiting_since"] == nil || len(pause["requested"].([]any)) != 1 || pause["requested"].([]any)[0] != "socket" {
		t.Errorf("pause = %v, want it requested by the socket and waiting since a time", pause)
	}
	time.Sleep(1500 * time.Millisecond) // a paused run writes nothing
	if again := r.status(); counter(again, "batches") != 1 || again["state"] != "paused" {
		t.Errorf("after 1.5 s paused: %v, want still one batch and paused", again)
	}
	expectOK(t, r.ask("resume"), "resume")
	if code := r.finish(); code != 0 {
		t.Fatalf("exit %d after resume, stderr:\n%s", code, r.errb.String())
	}
	if got := scalar(t, f.dst, "SELECT COUNT(*) FROM orders"); got != 3 {
		t.Errorf("destination orders = %d, want all 3", got)
	}
	if got := scalar(t, f.src, oldOrders); got != 0 {
		t.Errorf("old orders left in the source = %d, want 0", got)
	}
}

// TestMySQLControlStop (command half): pause before the first batch, wait for `paused`, stop: the run
// ends stopped at the boundary and exits 3, with nothing copied.
func TestMySQLControlStop(t *testing.T) {
	f, dir := controlRunFixture(t, "ctlstop", "")
	r := startLive(t, filepath.Join(dir, "archive_orders.sock"), "archive", "-c", f.config, "--job", "archive_orders")
	expectOK(t, r.ask("pause"), "pause")
	r.waitState("paused")
	reply := r.ask("stop")
	if !strings.HasPrefix(reply, "OK\nstop requested") {
		t.Fatalf("stop = %q", reply)
	}
	if code := r.finish(); code != 3 {
		t.Fatalf("exit %d after a stop, want 3, stderr:\n%s", code, r.errb.String())
	}
	if got := scalar(t, f.dst, "SELECT COUNT(*) FROM orders"); got != 0 {
		t.Errorf("destination orders = %d, want none: the run stopped before its first batch", got)
	}
	if got := scalar(t, f.src, oldOrders); got != 3 {
		t.Errorf("old orders in the source = %d, want 3", got)
	}
}

// slowDeletes is one short batch whose two DELETEs are a second apart: batch_size above the roots,
// one chunk per table, delete_sleep_seconds 1.
const slowDeletes = "processing:\n  batch_size: 10\n  batch_delete_size: 100\n  delete_sleep_seconds: 1\n"

// TestMySQLControlStopFinalBatch (command half): stop during the final short batch: the run completes,
// is not reported stopped, and the command exits 0 by the existing derivation.
func TestMySQLControlStopFinalBatch(t *testing.T) {
	f, dir := controlRunFixture(t, "ctlfinal", slowDeletes)
	r := startLive(t, filepath.Join(dir, "archive_orders.sock"), "archive", "-c", f.config, "--job", "archive_orders")
	r.waitFor("the sleep between the two DELETEs", 60*time.Second, func(st map[string]any) bool { return step(st) == "sleep" })
	reply := r.ask("stop")
	if !strings.HasPrefix(reply, "OK\nstop requested") {
		t.Fatalf("stop = %q", reply)
	}
	if code := r.finish(); code != 0 {
		t.Fatalf("exit %d, want 0: the batch in flight was the last, so the run completed; stderr:\n%s", code, r.errb.String())
	}
	if got := scalar(t, f.src, oldOrders); got != 0 {
		t.Errorf("old orders left in the source = %d, want 0", got)
	}
}

// TestMySQLControlStopWithError (command half): a stop followed by the batch's error: the command exits
// 1, the error stands. A row is removed from the source behind the run's back (a violation of the run
// contract, made by the test) between the two DELETEs, so the second deletes fewer rows than its keys.
func TestMySQLControlStopWithError(t *testing.T) {
	f, dir := controlRunFixture(t, "ctlerror", slowDeletes)
	r := startLive(t, filepath.Join(dir, "archive_orders.sock"), "archive", "-c", f.config, "--job", "archive_orders")
	r.waitFor("the sleep between the two DELETEs", 60*time.Second, func(st map[string]any) bool { return step(st) == "sleep" })
	reply := r.ask("stop")
	if !strings.HasPrefix(reply, "OK\nstop requested") {
		t.Fatalf("stop = %q", reply)
	}
	mustExec(t, f.src, "DELETE FROM orders WHERE id = '00000000-0000-0000-0000-000000000000'")
	if code := r.finish(); code != 1 {
		t.Fatalf("exit %d, want 1: the stop met an error; stderr:\n%s", code, r.errb.String())
	}
	if !strings.Contains(r.errb.String(), "Run failed") {
		t.Errorf("stderr lacks the closing entry of a failed run:\n%s", r.errb.String())
	}
}

// TestMySQLControlSummary (spec §7): a run that accepted changes ends with them in its closing entry,
// after the audit entries that add up to them.
func TestMySQLControlSummary(t *testing.T) {
	f, dir := controlRunFixture(t, "ctlsummary", "")
	r := startLive(t, filepath.Join(dir, "archive_orders.sock"), "archive", "-c", f.config, "--job", "archive_orders", "--progress=1h")
	expectOK(t, r.ask("pause"), "pause")
	r.waitState("paused")
	if reply := r.ask("batch_size=1"); !strings.Contains(reply, "batch_size: 2 -> 1, from the next batch or recovery page") {
		t.Fatalf("batch_size=1 = %q", reply)
	}
	if reply := r.ask("batch_size=3"); !strings.HasPrefix(reply, "ERR ceiling ") {
		t.Fatalf("batch_size=3 above the starting 2 = %q, want ERR ceiling", reply)
	}
	expectOK(t, r.ask("resume"), "resume")
	if code := r.finish(); code != 0 {
		t.Fatalf("exit %d, stderr:\n%s", code, r.errb.String())
	}
	out := r.out.String() + r.errb.String()
	for _, w := range []string{
		`"msg":"Control socket: ` + filepath.Join(dir, "archive_orders.sock") + `"`,
		"Runtime setting changed: batch_size 2 -\\u003e 1", "Runtime control: pause requested", "Runtime control: resume requested",
		"Control request refused: ceiling",
	} {
		if !strings.Contains(out, w) && !strings.Contains(out, strings.ReplaceAll(w, `\u003e`, ">")) {
			t.Errorf("the output lacks %q:\n%s", w, out)
		}
	}
	closing := strings.Index(out, `"msg":"Run completed"`)
	if closing < 0 || !strings.Contains(out[closing:], `"runtime_changes":"batch_size 2 -\u003e 1 (1 change); paused 1 time for `) &&
		!strings.Contains(out[closing:], `"runtime_changes":"batch_size 2 -> 1 (1 change); paused 1 time for `) {
		t.Errorf("the closing entry lacks the runtime-changes line:\n%s", out)
	}
	if audit := strings.Index(out, "Runtime setting changed"); audit < 0 || audit > closing {
		t.Errorf("the audit entry comes at %d, the closing entry at %d: want the audit entries first", audit, closing)
	}
	if got := scalar(t, f.dst, "SELECT COUNT(*) FROM orders"); got != 3 {
		t.Errorf("destination orders = %d, want 3 (batches of 1 after the change)", got)
	}
}

// TestMySQLControlRefusalsAreLogged (spec §4, completed over the socket): a size above the starting
// value is `ERR ceiling`, the refusal is a log entry, and the value is unchanged.
func TestMySQLControlRefusalsAreLogged(t *testing.T) {
	f, dir := controlRunFixture(t, "ctlrefuse", "")
	r := startLive(t, filepath.Join(dir, "archive_orders.sock"), "archive", "-c", f.config, "--job", "archive_orders")
	expectOK(t, r.ask("pause"), "pause")
	r.waitState("paused")
	if reply := r.ask("batch_size=5000"); !strings.HasPrefix(reply, "ERR ceiling batch_size 5000 is above this run's configured 2;") {
		t.Errorf("batch_size=5000 = %q, want ERR ceiling", reply)
	}
	if reply := r.ask("batch_size=?"); reply != "OK\nbatch_size=2\n" {
		t.Errorf("batch_size=? = %q, want it unchanged", reply)
	}
	if reply := r.ask("frobnicate"); !strings.HasPrefix(reply, "ERR unknown ") {
		t.Errorf("frobnicate = %q", reply)
	}
	r.ask("stop")
	if code := r.finish(); code != 3 {
		t.Fatalf("exit %d, want 3", code)
	}
	if errs := r.errb.String(); !strings.Contains(errs, "Control request refused: ceiling") || !strings.Contains(errs, "Control request refused: unknown") {
		t.Errorf("stderr lacks the refusals:\n%s", errs)
	}
}

// TestMySQLControlCopyOnlySocket (spec §4, completed over the socket): on a copy-only run the delete and
// gate setters are refused with their messages, and help does not list them.
func TestMySQLControlCopyOnlySocket(t *testing.T) {
	f, dir := controlRunFixture(t, "ctlcopy", "")
	r := startLive(t, filepath.Join(dir, "copy_orders.sock"), "copy-only", "-c", f.config, "--job", "copy_orders")
	expectOK(t, r.ask("pause"), "pause")
	r.waitState("paused")
	for line, want := range map[string]string{
		"batch_delete_size=1":            "ERR inapplicable a copy-only job deletes nothing\n",
		"delete_sleep_seconds=1":         "ERR inapplicable a copy-only job deletes nothing\n",
		"seconds_behind_source_within=5": "ERR inapplicable this run does not use the replication gate\n",
		"check_interval=2":               "ERR inapplicable this run does not use the replication gate\n",
		"batch_delete_size=?":            "ERR inapplicable a copy-only job deletes nothing\n",
		"batch_size=1":                   "OK\nbatch_size: 2 -> 1, from the next batch or recovery page\n",
		"sleep_seconds=0":                "OK\nsleep_seconds: 1 -> 0, at once\n",
	} {
		if got := r.ask(line); got != want {
			t.Errorf("%s = %q, want %q", line, got, want)
		}
	}
	help := r.ask("help")
	for _, absent := range []string{"batch_delete_size", "delete_sleep_seconds", "seconds_behind_source_within", "check_interval"} {
		if strings.Contains(help, absent) {
			t.Errorf("help lists %s on a copy-only run:\n%s", absent, help)
		}
	}
	if st := r.status(); st["job_type"] != "copy-only" || len(st["settings"].(map[string]any)) != 2 {
		t.Errorf("status = job_type %v, settings %v, want copy-only with two settings", st["job_type"], st["settings"])
	}
	expectOK(t, r.ask("resume"), "resume")
	if code := r.finish(); code != 0 {
		t.Fatalf("exit %d, stderr:\n%s", code, r.errb.String())
	}
}

// TestMySQLControlSocketInUse (spec §7 row 18, command half): a run that finds its control socket in use
// by a live run exits 4, "not started", and nothing was connected or changed. A live run is stood for by
// a socket server on the same job and directory; when it ends, the run starts.
func TestMySQLControlSocketInUse(t *testing.T) {
	f, dir := controlRunFixture(t, "ctlinuse", "")
	doc, err := jobspec.Load(f.config)
	if err != nil {
		t.Fatal(err)
	}
	spec := doc.Jobs["archive_orders"]
	holder, err := startControl(controlParams{
		doc: doc, spec: spec, name: "archive_orders", typ: spec.Type,
		store: newRunStore(doc, spec, func() {}), tracker: engine.NewProgress(time.Now()),
		shutdown: newTestShutdown(t), log: logger.NewDefault(), stderr: io.Discard, started: time.Now(),
	})
	if err != nil || holder.srv == nil {
		t.Fatalf("the holder: %v, %v", holder, err)
	}
	out, errOut, code := mainStreams(t, "archive", "-c", f.config, "--job", "archive_orders")
	// The fixture logs JSON on stderr, so the failure is the closing entry itself (console-output spec §3.4a).
	if code != 4 || out != "" || !strings.Contains(errOut, `"msg":"Run failed"`) || !strings.Contains(errOut, "the control socket is in use") ||
		!strings.Contains(errOut, filepath.Join(dir, "archive_orders.lock")) || !strings.Contains(errOut, "already uses this socket directory") {
		t.Errorf("code %d, stdout %q, stderr %q: want 4 and the lock file named", code, out, errOut)
	}
	if got := scalar(t, f.dst, "SELECT COUNT(*) FROM orders"); got != 0 {
		t.Errorf("destination orders = %d, want none: nothing was done", got)
	}
	if err := holder.close(); err != nil {
		t.Fatal(err)
	}
	if _, errOut, code := mainStreams(t, "archive", "-c", f.config, "--job", "archive_orders"); code != 0 {
		t.Errorf("a run once the socket is free: code %d, stderr %q", code, errOut)
	}
}

// TestMySQLOutputStandardWithControl (console-output spec §3, *A cron: silent*, with control enabled): a
// successful run with a log file, no --progress and control enabled prints nothing on stdout or stderr,
// and its log holds the `Control socket: <path>` entry, under level error too.
func TestMySQLOutputStandardWithControl(t *testing.T) {
	for _, level := range []string{"info", "error"} {
		t.Run(level, func(t *testing.T) {
			f, dir := controlRunFixture(t, "ctlcron"+level, "")
			logPath := filepath.Join(t.TempDir(), "run.log")
			setLogging(t, f, "logging:\n  level: "+level+"\n  format: json\n  output: "+logPath+"\n")
			out, errOut, code := mainStreams(t, "archive", "-c", f.config, "--job", "archive_orders")
			if code != 0 || out != "" || errOut != "" {
				t.Errorf("code %d, stdout %q, stderr %q: want 0 and silence", code, out, errOut)
			}
			log := readAll(t, logPath)
			sock := filepath.Join(dir, "archive_orders.sock")
			if n := strings.Count(log, `"msg":"Control socket: `+sock+`"`); n != 1 {
				t.Errorf("the log has the socket's path %d times, want once:\n%s", n, log)
			}
			// Under level error only a failure is written (the closing entry of a successful run is info).
			if closing := strings.Contains(log, `"msg":"Run completed"`); closing != (level == "info") {
				t.Errorf("level %s: the log has the closing entry = %v, want %v:\n%s", level, closing, level == "info", log)
			}
		})
	}
}

// ---- the replica: the gate, a hold, and the tolerance over the socket ----

func replicaDB(t *testing.T) *sql.DB { return serverFor(t, "TEST_REPLICA").open(t, "") }

// replicationBlock is a replication block for the estate's replica (db3).
func replicationBlock(t *testing.T, tolerance int) string {
	t.Helper()
	s := serverFor(t, "TEST_REPLICA")
	return fmt.Sprintf("replication:\n  enabled: true\n  seconds_behind_source_within: %d\n  check_interval: 1\n  cache_ttl: 0\n  servers:\n"+
		"    - host: %s\n      port: %d\n      user: %s\n      password: %q\n      tls: disable\n", tolerance, s.host, s.port, s.user, s.pass)
}

// stopApplier stops the replica's applier, and starts it again whatever the test does.
func stopApplier(t *testing.T) *sql.DB {
	t.Helper()
	replica := replicaDB(t)
	t.Cleanup(func() { _, _ = replica.Exec("START REPLICA SQL_THREAD") })
	mustExec(t, replica, "STOP REPLICA SQL_THREAD")
	return replica
}

// holdFixture is an order fixture with the replica's gate on, a single short batch of one chunk per
// table, and the applier stopped: the run holds at the first DELETE.
func holdFixture(t *testing.T, name string) (fixture, string) {
	t.Helper()
	f, dir := controlRunFixture(t, name, "processing:\n  batch_size: 10\n  batch_delete_size: 100\n")
	appendConfig(t, f, replicationBlock(t, 30))
	stopApplier(t)
	return f, dir
}

// TestMySQLControlStatus (spec §7): status-json during a hold: state held, the server's cause, tolerance,
// the counters equal to the rows committed so far, and the job's name, the config file's path, the pid,
// the socket's path and `"protocol": 1`.
func TestMySQLControlStatus(t *testing.T) {
	f, dir := holdFixture(t, "ctlstatus")
	sock := filepath.Join(dir, "archive_orders.sock")
	r := startLive(t, sock, "archive", "-c", f.config, "--job", "archive_orders")
	st := r.waitState("held")

	abs, _ := filepath.Abs(f.config)
	for key, want := range map[string]any{
		"protocol": 1.0, "job": "archive_orders", "job_type": "archive", "config_file": abs, "pid": float64(os.Getpid()),
		"socket": sock, "stop_requested": false,
	} {
		if st[key] != want {
			t.Errorf("%s = %v, want %v", key, st[key], want)
		}
	}
	if counter(st, "rows_copied") != 9 || counter(st, "rows_deleted") != 0 || counter(st, "roots") != 0 {
		t.Errorf("counters = %v, want 9 rows copied (3 orders and 6 lines), none deleted, no batch done", st["counters"])
	}
	hold, _ := st["hold"].(map[string]any)
	servers, _ := hold["servers"].([]any)
	if hold["since"] == nil || len(servers) != 1 {
		t.Fatalf("hold = %v, want a time and the one server", hold)
	}
	server := servers[0].(map[string]any)
	if server["cause"] != "unhealthy" || server["tolerance"] != 30.0 || server["healthy"] != false || server["checked_at"] == nil {
		t.Errorf("the held server = %v, want unhealthy at tolerance 30, checked", server)
	}
	repl := st["replication"].(map[string]any)
	if repl["enabled"] != true || repl["used"] != true || repl["cache_ttl"] != 0.0 {
		t.Errorf("replication = %v", repl)
	}
	if text := r.ask("status"); !strings.Contains(text, "held ") || !strings.Contains(text, "  hold:      ") || !strings.Contains(text, " unhealthy (") {
		t.Errorf("status text:\n%s\nwant the hold line", text)
	}

	r.ask("stop")
	if code := r.finish(); code != 3 {
		t.Fatalf("exit %d, want 3", code)
	}
}

// TestMySQLControlStopDuringHold (spec §7): with the replica's applier stopped, stop ends the hold at once
// and the next run's recovery finishes the batch.
func TestMySQLControlStopDuringHold(t *testing.T) {
	f, dir := holdFixture(t, "ctlholdstop")
	sock := filepath.Join(dir, "archive_orders.sock")
	r := startLive(t, sock, "archive", "-c", f.config, "--job", "archive_orders")
	r.waitState("held")
	began := time.Now()
	r.ask("stop")
	if code := r.finish(); code != 3 {
		t.Fatalf("exit %d, want 3, stderr:\n%s", code, r.errb.String())
	}
	if took := time.Since(began); took > 10*time.Second {
		t.Errorf("the run took %v to end after a stop during a hold, want it at once", took)
	}
	if got := scalar(t, f.src, oldOrders); got != 3 {
		t.Errorf("old orders in the source = %d, want 3: the held batch's deletes did not run", got)
	}
	if got := scalar(t, f.dst, "SELECT COUNT(*) FROM orders"); got != 3 {
		t.Errorf("destination orders = %d, want the copied batch's 3", got)
	}

	// The next run recovers the batch: the applier runs again.
	mustExec(t, replicaDB(t), "START REPLICA SQL_THREAD")
	if _, errOut, code := mainStreams(t, "archive", "-c", f.config, "--job", "archive_orders"); code != 0 {
		t.Fatalf("the rerun: code %d, stderr %q", code, errOut)
	}
	if got := scalar(t, f.src, oldOrders); got != 0 {
		t.Errorf("old orders left in the source after the rerun = %d, want 0", got)
	}
}

// TestMySQLControlRecoveringStatus (spec §4, completed over the socket): a rerun that replays logged
// roots answers `recovering` with rising counters.
func TestMySQLControlRecoveringStatus(t *testing.T) {
	f, dir := holdFixture(t, "ctlrecover")
	sock := filepath.Join(dir, "archive_orders.sock")
	r := startLive(t, sock, "archive", "-c", f.config, "--job", "archive_orders")
	r.waitState("held")
	r.ask("stop")
	if code := r.finish(); code != 3 {
		t.Fatalf("exit %d, want 3", code)
	}

	// The rerun deletes one key at a time, a second apart, so recovery lasts long enough to watch.
	mustExec(t, replicaDB(t), "START REPLICA SQL_THREAD")
	setConfig(t, f, "processing:\n  batch_size: 10\n  batch_delete_size: 100\n", "processing:\n  batch_size: 10\n  batch_delete_size: 1\n  delete_sleep_seconds: 1\n")
	r2 := startLive(t, sock, "archive", "-c", f.config, "--job", "archive_orders")
	var seen []float64
	r2.waitFor("recovery to delete more than one row", 60*time.Second, func(st map[string]any) bool {
		if st["state"] == "recovering" {
			if n := counter(st, "rows_deleted"); len(seen) == 0 || n != seen[len(seen)-1] {
				seen = append(seen, n)
			}
		}
		return len(seen) >= 3
	})
	for i := 1; i < len(seen); i++ {
		if seen[i] <= seen[i-1] {
			t.Errorf("rows_deleted while recovering = %v, want it rising", seen)
		}
	}
	r2.ask("stop")
	r2.finish()
}

// delayedReplica makes db3 apply every event delay seconds late, and returns it to no delay when the test
// ends. Call it before the fixture, so its seed writes are the delayed ones.
func delayedReplica(t *testing.T, delay int) *sql.DB {
	t.Helper()
	replica := replicaDB(t)
	t.Cleanup(func() {
		for _, stmt := range []string{"STOP REPLICA SQL_THREAD", "CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 0", "START REPLICA SQL_THREAD"} {
			_, _ = replica.Exec(stmt)
		}
	})
	mustExec(t, replica, "STOP REPLICA SQL_THREAD", fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_DELAY = %d", delay), "START REPLICA SQL_THREAD")
	return replica
}

func replicaLag(t *testing.T, replica *sql.DB) int64 {
	t.Helper()
	channels, err := dbsrepl.NewInspector(replica).ReplicaStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range channels {
		if c.ChannelName == "" && c.SecondsBehindSource.Valid {
			return c.SecondsBehindSource.Int64
		}
	}
	return -1
}

// TestMySQLControlToleranceRaisedSocket (spec §7, completed over the socket): a run that holds because the
// replica's lag is above its configured tolerance has the tolerance raised one step at a time; each raise
// wrote its WARNING, the hold ends at the first step above the lag, and the closing entry marks the final
// value as above the configuration. A raise straight to a large value is refused.
func TestMySQLControlToleranceRaisedSocket(t *testing.T) {
	replica := delayedReplica(t, 20)
	f, dir := controlRunFixture(t, "ctltol", "processing:\n  batch_size: 10\n  batch_delete_size: 100\n")
	appendConfig(t, f, replicationBlock(t, 1))
	deadline := time.Now().Add(30 * time.Second)
	for replicaLag(t, replica) < 5 {
		if time.Now().After(deadline) {
			t.Fatal("the replica's lag never reached 5 s with SOURCE_DELAY set")
		}
		time.Sleep(200 * time.Millisecond)
	}
	sock := filepath.Join(dir, "archive_orders.sock")
	r := startLive(t, sock, "archive", "-c", f.config, "--job", "archive_orders", "--progress=1h")
	r.waitState("held")

	// Wait for the gate to have probed twice, so the first round has completed: then a raise too large is
	// a step refusal, not "not yet checked".
	first := r.status()["replication"].(map[string]any)["servers"].([]any)[0].(map[string]any)["checked_at"]
	r.waitFor("a second probe round", 30*time.Second, func(st map[string]any) bool {
		return st["replication"].(map[string]any)["servers"].([]any)[0].(map[string]any)["checked_at"] != first
	})
	if reply := r.ask("seconds_behind_source_within=100"); !strings.HasPrefix(reply, "ERR step ") || !strings.Contains(reply, "largest value accepted now is 2") {
		t.Fatalf("a raise straight to 100 = %q, want a step refusal naming 2", reply)
	}

	raises := 0
	for current := 1; ; {
		st := r.status()
		if st["state"] != "held" {
			break
		}
		next := current + 1
		reply := r.ask("seconds_behind_source_within=" + strconv.Itoa(next))
		switch {
		case strings.HasPrefix(reply, "OK\n"):
			raises, current = raises+1, next
		case strings.HasPrefix(reply, "ERR step ") && strings.Contains(reply, "not yet been checked"):
			time.Sleep(20 * time.Millisecond)
		default:
			t.Fatalf("seconds_behind_source_within=%d = %q", next, reply)
		}
		if raises > 40 {
			t.Fatal("the hold has not ended after 40 steps")
		}
	}
	if code := r.finish(); code != 0 {
		t.Fatalf("exit %d, stderr:\n%s", code, r.errb.String())
	}
	if raises < 2 {
		t.Errorf("raises = %d, want the hold to have gone on through at least two steps", raises)
	}
	out := r.out.String() + r.errb.String()
	if n := strings.Count(out, "WARNING: seconds_behind_source_within raised to "); n != raises {
		t.Errorf("%d WARNING lines, want one for each of the %d raises:\n%s", n, raises, out)
	}
	if !strings.Contains(out, "above the configured value)") || !strings.Contains(out, `runtime_changes`) {
		t.Errorf("the closing entry does not mark the final value as above the configuration:\n%s", out)
	}
	if got := scalar(t, f.src, oldOrders); got != 0 {
		t.Errorf("old orders left in the source = %d, want 0", got)
	}
}
