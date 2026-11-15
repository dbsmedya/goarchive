package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/control"
	"github.com/dbsmedya/goarchive/v3/internal/controlpath"
	"github.com/dbsmedya/goarchive/v3/internal/engine"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
	"github.com/dbsmedya/goarchive/v3/internal/replication"
	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// The control socket's wiring in the command (TEL-1 C6): the store a run builds, the adapters from the
// engine's and the gate's types to the server's, the log entry that names the socket, the closing
// output's runtime-changes line, the control-socket line of validate and dry-run, and the exit code of
// a socket in use. No database is needed here; what a run does over the socket is the integration tests'.

// shortTemp is a directory with a short path for a socket; t.TempDir is too long on macOS. /tmp is
// the short one where it exists; where it does not (Windows) the system's temporary directory is,
// which is short enough there, and a directory whose path would not leave room for a socket skips.
func shortTemp(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "gc")
	if err != nil {
		dir, err = os.MkdirTemp("", "gc")
	}
	if err != nil {
		t.Skipf("no short directory for a socket: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if len(dir) > 60 {
		t.Skipf("no short directory for a socket: %s is %d bytes", dir, len(dir))
	}
	return dir
}

// parseDoc loads a configuration with the shared blocks, a control block (a YAML fragment at the top
// level, or empty) and one job named job.
func parseDoc(t *testing.T, global, jobControl, jobName string, jobType jobspec.Type) *jobspec.Document {
	t.Helper()
	body := `
source:
  host: src
  user: u
  database: sakila
destination:
  host: dst
  user: u
  database: archive
` + global + `
jobs:
  ` + jobName + `:
    job_type: ` + string(jobType) + `
    root_table: orders
    primary_key: id
    where: "1=1"
` + jobControl
	doc, err := jobspec.Parse([]byte(body))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return doc
}

// TestRunStore: a run's settings store starts at the job's processing values and the configured gate
// values, and the run's kind says which settings apply: nothing is deleted by a copy-only job, and the
// gate is used by an archive or purge job whose replication is enabled.
func TestRunStore(t *testing.T) {
	repl := "replication:\n  enabled: true\n  seconds_behind_source_within: 7\n  check_interval: 3\n  servers:\n    - host: r1\n      user: u\n"
	for _, tc := range []struct {
		name   string
		global string
		typ    jobspec.Type
		want   tuning.Applicability
	}{
		{"archive with the gate", repl, jobspec.Archive, tuning.Applicability{Delete: true, Gate: true}},
		{"purge with the gate", repl, jobspec.Purge, tuning.Applicability{Delete: true, Gate: true}},
		{"archive without replication", "", jobspec.Archive, tuning.Applicability{Delete: true}},
		{"copy-only with replication", repl, jobspec.CopyOnly, tuning.Applicability{}},
	} {
		doc := parseDoc(t, tc.global, "", "j", tc.typ)
		spec := doc.Jobs["j"]
		var stops int
		store := newRunStore(doc, spec, func() { stops++ })
		start := store.Start()
		proc := spec.EffectiveProcessing(doc.Shared.Processing)
		if start.BatchSize != proc.BatchSize || start.BatchDeleteSize != proc.BatchDeleteSize ||
			start.SleepSeconds != proc.SleepSeconds || start.DeleteSleepSeconds != proc.DeleteSleepSeconds ||
			start.SecondsBehindSourceWithin != doc.Shared.Replication.SecondsBehindSourceWithin || start.CheckInterval != doc.Shared.Replication.CheckInterval {
			t.Errorf("%s: store starts at %+v, want the job's processing values and the configured gate values", tc.name, start)
		}
		for _, k := range tuning.Keys() {
			delete := k == tuning.BatchDeleteSize || k == tuning.DeleteSleepSeconds
			gate := k == tuning.SecondsBehindSourceWithin || k == tuning.CheckInterval
			wantApplies := (!delete && !gate) || (delete && tc.want.Delete) || (gate && tc.want.Gate)
			if got, _ := store.Applies(k); got != wantApplies {
				t.Errorf("%s: %s applies = %v, want %v", tc.name, k, got, wantApplies)
			}
		}
		if _, err := store.Stop(tuning.Peer{UID: 1}); err != nil || stops != 1 {
			t.Errorf("%s: a stop through the store called RequestStop %d times (%v), want once", tc.name, stops, err)
		}
	}
}

// fakeRecorder and fakeStatuser are the sources the adapters read, in place of the gate and the tracker.
type fakeRecorder struct{ rec replication.Record }

func (f fakeRecorder) Record() replication.Record { return f.rec }

type fakeStatuser struct{ st engine.ProgressStatus }

func (f fakeStatuser) Status() engine.ProgressStatus { return f.st }

// TestGateRecordSource (spec §7, plan C6): the adapter maps the gate's record to the server's field for
// field, and a gate that does not exist yet (the server binds before the run connects) answers an
// empty record.
func TestGateRecordSource(t *testing.T) {
	var slot gateSlot
	if got := slot.Record(); len(got.Servers) != 0 || !got.HoldingSince.IsZero() || !got.LastPass.IsZero() {
		t.Errorf("a record before the gate exists = %+v, want it empty", got)
	}
	lag := int64(42)
	at := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	rec := replication.Record{
		HoldingSince: at.Add(-time.Minute), LastPass: at.Add(-time.Hour),
		Servers: []replication.ServerVerdict{
			{Server: "r1:3306", Healthy: false, Cause: "unhealthy", Detail: "lag=42s", Lag: &lag, Tolerance: 10, CheckedAt: at},
			{Server: "r2:3306", Healthy: true, Tolerance: 10, CheckedAt: at},
			{Server: "r3:3306"},
		},
	}
	slot.set(fakeRecorder{rec})
	got := slot.Record()
	if !got.HoldingSince.Equal(rec.HoldingSince) || !got.LastPass.Equal(rec.LastPass) || len(got.Servers) != 3 {
		t.Fatalf("record = %+v, want the gate's", got)
	}
	for i, want := range rec.Servers {
		g := got.Servers[i]
		if g.Server != want.Server || g.Healthy != want.Healthy || g.Cause != want.Cause || g.Detail != want.Detail ||
			g.Tolerance != want.Tolerance || !g.CheckedAt.Equal(want.CheckedAt) || (g.Lag == nil) != (want.Lag == nil) {
			t.Errorf("server %d: %+v, want %+v", i, g, want)
		}
	}
	if got.Servers[0].Lag == nil || *got.Servers[0].Lag != 42 {
		t.Errorf("lag = %v, want 42", got.Servers[0].Lag)
	}
	*got.Servers[0].Lag = 1 // the server's copy is its own
	if lag != 42 {
		t.Error("the adapter handed out the gate's own lag")
	}
}

// TestTrackerSource: the adapter maps the engine's status to the server's: the phase, the activity, the
// pause, every counter, the tables in order, and the estimate with whether its ETA is known.
func TestTrackerSource(t *testing.T) {
	since := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	st := engine.ProgressStatus{
		Phase: engine.PhaseRecovering, Step: "delete", Table: "orders", WaitingSince: since, Sentinel: true,
		Batches: 1, Roots: 2, RootsSkipped: 3, RootsRecovered: 4, RowsCopied: 5, RowsDeleted: 6,
		Tables:   []engine.TableCounts{{Name: "orders", Copied: 7, Deleted: 8}, {Name: "lines", Copied: 9, Deleted: 10}},
		Estimate: &engine.ProgressEstimate{Roots: 100, Percent: 12.5, ETA: time.Minute, ETAKnown: true},
	}
	got := trackerSource{fakeStatuser{st}}.Status()
	if got.Phase != control.PhaseRecovering || got.Step != "delete" || got.Table != "orders" || !got.WaitingSince.Equal(since) || !got.Sentinel ||
		got.Batches != 1 || got.Roots != 2 || got.RootsSkipped != 3 || got.RootsRecovered != 4 || got.RowsCopied != 5 || got.RowsDeleted != 6 {
		t.Errorf("status = %+v", got)
	}
	if len(got.Tables) != 2 || got.Tables[0] != (control.TableCounts{Name: "orders", Copied: 7, Deleted: 8}) || got.Tables[1] != (control.TableCounts{Name: "lines", Copied: 9, Deleted: 10}) {
		t.Errorf("tables = %+v, want them in order", got.Tables)
	}
	if e := got.Estimate; e == nil || e.Roots != 100 || e.Percent != 12.5 || e.ETA != time.Minute || !e.ETAKnown {
		t.Errorf("estimate = %+v", e)
	}
	for _, tc := range []struct {
		engine string
		want   control.Phase
	}{{engine.PhaseStarting, control.PhaseStarting}, {engine.PhaseRunning, control.PhaseRunning}, {"", control.PhaseStarting}} {
		if got := (trackerSource{fakeStatuser{engine.ProgressStatus{Phase: tc.engine}}}).Status().Phase; got != tc.want {
			t.Errorf("phase %q = %q, want %q", tc.engine, got, tc.want)
		}
	}
	if got := (trackerSource{fakeStatuser{}}).Status(); got.Estimate != nil {
		t.Errorf("a status with no estimate maps to %+v, want none", got.Estimate)
	}
}

// TestRuntimeChanges (spec §4.10): the line, for each setting changed its configured value, its final
// value and the number of changes (a setting changed and restored is still listed), a tolerance that
// ends above its configuration marked, the pauses with their time; nothing at all when nothing was
// accepted.
func TestRuntimeChanges(t *testing.T) {
	for _, tc := range []struct {
		name string
		sum  tuning.Summary
		want string
	}{
		{"nothing", tuning.Summary{}, ""},
		{"one setting, two changes, one pause", tuning.Summary{
			Settings: []tuning.SettingSummary{{Key: tuning.BatchDeleteSize, Configured: "500", Final: "100", Changes: 2}},
			Pauses:   1, PausedFor: 4*time.Minute + 10*time.Second},
			"batch_delete_size 500 -> 100 (2 changes); paused 1 time for 4m10s"},
		{"one change", tuning.Summary{Settings: []tuning.SettingSummary{{Key: tuning.SleepSeconds, Configured: "1", Final: "0.5", Changes: 1}}},
			"sleep_seconds 1 -> 0.5 (1 change)"},
		{"restored is still listed", tuning.Summary{Settings: []tuning.SettingSummary{{Key: tuning.BatchSize, Configured: "1000", Final: "1000", Changes: 2}}},
			"batch_size 1000 -> 1000 (2 changes)"},
		{"tolerance above the configuration", tuning.Summary{Settings: []tuning.SettingSummary{
			{Key: tuning.SecondsBehindSourceWithin, Configured: "10", Final: "40", Changes: 3, AboveConfigured: true}}},
			"seconds_behind_source_within 10 -> 40 (3 changes, above the configured value)"},
		{"several settings and pauses", tuning.Summary{
			Settings: []tuning.SettingSummary{{Key: tuning.BatchSize, Configured: "1000", Final: "500", Changes: 1}, {Key: tuning.CheckInterval, Configured: "5", Final: "2", Changes: 4}},
			Pauses:   2, PausedFor: 65 * time.Second},
			"batch_size 1000 -> 500 (1 change); check_interval 5 -> 2 (4 changes); paused 2 times for 1m05s"},
		{"only a pause", tuning.Summary{Pauses: 1, PausedFor: 900 * time.Millisecond}, "paused 1 time for 0s"},
	} {
		if got := runtimeChanges(tc.sum); got != tc.want {
			t.Errorf("%s: runtimeChanges = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestClosingOutputRuntimeChanges (spec §7 row 67): the runtime-changes line is in the closing entry
// and in the closing block, equal in both, and in neither when nothing was accepted.
func TestClosingOutputRuntimeChanges(t *testing.T) {
	rep := &engine.Report{Batches: 2, Roots: 5, Copied: map[string]int64{"orders": 5}, Deleted: map[string]int64{"orders": 5}, Elapsed: time.Second}
	changes := "batch_delete_size 500 -> 100 (2 changes); paused 1 time for 4m10s"

	var block strings.Builder
	printReport(&block, "j", jobspec.Archive, []string{"orders"}, rep, nil, changes)
	lines := strings.Split(block.String(), "\n")
	if len(lines) < 3 || lines[1] != "  roots: 5 in 2 batches" || lines[2] != "  runtime changes: "+changes {
		t.Errorf("closing block:\n%s\nwant the runtime-changes line right under roots", block.String())
	}

	entry := func(changes string) map[string]any {
		path := filepath.Join(t.TempDir(), "run.log")
		log, err := logger.New(&config.LoggingConfig{Level: "info", Format: "json", Output: path, FileOnly: true}, logger.Options{})
		if err != nil {
			t.Fatal(err)
		}
		logOutcome(log, jobspec.Archive, rep, nil, false, changes)
		_ = log.Close()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var e map[string]any
		if err := json.Unmarshal(bytes.TrimSpace(data), &e); err != nil {
			t.Fatalf("closing entry %q: %v", data, err)
		}
		return e
	}
	if got := entry(changes)["runtime_changes"]; got != changes {
		t.Errorf("closing entry's runtime_changes = %v, want the block's line %q", got, changes)
	}

	// With nothing accepted neither carries it.
	block.Reset()
	printReport(&block, "j", jobspec.Archive, []string{"orders"}, rep, nil, "")
	if strings.Contains(block.String(), "runtime changes") {
		t.Errorf("closing block with nothing accepted:\n%s", block.String())
	}
	if _, has := entry("")["runtime_changes"]; has {
		t.Error("the closing entry has runtime_changes with nothing accepted")
	}
}

// TestSocketPathInLog (spec §7 row 70): a run with control enabled writes `Control socket: <path>` once
// in its log, under level: error too; with a log file nothing of it reaches stdout or stderr, with the
// tee on and off, with file_only on and off; on a console log it is one entry of that log.
func TestSocketPathInLog(t *testing.T) {
	const path = "/home/archiver/.goarchive/run/job.sock"
	for _, tc := range []struct{ tee, fileOnly bool }{{false, false}, {false, true}, {true, false}, {true, true}} {
		file := filepath.Join(t.TempDir(), "run.log")
		var out, errb bytes.Buffer
		log, err := logger.New(&config.LoggingConfig{Level: "error", Format: "json", Output: file, FileOnly: tc.fileOnly}, logger.Options{Stdout: &out, Stderr: &errb, Tee: tc.tee})
		if err != nil {
			t.Fatal(err)
		}
		logControlSocket(log, path)
		_ = log.Close()
		data, _ := os.ReadFile(file)
		if n := strings.Count(string(data), "Control socket: "+path); n != 1 {
			t.Errorf("tee %v, file_only %v: the log has the entry %d times, want once:\n%s", tc.tee, tc.fileOnly, n, data)
		}
		if out.Len() != 0 || errb.Len() != 0 {
			t.Errorf("tee %v, file_only %v: stdout %q, stderr %q, want nothing of the path on either", tc.tee, tc.fileOnly, out.String(), errb.String())
		}
	}

	var out bytes.Buffer
	log, err := logger.New(&config.LoggingConfig{Level: "error", Format: "json", Output: "stdout"}, logger.Options{Stdout: &out})
	if err != nil {
		t.Fatal(err)
	}
	logControlSocket(log, path)
	if n := strings.Count(out.String(), "Control socket: "+path); n != 1 {
		t.Errorf("a log on the console has the entry %d times, want once:\n%s", n, out.String())
	}
}

// TestControlSink: the entries the server writes go past the level filter: an accepted change is an
// info entry, live; a warning and an error keep their levels; fields are kept.
func TestControlSink(t *testing.T) {
	file := filepath.Join(t.TempDir(), "run.log")
	log, err := logger.New(&config.LoggingConfig{Level: "error", Format: "json", Output: file, FileOnly: true}, logger.Options{})
	if err != nil {
		t.Fatal(err)
	}
	sink := logSink{log}
	sink.Entry(tuning.LevelInfo, "Runtime setting changed: batch_size 1000 -> 500", "setting", "batch_size", "revision", uint64(1))
	sink.Entry(tuning.LevelWarn, "Control request refused: ceiling")
	sink.Entry(tuning.LevelError, "Control socket handler panicked: boom")
	_ = log.Close()
	data, _ := os.ReadFile(file)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 3 {
		t.Fatalf("log = %q, want three entries at level error", data)
	}
	var first, second, third map[string]any
	for i, dst := range []*map[string]any{&first, &second, &third} {
		if err := json.Unmarshal([]byte(lines[i]), dst); err != nil {
			t.Fatalf("entry %q: %v", lines[i], err)
		}
	}
	if first["level"] != "info" || first["setting"] != "batch_size" || first["revision"] != 1.0 || second["level"] != "warn" || third["level"] != "error" {
		t.Errorf("entries = %v %v %v", first, second, third)
	}
	if _, has := third["stacktrace"]; has {
		t.Error("the error entry carries a stack trace")
	}
}

// TestControlReport (spec §7 row 71): what validate and dry-run say of a job's control socket: the
// path the job would use, the default directory marked as the invoking user's, off for a job with
// control off, and the reason when the path cannot be formed, which fails the job.
func TestControlReport(t *testing.T) {
	off := parseDoc(t, "", "", "job", jobspec.Archive)
	if line, err := controlReport(off, off.Jobs["job"], "job"); err != nil || line != "control socket: off" {
		t.Errorf("control off = %q, %v, want %q", line, err, "control socket: off")
	}

	// An absolute directory on every platform: /run/goarchive is not one on Windows.
	dir := filepath.Join(os.TempDir(), "goarchive-run")
	set := parseDoc(t, "", "    control:\n      enabled: true\n      socket_dir: '"+dir+"'\n", "job", jobspec.Archive)
	if line, err := controlReport(set, set.Jobs["job"], "job"); err != nil || line != "control socket: "+filepath.Join(dir, "job.sock") {
		t.Errorf("socket_dir set = %q, %v, want its path, with no remark about a default", line, err)
	}

	def := parseDoc(t, "control:\n  enabled: true\n", "", "job", jobspec.Archive)
	want, rerr := controlpath.Resolve(true, "", "job", os.Geteuid())
	line, err := controlReport(def, def.Jobs["job"], "job")
	switch {
	case rerr != nil:
		// This user has no default directory: the report says why, and the job fails as a run would.
		if err == nil || !strings.Contains(line, "control socket: none") || !strings.Contains(line, "control.socket_dir") {
			t.Errorf("no default directory: %q, %v, want the reason and an error", line, err)
		}
	default:
		if err != nil || line != "control socket: "+want+" (the default directory of the invoking user)" {
			t.Errorf("the default = %q, %v, want %q marked as the invoking user's", line, err, want)
		}
	}

	// A path over the limit under the default directory is the run's check, and so the report's.
	long := parseDoc(t, "control:\n  enabled: true\n", "", strings.Repeat("n", 120), jobspec.Archive)
	line, err = controlReport(long, long.Jobs[strings.Repeat("n", 120)], strings.Repeat("n", 120))
	if err == nil || !strings.Contains(line, "control socket: none") {
		t.Errorf("a path over the limit = %q, %v, want it refused with a reason", line, err)
	}
}

// TestExitCodeSocketInUse (spec §4.11): a run that finds its control socket in use exits 4, "not
// started", whether the lock is held or a live socket answered; every other socket failure exits 1.
func TestExitCodeSocketInUse(t *testing.T) {
	started = true
	defer func() { started = false }()
	for _, err := range []error{
		&control.InUseError{Path: "/x/job.lock", Lock: true, Job: "job", Dir: "/x"},
		&control.InUseError{Path: "/x/job.sock", Job: "job", Dir: "/x"},
		fmt.Errorf("start: %w", &control.InUseError{Path: "/x/job.lock", Lock: true}),
	} {
		if got := exitCode(err); got != exitNotStarted {
			t.Errorf("exitCode(%v) = %d, want %d", err, got, exitNotStarted)
		}
	}
	for _, err := range []error{
		errors.New("control socket directory \"/tmp\" is owned by uid 0"),
		&controlpath.NoDefaultError{UID: 1, Reason: "no entry"},
	} {
		if got := exitCode(err); got != exitFailed {
			t.Errorf("exitCode(%v) = %d, want %d", err, got, exitFailed)
		}
	}
}

// controlParams builds a run's start of its control socket for the doc's job, in a short directory.
func testControlParams(t *testing.T, global, jobControl string) controlParams {
	t.Helper()
	doc := parseDoc(t, global, jobControl, "job", jobspec.Archive)
	spec := doc.Jobs["job"]
	shutdown := newTestShutdown(t)
	return controlParams{
		doc: doc, spec: spec, name: "job", typ: spec.Type,
		store: newRunStore(doc, spec, shutdown.RequestFirst), tracker: engine.NewProgress(time.Now()),
		shutdown: shutdown, log: logger.NewDefault(), stderr: &bytes.Buffer{}, started: time.Now(),
	}
}

func controlYAML(dir string) string {
	return "    control:\n      enabled: true\n      socket_dir: " + dir + "\n"
}

// TestStartControl: a run with control off starts nothing and creates nothing; with it on the socket is
// served before anything connects, status answers, a change is accepted and is in the summary the end
// returns; the end leaves the socket and the lock gone; a second start on the same path is an in-use
// error, exit 4; and a stop over the socket reaches the shutdown handler.
func TestStartControl(t *testing.T) {
	off := testControlParams(t, "", "")
	ctl, err := startControl(off)
	if err != nil || ctl == nil || ctl.srv != nil {
		t.Fatalf("control off: %v, %v, want a controlRun with no server", ctl, err)
	}
	if sum := ctl.end(); len(sum.Settings) != 0 || sum.Pauses != 0 {
		t.Errorf("control off: summary %+v, want none", sum)
	}
	if err := ctl.close(); err != nil {
		t.Errorf("control off: close = %v", err)
	}

	dir := filepath.Join(shortTemp(t), "run")
	p := testControlParams(t, "", controlYAML(dir))
	ctl, err = startControl(p)
	if err != nil || ctl.srv == nil {
		t.Fatalf("startControl = %v, %v", ctl, err)
	}
	sock := filepath.Join(dir, "job.sock")
	if ctl.srv.Path() != sock {
		t.Errorf("socket = %q, want %q", ctl.srv.Path(), sock)
	}
	if got := askSocket(t, sock, "status-json"); !strings.Contains(got, `"state":"starting"`) || !strings.Contains(got, `"job":"job"`) ||
		!strings.Contains(got, `"job_type":"archive"`) || !strings.Contains(got, `"socket":"`+sock+`"`) {
		t.Errorf("status-json = %q", got)
	}
	if got := askSocket(t, sock, "batch_size=500"); !strings.HasPrefix(got, "OK\nbatch_size: 1000 -> 500") {
		t.Errorf("setter = %q", got)
	}

	second := testControlParams(t, "", controlYAML(dir))
	if _, err := startControl(second); err == nil || exitCode(err) != exitNotStarted {
		t.Errorf("a second start = %v (exit %d), want an in-use error, exit %d", err, exitCode(err), exitNotStarted)
	}

	if got := askSocket(t, sock, "stop"); !strings.HasPrefix(got, "OK\nstop requested") {
		t.Errorf("stop = %q", got)
	}
	select {
	case <-p.shutdown.Stop():
	case <-time.After(2 * time.Second):
		t.Fatal("a stop over the socket did not reach the shutdown handler")
	}

	sum := ctl.end()
	if again := ctl.end(); fmt.Sprint(again) != fmt.Sprint(sum) {
		t.Errorf("a second end = %+v, want the same frozen summary %+v", again, sum)
	}
	if len(sum.Settings) != 1 || sum.Settings[0].Key != tuning.BatchSize || sum.Settings[0].Final != "500" {
		t.Errorf("summary = %+v, want the one change", sum.Settings)
	}
	if got := askSocket(t, sock, "status-json"); !strings.Contains(got, `"state":"ending"`) {
		t.Errorf("status after the end = %q, want ending", got)
	}
	if err := ctl.close(); err != nil {
		t.Errorf("close = %v", err)
	}
	if _, err := os.Lstat(sock); !os.IsNotExist(err) {
		t.Errorf("the socket is left after close: %v", err)
	}
}

// TestStartControlRefusals: a run whose control socket cannot be set up fails before it connects, with an
// ordinary error (exit 1) naming the problem: a directory open to others, and a gate slot that is empty
// until the gate exists.
func TestStartControlRefusals(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("directory modes are not enforced on Windows")
	}
	open := filepath.Join(shortTemp(t), "open")
	if err := os.Mkdir(open, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(open, 0o755); err != nil {
		t.Fatal(err)
	}
	started = true
	defer func() { started = false }()
	_, err := startControl(testControlParams(t, "", controlYAML(open)))
	if err == nil || exitCode(err) != exitFailed || !strings.Contains(err.Error(), open) {
		t.Errorf("startControl over an open directory = %v (exit %d), want a failure naming it", err, exitCode(err))
	}
}

// TestReportsPrintSocketPath (spec §7 row 71): dry-run prints the control socket in its Settings section
// and validate under the job: the path a job with control enabled would use, the default directory
// marked as the invoking user's, `off` for a job with control off; a job whose path cannot be formed
// prints the reason, and validate fails it.
func TestReportsPrintSocketPath(t *testing.T) {
	render := func(doc *jobspec.Document, name string) string {
		var out strings.Builder
		printExplanation(&out, name, jobspec.Archive, "c.yaml", doc, doc.Jobs[name], jobspec.VerifySHA256, dryRunExplanation())
		return out.String()
	}
	section := func(report string) string {
		_, after, _ := strings.Cut(report, "Settings\n")
		before, _, _ := strings.Cut(after, "\n\nStatement caps")
		return before
	}

	off := parseDoc(t, "", "", "job", jobspec.Archive)
	if got := section(render(off, "job")); !strings.Contains(got, "\n  control socket: off") {
		t.Errorf("dry-run, control off, Settings:\n%s\nwant `control socket: off`", got)
	}
	set := parseDoc(t, "", controlYAML("/run/goarchive"), "job", jobspec.Archive)
	if got := section(render(set, "job")); !strings.Contains(got, "\n  control socket: /run/goarchive/job.sock") || strings.Contains(got, "invoking user") {
		t.Errorf("dry-run, socket_dir set, Settings:\n%s\nwant its path with no remark about a default", got)
	}
	def := parseDoc(t, "control:\n  enabled: true\n", "", "job", jobspec.Archive)
	line, _ := controlReport(def, def.Jobs["job"], "job")
	if got := section(render(def, "job")); !strings.Contains(got, "\n  "+line) {
		t.Errorf("dry-run, the default, Settings:\n%s\nwant %q", got, line)
	}
	long := strings.Repeat("n", 120)
	over := parseDoc(t, "control:\n  enabled: true\n", "", long, jobspec.Archive)
	if got := section(render(over, long)); !strings.Contains(got, "\n  control socket: none: ") {
		t.Errorf("dry-run, a path that cannot be formed, Settings:\n%s\nwant the reason", got)
	}

	// validate: a line under the job; a job that cannot start fails.
	var out strings.Builder
	if !printControlLine(&out, off, off.Jobs["job"], "job") || out.String() != "   control socket: off\n" {
		t.Errorf("validate, control off: %q", out.String())
	}
	out.Reset()
	if !printControlLine(&out, set, set.Jobs["job"], "job") || out.String() != "   control socket: /run/goarchive/job.sock\n" {
		t.Errorf("validate, socket_dir set: %q", out.String())
	}
	out.Reset()
	if printControlLine(&out, over, over.Jobs[long], long) || !strings.Contains(out.String(), "   control socket: none: ") || !strings.Contains(out.String(), "FAILED") {
		t.Errorf("validate, a path that cannot be formed: %q, want the reason and a failure", out.String())
	}
}
