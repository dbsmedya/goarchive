package cmd

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/dbsmedya/goarchive/v3/internal/control"
	"github.com/dbsmedya/goarchive/v3/internal/controlpath"
	"github.com/dbsmedya/goarchive/v3/internal/database"
	"github.com/dbsmedya/goarchive/v3/internal/engine"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
	"github.com/dbsmedya/goarchive/v3/internal/replication"
	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// The control socket's wiring in a run (TEL-1, spec §4.1 to §4.10): the settings store every run
// builds, the adapters from the engine's and the gate's types to the server's (the server imports
// neither package), and the server's start and end around the run.

// newRunStore builds the settings store a run reads: it starts at the job's processing values and the
// configured gate values, which are the ceilings and the tolerance the step rule measures a raise by,
// and the run's kind says which settings apply. A run without a socket gets the same store and nothing
// writes it, so its behaviour is the configuration's. requestStop is the first phase of a stop.
func newRunStore(doc *jobspec.Document, spec *jobspec.Spec, requestStop func()) *tuning.Store {
	proc := spec.EffectiveProcessing(doc.Shared.Processing)
	repl := doc.Shared.Replication
	return tuning.New(tuning.Options{
		Start: tuning.Settings{
			BatchSize: proc.BatchSize, BatchDeleteSize: proc.BatchDeleteSize,
			SleepSeconds: proc.SleepSeconds, DeleteSleepSeconds: proc.DeleteSleepSeconds,
			SecondsBehindSourceWithin: repl.SecondsBehindSourceWithin, CheckInterval: repl.CheckInterval,
		},
		Applies: tuning.Applicability{
			Delete: spec.Type != jobspec.CopyOnly,
			Gate:   repl.Enabled && spec.Type != jobspec.CopyOnly,
		},
		RequestStop: requestStop,
	})
}

// statuser is the engine's tracker, as the adapter reads it.
type statuser interface{ Status() engine.ProgressStatus }

// trackerSource adapts the engine's progress tracker to the server's TrackerSource.
type trackerSource struct{ s statuser }

func (t trackerSource) Status() control.TrackerStatus {
	st := t.s.Status()
	out := control.TrackerStatus{
		Phase: phaseOf(st.Phase), Step: st.Step, Table: st.Table, WaitingSince: st.WaitingSince, Sentinel: st.Sentinel,
		Batches: st.Batches, Roots: st.Roots, RootsSkipped: st.RootsSkipped, RootsRecovered: st.RootsRecovered,
		RowsCopied: st.RowsCopied, RowsDeleted: st.RowsDeleted,
	}
	for _, tc := range st.Tables {
		out.Tables = append(out.Tables, control.TableCounts{Name: tc.Name, Copied: tc.Copied, Deleted: tc.Deleted})
	}
	if e := st.Estimate; e != nil {
		out.Estimate = &control.Estimate{Roots: e.Roots, Percent: e.Percent, ETA: e.ETA, ETAKnown: e.ETAKnown}
	}
	return out
}

func phaseOf(p string) control.Phase {
	switch p {
	case engine.PhaseRecovering:
		return control.PhaseRecovering
	case engine.PhaseRunning:
		return control.PhaseRunning
	}
	return control.PhaseStarting
}

// recorder is the gate's last-verdict record, as the adapter reads it.
type recorder interface{ Record() replication.Record }

// gateSlot is the server's GateRecordSource for a run that uses the gate. The server binds before the
// run connects, and the gate needs its connections, so the slot is handed to the server first and
// filled when the gate exists; until then it answers an empty record.
type gateSlot struct{ r atomic.Pointer[recorder] }

func (s *gateSlot) set(r recorder) { s.r.Store(&r) }

func (s *gateSlot) Record() control.GateRecord {
	p := s.r.Load()
	if p == nil {
		return control.GateRecord{}
	}
	rec := (*p).Record()
	out := control.GateRecord{HoldingSince: rec.HoldingSince, LastPass: rec.LastPass}
	for _, v := range rec.Servers {
		sv := control.ServerVerdict{
			Server: v.Server, Healthy: v.Healthy, Cause: v.Cause, Detail: v.Detail,
			Tolerance: v.Tolerance, CheckedAt: v.CheckedAt,
		}
		if v.Lag != nil {
			lag := *v.Lag
			sv.Lag = &lag
		}
		out.Servers = append(out.Servers, sv)
	}
	return out
}

// logSink writes the server's audit entries to the run's log, past the level filter as the banners
// are, and by the logger's Audit route, which also reaches the console tee by the tee's own policy.
// An accepted change is an info entry marked live, so that with --progress and a log file it reaches
// stderr whatever the level; a warning keeps its level, and shows there when the level keeps warnings;
// an error is logged without the stack trace the logger attaches at that level. The file holds each
// entry once. (logControlSocket stays on Always, off the tee, so a cron run is silent.)
type logSink struct{ log *logger.Logger }

func (s logSink) Entry(level tuning.Level, msg string, kv ...any) {
	out := s.log.Audit()
	switch level {
	case tuning.LevelInfo:
		out.Infow(msg, append(kv, logger.Live)...)
	case tuning.LevelWarn:
		out.Warnw(msg, kv...)
	default:
		out.Desugar().WithOptions(zap.AddStacktrace(zapcore.FatalLevel)).Sugar().Errorw(msg, kv...)
	}
}

// logControlSocket writes `Control socket: <path>` once, when the socket is bound (spec §4.1): past the
// level filter, to the log's own stream and never to the console tee, so a successful cron-style run
// stays silent on both streams with control enabled.
func logControlSocket(log *logger.Logger, path string) {
	fields := []any{"socket", path}
	if log.File() != "" {
		fields = append(fields, logger.FileOnly)
	}
	log.Always().Infow("Control socket: "+path, fields...)
}

// controlReport is what validate and dry-run say of a job's control socket (spec §4.1): the path the
// job would use, the default directory marked as the invoking user's (it depends on the user), `off`
// for a job with control off, and when the path cannot be formed the reason, with an error: validate
// fails that job as a run would refuse it.
func controlReport(doc *jobspec.Document, spec *jobspec.Spec, name string) (string, error) {
	eff := spec.EffectiveControl(doc.Shared.Control)
	if !eff.Enabled {
		return "control socket: off", nil
	}
	path, err := controlpath.Resolve(true, eff.SocketDir, name, os.Geteuid())
	if err != nil {
		return "control socket: none: " + err.Error(), err
	}
	if eff.SocketDir == "" {
		return "control socket: " + path + " (the default directory of the invoking user)", nil
	}
	return "control socket: " + path, nil
}

// runtimeChanges is the closing line of runtime changes (spec §4.10): for each setting that was
// changed its configured value, its final value and the number of changes, a lag tolerance that ends
// above its configuration marked, and the socket pauses with their time. It is empty when nothing was
// accepted, and then neither the closing entry nor the closing block carries it.
func runtimeChanges(sum tuning.Summary) string {
	var parts []string
	for _, s := range sum.Settings {
		changes, above := "changes", ""
		if s.Changes == 1 {
			changes = "change"
		}
		if s.AboveConfigured {
			above = ", above the configured value"
		}
		parts = append(parts, fmt.Sprintf("%s %s -> %s (%d %s%s)", s.Key, s.Configured, s.Final, s.Changes, changes, above))
	}
	if sum.Pauses > 0 {
		times := "times"
		if sum.Pauses == 1 {
			times = "time"
		}
		parts = append(parts, fmt.Sprintf("paused %d %s for %s", sum.Pauses, times, control.FormatDuration(sum.PausedFor)))
	}
	return strings.Join(parts, "; ")
}

// controlParams are what a run's control socket is built from.
type controlParams struct {
	doc      *jobspec.Document
	spec     *jobspec.Spec
	name     string
	typ      jobspec.Type
	store    *tuning.Store
	tracker  *engine.Progress
	shutdown *database.Shutdown
	log      *logger.Logger
	stderr   io.Writer
	started  time.Time
}

// controlRun is a run's control socket: the server, or nothing for a run with control off. Its methods
// are safe on that nothing.
type controlRun struct {
	srv  *control.Server
	gate gateSlot

	endOnce sync.Once
	sum     tuning.Summary
}

// startControl brings the socket up before the run connects to MySQL (spec §4.1 *Bind first*), so that
// status answers during preflight and startup. A socket that cannot be set up fails the run here,
// naming the path and the reason, with nothing connected or changed; a socket in use is an
// *control.InUseError, which exits 4. A run with control off creates and looks up nothing.
func startControl(p controlParams) (*controlRun, error) {
	c := &controlRun{}
	eff := p.spec.EffectiveControl(p.doc.Shared.Control)
	if !eff.Enabled {
		return c, nil
	}
	path, err := controlpath.Resolve(true, eff.SocketDir, p.name, os.Geteuid())
	if err != nil {
		return nil, err
	}
	configFile, err := filepath.Abs(GetConfigFile())
	if err != nil {
		configFile = GetConfigFile()
	}
	repl := p.doc.Shared.Replication
	opts := control.Options{
		Job: p.name, Dir: filepath.Dir(path), DefaultDir: eff.SocketDir == "", Euid: os.Geteuid(),
		Store: p.store,
		Info: control.Info{
			Version: Version, Job: p.name, JobType: string(p.typ), ConfigFile: configFile, PID: os.Getpid(),
			StartedAt: p.started, SentinelFile: p.spec.EffectiveProcessing(p.doc.Shared.Processing).SentinelFile,
			ReplicationEnabled: repl.Enabled, CacheTTL: repl.CacheTTL,
		},
		Tracker:  trackerSource{p.tracker},
		Shutdown: p.shutdown,
		Sink:     logSink{p.log},
		Banner:   func(b string) { emitBanners(p.log, p.stderr, []string{b}) },
	}
	if gate, _ := p.store.Applies(tuning.SecondsBehindSourceWithin); gate {
		opts.Gate = &c.gate
	}
	if c.srv, err = control.Start(opts); err != nil {
		return nil, err
	}
	logControlSocket(p.log, c.srv.Path())
	return c, nil
}

// setGate hands the gate to the server once the run has connected and built it.
func (c *controlRun) setGate(g recorder) {
	if c != nil {
		c.gate.set(g)
	}
}

// end is the terminal transition and the drain of the audit emitter (spec §4.2 End, steps 1 to 4): from
// here no change is accepted, and what was accepted is in the frozen summary it returns and in the log.
// The run's final output follows it, so the summary and the audit entries agree. It may be called again
// and returns the same summary; a run with no socket has none.
func (c *controlRun) end() tuning.Summary {
	if c == nil || c.srv == nil {
		return tuning.Summary{}
	}
	c.endOnce.Do(func() {
		c.sum = c.srv.Ending()
		c.srv.Drain()
	})
	return c.sum
}

// close is the rest of the end: the listener, the handlers, the run's own socket removed by identity and
// the lock released. It runs after the run's final output. The error says what it left at the path.
func (c *controlRun) close() error {
	if c == nil || c.srv == nil {
		return nil
	}
	c.end()
	return c.srv.Close()
}
