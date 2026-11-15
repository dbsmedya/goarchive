package control

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// The status is read from four sources, none of which blocks the run (spec §4.8): the engine's
// tracker, the gate's record, the settings store and the shutdown state. The package imports
// neither the engine nor the replication package, so each source is an interface here and the
// command adapts its own types to them. A status read is pure: it writes no log entry, emits
// nothing and queries nothing.

// Phase is where the run is, as the tracker knows it.
type Phase string

const (
	// PhaseStarting is connecting, preflight, locks and registration.
	PhaseStarting Phase = "starting"
	// PhaseRecovering is replaying the roots a log still holds.
	PhaseRecovering Phase = "recovering"
	// PhaseRunning is the batch loop, working or sleeping between batches.
	PhaseRunning Phase = "running"
)

// TableCounts are what a run has committed on one table.
type TableCounts struct {
	Name    string
	Copied  int64
	Deleted int64
}

// Estimate is the optimizer's estimate of the roots in range, which only a --progress run reads.
type Estimate struct {
	Roots   int64
	Percent float64
	ETA     time.Duration
}

// TrackerStatus is what the engine's progress tracker reports. The counters count committed work
// and never go down within a run.
type TrackerStatus struct {
	Phase Phase
	// Step is what the run is doing (fetch, discover, copy, delete, log, sleep, wait), Table the
	// table the step works on where it has one; both are empty when the run does nothing yet.
	Step, Table string
	// WaitingSince is when the loop began waiting in the pause gate; zero when it is not waiting.
	WaitingSince time.Time
	// Sentinel says that the pause gate saw the sentinel file at its last check.
	Sentinel bool

	Batches, Roots, RootsSkipped, RootsRecovered, RowsCopied, RowsDeleted int64
	Tables                                                                []TableCounts
	// Estimate is nil without --progress and before the estimate is read.
	Estimate *Estimate
}

// TrackerSource is the engine's tracker, as the status reads it.
type TrackerSource interface{ Status() TrackerStatus }

// ServerVerdict is the gate's last verdict on one server. A server never probed has a zero
// CheckedAt.
type ServerVerdict struct {
	Server  string
	Healthy bool
	// Cause is unreachable, unreadable or unhealthy, and empty for a healthy server.
	Cause, Detail string
	// Lag is the largest lag among the gated channels in seconds; nil when it is NULL or the read
	// failed.
	Lag       *int64
	Tolerance int64
	CheckedAt time.Time
}

// GateRecord is the gate's last-verdict record. HoldingSince is zero when the gate is not holding.
type GateRecord struct {
	Servers      []ServerVerdict
	HoldingSince time.Time
	LastPass     time.Time
}

// GateRecordSource is the gate's record, as the status reads it. A run without the gate has none.
type GateRecordSource interface{ Record() GateRecord }

// ShutdownSource is the shutdown state: whether a stop was requested by a signal.
type ShutdownSource interface{ StopRequested() bool }

// Info is what the status says of the run that does not change.
type Info struct {
	Version, Job, JobType, ConfigFile string
	PID                               int
	StartedAt                         time.Time
	// SentinelFile is processing.sentinel_file, empty when none is configured.
	SentinelFile string
	// ReplicationEnabled is replication.enabled; CacheTTL is replication.cache_ttl in seconds, a
	// configured value that is not a setting.
	ReplicationEnabled bool
	CacheTTL           int
}

// view renders the status from its sources. The store and the tracker are required; gate is nil
// for a run without the gate, shutdown may be nil.
type view struct {
	info     Info
	socket   string
	store    *tuning.Store
	tracker  TrackerSource
	gate     GateRecordSource
	shutdown ShutdownSource
	now      func() time.Time
}

// read assembles one snapshot from separate reads: each value is exact, and two values may be a
// moment apart.
type read struct {
	now     time.Time
	snap    *tuning.Snapshot
	ended   bool
	tracker TrackerStatus
	gate    GateRecord
	hasGate bool
	stop    bool
}

func (v *view) read() read {
	r := read{now: v.now(), snap: v.store.Load(), ended: v.store.Ended(), tracker: v.tracker.Status()}
	if v.gate != nil {
		r.gate, r.hasGate = v.gate.Record(), true
	}
	r.stop = r.snap.Stopping || (v.shutdown != nil && v.shutdown.StopRequested())
	return r
}

func (r read) holding() bool { return r.hasGate && !r.gate.HoldingSince.IsZero() }

// state is the one word, by precedence (spec §4.8): ending, stopping, held, paused, recovering,
// running, starting.
func (r read) state() string {
	switch {
	case r.ended:
		return "ending"
	case r.stop:
		return "stopping"
	case r.holding():
		return "held"
	case !r.tracker.WaitingSince.IsZero():
		return "paused"
	case r.tracker.Phase == PhaseRecovering:
		return "recovering"
	case r.tracker.Phase == PhaseRunning:
		return "running"
	}
	return "starting"
}

// requested lists the inputs that ask for a pause.
func (r read) requested() []string {
	out := []string{}
	if r.snap.Paused {
		out = append(out, "socket")
	}
	if r.tracker.Sentinel {
		out = append(out, "sentinel")
	}
	return out
}

// failing lists the servers whose last verdict is a cause of a hold.
func (r read) failing() []ServerVerdict {
	var out []ServerVerdict
	if !r.hasGate {
		return out
	}
	for _, s := range r.gate.Servers {
		if s.Cause != "" {
			out = append(out, s)
		}
	}
	return out
}

func rfc3339(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func nullable(t time.Time) *string {
	if t.IsZero() {
		return nil
	}
	s := rfc3339(t)
	return &s
}

type activityJSON struct {
	Step  string `json:"step"`
	Table string `json:"table,omitempty"`
}

type pauseJSON struct {
	Requested    []string `json:"requested"`
	WaitingSince *string  `json:"waiting_since"`
	SentinelFile string   `json:"sentinel_file,omitempty"`
}

type verdictJSON struct {
	Server     string  `json:"server"`
	Healthy    *bool   `json:"healthy"`
	Cause      string  `json:"cause"`
	Detail     string  `json:"detail"`
	LagSeconds *int64  `json:"lag_seconds"`
	Tolerance  int64   `json:"tolerance"`
	CheckedAt  *string `json:"checked_at"`
}

type holdJSON struct {
	Since   string        `json:"since"`
	Servers []verdictJSON `json:"servers"`
}

type countersJSON struct {
	Batches        int64 `json:"batches"`
	Roots          int64 `json:"roots"`
	RootsSkipped   int64 `json:"roots_skipped"`
	RootsRecovered int64 `json:"roots_recovered"`
	RowsCopied     int64 `json:"rows_copied"`
	RowsDeleted    int64 `json:"rows_deleted"`
}

type tableJSON struct {
	Copied  int64 `json:"copied"`
	Deleted int64 `json:"deleted"`
}

type progressJSON struct {
	RootsEstimate int64   `json:"roots_estimate"`
	Percent       float64 `json:"percent"`
	ETA           float64 `json:"eta"`
}

type settingJSON struct {
	Value      json.Number `json:"value"`
	Configured json.Number `json:"configured"`
}

type replicationJSON struct {
	Enabled    bool          `json:"enabled"`
	Used       bool          `json:"used"`
	CacheTTL   int           `json:"cache_ttl"`
	LastPassAt *string       `json:"last_pass_at"`
	Servers    []verdictJSON `json:"servers"`
}

// statusJSON is the object of status-json, part of the protocol contract (spec §4.3): adding a
// field keeps protocol 1, renaming or removing one raises it.
type statusJSON struct {
	Protocol       int                    `json:"protocol"`
	Version        string                 `json:"version"`
	Job            string                 `json:"job"`
	JobType        string                 `json:"job_type"`
	ConfigFile     string                 `json:"config_file"`
	PID            int                    `json:"pid"`
	Socket         string                 `json:"socket"`
	StartedAt      string                 `json:"started_at"`
	Now            string                 `json:"now"`
	ElapsedSeconds float64                `json:"elapsed_seconds"`
	State          string                 `json:"state"`
	StopRequested  bool                   `json:"stop_requested"`
	Activity       *activityJSON          `json:"activity"`
	Pause          pauseJSON              `json:"pause"`
	Hold           *holdJSON              `json:"hold"`
	Counters       countersJSON           `json:"counters"`
	Tables         map[string]tableJSON   `json:"tables"`
	Progress       *progressJSON          `json:"progress"`
	Settings       map[string]settingJSON `json:"settings"`
	Replication    replicationJSON        `json:"replication"`
	Changes        uint64                 `json:"changes"`
}

func verdicts(in []ServerVerdict) []verdictJSON {
	out := []verdictJSON{}
	for _, s := range in {
		j := verdictJSON{Server: s.Server, Cause: s.Cause, Detail: s.Detail, LagSeconds: s.Lag, Tolerance: s.Tolerance, CheckedAt: nullable(s.CheckedAt)}
		if !s.CheckedAt.IsZero() {
			healthy := s.Healthy
			j.Healthy = &healthy
		}
		out = append(out, j)
	}
	return out
}

// jsonLine is the snapshot as one JSON object on one line.
func (v *view) jsonLine() (string, error) {
	r := v.read()
	st := r.tracker
	j := statusJSON{
		Protocol: protocolVersion, Version: v.info.Version, Job: v.info.Job, JobType: v.info.JobType,
		ConfigFile: v.info.ConfigFile, PID: v.info.PID, Socket: v.socket,
		StartedAt: rfc3339(v.info.StartedAt), Now: rfc3339(r.now), ElapsedSeconds: r.now.Sub(v.info.StartedAt).Seconds(),
		State: r.state(), StopRequested: r.stop,
		Pause: pauseJSON{Requested: r.requested(), WaitingSince: nullable(st.WaitingSince), SentinelFile: v.info.SentinelFile},
		Counters: countersJSON{
			Batches: st.Batches, Roots: st.Roots, RootsSkipped: st.RootsSkipped, RootsRecovered: st.RootsRecovered,
			RowsCopied: st.RowsCopied, RowsDeleted: st.RowsDeleted,
		},
		Tables:   map[string]tableJSON{},
		Settings: map[string]settingJSON{},
		Replication: replicationJSON{
			Enabled: v.info.ReplicationEnabled, Used: r.hasGate, CacheTTL: v.info.CacheTTL,
			Servers: verdicts(r.gate.Servers),
		},
		Changes: r.snap.Revision,
	}
	if st.Step != "" {
		j.Activity = &activityJSON{Step: st.Step, Table: st.Table}
	}
	if r.holding() {
		j.Hold = &holdJSON{Since: rfc3339(r.gate.HoldingSince), Servers: verdicts(r.failing())}
	}
	for _, t := range st.Tables {
		j.Tables[t.Name] = tableJSON{Copied: t.Copied, Deleted: t.Deleted}
	}
	if e := st.Estimate; e != nil {
		j.Progress = &progressJSON{RootsEstimate: e.Roots, Percent: e.Percent, ETA: e.ETA.Seconds()}
	}
	if r.hasGate {
		j.Replication.LastPassAt = nullable(r.gate.LastPass)
	}
	for _, k := range v.store.ApplicableKeys() {
		j.Settings[string(k)] = settingJSON{Value: json.Number(r.snap.Text(k)), Configured: json.Number(v.store.Start().Text(k))}
	}

	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(j); err != nil {
		return "", err
	}
	return strings.TrimSuffix(b.String(), "\n"), nil
}

// text is the snapshot as lines: a summary, then the same content in labelled lines (spec §4.8).
func (v *view) text() []string {
	r := v.read()
	st := r.tracker
	state := r.state()
	switch state {
	case "held":
		state += " " + formatDuration(r.now.Sub(r.gate.HoldingSince))
	case "paused":
		state += " " + formatDuration(r.now.Sub(st.WaitingSince))
	}
	lines := []string{fmt.Sprintf("%s (%s): %s, batch %d, roots %d, copied %d, deleted %d, elapsed %s",
		v.info.Job, v.info.JobType, state, st.Batches, st.Roots, st.RowsCopied, st.RowsDeleted,
		formatDuration(r.now.Sub(v.info.StartedAt)))}

	const indent = "             "
	label := func(name, text string) { lines = append(lines, fmt.Sprintf("  %-11s%s", name+":", text)) }
	label("run", fmt.Sprintf("pid %d, config %s", v.info.PID, v.info.ConfigFile))
	label("socket", v.socket)
	switch {
	case st.Step != "" && st.Table != "":
		label("activity", st.Step+" on "+st.Table)
	case st.Step != "":
		label("activity", st.Step)
	default:
		label("activity", "none")
	}

	if failing := r.failing(); len(failing) > 0 {
		for i, s := range failing {
			text := fmt.Sprintf("%s %s (%s), checked %s ago", s.Server, s.Cause, s.Detail, formatDuration(r.now.Sub(s.CheckedAt)))
			if i == 0 {
				label("hold", text)
			} else {
				lines = append(lines, indent+text)
			}
		}
	} else if r.holding() {
		label("hold", "holding since "+rfc3339(r.gate.HoldingSince))
	} else {
		label("hold", "none")
	}

	label("pause", pauseText(r))

	var items []string
	for _, k := range v.store.ApplicableKeys() {
		item := fmt.Sprintf("%s %s", k, r.snap.Text(k))
		if configured := v.store.Start().Text(k); configured != r.snap.Text(k) {
			item += " (configured " + configured + ")"
		}
		items = append(items, item)
	}
	for i := 0; i < len(items); i += 4 {
		group := strings.Join(items[i:min(i+4, len(items))], "   ")
		if i == 0 {
			label("settings", group)
		} else {
			lines = append(lines, indent+group)
		}
	}

	// The spec's example ends at the settings. What status-json has that it does not show follows,
	// in this order: the gate, the changes accepted, the estimate, the per-table counts.
	lines = append(lines, v.replicationLines(r)...)
	label("changes", changesText(r.snap.Revision))
	if e := st.Estimate; e != nil {
		label("progress", fmt.Sprintf("about %d roots, %.0f%%, ETA %s", e.Roots, e.Percent, formatDuration(e.ETA)))
	}
	for i, t := range st.Tables {
		text := fmt.Sprintf("%s copied %d, deleted %d", t.Name, t.Copied, t.Deleted)
		if i == 0 {
			label("tables", text)
		} else {
			lines = append(lines, indent+text)
		}
	}
	return lines
}

// replicationIndent lines a server's line up under the text of the replication line, whose label is
// one character longer than the others' column.
const replicationIndent = "               "

// replicationLines says whether the replication block is enabled and whether this run uses the
// gate; for a run that does, the configured cache_ttl, when the last pass was and each server's last
// verdict with when it was checked.
func (v *view) replicationLines(r read) []string {
	if !r.hasGate {
		if !v.info.ReplicationEnabled {
			return []string{"  replication: not enabled"}
		}
		return []string{"  replication: enabled, not used by this run"}
	}
	enabled := "not enabled"
	if v.info.ReplicationEnabled {
		enabled = "enabled"
	}
	last := "last pass never"
	if !r.gate.LastPass.IsZero() {
		last = fmt.Sprintf("last pass %s ago (%s)", formatDuration(r.now.Sub(r.gate.LastPass)), rfc3339(r.gate.LastPass))
	}
	lines := []string{fmt.Sprintf("  replication: %s, used, cache_ttl %ds, %s", enabled, v.info.CacheTTL, last)}
	for _, s := range r.gate.Servers {
		lines = append(lines, replicationIndent+verdictText(s, r.now))
	}
	return lines
}

// verdictText is one server's last verdict on one line, with its age and the time it was checked.
func verdictText(s ServerVerdict, now time.Time) string {
	if s.CheckedAt.IsZero() {
		return s.Server + " not checked yet"
	}
	age, at := formatDuration(now.Sub(s.CheckedAt)), rfc3339(s.CheckedAt)
	if s.Cause != "" {
		return fmt.Sprintf("%s %s (%s), checked %s ago (%s)", s.Server, s.Cause, s.Detail, age, at)
	}
	lag := "lag unknown"
	if s.Lag != nil {
		lag = fmt.Sprintf("lag %ds", *s.Lag)
	}
	return fmt.Sprintf("%s healthy, %s, tolerance %ds, checked %s ago (%s)", s.Server, lag, s.Tolerance, age, at)
}

// changesText counts the runtime changes the run has accepted (the store's revision).
func changesText(n uint64) string {
	switch n {
	case 0:
		return "none"
	case 1:
		return "1 accepted runtime change"
	}
	return fmt.Sprintf("%d accepted runtime changes", n)
}

// pauseText says which inputs hold the run and whether it is actually waiting.
func pauseText(r read) string {
	var inputs []string
	if r.snap.Paused {
		inputs = append(inputs, "control socket")
	}
	if r.tracker.Sentinel {
		inputs = append(inputs, "sentinel file")
	}
	switch list := strings.Join(inputs, ", "); {
	case !r.tracker.WaitingSince.IsZero():
		return fmt.Sprintf("waiting for %s (%s)", formatDuration(r.now.Sub(r.tracker.WaitingSince)), list)
	case list != "":
		return "requested (" + list + ")"
	}
	return "none"
}

// formatDuration shows whole seconds: 5s, 2m10s, 6m02s, 1h00m00s. A negative duration is 0s.
func formatDuration(d time.Duration) string {
	d = max(d, 0).Truncate(time.Second)
	h, m, s := int(d/time.Hour), int(d/time.Minute)%60, int(d/time.Second)%60
	switch {
	case h > 0:
		return fmt.Sprintf("%dh%02dm%02ds", h, m, s)
	case m > 0:
		return fmt.Sprintf("%dm%02ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}
