package replication

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// ErrStopped reports that a stop request ended a hold (LAG-1). A call returns
// it only once a probe has reported a server unhealthy: a stop never cuts a
// pass short.
var ErrStopped = errors.New("replication: hold ended by a stop request")

// statusReader is the one library behavior the gate needs, named locally so
// the policy layer can be tested without a connection.
type statusReader interface {
	ReplicaStatus(ctx context.Context) ([]dbsrepl.ChannelStatus, error)
}

// server is one monitored replica: its operator-facing identity, the reader
// bound to its connection, and the channels this gate cares about.
type server struct {
	// id is the config's host:port, IPv6-safe. It is the only server identity
	// that reaches a log line — never a DSN, user, or password.
	id string
	// reader reads this server's replication facts.
	reader statusReader
	// channels selects the gated channels: empty means all of them, and ""
	// inside the slice is the default channel.
	channels []string
}

// Gate holds a job while any monitored replica fails replication policy. It
// implements the archiver's lag-waiter seam.
//
// The lag tolerance and the check interval are read from the settings store,
// once per probe round (TEL-1, spec §4.6); cache_ttl is startup configuration. A
// cached pass and every round carry the gate revision they were judged under.
//
// A Gate is safe for concurrent callers: mu guards the cached pass and the
// verdict record, and the hold-loop state machine is scoped to a single
// WaitForLag call. The gate never takes the store's writer lock: it only loads
// the current snapshot and reports completed rounds, which are atomic.
type Gate struct {
	enabled bool
	servers []server
	store   *tuning.Store
	ttl     time.Duration

	mu       sync.Mutex
	lastPass time.Time
	// passRev is the gate revision lastPass was judged under: a call takes a cache
	// hit only under the current gate revision.
	passRev uint64
	// verdicts is the last verdict of each server, index-paired with servers; it
	// is allocated at the first probe, and a server not yet probed has a zero
	// CheckedAt. holdSince is when the current call began holding.
	verdicts  []ServerVerdict
	holdSince time.Time

	// now and sleep are seams: injected in tests so TTL expiry, tick cadence
	// and held durations need no wall clock. sleep waits for the duration, for
	// the context, for stop, or for wake (the signal of the settings snapshot
	// the wait was computed from), whichever comes first.
	now   func() time.Time
	sleep func(ctx context.Context, stop, wake <-chan struct{}, d time.Duration) error

	logger *logger.Logger
}

// New builds the gate for one fleet. dbs is index-paired with cfg.Servers, and
// a mismatch or a nil handle is a construction error rather than a surprise at
// first use.
//
// The tolerance, interval and TTL are taken exactly as configured: omission is
// DefaultConfig's job and rejection is validation's, so nothing here clamps or
// defaults a value the operator set.
//
// store is the settings store the gate reads its tolerance and interval from;
// nil is a store that never changes, at the configured values. A store that
// starts at other values than the configuration is refused: the configured
// tolerance is what the step rule measures a raise by.
func New(cfg config.ReplicationConfig, dbs []*sql.DB, log *logger.Logger, store *tuning.Store) (*Gate, error) {
	if log == nil {
		log = logger.NewDefault()
	}

	if !cfg.Enabled {
		log.Info("Replication monitoring is DISABLED (no replication block enabled)")

		return &Gate{enabled: false, logger: log}, nil
	}

	if len(dbs) != len(cfg.Servers) {
		return nil, fmt.Errorf("replication: %d server configs but %d connections",
			len(cfg.Servers), len(dbs))
	}

	if store == nil {
		store = tuning.Constant(tuning.Settings{
			SecondsBehindSourceWithin: cfg.SecondsBehindSourceWithin,
			CheckInterval:             cfg.CheckInterval,
		})
	}
	if st := store.Start(); st.SecondsBehindSourceWithin != cfg.SecondsBehindSourceWithin || st.CheckInterval != cfg.CheckInterval {
		return nil, fmt.Errorf("replication: the settings store starts at tolerance %ds and interval %ds, but the configuration says %ds and %ds",
			st.SecondsBehindSourceWithin, st.CheckInterval, cfg.SecondsBehindSourceWithin, cfg.CheckInterval)
	}

	servers := make([]server, 0, len(cfg.Servers))
	for i := range cfg.Servers {
		if dbs[i] == nil {
			return nil, fmt.Errorf("replication: nil connection for server %s", cfg.Servers[i].Addr())
		}
		servers = append(servers, server{
			id:       cfg.Servers[i].Addr(),
			reader:   dbsrepl.NewInspector(dbs[i]),
			channels: cfg.Servers[i].Channels,
		})
	}

	g := &Gate{
		enabled: true,
		servers: servers,
		store:   store,
		ttl:     time.Duration(cfg.CacheTTL) * time.Second,
		now:     time.Now,
		sleep:   sleepUntil,
		logger:  log,
	}
	g.logStartup()

	return g, nil
}

// sleepUntil waits for d, for the context, for stop or for wake, whichever comes
// first. A stop or a wake returns nil, so the caller's check decides. It is the
// production seam; tests inject a deterministic replacement.
func sleepUntil(ctx context.Context, stop, wake <-chan struct{}, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-stop:
		return nil
	case <-wake:
		return nil
	case <-timer.C:
		return nil
	}
}

// logStartup records the fleet the gate will watch. Its inputs are host:port
// identities and channel names only, so no credential can appear.
func (g *Gate) logStartup() {
	st := g.store.Load()
	g.logger.Infof("Replication monitoring ENABLED: %d server(s), tolerance %ds, interval %s, cache TTL %s",
		len(g.servers), st.SecondsBehindSourceWithin, config.CheckIntervalDuration(st.CheckInterval), g.ttl)

	for _, srv := range g.servers {
		g.logger.Infof("  server %s channels: %s", srv.id, renderChannelList(srv.channels))
	}
}

// holdInfo is one server's hold state within a single WaitForLag call: why it
// is held, and since when.
type holdInfo struct {
	cause holdCause
	since time.Time
}

// WaitForLag holds the caller until every monitored replica satisfies
// replication policy, or until ctx is cancelled. It is the gate's only public
// behavior, and it satisfies the archiver's lag-waiter seam.
//
// A passing verdict is cached for the configured TTL, so a fast batch pays no
// round trip. Only passes are cached: once a hold begins, every tick re-probes
// at full fidelity, and the job resumes within one check interval of recovery.
// The trade is a bounded detection delay — a failure that begins after a
// cached pass goes unnoticed for up to the TTL.
//
// The hold state machine is scoped to this call: a hold spanning two calls
// logs a fresh entry line in the second, because from the job's perspective it
// is a new hold.
//
// stop is the run's stop channel; nil never closes. A stop ends a hold, never a
// pass: the first probe of a call runs on ctx alone, so a stop before its
// result still lets a healthy fleet pass. Once a probe has reported a server
// unhealthy, a stop ends the call at once with ErrStopped: during the sleep, or
// during a later probe, which runs on a context the stop cancels, since a probe
// of a replica whose host is down blocks for the connect timeout, and whatever
// that probe returns. A cancelled ctx outranks a stop that came with it.
//
// Runtime settings (TEL-1, spec §4.6). Each round loads one snapshot of the
// store and judges every server of the round by its tolerance; its revision is
// the round's gate revision. A pass is valid only for the gate revision it was
// judged under: a cached pass of an older revision never hits, and a round that
// passes under a revision replaced while it ran is discarded, recording and
// returning nothing, and the next round runs at once. The call's hold state is
// kept through a discard, so the stop rules above are unchanged by it. After a
// failing round the wait for the interval ends at once when a gate setting
// changed, and goes on for the time left when another setting did. Every
// completed round, passing or failing, is reported to the store, which is what
// lets a raise of the tolerance above its configured value take its next step.
func (g *Gate) WaitForLag(ctx context.Context, stop <-chan struct{}) error {
	if !g.enabled {
		return nil
	}

	if g.cachedPass() {
		return nil
	}

	holds := make(map[string]holdInfo)
	var firstHold time.Time
	defer func() {
		if firstHold.IsZero() {
			return
		}
		g.mu.Lock()
		if g.holdSince.Equal(firstHold) {
			g.holdSince = time.Time{}
		}
		g.mu.Unlock()
	}()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		snap := g.store.Load()
		tolerance := int64(snap.SecondsBehindSourceWithin)
		interval := config.CheckIntervalDuration(snap.CheckInterval)

		allOK := true
		for i, srv := range g.servers {
			// Decided per probe: once any server has reported unhealthy, in
			// this round or an earlier one, the call is a hold and a stop
			// also aborts the probe.
			holding := !firstHold.IsZero()
			probe, release := ctx, context.CancelFunc(func() {})
			if holding {
				probe, release = withStop(ctx, stop)
			}
			status, err := srv.reader.ReplicaStatus(probe)
			release()
			// Before classifying or logging: a query aborted by shutdown is
			// not evidence that the server is unreachable.
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
			// Nor is a probe the stop aborted. Once the call holds, a stop
			// that arrived during the probe ends it whatever the probe
			// returned: a healthy result is not a pass to finish.
			if holding {
				if err := g.stopHold(ctx, stop, firstHold); err != nil {
					return err
				}
			}

			var cause holdCause
			var detail string
			var lag *int64
			ok := true
			switch {
			case err != nil:
				ok, cause, detail = false, classify(err), sanitizeLine(err.Error())
			default:
				lag = lagOf(status, srv.channels)
				if ps := evaluate(status, srv.channels, tolerance); len(ps) > 0 {
					ok, cause, detail = false, causeUnhealthy, renderProblems(ps, tolerance)
				}
			}

			g.transition(holds, &firstHold, srv.id, ok, cause, detail, interval)
			g.record(i, ok, cause, detail, lag, tolerance, firstHold)
			if !ok {
				allOK = false
			}
		}

		// The round is complete, whatever it found: an unhealthy or unreadable
		// round counts, and bypasses nothing, since those causes hold the job at
		// any tolerance.
		g.store.RoundCompleted(snap.GateRevision)

		if allOK {
			if g.store.Load().GateRevision != snap.GateRevision {
				// Passed under settings that have since been replaced: record
				// nothing, return nothing, and judge the fleet again at once.
				continue
			}
			g.mu.Lock()
			g.lastPass, g.passRev = g.now(), snap.GateRevision
			g.mu.Unlock()

			if !firstHold.IsZero() {
				g.logger.Infow(fmt.Sprintf("replication gate passed: all %d servers healthy; job resuming after %s held",
					len(g.servers), g.now().Sub(firstHold).Round(time.Second)), logger.Live)
			}

			return nil
		}

		if err := g.stopHold(ctx, stop, firstHold); err != nil {
			return err
		}
		if err := g.wait(ctx, stop, snap, interval); err != nil {
			return err
		}
		if err := g.stopHold(ctx, stop, firstHold); err != nil {
			return err
		}
	}
}

// cachedPass reports whether the last pass is a hit: judged under the current gate
// revision and younger than cache_ttl.
func (g *Gate) cachedPass() bool {
	rev := g.store.Load().GateRevision

	g.mu.Lock()
	defer g.mu.Unlock()

	return g.ttl > 0 && !g.lastPass.IsZero() && g.passRev == rev && g.now().Sub(g.lastPass) < g.ttl
}

// wait holds for the check interval after a failing round judged under snap. A change
// of a gate setting (a different gate revision) ends it at once, since the round's
// interval and judgement are obsolete; a change of any other setting only wakes the
// wait, which then goes on for the time that is left. A stop returns nil with time
// left, so the caller's check decides.
func (g *Gate) wait(ctx context.Context, stop <-chan struct{}, snap *tuning.Snapshot, interval time.Duration) error {
	deadline := g.now().Add(interval)

	for {
		cur := g.store.Load()
		if cur.GateRevision != snap.GateRevision {
			return nil
		}
		left := deadline.Sub(g.now())
		if left <= 0 {
			return nil
		}

		wake := cur.Changed()
		if err := g.sleep(ctx, stop, wake, left); err != nil {
			return err
		}
		if stopped(stop) {
			return nil
		}
		select {
		case <-wake:
			// The snapshot was replaced: decide again from the new one.
		default:
			return nil
		}
	}
}

// stopHold decides whether a hold ends here: the context's error once it is
// cancelled, since a second signal outranks the first even when both arrived
// together; else ErrStopped, through endHold, once stop is closed; else nil.
func (g *Gate) stopHold(ctx context.Context, stop <-chan struct{}, firstHold time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if stopped(stop) {
		return g.endHold(firstHold)
	}

	return nil
}

// endHold logs that a stop ended the hold begun at firstHold and returns
// ErrStopped. WARN and Live, like the hold lines.
func (g *Gate) endHold(firstHold time.Time) error {
	g.logger.Warnw(fmt.Sprintf("replication hold ended by a stop request; job held %s",
		g.now().Sub(firstHold).Round(time.Second)), logger.Live)

	return ErrStopped
}

// stopped reports whether stop is closed. A nil channel never is.
func stopped(stop <-chan struct{}) bool {
	select {
	case <-stop:
		return true
	default:
		return false
	}
}

// withStop returns a child of ctx that stop also cancels, for a probe made
// during a hold; the returned cancel ends it and its watcher.
func withStop(ctx context.Context, stop <-chan struct{}) (context.Context, context.CancelFunc) {
	probe, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-stop:
			cancel()
		case <-probe.Done():
		}
	}()

	return probe, cancel
}

// transition advances one server's hold state and emits exactly one log line
// per failing server per tick. Healthy servers that were already healthy say
// nothing. No line is logged above Warn: zap attaches a stack trace at Error
// level, and an unreachable replica must not produce a stack-trace storm.
func (g *Gate) transition(
	holds map[string]holdInfo,
	firstHold *time.Time,
	id string,
	ok bool,
	cause holdCause,
	detail string,
	interval time.Duration,
) {
	prev, holding := holds[id]
	now := g.now()

	if ok {
		if !holding {
			return
		}
		delete(holds, id)
		g.logger.Infow(fmt.Sprintf("replication recovered: server %s healthy; held %s",
			id, now.Sub(prev.since).Round(time.Second)), logger.Live)

		return
	}

	if !holding {
		holds[id] = holdInfo{cause: cause, since: now}
		if firstHold.IsZero() {
			*firstHold = now
		}
		g.logger.Warnw(fmt.Sprintf("replication hold: server %s %s (%s); job held, retrying in %s",
			id, cause, detail, interval), logger.Live)

		return
	}

	held := now.Sub(prev.since).Round(time.Second)
	if prev.cause == cause {
		g.logger.Warnw(fmt.Sprintf("replication hold: server %s %s (%s); job held, retrying in %s (held %s)",
			id, cause, detail, interval, held), logger.Live)

		return
	}

	// A cause change keeps the original entry time: the job has been held
	// continuously, and the duration must not restart.
	holds[id] = holdInfo{cause: cause, since: prev.since}
	g.logger.Warnw(fmt.Sprintf("replication hold: server %s %s (%s); job held, retrying in %s (was: %s; held %s)",
		id, cause, detail, interval, prev.cause, held), logger.Live)
}

// renderChannelList spells a server's channel selection, where an empty
// selection gates every channel the server reports.
func renderChannelList(channels []string) string {
	if len(channels) == 0 {
		return "all"
	}

	names := make([]string, 0, len(channels))
	for _, name := range channels {
		names = append(names, renderChannel(name))
	}

	return strings.Join(names, ", ")
}

// ServerVerdict is the gate's last verdict on one server: what its latest probe found,
// by the tolerance of the round that made it. A server not yet probed has a zero
// CheckedAt.
type ServerVerdict struct {
	// Server is the config's host:port.
	Server string
	// Healthy is false for any of the three causes.
	Healthy bool
	// Cause is "unreachable", "unreadable" or "unhealthy", and empty for a healthy server.
	Cause string
	// Detail is the rendered line the hold log shows for the cause, and empty for a
	// healthy server.
	Detail string
	// Lag is the largest Seconds_Behind_Source among the gated channels, in seconds;
	// nil when a gated channel reports NULL or the read failed.
	Lag *int64
	// Tolerance is the tolerance the verdict was judged by, in seconds.
	Tolerance int64
	// CheckedAt is the time of the probe.
	CheckedAt time.Time
}

// Record is the gate's last-verdict record (spec §4.6): one verdict per server, in
// configuration order, since when the current call has been holding (zero when none
// is), and the time of the last recorded pass (zero when none was). It is a snapshot
// of what the probes found, and its age is its CheckedAt: outside a probe, status
// shows the last verdict, not a live value.
type Record struct {
	Servers      []ServerVerdict
	HoldingSince time.Time
	LastPass     time.Time
}

// Record returns a copy of the gate's record. It makes no probe and takes no lock but
// the gate's own, so a status read never waits for a replica.
func (g *Gate) Record() Record {
	g.mu.Lock()
	defer g.mu.Unlock()

	rec := Record{HoldingSince: g.holdSince, LastPass: g.lastPass}
	for i, srv := range g.servers {
		v := ServerVerdict{Server: srv.id}
		if i < len(g.verdicts) {
			v = g.verdicts[i]
			if v.Lag != nil {
				lag := *v.Lag
				v.Lag = &lag
			}
		}
		rec.Servers = append(rec.Servers, v)
	}

	return rec
}

// record writes server i's verdict, and since when the call holds, under the mutex.
func (g *Gate) record(i int, ok bool, cause holdCause, detail string, lag *int64, tolerance int64, holdSince time.Time) {
	v := ServerVerdict{Server: g.servers[i].id, Healthy: ok, Lag: lag, Tolerance: tolerance, CheckedAt: g.now()}
	if !ok {
		v.Cause, v.Detail = cause.String(), detail
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.verdicts == nil {
		g.verdicts = make([]ServerVerdict, len(g.servers))
		for j := range g.servers {
			g.verdicts[j].Server = g.servers[j].id
		}
	}
	g.verdicts[i] = v
	if !holdSince.IsZero() && g.holdSince.IsZero() {
		g.holdSince = holdSince
	}
}
