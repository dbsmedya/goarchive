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
// A Gate is safe for concurrent callers: mu guards the cached pass, and the
// hold-loop state machine is scoped to a single WaitForLag call.
type Gate struct {
	enabled   bool
	servers   []server
	tolerance int64
	interval  time.Duration
	ttl       time.Duration

	mu       sync.Mutex
	lastPass time.Time

	// now and sleep are seams: injected in tests so TTL expiry, tick cadence
	// and held durations need no wall clock.
	now   func() time.Time
	sleep func(context.Context, <-chan struct{}, time.Duration) error

	logger *logger.Logger
}

// New builds the gate for one fleet. dbs is index-paired with cfg.Servers, and
// a mismatch or a nil handle is a construction error rather than a surprise at
// first use.
//
// The tolerance, interval and TTL are taken exactly as configured: omission is
// DefaultConfig's job and rejection is validation's, so nothing here clamps or
// defaults a value the operator set.
func New(cfg config.ReplicationConfig, dbs []*sql.DB, log *logger.Logger) (*Gate, error) {
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
		enabled:   true,
		servers:   servers,
		tolerance: int64(cfg.SecondsBehindSourceWithin),
		interval:  time.Duration(cfg.CheckInterval) * time.Second,
		ttl:       time.Duration(cfg.CacheTTL) * time.Second,
		now:       time.Now,
		sleep:     sleepUntil,
		logger:    log,
	}
	g.logStartup()

	return g, nil
}

// sleepUntil waits for d, for the context or for stop, whichever comes first. A
// stop returns nil, so the caller's check decides. It is the production seam;
// tests inject a deterministic replacement.
func sleepUntil(ctx context.Context, stop <-chan struct{}, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-stop:
		return nil
	case <-timer.C:
		return nil
	}
}

// logStartup records the fleet the gate will watch. Its inputs are host:port
// identities and channel names only, so no credential can appear.
func (g *Gate) logStartup() {
	g.logger.Infof("Replication monitoring ENABLED: %d server(s), tolerance %ds, interval %s, cache TTL %s",
		len(g.servers), g.tolerance, g.interval, g.ttl)

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
// of a replica whose host is down blocks for the connect timeout.
func (g *Gate) WaitForLag(ctx context.Context, stop <-chan struct{}) error {
	if !g.enabled {
		return nil
	}

	g.mu.Lock()
	fresh := g.ttl > 0 && !g.lastPass.IsZero() && g.now().Sub(g.lastPass) < g.ttl
	g.mu.Unlock()
	if fresh {
		return nil
	}

	holds := make(map[string]holdInfo)
	var firstHold time.Time

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		holding := !firstHold.IsZero()
		probe, release := ctx, context.CancelFunc(func() {})
		if holding {
			probe, release = withStop(ctx, stop)
		}

		allOK := true
		for _, srv := range g.servers {
			snapshot, err := srv.reader.ReplicaStatus(probe)
			// Before classifying or logging: a query aborted by shutdown is
			// not evidence that the server is unreachable.
			if cerr := ctx.Err(); cerr != nil {
				release()
				return cerr
			}
			// Nor is a probe the stop aborted.
			if err != nil && holding && stopped(stop) {
				release()
				return g.endHold(firstHold)
			}

			var cause holdCause
			var detail string
			ok := true
			switch {
			case err != nil:
				ok, cause, detail = false, classify(err), sanitizeLine(err.Error())
			default:
				if ps := evaluate(snapshot, srv.channels, g.tolerance); len(ps) > 0 {
					ok, cause, detail = false, causeUnhealthy, renderProblems(ps, g.tolerance)
				}
			}

			g.transition(holds, &firstHold, srv.id, ok, cause, detail)
			if !ok {
				allOK = false
			}
		}
		release()

		if allOK {
			g.mu.Lock()
			g.lastPass = g.now()
			g.mu.Unlock()

			if !firstHold.IsZero() {
				g.logger.Infow(fmt.Sprintf("replication gate passed: all %d servers healthy; job resuming after %s held",
					len(g.servers), g.now().Sub(firstHold).Round(time.Second)), logger.Live)
			}

			return nil
		}

		if stopped(stop) {
			return g.endHold(firstHold)
		}
		if err := g.sleep(ctx, stop, g.interval); err != nil {
			return err
		}
		if stopped(stop) {
			return g.endHold(firstHold)
		}
	}
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
			id, cause, detail, g.interval), logger.Live)

		return
	}

	held := now.Sub(prev.since).Round(time.Second)
	if prev.cause == cause {
		g.logger.Warnw(fmt.Sprintf("replication hold: server %s %s (%s); job held, retrying in %s (held %s)",
			id, cause, detail, g.interval, held), logger.Live)

		return
	}

	// A cause change keeps the original entry time: the job has been held
	// continuously, and the duration must not restart.
	holds[id] = holdInfo{cause: cause, since: prev.since}
	g.logger.Warnw(fmt.Sprintf("replication hold: server %s %s (%s); job held, retrying in %s (was: %s; held %s)",
		id, cause, detail, g.interval, prev.cause, held), logger.Live)
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
