package replication

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/logger"
)

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
	sleep func(context.Context, time.Duration) error

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

// sleepUntil waits for d or for the context, whichever comes first. It is the
// production seam; tests inject a deterministic replacement.
func sleepUntil(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
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
