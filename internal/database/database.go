// Package database provides MySQL database connection management for GoArchive.
package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/internal/config"
)

const (
	defaultMaxOpenConns = 10
	defaultMaxIdleConns = 5
	defaultConnMaxLife  = 10 * time.Minute
	defaultConnMaxIdle  = 5 * time.Minute
)

// Manager handles database connections for source, destination, and the
// monitored replica fleet.
type Manager struct {
	Source      *sql.DB
	Destination *sql.DB
	// Replicas is index-paired with config.Replication.Servers, so
	// Replicas[i] is the handle for Servers[i] and Servers[i].Addr() is its
	// identity in logs and errors.
	Replicas []*sql.DB
	config   *config.Config
}

// replicaDatabaseConfig maps a fleet entry onto the DSN builder's input.
// TLS now propagates (the old inline replicaCfg dropped it — fixed here).
func replicaDatabaseConfig(s config.ReplicationServerConfig) *config.DatabaseConfig {
	return &config.DatabaseConfig{Host: s.Host, Port: s.Port, User: s.User,
		Password: s.Password, TLS: s.TLS}
}

// utcSessionProbe proves a session honours the time_zone the DSN pinned
// (issue #16). It measures the EFFECTIVE offset instead of reading
// @@session.time_zone: two servers can both say SYSTEM and mean different
// zones. Under '+00:00' the answer is exactly one day of seconds. CAST keeps
// the column an integer whatever the server's fractional-second defaults.
const utcSessionProbe = "SELECT CAST(UNIX_TIMESTAMP('1970-01-02 00:00:00') AS SIGNED)"

const utcSessionProbeWant int64 = 86400

func assertUTCSession(ctx context.Context, db *sql.DB) error {
	var got int64
	if err := db.QueryRowContext(ctx, utcSessionProbe).Scan(&got); err != nil {
		return fmt.Errorf("session time-zone check failed: %w", err)
	}
	if got != utcSessionProbeWant {
		return fmt.Errorf("session time zone is not UTC: %s returned %d, expected %d. GoArchive pins every session to time_zone='+00:00' so TIMESTAMP values keep their instant across servers; a proxy or server setting is overriding it", utcSessionProbe, got, utcSessionProbeWant)
	}
	return nil
}

// NewManager creates a new database manager from configuration.
func NewManager(cfg *config.Config) *Manager {
	return &Manager{
		config: cfg,
	}
}

// Connect establishes connections to all configured databases.
func (m *Manager) Connect(ctx context.Context) error {
	if m == nil {
		return fmt.Errorf("database manager is nil")
	}
	if m.config == nil {
		return fmt.Errorf("database manager config is nil")
	}

	var err error

	if err := m.closeExistingConnections(); err != nil {
		return fmt.Errorf("failed to close existing connections: %w", err)
	}

	// Connect to source database
	m.Source, err = m.connectWithRetry(ctx, "source", &m.config.Source)
	if err != nil {
		return fmt.Errorf("failed to connect to source database: %w", err)
	}

	// Connect to destination database
	m.Destination, err = m.connectWithRetry(ctx, "destination", &m.config.Destination)
	if err != nil {
		_ = m.Source.Close() // Ignore error during cleanup of failed connection
		m.Source = nil
		return fmt.Errorf("failed to connect to destination database: %w", err)
	}

	// Connect to every monitored replica. Startup is fail-closed per server:
	// one unreachable member aborts Connect and releases every handle already
	// opened, including the healthy replicas ahead of it.
	if m.config.Replication.Enabled {
		for _, s := range m.config.Replication.Servers {
			addr := s.Addr()

			replica, replicaErr := m.connectWithRetry(ctx, "replica "+addr, replicaDatabaseConfig(s))
			if replicaErr != nil {
				// Ignore errors during cleanup of a failed connection set.
				for _, opened := range m.Replicas {
					_ = opened.Close()
				}
				_ = m.Destination.Close()
				_ = m.Source.Close()
				m.Replicas = nil
				m.Destination = nil
				m.Source = nil
				return fmt.Errorf("failed to connect to replica %s: %w", addr, replicaErr)
			}

			m.Replicas = append(m.Replicas, replica)
		}
	}

	return nil
}

// connectWithRetry attempts to connect with exponential backoff.
func (m *Manager) connectWithRetry(ctx context.Context, name string, cfg *config.DatabaseConfig) (*sql.DB, error) {
	var db *sql.DB
	var err error

	maxRetries := 3
	backoff := time.Second

	for i := 0; i < maxRetries; i++ {
		db, err = m.connect(cfg)
		if err == nil {
			// Verify connection
			if pingErr := db.PingContext(ctx); pingErr == nil {
				// A property of the server or proxy, not a transient: no retry.
				if tzErr := assertUTCSession(ctx, db); tzErr != nil {
					_ = db.Close()
					return nil, fmt.Errorf("%s: %w", name, tzErr)
				}
				return db, nil
			} else {
				_ = db.Close()
				err = pingErr
			}
		}

		if i < maxRetries-1 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
				backoff *= 2 // Exponential backoff
			}
		}
	}

	return nil, fmt.Errorf("failed after %d retries: %w", maxRetries, err)
}

// connect creates a database connection.
func (m *Manager) connect(cfg *config.DatabaseConfig) (*sql.DB, error) {
	dsn := BuildDSN(cfg)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	// Configure connection pool
	maxOpen := cfg.MaxConnections
	if maxOpen <= 0 {
		maxOpen = defaultMaxOpenConns
	}
	db.SetMaxOpenConns(maxOpen)

	maxIdle := cfg.MaxIdleConnections
	if maxIdle <= 0 {
		maxIdle = defaultMaxIdleConns
	}
	if maxIdle > maxOpen {
		maxIdle = maxOpen
	}
	db.SetMaxIdleConns(maxIdle)

	db.SetConnMaxLifetime(defaultConnMaxLife)
	db.SetConnMaxIdleTime(defaultConnMaxIdle)

	return db, nil
}

// BuildDSN constructs a MySQL DSN from configuration.
func BuildDSN(cfg *config.DatabaseConfig) string {
	// Start from NewConfig() so the driver's safe defaults are preserved.
	// A bare mysql.Config{} literal would zero AllowNativePasswords (default
	// true), CheckConnLiveness (default true) and MaxAllowedPacket, emitting
	// allowNativePasswords=false — which breaks any server whose user
	// authenticates with the mysql_native_password plugin (e.g. MySQL 8.4 /
	// Cloud SQL) with "this user requires mysql native password authentication".
	dsnCfg := mysql.NewConfig()
	dsnCfg.User = cfg.User
	dsnCfg.Passwd = cfg.Password
	dsnCfg.Net = "tcp"
	dsnCfg.Addr = net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	dsnCfg.DBName = cfg.Database
	dsnCfg.ParseTime = true
	// Issue #16: MySQL converts TIMESTAMP through the SESSION zone on every
	// read and write, so a copied value is the same instant on both servers
	// only if both sessions share one zone. Pin it. The driver sends this as
	// "SET time_zone = '+00:00'" on every new connection — pooled and
	// reconnected sessions included — and fails the connection if the server
	// rejects it. Offset form: needs no time-zone tables.
	dsnCfg.Params = map[string]string{"time_zone": "'+00:00'"}
	dsnCfg.MultiStatements = true

	switch cfg.TLS {
	case "disable":
		dsnCfg.TLSConfig = "false"
	case "required":
		// Full verification: chain + hostname/IP. Use a CA-signed cert with the
		// host in its SAN, or choose "skip-verify" for self-signed/Cloud SQL.
		dsnCfg.TLSConfig = "true"
	case "skip-verify":
		// Encrypted but the server certificate is NOT validated. Needed for
		// self-signed or Cloud SQL certs whose CN/SAN cannot pass Go's x509
		// checks (e.g. no IP SANs, non-RFC-compliant CN). No MITM protection.
		dsnCfg.TLSConfig = "skip-verify"
	case "preferred", "":
		dsnCfg.TLSConfig = "preferred"
	}

	return dsnCfg.FormatDSN()
}

// Close closes all database connections gracefully.
func (m *Manager) Close() error {
	var errs []error

	for i, replica := range m.Replicas {
		if replica == nil {
			continue
		}
		if err := replica.Close(); err != nil {
			errs = append(errs, fmt.Errorf("replica %s close: %w", m.replicaAddr(i), err))
		}
	}
	m.Replicas = nil

	if m.Destination != nil {
		if err := m.Destination.Close(); err != nil {
			errs = append(errs, fmt.Errorf("destination close: %w", err))
		}
		m.Destination = nil
	}

	if m.Source != nil {
		if err := m.Source.Close(); err != nil {
			errs = append(errs, fmt.Errorf("source close: %w", err))
		}
		m.Source = nil
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (m *Manager) closeExistingConnections() error {
	return m.Close()
}

// Ping verifies all connections are alive.
func (m *Manager) Ping(ctx context.Context) error {
	if m.Source != nil {
		if err := m.Source.PingContext(ctx); err != nil {
			return fmt.Errorf("source ping failed: %w", err)
		}
	}

	if m.Destination != nil {
		if err := m.Destination.PingContext(ctx); err != nil {
			return fmt.Errorf("destination ping failed: %w", err)
		}
	}

	for i, replica := range m.Replicas {
		if replica == nil {
			continue
		}
		if err := replica.PingContext(ctx); err != nil {
			return fmt.Errorf("replica %s ping failed: %w", m.replicaAddr(i), err)
		}
	}

	return nil
}

// replicaAddr names fleet member i for logs and errors. Replicas is
// index-paired with the configured servers; the index itself is the fallback
// if a caller ever populates the fleet without matching config (tests do).
func (m *Manager) replicaAddr(i int) string {
	if m.config != nil && i < len(m.config.Replication.Servers) {
		return m.config.Replication.Servers[i].Addr()
	}
	return "#" + strconv.Itoa(i)
}
