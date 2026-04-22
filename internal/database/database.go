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

// Manager handles database connections for source, destination, and replica.
type Manager struct {
	Source      *sql.DB
	Destination *sql.DB
	Replica     *sql.DB
	config      *config.Config
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

	// Connect to replica if enabled
	if m.config.Replica.Enabled {
		replicaCfg := &config.DatabaseConfig{
			Host:     m.config.Replica.Host,
			Port:     m.config.Replica.Port,
			User:     m.config.Replica.User,
			Password: m.config.Replica.Password,
		}
		m.Replica, err = m.connectWithRetry(ctx, "replica", replicaCfg)
		if err != nil {
			_ = m.Source.Close()      // Ignore error during cleanup of failed connection
			_ = m.Destination.Close() // Ignore error during cleanup of failed connection
			m.Source = nil
			m.Destination = nil
			return fmt.Errorf("failed to connect to replica database: %w", err)
		}
	}

	return nil
}

// ConnectSource establishes connection to source database only.
//
// Deprecated: All orchestrators (archive, purge, copy-only) now require a
// destination connection because resume metadata and advisory locks live on
// Destination. Use Connect instead. Retained for callers that still need a
// source-only connection path.
func (m *Manager) ConnectSource(ctx context.Context) error {
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

	// Connect to source database only
	m.Source, err = m.connectWithRetry(ctx, "source", &m.config.Source)
	if err != nil {
		return fmt.Errorf("failed to connect to source database: %w", err)
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
	dsnCfg := mysql.Config{
		User:            cfg.User,
		Passwd:          cfg.Password,
		Net:             "tcp",
		Addr:            net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		DBName:          cfg.Database,
		ParseTime:       true,
		MultiStatements: true,
	}

	switch cfg.TLS {
	case "disable":
		dsnCfg.TLSConfig = "false"
	case "required":
		dsnCfg.TLSConfig = "true"
	case "preferred", "":
		dsnCfg.TLSConfig = "preferred"
	}

	return dsnCfg.FormatDSN()
}

// GetConfig returns the configuration used by this manager.
func (m *Manager) GetConfig() *config.Config {
	return m.config
}

// Close closes all database connections gracefully.
func (m *Manager) Close() error {
	var errs []error

	if m.Replica != nil {
		if err := m.Replica.Close(); err != nil {
			errs = append(errs, fmt.Errorf("replica close: %w", err))
		}
		m.Replica = nil
	}

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

	if m.Replica != nil {
		if err := m.Replica.PingContext(ctx); err != nil {
			return fmt.Errorf("replica ping failed: %w", err)
		}
	}

	return nil
}
