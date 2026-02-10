// Package database provides MySQL database connection management for GoArchive.
package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql" // MySQL driver

	"github.com/dbsmedya/goarchive/internal/config"
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
	var err error

	// Connect to source database
	m.Source, err = m.connectWithRetry(ctx, "source", &m.config.Source)
	if err != nil {
		return fmt.Errorf("failed to connect to source database: %w", err)
	}

	// Connect to destination database
	m.Destination, err = m.connectWithRetry(ctx, "destination", &m.config.Destination)
	if err != nil {
		m.Source.Close()
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
			m.Source.Close()
			m.Destination.Close()
			return fmt.Errorf("failed to connect to replica database: %w", err)
		}
	}

	return nil
}

// ConnectSource establishes connection to source database only.
// Use this when only source access is needed (e.g., purge operations).
func (m *Manager) ConnectSource(ctx context.Context) error {
	var err error

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
				db.Close()
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
	if cfg.MaxConnections > 0 {
		db.SetMaxOpenConns(cfg.MaxConnections)
	}
	if cfg.MaxIdleConnections > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConnections)
	}
	db.SetConnMaxLifetime(10 * time.Minute)

	return db, nil
}

// BuildDSN constructs a MySQL DSN from configuration.
func BuildDSN(cfg *config.DatabaseConfig) string {
	// Format: user:password@tcp(host:port)/database?params
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
	)

	if cfg.Database != "" {
		dsn += cfg.Database
	}

	// Add TLS configuration
	params := "?parseTime=true&multiStatements=true"
	switch cfg.TLS {
	case "disable":
		params += "&tls=false"
	case "required":
		params += "&tls=true"
	case "preferred", "":
		params += "&tls=preferred"
	}

	return dsn + params
}

// Close closes all database connections gracefully.
func (m *Manager) Close() error {
	var errs []error

	if m.Replica != nil {
		if err := m.Replica.Close(); err != nil {
			errs = append(errs, fmt.Errorf("replica close: %w", err))
		}
	}

	if m.Destination != nil {
		if err := m.Destination.Close(); err != nil {
			errs = append(errs, fmt.Errorf("destination close: %w", err))
		}
	}

	if m.Source != nil {
		if err := m.Source.Close(); err != nil {
			errs = append(errs, fmt.Errorf("source close: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing connections: %v", errs)
	}
	return nil
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
