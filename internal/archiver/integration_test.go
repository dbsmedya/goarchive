package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/yaml.v3"
)

// IntegrationConfig holds database configuration for integration tests
type IntegrationConfig struct {
	Databases   []DatabaseConfig `yaml:"databases"`
	Force       bool             `yaml:"force"`        // Drop and recreate databases
	FixturePath string           `yaml:"fixture_path"` // Path to SQL fixture file
}

type DatabaseConfig struct {
	Name     string `yaml:"name"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
}

// LoadIntegrationConfig loads integration test configuration
// Priority: 1) INTEGRATION_CONFIG env var, 2) ./integration_test.yaml, 3) Default config
func LoadIntegrationConfig() (*IntegrationConfig, error) {
	// Check for env var first
	if configPath := os.Getenv("INTEGRATION_CONFIG"); configPath != "" {
		return loadConfigFromFile(configPath)
	}

	// Try default location
	defaultPath := "integration_test.yaml"
	if _, err := os.Stat(defaultPath); err == nil {
		cfg, err := loadConfigFromFile(defaultPath)
		if err != nil {
			return nil, err
		}
		// Check if config is valid (has actual credentials, not template placeholders)
		if err := cfg.Validate(); err != nil {
			return nil, fmt.Errorf("invalid integration_test.yaml: %w\n\nTo fix this, either:\n"+
				"1. Run: make integration-config\n"+
				"2. Edit internal/archiver/integration_test.yaml with your credentials\n"+
				"3. Set MYSQL_ROOT_PASSWORD environment variable\n"+
				"4. Create a custom config file and set INTEGRATION_CONFIG=/path/to/config.yaml", err)
		}
		return cfg, nil
	}

	// Return default configuration (uses MYSQL_ROOT_PASSWORD env var or default)
	return defaultIntegrationConfig(), nil
}

func loadConfigFromFile(path string) (*IntegrationConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Expand environment variables in the config
	expandedData := os.ExpandEnv(string(data))

	var cfg IntegrationConfig
	if err := yaml.Unmarshal([]byte(expandedData), &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &cfg, nil
}

// Validate checks if the configuration has valid credentials
func (cfg *IntegrationConfig) Validate() error {
	if len(cfg.Databases) == 0 {
		return fmt.Errorf("no databases configured")
	}

	for _, db := range cfg.Databases {
		// Check for placeholder values or empty credentials
		if strings.Contains(db.Password, "${") || strings.Contains(db.Password, "YOUR_") {
			return fmt.Errorf("database %q has unconfigured password placeholder: %s", db.Name, db.Password)
		}
		if db.Password == "" {
			return fmt.Errorf("database %q has empty password", db.Name)
		}
		if db.Host == "" {
			return fmt.Errorf("database %q has empty host", db.Name)
		}
		if db.Port == 0 {
			return fmt.Errorf("database %q has empty port", db.Name)
		}
		if db.User == "" {
			return fmt.Errorf("database %q has empty user", db.Name)
		}
	}

	return nil
}

// defaultIntegrationConfig returns sensible defaults for local development
func defaultIntegrationConfig() *IntegrationConfig {
	password := os.Getenv("MYSQL_ROOT_PASSWORD")
	if password == "" {
		password = "" // Empty - will fail validation if no config file exists
	}

	return &IntegrationConfig{
		Databases: []DatabaseConfig{
			{
				Name:     "source",
				Host:     "127.0.0.1",
				Port:     3305,
				User:     "root",
				Password: password,
				Database: "goarchive_test",
			},
			{
				Name:     "destination",
				Host:     "127.0.0.1",
				Port:     3307,
				User:     "root",
				Password: password,
				Database: "goarchive_test",
			},
		},
		Force:       false,
		FixturePath: "testdata/customer_orders.sql",
	}
}

// IntegrationTestSetup manages the lifecycle of integration test databases
type IntegrationTestSetup struct {
	Config *IntegrationConfig
	DBs    map[string]*sql.DB
}

// NewIntegrationTestSetup creates a new test setup manager
func NewIntegrationTestSetup(cfg *IntegrationConfig) *IntegrationTestSetup {
	return &IntegrationTestSetup{
		Config: cfg,
		DBs:    make(map[string]*sql.DB),
	}
}

// ValidateConnections checks all database connections
func (its *IntegrationTestSetup) ValidateConnections(ctx context.Context) error {
	for _, dbCfg := range its.Config.Databases {
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=5s",
			dbCfg.User, dbCfg.Password, dbCfg.Host, dbCfg.Port)

		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return fmt.Errorf("failed to open connection to %s: %w", dbCfg.Name, err)
		}
		defer func() { _ = db.Close() }()

		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		if err := db.PingContext(ctx); err != nil {
			return fmt.Errorf("failed to ping %s at %s:%d: %w", dbCfg.Name, dbCfg.Host, dbCfg.Port, err)
		}
	}
	return nil
}

// SetupDatabases creates databases and applies fixtures
func (its *IntegrationTestSetup) SetupDatabases(ctx context.Context) error {
	for _, dbCfg := range its.Config.Databases {
		if err := its.setupDatabase(ctx, dbCfg); err != nil {
			return fmt.Errorf("failed to setup %s: %w", dbCfg.Name, err)
		}
	}
	return nil
}

func (its *IntegrationTestSetup) setupDatabase(ctx context.Context, dbCfg DatabaseConfig) error {
	// Connect without database
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=30s&multiStatements=true",
		dbCfg.User, dbCfg.Password, dbCfg.Host, dbCfg.Port)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	// Create database
	if its.Config.Force {
		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbCfg.Database))
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbCfg.Database))
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	// Close the first connection and remove from defer
	_ = db.Close()

	// Reconnect with database
	dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=30s&multiStatements=true",
		dbCfg.User, dbCfg.Password, dbCfg.Host, dbCfg.Port, dbCfg.Database)

	db2, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	// Note: We don't defer db2.Close() here because we store the connection for later use
	// The caller is responsible for calling its.Close() to clean up connections

	// Store connection for later use
	its.DBs[dbCfg.Name] = db2

	// Apply fixtures
	if its.Config.Force || !its.hasTables(ctx, db2) {
		if err := its.applyFixtures(ctx, db2); err != nil {
			return fmt.Errorf("failed to apply fixtures: %w", err)
		}
	}

	return nil
}

func (its *IntegrationTestSetup) hasTables(ctx context.Context, db *sql.DB) bool {
	var count int
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.tables 
		WHERE table_schema = DATABASE()
	`).Scan(&count)
	return err == nil && count > 0
}

func (its *IntegrationTestSetup) applyFixtures(ctx context.Context, db *sql.DB) error {
	// Determine fixture path
	fixturePath := its.Config.FixturePath
	if fixturePath == "" {
		fixturePath = "testdata/customer_orders.sql"
	}

	// If relative path, resolve from test file location
	if !filepath.IsAbs(fixturePath) {
		// Try to find relative to this file's directory
		_, filename, _, _ := getTestFileInfo()
		if filename != "" {
			dir := filepath.Dir(filename)
			fixturePath = filepath.Join(dir, fixturePath)
		}
	}

	data, err := os.ReadFile(fixturePath)
	if err != nil {
		return fmt.Errorf("failed to read fixture file %s: %w", fixturePath, err)
	}

	_, err = db.ExecContext(ctx, string(data))
	if err != nil {
		return fmt.Errorf("failed to execute fixtures: %w", err)
	}

	return nil
}

// GetDB returns a database connection by name
func (its *IntegrationTestSetup) GetDB(name string) (*sql.DB, bool) {
	db, ok := its.DBs[name]
	return db, ok
}

// Close closes all database connections
func (its *IntegrationTestSetup) Close() {
	for _, db := range its.DBs {
		_ = db.Close()
	}
}

// SeedData truncates tables and inserts test data
func (its *IntegrationTestSetup) SeedData(ctx context.Context, dbName string) error {
	db, ok := its.GetDB(dbName)
	if !ok {
		return fmt.Errorf("database %s not found", dbName)
	}

	// Disable FK checks, truncate, re-enable
	queries := []string{
		"SET FOREIGN_KEY_CHECKS = 0",
		"TRUNCATE TABLE order_payments",
		"TRUNCATE TABLE order_items",
		"TRUNCATE TABLE orders",
		"TRUNCATE TABLE customers",
		"SET FOREIGN_KEY_CHECKS = 1",
	}

	for _, q := range queries {
		if _, err := db.ExecContext(ctx, q); err != nil {
			// Log but don't fail on truncate errors
			continue
		}
	}

	// Insert test customers
	customerQueries := []string{
		`INSERT INTO customers (customer_id, first_name, last_name, email, created_at) VALUES
			(1, 'John', 'Doe', 'john@example.com', DATE_SUB(NOW(), INTERVAL 2 YEAR)),
			(2, 'Jane', 'Smith', 'jane@example.com', DATE_SUB(NOW(), INTERVAL 1 YEAR)),
			(3, 'Bob', 'Wilson', 'bob@example.com', NOW())`,
		`INSERT INTO orders (order_id, customer_id, order_date, total_amount, status) VALUES
			(1, 1, DATE_SUB(NOW(), INTERVAL 2 YEAR), 100.00, 'completed'),
			(2, 1, DATE_SUB(NOW(), INTERVAL 700 DAY), 200.00, 'completed'),
			(3, 2, DATE_SUB(NOW(), INTERVAL 1 YEAR), 150.00, 'completed'),
			(4, 3, NOW(), 300.00, 'pending')`,
		`INSERT INTO order_items (item_id, order_id, product_name, quantity, price) VALUES
			(1, 1, 'Product A', 2, 50.00),
			(2, 2, 'Product B', 1, 200.00),
			(3, 3, 'Product C', 3, 50.00),
			(4, 4, 'Product D', 1, 300.00)`,
		`INSERT INTO order_payments (payment_id, order_id, amount, payment_date, payment_method) VALUES
			(1, 1, 100.00, DATE_SUB(NOW(), INTERVAL 2 YEAR), 'credit_card'),
			(2, 2, 200.00, DATE_SUB(NOW(), INTERVAL 700 DAY), 'credit_card'),
			(3, 3, 150.00, DATE_SUB(NOW(), INTERVAL 1 YEAR), 'paypal')`,
	}

	for _, q := range customerQueries {
		if _, err := db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("failed to seed data: %w", err)
		}
	}

	return nil
}

// Helper function to get current test file info
func getTestFileInfo() (string, string, int, bool) {
	// This is a placeholder - in real tests we can use runtime.Caller
	return "", "", 0, false
}

// SkipIfNoIntegrationEnv skips the test if integration test requirements aren't met
func SkipIfNoIntegrationEnv(t *testing.T) {
	// First check if config file exists and has valid credentials
	_, err := LoadIntegrationConfig()
	if err != nil {
		t.Skipf("Skipping integration test: %v", err)
	}

	// Check if running in CI or explicit integration test mode
	if os.Getenv("INTEGRATION_TESTS") != "true" && os.Getenv("CI") != "true" {
		// Try to connect to default database
		cfg := defaultIntegrationConfig()

		// If password is empty and no config file exists, skip
		if cfg.Databases[0].Password == "" {
			t.Skip("Skipping integration test: no database password configured. " +
				"Set MYSQL_ROOT_PASSWORD environment variable or create integration_test.yaml")
		}

		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=2s",
			cfg.Databases[0].User, cfg.Databases[0].Password,
			cfg.Databases[0].Host, cfg.Databases[0].Port)

		db, err := sql.Open("mysql", dsn)
		if err != nil {
			t.Skip("Skipping integration test: no database connection available")
		}
		defer func() { _ = db.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		if err := db.PingContext(ctx); err != nil {
			t.Skip("Skipping integration test: cannot connect to database")
		}
	}
}

// SetupIntegrationTest is a convenience function for test setup
func SetupIntegrationTest(t *testing.T) (*IntegrationTestSetup, context.Context) {
	SkipIfNoIntegrationEnv(t)

	ctx := context.Background()

	cfg, err := LoadIntegrationConfig()
	if err != nil {
		t.Fatalf("Failed to load integration config: %v\n\n"+
			"To run integration tests:\n"+
			"1. Run: make integration-config\n"+
			"2. Edit internal/archiver/integration_test.yaml with your credentials\n"+
			"3. Or set: MYSQL_ROOT_PASSWORD=your_password go test ...", err)
	}

	// Check for --force flag from env
	if os.Getenv("INTEGRATION_FORCE") == "true" {
		cfg.Force = true
	}

	setup := NewIntegrationTestSetup(cfg)

	// Validate connections
	if err := setup.ValidateConnections(ctx); err != nil {
		t.Fatalf("Database connection validation failed: %v\n\n"+
			"Make sure your databases are running. You can start them with:\n"+
			"  make test-up\n\n"+
			"Or check your integration_test.yaml configuration.", err)
	}

	// Setup databases
	if err := setup.SetupDatabases(ctx); err != nil {
		t.Fatalf("Database setup failed: %v", err)
	}

	return setup, ctx
}
