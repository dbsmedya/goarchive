package database

import (
	"context"
	"database/sql"
	"testing"

	"github.com/dbsmedya/goarchive/internal/config"
)

func TestBuildDSN(t *testing.T) {
	tests := []struct {
		name             string
		cfg              *config.DatabaseConfig
		expectedContains []string
	}{
		{
			name: "basic DSN",
			cfg: &config.DatabaseConfig{
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "secret",
				Database: "testdb",
				TLS:      "preferred",
			},
			expectedContains: []string{"root:secret@tcp(localhost:3306)/testdb?", "parseTime=true", "multiStatements=true", "tls=preferred"},
		},
		{
			name: "DSN without database",
			cfg: &config.DatabaseConfig{
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "secret",
				TLS:      "preferred",
			},
			expectedContains: []string{"root:secret@tcp(localhost:3306)/?", "parseTime=true", "multiStatements=true", "tls=preferred"},
		},
		{
			name: "DSN with TLS disabled",
			cfg: &config.DatabaseConfig{
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "secret",
				Database: "testdb",
				TLS:      "disable",
			},
			expectedContains: []string{"root:secret@tcp(localhost:3306)/testdb?", "parseTime=true", "multiStatements=true", "tls=false"},
		},
		{
			name: "DSN with TLS required",
			cfg: &config.DatabaseConfig{
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "secret",
				Database: "testdb",
				TLS:      "required",
			},
			expectedContains: []string{"root:secret@tcp(localhost:3306)/testdb?", "parseTime=true", "multiStatements=true", "tls=true"},
		},
		{
			name: "DSN with custom port",
			cfg: &config.DatabaseConfig{
				Host:     "remote-host",
				Port:     3307,
				User:     "admin",
				Password: "p@ssw0rd!",
				Database: "mydb",
				TLS:      "preferred",
			},
			expectedContains: []string{"admin:p@ssw0rd!@tcp(remote-host:3307)/mydb?", "parseTime=true", "multiStatements=true", "tls=preferred"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildDSN(tt.cfg)
			for _, expected := range tt.expectedContains {
				if !contains(result, expected) {
					t.Errorf("BuildDSN() = %q, expected to contain %q", result, expected)
				}
			}
		})
	}
}

func TestNewManager(t *testing.T) {
	cfg := &config.Config{
		Source: config.DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Password: "secret",
			Database: "sourcedb",
		},
		Destination: config.DatabaseConfig{
			Host:     "archive-host",
			Port:     3306,
			User:     "root",
			Password: "secret",
			Database: "archivedb",
		},
		Replication: config.ReplicationConfig{
			Enabled: false,
		},
	}

	manager := NewManager(cfg)
	if manager == nil {
		t.Fatal("NewManager() returned nil")
	}

	if manager.config != cfg {
		t.Error("manager.config should point to provided config")
	}

	if manager.Source != nil {
		t.Error("Source should be nil before Connect()")
	}

	if manager.Destination != nil {
		t.Error("Destination should be nil before Connect()")
	}

	if len(manager.Replicas) != 0 {
		t.Errorf("Replicas should be empty when replication is not enabled, got %d", len(manager.Replicas))
	}
}

func TestManagerCloseWithoutConnect(t *testing.T) {
	cfg := &config.Config{
		Source:      config.DatabaseConfig{Host: "localhost"},
		Destination: config.DatabaseConfig{Host: "archive"},
		Replication: config.ReplicationConfig{Enabled: false},
	}

	manager := NewManager(cfg)

	// Should not panic when closing unconnected manager
	err := manager.Close()
	if err != nil {
		t.Errorf("Close() returned error for unconnected manager: %v", err)
	}
}

// Additional tests for Phase 2

func TestBuildDSN_EdgeCases(t *testing.T) {
	tests := []struct {
		name             string
		cfg              *config.DatabaseConfig
		expectedContains []string
	}{
		{
			name: "Empty password",
			cfg: &config.DatabaseConfig{
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "",
				Database: "testdb",
				TLS:      "preferred",
			},
			expectedContains: []string{"root@tcp(localhost:3306)/testdb?", "parseTime=true", "multiStatements=true", "tls=preferred"},
		},
		{
			name: "Special characters in password",
			cfg: &config.DatabaseConfig{
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "p@ss!w0rd#123",
				Database: "testdb",
				TLS:      "disable",
			},
			expectedContains: []string{"root:p@ss!w0rd#123@tcp(localhost:3306)/testdb?", "parseTime=true", "multiStatements=true", "tls=false"},
		},
		{
			name: "IPv6 host",
			cfg: &config.DatabaseConfig{
				Host:     "::1",
				Port:     3306,
				User:     "root",
				Password: "secret",
				Database: "testdb",
				TLS:      "preferred",
			},
			expectedContains: []string{"root:secret@tcp([::1]:3306)/testdb?", "parseTime=true", "multiStatements=true", "tls=preferred"},
		},
		{
			name: "Non-standard port",
			cfg: &config.DatabaseConfig{
				Host:     "localhost",
				Port:     33060,
				User:     "admin",
				Password: "admin123",
				Database: "testdb",
				TLS:      "required",
			},
			expectedContains: []string{"admin:admin123@tcp(localhost:33060)/testdb?", "parseTime=true", "multiStatements=true", "tls=true"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildDSN(tt.cfg)
			for _, expected := range tt.expectedContains {
				if !contains(result, expected) {
					t.Errorf("BuildDSN() = %q, expected to contain %q", result, expected)
				}
			}
		})
	}
}

func TestNewManager_NilConfig(t *testing.T) {
	manager := NewManager(nil)
	if manager == nil {
		t.Fatal("NewManager() should not return nil even with nil config")
	}
	if manager.config != nil {
		t.Error("manager.config should be nil when provided nil config")
	}
	if err := manager.Connect(context.Background()); err == nil {
		t.Fatal("Connect() should fail with nil manager config")
	}
}

func TestNewManager_WithReplicaEnabled(t *testing.T) {
	cfg := &config.Config{
		Source: config.DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Password: "secret",
			Database: "sourcedb",
		},
		Destination: config.DatabaseConfig{
			Host:     "archive-host",
			Port:     3306,
			User:     "root",
			Password: "secret",
			Database: "archivedb",
		},
		Replication: config.ReplicationConfig{
			Enabled: true,
			Servers: []config.ReplicationServerConfig{
				{Host: "replica-host", Port: 3306, User: "replica", Password: "secret", TLS: "preferred", Type: "async"},
			},
		},
	}

	manager := NewManager(cfg)
	if manager == nil {
		t.Fatal("NewManager() returned nil")
	}

	if manager.config.Replication.Enabled != true {
		t.Error("Replication should be enabled in manager config")
	}

	if len(manager.Replicas) != 0 {
		t.Errorf("Replicas should be empty before Connect(), got %d", len(manager.Replicas))
	}
}

func TestBuildDSN_TLSVariants(t *testing.T) {
	tests := []struct {
		name        string
		tlsValue    string
		expectedTLS string
	}{
		{name: "TLS preferred", tlsValue: "preferred", expectedTLS: "tls=preferred"},
		{name: "TLS disable", tlsValue: "disable", expectedTLS: "tls=false"},
		{name: "TLS required", tlsValue: "required", expectedTLS: "tls=true"},
		{name: "TLS skip-verify", tlsValue: "skip-verify", expectedTLS: "tls=skip-verify"},
		{name: "TLS empty defaults to preferred", tlsValue: "", expectedTLS: "tls=preferred"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.DatabaseConfig{
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "secret",
				Database: "testdb",
				TLS:      tt.tlsValue,
			}
			result := BuildDSN(cfg)
			if !contains(result, tt.expectedTLS) {
				t.Errorf("BuildDSN() = %q, should contain %q", result, tt.expectedTLS)
			}
		})
	}
}

// TestBuildDSN_AllowsNativePasswords guards against the struct-literal
// regression that emitted allowNativePasswords=false, breaking servers whose
// users authenticate via the mysql_native_password plugin (MySQL 8.4 / Cloud SQL).
func TestBuildDSN_AllowsNativePasswords(t *testing.T) {
	cfg := &config.DatabaseConfig{
		Host:     "localhost",
		Port:     3306,
		User:     "sinan",
		Password: "secret",
		Database: "testdb",
		TLS:      "skip-verify",
	}
	result := BuildDSN(cfg)
	if contains(result, "allowNativePasswords=false") {
		t.Errorf("BuildDSN() = %q, must not disable native passwords", result)
	}
}

func TestBuildDSN_RequiredParams(t *testing.T) {
	cfg := &config.DatabaseConfig{
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "secret",
		Database: "testdb",
		TLS:      "preferred",
	}

	dsn := BuildDSN(cfg)

	// Verify required parameters are present
	required := []string{
		"parseTime=true",
		"multiStatements=true",
	}

	for _, param := range required {
		if !contains(dsn, param) {
			t.Errorf("BuildDSN() should contain %q", param)
		}
	}
}

func TestManager_FieldsInitialization(t *testing.T) {
	cfg := &config.Config{
		Source:      config.DatabaseConfig{Host: "localhost"},
		Destination: config.DatabaseConfig{Host: "archive"},
		Replication: config.ReplicationConfig{Enabled: false},
	}

	manager := NewManager(cfg)

	// Verify all connection fields are nil initially
	if manager.Source != nil {
		t.Error("Source should be nil before Connect()")
	}
	if manager.Destination != nil {
		t.Error("Destination should be nil before Connect()")
	}
	if manager.Replicas != nil {
		t.Error("Replicas should be nil before Connect()")
	}

	// Verify config is set
	if manager.config == nil {
		t.Error("config should not be nil")
	}
	if manager.config.Source.Host != "localhost" {
		t.Error("Source host should match config")
	}
}

// TestReplicaDatabaseConfig_MapsAllFields pins the fleet-entry → DSN-input
// mapping. TLS is the field the old inline replicaCfg silently dropped, so it
// is asserted through BuildDSN rather than by struct comparison alone.
func TestReplicaDatabaseConfig_MapsAllFields(t *testing.T) {
	server := config.ReplicationServerConfig{
		Host:     "replica-host",
		Port:     3308,
		User:     "monitor",
		Password: "s3cret",
		TLS:      "required",
		Type:     "async",
		Channels: []string{"billing"},
	}

	got := replicaDatabaseConfig(server)

	if got.Host != "replica-host" {
		t.Errorf("Host = %q, want %q", got.Host, "replica-host")
	}
	if got.Port != 3308 {
		t.Errorf("Port = %d, want %d", got.Port, 3308)
	}
	if got.User != "monitor" {
		t.Errorf("User = %q, want %q", got.User, "monitor")
	}
	if got.Password != "s3cret" {
		t.Errorf("Password = %q, want %q", got.Password, "s3cret")
	}
	if got.TLS != "required" {
		t.Errorf("TLS = %q, want %q", got.TLS, "required")
	}

	dsn := BuildDSN(got)
	if !contains(dsn, "tls=true") {
		t.Errorf("BuildDSN(replicaDatabaseConfig(...)) = %q, should carry TLS required as %q", dsn, "tls=true")
	}
	if !contains(dsn, "replica-host:3308") {
		t.Errorf("BuildDSN(replicaDatabaseConfig(...)) = %q, should address %q", dsn, "replica-host:3308")
	}
}

// TestManagerConnect_ReplicationDisabled_LeavesFleetEmpty proves the disabled
// path never populates the fleet AND that Connect's opening
// closeExistingConnections() resets a fleet left over from a previous Connect.
// A stale handle is seeded first so the assertion cannot pass vacuously.
func TestManagerConnect_ReplicationDisabled_LeavesFleetEmpty(t *testing.T) {
	cfg := &config.Config{
		// Port 1 is unbound: Connect fails at the source, well before any
		// replica would be dialed.
		Source:      config.DatabaseConfig{Host: "127.0.0.1", Port: 1, User: "u", Password: "p", Database: "d"},
		Destination: config.DatabaseConfig{Host: "127.0.0.1", Port: 1, User: "u", Password: "p", Database: "d"},
		Replication: config.ReplicationConfig{Enabled: false},
	}

	manager := NewManager(cfg)
	manager.Replicas = []*sql.DB{openLazyHandle(t), openLazyHandle(t)}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // keep connectWithRetry's backoff from sleeping through 3 retries

	if err := manager.Connect(ctx); err == nil {
		t.Fatal("Connect() to an unbound port returned nil error")
	}

	if manager.Replicas != nil {
		t.Errorf("Replicas = %d entries, want nil when replication is disabled", len(manager.Replicas))
	}
}

// TestManagerClose_PopulatedFleet proves Close() closes every fleet member,
// nils the slice, and is idempotent.
func TestManagerClose_PopulatedFleet(t *testing.T) {
	cfg := &config.Config{
		Source:      config.DatabaseConfig{Host: "localhost"},
		Destination: config.DatabaseConfig{Host: "archive"},
		Replication: config.ReplicationConfig{Enabled: true},
	}

	manager := NewManager(cfg)
	first, second := openLazyHandle(t), openLazyHandle(t)
	manager.Replicas = []*sql.DB{first, second}

	if err := manager.Close(); err != nil {
		t.Fatalf("Close() with a populated fleet returned error: %v", err)
	}
	if manager.Replicas != nil {
		t.Errorf("Replicas = %d entries after Close(), want nil", len(manager.Replicas))
	}

	// Every member is genuinely closed, not merely dropped from the slice.
	for i, db := range []*sql.DB{first, second} {
		err := db.PingContext(context.Background())
		if err == nil || !contains(err.Error(), "database is closed") {
			t.Errorf("Replicas[%d] after Close(): Ping error = %v, want a closed-database error", i, err)
		}
	}

	// Idempotent: the second Close() on the now-nil fleet returns nil.
	if err := manager.Close(); err != nil {
		t.Errorf("second Close() returned error: %v", err)
	}
}

// TestManagerReplicaAddr covers both branches of the fleet-member naming
// helper: the config-mapped address, and the fallback used when the index has
// no matching server entry (or the manager carries no config at all).
func TestManagerReplicaAddr(t *testing.T) {
	cfg := &config.Config{
		Replication: config.ReplicationConfig{
			Enabled: true,
			Servers: []config.ReplicationServerConfig{
				{Host: "replica-a", Port: 3306, User: "monitor", TLS: "disable", Type: "async"},
				{Host: "2001:db8::1", Port: 3308, User: "monitor", TLS: "disable", Type: "async"},
			},
		},
	}

	t.Run("config-mapped address", func(t *testing.T) {
		m := NewManager(cfg)
		if got, want := m.replicaAddr(0), "replica-a:3306"; got != want {
			t.Errorf("replicaAddr(0) = %q, want %q", got, want)
		}
		// IPv6 arrives bracketed via net.JoinHostPort.
		if got, want := m.replicaAddr(1), "[2001:db8::1]:3308"; got != want {
			t.Errorf("replicaAddr(1) = %q, want %q", got, want)
		}
	})

	t.Run("index beyond the configured servers", func(t *testing.T) {
		m := NewManager(cfg)
		if got, want := m.replicaAddr(2), "#2"; got != want {
			t.Errorf("replicaAddr(2) = %q, want %q", got, want)
		}
	})

	t.Run("manager without config", func(t *testing.T) {
		m := &Manager{}
		if got, want := m.replicaAddr(0), "#0"; got != want {
			t.Errorf("replicaAddr(0) with no config = %q, want %q", got, want)
		}
	})
}

// openLazyHandle returns a real *sql.DB that has never dialed anything —
// sql.Open does not connect — so fleet lifecycle can be tested without a server.
func openLazyHandle(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", BuildDSN(&config.DatabaseConfig{
		Host: "127.0.0.1", Port: 1, User: "u", Password: "p", Database: "d",
	}))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	return db
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || containsSubstring(s, substr)))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
