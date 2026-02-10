package database

import (
	"testing"

	"github.com/dbsmedya/goarchive/internal/config"
)

func TestBuildDSN(t *testing.T) {
	tests := []struct {
		name     string
		cfg      *config.DatabaseConfig
		expected string
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
			expected: "root:secret@tcp(localhost:3306)/testdb?parseTime=true&multiStatements=true&tls=preferred",
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
			expected: "root:secret@tcp(localhost:3306)/?parseTime=true&multiStatements=true&tls=preferred",
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
			expected: "root:secret@tcp(localhost:3306)/testdb?parseTime=true&multiStatements=true&tls=false",
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
			expected: "root:secret@tcp(localhost:3306)/testdb?parseTime=true&multiStatements=true&tls=true",
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
			expected: "admin:p@ssw0rd!@tcp(remote-host:3307)/mydb?parseTime=true&multiStatements=true&tls=preferred",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildDSN(tt.cfg)
			if result != tt.expected {
				t.Errorf("BuildDSN() = %q, expected %q", result, tt.expected)
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
		Replica: config.ReplicaConfig{
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

	if manager.Replica != nil {
		t.Error("Replica should be nil when not enabled")
	}
}

func TestManagerCloseWithoutConnect(t *testing.T) {
	cfg := &config.Config{
		Source:      config.DatabaseConfig{Host: "localhost"},
		Destination: config.DatabaseConfig{Host: "archive"},
		Replica:     config.ReplicaConfig{Enabled: false},
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
		name     string
		cfg      *config.DatabaseConfig
		expected string
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
			expected: "root:@tcp(localhost:3306)/testdb?parseTime=true&multiStatements=true&tls=preferred",
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
			expected: "root:p@ss!w0rd#123@tcp(localhost:3306)/testdb?parseTime=true&multiStatements=true&tls=false",
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
			expected: "root:secret@tcp(::1:3306)/testdb?parseTime=true&multiStatements=true&tls=preferred",
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
			expected: "admin:admin123@tcp(localhost:33060)/testdb?parseTime=true&multiStatements=true&tls=true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildDSN(tt.cfg)
			if result != tt.expected {
				t.Errorf("BuildDSN() = %q, expected %q", result, tt.expected)
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
		Replica: config.ReplicaConfig{
			Enabled:  true,
			Host:     "replica-host",
			Port:     3306,
			User:     "replica",
			Password: "secret",
		},
	}

	manager := NewManager(cfg)
	if manager == nil {
		t.Fatal("NewManager() returned nil")
	}

	if manager.config.Replica.Enabled != true {
		t.Error("Replica should be enabled in manager config")
	}

	if manager.Replica != nil {
		t.Error("Replica should be nil before Connect()")
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
		Replica:     config.ReplicaConfig{Enabled: false},
	}

	manager := NewManager(cfg)

	// Verify all connection fields are nil initially
	if manager.Source != nil {
		t.Error("Source should be nil before Connect()")
	}
	if manager.Destination != nil {
		t.Error("Destination should be nil before Connect()")
	}
	if manager.Replica != nil {
		t.Error("Replica should be nil before Connect()")
	}

	// Verify config is set
	if manager.config == nil {
		t.Error("config should not be nil")
	}
	if manager.config.Source.Host != "localhost" {
		t.Error("Source host should match config")
	}
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
