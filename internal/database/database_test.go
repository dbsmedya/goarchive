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
