package config

import (
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	// Test source defaults
	if cfg.Source.Port != 3306 {
		t.Errorf("expected source port 3306, got %d", cfg.Source.Port)
	}
	if cfg.Source.TLS != "preferred" {
		t.Errorf("expected source TLS 'preferred', got %s", cfg.Source.TLS)
	}
	if cfg.Source.MaxConnections != 10 {
		t.Errorf("expected source max_connections 10, got %d", cfg.Source.MaxConnections)
	}

	// Test destination defaults
	if cfg.Destination.Port != 3306 {
		t.Errorf("expected destination port 3306, got %d", cfg.Destination.Port)
	}

	// Test processing defaults
	if cfg.Processing.BatchSize != 1000 {
		t.Errorf("expected batch_size 1000, got %d", cfg.Processing.BatchSize)
	}
	if cfg.Processing.BatchDeleteSize != 500 {
		t.Errorf("expected batch_delete_size 500, got %d", cfg.Processing.BatchDeleteSize)
	}

	// Test safety defaults
	if cfg.Safety.DisableForeignKeyChecks {
		t.Errorf("expected disable_foreign_key_checks false by default")
	}

	// Test logging defaults
	if cfg.Logging.Level != "info" {
		t.Errorf("expected logging level 'info', got %s", cfg.Logging.Level)
	}
	if cfg.Logging.Format != "json" {
		t.Errorf("expected logging format 'json', got %s", cfg.Logging.Format)
	}
}

func TestDefaultConfigReplication(t *testing.T) {
	cfg := DefaultConfig()

	// new block defaults
	if cfg.Replication.Enabled {
		t.Fatal("replication must default disabled")
	}
	if cfg.Replication.SecondsBehindSourceWithin != 10 {
		t.Fatalf("tolerance default = %d, want 10", cfg.Replication.SecondsBehindSourceWithin)
	}
	if cfg.Replication.CheckInterval != 5 {
		t.Fatalf("check_interval default = %d, want 5", cfg.Replication.CheckInterval)
	}
	if cfg.Replication.CacheTTL != 15 {
		t.Fatalf("cache_ttl default = %d, want 15", cfg.Replication.CacheTTL)
	}
}

func TestReplicationServerAddrIPv6(t *testing.T) {
	s := ReplicationServerConfig{Host: "::1", Port: 3306}
	if got := s.Addr(); got != "[::1]:3306" {
		t.Fatalf("Addr() = %q", got)
	}
}

func TestDatabaseConfig_EffectiveJobSchema(t *testing.T) {
	t.Run("defaults to Database when JobSchema empty", func(t *testing.T) {
		c := DatabaseConfig{Database: "archive_db"}
		if got := c.EffectiveJobSchema(); got != "archive_db" {
			t.Fatalf("got %q, want archive_db", got)
		}
	})
	t.Run("uses JobSchema when set", func(t *testing.T) {
		c := DatabaseConfig{Database: "archive_db", JobSchema: "goarchive"}
		if got := c.EffectiveJobSchema(); got != "goarchive" {
			t.Fatalf("got %q, want goarchive", got)
		}
	})
}
