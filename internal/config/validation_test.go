package config

import (
	"strings"
	"testing"
)

func TestValidConfig(t *testing.T) {
	cfg := &Config{
		Source: DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Password: "pass",
			Database: "testdb",
		},
		Destination: DatabaseConfig{
			Host:     "localhost",
			Port:     3307,
			User:     "root",
			Password: "pass",
			Database: "archivedb",
		},
		Jobs: map[string]JobConfig{
			"test_job": {
				RootTable:  "orders",
				PrimaryKey: "id",
			},
		},
		Processing: ProcessingConfig{
			BatchSize:       1000,
			BatchDeleteSize: 500,
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Errorf("expected no validation errors, got: %v", err)
	}
}

func TestMissingSourceHost(t *testing.T) {
	cfg := &Config{
		Source: DatabaseConfig{
			Port:     3306,
			User:     "root",
			Database: "testdb",
		},
		Destination: DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Database: "archivedb",
		},
		Jobs: map[string]JobConfig{
			"test_job": {RootTable: "orders", PrimaryKey: "id"},
		},
		Processing: ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500},
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("expected validation error for missing source host")
	}
	if !strings.Contains(err.Error(), "source.host") {
		t.Errorf("expected error to mention 'source.host', got: %v", err)
	}
}

func TestInvalidPort(t *testing.T) {
	cfg := &Config{
		Source: DatabaseConfig{
			Host:     "localhost",
			Port:     99999, // Invalid port
			User:     "root",
			Database: "testdb",
		},
		Destination: DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Database: "archivedb",
		},
		Jobs: map[string]JobConfig{
			"test_job": {RootTable: "orders", PrimaryKey: "id"},
		},
		Processing: ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500},
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("expected validation error for invalid port")
	}
	if !strings.Contains(err.Error(), "source.port") {
		t.Errorf("expected error to mention 'source.port', got: %v", err)
	}
}

func TestNoJobs(t *testing.T) {
	cfg := &Config{
		Source: DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Database: "testdb",
		},
		Destination: DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Database: "archivedb",
		},
		Jobs:       map[string]JobConfig{},
		Processing: ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500},
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("expected validation error for no jobs")
	}
	if !strings.Contains(err.Error(), "at least one job") {
		t.Errorf("expected error about jobs, got: %v", err)
	}
}

func TestJobMissingRootTable(t *testing.T) {
	cfg := &Config{
		Source: DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Database: "testdb",
		},
		Destination: DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Database: "archivedb",
		},
		Jobs: map[string]JobConfig{
			"test_job": {
				PrimaryKey: "id",
				// Missing RootTable
			},
		},
		Processing: ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500},
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("expected validation error for missing root_table")
	}
	if !strings.Contains(err.Error(), "root_table") {
		t.Errorf("expected error about root_table, got: %v", err)
	}
}

func TestInvalidTLS(t *testing.T) {
	cfg := &Config{
		Source: DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Database: "testdb",
			TLS:      "invalid_tls",
		},
		Destination: DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Database: "archivedb",
		},
		Jobs: map[string]JobConfig{
			"test_job": {RootTable: "orders", PrimaryKey: "id"},
		},
		Processing: ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500},
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("expected validation error for invalid TLS")
	}
	if !strings.Contains(err.Error(), "tls") {
		t.Errorf("expected error about tls, got: %v", err)
	}
}

func TestInvalidVerificationMethod(t *testing.T) {
	cfg := &Config{
		Source: DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Database: "testdb",
		},
		Destination: DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Database: "archivedb",
		},
		Jobs: map[string]JobConfig{
			"test_job": {RootTable: "orders", PrimaryKey: "id"},
		},
		Processing: ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500},
		Verification: VerificationConfig{
			Method: "invalid_method",
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("expected validation error for invalid verification method")
	}
	if !strings.Contains(err.Error(), "verification.method") {
		t.Errorf("expected error about verification.method, got: %v", err)
	}
}

func TestInvalidBatchSize(t *testing.T) {
	cfg := &Config{
		Source: DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Database: "testdb",
		},
		Destination: DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Database: "archivedb",
		},
		Jobs: map[string]JobConfig{
			"test_job": {RootTable: "orders", PrimaryKey: "id"},
		},
		Processing: ProcessingConfig{
			BatchSize:       0, // Invalid
			BatchDeleteSize: 500,
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("expected validation error for invalid batch_size")
	}
	if !strings.Contains(err.Error(), "batch_size") {
		t.Errorf("expected error about batch_size, got: %v", err)
	}
}

func TestReplicaValidation(t *testing.T) {
	cfg := &Config{
		Source: DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Database: "testdb",
		},
		Destination: DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Database: "archivedb",
		},
		Replica: ReplicaConfig{
			Enabled: true,
			// Missing host and user
			Port: 3306,
		},
		Jobs: map[string]JobConfig{
			"test_job": {RootTable: "orders", PrimaryKey: "id"},
		},
		Processing: ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500},
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("expected validation error for enabled replica without host")
	}
	if !strings.Contains(err.Error(), "replica.host") {
		t.Errorf("expected error about replica.host, got: %v", err)
	}
}

func TestRelationValidation(t *testing.T) {
	cfg := &Config{
		Source: DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Database: "testdb",
		},
		Destination: DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Database: "archivedb",
		},
		Jobs: map[string]JobConfig{
			"test_job": {
				RootTable:  "orders",
				PrimaryKey: "id",
				Relations: []Relation{
					{
						Table: "order_items",
						// Missing ForeignKey
						DependencyType: "1-N",
					},
				},
			},
		},
		Processing: ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500},
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("expected validation error for missing foreign_key")
	}
	if !strings.Contains(err.Error(), "foreign_key") {
		t.Errorf("expected error about foreign_key, got: %v", err)
	}
}

func TestMultipleErrors(t *testing.T) {
	cfg := &Config{
		Source: DatabaseConfig{
			// Missing everything
		},
		Destination: DatabaseConfig{
			// Missing everything
		},
		Jobs:       map[string]JobConfig{},
		Processing: ProcessingConfig{BatchSize: 0, BatchDeleteSize: 0},
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("expected multiple validation errors")
	}

	// Should contain multiple errors
	errStr := err.Error()
	if !strings.Contains(errStr, "source.host") {
		t.Error("expected error about source.host")
	}
	if !strings.Contains(errStr, "destination.host") {
		t.Error("expected error about destination.host")
	}
	if !strings.Contains(errStr, "at least one job") {
		t.Error("expected error about jobs")
	}
}
