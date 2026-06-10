package config

import (
	"fmt"
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
				Where:      "1=1",
			},
		},
		Processing: ProcessingConfig{
			BatchSize:       1000,
			BatchDeleteSize: 500,
		},
		Verification: VerificationConfig{Method: "count"},
	}

	if err := cfg.Validate(); err != nil {
		t.Errorf("expected no validation errors, got: %v", err)
	}
}

func TestWhereIsRequired(t *testing.T) {
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
			"test_job": {RootTable: "orders", PrimaryKey: "id"},
		},
		Processing:   ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500},
		Verification: VerificationConfig{Method: "count"},
	}
	job := cfg.Jobs["test_job"]
	job.Where = "   "
	cfg.Jobs["test_job"] = job

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error for empty where, got nil")
	}
	if !strings.Contains(err.Error(), "jobs.test_job.where") {
		t.Fatalf("expected jobs.test_job.where in error, got: %v", err)
	}

	job.Where = "1=1" // explicit full-table opt-in must be allowed
	cfg.Jobs["test_job"] = job
	if err := cfg.Validate(); err != nil {
		t.Fatalf("where=1=1 must be allowed, got: %v", err)
	}
}

func TestFileOnlyRequiresFileOutput(t *testing.T) {
	base := func() *Config {
		return &Config{
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
				"test_job": {RootTable: "orders", PrimaryKey: "id", Where: "1=1"},
			},
			Processing:   ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500},
			Verification: VerificationConfig{Method: "count"},
		}
	}

	for _, output := range []string{"", "stdout", "stderr"} {
		t.Run("rejects_"+output, func(t *testing.T) {
			cfg := base()
			cfg.Logging = LoggingConfig{Output: output, FileOnly: true}
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("expected validation error for file_only with output %q", output)
			}
			if !strings.Contains(err.Error(), "logging.file_only") {
				t.Errorf("expected error to mention 'logging.file_only', got: %v", err)
			}
		})
	}

	t.Run("accepts_file_path", func(t *testing.T) {
		cfg := base()
		cfg.Logging = LoggingConfig{Output: "/var/log/goarchive.log", FileOnly: true}
		if err := cfg.Validate(); err != nil {
			t.Errorf("expected no validation error for file_only with file path, got: %v", err)
		}
	})
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

func TestTopLevelEmptyVerificationMethodInvalid(t *testing.T) {
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
			Method: "",
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("expected validation error for empty top-level verification method")
	}
	if !strings.Contains(err.Error(), "verification.method") {
		t.Errorf("expected error about verification.method, got: %v", err)
	}
}

func TestJobLevelEmptyVerificationMethodInheritsGlobal(t *testing.T) {
	skip := true
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
				Where:      "1=1",
				Verification: &VerificationOverrides{
					Method:           "",
					SkipVerification: &skip,
				},
			},
		},
		Processing:   ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500},
		Verification: VerificationConfig{Method: "sha256"},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected job-level empty method to be valid for inheritance, got: %v", err)
	}

	got := cfg.GetJobVerification("test_job")
	if got.Method != "sha256" {
		t.Fatalf("expected job verification method to inherit sha256, got %q", got.Method)
	}
	if !got.SkipVerification {
		t.Fatal("expected job skip_verification override to remain true")
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

func TestRelationValidation_MissingPrimaryKey(t *testing.T) {
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
						Table:          "order_items",
						ForeignKey:     "order_id",
						DependencyType: "1-N",
						// Missing PrimaryKey
					},
				},
			},
		},
		Processing: ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500},
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("expected validation error for missing primary_key")
	}
	if !strings.Contains(err.Error(), "primary_key") {
		t.Errorf("expected error about primary_key, got: %v", err)
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

func TestJobLevelProcessingAndVerificationValidation(t *testing.T) {
	badBatchSize := -5
	batchDeleteSize := 100
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
				Processing: &ProcessingOverrides{
					BatchSize:       &badBatchSize,
					BatchDeleteSize: &batchDeleteSize,
				},
				Verification: &VerificationOverrides{
					Method: "bad_method",
				},
			},
		},
		Processing: ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation errors for invalid job overrides")
	}
	if !strings.Contains(err.Error(), "jobs.test_job.processing.batch_size") {
		t.Errorf("expected error about job processing batch_size, got: %v", err)
	}
	if !strings.Contains(err.Error(), "jobs.test_job.verification.method") {
		t.Errorf("expected error about job verification method, got: %v", err)
	}
}

func TestJobPartialProcessingBlockValidatesMerged(t *testing.T) {
	two := 2.0
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
				Where:      "1=1",
				Processing: &ProcessingOverrides{SleepSeconds: &two},
			},
		},
		Processing:   ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500},
		Verification: VerificationConfig{Method: "count"},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("partial per-job processing block must validate against merged config, got: %v", err)
	}
}

func TestCheckIntervalZeroWithReplicaEnabled(t *testing.T) {
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
			Host:    "localhost",
			Port:    3308,
			User:    "root",
		},
		Safety: SafetyConfig{
			CheckInterval: 0,
		},
		Jobs: map[string]JobConfig{
			"test_job": {RootTable: "orders", PrimaryKey: "id"},
		},
		Processing: ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error for check_interval=0 with replica enabled")
	}
	if !strings.Contains(err.Error(), "safety.check_interval") {
		t.Errorf("expected error about safety.check_interval, got: %v", err)
	}
}

func TestValidate_RelationMaxDepthExceeded(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Source = DatabaseConfig{Host: "localhost", Port: 3306, User: "root", Password: "pass", Database: "src"}
	cfg.Destination = DatabaseConfig{Host: "localhost", Port: 3306, User: "root", Password: "pass", Database: "dst"}

	// Build a chain 11 levels deep
	deepest := Relation{Table: "t11", ForeignKey: "fk", PrimaryKey: "id", DependencyType: "1-N"}
	for i := 10; i >= 1; i-- {
		deepest = Relation{
			Table:          fmt.Sprintf("t%d", i),
			ForeignKey:     "fk",
			PrimaryKey:     "id",
			DependencyType: "1-N",
			Relations:      []Relation{deepest},
		}
	}

	cfg.Jobs = map[string]JobConfig{
		"deep": {RootTable: "root", PrimaryKey: "id", Relations: []Relation{deepest}},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error for depth > 10")
	}
	if !strings.Contains(err.Error(), "exceeds maximum nesting depth") {
		t.Errorf("expected depth error, got: %v", err)
	}
}

func TestValidate_RelationAtMaxDepth(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Source = DatabaseConfig{Host: "localhost", Port: 3306, User: "root", Password: "pass", Database: "src"}
	cfg.Destination = DatabaseConfig{Host: "localhost", Port: 3306, User: "root", Password: "pass", Database: "dst"}

	// Build a chain exactly 10 levels deep - should pass
	deepest := Relation{Table: "t10", ForeignKey: "fk", PrimaryKey: "id", DependencyType: "1-N"}
	for i := 9; i >= 1; i-- {
		deepest = Relation{
			Table:          fmt.Sprintf("t%d", i),
			ForeignKey:     "fk",
			PrimaryKey:     "id",
			DependencyType: "1-N",
			Relations:      []Relation{deepest},
		}
	}

	cfg.Jobs = map[string]JobConfig{
		"deep": {RootTable: "root", PrimaryKey: "id", Where: "1=1", Relations: []Relation{deepest}},
	}

	err := cfg.Validate()
	if err != nil {
		t.Fatalf("depth=10 should pass validation, got: %v", err)
	}
}

func TestValidate_JobSchemaInvalidIdentifier(t *testing.T) {
	t.Run("destination job_schema with invalid identifier is rejected", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Source = DatabaseConfig{Host: "localhost", Port: 3306, User: "root", Password: "pass", Database: "testdb"}
		cfg.Destination = DatabaseConfig{Host: "localhost", Port: 3307, User: "root", Password: "pass", Database: "archivedb"}
		cfg.Jobs = map[string]JobConfig{
			"test_job": {RootTable: "orders", PrimaryKey: "id", Where: "1=1"},
		}
		cfg.Destination.JobSchema = "bad-schema!" // hyphen/punctuation invalid
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "destination.job_schema") {
			t.Fatalf("expected destination.job_schema validation error, got %v", err)
		}
	})

	t.Run("source job_schema not validated", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Source = DatabaseConfig{Host: "localhost", Port: 3306, User: "root", Password: "pass", Database: "testdb"}
		cfg.Destination = DatabaseConfig{Host: "localhost", Port: 3307, User: "root", Password: "pass", Database: "archivedb"}
		cfg.Jobs = map[string]JobConfig{
			"test_job": {RootTable: "orders", PrimaryKey: "id", Where: "1=1"},
		}
		cfg.Source.JobSchema = "bad-schema!" // invalid identifier, but source is not validated
		err := cfg.Validate()
		if err != nil && strings.Contains(err.Error(), "source.job_schema") {
			t.Fatalf("source.job_schema must not be validated, got error: %v", err)
		}
	})
}

func TestJobLoggingValidation(t *testing.T) {
	base := func() *Config {
		return &Config{
			Source: DatabaseConfig{
				Host: "localhost", Port: 3306, User: "root", Password: "p", Database: "testdb",
			},
			Destination: DatabaseConfig{
				Host: "localhost", Port: 3307, User: "root", Password: "p", Database: "archivedb",
			},
			Jobs: map[string]JobConfig{
				"test_job": {RootTable: "orders", PrimaryKey: "id", Where: "1=1"},
			},
			Processing:   ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500},
			Verification: VerificationConfig{Method: "count"},
		}
	}

	withJobLogging := func(cfg *Config, lc *LoggingConfig) {
		job := cfg.Jobs["test_job"]
		job.Logging = lc
		cfg.Jobs["test_job"] = job
	}

	t.Run("invalid job level", func(t *testing.T) {
		cfg := base()
		withJobLogging(cfg, &LoggingConfig{Level: "verbose"})
		err := cfg.Validate()
		if err == nil {
			t.Fatal("expected validation error for invalid job logging level")
		}
		if !strings.Contains(err.Error(), "jobs.test_job.logging.level") {
			t.Errorf("expected error to mention 'jobs.test_job.logging.level', got: %v", err)
		}
	})

	t.Run("invalid job format", func(t *testing.T) {
		cfg := base()
		withJobLogging(cfg, &LoggingConfig{Format: "xml"})
		err := cfg.Validate()
		if err == nil {
			t.Fatal("expected validation error for invalid job logging format")
		}
		if !strings.Contains(err.Error(), "jobs.test_job.logging.format") {
			t.Errorf("expected error to mention 'jobs.test_job.logging.format', got: %v", err)
		}
	})

	t.Run("job file_only with inherited stdout output", func(t *testing.T) {
		cfg := base()
		withJobLogging(cfg, &LoggingConfig{FileOnly: true})
		err := cfg.Validate()
		if err == nil {
			t.Fatal("expected validation error for job file_only with inherited stdout output")
		}
		if !strings.Contains(err.Error(), "jobs.test_job.logging.file_only") {
			t.Errorf("expected error to mention 'jobs.test_job.logging.file_only', got: %v", err)
		}
	})

	t.Run("job file_only with job file output is valid", func(t *testing.T) {
		cfg := base()
		withJobLogging(cfg, &LoggingConfig{
			Level: "info", Format: "text",
			Output: "/opt/goarchive/ShipmentErrorLogs.log", FileOnly: true,
		})
		if err := cfg.Validate(); err != nil {
			t.Errorf("expected no validation error, got: %v", err)
		}
	})

	t.Run("job file_only valid with global file output inherited", func(t *testing.T) {
		cfg := base()
		cfg.Logging = LoggingConfig{Output: "/var/log/goarchive.log"}
		withJobLogging(cfg, &LoggingConfig{FileOnly: true})
		if err := cfg.Validate(); err != nil {
			t.Errorf("expected no validation error (output inherited from global), got: %v", err)
		}
	})
}
