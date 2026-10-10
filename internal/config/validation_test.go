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
		Processing: ProcessingConfig{
			BatchSize:       1000,
			BatchDeleteSize: 500,
		},
	}

	if err := cfg.ValidateShared(); err != nil {
		t.Errorf("expected no validation errors, got: %v", err)
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
			Processing: ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500},
		}
	}

	for _, output := range []string{"", "stdout", "stderr"} {
		t.Run("rejects_"+output, func(t *testing.T) {
			cfg := base()
			cfg.Logging = LoggingConfig{Output: output, FileOnly: true}
			err := cfg.ValidateShared()
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
		if err := cfg.ValidateShared(); err != nil {
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
		Processing: ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500},
	}

	err := cfg.ValidateShared()
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
		Processing: ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500},
	}

	err := cfg.ValidateShared()
	if err == nil {
		t.Error("expected validation error for invalid port")
	}
	if !strings.Contains(err.Error(), "source.port") {
		t.Errorf("expected error to mention 'source.port', got: %v", err)
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
		Processing: ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500},
	}

	err := cfg.ValidateShared()
	if err == nil {
		t.Error("expected validation error for invalid TLS")
	}
	if !strings.Contains(err.Error(), "tls") {
		t.Errorf("expected error about tls, got: %v", err)
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
		Processing: ProcessingConfig{
			BatchSize:       0, // Invalid
			BatchDeleteSize: 500,
		},
	}

	err := cfg.ValidateShared()
	if err == nil {
		t.Error("expected validation error for invalid batch_size")
	}
	if !strings.Contains(err.Error(), "batch_size") {
		t.Errorf("expected error about batch_size, got: %v", err)
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
		Processing: ProcessingConfig{BatchSize: 0, BatchDeleteSize: 0},
	}

	err := cfg.ValidateShared()
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
	if !strings.Contains(errStr, "processing.batch_size") {
		t.Error("expected error about processing.batch_size")
	}
}

func TestValidate_JobSchemaInvalidIdentifier(t *testing.T) {
	t.Run("destination job_schema with invalid identifier is rejected", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Source = DatabaseConfig{Host: "localhost", Port: 3306, User: "root", Password: "pass", Database: "testdb"}
		cfg.Destination = DatabaseConfig{Host: "localhost", Port: 3307, User: "root", Password: "pass", Database: "archivedb"}
		cfg.Destination.JobSchema = "bad-schema!" // hyphen/punctuation invalid
		err := cfg.ValidateShared()
		if err == nil || !strings.Contains(err.Error(), "destination.job_schema") {
			t.Fatalf("expected destination.job_schema validation error, got %v", err)
		}
	})

	t.Run("source job_schema not validated", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Source = DatabaseConfig{Host: "localhost", Port: 3306, User: "root", Password: "pass", Database: "testdb"}
		cfg.Destination = DatabaseConfig{Host: "localhost", Port: 3307, User: "root", Password: "pass", Database: "archivedb"}
		cfg.Source.JobSchema = "bad-schema!" // invalid identifier, but source is not validated
		err := cfg.ValidateShared()
		if err != nil && strings.Contains(err.Error(), "source.job_schema") {
			t.Fatalf("source.job_schema must not be validated, got error: %v", err)
		}
	})
}

// validReplicationServer returns a server entry as it looks after the loader's
// normalization pass, so a subtest's only defect is the one it introduces.
func validReplicationServer(host string) ReplicationServerConfig {
	return ReplicationServerConfig{
		Host:     host,
		Port:     3306,
		User:     "monitor",
		Password: "pw",
		TLS:      "preferred",
		Type:     "async",
	}
}

// replicationBaseConfig returns a config that validates cleanly with a valid
// enabled replication block.
func replicationBaseConfig() *Config {
	return &Config{
		Source: DatabaseConfig{
			Host: "localhost", Port: 3306, User: "root", Password: "pass", Database: "testdb",
		},
		Destination: DatabaseConfig{
			Host: "localhost", Port: 3307, User: "root", Password: "pass", Database: "archivedb",
		},
		Replication: ReplicationConfig{
			Enabled:                   true,
			SecondsBehindSourceWithin: 10,
			CheckInterval:             5,
			CacheTTL:                  15,
			Servers:                   []ReplicationServerConfig{validReplicationServer("r1")},
		},
		Processing: ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500},
	}
}

// assertValidationError fails unless errs carries exactly this Field+Message.
func assertValidationError(t *testing.T, cfg *Config, field, message string) {
	t.Helper()
	err := cfg.ValidateShared()
	if err == nil {
		t.Fatalf("expected validation error {Field:%q Message:%q}, got none", field, message)
	}
	errs, ok := err.(ValidationErrors)
	if !ok {
		t.Fatalf("ValidateShared() returned %T, want ValidationErrors: %v", err, err)
	}
	for _, e := range errs {
		if e.Field == field && e.Message == message {
			return
		}
	}
	t.Fatalf("missing validation error {Field:%q Message:%q}; got: %v", field, message, errs)
}

func TestValidateReplication(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(c *Config)
		field   string // empty means: expect no validation errors at all
		message string
	}{
		{
			name:    "enabled with no servers",
			mutate:  func(c *Config) { c.Replication.Servers = nil },
			field:   "replication.servers",
			message: "at least one server is required when replication.enabled is true",
		},
		{
			name:    "server missing host",
			mutate:  func(c *Config) { c.Replication.Servers[0].Host = "" },
			field:   "replication.servers[0].host",
			message: "host is required",
		},
		{
			name:    "server missing user",
			mutate:  func(c *Config) { c.Replication.Servers[0].User = "" },
			field:   "replication.servers[0].user",
			message: "user is required",
		},
		{
			name:    "server port below range",
			mutate:  func(c *Config) { c.Replication.Servers[0].Port = -1 },
			field:   "replication.servers[0].port",
			message: "port must be between 1 and 65535",
		},
		{
			name:    "server port above range",
			mutate:  func(c *Config) { c.Replication.Servers[0].Port = 70000 },
			field:   "replication.servers[0].port",
			message: "port must be between 1 and 65535",
		},
		{
			name: "duplicate server address",
			mutate: func(c *Config) {
				c.Replication.Servers = append(c.Replication.Servers, validReplicationServer("r1"))
			},
			field:   "replication.servers[1]",
			message: `duplicate server address "r1:3306"`,
		},
		{
			name: "distinct ports are not duplicates",
			mutate: func(c *Config) {
				second := validReplicationServer("r1")
				second.Port = 3307
				c.Replication.Servers = append(c.Replication.Servers, second)
			},
		},
		{
			name:    "duplicate named channel",
			mutate:  func(c *Config) { c.Replication.Servers[0].Channels = []string{"ch1", "ch1"} },
			field:   "replication.servers[0].channels",
			message: `duplicate channel "ch1" for server r1:3306`,
		},
		{
			name:    "duplicate default channel",
			mutate:  func(c *Config) { c.Replication.Servers[0].Channels = []string{"", ""} },
			field:   "replication.servers[0].channels",
			message: `duplicate channel "<default>" for server r1:3306`,
		},
		{
			name:   "single default channel is valid",
			mutate: func(c *Config) { c.Replication.Servers[0].Channels = []string{""} },
		},
		{
			name:   "distinct channels are valid",
			mutate: func(c *Config) { c.Replication.Servers[0].Channels = []string{"", "ch1"} },
		},
		{
			name:    "unsupported replication type",
			mutate:  func(c *Config) { c.Replication.Servers[0].Type = "galera" },
			field:   "replication.servers[0].type",
			message: `unsupported replication type "galera" for server r1:3306; supported: async`,
		},
		{
			name:    "negative tolerance",
			mutate:  func(c *Config) { c.Replication.SecondsBehindSourceWithin = -1 },
			field:   "replication.seconds_behind_source_within",
			message: "seconds_behind_source_within cannot be negative",
		},
		{
			name:   "zero tolerance is valid",
			mutate: func(c *Config) { c.Replication.SecondsBehindSourceWithin = 0 },
		},
		{
			name:    "negative cache_ttl",
			mutate:  func(c *Config) { c.Replication.CacheTTL = -1 },
			field:   "replication.cache_ttl",
			message: "cache_ttl cannot be negative",
		},
		{
			name:   "zero cache_ttl is valid",
			mutate: func(c *Config) { c.Replication.CacheTTL = 0 },
		},
		{
			name:    "enabled with zero check_interval",
			mutate:  func(c *Config) { c.Replication.CheckInterval = 0 },
			field:   "replication.check_interval",
			message: "check_interval must be positive when replication is enabled",
		},
		{
			name:    "enabled with negative check_interval",
			mutate:  func(c *Config) { c.Replication.CheckInterval = -5 },
			field:   "replication.check_interval",
			message: "check_interval must be positive when replication is enabled",
		},
		{
			name:   "disabled block with empty servers is valid",
			mutate: func(c *Config) { c.Replication = ReplicationConfig{} },
		},
		{
			name:    "unsupported tls value",
			mutate:  func(c *Config) { c.Replication.Servers[0].TLS = "bogus" },
			field:   "replication.servers[0].tls",
			message: `unsupported tls value "bogus" for server r1:3306; supported: disable, preferred, skip-verify, required`,
		},
		{
			name:   "tls disable is valid",
			mutate: func(c *Config) { c.Replication.Servers[0].TLS = "disable" },
		},
		{
			name:   "tls preferred is valid",
			mutate: func(c *Config) { c.Replication.Servers[0].TLS = "preferred" },
		},
		{
			name:   "tls skip-verify is valid",
			mutate: func(c *Config) { c.Replication.Servers[0].TLS = "skip-verify" },
		},
		{
			name:   "tls required is valid",
			mutate: func(c *Config) { c.Replication.Servers[0].TLS = "required" },
		},
		{
			name:    "host containing newline",
			mutate:  func(c *Config) { c.Replication.Servers[0].Host = "r1\nforged" },
			field:   "replication.servers[0].host",
			message: "host must not contain newline or carriage return",
		},
		{
			name:    "host containing carriage return",
			mutate:  func(c *Config) { c.Replication.Servers[0].Host = "r1\rforged" },
			field:   "replication.servers[0].host",
			message: "host must not contain newline or carriage return",
		},
		{
			name:   "dns host is valid",
			mutate: func(c *Config) { c.Replication.Servers[0].Host = "replica-1.db.internal" },
		},
		{
			name:   "ipv4 host is valid",
			mutate: func(c *Config) { c.Replication.Servers[0].Host = "127.0.0.1" },
		},
		{
			name:   "ipv6 host is valid",
			mutate: func(c *Config) { c.Replication.Servers[0].Host = "2001:db8::1" },
		},
		{
			name: "host content rule applies to a disabled block",
			mutate: func(c *Config) {
				c.Replication.Enabled = false
				c.Replication.Servers[0].Host = "r1\nforged"
			},
			field:   "replication.servers[0].host",
			message: "host must not contain newline or carriage return",
		},
		{
			name:   "fully valid enabled block",
			mutate: func(c *Config) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := replicationBaseConfig()
			tt.mutate(cfg)

			if tt.field == "" {
				if err := cfg.ValidateShared(); err != nil {
					t.Fatalf("expected no validation errors, got: %v", err)
				}
				return
			}
			assertValidationError(t, cfg, tt.field, tt.message)
		})
	}
}

func TestValidateLegacyReplicaKeys(t *testing.T) {
	const (
		replicaMsg  = "the replica: block was removed in 2.1 — replication monitoring is now configured by the replication: block; see docs/README_UPGRADING_2_1.md"
		lagMsg      = "lag_threshold was removed in 2.1 — use replication.seconds_behind_source_within; see docs/README_UPGRADING_2_1.md"
		intervalMsg = "safety.check_interval was removed in 2.1 — use replication.check_interval; see docs/README_UPGRADING_2_1.md"
	)

	tests := []struct {
		name    string
		mutate  func(c *Config)
		field   string
		message string
	}{
		{"replica.enabled", func(c *Config) { c.Replica.Enabled = true }, "replica", replicaMsg},
		{"replica.host", func(c *Config) { c.Replica.Host = "replica-host" }, "replica", replicaMsg},
		{"replica.user", func(c *Config) { c.Replica.User = "monitor" }, "replica", replicaMsg},
		{"replica.password", func(c *Config) { c.Replica.Password = "pw" }, "replica", replicaMsg},
		{"replica.port", func(c *Config) { c.Replica.Port = 3308 }, "replica", replicaMsg},
		{"replica.replication_channel", func(c *Config) { c.Replica.ReplicationChannel = "ch1" }, "replica", replicaMsg},
		{"safety.lag_threshold", func(c *Config) { c.Safety.LagThreshold = 10 }, "safety.lag_threshold", lagMsg},
		{"safety.check_interval", func(c *Config) { c.Safety.CheckInterval = 5 }, "safety.check_interval", intervalMsg},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := replicationBaseConfig()
			tt.mutate(cfg)
			assertValidationError(t, cfg, tt.field, tt.message)
		})
	}

	t.Run("no legacy keys is valid", func(t *testing.T) {
		if err := replicationBaseConfig().ValidateShared(); err != nil {
			t.Fatalf("expected no validation errors, got: %v", err)
		}
	})
}
