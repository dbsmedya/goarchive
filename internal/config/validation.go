package config

import (
	"fmt"
	"strings"

	"github.com/dbsmedya/dbsgomysql/pkg/sqlutil"
)

// ValidationError represents a configuration validation error.
type ValidationError struct {
	Field   string
	Message string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

// ValidationErrors is a collection of validation errors.
type ValidationErrors []ValidationError

func (e ValidationErrors) Error() string {
	if len(e) == 0 {
		return ""
	}
	var msgs []string
	for _, err := range e {
		msgs = append(msgs, err.Error())
	}
	return fmt.Sprintf("validation failed:\n  - %s", strings.Join(msgs, "\n  - "))
}

// Validate checks the configuration for required fields and valid values.
func (c *Config) Validate() error {
	var errors ValidationErrors

	// Validate source database
	if err := c.validateDatabase("source", &c.Source); err != nil {
		errors = append(errors, err...)
	}

	// Validate destination database
	if err := c.validateDatabase("destination", &c.Destination); err != nil {
		errors = append(errors, err...)
	}

	// Validate the replication block
	if err := c.validateReplication(); err != nil {
		errors = append(errors, err...)
	}

	// Reject configs still carrying the 2.0 replica/lag keys
	if err := c.validateLegacyReplicaKeys(); err != nil {
		errors = append(errors, err...)
	}

	// Validate jobs
	if len(c.Jobs) == 0 {
		errors = append(errors, ValidationError{
			Field:   "jobs",
			Message: "at least one job must be defined",
		})
	}
	for name, job := range c.Jobs {
		if err := c.validateJob(name, &job); err != nil {
			errors = append(errors, err...)
		}
	}

	// Validate processing settings
	if err := c.validateProcessing(); err != nil {
		errors = append(errors, err...)
	}

	// Validate safety settings
	if err := c.validateSafety(); err != nil {
		errors = append(errors, err...)
	}

	// Validate verification settings
	if err := c.validateVerification(); err != nil {
		errors = append(errors, err...)
	}

	// Validate logging settings
	if err := c.validateLogging(); err != nil {
		errors = append(errors, err...)
	}

	if len(errors) > 0 {
		return errors
	}
	return nil
}

func (c *Config) validateDatabase(prefix string, db *DatabaseConfig) ValidationErrors {
	var errors ValidationErrors

	if db.Host == "" {
		errors = append(errors, ValidationError{
			Field:   prefix + ".host",
			Message: "host is required",
		})
	}

	if db.Port <= 0 || db.Port > 65535 {
		errors = append(errors, ValidationError{
			Field:   prefix + ".port",
			Message: "port must be between 1 and 65535",
		})
	}

	if db.User == "" {
		errors = append(errors, ValidationError{
			Field:   prefix + ".user",
			Message: "user is required",
		})
	}

	if db.Database == "" {
		errors = append(errors, ValidationError{
			Field:   prefix + ".database",
			Message: "database name is required",
		})
	}

	validTLS := map[string]bool{"disable": true, "preferred": true, "skip-verify": true, "required": true, "": true}
	if !validTLS[db.TLS] {
		errors = append(errors, ValidationError{
			Field:   prefix + ".tls",
			Message: "tls must be 'disable', 'preferred', 'skip-verify', or 'required'",
		})
	}

	if db.MaxConnections < 0 {
		errors = append(errors, ValidationError{
			Field:   prefix + ".max_connections",
			Message: "max_connections cannot be negative",
		})
	}

	if db.MaxIdleConnections < 0 {
		errors = append(errors, ValidationError{
			Field:   prefix + ".max_idle_connections",
			Message: "max_idle_connections cannot be negative",
		})
	}

	// job_schema is destination-only; source ignores it.
	if prefix == "destination" && db.JobSchema != "" && !sqlutil.IsSimpleIdentifier(db.JobSchema) {
		errors = append(errors, ValidationError{
			Field:   prefix + ".job_schema",
			Message: "must contain only alphanumeric characters and underscores",
		})
	}

	return errors
}

// validReplicationTLS is the TLS mode enum BuildDSN understands. Unlike
// validateDatabase's copy, "" is not accepted: the loader's normalization pass
// fills it in, and an empty value reaching BuildDSN's default-less switch would
// silently connect unencrypted.
var validReplicationTLS = map[string]bool{
	"disable": true, "preferred": true, "skip-verify": true, "required": true,
}

// renderChannel renders a channel name for operator-facing messages: the
// default (unnamed) channel is "" on the wire and <default> on the page.
func renderChannel(name string) string {
	if name == "" {
		return "<default>"
	}
	return name
}

// validateReplication checks the replication block. Per-server rules apply to
// whatever servers are declared, so a typo is caught before the block is
// enabled; the fleet-size and cadence rules apply only when it is enabled.
// Values are assumed normalized (see Config.normalizeReplication).
func (c *Config) validateReplication() ValidationErrors {
	var errors ValidationErrors

	if c.Replication.Enabled && len(c.Replication.Servers) == 0 {
		errors = append(errors, ValidationError{
			Field:   "replication.servers",
			Message: "at least one server is required when replication.enabled is true",
		})
	}

	if c.Replication.SecondsBehindSourceWithin < 0 {
		errors = append(errors, ValidationError{
			Field:   "replication.seconds_behind_source_within",
			Message: "seconds_behind_source_within cannot be negative",
		})
	}

	if c.Replication.CacheTTL < 0 {
		errors = append(errors, ValidationError{
			Field:   "replication.cache_ttl",
			Message: "cache_ttl cannot be negative",
		})
	}

	if c.Replication.Enabled && c.Replication.CheckInterval <= 0 {
		errors = append(errors, ValidationError{
			Field:   "replication.check_interval",
			Message: "check_interval must be positive when replication is enabled",
		})
	}

	seenAddr := make(map[string]bool, len(c.Replication.Servers))
	for i, server := range c.Replication.Servers {
		prefix := fmt.Sprintf("replication.servers[%d]", i)
		addr := server.Addr()

		if server.Host == "" {
			errors = append(errors, ValidationError{
				Field:   prefix + ".host",
				Message: "host is required",
			})
		}

		// Addr() is at once the log identity, the duplicate-detection key above,
		// and the identity the database and estimator wiring consume, so a host
		// carrying \n or \r would both split a log line and admit two spellings
		// of one server. The message is fixed: it must never echo the value.
		if strings.ContainsAny(server.Host, "\n\r") {
			errors = append(errors, ValidationError{
				Field:   prefix + ".host",
				Message: "host must not contain newline or carriage return",
			})
		}

		if server.User == "" {
			errors = append(errors, ValidationError{
				Field:   prefix + ".user",
				Message: "user is required",
			})
		}

		if server.Port < 1 || server.Port > 65535 {
			errors = append(errors, ValidationError{
				Field:   prefix + ".port",
				Message: "port must be between 1 and 65535",
			})
		}

		if seenAddr[addr] {
			errors = append(errors, ValidationError{
				Field:   prefix,
				Message: fmt.Sprintf("duplicate server address %q", addr),
			})
		}
		seenAddr[addr] = true

		if server.Type != "async" {
			errors = append(errors, ValidationError{
				Field:   prefix + ".type",
				Message: fmt.Sprintf("unsupported replication type %q for server %s; supported: async", server.Type, addr),
			})
		}

		if !validReplicationTLS[server.TLS] {
			errors = append(errors, ValidationError{
				Field:   prefix + ".tls",
				Message: fmt.Sprintf("unsupported tls value %q for server %s; supported: disable, preferred, skip-verify, required", server.TLS, addr),
			})
		}

		seenChannel := make(map[string]bool, len(server.Channels))
		for _, channel := range server.Channels {
			if seenChannel[channel] {
				errors = append(errors, ValidationError{
					Field:   prefix + ".channels",
					Message: fmt.Sprintf("duplicate channel %q for server %s", renderChannel(channel), addr),
				})
			}
			seenChannel[channel] = true
		}
	}

	return errors
}

// validateLegacyReplicaKeys rejects configs still carrying the 2.0 keys that
// 2.1 removed. The fields survive as zero-valued sentinels for exactly this
// detection, so any non-zero value means the operator has not migrated.
func (c *Config) validateLegacyReplicaKeys() ValidationErrors {
	var errors ValidationErrors

	if c.Replica != (ReplicaConfig{}) {
		errors = append(errors, ValidationError{
			Field:   "replica",
			Message: "the replica: block was removed in 2.1 — replication monitoring is now configured by the replication: block; see docs/README_UPGRADING_2_1.md",
		})
	}

	if c.Safety.LagThreshold != 0 {
		errors = append(errors, ValidationError{
			Field:   "safety.lag_threshold",
			Message: "lag_threshold was removed in 2.1 — use replication.seconds_behind_source_within; see docs/README_UPGRADING_2_1.md",
		})
	}

	if c.Safety.CheckInterval != 0 {
		errors = append(errors, ValidationError{
			Field:   "safety.check_interval",
			Message: "safety.check_interval was removed in 2.1 — use replication.check_interval; see docs/README_UPGRADING_2_1.md",
		})
	}

	return errors
}

func (c *Config) validateJob(name string, job *JobConfig) ValidationErrors {
	var errors ValidationErrors
	prefix := fmt.Sprintf("jobs.%s", name)

	if job.RootTable == "" {
		errors = append(errors, ValidationError{
			Field:   prefix + ".root_table",
			Message: "root_table is required",
		})
	} else if !sqlutil.IsSimpleIdentifier(job.RootTable) {
		errors = append(errors, ValidationError{
			Field:   prefix + ".root_table",
			Message: "must contain only alphanumeric characters and underscores",
		})
	}

	if job.PrimaryKey == "" {
		errors = append(errors, ValidationError{
			Field:   prefix + ".primary_key",
			Message: "primary_key is required",
		})
	} else if !sqlutil.IsSimpleIdentifier(job.PrimaryKey) {
		errors = append(errors, ValidationError{
			Field:   prefix + ".primary_key",
			Message: "must contain only alphanumeric characters and underscores",
		})
	}

	if strings.TrimSpace(job.Where) == "" {
		errors = append(errors, ValidationError{
			Field:   prefix + ".where",
			Message: `where is required; use where: "1=1" explicitly to process the entire table`,
		})
	}

	// Validate relations recursively
	for i, rel := range job.Relations {
		relPrefix := fmt.Sprintf("%s.relations[%d]", prefix, i)
		if err := c.validateRelation(relPrefix, &rel, 1); err != nil {
			errors = append(errors, err...)
		}
	}

	if job.Processing != nil {
		merged := job.GetJobProcessing(c.Processing)
		if err := c.validateProcessingConfig(prefix+".processing", &merged); err != nil {
			errors = append(errors, err...)
		}
	}

	if job.Verification != nil {
		merged := job.GetJobVerification(c.Verification)
		if err := c.validateVerificationConfig(prefix+".verification", &merged, false); err != nil {
			errors = append(errors, err...)
		}
	}

	return errors
}

const maxRelationDepth = 10

func (c *Config) validateRelation(prefix string, rel *Relation, depth int) ValidationErrors {
	var errors ValidationErrors

	if depth > maxRelationDepth {
		errors = append(errors, ValidationError{
			Field:   prefix,
			Message: fmt.Sprintf("relation nesting exceeds maximum nesting depth of %d", maxRelationDepth),
		})
		return errors
	}

	if rel.Table == "" {
		errors = append(errors, ValidationError{
			Field:   prefix + ".table",
			Message: "table name is required",
		})
	} else if !sqlutil.IsSimpleIdentifier(rel.Table) {
		errors = append(errors, ValidationError{
			Field:   prefix + ".table",
			Message: "must contain only alphanumeric characters and underscores",
		})
	}

	if rel.ForeignKey == "" {
		errors = append(errors, ValidationError{
			Field:   prefix + ".foreign_key",
			Message: "foreign_key is required",
		})
	} else if !sqlutil.IsSimpleIdentifier(rel.ForeignKey) {
		errors = append(errors, ValidationError{
			Field:   prefix + ".foreign_key",
			Message: "must contain only alphanumeric characters and underscores",
		})
	}

	if rel.PrimaryKey == "" {
		errors = append(errors, ValidationError{
			Field:   prefix + ".primary_key",
			Message: "primary_key is required",
		})
	} else if !sqlutil.IsSimpleIdentifier(rel.PrimaryKey) {
		errors = append(errors, ValidationError{
			Field:   prefix + ".primary_key",
			Message: "must contain only alphanumeric characters and underscores",
		})
	}

	validTypes := map[string]bool{"1-1": true, "1-N": true, "": true}
	if !validTypes[rel.DependencyType] {
		errors = append(errors, ValidationError{
			Field:   prefix + ".dependency_type",
			Message: "dependency_type must be '1-1' or '1-N'",
		})
	}

	// Validate nested relations
	for i, nested := range rel.Relations {
		nestedPrefix := fmt.Sprintf("%s.relations[%d]", prefix, i)
		if err := c.validateRelation(nestedPrefix, &nested, depth+1); err != nil {
			errors = append(errors, err...)
		}
	}

	return errors
}

func (c *Config) validateProcessing() ValidationErrors {
	return c.validateProcessingConfig("processing", &c.Processing)
}

func (c *Config) validateProcessingConfig(prefix string, processing *ProcessingConfig) ValidationErrors {
	var errors ValidationErrors

	if processing.BatchSize <= 0 {
		errors = append(errors, ValidationError{
			Field:   prefix + ".batch_size",
			Message: "batch_size must be positive",
		})
	}

	if processing.BatchDeleteSize <= 0 {
		errors = append(errors, ValidationError{
			Field:   prefix + ".batch_delete_size",
			Message: "batch_delete_size must be positive",
		})
	}

	if processing.SleepSeconds < 0 {
		errors = append(errors, ValidationError{
			Field:   prefix + ".sleep_seconds",
			Message: "sleep_seconds cannot be negative",
		})
	}

	if processing.DeleteSleepSeconds < 0 {
		errors = append(errors, ValidationError{
			Field:   prefix + ".delete_sleep_seconds",
			Message: "delete_sleep_seconds cannot be negative",
		})
	}

	return errors
}

// validateSafety checks the safety block. Its lag_threshold and check_interval
// rules moved to validateReplication in 2.1; the removed keys are now rejected
// outright by validateLegacyReplicaKeys, leaving only
// disable_foreign_key_checks, a bool with nothing to reject.
func (c *Config) validateSafety() ValidationErrors {
	// disable_foreign_key_checks is a bool; there is nothing left to reject.
	return nil
}

func (c *Config) validateVerification() ValidationErrors {
	return c.validateVerificationConfig("verification", &c.Verification, true)
}

func (c *Config) validateVerificationConfig(prefix string, verification *VerificationConfig, requireMethod bool) ValidationErrors {
	var errors ValidationErrors

	validMethods := map[string]bool{"count": true, "sha256": true}
	if !requireMethod && verification.Method == "" {
		return errors
	}
	if !validMethods[verification.Method] {
		errors = append(errors, ValidationError{
			Field:   prefix + ".method",
			Message: "method must be 'count' or 'sha256'",
		})
	}

	return errors
}

func (c *Config) validateLogging() ValidationErrors {
	errors := validateLoggingConfig(c.Logging, "logging")

	// Validate each job's effective (merged) logging config so errors in
	// job-level overrides are reported against the job that set them.
	for name, job := range c.Jobs {
		if job.Logging == nil {
			continue
		}
		merged := job.GetJobLogging(c.Logging)
		errors = append(errors, validateLoggingConfig(merged, "jobs."+name+".logging")...)
	}

	return errors
}

func validateLoggingConfig(lc LoggingConfig, prefix string) ValidationErrors {
	var errors ValidationErrors

	validLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true, "": true}
	if !validLevels[lc.Level] {
		errors = append(errors, ValidationError{
			Field:   prefix + ".level",
			Message: "level must be 'debug', 'info', 'warn', or 'error'",
		})
	}

	validFormats := map[string]bool{"json": true, "text": true, "": true}
	if !validFormats[lc.Format] {
		errors = append(errors, ValidationError{
			Field:   prefix + ".format",
			Message: "format must be 'json' or 'text'",
		})
	}

	if lc.FileOnly {
		switch lc.Output {
		case "", "stdout", "stderr":
			errors = append(errors, ValidationError{
				Field:   prefix + ".file_only",
				Message: "file_only requires " + prefix + ".output to be a file path",
			})
		}
	}

	return errors
}
