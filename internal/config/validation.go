package config

import (
	"fmt"
	"math"
	"strings"
	"time"

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

	if err := ValidateSleepSeconds(processing.SleepSeconds); err != nil {
		errors = append(errors, ValidationError{Field: prefix + ".sleep_seconds", Message: err.Error()})
	}

	if err := ValidateDeleteSleepSeconds(processing.DeleteSleepSeconds); err != nil {
		errors = append(errors, ValidationError{Field: prefix + ".delete_sleep_seconds", Message: err.Error()})
	}

	return errors
}

// The per-value validators below judge one setting's value, and are the single rule that the
// file and a runtime setter share (TEL-1, spec §4.5 *Validation*). The shape every setting
// follows: one function ValidateX(v T) error per setting; the message names the setting by its
// bare key; the file wraps it as ValidationError{Field: prefix + ".key", Message: err.Error()},
// and a setter reports the same text. A value out of range is refused, never clamped.

// ValidateSleepSeconds judges a sleep_seconds value (SLP-1, D14): not negative, finite, and
// small enough to be a time.Duration.
func ValidateSleepSeconds(v float64) error { return validateSleep("sleep_seconds", v) }

// ValidateDeleteSleepSeconds judges a delete_sleep_seconds value by the same rule.
func ValidateDeleteSleepSeconds(v float64) error { return validateSleep("delete_sleep_seconds", v) }

// maxDurationNanoseconds is the largest duration, math.MaxInt64 nanoseconds, as a float64: the
// integer is not representable and rounds up to 2^63, so a converted value must be strictly
// below it to fit.
const maxDurationNanoseconds = float64(1 << 63)

// validateSleep refuses a negative sleep, a NaN or infinite one, and one whose value in
// nanoseconds does not fit a duration. The last test is made on the converted value, with the
// expression the engine converts a sleep by (s * float64(time.Second)), not on a constant in
// seconds: a float just under 2^63/1e9 can still round up to 2^63 when multiplied, and what is
// accepted here must convert in range. No cap below the largest duration is chosen.
func validateSleep(key string, v float64) error {
	switch {
	case v < 0:
		return fmt.Errorf("%s cannot be negative", key)
	case math.IsNaN(v) || math.IsInf(v, 0):
		return fmt.Errorf("%s must be a finite number", key)
	case v*float64(time.Second) >= maxDurationNanoseconds:
		return fmt.Errorf("%s is too large: a sleep must fit a duration (less than 9.2e9 seconds)", key)
	}
	return nil
}

// validateSafety checks the safety block: disable_foreign_key_checks, a bool with nothing
// to reject. The removed 2.0 keys (lag_threshold, check_interval) are unknown keys to
// jobspec, which refuses them with the upgrade pointer.
func (c *Config) validateSafety() ValidationErrors {
	// disable_foreign_key_checks is a bool; there is nothing left to reject.
	return nil
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
