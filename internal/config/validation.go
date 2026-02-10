package config

import (
	"fmt"
	"strings"
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

	// Validate replica if enabled
	if c.Replica.Enabled {
		if err := c.validateReplica(); err != nil {
			errors = append(errors, err...)
		}
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

	validTLS := map[string]bool{"disable": true, "preferred": true, "required": true, "": true}
	if !validTLS[db.TLS] {
		errors = append(errors, ValidationError{
			Field:   prefix + ".tls",
			Message: "tls must be 'disable', 'preferred', or 'required'",
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

	return errors
}

func (c *Config) validateReplica() ValidationErrors {
	var errors ValidationErrors

	if c.Replica.Host == "" {
		errors = append(errors, ValidationError{
			Field:   "replica.host",
			Message: "host is required when replica is enabled",
		})
	}

	if c.Replica.Port <= 0 || c.Replica.Port > 65535 {
		errors = append(errors, ValidationError{
			Field:   "replica.port",
			Message: "port must be between 1 and 65535",
		})
	}

	if c.Replica.User == "" {
		errors = append(errors, ValidationError{
			Field:   "replica.user",
			Message: "user is required when replica is enabled",
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
	}

	if job.PrimaryKey == "" {
		errors = append(errors, ValidationError{
			Field:   prefix + ".primary_key",
			Message: "primary_key is required",
		})
	}

	// Validate relations recursively
	for i, rel := range job.Relations {
		relPrefix := fmt.Sprintf("%s.relations[%d]", prefix, i)
		if err := c.validateRelation(relPrefix, &rel); err != nil {
			errors = append(errors, err...)
		}
	}

	return errors
}

func (c *Config) validateRelation(prefix string, rel *Relation) ValidationErrors {
	var errors ValidationErrors

	if rel.Table == "" {
		errors = append(errors, ValidationError{
			Field:   prefix + ".table",
			Message: "table name is required",
		})
	}

	if rel.ForeignKey == "" {
		errors = append(errors, ValidationError{
			Field:   prefix + ".foreign_key",
			Message: "foreign_key is required",
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
		if err := c.validateRelation(nestedPrefix, &nested); err != nil {
			errors = append(errors, err...)
		}
	}

	return errors
}

func (c *Config) validateProcessing() ValidationErrors {
	var errors ValidationErrors

	if c.Processing.BatchSize <= 0 {
		errors = append(errors, ValidationError{
			Field:   "processing.batch_size",
			Message: "batch_size must be positive",
		})
	}

	if c.Processing.BatchDeleteSize <= 0 {
		errors = append(errors, ValidationError{
			Field:   "processing.batch_delete_size",
			Message: "batch_delete_size must be positive",
		})
	}

	if c.Processing.SleepSeconds < 0 {
		errors = append(errors, ValidationError{
			Field:   "processing.sleep_seconds",
			Message: "sleep_seconds cannot be negative",
		})
	}

	return errors
}

func (c *Config) validateSafety() ValidationErrors {
	var errors ValidationErrors

	if c.Safety.LagThreshold < 0 {
		errors = append(errors, ValidationError{
			Field:   "safety.lag_threshold",
			Message: "lag_threshold cannot be negative",
		})
	}

	if c.Safety.CheckInterval < 0 {
		errors = append(errors, ValidationError{
			Field:   "safety.check_interval",
			Message: "check_interval cannot be negative",
		})
	}

	return errors
}

func (c *Config) validateVerification() ValidationErrors {
	var errors ValidationErrors

	validMethods := map[string]bool{"count": true, "sha256": true, "": true}
	if !validMethods[c.Verification.Method] {
		errors = append(errors, ValidationError{
			Field:   "verification.method",
			Message: "method must be 'count' or 'sha256'",
		})
	}

	return errors
}

func (c *Config) validateLogging() ValidationErrors {
	var errors ValidationErrors

	validLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true, "": true}
	if !validLevels[c.Logging.Level] {
		errors = append(errors, ValidationError{
			Field:   "logging.level",
			Message: "level must be 'debug', 'info', 'warn', or 'error'",
		})
	}

	validFormats := map[string]bool{"json": true, "text": true, "": true}
	if !validFormats[c.Logging.Format] {
		errors = append(errors, ValidationError{
			Field:   "logging.format",
			Message: "format must be 'json' or 'text'",
		})
	}

	return errors
}
