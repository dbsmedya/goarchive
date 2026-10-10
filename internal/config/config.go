// Package config provides configuration structures and loading for GoArchive.
package config

import (
	"net"
	"strconv"
)

// Config represents the complete application configuration.
type Config struct {
	Source      DatabaseConfig    `yaml:"source" mapstructure:"source"`
	Destination DatabaseConfig    `yaml:"destination" mapstructure:"destination"`
	Replication ReplicationConfig `yaml:"replication" mapstructure:"replication"`
	Processing  ProcessingConfig  `yaml:"processing" mapstructure:"processing"`
	Safety      SafetyConfig      `yaml:"safety" mapstructure:"safety"`
	Logging     LoggingConfig     `yaml:"logging" mapstructure:"logging"`
}

// DatabaseConfig represents a MySQL database connection configuration.
type DatabaseConfig struct {
	Host     string `yaml:"host" mapstructure:"host"`
	Port     int    `yaml:"port" mapstructure:"port"`
	User     string `yaml:"user" mapstructure:"user"`
	Password string `yaml:"password" mapstructure:"password"`
	Database string `yaml:"database" mapstructure:"database"`
	// JobSchema is the schema holding GoArchive's tracking tables
	// (archiver_job, archiver_job_log_<id>). Empty (default) resolves to
	// Database. A DBA must pre-create this schema and grant CREATE + CRUD.
	JobSchema          string `yaml:"job_schema" mapstructure:"job_schema"`
	TLS                string `yaml:"tls" mapstructure:"tls"` // disable, preferred, skip-verify, required
	MaxConnections     int    `yaml:"max_connections" mapstructure:"max_connections"`
	MaxIdleConnections int    `yaml:"max_idle_connections" mapstructure:"max_idle_connections"`
}

// ReplicationConfig configures the replication gate: the fleet of replicas to
// monitor and the tolerance, cadence and cache lifetime applied to them.
type ReplicationConfig struct {
	Enabled                   bool                      `yaml:"enabled" mapstructure:"enabled"`
	SecondsBehindSourceWithin int                       `yaml:"seconds_behind_source_within" mapstructure:"seconds_behind_source_within"`
	CheckInterval             int                       `yaml:"check_interval" mapstructure:"check_interval"`
	CacheTTL                  int                       `yaml:"cache_ttl" mapstructure:"cache_ttl"`
	Servers                   []ReplicationServerConfig `yaml:"servers" mapstructure:"servers"`
}

// ReplicationServerConfig is one monitored replica. Channels selects which
// replication channels are gated: an empty slice means all channels, while a
// slice containing "" means the default (unnamed) channel only.
type ReplicationServerConfig struct {
	Host     string   `yaml:"host" mapstructure:"host"`
	Port     int      `yaml:"port" mapstructure:"port"`
	User     string   `yaml:"user" mapstructure:"user"`
	Password string   `yaml:"password" mapstructure:"password"`
	TLS      string   `yaml:"tls" mapstructure:"tls"`
	Type     string   `yaml:"type" mapstructure:"type"`
	Channels []string `yaml:"channels" mapstructure:"channels"`
}

// Addr returns the IPv6-safe "host:port" identity used in logs and pairing.
func (s ReplicationServerConfig) Addr() string {
	return net.JoinHostPort(s.Host, strconv.Itoa(s.Port))
}

// normalizeReplication applies per-server defaults that struct-level defaults
// cannot reach: YAML-created slice elements arrive zero-valued (loader.go).
// Channels is deliberately untouched — [""] (default channel only) must stay
// distinct from [] (all channels).
func (c *Config) normalizeReplication() {
	for i := range c.Replication.Servers {
		s := &c.Replication.Servers[i]
		if s.Port == 0 {
			s.Port = 3306
		}
		if s.TLS == "" {
			s.TLS = "preferred"
		}
		if s.Type == "" {
			s.Type = "async"
		}
	}
}

// ProcessingOverrides is the per-job processing block. Pointer fields
// distinguish "not set — inherit global" (nil) from an explicit value, so a
// job can set sleep_seconds: 0 to disable a global sleep.
type ProcessingOverrides struct {
	BatchSize          *int     `yaml:"batch_size,omitempty" mapstructure:"batch_size"`
	BatchDeleteSize    *int     `yaml:"batch_delete_size,omitempty" mapstructure:"batch_delete_size"`
	SleepSeconds       *float64 `yaml:"sleep_seconds,omitempty" mapstructure:"sleep_seconds"`
	DeleteSleepSeconds *float64 `yaml:"delete_sleep_seconds,omitempty" mapstructure:"delete_sleep_seconds"`
	SentinelFile       *string  `yaml:"sentinel_file,omitempty" mapstructure:"sentinel_file"`
}

// ProcessingConfig represents batch processing settings.
type ProcessingConfig struct {
	BatchSize       int     `yaml:"batch_size" mapstructure:"batch_size"`
	BatchDeleteSize int     `yaml:"batch_delete_size" mapstructure:"batch_delete_size"`
	SleepSeconds    float64 `yaml:"sleep_seconds" mapstructure:"sleep_seconds"`
	// DeleteSleepSeconds throttles the delete phase by pausing between delete
	// chunks (every batch_delete_size rows) to limit binlog generation and
	// replication lag. 0 (default) disables the throttle.
	DeleteSleepSeconds float64 `yaml:"delete_sleep_seconds" mapstructure:"delete_sleep_seconds"`
	// SentinelFile, when set, is an operator pause switch. Before each batch, if
	// this file exists, processing pauses and re-checks every second until the
	// file is removed. Empty (default) disables the pause switch.
	SentinelFile string `yaml:"sentinel_file" mapstructure:"sentinel_file"`
}

// SafetyConfig represents safety settings for archive operations.
type SafetyConfig struct {
	DisableForeignKeyChecks bool `yaml:"disable_foreign_key_checks" mapstructure:"disable_foreign_key_checks"`
}

// EffectiveJobSchema returns the schema for tracking tables, defaulting to the
// connection's Database when job_schema is unset. It is intended for use with
// a validated destination config where Database is guaranteed non-empty; on an
// unconfigured (zero-value) DatabaseConfig it returns "".
func (d DatabaseConfig) EffectiveJobSchema() string {
	if d.JobSchema != "" {
		return d.JobSchema
	}
	return d.Database
}

// LoggingConfig represents logging settings.
type LoggingConfig struct {
	Level    string `yaml:"level" mapstructure:"level"`         // debug, info, warn, error
	Format   string `yaml:"format" mapstructure:"format"`       // json or text
	Output   string `yaml:"output" mapstructure:"output"`       // stdout, stderr, or file path
	FileOnly bool   `yaml:"file_only" mapstructure:"file_only"` // suppress stdout tee when output is a file
}

// DefaultConfig returns a Config with sensible default values.
func DefaultConfig() *Config {
	return &Config{
		Source: DatabaseConfig{
			Port:               3306,
			TLS:                "preferred",
			MaxConnections:     10,
			MaxIdleConnections: 5,
		},
		Destination: DatabaseConfig{
			Port:               3306,
			TLS:                "preferred",
			MaxConnections:     10,
			MaxIdleConnections: 5,
		},
		Replication: ReplicationConfig{
			Enabled:                   false,
			SecondsBehindSourceWithin: 10,
			CheckInterval:             5,
			CacheTTL:                  15,
		},
		Processing: ProcessingConfig{
			BatchSize:       1000,
			BatchDeleteSize: 500,
			SleepSeconds:    1,
		},
		Safety: SafetyConfig{
			DisableForeignKeyChecks: false,
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "json",
			Output: "stdout",
		},
	}
}

// Apply merges these per-job overrides over global. A nil pointer inherits the global value;
// an explicit value, zero included, wins. A nil receiver returns global.
func (o *ProcessingOverrides) Apply(global ProcessingConfig) ProcessingConfig {
	if o == nil {
		return global
	}
	result := global
	if o.BatchSize != nil {
		result.BatchSize = *o.BatchSize
	}
	if o.BatchDeleteSize != nil {
		result.BatchDeleteSize = *o.BatchDeleteSize
	}
	if o.SleepSeconds != nil {
		result.SleepSeconds = *o.SleepSeconds
	}
	if o.DeleteSleepSeconds != nil {
		result.DeleteSleepSeconds = *o.DeleteSleepSeconds
	}
	if o.SentinelFile != nil {
		result.SentinelFile = *o.SentinelFile
	}
	return result
}
