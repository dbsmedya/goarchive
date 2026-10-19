// Package config provides configuration structures and loading for GoArchive.
package config

import (
	"net"
	"strconv"
)

// Config represents the complete application configuration.
type Config struct {
	Source      DatabaseConfig    `yaml:"source"`
	Destination DatabaseConfig    `yaml:"destination"`
	Replication ReplicationConfig `yaml:"replication"`
	Processing  ProcessingConfig  `yaml:"processing"`
	Safety      SafetyConfig      `yaml:"safety"`
	Logging     LoggingConfig     `yaml:"logging"`
}

// DatabaseConfig represents a MySQL database connection configuration.
type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	// JobSchema is the schema holding GoArchive's tracking tables
	// (archiver_job, archiver_job_log_<id>, goarchive_meta). Only the
	// destination's is used. Empty (default) resolves to
	// Database. A DBA must pre-create this schema and grant CREATE + CRUD.
	JobSchema          string `yaml:"job_schema"`
	TLS                string `yaml:"tls"` // disable, preferred, skip-verify, required
	MaxConnections     int    `yaml:"max_connections"`
	MaxIdleConnections int    `yaml:"max_idle_connections"`
}

// ReplicationConfig configures the replication gate: the fleet of replicas to
// monitor and the tolerance, cadence and cache lifetime applied to them.
type ReplicationConfig struct {
	Enabled                   bool                      `yaml:"enabled"`
	SecondsBehindSourceWithin int                       `yaml:"seconds_behind_source_within"`
	CheckInterval             int                       `yaml:"check_interval"`
	CacheTTL                  int                       `yaml:"cache_ttl"`
	Servers                   []ReplicationServerConfig `yaml:"servers"`
}

// ReplicationServerConfig is one monitored replica. Channels selects which
// replication channels are gated: an empty slice means all channels, while a
// slice containing "" means the default (unnamed) channel only.
type ReplicationServerConfig struct {
	Host     string   `yaml:"host"`
	Port     int      `yaml:"port"`
	User     string   `yaml:"user"`
	Password string   `yaml:"password"`
	TLS      string   `yaml:"tls"`
	Type     string   `yaml:"type"`
	Channels []string `yaml:"channels"`
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
	BatchSize          *int     `yaml:"batch_size,omitempty"`
	BatchDeleteSize    *int     `yaml:"batch_delete_size,omitempty"`
	SleepSeconds       *float64 `yaml:"sleep_seconds,omitempty"`
	DeleteSleepSeconds *float64 `yaml:"delete_sleep_seconds,omitempty"`
	SentinelFile       *string  `yaml:"sentinel_file,omitempty"`
}

// ProcessingConfig represents batch processing settings.
type ProcessingConfig struct {
	BatchSize       int     `yaml:"batch_size"`
	BatchDeleteSize int     `yaml:"batch_delete_size"`
	SleepSeconds    float64 `yaml:"sleep_seconds"`
	// DeleteSleepSeconds throttles the delete phase by pausing between delete
	// chunks (every batch_delete_size rows) to limit binlog generation and
	// replication lag. 0 (default) disables the throttle.
	DeleteSleepSeconds float64 `yaml:"delete_sleep_seconds"`
	// SentinelFile, when set, is an operator pause switch. Before each batch, if
	// this file exists, processing pauses and re-checks every second until the
	// file is removed. Empty (default) disables the pause switch.
	SentinelFile string `yaml:"sentinel_file"`
}

// SafetyConfig represents safety settings for archive operations.
type SafetyConfig struct {
	DisableForeignKeyChecks bool `yaml:"disable_foreign_key_checks"`
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
	Level    string `yaml:"level"`     // debug, info, warn, error
	Format   string `yaml:"format"`    // json or text
	Output   string `yaml:"output"`    // stdout, stderr, or file path
	FileOnly bool   `yaml:"file_only"` // no console tee under --progress (stderr) when output is a file
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
