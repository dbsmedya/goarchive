// Package config provides configuration structures and loading for GoArchive.
package config

// Config represents the complete application configuration.
type Config struct {
	Source       DatabaseConfig       `yaml:"source" mapstructure:"source"`
	Destination  DatabaseConfig       `yaml:"destination" mapstructure:"destination"`
	Replica      ReplicaConfig        `yaml:"replica" mapstructure:"replica"`
	Jobs         map[string]JobConfig `yaml:"jobs" mapstructure:"jobs"`
	Processing   ProcessingConfig     `yaml:"processing" mapstructure:"processing"`
	Safety       SafetyConfig         `yaml:"safety" mapstructure:"safety"`
	Verification VerificationConfig   `yaml:"verification" mapstructure:"verification"`
	Logging      LoggingConfig        `yaml:"logging" mapstructure:"logging"`
}

// DatabaseConfig represents a MySQL database connection configuration.
type DatabaseConfig struct {
	Host               string `yaml:"host" mapstructure:"host"`
	Port               int    `yaml:"port" mapstructure:"port"`
	User               string `yaml:"user" mapstructure:"user"`
	Password           string `yaml:"password" mapstructure:"password"`
	Database           string `yaml:"database" mapstructure:"database"`
	TLS                string `yaml:"tls" mapstructure:"tls"` // disable, preferred, skip-verify, required
	MaxConnections     int    `yaml:"max_connections" mapstructure:"max_connections"`
	MaxIdleConnections int    `yaml:"max_idle_connections" mapstructure:"max_idle_connections"`
}

// ReplicaConfig represents the replica database for replication lag monitoring.
type ReplicaConfig struct {
	Enabled  bool   `yaml:"enabled" mapstructure:"enabled"`
	Host     string `yaml:"host" mapstructure:"host"`
	Port     int    `yaml:"port" mapstructure:"port"`
	User     string `yaml:"user" mapstructure:"user"`
	Password string `yaml:"password" mapstructure:"password"`
	// ReplicationChannel scopes lag checks to a named replication channel via
	// SHOW REPLICA STATUS FOR CHANNEL '<name>'. Empty (default) queries the
	// default/unnamed channel.
	ReplicationChannel string `yaml:"replication_channel" mapstructure:"replication_channel"`
}

// JobConfig represents an archive job configuration.
type JobConfig struct {
	RootTable    string                 `yaml:"root_table" mapstructure:"root_table"`
	PrimaryKey   string                 `yaml:"primary_key" mapstructure:"primary_key"`
	Where        string                 `yaml:"where" mapstructure:"where"`
	Relations    []Relation             `yaml:"relations" mapstructure:"relations"`
	Processing   *ProcessingOverrides   `yaml:"processing,omitempty" mapstructure:"processing"`
	Verification *VerificationOverrides `yaml:"verification,omitempty" mapstructure:"verification"`
	Logging      *LoggingConfig         `yaml:"logging,omitempty" mapstructure:"logging"`
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

// VerificationOverrides is the per-job verification block.
type VerificationOverrides struct {
	Method           string `yaml:"method,omitempty" mapstructure:"method"`
	SkipVerification *bool  `yaml:"skip_verification,omitempty" mapstructure:"skip_verification"`
}

// Relation represents a table relationship for dependency resolution.
type Relation struct {
	Table          string     `yaml:"table" mapstructure:"table"`
	PrimaryKey     string     `yaml:"primary_key" mapstructure:"primary_key"` // PK column name (required)
	ForeignKey     string     `yaml:"foreign_key" mapstructure:"foreign_key"`
	DependencyType string     `yaml:"dependency_type" mapstructure:"dependency_type"` // "1-1" or "1-N"
	Relations      []Relation `yaml:"relations" mapstructure:"relations"`             // Nested relations
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
	LagThreshold            int  `yaml:"lag_threshold" mapstructure:"lag_threshold"`
	CheckInterval           int  `yaml:"check_interval" mapstructure:"check_interval"`
	DisableForeignKeyChecks bool `yaml:"disable_foreign_key_checks" mapstructure:"disable_foreign_key_checks"`
}

// VerificationConfig represents data verification settings.
type VerificationConfig struct {
	Method           string `yaml:"method" mapstructure:"method"` // "count" or "sha256"
	SkipVerification bool   `yaml:"skip_verification" mapstructure:"skip_verification"`
}

// EffectiveMethod returns the verifier method after applying defaults.
func (v VerificationConfig) EffectiveMethod() string {
	if v.Method == "" {
		return "count"
	}
	return v.Method
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
		Replica: ReplicaConfig{
			Enabled: false,
			Port:    3306,
		},
		Processing: ProcessingConfig{
			BatchSize:       1000,
			BatchDeleteSize: 500,
			SleepSeconds:    1,
		},
		Safety: SafetyConfig{
			LagThreshold:            10,
			CheckInterval:           5,
			DisableForeignKeyChecks: false,
		},
		Verification: VerificationConfig{
			Method:           "count",
			SkipVerification: false,
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "json",
			Output: "stdout",
		},
	}
}

// GetJobProcessing returns the processing config for a job by name, falling back to global if not set.
func (c *Config) GetJobProcessing(jobName string) ProcessingConfig {
	job, err := c.GetJob(jobName)
	if err != nil {
		return c.Processing
	}
	return job.GetJobProcessing(c.Processing)
}

// GetJobVerification returns the verification config for a job by name, falling back to global if not set.
func (c *Config) GetJobVerification(jobName string) VerificationConfig {
	job, err := c.GetJob(jobName)
	if err != nil {
		return c.Verification
	}
	return job.GetJobVerification(c.Verification)
}

// GetJobProcessing merges the job's overrides over the global config.
// nil pointer = inherit; explicit value (including zero) wins.
func (jc *JobConfig) GetJobProcessing(global ProcessingConfig) ProcessingConfig {
	if jc.Processing == nil {
		return global
	}
	result := global
	if jc.Processing.BatchSize != nil {
		result.BatchSize = *jc.Processing.BatchSize
	}
	if jc.Processing.BatchDeleteSize != nil {
		result.BatchDeleteSize = *jc.Processing.BatchDeleteSize
	}
	if jc.Processing.SleepSeconds != nil {
		result.SleepSeconds = *jc.Processing.SleepSeconds
	}
	if jc.Processing.DeleteSleepSeconds != nil {
		result.DeleteSleepSeconds = *jc.Processing.DeleteSleepSeconds
	}
	if jc.Processing.SentinelFile != nil {
		result.SentinelFile = *jc.Processing.SentinelFile
	}
	return result
}

// GetJobLogging returns the logging config for a job by name, falling back to global if not set.
func (c *Config) GetJobLogging(jobName string) LoggingConfig {
	job, err := c.GetJob(jobName)
	if err != nil {
		return c.Logging
	}
	return job.GetJobLogging(c.Logging)
}

// GetJobLogging returns the logging config for a job, falling back to global if not set.
func (jc *JobConfig) GetJobLogging(global LoggingConfig) LoggingConfig {
	if jc.Logging == nil {
		return global
	}

	// Merge job-specific with global defaults
	result := global
	if jc.Logging.Level != "" {
		result.Level = jc.Logging.Level
	}
	if jc.Logging.Format != "" {
		result.Format = jc.Logging.Format
	}
	if jc.Logging.Output != "" {
		result.Output = jc.Logging.Output
	}
	result.FileOnly = jc.Logging.FileOnly
	return result
}

// GetJobVerification merges the job's overrides over the global config.
func (jc *JobConfig) GetJobVerification(global VerificationConfig) VerificationConfig {
	if jc.Verification == nil {
		return global
	}
	result := global
	if jc.Verification.Method != "" {
		result.Method = jc.Verification.Method
	}
	if jc.Verification.SkipVerification != nil {
		result.SkipVerification = *jc.Verification.SkipVerification
	}
	return result
}
