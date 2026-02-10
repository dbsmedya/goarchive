// Package config provides configuration structures and loading for GoArchive.
package config

// Config represents the complete application configuration.
type Config struct {
	Source       DatabaseConfig            `yaml:"source" mapstructure:"source"`
	Destination  DatabaseConfig            `yaml:"destination" mapstructure:"destination"`
	Replica      ReplicaConfig             `yaml:"replica" mapstructure:"replica"`
	Jobs         map[string]JobConfig      `yaml:"jobs" mapstructure:"jobs"`
	Processing   ProcessingConfig          `yaml:"processing" mapstructure:"processing"`
	Safety       SafetyConfig              `yaml:"safety" mapstructure:"safety"`
	Verification VerificationConfig        `yaml:"verification" mapstructure:"verification"`
	Logging      LoggingConfig             `yaml:"logging" mapstructure:"logging"`
}

// DatabaseConfig represents a MySQL database connection configuration.
type DatabaseConfig struct {
	Host               string `yaml:"host" mapstructure:"host"`
	Port               int    `yaml:"port" mapstructure:"port"`
	User               string `yaml:"user" mapstructure:"user"`
	Password           string `yaml:"password" mapstructure:"password"`
	Database           string `yaml:"database" mapstructure:"database"`
	TLS                string `yaml:"tls" mapstructure:"tls"` // disable, preferred, required
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
}

// JobConfig represents an archive job configuration.
type JobConfig struct {
	RootTable    string              `yaml:"root_table" mapstructure:"root_table"`
	PrimaryKey   string              `yaml:"primary_key" mapstructure:"primary_key"`
	Where        string              `yaml:"where" mapstructure:"where"`
	Relations    []Relation          `yaml:"relations" mapstructure:"relations"`
	Processing   *ProcessingConfig   `yaml:"processing,omitempty" mapstructure:"processing"`
	Verification *VerificationConfig `yaml:"verification,omitempty" mapstructure:"verification"`
}

// Relation represents a table relationship for dependency resolution.
type Relation struct {
	Table          string     `yaml:"table" mapstructure:"table"`
	PrimaryKey     string     `yaml:"primary_key" mapstructure:"primary_key"`         // PK column name (defaults to "id")
	ForeignKey     string     `yaml:"foreign_key" mapstructure:"foreign_key"`
	DependencyType string     `yaml:"dependency_type" mapstructure:"dependency_type"` // "1-1" or "1-N"
	Relations      []Relation `yaml:"relations" mapstructure:"relations"`             // Nested relations
}

// ProcessingConfig represents batch processing settings.
type ProcessingConfig struct {
	BatchSize       int     `yaml:"batch_size" mapstructure:"batch_size"`
	BatchDeleteSize int     `yaml:"batch_delete_size" mapstructure:"batch_delete_size"`
	SleepSeconds    float64 `yaml:"sleep_seconds" mapstructure:"sleep_seconds"`
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

// LoggingConfig represents logging settings.
type LoggingConfig struct {
	Level  string `yaml:"level" mapstructure:"level"`   // debug, info, warn, error
	Format string `yaml:"format" mapstructure:"format"` // json or text
	Output string `yaml:"output" mapstructure:"output"` // stdout, stderr, or file path
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

// GetJobProcessing returns the processing config for a job, falling back to global if not set.
func (jc *JobConfig) GetJobProcessing(global ProcessingConfig) ProcessingConfig {
	if jc.Processing == nil {
		return global
	}
	
	// Merge job-specific with global defaults
	result := global
	if jc.Processing.BatchSize > 0 {
		result.BatchSize = jc.Processing.BatchSize
	}
	if jc.Processing.BatchDeleteSize > 0 {
		result.BatchDeleteSize = jc.Processing.BatchDeleteSize
	}
	if jc.Processing.SleepSeconds > 0 {
		result.SleepSeconds = jc.Processing.SleepSeconds
	}
	return result
}

// GetJobVerification returns the verification config for a job, falling back to global if not set.
func (jc *JobConfig) GetJobVerification(global VerificationConfig) VerificationConfig {
	if jc.Verification == nil {
		return global
	}
	
	// Merge job-specific with global defaults
	result := global
	if jc.Verification.Method != "" {
		result.Method = jc.Verification.Method
	}
	result.SkipVerification = jc.Verification.SkipVerification || global.SkipVerification
	return result
}
