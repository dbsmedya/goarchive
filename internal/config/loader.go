package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/spf13/viper"
)

// Load reads configuration from the specified file path.
// It supports YAML files and performs environment variable substitution.
func Load(configPath string) (*Config, error) {
	v := viper.New()

	v.SetConfigFile(configPath)
	v.SetConfigType("yaml")

	// Read the config file
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Start with defaults
	cfg := DefaultConfig()

	// Unmarshal into config struct
	if err := v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Perform environment variable substitution
	if err := substituteEnvVars(cfg); err != nil {
		return nil, fmt.Errorf("failed to substitute environment variables: %w", err)
	}

	return cfg, nil
}

// LoadFromViper creates a Config from an existing Viper instance.
// Useful for testing or when Viper is configured externally.
func LoadFromViper(v *viper.Viper) (*Config, error) {
	cfg := DefaultConfig()

	if err := v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if err := substituteEnvVars(cfg); err != nil {
		return nil, fmt.Errorf("failed to substitute environment variables: %w", err)
	}

	return cfg, nil
}

// envVarPattern matches ${VAR_NAME} or $VAR_NAME patterns
var envVarPattern = regexp.MustCompile(`\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)`)

// substituteEnvVars replaces ${VAR_NAME} patterns with environment variable values.
func substituteEnvVars(cfg *Config) error {
	// Substitute in source config
	cfg.Source.Host = expandEnvVar(cfg.Source.Host)
	cfg.Source.User = expandEnvVar(cfg.Source.User)
	cfg.Source.Password = expandEnvVar(cfg.Source.Password)
	cfg.Source.Database = expandEnvVar(cfg.Source.Database)

	// Substitute in destination config
	cfg.Destination.Host = expandEnvVar(cfg.Destination.Host)
	cfg.Destination.User = expandEnvVar(cfg.Destination.User)
	cfg.Destination.Password = expandEnvVar(cfg.Destination.Password)
	cfg.Destination.Database = expandEnvVar(cfg.Destination.Database)

	// Substitute in replica config
	cfg.Replica.Host = expandEnvVar(cfg.Replica.Host)
	cfg.Replica.User = expandEnvVar(cfg.Replica.User)
	cfg.Replica.Password = expandEnvVar(cfg.Replica.Password)

	// Substitute in logging config
	cfg.Logging.Output = expandEnvVar(cfg.Logging.Output)

	return nil
}

// expandEnvVar expands environment variables in the format ${VAR} or $VAR.
func expandEnvVar(s string) string {
	return envVarPattern.ReplaceAllStringFunc(s, func(match string) string {
		var varName string
		if strings.HasPrefix(match, "${") {
			varName = match[2 : len(match)-1]
		} else {
			varName = match[1:]
		}

		if value, exists := os.LookupEnv(varName); exists {
			return value
		}
		// Return original if env var not found
		return match
	})
}

// GetJob retrieves a specific job configuration by name.
func (c *Config) GetJob(name string) (*JobConfig, error) {
	job, exists := c.Jobs[name]
	if !exists {
		return nil, fmt.Errorf("job %q not found in configuration", name)
	}
	return &job, nil
}

// ListJobs returns all job names defined in the configuration.
func (c *Config) ListJobs() []string {
	jobs := make([]string, 0, len(c.Jobs))
	for name := range c.Jobs {
		jobs = append(jobs, name)
	}
	return jobs
}

// ApplyOverrides applies CLI flag overrides to the global configuration.
// Only non-zero/non-empty values are applied.
func (c *Config) ApplyOverrides(logLevel, logFormat string, batchSize, batchDeleteSize int, sleepSeconds float64, skipVerify bool) {
	if logLevel != "" {
		c.Logging.Level = logLevel
	}
	if logFormat != "" {
		c.Logging.Format = logFormat
	}
	if batchSize > 0 {
		c.Processing.BatchSize = batchSize
	}
	if batchDeleteSize > 0 {
		c.Processing.BatchDeleteSize = batchDeleteSize
	}
	if sleepSeconds > 0 {
		c.Processing.SleepSeconds = sleepSeconds
	}
	if skipVerify {
		c.Verification.SkipVerification = true
	}
}

// ApplyJobOverrides applies CLI flag overrides to a specific job's configuration.
// This creates a modified ProcessingConfig that combines global, job-specific, and CLI values.
func (c *Config) ApplyJobOverrides(jobName string, batchSize, batchDeleteSize int, sleepSeconds float64, skipVerify bool) ProcessingConfig {
	processing := c.GetJobProcessing(jobName)

	if batchSize > 0 {
		processing.BatchSize = batchSize
	}
	if batchDeleteSize > 0 {
		processing.BatchDeleteSize = batchDeleteSize
	}
	if sleepSeconds > 0 {
		processing.SleepSeconds = sleepSeconds
	}

	return processing
}
