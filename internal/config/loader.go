package config

import (
	"fmt"

	"github.com/spf13/viper"
)

// Load reads configuration from the specified file path.
// It supports YAML files only.
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

	return cfg, nil
}

// LoadFromViper creates a Config from an existing Viper instance.
// Useful for testing or when Viper is configured externally.
func LoadFromViper(v *viper.Viper) (*Config, error) {
	cfg := DefaultConfig()

	if err := v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return cfg, nil
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
