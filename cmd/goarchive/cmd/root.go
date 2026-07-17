package cmd

import (
	"fmt"
	"os"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/spf13/cobra"
)

// Version information (set via ldflags at build time)
var (
	Version = "1.7.0-community"
	Commit  = "unknown"
)

// CLI flags that override config file values
var (
	cfgFile    string
	logLevel   string
	logFormat  string
	skipVerify bool
)

var rootCmd = &cobra.Command{
	Use:   "goarchive",
	Short: "MySQL Batch Archiver & Purger",
	Long: `A CLI tool for safely archiving MySQL relational data across servers
with automatic dependency resolution and crash recovery.

Community edition — recommended for single-operator workstation archival of
cold data. Use with caution on very large or deeply nested schemas; see README
for known limits. Enterprise edition (with metrics, parallelism, and load-
tested scaling) is planned separately.

Features:
  - Automatic table dependency resolution using Kahn's algorithm
  - Transactional batch processing with configurable sizes
  - Crash recovery via checkpoint logging
  - Replication lag monitoring
  - Data verification (count and SHA256)`,
	Version: Version,
	// Errors from RunE are already reported with command-specific context;
	// Cobra's usage block would only add noise after the error.
	SilenceUsage: true,
}

// Execute runs the root command
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	// Config file flag
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "archiver.yaml",
		"Path to configuration file")

	// Logging overrides
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "",
		"Override log level (debug, info, warn, error)")
	rootCmd.PersistentFlags().StringVar(&logFormat, "log-format", "",
		"Override log format (json, text)")

	// Safety overrides
	rootCmd.PersistentFlags().BoolVar(&skipVerify, "skip-verify", false,
		"Skip data verification after copy")
}

// effectiveJobLogging resolves the logging config for a job run with
// precedence: CLI flags > job logging block > global logging.
func effectiveJobLogging(cfg *config.Config, jobCfg *config.JobConfig, overrides CLIOverrides) config.LoggingConfig {
	logCfg := cfg.Logging
	if jobCfg != nil {
		logCfg = jobCfg.GetJobLogging(cfg.Logging)
	}
	if overrides.LogLevel != "" {
		logCfg.Level = overrides.LogLevel
	}
	if overrides.LogFormat != "" {
		logCfg.Format = overrides.LogFormat
	}
	return logCfg
}

// newJobLogger builds the logger for a job command from the effective
// per-job logging configuration. Every entry is tagged with the job name
// so runs remain attributable when jobs share an output.
func newJobLogger(cfg *config.Config, jobCfg *config.JobConfig, jobName string) (*logger.Logger, error) {
	logCfg := effectiveJobLogging(cfg, jobCfg, GetCLIOverrides())
	log, err := logger.New(&logCfg)
	if err != nil {
		return nil, err
	}
	return log.WithJob(jobName), nil
}

func syncLogger(log *logger.Logger) {
	if log == nil {
		return
	}
	if err := log.Close(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "warning: failed to sync logger: %v\n", err)
	}
}

// GetConfigFile returns the config file path
func GetConfigFile() string {
	return cfgFile
}

// CLIOverrides contains flag values that override config file settings
type CLIOverrides struct {
	LogLevel   string
	LogFormat  string
	SkipVerify bool
}

// GetCLIOverrides returns the CLI flag override values
func GetCLIOverrides() CLIOverrides {
	return CLIOverrides{
		LogLevel:   logLevel,
		LogFormat:  logFormat,
		SkipVerify: skipVerify,
	}
}
