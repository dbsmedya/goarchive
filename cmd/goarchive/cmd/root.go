package cmd

import (
	"fmt"
	"os"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
	"github.com/spf13/cobra"
)

// Version information (set via ldflags at build time)
var (
	Version = "3.0.0-beta.1"
	Commit  = "unknown"
)

// CLI flags that override config file values
var (
	cfgFile   string
	logLevel  string
	logFormat string
)

var rootCmd = &cobra.Command{
	Use:   "goarchive",
	Short: "MySQL Batch Archiver & Purger",
	Long: `A CLI tool for safely archiving MySQL relational data across servers
with automatic dependency resolution and crash recovery.
Recommended for archival of cold data. Use with caution on
very large or deeply nested schemas;
see README and LIMITATIONS for known limits

Features:
  - Parent/child trees from the job's declared relations, with composite and
    non-integer keys
  - Key-ordered batches: copy, verification and the log write in one destination
    transaction; deletes in chunks committed only on an exact row count
  - Crash recovery by replaying the job's log
  - Replication lag gate before every delete
  - Verification by SHA256 (default) or count`,
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
}

// effectiveJobLogging resolves the logging config for a job run with
// precedence: CLI flags > job logging block > global logging.
func effectiveJobLogging(global config.LoggingConfig, spec *jobspec.Spec, overrides CLIOverrides) config.LoggingConfig {
	logCfg := spec.EffectiveLogging(global)
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
func newJobLogger(doc *jobspec.Document, spec *jobspec.Spec, jobName string) (*logger.Logger, error) {
	logCfg := effectiveJobLogging(doc.Shared.Logging, spec, GetCLIOverrides())
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
	LogLevel  string
	LogFormat string
}

// GetCLIOverrides returns the CLI flag override values
func GetCLIOverrides() CLIOverrides {
	return CLIOverrides{LogLevel: logLevel, LogFormat: logFormat}
}
