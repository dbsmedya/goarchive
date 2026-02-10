package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

// Version information (set via ldflags at build time)
var (
	Version = "0.0.1-dev"
	Commit  = "unknown"
)

// CLI flags that override config file values
var (
	cfgFile         string
	logLevel        string
	logFormat       string
	batchSize       int
	batchDeleteSize int
	sleepSeconds    float64
	skipVerify      bool
)

var rootCmd = &cobra.Command{
	Use:   "goarchive",
	Short: "MySQL Batch Archiver & Purger",
	Long: `A production-grade CLI tool for safely archiving MySQL relational data
across servers with automatic dependency resolution and crash recovery.

Features:
  - Automatic table dependency resolution using Kahn's algorithm
  - Transactional batch processing with configurable sizes
  - Crash recovery via checkpoint logging
  - Replication lag monitoring
  - Data verification (count and SHA256)`,
	Version: Version,
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

	// Processing overrides
	rootCmd.PersistentFlags().IntVar(&batchSize, "batch-size", 0,
		"Override batch size (number of root IDs per batch)")
	rootCmd.PersistentFlags().IntVar(&batchDeleteSize, "batch-delete-size", 0,
		"Override batch delete size (rows per DELETE statement)")
	rootCmd.PersistentFlags().Float64Var(&sleepSeconds, "sleep", 0,
		"Override sleep seconds between batches")

	// Safety overrides
	rootCmd.PersistentFlags().BoolVar(&skipVerify, "skip-verify", false,
		"Skip data verification after copy")
}

// GetConfigFile returns the config file path
func GetConfigFile() string {
	return cfgFile
}

// CLIOverrides contains flag values that override config file settings
type CLIOverrides struct {
	LogLevel        string
	LogFormat       string
	BatchSize       int
	BatchDeleteSize int
	SleepSeconds    float64
	SkipVerify      bool
}

// GetCLIOverrides returns the CLI flag override values
func GetCLIOverrides() CLIOverrides {
	return CLIOverrides{
		LogLevel:        logLevel,
		LogFormat:       logFormat,
		BatchSize:       batchSize,
		BatchDeleteSize: batchDeleteSize,
		SleepSeconds:    sleepSeconds,
		SkipVerify:      skipVerify,
	}
}
