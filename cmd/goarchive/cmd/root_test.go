package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetConfigFile(t *testing.T) {
	// Save original value and restore after test
	originalCfgFile := cfgFile
	defer func() {
		cfgFile = originalCfgFile
	}()

	tests := []struct {
		name     string
		cfgValue string
		want     string
	}{
		{
			name:     "default config file",
			cfgValue: "",
			want:     "",
		},
		{
			name:     "custom config file",
			cfgValue: "/path/to/custom.yaml",
			want:     "/path/to/custom.yaml",
		},
		{
			name:     "config file with spaces",
			cfgValue: "/path/to/my config.yaml",
			want:     "/path/to/my config.yaml",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfgFile = tt.cfgValue
			got := GetConfigFile()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetCLIOverrides(t *testing.T) {
	// Save original values and restore after test
	originalLogLevel := logLevel
	originalLogFormat := logFormat
	originalBatchSize := batchSize
	originalBatchDeleteSize := batchDeleteSize
	originalSleepSeconds := sleepSeconds
	originalSkipVerify := skipVerify
	defer func() {
		logLevel = originalLogLevel
		logFormat = originalLogFormat
		batchSize = originalBatchSize
		batchDeleteSize = originalBatchDeleteSize
		sleepSeconds = originalSleepSeconds
		skipVerify = originalSkipVerify
	}()

	tests := []struct {
		name            string
		logLevel        string
		logFormat       string
		batchSize       int
		batchDeleteSize int
		sleepSeconds    float64
		skipVerify      bool
		want            CLIOverrides
	}{
		{
			name:            "empty overrides",
			logLevel:        "",
			logFormat:       "",
			batchSize:       0,
			batchDeleteSize: 0,
			sleepSeconds:    0,
			skipVerify:      false,
			want: CLIOverrides{
				LogLevel:        "",
				LogFormat:       "",
				BatchSize:       0,
				BatchDeleteSize: 0,
				SleepSeconds:    0,
				SkipVerify:      false,
			},
		},
		{
			name:            "all overrides set",
			logLevel:        "debug",
			logFormat:       "text",
			batchSize:       500,
			batchDeleteSize: 100,
			sleepSeconds:    2.5,
			skipVerify:      true,
			want: CLIOverrides{
				LogLevel:        "debug",
				LogFormat:       "text",
				BatchSize:       500,
				BatchDeleteSize: 100,
				SleepSeconds:    2.5,
				SkipVerify:      true,
			},
		},
		{
			name:            "partial overrides",
			logLevel:        "warn",
			logFormat:       "",
			batchSize:       1000,
			batchDeleteSize: 0,
			sleepSeconds:    0.5,
			skipVerify:      false,
			want: CLIOverrides{
				LogLevel:        "warn",
				LogFormat:       "",
				BatchSize:       1000,
				BatchDeleteSize: 0,
				SleepSeconds:    0.5,
				SkipVerify:      false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logLevel = tt.logLevel
			logFormat = tt.logFormat
			batchSize = tt.batchSize
			batchDeleteSize = tt.batchDeleteSize
			sleepSeconds = tt.sleepSeconds
			skipVerify = tt.skipVerify

			got := GetCLIOverrides()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRootCommandStructure(t *testing.T) {
	assert.NotNil(t, rootCmd)
	assert.Equal(t, "goarchive", rootCmd.Use)
	assert.NotEmpty(t, rootCmd.Short)
	assert.NotEmpty(t, rootCmd.Long)
	assert.Equal(t, Version, rootCmd.Version)
}

func TestRootCommandPersistentFlags(t *testing.T) {
	flags := rootCmd.PersistentFlags()

	// Test config flag
	configFlag, err := flags.GetString("config")
	assert.NoError(t, err)
	assert.Equal(t, "archiver.yaml", configFlag)

	// Test log-level flag
	logLevelFlag, err := flags.GetString("log-level")
	assert.NoError(t, err)
	assert.Equal(t, "", logLevelFlag)

	// Test log-format flag
	logFormatFlag, err := flags.GetString("log-format")
	assert.NoError(t, err)
	assert.Equal(t, "", logFormatFlag)

	// Test batch-size flag
	batchSizeFlag, err := flags.GetInt("batch-size")
	assert.NoError(t, err)
	assert.Equal(t, 0, batchSizeFlag)

	// Test batch-delete-size flag
	batchDeleteSizeFlag, err := flags.GetInt("batch-delete-size")
	assert.NoError(t, err)
	assert.Equal(t, 0, batchDeleteSizeFlag)

	// Test sleep flag
	sleepFlag, err := flags.GetFloat64("sleep")
	assert.NoError(t, err)
	assert.Equal(t, float64(0), sleepFlag)

	// Test skip-verify flag
	skipVerifyFlag, err := flags.GetBool("skip-verify")
	assert.NoError(t, err)
	assert.Equal(t, false, skipVerifyFlag)
}

func TestRootCommandSubcommands(t *testing.T) {
	commands := rootCmd.Commands()
	commandNames := make([]string, len(commands))
	for i, cmd := range commands {
		commandNames[i] = cmd.Name()
	}

	expectedCommands := []string{
		"archive",
		"dry-run",
		"list-jobs",
		"plan",
		"purge",
		"validate",
		"version",
	}

	for _, expected := range expectedCommands {
		assert.Contains(t, commandNames, expected, "Expected command %s not found", expected)
	}
}
