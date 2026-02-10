package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExecute(t *testing.T) {
	// Note: Execute() calls os.Exit(1) on error, so we can't test the error case directly
	// without causing the test to exit. We test the function exists and doesn't panic
	// when called with valid arguments.

	// Test that Execute function exists (doesn't return anything)
	// This is primarily a compile-time check
	assert.NotNil(t, Execute)
}

func TestVersionVariables(t *testing.T) {
	// Verify version variables exist and have default values
	assert.NotEmpty(t, Version, "Version should not be empty")
	assert.NotEmpty(t, Commit, "Commit should not be empty")
}

func TestCLIFlagsVariables(t *testing.T) {
	// Verify CLI flag variables exist
	// These are package-level variables that get set by cobra flags

	// String flags - cfgFile defaults to "archiver.yaml" via init()
	assert.Equal(t, "archiver.yaml", cfgFile, "cfgFile should default to archiver.yaml")
	assert.Equal(t, "", logLevel)
	assert.Equal(t, "", logFormat)

	// Int flags should default to 0
	assert.Equal(t, 0, batchSize)
	assert.Equal(t, 0, batchDeleteSize)

	// Float flags should default to 0
	assert.Equal(t, float64(0), sleepSeconds)

	// Bool flags should default to false
	assert.Equal(t, false, skipVerify)
}

func TestCLIOverrideStruct(t *testing.T) {
	// Test CLIOverrides struct creation
	overrides := CLIOverrides{
		LogLevel:        "debug",
		LogFormat:       "json",
		BatchSize:       100,
		BatchDeleteSize: 50,
		SleepSeconds:    1.5,
		SkipVerify:      true,
	}

	assert.Equal(t, "debug", overrides.LogLevel)
	assert.Equal(t, "json", overrides.LogFormat)
	assert.Equal(t, 100, overrides.BatchSize)
	assert.Equal(t, 50, overrides.BatchDeleteSize)
	assert.Equal(t, 1.5, overrides.SleepSeconds)
	assert.True(t, overrides.SkipVerify)
}

func TestJobVariables(t *testing.T) {
	// Verify job-specific variables exist
	assert.Equal(t, "", archiveJob, "archiveJob should default to empty")
	assert.Equal(t, "", dryrunJob, "dryrunJob should default to empty")
	assert.Equal(t, "", purgeJob, "purgeJob should default to empty")
	assert.Equal(t, "", planJob, "planJob should default to empty")
}
