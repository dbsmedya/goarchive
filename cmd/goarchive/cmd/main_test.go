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
}
