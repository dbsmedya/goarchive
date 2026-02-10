package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateCommandStructure(t *testing.T) {
	assert.NotNil(t, validateCmd)
	assert.Equal(t, "validate", validateCmd.Use)
	assert.NotEmpty(t, validateCmd.Short)
	assert.NotEmpty(t, validateCmd.Long)
	assert.NotNil(t, validateCmd.RunE)
}

func TestValidateCommandFlags(t *testing.T) {
	flags := validateCmd.Flags()

	// Validate command currently has no specific flags
	// It uses the persistent flags from root
	assert.NotNil(t, flags)
}

func TestValidateIsAddedToRoot(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Name() == "validate" {
			found = true
			break
		}
	}
	assert.True(t, found, "validate command should be added to root command")
}

func TestValidateCommandExample(t *testing.T) {
	// Verify the command has example usage documentation
	assert.Contains(t, validateCmd.Long, "Example:")
	assert.Contains(t, validateCmd.Long, "goarchive validate")
}

func TestValidateCommandUsage(t *testing.T) {
	assert.Equal(t, "validate", validateCmd.Use)
	assert.NotEmpty(t, validateCmd.Short)
	assert.Contains(t, validateCmd.Short, "Validate")
}

func TestValidateCommandChecks(t *testing.T) {
	// Verify the command documents the validation checks
	doc := validateCmd.Long
	assert.Contains(t, doc, "Checks performed")
	assert.Contains(t, doc, "Configuration")
	assert.Contains(t, doc, "Database connectivity")
	assert.Contains(t, doc, "Table existence")
	assert.Contains(t, doc, "Foreign key")
	assert.Contains(t, doc, "DELETE trigger")
	assert.Contains(t, doc, "CASCADE")
}

func TestValidateCommandPreflight(t *testing.T) {
	// Verify the command mentions preflight checks
	doc := validateCmd.Long
	assert.Contains(t, doc, "preflight checks")
}

func TestValidateCommandNoJobFlag(t *testing.T) {
	// Validate command operates on all jobs, not a specific one
	flags := validateCmd.Flags()
	jobFlag := flags.Lookup("job")
	assert.Nil(t, jobFlag, "validate command should not have a job flag")
}
