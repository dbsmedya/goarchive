package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPurgeCommandStructure(t *testing.T) {
	assert.NotNil(t, purgeCmd)
	assert.Equal(t, "purge", purgeCmd.Use)
	assert.NotEmpty(t, purgeCmd.Short)
	assert.NotEmpty(t, purgeCmd.Long)
	assert.NotNil(t, purgeCmd.RunE)
}

func TestPurgeCommandFlags(t *testing.T) {
	flags := purgeCmd.Flags()

	// Check job flag exists and is required
	jobFlag := flags.Lookup("job")
	assert.NotNil(t, jobFlag)
	assert.Equal(t, "j", jobFlag.Shorthand)
	assert.Equal(t, "", jobFlag.DefValue)

	// Check that job flag is required
	requiredAnnotation := jobFlag.Annotations["cobra_annotation_bash_completion_one_required_flag"]
	assert.NotNil(t, requiredAnnotation)
}

func TestPurgeIsAddedToRoot(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Name() == "purge" {
			found = true
			break
		}
	}
	assert.True(t, found, "purge command should be added to root command")
}

func TestPurgeCommandExample(t *testing.T) {
	// Verify the command has example usage documentation
	assert.Contains(t, purgeCmd.Long, "Example:")
	assert.Contains(t, purgeCmd.Long, "goarchive purge")
}

func TestPurgeCommandUsage(t *testing.T) {
	assert.Equal(t, "purge", purgeCmd.Use)
	assert.NotEmpty(t, purgeCmd.Short)
	assert.Contains(t, purgeCmd.Short, "Delete")
}

func TestPurgeCommandWarning(t *testing.T) {
	// Verify the command has appropriate warnings
	doc := purgeCmd.Long
	assert.Contains(t, doc, "WARNING")
	assert.Contains(t, doc, "permanently deletes")
	assert.Contains(t, doc, "Run dry-run first")
}

func TestPurgeCommandNoArchive(t *testing.T) {
	// Verify the command emphasizes no archiving
	doc := purgeCmd.Long
	assert.Contains(t, doc, "without copying")
}

func TestPurgeCommandSteps(t *testing.T) {
	// Verify the command documents the purge process steps
	doc := purgeCmd.Long
	assert.Contains(t, doc, "Discover")
	assert.Contains(t, doc, "delete from source, child first")
}

// ============================================================================
// Phase 3: CLI Execution Tests
// ============================================================================
