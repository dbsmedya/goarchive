package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDryrunCommandStructure(t *testing.T) {
	assert.NotNil(t, dryrunCmd)
	assert.Equal(t, "dry-run", dryrunCmd.Use)
	assert.NotEmpty(t, dryrunCmd.Short)
	assert.NotEmpty(t, dryrunCmd.Long)
	assert.NotNil(t, dryrunCmd.RunE)
}

func TestDryrunCommandFlags(t *testing.T) {
	flags := dryrunCmd.Flags()

	// Check job flag exists and is required
	jobFlag := flags.Lookup("job")
	assert.NotNil(t, jobFlag)
	assert.Equal(t, "j", jobFlag.Shorthand)
	assert.Equal(t, "", jobFlag.DefValue)

	// Check that job flag is required
	requiredAnnotation := jobFlag.Annotations["cobra_annotation_bash_completion_one_required_flag"]
	assert.NotNil(t, requiredAnnotation)
}

func TestDryrunIsAddedToRoot(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Name() == "dry-run" {
			found = true
			break
		}
	}
	assert.True(t, found, "dry-run command should be added to root command")
}

func TestDryrunCommandExample(t *testing.T) {
	// Verify the command has example usage documentation
	assert.Contains(t, dryrunCmd.Long, "Example:")
	assert.Contains(t, dryrunCmd.Long, "goarchive dry-run")
}

func TestDryrunCommandUsage(t *testing.T) {
	assert.Equal(t, "dry-run", dryrunCmd.Use)
	assert.NotEmpty(t, dryrunCmd.Short)
	assert.Contains(t, dryrunCmd.Short, "without changing anything")
}

func TestDryrunCommandFeatures(t *testing.T) {
	// Verify the command documents what dry-run shows
	doc := dryrunCmd.Long
	assert.Contains(t, doc, "preflight under its job type's profile")
	assert.Contains(t, doc, "first batch of root keys")
	assert.Contains(t, doc, "EXPLAIN FORMAT=TRADITIONAL")
	assert.Contains(t, doc, "validate -> dry-run -> archive")
}

func TestDryrunCommandNoChanges(t *testing.T) {
	// Verify the command emphasizes no changes are made
	doc := dryrunCmd.Long
	assert.Contains(t, doc, "takes no lock, writes no tracking row and\nchanges no data")
}

// ============================================================================
// Phase 3: CLI Execution Tests
// ============================================================================
