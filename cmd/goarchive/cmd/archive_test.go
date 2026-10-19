package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestArchiveCommandStructure(t *testing.T) {
	assert.NotNil(t, archiveCmd)
	assert.Equal(t, "archive", archiveCmd.Use)
	assert.NotEmpty(t, archiveCmd.Short)
	assert.NotEmpty(t, archiveCmd.Long)
	assert.NotNil(t, archiveCmd.RunE)
}

func TestArchiveCommandFlags(t *testing.T) {
	flags := archiveCmd.Flags()

	// Check job flag exists and is required
	jobFlag := flags.Lookup("job")
	assert.NotNil(t, jobFlag)
	assert.Equal(t, "j", jobFlag.Shorthand)
	assert.Equal(t, "", jobFlag.DefValue)

	// Check that job flag is required
	requiredAnnotation := jobFlag.Annotations["cobra_annotation_bash_completion_one_required_flag"]
	assert.NotNil(t, requiredAnnotation)
}

func TestArchiveIsAddedToRoot(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Name() == "archive" {
			found = true
			break
		}
	}
	assert.True(t, found, "archive command should be added to root command")
}

func TestArchiveCommandExample(t *testing.T) {
	// Verify the command has example usage documentation
	assert.Contains(t, archiveCmd.Long, "Example:")
	assert.Contains(t, archiveCmd.Long, "goarchive archive")
}

func TestArchiveCommandUsage(t *testing.T) {
	assert.Equal(t, "archive", archiveCmd.Use)
	assert.NotEmpty(t, archiveCmd.Short)
	assert.Contains(t, archiveCmd.Short, "Archive")
}

func TestArchiveCommandStepsDocumentation(t *testing.T) {
	// Verify the command documents the archive process steps
	doc := archiveCmd.Long
	assert.Contains(t, doc, "Discover")
	assert.Contains(t, doc, "Copy")
	assert.Contains(t, doc, "verify the copy")
	assert.Contains(t, doc, "Delete from source, child first")
}

// ============================================================================
// Phase 3: CLI Execution Tests
// ============================================================================

// TestArchiveCmd_Execute_MissingJobFlag tests execution without required --job flag
func TestArchiveCmd_Execute_MissingJobFlag(t *testing.T) {
	origCfgFile := cfgFile
	defer func() {
		cfgFile = origCfgFile
		rootCmd.SetArgs(nil)
	}()

	rootCmd.SetArgs([]string{"archive"})
	err := rootCmd.Execute()
	assert.Error(t, err)
}

// ============================================================================
// Test Helpers
// ============================================================================

// SetConfigFile is a helper to set the global config file path for testing
func SetConfigFile(path string) {
	cfgFile = path
}
