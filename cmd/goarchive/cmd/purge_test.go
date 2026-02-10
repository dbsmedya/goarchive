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

func TestPurgeJobVariable(t *testing.T) {
	// Save original value and restore after test
	originalPurgeJob := purgeJob
	defer func() {
		purgeJob = originalPurgeJob
	}()

	tests := []struct {
		name     string
		jobValue string
	}{
		{
			name:     "empty job",
			jobValue: "",
		},
		{
			name:     "simple job name",
			jobValue: "purge_old_logs",
		},
		{
			name:     "job with hyphens",
			jobValue: "purge-old-logs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			purgeJob = tt.jobValue
			assert.Equal(t, tt.jobValue, purgeJob)
		})
	}
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
	assert.Contains(t, doc, "--dry-run")
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
	assert.Contains(t, doc, "Delete")
}

// ============================================================================
// Phase 3: CLI Execution Tests
// ============================================================================

// TestPurgeCmd_Execute_InvalidJob tests execution with non-existent job name
func TestPurgeCmd_Execute_InvalidJob(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping CLI execution test in short mode")
	}

	origCfgFile := cfgFile
	origPurgeJob := purgeJob
	defer func() {
		cfgFile = origCfgFile
		purgeJob = origPurgeJob
		rootCmd.SetArgs(nil)
	}()

	rootCmd.SetArgs([]string{"purge", "--job", "nonexistent_job", "--config", "/tmp/nonexistent_purge_config.yaml"})
	err := rootCmd.Execute()
	assert.Error(t, err)
}

// TestPurgeCmd_Execute_MissingConfig tests execution when config file doesn't exist
func TestPurgeCmd_Execute_MissingConfig(t *testing.T) {
	origCfgFile := cfgFile
	origPurgeJob := purgeJob
	defer func() {
		cfgFile = origCfgFile
		purgeJob = origPurgeJob
		rootCmd.SetArgs(nil)
	}()

	rootCmd.SetArgs([]string{"purge", "--job", "test_purge", "--config", "/tmp/nonexistent_purge_config.yaml"})
	err := rootCmd.Execute()
	assert.Error(t, err)
}
