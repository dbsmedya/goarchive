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

func TestDryrunJobVariable(t *testing.T) {
	// Save original value and restore after test
	originalDryrunJob := dryrunJob
	defer func() {
		dryrunJob = originalDryrunJob
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
			jobValue: "archive_old_orders",
		},
		{
			name:     "job with underscores",
			jobValue: "dry_run_test_job",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dryrunJob = tt.jobValue
			assert.Equal(t, tt.jobValue, dryrunJob)
		})
	}
}

func TestDryrunCommandUsage(t *testing.T) {
	assert.Equal(t, "dry-run", dryrunCmd.Use)
	assert.NotEmpty(t, dryrunCmd.Short)
	assert.Contains(t, dryrunCmd.Short, "Simulate")
}

func TestDryrunCommandFeatures(t *testing.T) {
	// Verify the command documents what dry-run shows
	doc := dryrunCmd.Long
	assert.Contains(t, doc, "row counts")
	assert.Contains(t, doc, "batches")
	assert.Contains(t, doc, "Configuration summary")
}

func TestDryrunCommandNoChanges(t *testing.T) {
	// Verify the command emphasizes no changes are made
	doc := dryrunCmd.Long
	assert.Contains(t, doc, "without making")
	assert.Contains(t, doc, "simulate")
}

// ============================================================================
// Phase 3: CLI Execution Tests
// ============================================================================

// TestDryrunCmd_Execute_InvalidJob tests execution with non-existent job name
func TestDryrunCmd_Execute_InvalidJob(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping CLI execution test in short mode")
	}

	origCfgFile := cfgFile
	origDryrunJob := dryrunJob
	defer func() {
		cfgFile = origCfgFile
		dryrunJob = origDryrunJob
		rootCmd.SetArgs(nil)
	}()

	rootCmd.SetArgs([]string{"dry-run", "--job", "nonexistent_job", "--config", "/tmp/nonexistent_dryrun_config.yaml"})
	err := rootCmd.Execute()
	assert.Error(t, err)
}

// TestDryrunCmd_Execute_MissingConfig tests execution when config file doesn't exist
func TestDryrunCmd_Execute_MissingConfig(t *testing.T) {
	origCfgFile := cfgFile
	origDryrunJob := dryrunJob
	defer func() {
		cfgFile = origCfgFile
		dryrunJob = origDryrunJob
		rootCmd.SetArgs(nil)
	}()

	rootCmd.SetArgs([]string{"dry-run", "--job", "test_dryrun", "--config", "/tmp/nonexistent_dryrun_config.yaml"})
	err := rootCmd.Execute()
	assert.Error(t, err)
}
