package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
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

func TestArchiveJobVariable(t *testing.T) {
	// Save original value and restore after test
	originalArchiveJob := archiveJob
	defer func() {
		archiveJob = originalArchiveJob
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
			name:     "job with hyphens",
			jobValue: "archive-old-orders",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			archiveJob = tt.jobValue
			assert.Equal(t, tt.jobValue, archiveJob)
		})
	}
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
	assert.Contains(t, doc, "Verify")
	assert.Contains(t, doc, "Delete")
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

// TestArchiveCmd_Execute_InvalidJob tests execution with non-existent job name
func TestArchiveCmd_Execute_InvalidJob(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping CLI execution test in short mode")
	}

	origCfgFile := cfgFile
	origArchiveJob := archiveJob
	defer func() {
		cfgFile = origCfgFile
		archiveJob = origArchiveJob
		rootCmd.SetArgs(nil)
	}()

	// Create temp config file with valid structure but different job name
	configFile := createTempTestConfig(t, map[string]interface{}{
		"jobs": map[string]interface{}{
			"valid_job": map[string]interface{}{
				"root_table":  "customers",
				"primary_key": "id",
				"where":       "created_at < '2024-01-01'",
			},
		},
	})

	rootCmd.SetArgs([]string{"archive", "--job", "nonexistent_job", "--config", configFile})
	err := rootCmd.Execute()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "job")
	assert.Contains(t, err.Error(), "not found")
}

// TestArchiveCmd_Execute_MissingConfig tests execution when config file doesn't exist
func TestArchiveCmd_Execute_MissingConfig(t *testing.T) {
	origCfgFile := cfgFile
	origArchiveJob := archiveJob
	defer func() {
		cfgFile = origCfgFile
		archiveJob = origArchiveJob
		rootCmd.SetArgs(nil)
	}()

	rootCmd.SetArgs([]string{"archive", "--job", "test_job", "--config", "/tmp/nonexistent_goarchive_config.yaml"})
	err := rootCmd.Execute()
	assert.Error(t, err)
}

// ============================================================================
// Test Helpers
// ============================================================================

// createTempTestConfig creates a temporary YAML config file for testing
func createTempTestConfig(t *testing.T, data map[string]interface{}) string {
	t.Helper()

	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "test_config.yaml")

	yamlData, err := yaml.Marshal(data)
	if err != nil {
		t.Fatalf("Failed to marshal config: %v", err)
	}

	err = os.WriteFile(configFile, yamlData, 0644)
	if err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	return configFile
}

// SetConfigFile is a helper to set the global config file path for testing
func SetConfigFile(path string) {
	cfgFile = path
}
