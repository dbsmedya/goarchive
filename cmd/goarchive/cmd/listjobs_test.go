package cmd

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestListJobsCommandStructure(t *testing.T) {
	assert.NotNil(t, listJobsCmd)
	assert.Equal(t, "list-jobs", listJobsCmd.Use)
	assert.NotEmpty(t, listJobsCmd.Short)
	assert.NotEmpty(t, listJobsCmd.Long)
	assert.NotNil(t, listJobsCmd.RunE)
}

func TestRunListJobs(t *testing.T) {
	// Save original value and restore after test
	originalCfgFile := cfgFile
	defer func() {
		cfgFile = originalCfgFile
	}()

	// Create a valid test config
	tmpDir := t.TempDir()
	validConfig := filepath.Join(tmpDir, "valid-config.yaml")

	configContent := `source:
  host: 127.0.0.1
  port: 3305
  user: root
  password: test
  database: test_db

destination:
  host: 127.0.0.1
  port: 3307
  user: root
  password: test
  database: test_archive

jobs:
  test-job:
    root_table: users
    primary_key: user_id
    where: "id < 100"
    relations: []
`

	err := os.WriteFile(validConfig, []byte(configContent), 0644)
	assert.NoError(t, err)

	tests := []struct {
		name       string
		configFile string
		wantErr    bool
	}{
		{
			name:       "valid config with jobs",
			configFile: validConfig,
			wantErr:    false,
		},
		{
			name:       "nonexistent config",
			configFile: "nonexistent-config.yaml",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfgFile = tt.configFile

			// Capture output
			var buf bytes.Buffer
			listJobsCmd.SetOut(&buf)
			listJobsCmd.SetErr(&buf)

			err := runListJobs(listJobsCmd, []string{})

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				output := buf.String()
				// Check that output contains job listing
				assert.Contains(t, output, "Jobs defined in")
			}
		})
	}
}

func TestListJobsCommandOutput(t *testing.T) {
	// Save original value and restore after test
	originalCfgFile := cfgFile
	defer func() {
		cfgFile = originalCfgFile
	}()

	// Create a temporary config file
	tmpDir := t.TempDir()
	testConfig := filepath.Join(tmpDir, "test-config.yaml")

	configContent := `source:
  host: 127.0.0.1
  port: 3305
  user: root
  password: test
  database: test_db

destination:
  host: 127.0.0.1
  port: 3307
  user: root
  password: test
  database: test_archive

jobs:
  test-job-1:
    root_table: users
    primary_key: user_id
    where: "created_at < '2024-01-01'"
    relations:
      - table: orders
        primary_key: order_id
        foreign_key: user_id
        dependency_type: "1-N"

  test-job-2:
    root_table: products
    primary_key: product_id
    where: "status = 'archived'"
    relations: []
`

	err := os.WriteFile(testConfig, []byte(configContent), 0644)
	assert.NoError(t, err)

	cfgFile = testConfig

	var buf bytes.Buffer
	listJobsCmd.SetOut(&buf)
	listJobsCmd.SetErr(&buf)

	err = runListJobs(listJobsCmd, []string{})
	assert.NoError(t, err)

	output := buf.String()
	// Check for expected job details
	assert.Contains(t, output, "Jobs defined in")
	assert.Contains(t, output, "test-job-1")
	assert.Contains(t, output, "test-job-2")
	assert.Contains(t, output, "Root Table:")
	assert.Contains(t, output, "Primary Key:")
	assert.Contains(t, output, "Relations:")
	assert.Contains(t, output, "Total: 2 job(s)")
}

func TestListJobsIsAddedToRoot(t *testing.T) {
	// Check that list-jobs command is registered
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Name() == "list-jobs" {
			found = true
			break
		}
	}
	assert.True(t, found, "list-jobs command should be added to root command")
}

// ============================================================================
// Phase 3: CLI Execution Tests
// ============================================================================

// TestListjobsCmd_Execute_MissingConfig tests listing jobs when config doesn't exist
func TestListjobsCmd_Execute_MissingConfig(t *testing.T) {
	origCfgFile := cfgFile
	defer func() {
		cfgFile = origCfgFile
		rootCmd.SetArgs(nil)
	}()

	rootCmd.SetArgs([]string{"list-jobs", "--config", "/tmp/nonexistent_listjobs_config.yaml"})
	err := rootCmd.Execute()
	assert.Error(t, err)
}
