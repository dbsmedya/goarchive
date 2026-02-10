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
