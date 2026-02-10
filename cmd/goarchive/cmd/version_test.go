package cmd

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVersionCommandStructure(t *testing.T) {
	assert.NotNil(t, versionCmd)
	assert.Equal(t, "version", versionCmd.Use)
	assert.NotEmpty(t, versionCmd.Short)
	assert.NotEmpty(t, versionCmd.Long)
	assert.NotNil(t, versionCmd.Run)
}

func TestRunVersion(t *testing.T) {
	// Save original values and restore after test
	originalVersion := Version
	originalCommit := Commit
	defer func() {
		Version = originalVersion
		Commit = originalCommit
	}()

	tests := []struct {
		name         string
		version      string
		commit       string
		wantInOutput []string
	}{
		{
			name:    "dev version",
			version: "0.0.1-dev",
			commit:  "unknown",
			wantInOutput: []string{
				"goarchive version 0.0.1-dev",
				"Commit: unknown",
				"Go version:",
				"OS/Arch:",
			},
		},
		{
			name:    "release version",
			version: "1.0.0",
			commit:  "abc123def456",
			wantInOutput: []string{
				"goarchive version 1.0.0",
				"Commit: abc123def456",
				"Go version:",
				"OS/Arch:",
			},
		},
		{
			name:    "empty version",
			version: "",
			commit:  "",
			wantInOutput: []string{
				"goarchive version ",
				"Commit: ",
				"Go version:",
				"OS/Arch:",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Version = tt.version
			Commit = tt.commit

			// Capture output
			var buf bytes.Buffer
			versionCmd.SetOut(&buf)

			runVersion(versionCmd, []string{})

			output := buf.String()
			for _, want := range tt.wantInOutput {
				assert.Contains(t, output, want)
			}
		})
	}
}

func TestVersionOutputFormat(t *testing.T) {
	// Save original values and restore after test
	originalVersion := Version
	originalCommit := Commit
	defer func() {
		Version = originalVersion
		Commit = originalCommit
	}()

	Version = "1.2.3"
	Commit = "abc123"

	var buf bytes.Buffer
	versionCmd.SetOut(&buf)

	runVersion(versionCmd, []string{})

	// Verify each line format
	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	assert.GreaterOrEqual(t, len(lines), 4, "Expected at least 4 lines in version output")

	// Check version line
	assert.Contains(t, string(lines[0]), "goarchive version 1.2.3")

	// Check commit line
	assert.Contains(t, string(lines[1]), "Commit: abc123")

	// Check Go version line
	assert.Contains(t, string(lines[2]), "Go version:")
	assert.Contains(t, string(lines[2]), runtime.Version())

	// Check OS/Arch line
	assert.Contains(t, string(lines[3]), "OS/Arch:")
	assert.Contains(t, string(lines[3]), runtime.GOOS)
	assert.Contains(t, string(lines[3]), runtime.GOARCH)
}

func TestVersionIsAddedToRoot(t *testing.T) {
	// Check that version command is registered
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Name() == "version" {
			found = true
			break
		}
	}
	assert.True(t, found, "version command should be added to root command")
}
