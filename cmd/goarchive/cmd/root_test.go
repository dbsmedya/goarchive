package cmd

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetConfigFile(t *testing.T) {
	// Save original value and restore after test
	originalCfgFile := cfgFile
	defer func() {
		cfgFile = originalCfgFile
	}()

	tests := []struct {
		name     string
		cfgValue string
		want     string
	}{
		{
			name:     "default config file",
			cfgValue: "",
			want:     "",
		},
		{
			name:     "custom config file",
			cfgValue: "/path/to/custom.yaml",
			want:     "/path/to/custom.yaml",
		},
		{
			name:     "config file with spaces",
			cfgValue: "/path/to/my config.yaml",
			want:     "/path/to/my config.yaml",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfgFile = tt.cfgValue
			got := GetConfigFile()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetCLIOverrides(t *testing.T) {
	originalLogLevel, originalLogFormat := logLevel, logFormat
	defer func() { logLevel, logFormat = originalLogLevel, originalLogFormat }()
	logLevel, logFormat = "debug", "text"
	assert.Equal(t, CLIOverrides{LogLevel: "debug", LogFormat: "text"}, GetCLIOverrides())
}

func TestRootCommandStructure(t *testing.T) {
	assert.NotNil(t, rootCmd)
	assert.Equal(t, "goarchive", rootCmd.Use)
	assert.NotEmpty(t, rootCmd.Short)
	assert.NotEmpty(t, rootCmd.Long)
	assert.Equal(t, Version, rootCmd.Version)
}

// TestRootHelpDescription pins the paragraph goarchive --help prints before its features
// (pre-alpha minors, item 2): 3.0 has no edition split, and the limits live in the README and
// LIMITATIONS.
func TestRootHelpDescription(t *testing.T) {
	const want = "A CLI tool for safely archiving MySQL relational data across servers\n" +
		"with automatic dependency resolution and crash recovery.\n" +
		"Recommended for archival of cold data. Use with caution on\n" +
		"very large or deeply nested schemas;\n" +
		"see README and LIMITATIONS for known limits\n\nFeatures:\n"
	if !strings.HasPrefix(rootCmd.Long, want) {
		t.Errorf("rootCmd.Long begins\n%s\nwant\n%s", rootCmd.Long[:min(len(rootCmd.Long), len(want))], want)
	}
	for _, gone := range []string{"Community edition", "Enterprise", "single-operator workstation"} {
		if strings.Contains(rootCmd.Long, gone) {
			t.Errorf("rootCmd.Long still says %q", gone)
		}
	}
}

func TestRootCommandPersistentFlags(t *testing.T) {
	flags := rootCmd.PersistentFlags()

	// Test config flag
	configFlag, err := flags.GetString("config")
	assert.NoError(t, err)
	assert.Equal(t, "archiver.yaml", configFlag)

	// Test log-level flag
	logLevelFlag, err := flags.GetString("log-level")
	assert.NoError(t, err)
	assert.Equal(t, "", logLevelFlag)

	// Test log-format flag
	logFormatFlag, err := flags.GetString("log-format")
	assert.NoError(t, err)
	assert.Equal(t, "", logFormatFlag)

	// Processing flags are config-file-only and must not exist on the CLI
	for _, removed := range []string{"batch-size", "batch-delete-size", "sleep"} {
		assert.Nil(t, flags.Lookup(removed), "flag --%s should not be registered", removed)
	}

	// --skip-verify is gone: --verification=none replaces it (R42, M6-1).
	assert.Nil(t, flags.Lookup("skip-verify"))
}

func TestRootCommandSubcommands(t *testing.T) {
	commands := rootCmd.Commands()
	commandNames := make([]string, len(commands))
	for i, cmd := range commands {
		commandNames[i] = cmd.Name()
	}

	expectedCommands := []string{
		"archive",
		"copy-only",
		"dry-run",
		"list-jobs",
		"plan",
		"purge",
		"validate",
		"version",
	}

	for _, expected := range expectedCommands {
		assert.Contains(t, commandNames, expected, "Expected command %s not found", expected)
	}
}
