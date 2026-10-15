package cmd

import (
	"github.com/spf13/cobra"
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
	// Console-output spec §3.9: filled evenly to 76 columns, corrected (3.0 builds the tree from
	// the declared relations; verification has none), and naming the exit codes.
	const want = "GoArchive archives related MySQL data across servers: it copies each batch\n" +
		"of a table tree to the destination, verifies it, and only then deletes it\n" +
		"from the source. An interrupted run is finished by replaying the job's log.\n" +
		"\n" +
		"Recommended for cold data: the rows in a job's where range must not change\n" +
		"while it runs. On very large or deeply nested trees, read the README and\n" +
		"docs/README_LIMITATIONS.md for the known limits first.\n" +
		"\n" +
		"Features:\n" +
		"  - Parent/child trees from the job's declared relations, with composite\n" +
		"    and non-integer keys\n" +
		"  - Key-ordered batches: copy, verification and the log write in one\n" +
		"    destination transaction; deletes in chunks committed only on an exact\n" +
		"    row count\n" +
		"  - Crash recovery by replaying the job's log\n" +
		"  - Replication lag gate before every delete\n" +
		"  - Verification by SHA256 (default), count, or none\n" +
		"\n" +
		"Exit codes: 0 done, 1 failed, 2 usage or configuration error, 3 stopped\n" +
		"before the end of the where range, 4 not started (another run holds the\n" +
		"work). See docs/README_OPERATIONS.md, Output and exit codes."
	if rootCmd.Long != want {
		t.Errorf("rootCmd.Long =\n%s\nwant\n%s", rootCmd.Long, want)
	}
	for i, line := range strings.Split(rootCmd.Long, "\n") {
		if len(line) > 76 {
			t.Errorf("help line %d is %d characters, over 76: %q", i+1, len(line), line)
		}
	}
	for _, gone := range []string{"Community edition", "Enterprise", "single-operator workstation", "automatic dependency resolution"} {
		if strings.Contains(rootCmd.Long, gone) {
			t.Errorf("rootCmd.Long still says %q", gone)
		}
	}
}

// TestRunHelpNamesExitCodes (console-output spec §3.9): each run command points at the codes.
func TestRunHelpNamesExitCodes(t *testing.T) {
	for _, c := range []*cobra.Command{archiveCmd, purgeCmd, copyOnlyCmd} {
		if !strings.Contains(c.Long, "Exit codes: see goarchive --help.") {
			t.Errorf("%s help does not point at the exit codes", c.Name())
		}
	}
	if f := archiveCmd.Flags().Lookup("progress"); f == nil || !strings.Contains(f.Usage, "log entries when the log is JSON on the console") {
		t.Errorf("--progress help does not say where its output goes")
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
