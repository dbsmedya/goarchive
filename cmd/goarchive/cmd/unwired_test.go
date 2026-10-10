package cmd

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// TestRunCommandsRefuseUntilWired: archive, purge, copy-only and dry-run refuse to run while
// they are not wired to the 3.0 engine (R45); M5 deleted the 2.x run path they called.
func TestRunCommandsRefuseUntilWired(t *testing.T) {
	for name, run := range map[string]func(*cobra.Command, []string) error{
		"archive": runArchive, "purge": runPurge, "copy-only": runCopyOnly, "dry-run": runDryrun,
	} {
		err := run(&cobra.Command{}, nil)
		if err == nil || !strings.Contains(err.Error(), name+" is not wired to the 3.0 engine yet") {
			t.Errorf("%s: %v", name, err)
		}
	}
}
