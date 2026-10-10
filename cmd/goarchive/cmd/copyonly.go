package cmd

import (
	"github.com/spf13/cobra"
)

var (
	copyOnlyJob      string
	copyOnlyForce    bool
	copyOnlyProgress string
)

var copyOnlyCmd = &cobra.Command{
	Use:   "copy-only",
	Short: "Copy data to destination without deleting from source",
	Long: `Copy-only copies matching records from source to destination database,
verifies the copied data, and never deletes from source.

Default mode runs non-interactively and logs:
  INFO: Source data will NOT be deleted

Force mode (--force) bypasses duplicate preflight checks.`,
	RunE: runCopyOnly,
}

func init() {
	copyOnlyCmd.Flags().StringVarP(&copyOnlyJob, "job", "j", "",
		"Job name from configuration file (required)")
	_ = copyOnlyCmd.MarkFlagRequired("job")
	copyOnlyCmd.Flags().BoolVar(&copyOnlyForce, "force", false,
		"Proceed past advisory lock contention only when the lock holder's heartbeat is stale (indicating a crashed prior instance). Also bypasses destination duplicate preflight checks after confirmation.")
	registerProgressFlag(copyOnlyCmd, &copyOnlyProgress)
	rootCmd.AddCommand(copyOnlyCmd)
}

func runCopyOnly(_ *cobra.Command, _ []string) error {
	return errUnwired("copy-only")
}
