package cmd

import (
	"github.com/spf13/cobra"
)

var dryrunJob string

var dryrunCmd = &cobra.Command{
	Use:   "dry-run",
	Short: "Simulate archive execution without making changes",
	Long: `Dry-run simulates the archive process and reports what would happen
without making any actual changes to the databases.

The dry-run:
  - Runs non-destructive preflight checks (schema, charset, grants)
  - Shows the job WHERE clause and estimated row counts (root and children,
    filtered through the relation chain)
  - Shows the number of batches that would be processed
  - Validates batch_size against destination payload limits (rolled back)

Recommended operator workflow: validate -> dry-run -> archive.

Example:
  goarchive dry-run --config archiver.yaml --job archive_old_orders`,
	RunE: runDryrun,
}

func init() {
	dryrunCmd.Flags().StringVarP(&dryrunJob, "job", "j", "",
		"Job name from configuration file (required)")
	_ = dryrunCmd.MarkFlagRequired("job") // Config-time error, cannot fail

	rootCmd.AddCommand(dryrunCmd)
}

func runDryrun(_ *cobra.Command, _ []string) error {
	return errUnwired("dry-run")
}
