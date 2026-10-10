package cmd

import (
	"github.com/spf13/cobra"
)

var (
	archiveJob      string
	archiveForce    bool
	archiveProgress string
)

var archiveCmd = &cobra.Command{
	Use:   "archive",
	Short: "Archive data from source to destination database",
	Long: `Archive copies matching records from source to destination database,
verifies the copy, then deletes from source.

The archive process follows these steps:
  1. Discover all related records using BFS traversal
  2. Copy records to destination in dependency order (parent-first)
  3. Verify copy integrity (count or SHA256)
  4. Delete from source in reverse order (child-first)

Example:
  goarchive archive --config archiver.yaml --job archive_old_orders`,
	RunE: runArchive,
}

func init() {
	archiveCmd.Flags().StringVarP(&archiveJob, "job", "j", "",
		"Job name from configuration file (required)")
	_ = archiveCmd.MarkFlagRequired("job") // Config-time error, cannot fail

	archiveCmd.Flags().BoolVar(&archiveForce, "force", false,
		"Refresh a stale heartbeat takeover only. Because archive is destructive, --force CANNOT proceed while the advisory GET_LOCK is still held by another connection (a held lock cannot be safely stolen): verify the prior process is dead and its MySQL session has closed, then retry. Cannot bypass: a live heartbeating job, the same-root concurrency check, or preflight checks.")
	registerProgressFlag(archiveCmd, &archiveProgress)

	rootCmd.AddCommand(archiveCmd)
}

func runArchive(_ *cobra.Command, _ []string) error {
	return errUnwired("archive")
}
