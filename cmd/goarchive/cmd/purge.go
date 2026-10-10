package cmd

import (
	"github.com/spf13/cobra"
)

var (
	purgeJob      string
	purgeForce    bool
	purgeProgress string
)

var purgeCmd = &cobra.Command{
	Use:   "purge",
	Short: "Delete data from source without archiving",
	Long: `Purge deletes matching records from source database without copying
to a destination. Use this when you don't need to preserve the data.

The purge process:
  1. Discover all related records using BFS traversal
  2. Delete from source in dependency order (child-first)

WARNING: This permanently deletes data. Use --dry-run first to verify.

Example:
  goarchive purge --config archiver.yaml --job purge_old_logs`,
	RunE: runPurge,
}

func init() {
	purgeCmd.Flags().StringVarP(&purgeJob, "job", "j", "",
		"Job name from configuration file (required)")
	_ = purgeCmd.MarkFlagRequired("job") // Config-time error, cannot fail

	purgeCmd.Flags().BoolVar(&purgeForce, "force", false,
		"Refresh a stale heartbeat takeover only. Because purge is destructive, --force CANNOT proceed while the advisory GET_LOCK is still held by another connection (a held lock cannot be safely stolen): verify the prior process is dead and its MySQL session has closed, then retry. Cannot bypass: a live heartbeating job, the same-root concurrency check, or preflight checks.")
	registerProgressFlag(purgeCmd, &purgeProgress)

	rootCmd.AddCommand(purgeCmd)
}

func runPurge(_ *cobra.Command, _ []string) error {
	return errUnwired("purge")
}
