package cmd

import (
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/spf13/cobra"
)

var archiveOpts runOptions

var archiveCmd = &cobra.Command{
	Use:   "archive",
	Short: "Archive data from source to destination database",
	Long: `Archive copies matching records from source to destination database,
verifies the copy, then deletes from source.

Each batch:
  1. Fetch the next root keys of the where range, in key order
  2. Discover every related row, parent first
  3. Copy them to the destination, verify the copy, and log the roots copied,
     all in one destination transaction
  4. Delete from source, child first, each chunk in its own transaction

A run first replays what an interrupted run logged.

Example:
  goarchive archive --config archiver.yaml --job archive_old_orders

Exit codes: see goarchive --help.`,
	RunE: runE(func(c *cobra.Command, _ []string) error { return runCommand(c, jobspec.Archive, &archiveOpts) }),
}

func init() {
	registerRunFlags(archiveCmd, &archiveOpts, jobspec.Archive)
	rootCmd.AddCommand(archiveCmd)
}
