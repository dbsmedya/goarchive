package cmd

import (
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/spf13/cobra"
)

var purgeOpts runOptions

var purgeCmd = &cobra.Command{
	Use:   "purge",
	Short: "Delete data from source without archiving",
	Long: `Purge deletes matching records from source database without copying
to a destination. Use this when you don't need to preserve the data.

Each batch:
  1. Fetch the next root keys of the where range, in key order
  2. Discover every related row, parent first
  3. Log the roots as deleting, then delete from source, child first,
     each chunk in its own transaction

WARNING: This permanently deletes data. Run dry-run first.

Example:
  goarchive purge --config archiver.yaml --job purge_old_logs`,
	RunE: func(c *cobra.Command, _ []string) error { return runJob(c, jobspec.Purge, &purgeOpts) },
}

func init() {
	registerRunFlags(purgeCmd, &purgeOpts, jobspec.Purge)
	rootCmd.AddCommand(purgeCmd)
}
