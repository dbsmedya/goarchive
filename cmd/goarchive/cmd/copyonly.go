package cmd

import (
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/spf13/cobra"
)

var copyOnlyOpts runOptions

var copyOnlyCmd = &cobra.Command{
	Use:   "copy-only",
	Short: "Copy data to destination without deleting from source",
	Long: `Copy-only copies matching records from source to destination database,
verifies the copied data, and never deletes from source.

A rerun copies again from the start of the where range. With resume: true
in the job, it skips the roots this job has already copied.

Example:
  goarchive copy-only --config archiver.yaml --job copy_orders`,
	RunE: func(c *cobra.Command, _ []string) error { return runJob(c, jobspec.CopyOnly, &copyOnlyOpts) },
}

func init() {
	registerRunFlags(copyOnlyCmd, &copyOnlyOpts, jobspec.CopyOnly)
	rootCmd.AddCommand(copyOnlyCmd)
}
