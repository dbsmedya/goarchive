package cmd

import (
	"fmt"
	"sort"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/spf13/cobra"
)

var listJobsCmd = &cobra.Command{
	Use:   "list-jobs",
	Short: "List all jobs defined in configuration",
	Long: `List-jobs displays all archive jobs defined in the configuration file
along with their basic settings.

Example:
  goarchive list-jobs --config archiver.yaml`,
	RunE: runListJobs,
}

func init() {
	rootCmd.AddCommand(listJobsCmd)
}

func runListJobs(cmd *cobra.Command, args []string) error {
	configFile := GetConfigFile()

	// Load configuration
	cfg, err := config.Load(configFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Get all job names
	jobNames := cfg.ListJobs()

	if len(jobNames) == 0 {
		cmd.Printf("No jobs defined in %s\n", configFile)
		return nil
	}

	// Sort job names for consistent output
	sort.Strings(jobNames)

	cmd.Printf("Jobs defined in %s:\n\n", configFile)

	for i, jobName := range jobNames {
		job, err := cfg.GetJob(jobName)
		if err != nil {
			return fmt.Errorf("failed to get job %q: %w", jobName, err)
		}

		// Job header
		cmd.Printf("%d. %s\n", i+1, jobName)
		cmd.Printf("   Root Table:    %s\n", job.RootTable)
		cmd.Printf("   Primary Key:   %s\n", job.PrimaryKey)

		// WHERE clause (if specified)
		if job.Where != "" {
			cmd.Printf("   WHERE:         %s\n", job.Where)
		} else {
			cmd.Printf("   WHERE:         (none)\n")
		}

		// Relations count
		cmd.Printf("   Relations:     %d table(s)\n", len(job.Relations))

		// Show relation details if any
		if len(job.Relations) > 0 {
			for _, rel := range job.Relations {
				cmd.Printf("      - %s (FK: %s, PK: %s, Type: %s)\n",
					rel.Table, rel.ForeignKey, rel.PrimaryKey, rel.DependencyType)
				// Show nested relations if any
				if len(rel.Relations) > 0 {
					for _, nested := range rel.Relations {
						cmd.Printf("         └─ %s (FK: %s, PK: %s, Type: %s)\n",
							nested.Table, nested.ForeignKey, nested.PrimaryKey, nested.DependencyType)
					}
				}
			}
		}

		// Job-specific processing config
		if job.Processing != nil {
			cmd.Printf("   Processing:    Custom (batch_size=%d, batch_delete_size=%d)\n",
				job.Processing.BatchSize, job.Processing.BatchDeleteSize)
		}

		// Job-specific verification config
		if job.Verification != nil {
			cmd.Printf("   Verification:  Custom (method=%s, skip=%v)\n",
				job.Verification.Method, job.Verification.SkipVerification)
		}

		// Add spacing between jobs
		if i < len(jobNames)-1 {
			cmd.Println()
		}
	}

	cmd.Printf("\nTotal: %d job(s)\n", len(jobNames))
	return nil
}
