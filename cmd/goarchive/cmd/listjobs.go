package cmd

import (
	"strings"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/spf13/cobra"
)

var listJobsCmd = &cobra.Command{
	Use:   "list-jobs",
	Short: "List all jobs defined in configuration",
	Long: `List-jobs displays all jobs defined in the configuration file
along with their type, keys and relations. It opens no connection.

Example:
  goarchive list-jobs --config archiver.yaml

Exit codes: see goarchive --help.`,
	RunE: runE(runListJobs),
}

func init() {
	rootCmd.AddCommand(listJobsCmd)
}

func runListJobs(c *cobra.Command, _ []string) error {
	configFile := GetConfigFile()
	doc, err := jobspec.Load(configFile)
	if err != nil {
		return usage(err)
	}
	names := doc.JobNames()
	c.Printf("Jobs defined in %s:\n\n", configFile)
	for i, name := range names {
		spec := doc.Jobs[name]
		c.Printf("%d. %s (%s)\n", i+1, name, spec.Type)
		c.Printf("   Root Table:    %s (primary key: %s)\n", spec.RootTable, strings.Join(spec.PrimaryKey, ", "))
		c.Printf("   WHERE:         %s\n", spec.Where)
		c.Printf("   Relations:     %d table(s)\n", len(jobspec.NewTree(spec).CopyOrder())-1)
		printRelations(c, spec.Relations, "      ")
		if spec.Processing != nil {
			p := spec.EffectiveProcessing(doc.Shared.Processing)
			c.Printf("   Processing:    batch_size=%d, batch_delete_size=%d\n", p.BatchSize, p.BatchDeleteSize)
		}
		if spec.Type == jobspec.CopyOnly {
			c.Printf("   Resume:        %v\n", spec.ResumeEnabled())
		}
		if spec.Verification != nil {
			c.Printf("   Verification:  %s\n", *spec.Verification)
		}
		if i < len(names)-1 {
			c.Println()
		}
	}
	c.Printf("\nTotal: %d job(s)\n", len(names))
	return nil
}

// printRelations prints each relation and, indented below it, its own relations.
func printRelations(c *cobra.Command, rels []jobspec.Relation, indent string) {
	for _, r := range rels {
		dep := r.DependencyType
		if dep == "" {
			dep = "1-N"
		}
		c.Printf("%s- %s (FK: %s -> %s, %s)\n", indent, r.Table, strings.Join(r.ForeignKey, ", "), strings.Join(r.References, ", "), dep)
		printRelations(c, r.Relations, indent+"   ")
	}
}
