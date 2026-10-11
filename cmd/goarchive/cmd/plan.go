package cmd

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/mermaidascii"
	"github.com/spf13/cobra"
)

// outputWriter is used for printing output, can be overridden in tests
var outputWriter io.Writer = os.Stdout

var planJob string

var planCmd = &cobra.Command{
	Use:   "plan",
	Short: "Show execution plan for a job",
	Long: `Plan analyzes the job configuration and displays the execution order
for tables based on dependency resolution.

The plan shows:
  - Visual relation tree (using mermaid-ascii)
  - Copy order (parent tables first)
  - Delete order (child tables first)
  - Each table's column pairing with its parent

Example:
  goarchive plan --config archiver.yaml --job archive_old_orders`,
	RunE: runPlan,
}

func init() {
	planCmd.Flags().StringVarP(&planJob, "job", "j", "",
		"Job name from configuration file (required)")
	_ = planCmd.MarkFlagRequired("job") // Config-time error, cannot fail

	rootCmd.AddCommand(planCmd)
}

func runPlan(c *cobra.Command, _ []string) error {
	doc, spec, err := loadJob(planJob, "")
	if err != nil {
		return err
	}
	outputWriter = c.OutOrStdout()
	tree := jobspec.NewTree(spec)
	processing := spec.EffectiveProcessing(doc.Shared.Processing)
	verification := "none needed (purge copies nothing)"
	if spec.Type != jobspec.Purge {
		verification = string(spec.EffectiveVerification(doc.Verification))
	}

	if err := printMermaidTree(spec, doc, processing, verification); err != nil {
		return fmt.Errorf("failed to render tree: %w", err)
	}
	_, _ = fmt.Fprintln(outputWriter)

	printHeader("Execution Plan: %s", planJob)
	_, _ = fmt.Fprintln(outputWriter)
	printSection("Job Overview")
	_, _ = fmt.Fprintf(outputWriter, "  Job:          %s (%s)\n", planJob, spec.Type)
	_, _ = fmt.Fprintf(outputWriter, "  Root Table:   %s (primary key: %s)\n", spec.RootTable, strings.Join(spec.PrimaryKey, ", "))
	_, _ = fmt.Fprintf(outputWriter, "  Tables:       %d\n", len(tree.CopyOrder()))
	_, _ = fmt.Fprintf(outputWriter, "  Where:        %s\n", spec.Where)

	_, _ = fmt.Fprintln(outputWriter)
	printSection("Copy Order (parent tables first)")
	for i, n := range tree.CopyOrder() {
		printOrderItem(i+1, n, false)
	}
	_, _ = fmt.Fprintln(outputWriter)
	printSection("Delete Order (child tables first)")
	for i, n := range tree.DeleteOrder() {
		printOrderItem(i+1, n, true)
	}

	_, _ = fmt.Fprintln(outputWriter)
	printSection("Configuration")
	_, _ = fmt.Fprintf(outputWriter, "  Batch Size:          %d%s\n", processing.BatchSize,
		jobSpecific(spec.Processing != nil && spec.Processing.BatchSize != nil))
	if spec.Type != jobspec.CopyOnly {
		_, _ = fmt.Fprintf(outputWriter, "  Batch Delete Size:   %d%s\n", processing.BatchDeleteSize,
			jobSpecific(spec.Processing != nil && spec.Processing.BatchDeleteSize != nil))
	}
	_, _ = fmt.Fprintf(outputWriter, "  Sleep Between Batches: %.1fs%s\n", processing.SleepSeconds,
		jobSpecific(spec.Processing != nil && spec.Processing.SleepSeconds != nil))
	_, _ = fmt.Fprintf(outputWriter, "  Verification:        %s%s\n", verification, jobSpecific(spec.Verification != nil))
	return nil
}

func jobSpecific(set bool) string {
	if set {
		return " (job-specific)"
	}
	return ""
}

// printHeader prints a formatted header
func printHeader(format string, args ...interface{}) {
	title := fmt.Sprintf(format, args...)
	width := len(title) + 4
	_, _ = fmt.Fprintln(outputWriter, strings.Repeat("=", width))
	_, _ = fmt.Fprintf(outputWriter, "  %s\n", title)
	_, _ = fmt.Fprintln(outputWriter, strings.Repeat("=", width))
}

// printSection prints a section header
func printSection(title string) {
	_, _ = fmt.Fprintf(outputWriter, "[%s]\n", title)
	_, _ = fmt.Fprintln(outputWriter, strings.Repeat("-", len(title)+2))
}

// printOrderItem prints a table in the copy or delete order, with the column pairing that
// ties it to its parent.
func printOrderItem(num int, n *jobspec.Node, isDelete bool) {
	numStr := fmt.Sprintf("[%d]", num)
	if n.Parent == nil {
		_, _ = fmt.Fprintf(outputWriter, "  %s %s (root)\n", numStr, n.Table)
		return
	}
	arrow := "->"
	if isDelete {
		arrow = "<-"
	}
	_, _ = fmt.Fprintf(outputWriter, "  %s %s | (%s) %s %s (%s)\n", numStr, n.Table,
		strings.Join(n.ForeignKey, ", "), arrow, n.Parent.Table, strings.Join(n.References, ", "))
}

// printMermaidTree renders the job's tree with mermaid-ascii beside a summary.
func printMermaidTree(spec *jobspec.Spec, doc *jobspec.Document, processing config.ProcessingConfig, verification string) error {
	output, err := mermaidascii.RenderDiagram(generateMermaidSyntax(spec), nil)
	if err != nil {
		return err
	}
	summaryLines := []string{
		"[ Tree Summary ]",
		strings.Repeat("-", 16),
		fmt.Sprintf("Root Table:     %s", spec.RootTable),
		fmt.Sprintf("Relations:      %d tables", len(jobspec.NewTree(spec).CopyOrder())-1),
		fmt.Sprintf("Max Depth:      %d levels", calculateMaxDepth(spec.Relations)),
		fmt.Sprintf("Destination DB: %s", doc.Shared.Destination.Database),
		"",
		"[ Processing ]",
		strings.Repeat("-", 14),
		fmt.Sprintf("Batch Size:      %d", processing.BatchSize),
		fmt.Sprintf("Batch Delete:    %d", processing.BatchDeleteSize),
		fmt.Sprintf("Sleep:           %.1fs", processing.SleepSeconds),
		"",
		"[ Verification ]",
		strings.Repeat("-", 16),
		fmt.Sprintf("Method:          %s", verification),
	}
	_, _ = fmt.Fprintln(outputWriter)
	printHeader("Relation Tree")
	_, _ = fmt.Fprintln(outputWriter)
	printSideBySide(output, summaryLines, 4)
	return nil
}

// printSideBySide prints two blocks of text side by side
// padding is the minimum spaces between the two columns
func printSideBySide(leftContent string, rightLines []string, padding int) {
	leftLines := strings.Split(strings.TrimRight(leftContent, "\n"), "\n")

	// Calculate max visual width of left column (using rune count for Unicode)
	leftWidth := 0
	for _, line := range leftLines {
		w := visualWidth(line)
		if w > leftWidth {
			leftWidth = w
		}
	}

	// Calculate height of each column
	leftHeight := len(leftLines)
	rightHeight := len(rightLines)
	maxHeight := leftHeight
	if rightHeight > maxHeight {
		maxHeight = rightHeight
	}

	// Print rows side by side
	for i := 0; i < maxHeight; i++ {
		leftPart := ""
		rightPart := ""

		// Get left column content
		if i < leftHeight {
			leftPart = leftLines[i]
		}

		// Get right column content
		if i < rightHeight {
			rightPart = rightLines[i]
		}

		// Print left content
		_, _ = fmt.Fprint(outputWriter, leftPart)

		// Calculate padding needed to align right column
		spacesNeeded := leftWidth - visualWidth(leftPart) + padding
		if spacesNeeded > 0 {
			_, _ = fmt.Fprint(outputWriter, strings.Repeat(" ", spacesNeeded))
		}

		// Print right content
		_, _ = fmt.Fprintln(outputWriter, rightPart)
	}
}

// visualWidth returns the visual width of a string, accounting for wide characters
func visualWidth(s string) int {
	width := 0
	for _, r := range s {
		// Box drawing characters and most Unicode characters are single-width in terminal
		// CJK characters are typically double-width but mermaid-ascii uses box drawing
		if r >= 0x2500 && r <= 0x257F {
			// Box drawing characters
			width++
		} else {
			width++
		}
	}
	return width
}

// generateMermaidSyntax creates mermaid graph syntax from the job's relations.
func generateMermaidSyntax(spec *jobspec.Spec) string {
	var sb strings.Builder
	sb.WriteString("graph TD\n")
	_, _ = fmt.Fprintf(&sb, "    %s\n", sanitizeNodeID(spec.RootTable))
	addRelationsToMermaid(&sb, spec.RootTable, spec.Relations)
	return sb.String()
}

// addRelationsToMermaid adds each relation's edge, labelled with its dependency type.
func addRelationsToMermaid(sb *strings.Builder, parentTable string, relations []jobspec.Relation) {
	for _, rel := range relations {
		dep := rel.DependencyType
		if dep == "" {
			dep = "1-N"
		}
		_, _ = fmt.Fprintf(sb, "    %s -->|%s| %s\n", sanitizeNodeID(parentTable), dep, sanitizeNodeID(rel.Table))
		addRelationsToMermaid(sb, rel.Table, rel.Relations)
	}
}

// sanitizeNodeID ensures table names are valid mermaid node IDs
func sanitizeNodeID(table string) string {
	// Replace dots and other special characters with underscores
	return strings.NewReplacer(
		".", "_",
		"-", "_",
		" ", "_",
	).Replace(table)
}

// calculateMaxDepth calculates the maximum nesting depth of relations
func calculateMaxDepth(relations []jobspec.Relation) int {
	maxDepth := 0
	for _, rel := range relations {
		depth := 1
		if len(rel.Relations) > 0 {
			childDepth := calculateMaxDepth(rel.Relations)
			depth += childDepth
		}
		if depth > maxDepth {
			maxDepth = depth
		}
	}
	return maxDepth
}
