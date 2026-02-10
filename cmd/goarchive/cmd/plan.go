package cmd

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/mermaidascii"
	"github.com/spf13/cobra"
)

// outputWriter is used for printing output, can be overridden in tests
var outputWriter io.Writer = os.Stdout

// setOutputWriter sets the output writer (used for testing)
func setOutputWriter(w io.Writer) {
	outputWriter = w
}

// resetOutputWriter resets output to stdout (used for testing)
func resetOutputWriter() {
	outputWriter = os.Stdout
}

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
  - Detected table relationships

Example:
  goarchive plan --config archiver.yaml --job archive_old_orders`,
	RunE: runPlan,
}

func init() {
	planCmd.Flags().StringVarP(&planJob, "job", "j", "",
		"Job name from configuration file (required)")
	planCmd.MarkFlagRequired("job")

	rootCmd.AddCommand(planCmd)
}

func runPlan(cmd *cobra.Command, args []string) error {
	configFile := GetConfigFile()

	// Load configuration
	cfg, err := config.Load(configFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Apply CLI overrides
	overrides := GetCLIOverrides()
	cfg.ApplyOverrides(overrides.LogLevel, overrides.LogFormat, overrides.BatchSize, overrides.BatchDeleteSize, overrides.SleepSeconds, overrides.SkipVerify)

	// Find the job in configuration
	jobValue, exists := cfg.Jobs[planJob]
	if !exists {
		return fmt.Errorf("job %q not found in configuration", planJob)
	}
	job := &jobValue

	// Build dependency graph
	g, err := graph.BuildFromJob(job)
	if err != nil {
		return fmt.Errorf("failed to build dependency graph: %w", err)
	}

	// Display visual tree using mermaid-ascii
	if err := printMermaidTree(job, cfg, g); err != nil {
		return fmt.Errorf("failed to render tree: %w", err)
	}
	fmt.Println()

	// Get copy and delete orders
	copyOrder, err := g.CopyOrder()
	if err != nil {
		return fmt.Errorf("failed to generate copy order: %w", err)
	}

	deleteOrder, err := g.DeleteOrder()
	if err != nil {
		return fmt.Errorf("failed to generate delete order: %w", err)
	}

	// Print execution plan header
	printHeader("Execution Plan: %s", planJob)

	// Job overview
	fmt.Println()
	printSection("Job Overview")
	fmt.Printf("  Root Table:  %s (PK: %s)\n", job.RootTable, job.PrimaryKey)
	fmt.Printf("  Total Tables: %d\n", g.NodeCount())
	if job.Where != "" {
		fmt.Printf("  WHERE Clause: %s\n", job.Where)
	}

	// Copy order section
	fmt.Println()
	printSection("Copy Order (parent tables first)")
	for i, table := range copyOrder {
		node := g.GetNode(table)
		printOrderItem(i+1, table, node, false)
	}

	// Delete order section
	fmt.Println()
	printSection("Delete Order (child tables first)")
	for i, table := range deleteOrder {
		node := g.GetNode(table)
		printOrderItem(i+1, table, node, true)
	}

	// Relationships section
	fmt.Println()
	printSection("Detected Relationships")
	for _, edge := range g.AllEdges() {
		meta := g.GetEdgeMeta(edge.From, edge.To)
		fmt.Printf("  • %s → %s (%s) FK: %s\n",
			edge.From,
			edge.To,
			meta.DependencyType,
			meta.ForeignKey,
		)
	}

	// Get job-specific configs
	jobProcessing := job.GetJobProcessing(cfg.Processing)
	jobVerification := job.GetJobVerification(cfg.Verification)

	// Configuration section
	fmt.Println()
	printSection("Configuration")
	fmt.Printf("  Batch Size:          %d", jobProcessing.BatchSize)
	if job.Processing != nil && job.Processing.BatchSize > 0 {
		fmt.Print(" (job-specific)")
	}
	fmt.Println()
	fmt.Printf("  Batch Delete Size:   %d", jobProcessing.BatchDeleteSize)
	if job.Processing != nil && job.Processing.BatchDeleteSize > 0 {
		fmt.Print(" (job-specific)")
	}
	fmt.Println()
	fmt.Printf("  Sleep Between Batches: %.1fs", jobProcessing.SleepSeconds)
	if job.Processing != nil && job.Processing.SleepSeconds > 0 {
		fmt.Print(" (job-specific)")
	}
	fmt.Println()
	fmt.Printf("  Verification Method: %s", jobVerification.Method)
	if job.Verification != nil && job.Verification.Method != "" {
		fmt.Print(" (job-specific)")
	}
	fmt.Println()

	return nil
}

// printHeader prints a formatted header
func printHeader(format string, args ...interface{}) {
	title := fmt.Sprintf(format, args...)
	width := len(title) + 4
	fmt.Fprintln(outputWriter, strings.Repeat("=", width))
	fmt.Fprintf(outputWriter, "  %s\n", title)
	fmt.Fprintln(outputWriter, strings.Repeat("=", width))
}

// printSection prints a section header
func printSection(title string) {
	fmt.Fprintf(outputWriter, "[%s]\n", title)
	fmt.Fprintln(outputWriter, strings.Repeat("-", len(title)+2))
}

// printOrderItem prints a table in the copy/delete order list
func printOrderItem(num int, table string, node *graph.Node, isDelete bool) {
	numStr := fmt.Sprintf("[%d]", num)

	if node.IsRoot {
		fmt.Fprintf(outputWriter, "  %s %s (root)\n", numStr, table)
	} else {
		arrow := "->"
		if isDelete {
			arrow = "<-"
		}
		fmt.Fprintf(outputWriter, "  %s %s | FK: %s %s %s\n",
			numStr,
			table,
			node.ForeignKey,
			arrow,
			node.ReferenceKey,
		)
	}
}

// printMermaidTree generates and displays an ASCII tree using mermaid-ascii
func printMermaidTree(job *config.JobConfig, cfg *config.Config, g *graph.Graph) error {
	// Generate mermaid syntax for the graph
	mermaidSyntax := generateMermaidSyntax(job, cfg)

	// Render using mermaid-ascii with default config (nil uses defaults)
	output, err := mermaidascii.RenderDiagram(mermaidSyntax, nil)
	if err != nil {
		return err
	}

	// Get job-specific configs for display
	jobProcessing := job.GetJobProcessing(cfg.Processing)
	jobVerification := job.GetJobVerification(cfg.Verification)

	// Prepare Tree Summary lines
	summaryLines := []string{
		"[ Tree Summary ]",
		strings.Repeat("-", 16),
		fmt.Sprintf("Root Table:     %s", job.RootTable),
		fmt.Sprintf("Relations:      %d tables", g.NodeCount()-1),
		fmt.Sprintf("Max Depth:      %d levels", calculateMaxDepth(job.Relations)),
		fmt.Sprintf("Destination DB: %s", cfg.Destination.Database),
		"",
		"[ Processing ]",
		strings.Repeat("-", 14),
		fmt.Sprintf("Batch Size:      %d", jobProcessing.BatchSize),
		fmt.Sprintf("Batch Delete:    %d", jobProcessing.BatchDeleteSize),
		fmt.Sprintf("Sleep:           %.1fs", jobProcessing.SleepSeconds),
		"",
		"[ Verification ]",
		strings.Repeat("-", 16),
		fmt.Sprintf("Method:          %s", jobVerification.Method),
	}

	// Print header
	fmt.Println()
	printHeader("Relation Tree")
	fmt.Println()

	// Print side-by-side: mermaid on left, summary on right
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
		fmt.Fprint(outputWriter, leftPart)

		// Calculate padding needed to align right column
		spacesNeeded := leftWidth - visualWidth(leftPart) + padding
		if spacesNeeded > 0 {
			fmt.Fprint(outputWriter, strings.Repeat(" ", spacesNeeded))
		}

		// Print right content
		fmt.Fprintln(outputWriter, rightPart)
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

// generateMermaidSyntax creates mermaid graph syntax from job configuration
func generateMermaidSyntax(job *config.JobConfig, cfg *config.Config) string {
	var sb strings.Builder

	sb.WriteString("graph TD\n")

	// Add root node (just the name, mermaid-ascii will box it)
	rootID := sanitizeNodeID(job.RootTable)
	sb.WriteString(fmt.Sprintf("    %s\n", rootID))

	// Recursively add nodes and edges
	addRelationsToMermaid(&sb, job.RootTable, job.Relations, cfg)

	return sb.String()
}

// addRelationsToMermaid recursively adds relation nodes and edges to mermaid syntax
func addRelationsToMermaid(sb *strings.Builder, parentTable string, relations []config.Relation, cfg *config.Config) {
	for _, rel := range relations {
		nodeID := sanitizeNodeID(rel.Table)
		parentID := sanitizeNodeID(parentTable)

		// Add edge from parent to child with dependency type label
		sb.WriteString(fmt.Sprintf("    %s -->|%s| %s\n", parentID, rel.DependencyType, nodeID))

		// Recursively add children
		if len(rel.Relations) > 0 {
			addRelationsToMermaid(sb, rel.Table, rel.Relations, cfg)
		}
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
func calculateMaxDepth(relations []config.Relation) int {
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
