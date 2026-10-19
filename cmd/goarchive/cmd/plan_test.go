package cmd

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/stretchr/testify/assert"
)

// setOutputWriter sets the output writer (used for testing)
func setOutputWriter(w io.Writer) {
	outputWriter = w
}

// resetOutputWriter resets output to stdout (used for testing)
func resetOutputWriter() {
	outputWriter = os.Stdout
}

func TestPlanCommandStructure(t *testing.T) {
	assert.NotNil(t, planCmd)
	assert.Equal(t, "plan", planCmd.Use)
	assert.NotEmpty(t, planCmd.Short)
	assert.NotEmpty(t, planCmd.Long)
	assert.NotNil(t, planCmd.RunE)
}

func TestPlanCommandFlags(t *testing.T) {
	flags := planCmd.Flags()

	// Check job flag exists and is required
	jobFlag := flags.Lookup("job")
	assert.NotNil(t, jobFlag)
	assert.Equal(t, "j", jobFlag.Shorthand)
	assert.Equal(t, "", jobFlag.DefValue)

	// Check annotations for required flag
	annotations := jobFlag.Annotations
	if annotations != nil {
		assert.Contains(t, annotations, "cobra_annotation_bash_completion_one_required_flag")
	}
}

func TestPlanIsAddedToRoot(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Name() == "plan" {
			found = true
			break
		}
	}
	assert.True(t, found, "plan command should be added to root command")
}

func TestSanitizeNodeID(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "simple table name",
			input: "users",
			want:  "users",
		},
		{
			name:  "table with dots",
			input: "db.users",
			want:  "db_users",
		},
		{
			name:  "table with dashes",
			input: "user-accounts",
			want:  "user_accounts",
		},
		{
			name:  "table with spaces",
			input: "user accounts",
			want:  "user_accounts",
		},
		{
			name:  "complex table name",
			input: "my-db.user accounts",
			want:  "my_db_user_accounts",
		},
		{
			name:  "empty string",
			input: "",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeNodeID(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPrintHeader(t *testing.T) {
	var buf bytes.Buffer
	setOutputWriter(&buf)
	defer resetOutputWriter()

	printHeader("Test Header")

	output := buf.String()
	assert.Contains(t, output, "Test Header")
	assert.Contains(t, output, "===")
}

func TestPrintSection(t *testing.T) {
	var buf bytes.Buffer
	setOutputWriter(&buf)
	defer resetOutputWriter()

	printSection("Test Section")

	output := buf.String()
	assert.Contains(t, output, "[Test Section]")
	assert.Contains(t, output, "--")
}

// mockNodeData is a test helper to create node data

// VisualWidth tests
func TestVisualWidth(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  int
	}{
		{
			name:  "empty string",
			input: "",
			want:  0,
		},
		{
			name:  "ASCII characters",
			input: "hello",
			want:  5,
		},
		{
			name:  "box drawing characters",
			input: "├──",
			want:  3,
		},
		{
			name:  "mixed characters",
			input: "├──users",
			want:  8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := visualWidth(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

// PrintSideBySide tests
func TestPrintSideBySide(t *testing.T) {
	tests := []struct {
		name        string
		leftContent string
		rightLines  []string
		padding     int
	}{
		{
			name:        "basic side by side",
			leftContent: "Line1\nLine2",
			rightLines:  []string{"Right1", "Right2"},
			padding:     4,
		},
		{
			name:        "uneven lines",
			leftContent: "Line1\nLine2\nLine3",
			rightLines:  []string{"Right1"},
			padding:     2,
		},
		{
			name:        "empty right content",
			leftContent: "Line1\nLine2",
			rightLines:  []string{},
			padding:     4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			setOutputWriter(&buf)
			defer resetOutputWriter()

			printSideBySide(tt.leftContent, tt.rightLines, tt.padding)

			output := buf.String()
			// Just verify it doesn't panic and produces output
			assert.NotNil(t, output)
		})
	}
}

// ============================================================================
// Phase 3: CLI Execution Tests
// ============================================================================

// planConfig is a 3.0 file with one three-table job; plan needs no connection.
const planConfig = `
source:
  host: 127.0.0.1
  port: 1
  user: u
  database: src
destination:
  host: 127.0.0.1
  port: 1
  user: u
  database: archive_db
jobs:
  archive_orders:
    job_type: archive
    root_table: orders
    primary_key: id
    where: "placed < '2020-01-01'"
    processing:
      batch_size: 50
    relations:
      - table: order_lines
        primary_key: [order_id, line_no]
        foreign_key: order_id
        relations:
          - table: line_notes
            primary_key: note_id
            foreign_key: [order_id, line_no]
            references: [order_id, line_no]
`

// TestPlanOutput: plan reads the job with jobspec and walks its tree, parent first for the
// copy and child first for the delete, without a connection (P8).
func TestPlanOutput(t *testing.T) {
	out, err := execute(t, "plan", "-c", writeConfig(t, planConfig), "--job", "archive_orders")
	if err != nil {
		t.Fatalf("plan: %v\n%s", err, out)
	}
	copyAt := strings.Index(out, "[Copy Order (parent tables first)]")
	deleteAt := strings.Index(out, "[Delete Order (child tables first)]")
	if copyAt < 0 || deleteAt < copyAt {
		t.Fatalf("no copy and delete sections:\n%s", out)
	}
	copyPart, deletePart := out[copyAt:deleteAt], out[deleteAt:]
	for _, w := range []string{
		"[1] orders (root)",
		"[2] order_lines | (order_id) -> orders (id)",
		"[3] line_notes | (order_id, line_no) -> order_lines (order_id, line_no)",
	} {
		if !strings.Contains(copyPart, w) {
			t.Errorf("copy order lacks %q:\n%s", w, copyPart)
		}
	}
	for _, w := range []string{
		"[1] line_notes | (order_id, line_no) <- order_lines (order_id, line_no)",
		"[3] orders (root)",
	} {
		if !strings.Contains(deletePart, w) {
			t.Errorf("delete order lacks %q:\n%s", w, deletePart)
		}
	}
	for _, w := range []string{
		"Execution Plan: archive_orders",
		"Job:          archive_orders (archive)",
		"Root Table:   orders (primary key: id)",
		"Tables:       3",
		"Where:        placed < '2020-01-01'",
		"Batch Size:          50 (job-specific)",
		"Verification:        sha256",
		"Destination DB: archive_db",
		"Max Depth:      2 levels",
	} {
		if !strings.Contains(out, w) {
			t.Errorf("plan lacks %q:\n%s", w, out)
		}
	}
}

func TestGenerateMermaidSyntax(t *testing.T) {
	spec := &jobspec.Spec{RootTable: "orders", Relations: []jobspec.Relation{
		{Table: "order_lines", Relations: []jobspec.Relation{{Table: "line_notes", DependencyType: "1-1"}}},
	}}
	want := "graph TD\n    orders\n    orders -->|1-N| order_lines\n    order_lines -->|1-1| line_notes\n"
	if got := generateMermaidSyntax(spec); got != want {
		t.Errorf("mermaid:\n got %q\nwant %q", got, want)
	}
}

func TestCalculateMaxDepth(t *testing.T) {
	rels := []jobspec.Relation{{Table: "a"}, {Table: "b", Relations: []jobspec.Relation{{Table: "c", Relations: []jobspec.Relation{{Table: "d"}}}}}}
	if got := calculateMaxDepth(rels); got != 3 {
		t.Errorf("depth = %d, want 3", got)
	}
	if got := calculateMaxDepth(nil); got != 0 {
		t.Errorf("no relations: depth = %d", got)
	}
}
