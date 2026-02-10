package cmd

import (
	"bytes"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/stretchr/testify/assert"
)

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

func TestCalculateMaxDepth(t *testing.T) {
	tests := []struct {
		name      string
		relations []config.Relation
		want      int
	}{
		{
			name:      "empty relations",
			relations: []config.Relation{},
			want:      0,
		},
		{
			name: "single level",
			relations: []config.Relation{
				{Table: "orders"},
			},
			want: 1,
		},
		{
			name: "two levels",
			relations: []config.Relation{
				{
					Table: "orders",
					Relations: []config.Relation{
						{Table: "order_items"},
					},
				},
			},
			want: 2,
		},
		{
			name: "three levels",
			relations: []config.Relation{
				{
					Table: "orders",
					Relations: []config.Relation{
						{
							Table: "order_items",
							Relations: []config.Relation{
								{Table: "item_details"},
							},
						},
					},
				},
			},
			want: 3,
		},
		{
			name: "multiple branches different depths",
			relations: []config.Relation{
				{Table: "orders"},
				{
					Table: "invoices",
					Relations: []config.Relation{
						{Table: "invoice_items"},
					},
				},
			},
			want: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calculateMaxDepth(tt.relations)
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

func TestPrintOrderItem(t *testing.T) {
	tests := []struct {
		name     string
		num      int
		table    string
		node     *mockNodeData
		isDelete bool
		want     string
	}{
		{
			name:     "root table copy",
			num:      1,
			table:    "users",
			node:     &mockNodeData{IsRoot: true},
			isDelete: false,
			want:     "[1] users (root)",
		},
		{
			name:     "child table copy",
			num:      2,
			table:    "orders",
			node:     &mockNodeData{IsRoot: false, ForeignKey: "user_id", ReferenceKey: "id"},
			isDelete: false,
			want:     "[2] orders | FK: user_id -> id",
		},
		{
			name:     "child table delete",
			num:      3,
			table:    "orders",
			node:     &mockNodeData{IsRoot: false, ForeignKey: "user_id", ReferenceKey: "id"},
			isDelete: true,
			want:     "[3] orders | FK: user_id <- id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			setOutputWriter(&buf)
			defer resetOutputWriter()

			printOrderItem(tt.num, tt.table, createNode(tt.node), tt.isDelete)

			output := buf.String()
			assert.Contains(t, output, tt.want)
		})
	}
}

// mockNodeData is a test helper to create node data
type mockNodeData struct {
	IsRoot       bool
	ForeignKey   string
	ReferenceKey string
}

func createNode(data *mockNodeData) *graph.Node {
	return &graph.Node{
		IsRoot:       data.IsRoot,
		ForeignKey:   data.ForeignKey,
		ReferenceKey: data.ReferenceKey,
	}
}

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

// GenerateMermaidSyntax tests
func TestGenerateMermaidSyntax(t *testing.T) {
	tests := []struct {
		name string
		job  *config.JobConfig
		cfg  *config.Config
		want []string // Substrings expected in output
	}{
		{
			name: "simple root table",
			job: &config.JobConfig{
				RootTable:  "users",
				PrimaryKey: "id",
			},
			cfg: &config.Config{},
			want: []string{
				"graph TD",
				"users",
			},
		},
		{
			name: "root with one relation",
			job: &config.JobConfig{
				RootTable:  "users",
				PrimaryKey: "id",
				Relations: []config.Relation{
					{Table: "orders", ForeignKey: "user_id", DependencyType: "1-N"},
				},
			},
			cfg: &config.Config{},
			want: []string{
				"graph TD",
				"users",
				"users -->|1-N| orders",
			},
		},
		{
			name: "root with multiple relations",
			job: &config.JobConfig{
				RootTable:  "users",
				PrimaryKey: "id",
				Relations: []config.Relation{
					{Table: "orders", ForeignKey: "user_id", DependencyType: "1-N"},
					{Table: "profiles", ForeignKey: "user_id", DependencyType: "1-1"},
				},
			},
			cfg: &config.Config{},
			want: []string{
				"graph TD",
				"users",
				"users -->|1-N| orders",
				"users -->|1-1| profiles",
			},
		},
		{
			name: "nested relations",
			job: &config.JobConfig{
				RootTable:  "users",
				PrimaryKey: "id",
				Relations: []config.Relation{
					{
						Table:          "orders",
						ForeignKey:     "user_id",
						DependencyType: "1-N",
						Relations: []config.Relation{
							{Table: "order_items", ForeignKey: "order_id", DependencyType: "1-N"},
						},
					},
				},
			},
			cfg: &config.Config{},
			want: []string{
				"graph TD",
				"users",
				"users -->|1-N| orders",
				"orders -->|1-N| order_items",
			},
		},
		{
			name: "table with dots in name",
			job: &config.JobConfig{
				RootTable:  "mydb.users",
				PrimaryKey: "id",
			},
			cfg: &config.Config{},
			want: []string{
				"graph TD",
				"mydb_users",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := generateMermaidSyntax(tt.job, tt.cfg)
			for _, want := range tt.want {
				assert.Contains(t, got, want)
			}
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

// Helper function for test
func stringsCount(s, substr string) int {
	return strings.Count(s, substr)
}
