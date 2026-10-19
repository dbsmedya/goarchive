package cmd

import (
	"strings"
	"testing"
)

const listConfig = `
source:
  host: h
  user: u
  database: s
destination:
  host: h
  user: u
  database: d
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
  copy_orders:
    job_type: copy-only
    root_table: orders
    primary_key: id
    where: "1=1"
    resume: true
    verification: count
`

// TestListJobsOutput: list-jobs reads the file with jobspec and shows each job's type, keys
// and relations at every depth, in name order, without a connection.
func TestListJobsOutput(t *testing.T) {
	cfg := writeConfig(t, listConfig)
	out, err := execute(t, "list-jobs", "-c", cfg)
	if err != nil {
		t.Fatalf("list-jobs: %v\n%s", err, out)
	}
	for _, w := range []string{
		"Jobs defined in " + cfg + ":",
		"1. archive_orders (archive)",
		"   Root Table:    orders (primary key: id)",
		"   WHERE:         placed < '2020-01-01'",
		"   Relations:     2 table(s)",
		"      - order_lines (FK: order_id -> id, 1-N)",
		"         - line_notes (FK: order_id, line_no -> order_id, line_no, 1-N)",
		"   Processing:    batch_size=50, batch_delete_size=500",
		"2. copy_orders (copy-only)",
		"   Resume:        true",
		"   Verification:  count",
		"Total: 2 job(s)",
	} {
		if !strings.Contains(out, w) {
			t.Errorf("list-jobs lacks %q:\n%s", w, out)
		}
	}
	if strings.Index(out, "archive_orders") > strings.Index(out, "copy_orders") {
		t.Error("jobs are not in name order")
	}
}

// TestListJobsRefusesA2xFile: a 2.x file fails loudly with the upgrade guide (spec §5).
func TestListJobsRefusesA2xFile(t *testing.T) {
	old := writeConfig(t, "\nsource:\n  host: h\n  user: u\n  database: s\ndestination:\n  host: h\n  user: u\n  database: d\njobs:\n  j:\n    root_table: t\n    primary_key: id\n    where: \"1=1\"\n")
	if _, err := execute(t, "list-jobs", "-c", old); err == nil || !strings.Contains(err.Error(), "docs/README_UPGRADING_3_0.md") {
		t.Errorf("list-jobs on a 2.x file = %v", err)
	}
}

func TestListJobsIsAddedToRoot(t *testing.T) {
	for _, c := range rootCmd.Commands() {
		if c.Name() == "list-jobs" {
			return
		}
	}
	t.Error("list-jobs is not a subcommand")
}
