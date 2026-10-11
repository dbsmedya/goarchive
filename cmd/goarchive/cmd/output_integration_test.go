//go:build integration

package cmd

import (
	"strings"
	"testing"
)

// TestMySQLCommandOutput: what the commands print, end to end.
func TestMySQLCommandOutput(t *testing.T) {
	f := newFixture(t, "output", orderJobs)
	mustExec(t, f.src, orderDDL...)
	mustExec(t, f.dst, orderDDL...)
	seed(t, f)

	out, err := execute(t, "dry-run", "-c", f.config, "--job", "archive_orders")
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range []string{"Dry-run of archive_orders (archive): the first batch holds 2 root keys", "Root fetch on orders:",
		"EXPLAIN FORMAT=TRADITIONAL SELECT", "Discovery of order_lines:", "Statement caps (key tuples per statement):"} {
		if !strings.Contains(out, w) {
			t.Errorf("dry-run output lacks %q:\n%s", w, out)
		}
	}

	out, err = execute(t, "validate", "-c", f.config, "--job", "archive_orders")
	if err != nil || !strings.Contains(out, "PASSED") || !strings.Contains(out, "source       by key     orders (id): ") {
		t.Errorf("validate = %v:\n%s", err, out)
	}

	out, err = execute(t, "archive", "-c", f.config, "--job", "archive_orders", "--verification=none", "--progress=1h")
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range []string{"WARNING: verification: none", "progress: roots 3/~3 (100.0%) remaining=0 copied_rows=9 deleted_rows=9",
		"Job archive_orders (archive): 3 roots in 2 batches", "copied:  order_lines=6 orders=3", "deleted: order_lines=6 orders=3"} {
		if !strings.Contains(out, w) {
			t.Errorf("archive output lacks %q:\n%s", w, out)
		}
	}
}
