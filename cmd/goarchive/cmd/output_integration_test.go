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
	for _, w := range []string{"Dry-run of archive_orders (archive). No data is changed.", "  matching roots:  3, about 2 batches of 2",
		"Copy order (parents first)", "Root fetch on orders\n", "Discovery of order_lines (first discovery chunk: all 2 orders tuples)",
		"Next: goarchive archive -c "} {
		if !strings.Contains(out, w) {
			t.Errorf("dry-run output lacks %q:\n%s", w, out)
		}
	}
	if n := strings.Count(out, "Statement caps (key tuples per statement):"); n != 1 || strings.Contains(out, "Statement cap:") || strings.Contains(out, "?,") {
		t.Errorf("dry-run output: the caps %d times, logged caps or a placeholder list:\n%s", n, out)
	}

	out, err = execute(t, "validate", "-c", f.config, "--job", "archive_orders")
	if err != nil || !strings.Contains(out, "PASSED") || !strings.Contains(out, "source       by key     orders (id): ") {
		t.Errorf("validate = %v:\n%s", err, out)
	}

	out, err = execute(t, "archive", "-c", f.config, "--job", "archive_orders", "--verification=none", "--progress=1h")
	if err != nil {
		t.Fatal(err)
	}
	// The fixture logs JSON on stderr (setup C): the banner is one entry there, and
	// --progress writes its progress and closing entries past level error.
	for _, w := range []string{`"msg":"WARNING: verification: none`, `"msg":"progress"`, `"roots_done":3`,
		`"msg":"Run completed"`, `"copied":{"order_lines":6,"orders":3}`} {
		if !strings.Contains(out, w) {
			t.Errorf("archive output lacks %s:\n%s", w, out)
		}
	}
}
