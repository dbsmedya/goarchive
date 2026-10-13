//go:build integration

package cmd

import (
	"strings"
	"testing"
)

// TestMySQLDestinationStorageEngine (destination-engine spec §7, I1–I4): through the commands
// an operator types. A non-InnoDB destination table refuses archive, copy-only, dry-run and
// validate; the bypass flag runs it with its warning; behind the bypass a BLACKHOLE child is
// still stopped by verification; purge has no destination to judge.
func TestMySQLDestinationStorageEngine(t *testing.T) {
	const bypass = "--skip-dangerously-validate-preflight-allow-experimental"
	// dstDDL is orderDDL with order_lines on another engine (no foreign key: only InnoDB keeps one).
	dstDDL := func(engine string) []string {
		return []string{orderDDL[0], "CREATE TABLE order_lines (order_id CHAR(36) NOT NULL, line_no SMALLINT NOT NULL, " +
			"sku VARCHAR(32), PRIMARY KEY (order_id, line_no)) ENGINE=" + engine}
	}
	setup := func(t *testing.T, name, engine string) fixture {
		f := newFixture(t, name, orderJobs)
		mustExec(t, f.src, orderDDL...)
		mustExec(t, f.dst, dstDDL(engine)...)
		seed(t, f)
		return f
	}

	t.Run("I1 refused", func(t *testing.T) {
		f := setup(t, "eng_refuse", "MyISAM")
		for _, args := range [][]string{
			{"archive", "--job", "archive_orders"}, {"copy-only", "--job", "copy_orders"},
			{"dry-run", "--job", "archive_orders"}, {"validate", "--job", "archive_orders"},
		} {
			out, err := execute(t, append([]string{args[0], "-c", f.config}, args[1:]...)...)
			got := out
			if err != nil {
				got += err.Error()
			}
			if err == nil || !strings.Contains(got, "DEST_STORAGE_ENGINE_CHECK") || !strings.Contains(got, "order_lines(MyISAM)") {
				t.Errorf("%v = %v\n%s, want DEST_STORAGE_ENGINE_CHECK naming order_lines(MyISAM)", args, err, out)
			}
		}
		if got := scalar(t, f.src, "SELECT COUNT(*) FROM order_lines"); got != 8 {
			t.Errorf("source lines = %d, want 8 (nothing ran)", got)
		}
	})
	t.Run("I2 bypassed", func(t *testing.T) {
		// Past preflight, the outcome is the server's: with enforce_gtid_consistency = ON
		// (this estate) MySQL refuses a MyISAM INSERT inside the copy transaction (error 1785)
		// and nothing is deleted; without it the run completes. Either way the bypass warns.
		f := setup(t, "eng_bypass", "MyISAM")
		out, err := execute(t, "archive", "-c", f.config, "--job", "archive_orders", bypass)
		if !strings.Contains(out, "WARNING: "+bypass) {
			t.Errorf("no bypass warning:\n%s", out)
		}
		lines := scalar(t, f.src, "SELECT COUNT(*) FROM order_lines")
		switch {
		case err == nil && lines != 2:
			t.Errorf("the run completed but source lines = %d, want 2", lines)
		case err != nil && strings.Contains(err.Error(), "DEST_STORAGE_ENGINE_CHECK"):
			t.Errorf("the bypass did not skip the check: %v", err)
		case err != nil && !strings.Contains(err.Error(), "Error 1785"):
			t.Errorf("the run failed past preflight with %v, want MySQL's GTID-consistency refusal (error 1785)", err)
		case err != nil && lines != 8:
			t.Errorf("the run failed (%v) but source lines = %d, want 8 (nothing deleted)", err, lines)
		}
		t.Logf("past preflight: err = %v, source lines %d", err, lines)
	})
	t.Run("I3 BLACKHOLE stopped by verification", func(t *testing.T) {
		f := setup(t, "eng_blackhole", "BLACKHOLE")
		out, err := execute(t, "archive", "-c", f.config, "--job", "archive_orders", bypass, "--verification=count")
		if err == nil || !strings.Contains(err.Error(), "count verification of order_lines failed") ||
			!strings.Contains(err.Error(), "0 destination rows") {
			t.Fatalf("archive into BLACKHOLE = %v\n%s, want the count verification of order_lines to fail on 0 destination rows", err, out)
		}
		if got := scalar(t, f.src, "SELECT COUNT(*) FROM order_lines"); got != 8 {
			t.Errorf("source lines = %d, want 8 (nothing deleted)", got)
		}
	})
	t.Run("I4 purge ignores the destination", func(t *testing.T) {
		f := setup(t, "eng_purge", "MyISAM")
		if out, err := execute(t, "purge", "-c", f.config, "--job", "purge_orders"); err != nil {
			t.Fatalf("purge: %v\n%s", err, out)
		}
	})
}
