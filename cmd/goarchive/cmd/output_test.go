package cmd

import (
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/engine"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
)

// TestBanners (R45, P5): each setting that weakens a run's checks is printed once, before
// the run.
func TestBanners(t *testing.T) {
	plain := &jobspec.Document{Shared: config.DefaultConfig()}
	if got := banners(plain, jobspec.Archive, jobspec.VerifySHA256, false, false); len(got) != 0 {
		t.Errorf("a plain run has banners: %q", got)
	}
	fkOff := &jobspec.Document{Shared: config.DefaultConfig()}
	fkOff.Shared.Safety.DisableForeignKeyChecks = true
	for _, tc := range []struct {
		name          string
		doc           *jobspec.Document
		typ           jobspec.Type
		v             jobspec.Verification
		skip, trigger bool
		want          []string
	}{
		{"skip preflight", plain, jobspec.Archive, jobspec.VerifySHA256, true, false,
			[]string{"--skip-validate-preflight", "keys are still read from MySQL", "log-key and binding checks still run",
				"misspelled foreign_key column fails as a statement-caps error"}},
		{"force triggers", plain, jobspec.Purge, "", false, true, []string{"--force-triggers", "every delete fires them"}},
		{"no verification, archive", plain, jobspec.Archive, jobspec.VerifyNone, false, false,
			[]string{"verification: none", "copied rows are not compared", "conversion warnings are accepted", "source rows are then deleted"}},
		{"no verification, copy-only", plain, jobspec.CopyOnly, jobspec.VerifyNone, false, false,
			[]string{"verification: none", "copied rows are not compared"}},
		{"foreign key checks off", fkOff, jobspec.CopyOnly, jobspec.VerifySHA256, false, false,
			[]string{"safety.disable_foreign_key_checks", "FOREIGN_KEY_CHECKS=0"}},
	} {
		got := strings.Join(banners(tc.doc, tc.typ, tc.v, tc.skip, tc.trigger), "\n")
		for _, w := range tc.want {
			if !strings.Contains(got, w) {
				t.Errorf("%s: banners %q lack %q", tc.name, got, w)
			}
		}
	}
	if got := banners(fkOff, jobspec.Purge, "", false, false); len(got) != 0 {
		t.Errorf("purge copies nothing, so the FK-checks banner does not apply: %q", got)
	}
	if got := strings.Join(banners(plain, jobspec.CopyOnly, jobspec.VerifyNone, false, false), "\n"); strings.Contains(got, "deleted") {
		t.Errorf("copy-only deletes nothing: %q", got)
	}
}

func TestPrintReport(t *testing.T) {
	var out strings.Builder
	printReport(&out, "archive_orders", jobspec.Archive, &engine.Report{
		Batches: 2, Roots: 3, Recovered: 2,
		Copied:      map[string]int64{"orders": 3, "order_lines": 6},
		Deleted:     map[string]int64{"orders": 5, "order_lines": 10},
		Conversions: map[string]map[uint16]uint64{"orders": {1265: 2}},
		Stopped:     true,
	})
	for _, w := range []string{
		"Job archive_orders (archive): 3 roots in 2 batches",
		"recovered: 2 logged roots finished by replay",
		"copied:  order_lines=6 orders=3",
		"deleted: order_lines=10 orders=5",
		"conversions accepted (verification: none): orders 1265=2",
		"stopped before the end of the where range: run the job again to continue",
	} {
		if !strings.Contains(out.String(), w) {
			t.Errorf("report %q lacks %q", out.String(), w)
		}
	}
	out.Reset()
	printReport(&out, "copy", jobspec.CopyOnly, &engine.Report{Batches: 1, Roots: 1, Skipped: 4, Copied: map[string]int64{"t": 1}})
	if got := out.String(); !strings.Contains(got, "skipped: 4 roots this job had already copied (resume)") ||
		strings.Contains(got, "deleted") || strings.Contains(got, "recovered") || strings.Contains(got, "stopped") {
		t.Errorf("copy-only report = %q", got)
	}
}

// TestPrintReportVerifiedAndElapsed (M7b): the report names the verification method with
// the rows it passed for, per table, and the run's elapsed time. A run that compared
// nothing prints no verified line.
func TestPrintReportVerifiedAndElapsed(t *testing.T) {
	var out strings.Builder
	printReport(&out, "archive_orders", jobspec.Archive, &engine.Report{
		Batches: 2, Roots: 3,
		Copied:       map[string]int64{"orders": 3, "order_lines": 6},
		Deleted:      map[string]int64{"orders": 3, "order_lines": 6},
		Verification: jobspec.VerifySHA256,
		Verified:     map[string]int64{"orders": 3, "order_lines": 6},
		Elapsed:      1500 * time.Millisecond,
	})
	for _, w := range []string{"  verified (sha256): order_lines=6 orders=3\n", "  elapsed: 1.500s\n"} {
		if !strings.Contains(out.String(), w) {
			t.Errorf("report %q lacks %q", out.String(), w)
		}
	}
	for _, tc := range []struct {
		typ jobspec.Type
		v   jobspec.Verification
	}{{jobspec.Archive, jobspec.VerifyNone}, {jobspec.Purge, ""}} {
		out.Reset()
		printReport(&out, "j", tc.typ, &engine.Report{Batches: 1, Roots: 1, Verification: tc.v,
			Deleted: map[string]int64{"t": 1}, Elapsed: 20 * time.Millisecond})
		if got := out.String(); strings.Contains(got, "verified") || !strings.Contains(got, "  elapsed: 0.020s\n") {
			t.Errorf("%s report with verification %q = %q", tc.typ, tc.v, got)
		}
	}
}

// TestPrintCaps (M6-5): validate and dry-run show the effective statement caps.
func TestPrintCaps(t *testing.T) {
	var out strings.Builder
	printCaps(&out, []preflight.Cap{
		{Table: "orders", Server: "source", Statement: "by key", Columns: []string{"id"}, Tuples: 16000},
		{Table: "order_lines", Server: "source", Statement: "discovery", Columns: []string{"order_id"}, Tuples: 16000},
		{Table: "order_lines", Server: "destination", Statement: "by key", Columns: []string{"order_id", "line_no"}, Tuples: 8000},
	})
	for _, w := range []string{
		"Statement caps (key tuples per statement):",
		"source       by key     orders (id): 16000",
		"source       discovery  order_lines (order_id): 16000",
		"destination  by key     order_lines (order_id, line_no): 8000",
	} {
		if !strings.Contains(out.String(), w) {
			t.Errorf("caps %q lack %q", out.String(), w)
		}
	}
}

// TestPrintExplanation (spec §7.2): dry-run prints each statement and its plan under the
// EXPLAIN FORMAT=TRADITIONAL column names, then the notes.
func TestPrintExplanation(t *testing.T) {
	var out strings.Builder
	printExplanation(&out, "archive_orders", jobspec.Archive, &engine.Explanation{
		Preflight: &preflight.Result{},
		Roots:     2,
		Fetch: engine.Plan{Table: "orders", Query: "EXPLAIN FORMAT=TRADITIONAL SELECT `id` FROM `orders` WHERE (1=1\n) ORDER BY `id` LIMIT ?",
			Rows: []engine.ExplainRow{{ID: "1", SelectType: "SIMPLE", Table: "orders", Type: "index", Key: "PRIMARY", KeyLen: "144", Rows: "3", Filtered: "100.00"}}},
		Discovery: []engine.Plan{{Table: "order_lines", Query: "EXPLAIN FORMAT=TRADITIONAL SELECT ...",
			Rows: []engine.ExplainRow{{ID: "1", SelectType: "SIMPLE", Table: "order_lines", Type: "range", Key: "PRIMARY", Rows: "4", Extra: "Using where; Using index"}}}},
		Notes: []string{"A note."},
	})
	got := out.String()
	for _, w := range []string{
		"Dry-run of archive_orders (archive): the first batch holds 2 root keys",
		"Root fetch on orders:",
		"WHERE (1=1 ) ORDER BY", // the statement on one line
		"Discovery of order_lines:",
		"Using where; Using index",
		"Note: A note.",
	} {
		if !strings.Contains(got, w) {
			t.Errorf("explanation lacks %q:\n%s", w, got)
		}
	}
	// The plan is a table under the TRADITIONAL column names; spacing aside, the header and
	// the fetch's row read as below.
	header := "id select_type table partitions type possible_keys key key_len ref rows filtered Extra"
	row := "1 SIMPLE orders index PRIMARY 144 3 100.00"
	var sawHeader, sawRow bool
	for _, line := range strings.Split(got, "\n") {
		switch strings.Join(strings.Fields(line), " ") {
		case header:
			sawHeader = true
		case row:
			sawRow = true
		}
	}
	if !sawHeader || !sawRow {
		t.Errorf("no EXPLAIN table (header %v, fetch row %v):\n%s", sawHeader, sawRow, got)
	}
}
