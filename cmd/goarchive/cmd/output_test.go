package cmd

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/engine"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
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
			[]string{"--skip-dangerously-validate-preflight-allow-experimental", "storage engines", "keys are still read from MySQL", "log-key and binding checks still run",
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

// TestPrintReport (spec §3.5): the closing block leads with the status and the duration, then
// the roots, then the counts as a table in copy order, one column per kind of count.
func TestPrintReport(t *testing.T) {
	order := []string{"orders", "order_details", "shipment"}
	rep := &engine.Report{Batches: 12, Roots: 12000,
		Copied:       map[string]int64{"orders": 12000, "order_details": 40920, "shipment": 12000},
		Deleted:      map[string]int64{"orders": 12000, "order_details": 40920, "shipment": 12000},
		Verification: jobspec.VerifySHA256,
		Verified:     map[string]int64{"orders": 12000, "order_details": 40920, "shipment": 12000},
		Elapsed:      109412 * time.Millisecond}
	var out strings.Builder
	printReport(&out, "archive_old_orders", jobspec.Archive, order, rep, nil)
	want := "Job archive_old_orders (archive): completed in 109.412s\n" +
		"  roots: 12000 in 12 batches\n" +
		"\n" +
		"  table          copied  deleted  verified (sha256)\n" +
		"  orders          12000    12000              12000\n" +
		"  order_details   40920    40920              40920\n" +
		"  shipment        12000    12000              12000\n"
	if got := out.String(); got != want {
		t.Errorf("report:\n%s\nwant:\n%s", got, want)
	}

	for _, tc := range []struct {
		name       string
		typ        jobspec.Type
		rep        *engine.Report
		err        error
		want, nots []string
	}{
		{"stopped, recovered, conversions", jobspec.Archive, &engine.Report{Batches: 2, Roots: 3, Recovered: 2,
			Copied: map[string]int64{"orders": 3}, Deleted: map[string]int64{"orders": 5},
			Conversions: map[string]map[uint16]uint64{"orders": {1265: 2}}, Stopped: true, Elapsed: 20 * time.Millisecond},
			nil, []string{"Job j (archive): stopped before the end of the where range: run the job again to continue in 0.020s\n",
				"  recovered: 2 logged roots finished by replay\n", "conversions accepted (verification: none): orders 1265=2",
				"  table   copied  deleted\n", "  orders       3        5\n"}, []string{"verified"}},
		{"failed", jobspec.Archive, &engine.Report{Batches: 1, Roots: 1, Copied: map[string]int64{"orders": 1},
			Deleted: map[string]int64{"orders": 1}, Elapsed: time.Second}, errors.New("boom"),
			[]string{"Job j (archive): failed in 1.000s\n", "  roots: 1 in 1 batch\n"}, []string{"boom"}},
		{"purge", jobspec.Purge, &engine.Report{Batches: 1, Roots: 1, Deleted: map[string]int64{"orders": 1}},
			nil, []string{"  table   deleted\n", "  orders        1\n"}, []string{"copied", "verified"}},
		{"copy-only, resume, none", jobspec.CopyOnly, &engine.Report{Batches: 1, Roots: 1, Skipped: 4,
			Copied: map[string]int64{"orders": 1}, Verification: jobspec.VerifyNone},
			nil, []string{"  skipped: 4 roots this job had already copied (resume)\n", "  table   copied\n"}, []string{"deleted", "verified"}},
	} {
		out.Reset()
		printReport(&out, "j", tc.typ, []string{"orders"}, tc.rep, tc.err)
		got := out.String()
		for _, w := range tc.want {
			if !strings.Contains(got, w) {
				t.Errorf("%s: report lacks %q:\n%s", tc.name, w, got)
			}
		}
		for _, n := range tc.nots {
			if strings.Contains(got, n) {
				t.Errorf("%s: report has %q:\n%s", tc.name, n, got)
			}
		}
	}
}

// TestPrintFailureSummary (spec §3.4a): the stderr line before the error, with the duration
// when the engine returned a report, and the log file when there is one.
func TestPrintFailureSummary(t *testing.T) {
	var out strings.Builder
	printFailureSummary(&out, "j", jobspec.Archive, &engine.Report{Elapsed: 61240 * time.Millisecond}, "/var/log/g.log")
	printFailureSummary(&out, "j", jobspec.Purge, nil, "")
	want := "Job j (archive): failed in 61.240s. Log: /var/log/g.log\nJob j (purge): failed before the first batch.\n"
	if got := out.String(); got != want {
		t.Errorf("summaries = %q, want %q", got, want)
	}
}

// TestPrintStart (spec §3.5): the start line names the log file when there is one.
func TestPrintStart(t *testing.T) {
	var out strings.Builder
	printStart(&out, "j", jobspec.Archive, "/var/log/g.log")
	printStart(&out, "j", jobspec.Archive, "")
	if want := "Job j (archive): starting. Log: /var/log/g.log\nJob j (archive): starting.\n"; out.String() != want {
		t.Errorf("start lines = %q, want %q", out.String(), want)
	}
}

// TestEmitBanners (spec §3.4a, Codex R3): the banners always reach stderr in stderr's format,
// past the level and file_only, and are recorded in the log; the tee never repeats them.
func TestEmitBanners(t *testing.T) {
	const b = "WARNING: verification: none: copied rows are not compared."
	newLog := func(cfg config.LoggingConfig, tee bool) (*logger.Logger, *strings.Builder, *strings.Builder) {
		var out, errb strings.Builder
		l, err := logger.New(&cfg, logger.Options{Stdout: &out, Stderr: &errb, Tee: tee})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = l.Close() })
		return l, &out, &errb
	}
	file := filepath.Join(t.TempDir(), "run.log")

	l, out, errb := newLog(config.LoggingConfig{Level: "error", Format: "json", Output: file}, true)
	emitBanners(l, errb, []string{b})
	_ = l.Sync()
	data, _ := os.ReadFile(file)
	if out.Len() != 0 || errb.String() != b+"\n" || !strings.Contains(string(data), "verification: none") {
		t.Errorf("file log, tee, level error: stdout %q stderr %q file %q", out, errb, data)
	}

	l, out, errb = newLog(config.LoggingConfig{Level: "error", Format: "json", Output: file, FileOnly: true}, true)
	emitBanners(l, errb, []string{b})
	if errb.String() != b+"\n" || out.Len() != 0 {
		t.Errorf("file_only: stdout %q stderr %q", out, errb)
	}

	l, out, errb = newLog(config.LoggingConfig{Level: "error", Format: "json", Output: "stdout"}, false)
	emitBanners(l, errb, []string{b})
	if errb.String() != b+"\n" {
		t.Errorf("JSON stdout log: stderr %q, want the plain banner", errb)
	}

	l, out, errb = newLog(config.LoggingConfig{Level: "error", Format: "json", Output: "stderr"}, false)
	emitBanners(l, errb, []string{b})
	lines := strings.Split(strings.TrimSpace(errb.String()), "\n")
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "{") || !strings.Contains(lines[0], `"level":"warn"`) || out.Len() != 0 {
		t.Errorf("JSON stderr log at level error: stderr %q, want one JSON warn entry", errb)
	}

	l, _, errb = newLog(config.LoggingConfig{Level: "error", Format: "text", Output: "stderr"}, false)
	emitBanners(l, errb, []string{b})
	if n := strings.Count(errb.String(), "verification: none"); n != 1 || strings.Contains(errb.String(), "{") {
		t.Errorf("text stderr log: stderr %q, want one text banner", errb)
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
