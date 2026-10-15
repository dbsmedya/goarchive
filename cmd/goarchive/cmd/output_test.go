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

// dryRunDoc and dryRunSpec are spec §3.8's example job: orders → order_details, shipment.
func dryRunDoc() *jobspec.Document {
	doc := &jobspec.Document{Shared: config.DefaultConfig()}
	doc.Shared.Processing = config.ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500, SleepSeconds: 2,
		SentinelFile: "/run/goarchive/orders.pause"}
	doc.Shared.Replication = config.ReplicationConfig{Enabled: true, SecondsBehindSourceWithin: 10, CheckInterval: 5, CacheTTL: 15,
		Servers: []config.ReplicationServerConfig{{Host: "replica1", Port: 3306, Channels: []string{""}},
			{Host: "replica2", Port: 3306, Channels: []string{""}}}}
	return doc
}

func dryRunSpec(typ jobspec.Type) *jobspec.Spec {
	return &jobspec.Spec{Type: typ, RootTable: "orders", PrimaryKey: jobspec.Columns{"id"},
		Where: "created_at < '2024-01-01'\nAND status = 'closed'",
		Relations: []jobspec.Relation{
			{Table: "order_details", PrimaryKey: jobspec.Columns{"id"}, ForeignKey: jobspec.Columns{"order_id"}},
			{Table: "shipment", PrimaryKey: jobspec.Columns{"id"}, ForeignKey: jobspec.Columns{"order_id"}},
		}}
}

func dryRunExplanation() *engine.Explanation {
	sel := func(t string) string { return "SELECT `id` FROM `" + t + "`" }
	return &engine.Explanation{
		Preflight: &preflight.Result{Caps: []preflight.Cap{
			{Table: "orders", Server: "source", Statement: "by key", Columns: []string{"id"}, Tuples: 16000},
			{Table: "order_details", Server: "source", Statement: "discovery", Columns: []string{"order_id"}, Tuples: 16000},
		}},
		Count: 12000, Roots: 1000,
		Rows: map[string]int{"orders": 1000, "order_details": 3412, "shipment": 998},
		Fetch: engine.Plan{Table: "orders",
			Display: engine.Display{Select: sel("orders"), Where: "created_at < '2024-01-01'\nAND status = 'closed'", Tail: "ORDER BY `id` LIMIT 1000"},
			Rows:    []engine.ExplainRow{{ID: "1", SelectType: "SIMPLE", Table: "orders", Type: "range", Key: "PRIMARY", Rows: "6000", Filtered: "10.00", Extra: "Using where"}}},
		Discovery: []engine.Plan{
			{Table: "order_details", Display: engine.Display{Select: sel("order_details"), Predicate: "`order_id` IN (<1000 orders tuples>)", Tuples: 1000, Of: 1000, Parent: "orders"},
				Rows: []engine.ExplainRow{{ID: "1", SelectType: "SIMPLE", Table: "order_details", Type: "range", Key: "idx_order_id", Rows: "3400", Filtered: "100.00", Extra: "Using where; Using index"}}},
			{Table: "shipment", Display: engine.Display{Select: sel("shipment"), Predicate: "`order_id` IN (<1000 orders tuples>)", Tuples: 1000, Of: 1000, Parent: "orders"},
				Rows: []engine.ExplainRow{{ID: "1", SelectType: "SIMPLE", Table: "shipment", Type: "ALL", Rows: "250000", Filtered: "0.40", Extra: "Using where"}}},
		},
	}
}

// dryRunGolden is spec §3.8's report for the example job.
var dryRunGolden = strings.Join([]string{
	"Dry-run of archive_old_orders (archive). No data is changed.",
	"",
	"Job",
	"  root table:      orders, key (id)",
	"  where:           created_at < '2024-01-01'",
	"                   AND status = 'closed'",
	"  matching roots:  12000, about 12 batches of 1000 (counted now; the range can change before the run)",
	"",
	"Copy order (parents first)     rows in the first batch",
	"  1. orders                    1000",
	"  2. order_details             3412",
	"  3. shipment                  998",
	"Delete order (children first)",
	"  1. shipment",
	"  2. order_details",
	"  3. orders",
	"",
	"Settings",
	"  batch_size: 1000   batch_delete_size: 500   sleep_seconds: 2   delete_sleep_seconds: 0",
	"  sentinel_file: /run/goarchive/orders.pause",
	"  verification: sha256",
	"  destination foreign key checks: on",
	"  replication lag gate: on, hold above 10s, check every 5s, cache 15s",
	"    replica1:3306  channels: <default>",
	"    replica2:3306  channels: <default>",
	"",
	"Statement caps (key tuples per statement):",
	"  source       by key     orders (id): 16000",
	"  source       discovery  order_details (order_id): 16000",
	"",
	"Plans for the first batch (EXPLAIN FORMAT=TRADITIONAL)",
	"",
	"Root fetch on orders",
	"  SELECT `id` FROM `orders`",
	"  WHERE (created_at < '2024-01-01'",
	"         AND status = 'closed')",
	"  ORDER BY `id` LIMIT 1000",
	"  orders           range on PRIMARY, ~6000 rows, 10% filtered; Using where",
	"",
	"Discovery of order_details (first discovery chunk: all 1000 orders tuples)",
	"  SELECT `id` FROM `order_details`",
	"  WHERE `order_id` IN (<1000 orders tuples>)",
	"  order_details    range on idx_order_id, ~3400 rows, 100% filtered; Using where; Using index",
	"",
	"Discovery of shipment (first discovery chunk: all 1000 orders tuples)",
	"  SELECT `id` FROM `shipment`",
	"  WHERE `order_id` IN (<1000 orders tuples>)",
	"  shipment         full table scan, possible keys: none, ~250000 rows, 0.4% filtered; Using where",
	"",
	"Next: goarchive archive -c archiver.yaml -j archive_old_orders",
	"",
}, "\n")

// TestPrintExplanation (spec §3.8): the report to 2.2.x's standard, byte for byte for the
// spec's example: coverage, orders with the first batch's rows, settings, caps once, then one
// readable section per statement.
func TestPrintExplanation(t *testing.T) {
	var out strings.Builder
	printExplanation(&out, "archive_old_orders", jobspec.Archive, "archiver.yaml", dryRunDoc(), dryRunSpec(jobspec.Archive), jobspec.VerifySHA256, dryRunExplanation())
	if got := out.String(); got != dryRunGolden {
		t.Errorf("report:\n%s\nwant:\n%s", got, dryRunGolden)
	}
}

// TestPrintExplanationCases (spec §3.8): per job type, and the report's edge cases.
func TestPrintExplanationCases(t *testing.T) {
	render := func(typ jobspec.Type, v jobspec.Verification, doc *jobspec.Document, spec *jobspec.Spec, ex *engine.Explanation) string {
		var out strings.Builder
		printExplanation(&out, "j", typ, "c.yaml", doc, spec, v, ex)
		return out.String()
	}
	check := func(name, got string, want, not []string) {
		for _, w := range want {
			if !strings.Contains(got, w) {
				t.Errorf("%s: report lacks %q:\n%s", name, w, got)
			}
		}
		for _, n := range not {
			if strings.Contains(got, n) {
				t.Errorf("%s: report has %q:\n%s", name, n, got)
			}
		}
	}

	purge := render(jobspec.Purge, "", dryRunDoc(), dryRunSpec(jobspec.Purge), dryRunExplanation())
	check("purge", purge, []string{"Delete order (children first)", "  1. shipment", "998", "Next: goarchive purge -c c.yaml -j j"},
		[]string{"Copy order", "verification:", "foreign key checks"})

	doc := dryRunDoc()
	doc.Shared.Replication.Servers[0].Channels = []string{"a\nb"}
	resume := true
	cspec := dryRunSpec(jobspec.CopyOnly)
	cspec.Resume = &resume
	copyOnly := render(jobspec.CopyOnly, jobspec.VerifyCount, doc, cspec, dryRunExplanation())
	check("copy-only", copyOnly, []string{"Copy order (parents first)", "replication lag gate: configured, not used (copy-only deletes nothing)",
		"resume: on", "verification: count"}, []string{"Delete order", "replica1"})

	gated := render(jobspec.Archive, jobspec.VerifySHA256, doc, dryRunSpec(jobspec.Archive), dryRunExplanation())
	check("channel", gated, []string{"replica1:3306  channels: a b\n"}, []string{"a\nb"})

	ex := dryRunExplanation()
	ex.Discovery[0].Display.Tuples, ex.Discovery[0].Display.Of = 1000, 3412
	ex.Discovery[1] = engine.Plan{Table: "shipment", Skipped: "the first batch has no orders rows"}
	ex.Fetch.Notes = []string{"The root fetch on orders sorts its matching rows on every batch (Using filesort)."}
	sp := dryRunSpec(jobspec.Archive)
	sp.Where = "note <> '?'\n  AND id IN (SELECT id FROM keep) -- old"
	ex.Fetch.Display.Where = sp.Where
	got := render(jobspec.Archive, jobspec.VerifySHA256, dryRunDoc(), sp, ex)
	check("edge", got, []string{
		"Discovery of order_details (first discovery chunk: 1000 of 3412 orders tuples)",
		"Discovery of shipment\n  Note: the first batch has no orders rows, so this table's discovery was not explained.\n",
		"  ORDER BY `id` LIMIT 1000\n  orders           range on PRIMARY, ~6000 rows, 10% filtered; Using where\n  Note: The root fetch on orders sorts",
		"  WHERE (note <> '?'\n           AND id IN (SELECT id FROM keep) -- old\n  )\n",
	}, nil)

	none := dryRunExplanation()
	none.Count, none.Roots, none.Discovery = 0, 0, nil
	none.Rows = map[string]int{"orders": 0}
	check("no rows", render(jobspec.Archive, jobspec.VerifySHA256, dryRunDoc(), dryRunSpec(jobspec.Archive), none),
		[]string{"matching roots:  0 (counted now", "Note: no rows match the where clause, so there are no keys to bind: discovery was not explained."},
		[]string{"Discovery of"})
}

// TestDescribeRow (spec §3.8, Codex F4): one EXPLAIN row per line, by access type; a row
// without a table is the optimizer's message, never a scan.
func TestDescribeRow(t *testing.T) {
	for _, tc := range []struct {
		row  engine.ExplainRow
		want string
	}{
		{engine.ExplainRow{SelectType: "SIMPLE", Table: "t", Type: "ALL", PossibleKeys: "ix_a,ix_b", Rows: "10", Filtered: "100.00"},
			"t                full table scan, possible keys: ix_a,ix_b, ~10 rows, 100% filtered"},
		{engine.ExplainRow{SelectType: "SIMPLE", Table: "t", Type: "index", Key: "PRIMARY", Rows: "10", Filtered: "33.33"},
			"t                full index scan on PRIMARY, ~10 rows, 33.33% filtered"},
		{engine.ExplainRow{SelectType: "DEPENDENT SUBQUERY", Table: "keeper", Type: "ref", Key: "ix_b", Rows: "2", Filtered: "100"},
			"[DEPENDENT SUBQUERY] keeper           ref on ix_b, ~2 rows, 100% filtered"},
		{engine.ExplainRow{ID: "1", SelectType: "SIMPLE", Extra: "Impossible WHERE"}, "optimizer: Impossible WHERE"},
	} {
		if got := describeRow(tc.row); got != tc.want {
			t.Errorf("describeRow(%+v) =\n %q\nwant\n %q", tc.row, got, tc.want)
		}
	}
}

// TestDescribeChannels (ported from 2.2.x's estimator test): all channels, the default
// channel, and a name folded onto one line.
func TestDescribeChannels(t *testing.T) {
	for _, tc := range []struct {
		in   []string
		want string
	}{{nil, "all"}, {[]string{""}, "<default>"}, {[]string{"", "ch2"}, "<default>, ch2"}, {[]string{"a\r\nb"}, "a  b"}} {
		if got := describeChannels(tc.in); got != tc.want {
			t.Errorf("describeChannels(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestDryRunHelp (spec §3.9): the help names the whole-range count and its cost.
func TestDryRunHelp(t *testing.T) {
	for _, w := range []string{"counts the rows", "takes as long as that count takes", "first discovery chunk"} {
		if !strings.Contains(dryrunCmd.Long, w) {
			t.Errorf("dry-run help lacks %q", w)
		}
	}
}
