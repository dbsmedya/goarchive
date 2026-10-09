package jobspec

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
)

// sharedYAML is a minimal valid set of shared blocks; tests append a jobs block.
const sharedYAML = `
source:
  host: src
  user: u
  database: sakila
destination:
  host: dst
  user: u
  database: archive
`

func parse(t *testing.T, jobs string) (*Document, error) {
	t.Helper()
	return Parse([]byte(sharedYAML + jobs))
}

// mustFail parses and returns the validation errors, failing the test on success.
func mustFail(t *testing.T, jobs string) config.ValidationErrors {
	t.Helper()
	d, err := parse(t, jobs)
	if err == nil {
		t.Fatalf("Parse accepted:\n%s\njobs: %v", jobs, d.JobNames())
	}
	var ve config.ValidationErrors
	if !errors.As(err, &ve) {
		t.Fatalf("Parse() = %v (%T), want config.ValidationErrors", err, err)
	}
	return ve
}

// hasError reports whether errs holds an error on field whose message contains msg.
func hasError(errs config.ValidationErrors, field, msg string) bool {
	return slices.ContainsFunc(errs, func(e config.ValidationError) bool {
		return e.Field == field && strings.Contains(e.Message, msg)
	})
}

// TestLoadSpecExample loads the example from spec §5 from a file.
func TestLoadSpecExample(t *testing.T) {
	path := filepath.Join(t.TempDir(), "archiver.yaml")
	if err := os.WriteFile(path, []byte(sharedYAML+`
jobs:
  archive_old_orders:
    job_type: archive
    root_table: orders
    primary_key: id
    where: "created_at < NOW() - INTERVAL 2 YEAR"
    relations:
      - table: order_lines
        primary_key: [order_id, line_no]
        foreign_key: order_id
        relations:
          - table: line_notes
            primary_key: [order_id, line_no, note_no]
            foreign_key: [order_id, line_no]
            references:  [order_id, line_no]
`), 0o600); err != nil {
		t.Fatal(err)
	}
	d, err := Load(path)
	if err != nil {
		t.Fatalf("Load() = %v", err)
	}
	s, err := d.Job("archive_old_orders", Archive)
	if err != nil {
		t.Fatal(err)
	}
	if s.Type != Archive || s.RootTable != "orders" || !slices.Equal(s.PrimaryKey, Columns{"id"}) {
		t.Fatalf("job = %+v", s)
	}
	lines := s.Relations[0]
	if !slices.Equal(lines.ForeignKey, Columns{"order_id"}) || !slices.Equal(lines.References, Columns{"id"}) {
		t.Fatalf("order_lines: foreign_key %v references %v; an omitted references must be filled with the parent's key", lines.ForeignKey, lines.References)
	}
	notes := lines.Relations[0]
	if !slices.Equal(notes.PrimaryKey, Columns{"order_id", "line_no", "note_no"}) || !slices.Equal(notes.References, Columns{"order_id", "line_no"}) {
		t.Fatalf("line_notes = %+v", notes)
	}
}

// TestParseKeepsDefaults: decoding over DefaultConfig keeps every default the file does
// not set, as the 2.x loader does.
func TestParseKeepsDefaults(t *testing.T) {
	d, err := parse(t, `
processing:
  batch_size: 50
jobs:
  j:
    job_type: purge
    root_table: t
    primary_key: id
    where: "1=1"
`)
	if err != nil {
		t.Fatal(err)
	}
	c := d.Shared
	if c.Source.Port != 3306 || c.Source.TLS != "preferred" || c.Processing.BatchSize != 50 || c.Processing.BatchDeleteSize != 500 ||
		c.Verification.Method != "count" || c.Logging.Format != "json" {
		t.Fatalf("defaults lost: %+v", c)
	}
	if len(c.Jobs) != 0 {
		t.Fatal("Shared.Jobs must stay empty")
	}
}

// TestParseRefusesUnknownKeys at every level, with the path and line of each.
func TestParseRefusesUnknownKeys(t *testing.T) {
	errs := mustFail(t, `
replica:
  enabled: true
safety:
  lag_threshold: 10
jobs:
  j:
    job_type: archive
    root_table: t
    primary_key: id
    where: "1=1"
    batch_size: 10
    relations:
      - table: c
        primary_key: id
        foreign_key: t_id
        foriegn_key: t_id
`)
	for _, field := range []string{"replica", "safety.lag_threshold", "jobs.j.batch_size", "jobs.j.relations[0].foriegn_key"} {
		if !hasError(errs, field, "unknown key (line ") {
			t.Errorf("no unknown-key error on %s:\n%v", field, errs)
		}
	}
	if !hasError(errs, "config", UpgradeGuide) {
		t.Errorf("no pointer to %s:\n%v", UpgradeGuide, errs)
	}
}

// TestParseRefusesA2xConfig: a 2.x job (no job_type) fails loudly with the upgrade pointer.
func TestParseRefusesA2xConfig(t *testing.T) {
	errs := mustFail(t, `
jobs:
  archive_customers:
    root_table: customer
    primary_key: customer_id
    where: "create_date < '2006-01-01'"
    relations:
      - table: rental
        primary_key: rental_id
        foreign_key: customer_id
        dependency_type: "1-N"
`)
	if !hasError(errs, "jobs.archive_customers.job_type", "job_type is required") {
		t.Errorf("missing job_type not reported:\n%v", errs)
	}
	if !hasError(errs, "config", UpgradeGuide) {
		t.Errorf("no pointer to %s:\n%v", UpgradeGuide, errs)
	}
	if len(errs) != 2 {
		t.Errorf("want exactly the job_type error and the pointer, got:\n%v", errs)
	}
}

// TestParseRefusesDuplicateKeys: a job name or a key written twice is refused, with its line.
// (2.x refused these too: viper 1.21 decodes with yaml.v3.)
func TestParseRefusesDuplicateKeys(t *testing.T) {
	for name, jobs := range map[string]string{
		"duplicate job name": `
jobs:
  j:
    job_type: purge
    root_table: t
    primary_key: id
    where: "1=1"
  j:
    job_type: archive
    root_table: t
    primary_key: id
    where: "1=1"
`,
		"duplicate job key": `
jobs:
  j:
    job_type: purge
    root_table: t
    primary_key: id
    primary_key: code
    where: "1=1"
`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parse(t, jobs)
			if err == nil || !strings.Contains(err.Error(), "already defined") {
				t.Fatalf("Parse() = %v, want a duplicate-key error", err)
			}
		})
	}
}

// TestParseFollowsAnchorsAndMergeKeys: a merge key is checked like the keys written in
// place, so an unknown key cannot hide behind an anchor.
func TestParseFollowsAnchorsAndMergeKeys(t *testing.T) {
	d, err := parse(t, `
jobs:
  base: &base
    job_type: purge
    root_table: t
    primary_key: id
    where: "1=1"
  other:
    <<: *base
    root_table: u
`)
	if err != nil {
		t.Fatalf("Parse() = %v", err)
	}
	if s := d.Jobs["other"]; s.Type != Purge || s.RootTable != "u" {
		t.Fatalf("merged job = %+v", s)
	}

	errs := mustFail(t, `
x-defaults: &defaults
  job_type: purge
jobs:
  j:
    <<: *defaults
    root_table: t
    primary_key: id
    where: "1=1"
`)
	if !hasError(errs, "x-defaults", "unknown key") {
		t.Errorf("an unknown top-level anchor holder was accepted:\n%v", errs)
	}
	errs = mustFail(t, `
jobs:
  base: &base
    job_type: purge
    root_table: t
    primary_key: id
    where: "1=1"
    sleep: 1
  other:
    <<: *base
`)
	if !hasError(errs, "jobs.other.sleep", "unknown key") {
		t.Errorf("an unknown key behind a merge key was accepted:\n%v", errs)
	}
}

// TestParseFollowsMergeKeysInTheJobsMap covers a merge key directly under jobs, whose
// operand is a map of jobs, not one job. Its jobs are checked like jobs written out, at
// their own path and line, whether the operand is a mapping, a list or an alias.
func TestParseFollowsMergeKeysInTheJobsMap(t *testing.T) {
	const job = `{job_type: copy-only, root_table: t, primary_key: id, where: "1=1", resume: true}`
	const typo = `{job_type: copy-only, root_table: t, primary_key: id, where: "1=1", resuem: true}`
	for _, tc := range []struct{ name, jobs string }{
		{"a mapping", "jobs:\n  <<: {j: " + job + "}\n"},
		{"a list", "jobs:\n  <<:\n    - j: " + job + "\n"},
		{"an alias", "jobs:\n  <<: [&m {j: " + job + "}, *m]\n"},
	} {
		t.Run("valid behind "+tc.name, func(t *testing.T) {
			d, err := parse(t, tc.jobs)
			if err != nil {
				t.Fatalf("Parse() = %v", err)
			}
			if s, ok := d.Jobs["j"]; !ok || !s.ResumeEnabled() {
				t.Fatalf("jobs = %v, want j with resume", d.JobNames())
			}
		})
	}
	for _, tc := range []struct{ name, jobs string }{
		{"a mapping", "jobs:\n  <<: {j: " + typo + "}\n"},
		{"a list", "jobs:\n  <<:\n    - j: " + typo + "\n"},
		{"an alias", "jobs:\n  <<: [&m {j: " + typo + "}, *m]\n"},
	} {
		t.Run("misspelt behind "+tc.name, func(t *testing.T) {
			errs := mustFail(t, tc.jobs)
			full := sharedYAML + tc.jobs
			line := strings.Count(full[:strings.Index(full, "resuem")], "\n") + 1
			if !hasError(errs, "jobs.j.resuem", fmt.Sprintf("unknown key (line %d)", line)) {
				t.Errorf("want jobs.j.resuem: unknown key (line %d), got:\n%v", line, errs)
			}
		})
	}
}

// TestColumnsScalarOrList covers the YAML forms of a column list.
func TestColumnsScalarOrList(t *testing.T) {
	job := func(pk string) string {
		return "\njobs:\n  j:\n    job_type: purge\n    root_table: t\n    primary_key: " + pk + "\n    where: \"1=1\"\n"
	}
	for pk, want := range map[string]Columns{
		"id":                     {"id"},
		"[id]":                   {"id"},
		"[a, b]":                 {"a", "b"},
		"\n      - a\n      - b": {"a", "b"},
		"123":                    {"123"},
	} {
		d, err := parse(t, job(pk))
		if err != nil {
			t.Errorf("primary_key: %s: %v", pk, err)
			continue
		}
		if got := d.Jobs["j"].PrimaryKey; !slices.Equal(got, want) {
			t.Errorf("primary_key: %s = %v, want %v", pk, got, want)
		}
	}
	for _, pk := range []string{"", "[]", "~"} {
		if errs := mustFail(t, job(pk)); !hasError(errs, "jobs.j.primary_key", "primary_key is required") {
			t.Errorf("primary_key: %q: %v", pk, errs)
		}
	}
	for _, pk := range []string{"[[a, b]]", "{a: b}", "[a, ~]"} {
		if errs := mustFail(t, job(pk)); !hasError(errs, "config", "column") {
			t.Errorf("primary_key: %s: %v, want a column-list error", pk, errs)
		}
	}
}

// TestJobRefusesOtherSubcommand: R4, before any connection.
func TestJobRefusesOtherSubcommand(t *testing.T) {
	d, err := parse(t, `
jobs:
  p:
    job_type: purge
    root_table: t
    primary_key: id
    where: "1=1"
`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Job("p", Purge); err != nil {
		t.Errorf("Job(p, purge) = %v", err)
	}
	if _, err := d.Job("p", ""); err != nil {
		t.Errorf("Job(p, \"\") = %v; type-neutral commands accept every job", err)
	}
	_, err = d.Job("p", Archive)
	if err == nil || !strings.Contains(err.Error(), `run it with "goarchive purge", not "goarchive archive"`) {
		t.Errorf("Job(p, archive) = %v", err)
	}
	if _, err := d.Job("missing", ""); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("Job(missing) = %v", err)
	}
}

func TestParseRequiresOneMapping(t *testing.T) {
	for _, data := range []string{"", "- a\n- b\n", "jobs: [\n"} {
		if _, err := Parse([]byte(data)); err == nil {
			t.Errorf("Parse(%q) accepted", data)
		}
	}
	if errs := mustFail(t, "\njobs: {}\n"); !hasError(errs, "jobs", "at least one job") {
		t.Errorf("an empty jobs block: %v", errs)
	}
}

func TestParseReportsSharedBlockErrors(t *testing.T) {
	errs := mustFail(t, `
logging:
  level: loud
jobs:
  j:
    job_type: purge
    root_table: t
    primary_key: id
    where: "1=1"
`)
	if !hasError(errs, "logging.level", "level must be") {
		t.Errorf("shared-block error not reported:\n%v", errs)
	}
}

// TestJobNames: the job registry's job_name column is unique regardless of case, so two
// names that differ only in case would share one registry row and one log. 2.x never met
// this because viper lowercased every job name; 3.0 keeps the name as written and refuses
// the collision, an empty name, and spaces or control characters.
func TestJobNames(t *testing.T) {
	job := func(name string) string {
		return "  " + name + ":\n    job_type: purge\n    root_table: t\n    primary_key: id\n    where: \"1=1\"\n"
	}
	errs := mustFail(t, "\njobs:\n"+job("Archive_Orders")+job("archive_orders"))
	if !hasError(errs, "jobs.archive_orders", `job name "archive_orders" differs from "Archive_Orders" only in case`) {
		t.Errorf("a case-only collision was accepted:\n%v", errs)
	}
	// Under utf8mb4_0900_ai_ci "Café" equals "cafe" and "Straße" equals "STRASSE"; Go
	// cannot fold like MySQL, so a non-ASCII name is refused outright.
	errs = mustFail(t, "\njobs:\n"+job("Straße")+job("STRASSE"))
	if !hasError(errs, "jobs.Straße", "must be ASCII") {
		t.Errorf("a non-ASCII job name was accepted:\n%v", errs)
	}
	for name, field := range map[string]string{
		`""`:                     "jobs",
		`"my job"`:               "jobs.my job",
		`"tab\tjob"`:             "jobs.tab\tjob",
		`"line\njob"`:            "jobs.line\njob",
		strings.Repeat("j", 256): "jobs." + strings.Repeat("j", 256),
		"Café":                   "jobs.Café",
		// Invisible characters would make two names look equal on screen and differ in the
		// registry, or the reverse: each is refused (operator, 2026-10-09).
		`"zero\u200bwidth"`:  "jobs.zero\u200bwidth",  // zero-width space
		`"\ufeffbom"`:        "jobs.\ufeffbom",        // byte-order mark
		`"no\u00a0break"`:    "jobs.no\u00a0break",    // no-break space
		`"soft\u00adhyphen"`: "jobs.soft\u00adhyphen", // soft hyphen
		`"rtl\u200fmark"`:    "jobs.rtl\u200fmark",    // right-to-left mark
		`"del\x7fjob"`:       "jobs.del\x7fjob",       // DEL, an ASCII control character
		`"nul\x00job"`:       "jobs.nul\x00job",       // NUL
	} {
		errs := mustFail(t, "\njobs:\n"+job(name))
		if !slices.ContainsFunc(errs, func(e config.ValidationError) bool {
			return e.Field == field && strings.Contains(e.Message, "job name")
		}) {
			t.Errorf("job name %s was accepted:\n%v", name, errs)
		}
	}
	d, err := parse(t, "\njobs:\n"+job("Archive-Orders.v2")+job(strings.Repeat("j", 255)))
	if err != nil {
		t.Fatalf("valid job names: %v", err)
	}
	if _, err := d.Job("Archive-Orders.v2", Purge); err != nil {
		t.Errorf("the job name's case must be kept: %v", err)
	}
}

// TestParseRefusesASecondDocument: yaml.Unmarshal reads only the first document, so a job
// written after a "---" line would vanish from the configuration without an error.
func TestParseRefusesASecondDocument(t *testing.T) {
	_, err := parse(t, `
jobs:
  a:
    job_type: purge
    root_table: t
    primary_key: id
    where: "1=1"
---
jobs:
  b:
    job_type: purge
    root_table: u
    primary_key: id
    where: "1=1"
`)
	if err == nil || !strings.Contains(err.Error(), "one YAML document") {
		t.Fatalf("Parse() = %v, want the one-document error", err)
	}
	// A leading "---" before the only document is still one document.
	if _, err := Parse([]byte("---\n" + sharedYAML + "jobs:\n  a:\n    job_type: purge\n    root_table: t\n    primary_key: id\n    where: \"1=1\"\n")); err != nil {
		t.Fatalf("a leading separator: %v", err)
	}
}

// TestUpgradePointerOnlyFor2xSignatures: the pointer to the upgrade guide is for files that
// show a 2.x sign (no job_type, or a key 3.0 removed), not for a 3.0 user's typo.
func TestUpgradePointerOnlyFor2xSignatures(t *testing.T) {
	const job = "\njobs:\n  j:\n    job_type: archive\n    root_table: t\n    primary_key: id\n    where: \"1=1\"\n"
	typo := mustFail(t, job+"    relations:\n      - table: c\n        primary_key: id\n        foriegn_key: t_id\n")
	if hasError(typo, "config", UpgradeGuide) {
		t.Errorf("a 3.0 typo was pointed to the upgrade guide:\n%v", typo)
	}
	for _, removed := range []string{"\nreplica:\n  enabled: false\n", "\nsafety:\n  check_interval: 5\n"} {
		if errs := mustFail(t, removed+job); !hasError(errs, "config", UpgradeGuide) {
			t.Errorf("a removed 2.x key got no pointer:\n%v", errs)
		}
	}
}

// TestParseBoundsAliasExpansion: nested aliases are refused by the decoder before the
// unknown-key walk expands them. Each level is a relation whose relations list holds eight
// aliases of the level below, so expanding level n visits 8^n relations; walking first
// took seconds per level.
func TestParseBoundsAliasExpansion(t *testing.T) {
	var b strings.Builder
	b.WriteString("\nx: &l0 {table: c, primary_key: id, foreign_key: t_id}\n")
	for level := 1; level <= 8; level++ {
		fmt.Fprintf(&b, "x%d: &l%d {table: c, primary_key: id, foreign_key: t_id, relations: [", level, level)
		for i := range 8 {
			if i > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "*l%d", level-1)
		}
		b.WriteString("]}\n")
	}
	b.WriteString("jobs:\n  j:\n    job_type: purge\n    root_table: t\n    primary_key: id\n    where: \"1=1\"\n    relations: [*l8]\n")
	start := time.Now()
	_, err := parse(t, b.String())
	if err == nil || !strings.Contains(err.Error(), "excessive aliasing") {
		t.Fatalf("Parse() = %v, want the decoder's excessive-aliasing refusal", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Parse took %v on nested aliases; the decoder's alias guard must run first", elapsed)
	}
}
