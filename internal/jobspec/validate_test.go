package jobspec

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

// TestReferencesValidation covers spec §5's pairing rules: references is a permutation of
// the parent's key columns, foreign_key has the same count, and references may be omitted
// only for a one-column parent key.
func TestReferencesValidation(t *testing.T) {
	// The parent has a composite documented key (a, b).
	job := func(rel string) string {
		return `
jobs:
  j:
    job_type: archive
    root_table: p
    primary_key: [a, b]
    where: "1=1"
    relations:
      - table: c
        primary_key: id
` + rel
	}
	accepted := map[string][]string{
		"in key order": {"foreign_key: [p_a, p_b]\n        references: [a, b]\n", "a,b"},
		"permuted":     {"foreign_key: [p_b, p_a]\n        references: [b, a]\n", "b,a"},
	}
	for name, tc := range accepted {
		t.Run(name, func(t *testing.T) {
			d, err := parse(t, job("        "+tc[0]))
			if err != nil {
				t.Fatal(err)
			}
			if got := strings.Join(d.Jobs["j"].Relations[0].References, ","); got != tc[1] {
				t.Fatalf("references = %s, want %s", got, tc[1])
			}
		})
	}
	refused := []struct {
		name, rel, field, msg string
	}{
		{"omitted for a composite parent", "foreign_key: [p_a, p_b]\n", "references", "references is required: the parent key has 2 columns (a, b)"},
		{"a column missing", "foreign_key: [p_a]\n        references: [a]\n", "references", "must list each parent key column (a, b) exactly once"},
		{"a column repeated", "foreign_key: [p_a, p_b]\n        references: [a, a]\n", "references", `column "a" is listed twice`},
		{"a column not in the parent key", "foreign_key: [p_a, p_b]\n        references: [a, c]\n", "references", "exactly once"},
		{"a different spelling", "foreign_key: [p_a, p_b]\n        references: [A, b]\n", "references", "references (A, b) must list each parent key column (a, b)"},
		{"foreign_key shorter", "foreign_key: p_a\n        references: [a, b]\n", "foreign_key", "foreign_key has 1 columns but references has 2"},
		{"foreign_key repeated", "foreign_key: [p_a, P_A]\n        references: [a, b]\n", "foreign_key", `column "P_A" is listed twice`},
		{"foreign_key missing", "references: [a, b]\n", "foreign_key", "foreign_key is required"},
	}
	for _, tc := range refused {
		t.Run(tc.name, func(t *testing.T) {
			errs := mustFail(t, job("        "+tc.rel))
			if !hasError(errs, "jobs.j.relations[0]."+tc.field, tc.msg) {
				t.Fatalf("want %s: %q, got:\n%v", tc.field, tc.msg, errs)
			}
		})
	}
}

// TestReferencesOmittedForOneColumnParent: the filled-in value is the parent's documented
// key column, for a child and a grandchild.
func TestReferencesOmittedForOneColumnParent(t *testing.T) {
	d, err := parse(t, `
jobs:
  j:
    job_type: archive
    root_table: p
    primary_key: pid
    where: "1=1"
    relations:
      - table: c
        primary_key: cid
        foreign_key: p_id
        relations:
          - table: g
            primary_key: gid
            foreign_key: c_id
`)
	if err != nil {
		t.Fatal(err)
	}
	c := d.Jobs["j"].Relations[0]
	if !slices.Equal(c.References, Columns{"pid"}) || !slices.Equal(c.Relations[0].References, Columns{"cid"}) {
		t.Fatalf("references = %v and %v", c.References, c.Relations[0].References)
	}
}

// TestJobRequiredKeys: every required key is reported, each against its own path.
func TestJobRequiredKeys(t *testing.T) {
	errs := mustFail(t, `
jobs:
  j:
    job_type: archive
    relations:
      - foreign_key: x
`)
	for _, want := range [][2]string{
		{"jobs.j.root_table", "table name is required"},
		{"jobs.j.primary_key", "primary_key is required"},
		{"jobs.j.where", "where is required"},
		{"jobs.j.relations[0].table", "table name is required"},
		{"jobs.j.relations[0].primary_key", "primary_key is required"},
	} {
		if !hasError(errs, want[0], want[1]) {
			t.Errorf("no %s error %q:\n%v", want[0], want[1], errs)
		}
	}
	if hasError(errs, "config", UpgradeGuide) {
		t.Errorf("a 3.0 job with errors was pointed to the upgrade guide:\n%v", errs)
	}
}

func TestJobValues(t *testing.T) {
	cases := []struct {
		name, job, field, msg string
	}{
		{"unknown job_type", "job_type: archiving\n    root_table: t\n    primary_key: id\n    where: \"1=1\"\n", "job_type", `job_type "archiving" is not one of`},
		{"bad root table", "job_type: purge\n    root_table: db.t\n    primary_key: id\n    where: \"1=1\"\n", "root_table", `table "db.t" must contain only`},
		{"bad key column", "job_type: purge\n    root_table: t\n    primary_key: [id, \"x y\"]\n    where: \"1=1\"\n", "primary_key", `column "x y" must contain only`},
		{"blank where", "job_type: purge\n    root_table: t\n    primary_key: id\n    where: \"  \"\n", "where", "where is required"},
		{"bad dependency_type", "job_type: purge\n    root_table: t\n    primary_key: id\n    where: \"1=1\"\n    relations:\n      - table: c\n        primary_key: id\n        foreign_key: t_id\n        dependency_type: N-1\n", "relations[0].dependency_type", "must be '1-1' or '1-N'"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			errs := mustFail(t, "\njobs:\n  j:\n    "+tc.job)
			if !hasError(errs, "jobs.j."+tc.field, tc.msg) {
				t.Fatalf("want jobs.j.%s: %q, got:\n%v", tc.field, tc.msg, errs)
			}
		})
	}
}

// TestEachTableAppearsOnce: a table twice in one job, including the root as its own
// relation (a self-reference), is refused, as in 2.x.
func TestEachTableAppearsOnce(t *testing.T) {
	errs := mustFail(t, `
jobs:
  j:
    job_type: archive
    root_table: emp
    primary_key: id
    where: "1=1"
    relations:
      - table: Emp
        primary_key: id
        foreign_key: manager_id
      - table: a
        primary_key: id
        foreign_key: emp_id
        relations:
          - table: a
            primary_key: id
            foreign_key: a_id
`)
	if !hasError(errs, "jobs.j.relations[0].table", `table "Emp" already appears at jobs.j.root_table`) {
		t.Errorf("the root as its own relation was accepted:\n%v", errs)
	}
	if !hasError(errs, "jobs.j.relations[1].relations[0].table", `already appears at jobs.j.relations[1].table`) {
		t.Errorf("a repeated child was accepted:\n%v", errs)
	}
}

func TestRelationDepthLimit(t *testing.T) {
	var b strings.Builder
	b.WriteString("\njobs:\n  j:\n    job_type: purge\n    root_table: t0\n    primary_key: id\n    where: \"1=1\"\n")
	indent := "    "
	for i := 1; i <= 11; i++ {
		fmt.Fprintf(&b, "%srelations:\n%s  - table: t%d\n%s    primary_key: id\n%s    foreign_key: p_id\n", indent, indent, i, indent, indent)
		indent += "    "
	}
	errs := mustFail(t, b.String())
	if !strings.Contains(errs.Error(), "maximum depth of 10") {
		t.Fatalf("depth 11 was accepted:\n%v", errs)
	}
}

// TestSettingsPerJobType: each job type accepts only the settings it uses (spec §5).
func TestSettingsPerJobType(t *testing.T) {
	job := func(typ, extra string) string {
		return "\njobs:\n  j:\n    job_type: " + typ + "\n    root_table: t\n    primary_key: id\n    where: \"1=1\"\n" + extra
	}
	refused := []struct {
		name, typ, extra, field, msg string
	}{
		{"purge with verification", "purge", "    verification: sha256\n", "verification", "nothing to verify"},
		{"copy-only with batch_delete_size", "copy-only", "    processing:\n      batch_delete_size: 10\n", "processing.batch_delete_size", "deletes nothing"},
		{"copy-only with delete_sleep_seconds", "copy-only", "    processing:\n      delete_sleep_seconds: 1\n", "processing.delete_sleep_seconds", "deletes nothing"},
		{"archive with resume", "archive", "    resume: true\n", "resume", "only to copy-only"},
		{"purge with resume false", "purge", "    resume: false\n", "resume", "only to copy-only"},
		{"bad merged processing", "archive", "    processing:\n      batch_size: 0\n", "processing.batch_size", "must be positive"},
		{"bad verification", "archive", "    verification: md5\n", "verification", "must be sha256, count or none"},
		{"bad merged logging", "copy-only", "    logging:\n      format: xml\n", "logging.format", "'json' or 'text'"},
	}
	for _, tc := range refused {
		t.Run(tc.name, func(t *testing.T) {
			errs := mustFail(t, job(tc.typ, tc.extra))
			if !hasError(errs, "jobs.j."+tc.field, tc.msg) {
				t.Fatalf("want jobs.j.%s: %q, got:\n%v", tc.field, tc.msg, errs)
			}
		})
	}
	accepted := []struct {
		name, typ, extra string
	}{
		{"copy-only with resume", "copy-only", "    resume: true\n    verification: sha256\n    processing:\n      batch_size: 10\n"},
		{"archive with every setting", "archive", "    processing:\n      batch_delete_size: 10\n      delete_sleep_seconds: 1\n    verification: none\n"},
		{"purge with delete settings", "purge", "    processing:\n      batch_delete_size: 10\n"},
	}
	for _, tc := range accepted {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parse(t, job(tc.typ, tc.extra)); err != nil {
				t.Fatal(err)
			}
		})
	}
	// A global verification applies to the archive and copy-only jobs of the file; it does
	// not make a purge job in the same file invalid.
	d, err := parse(t, "\nverification: count\n"+job("purge", ""))
	if err != nil {
		t.Fatalf("a purge job beside a global verification block: %v", err)
	}
	if d.Jobs["j"].ResumeEnabled() {
		t.Fatal("resume defaults to false")
	}
}
