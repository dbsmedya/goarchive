package tracking

import (
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

func loadJob(t *testing.T, body string) *jobspec.Spec {
	t.Helper()
	doc, err := jobspec.Parse([]byte("source: {host: s, user: u, database: src}\ndestination: {host: d, user: u, database: dst}\n" +
		"jobs:\n  j:\n    job_type: archive\n" + body))
	if err != nil {
		t.Fatalf("jobspec.Parse: %v", err)
	}
	s, err := doc.Job("j", "")
	if err != nil {
		t.Fatal(err)
	}
	return s
}

const tree = `    root_table: orders
    primary_key: id
    where: "created < '2020-01-01'"
    relations:
      - table: lines
        primary_key: [order_id, line_no]
        foreign_key: order_id
        relations:
          - table: notes
            primary_key: note_id
            foreign_key: [order_id, line_no]
            references: [order_id, line_no]
      - table: payments
        primary_key: id
        foreign_key: order_id
`

// TestFingerprintOf: the relations part lists every relation depth-first in config order,
// with omitted references filled in and its pairs as a set; the where part is trimmed and
// otherwise verbatim (spec §6.2, M4-2).
func TestFingerprintOf(t *testing.T) {
	f := FingerprintOf(loadJob(t, tree))
	const want = `{"relations":[{"parent":"orders","table":"lines","pairs":[["order_id","id"]]},` +
		`{"parent":"lines","table":"notes","pairs":[["line_no","line_no"],["order_id","order_id"]]},` +
		`{"parent":"orders","table":"payments","pairs":[["order_id","id"]]}],"where":"created < '2020-01-01'"}`
	if got := f.Text(); got != want {
		t.Errorf("Text() =\n %s\nwant\n %s", got, want)
	}

	same := map[string]string{
		"references written out":       strings.Replace(tree, "        foreign_key: order_id\n        relations:", "        foreign_key: order_id\n        references: id\n        relations:", 1),
		"a permuted equal pairing":     strings.Replace(tree, "foreign_key: [order_id, line_no]\n            references: [order_id, line_no]", "foreign_key: [line_no, order_id]\n            references: [line_no, order_id]", 1),
		"where with surrounding space": strings.Replace(tree, `where: "created < '2020-01-01'"`, `where: "  created < '2020-01-01' "`, 1),
		"where as a block scalar":      strings.Replace(tree, `where: "created < '2020-01-01'"`, "where: |\n      created < '2020-01-01'", 1),
	}
	for name, body := range same {
		if g := FingerprintOf(loadJob(t, body)); g.Text() != f.Text() {
			t.Errorf("%s changed the fingerprint:\n %s", name, g.Text())
		}
	}
	differs := map[string]string{
		"a relation added":    tree + "      - {table: refunds, primary_key: id, foreign_key: order_id}\n",
		"relations reordered": strings.Replace(strings.Replace(tree, "      - table: payments\n        primary_key: id\n        foreign_key: order_id\n", "", 1), "    relations:\n", "    relations:\n      - table: payments\n        primary_key: id\n        foreign_key: order_id\n", 1),
		"a swapped pairing":   strings.Replace(tree, "references: [order_id, line_no]", "references: [line_no, order_id]", 1),
		"where edited":        strings.Replace(tree, "2020-01-01", "2020-01-02", 1),
		"where respaced":      strings.Replace(tree, "created < '2020", "created <  '2020", 1),
	}
	for name, body := range differs {
		g := FingerprintOf(loadJob(t, body))
		if g.Text() == f.Text() {
			t.Errorf("%s left the fingerprint unchanged", name)
		}
	}
}

func TestFingerprintRoundTrip(t *testing.T) {
	f := FingerprintOf(loadJob(t, tree))
	g, err := parseFingerprint(f.Text())
	if err != nil || g.Text() != f.Text() || !g.sameRelations(f) {
		t.Fatalf("parseFingerprint(Text()) = %+v, %v", g, err)
	}
	// A fingerprint built by hand with nil relations stores an empty relations part.
	if got := (Fingerprint{Where: "x"}).Text(); got != `{"relations":[],"where":"x"}` {
		t.Errorf("nil relations: %s", got)
	}
	// A job without relations has an empty relations part, never a missing one.
	if got := FingerprintOf(loadJob(t, "    root_table: orders\n    primary_key: id\n    where: \"1=1\"\n")).Text(); got != `{"relations":[],"where":"1=1"}` {
		t.Errorf("no relations: %s", got)
	}
}

// TestParseFingerprintFailsClosed: a stored fingerprint that cannot be read exactly is an
// error, never a guess the binding check would compare against.
func TestParseFingerprintFailsClosed(t *testing.T) {
	for _, text := range []string{
		"", "not json", `{"relations":[],"where":"x","extra":1}`, `{"relations":[],"where":"x"} {}`, `{"where":"x"}`,
	} {
		if _, err := parseFingerprint(text); err == nil {
			t.Errorf("parseFingerprint(%q) passed", text)
		}
	}
}
