package tracking

import (
	"slices"
	"strings"
	"testing"
)

// TestStoredTables (U1, unfinished-work guard spec §4): a stored job's table set is its root
// and both ends of every relation, folded, de-duplicated and sorted, whatever the edges'
// shape; a fingerprint that cannot be read exactly, or names a table that is not a simple
// identifier, is an error, never a guess.
func TestStoredTables(t *testing.T) {
	edge := func(parent, table string) string {
		return `{"parent":"` + parent + `","table":"` + table + `","pairs":[["id","id"]]}`
	}
	fp := func(edges ...string) string {
		return `{"relations":[` + strings.Join(edges, ",") + `],"where":"1=1"}`
	}
	for _, tc := range []struct {
		name, root, fingerprint string
		want                    []string
	}{
		{"root only", "orders", fp(), []string{"orders"}},
		{"flat", "orders", fp(edge("orders", "lines"), edge("orders", "notes")), []string{"lines", "notes", "orders"}},
		{"nested", "orders", fp(edge("orders", "lines"), edge("lines", "line_notes")), []string{"line_notes", "lines", "orders"}},
		{"folded and de-duplicated", "Orders", fp(edge("ORDERS", "Lines"), edge("orders", "lines")), []string{"lines", "orders"}},
		{"not a tree: both ends still count", "orders", fp(edge("ghost", "lines")), []string{"ghost", "lines", "orders"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := storedTables(tc.root, tc.fingerprint)
			if err != nil || !slices.Equal(got, tc.want) {
				t.Errorf("storedTables(%q, %s) = %v, %v; want %v", tc.root, tc.fingerprint, got, err, tc.want)
			}
		})
	}
	for _, tc := range []struct{ name, root, fingerprint string }{
		{"not JSON", "orders", "orders"},
		{"unknown field", "orders", `{"relations":[],"where":"","tables":[]}`},
		{"trailing data", "orders", `{"relations":[],"where":""} {}`},
		{"no relations part", "orders", `{"where":""}`},
		{"blank table", "orders", fp(edge("orders", ""))},
		{"not a simple identifier", "orders", fp(edge("orders", "a-b"))},
		{"blank root", "", fp()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := storedTables(tc.root, tc.fingerprint); err == nil {
				t.Errorf("storedTables(%q, %s) = %v, want an error", tc.root, tc.fingerprint, got)
			}
		})
	}
}
