package preflight

import (
	"strings"
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// TestKeysRefuseATableWithoutAPrimaryKey: PRIMARY_KEY_CHECK names every such table.
func TestKeysRefuseATableWithoutAPrimaryKey(t *testing.T) {
	w := healthy(t, jobspec.Archive)
	for _, name := range []string{"order_lines", "line_notes"} {
		spec := w.src.specs[name]
		spec.Indexes = nil
		w.src.specs[name] = spec
	}
	e := w.mustFail("PRIMARY_KEY_CHECK")
	if !hasTable(e, "order_lines") || !hasTable(e, "line_notes") {
		t.Errorf("tables = %v, want both keyless tables", e.Tables)
	}
}

// TestKeysRefuseUnsupportedTypes: each refused key type (spec §4.4) fails with its column
// and reason, at the root and at a child.
func TestKeysRefuseUnsupportedTypes(t *testing.T) {
	for _, tc := range []struct{ table, column, typ, reason string }{
		{"orders", "id", "float", "approximate"},
		{"orders", "id", "double", "approximate"},
		{"orders", "id", "enum('a','b')", "internal index"},
		{"orders", "id", "set('a','b')", "internal index"},
		{"line_notes", "note_id", "bit(8)", "BIT"},
	} {
		t.Run(tc.typ, func(t *testing.T) {
			w := healthy(t, jobspec.Archive)
			setColumn(w.src, tc.table, col(tc.column, tc.typ, ""))
			e := w.mustFail("KEY_TYPE_UNSUPPORTED")
			if !hasTable(e, tc.table+"."+tc.column) || !hasTable(e, tc.reason) {
				t.Errorf("tables = %v, want %s.%s with %q", e.Tables, tc.table, tc.column, tc.reason)
			}
		})
	}
	t.Run("prefix part", func(t *testing.T) {
		w := healthy(t, jobspec.Archive)
		setColumn(w.src, "orders", col("id", "varchar(64)", "utf8mb4 utf8mb4_bin"))
		spec := w.src.specs["orders"]
		spec.Indexes[0].Parts[0].SubPart = 10
		e := w.mustFail("KEY_TYPE_UNSUPPORTED")
		if !hasTable(e, "prefix") {
			t.Errorf("tables = %v, want the prefix reason", e.Tables)
		}
	})
}

// TestPrimaryKeyColumns is R31: the documented primary_key must be MySQL's key exactly,
// and the message shows both keys.
func TestPrimaryKeyColumns(t *testing.T) {
	for _, tc := range []struct {
		name, check, documented, shows string
	}{
		{"a column missing", "PK_COLUMN_CHECK", "order_id", "primary_key (order_id) but the PRIMARY KEY is (order_id, line_no)"},
		{"a column added", "PK_COLUMN_CHECK", "[order_id, line_no, sku]", "primary_key (order_id, line_no, sku) but the PRIMARY KEY is (order_id, line_no)"},
		{"reordered", "PK_COLUMN_CHECK", "[line_no, order_id]", "primary_key (line_no, order_id) but the PRIMARY KEY is (order_id, line_no)"},
		{"case only", "PK_COLUMN_CASE_CHECK", "[Order_ID, line_no]", "primary_key (Order_ID, line_no) but the PRIMARY KEY is (order_id, line_no)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := healthy(t, jobspec.Archive)
			w.jobYAML = strings.Replace(withoutNotes, "primary_key: [order_id, line_no]", "primary_key: "+tc.documented, 1)
			e := w.mustFail(tc.check)
			if !hasTable(e, "order_lines("+tc.shows) {
				t.Errorf("tables = %v, want %q", e.Tables, tc.shows)
			}
		})
	}
}

// TestCaseOnlyIsReportedFirst: with a case-only table and a reordered one in the same job,
// the case-only check reports first, because its fix differs.
func TestCaseOnlyIsReportedFirst(t *testing.T) {
	w := healthy(t, jobspec.Archive)
	w.jobYAML = strings.Replace(strings.Replace(withoutNotes, "primary_key: id", "primary_key: ID", 1),
		"primary_key: [order_id, line_no]", "primary_key: [line_no, order_id]", 1)
	if e := w.mustFail("PK_COLUMN_CASE_CHECK"); !hasTable(e, "orders(primary_key (ID)") {
		t.Errorf("tables = %v", e.Tables)
	}
}

// TestSkipPolicyStillReadsKeys: --skip-dangerously-validate-preflight-allow-experimental skips policy, not the key read
// the SQL is built from (spec §8.3).
func TestSkipPolicyStillReadsKeys(t *testing.T) {
	w := healthy(t, jobspec.Archive)
	w.cfg.SkipPolicy = true
	w.src.types = map[string]string{"order_lines": "VIEW"}                                      // TABLE_EXISTENCE_CHECK skipped
	w.src.lacking = map[validations.Privilege][]string{validations.PrivilegeDelete: {"orders"}} // permission checks skipped
	w.jobYAML = strings.Replace(withoutNotes, "primary_key: [order_id, line_no]", "primary_key: order_id", 1)
	res := w.mustPass()
	if len(res.Keys["order_lines"].Columns) != 2 {
		t.Errorf("keys = %v, want MySQL's two-column key even though the documented one differs", res.Keys)
	}
	// The engine sizes destination statements by the destination's own key, so its caps are
	// computed under skip too (found by the M5 engine).
	dst := map[string]bool{}
	for _, c := range res.Caps {
		if c.Server == "destination" && c.Statement == "by key" {
			dst[c.Table] = true
		}
	}
	if len(dst) != len(res.Keys) {
		t.Errorf("destination caps under skip = %v, want one per table (%d)", dst, len(res.Keys))
	}

	setColumn(w.src, "orders", col("id", "float", ""))
	w.refused("KEY_TYPE_UNSUPPORTED")
}
