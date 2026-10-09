package preflight

import (
	"strings"
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
)

func mustColumn(t *testing.T, typ, cs string) key.Column {
	t.Helper()
	c, err := key.ColumnFromSpec("t", col("c", typ, cs))
	if err != nil {
		t.Fatalf("ColumnFromSpec(%s): %v", typ, err)
	}
	return c
}

// TestPairMismatch is spec §8.3's pairing table: each allowed pair passes and each other
// pair is refused with its reason.
func TestPairMismatch(t *testing.T) {
	const ci, bin = "utf8mb4 utf8mb4_0900_ai_ci", "utf8mb4 utf8mb4_bin"
	for _, tc := range []struct{ child, childCS, parent, parentCS, reason string }{
		{"int", "", "bigint", "", ""},
		{"tinyint unsigned", "", "bigint unsigned", "", ""},
		{"int", "", "int unsigned", "", "signedness"},
		{"decimal(10,2)", "", "decimal(30,10)", "", ""},
		{"decimal(10,0)", "", "bigint", "", "do not pair"},
		{"varchar(10)", ci, "char(36)", ci, ""},
		{"varchar(36)", bin, "varchar(36)", ci, "collation differ"},
		{"varchar(36)", "latin1 latin1_swedish_ci", "varchar(36)", ci, "charset and collation differ"},
		{"binary(16)", "", "binary(16)", "", ""},
		{"binary(16)", "", "binary(20)", "", "lengths differ"},
		{"varbinary(16)", "", "varbinary(255)", "", ""},
		{"varbinary(16)", "", "binary(16)", "", "pads with 0x00"},
		{"datetime(3)", "", "datetime(3)", "", ""},
		{"datetime", "", "datetime(6)", "", "fractional-seconds"},
		{"date", "", "datetime", "", "do not pair"},
		{"timestamp", "", "datetime", "", "do not pair"},
		{"year", "", "year", "", ""},
		{"varchar(10)", ci, "int", "", "do not pair"},
	} {
		got := pairMismatch(mustColumn(t, tc.child, tc.childCS), mustColumn(t, tc.parent, tc.parentCS))
		if (tc.reason == "") != (got == "") || !strings.Contains(got, tc.reason) {
			t.Errorf("pair %s -> %s = %q, want %q", tc.child, tc.parent, got, tc.reason)
		}
	}
}

// TestRelationKeyCheck covers the pair check through a run: a foreign_key column that is
// missing, spelled in another case, of a type no key may have, or of the wrong signedness.
func TestRelationKeyCheck(t *testing.T) {
	for _, tc := range []struct {
		name  string
		edit  func(w *world)
		shows string
	}{
		{"missing column", func(w *world) {
			w.jobYAML = strings.Replace(w.jobYAML, "foreign_key: order_id", "foreign_key: ordr_id", 1)
		}, "order_lines.ordr_id(no such column)"},
		{"case only", func(w *world) {
			w.jobYAML = strings.Replace(w.jobYAML, "foreign_key: order_id", "foreign_key: Order_Id", 1)
		}, "order_lines.Order_Id(the column is named order_id"},
		{"unsupported type", func(w *world) {
			setColumn(w.src, "line_notes", col("line_no", "float", ""))
		}, "line_notes.line_no (float) -> order_lines.line_no (smallint): approximate"},
		{"signedness", func(w *world) {
			setColumn(w.src, "order_lines", col("order_id", "bigint", ""))
			setColumn(w.src, "line_notes", col("order_id", "bigint", ""))
		}, "order_lines.order_id (bigint) -> orders.id (bigint unsigned): integer signedness differs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := healthy(t, jobspec.Archive)
			w.src.fks = nil // the declared constraints are not what this test is about
			tc.edit(w)
			e := w.mustFail("RELATION_KEY_CHECK")
			if !hasTable(e, tc.shows) {
				t.Errorf("tables = %v, want %q", e.Tables, tc.shows)
			}
		})
	}
}

// TestNullableForeignKeyPairs: a nullable foreign_key column is allowed (Review Focus 4).
// A child row with a NULL in any foreign_key column matches no parent key.
func TestNullableForeignKeyPairs(t *testing.T) {
	w := healthy(t, jobspec.Archive)
	c := col("line_no", "smallint", "")
	c.Nullable = true
	setColumns(w, "line_notes", c)
	w.mustPass()
}

func TestHasLeadingIndex(t *testing.T) {
	spec := table("c", nil, []string{"id"},
		index("ix_ab", false, "a", "b", "c"),
		validations.IndexSpec{Name: "ix_prefix", Visible: true, Parts: []validations.IndexPart{{Column: "p", SubPart: 4}, {Column: "q"}}},
		validations.IndexSpec{Name: "ix_func", Visible: true, Parts: []validations.IndexPart{{Expression: "lower(e)"}, {Column: "f"}}},
		validations.IndexSpec{Name: "ix_hidden", Parts: []validations.IndexPart{{Column: "h"}}}, // INVISIBLE
	)
	for _, tc := range []struct {
		cols []string
		want bool
	}{
		{[]string{"a"}, true},
		{[]string{"a", "b"}, true},
		{[]string{"b", "a"}, true}, // any order among the leading columns
		{[]string{"b"}, false},     // not leading
		{[]string{"a", "c"}, false},
		{[]string{"a", "b", "c", "d"}, false},
		{[]string{"id"}, true}, // PRIMARY counts
		{[]string{"p"}, false}, // a prefix part does not identify the value
		{[]string{"q", "p"}, false},
		{[]string{"f"}, false},
		{[]string{"h"}, false}, // the optimizer does not use an INVISIBLE index
	} {
		if got := hasLeadingIndex(spec, tc.cols); got != tc.want {
			t.Errorf("hasLeadingIndex(%v) = %v, want %v", tc.cols, got, tc.want)
		}
	}
}

// TestForeignKeyIndexes: a relation's foreign_key needs a supporting index, configured or
// declared, and a declared constraint without one is refused too.
func TestForeignKeyIndexes(t *testing.T) {
	w := healthy(t, jobspec.Archive)
	spec := w.src.specs["line_notes"]
	spec.Indexes = spec.Indexes[:1] // PRIMARY only
	w.src.specs["line_notes"] = spec
	e := w.mustFail("FK_INDEX_CHECK")
	if !hasTable(e, "line_notes(order_id, line_no)") {
		t.Errorf("tables = %v, want line_notes(order_id, line_no)", e.Tables)
	}

	w = healthy(t, jobspec.Archive)
	w.src.fks[0].Indexed = false
	e = w.mustFail("FK_INDEX_CHECK")
	if !hasTable(e, "order_lines(order_id)") {
		t.Errorf("tables = %v, want the declared constraint's child columns", e.Tables)
	}
}

// TestInternalForeignKeys reconciles declared constraints between job tables with the
// relations.
func TestInternalForeignKeys(t *testing.T) {
	t.Run("no relation", func(t *testing.T) {
		w := healthy(t, jobspec.Archive)
		w.src.fks = append(w.src.fks, fk("fk_notes_order", "line_notes", []string{"order_id"}, "orders", []string{"id"}))
		e := w.mustFail("INTERNAL_FK_COVERAGE")
		if !strings.Contains(e.Message, "line_notes(order_id) -> orders(id) (constraint fk_notes_order) [no relation") {
			t.Errorf("message = %s", e.Message)
		}
	})
	t.Run("a second foreign key to the same parent", func(t *testing.T) {
		w := healthy(t, jobspec.Archive)
		setColumns(w, "order_lines", col("sku", "bigint unsigned", ""))
		w.src.fks = append(w.src.fks, fk("fk_lines_order2", "order_lines", []string{"sku"}, "orders", []string{"id"}))
		e := w.mustFail("INTERNAL_FK_COVERAGE")
		if !strings.Contains(e.Message, "[the relation's foreign_key is (order_id)]") {
			t.Errorf("message = %s", e.Message)
		}
	})
	t.Run("a swapped composite pairing", func(t *testing.T) {
		// Spec §10: refused when the foreign key is declared.
		w := healthy(t, jobspec.Archive)
		setColumns(w, "order_lines", col("line_no", "bigint unsigned", ""))
		setColumns(w, "line_notes", col("line_no", "bigint unsigned", ""))
		w.jobYAML = strings.Replace(w.jobYAML, "references: [order_id, line_no]", "references: [line_no, order_id]", 1)
		e := w.mustFail("FK_PAIRING_CHECK")
		if !strings.Contains(e.Message, "[configured: order_id=line_no, line_no=order_id]") {
			t.Errorf("message = %s", e.Message)
		}
	})
	t.Run("a permuted but equal pairing", func(t *testing.T) {
		w := healthy(t, jobspec.Archive)
		w.jobYAML = strings.Replace(w.jobYAML, "foreign_key: [order_id, line_no]\n            references: [order_id, line_no]",
			"foreign_key: [line_no, order_id]\n            references: [line_no, order_id]", 1)
		w.mustPass()
	})
	t.Run("a constraint on a unique key, not the parent's key", func(t *testing.T) {
		// No references can name it: references lists the parent's key. A foreign key to a
		// unique key is not supported in 3.0 (R11), so the message says that.
		w := healthy(t, jobspec.Archive)
		w.src.fks[0] = fk("fk_lines_order", "order_lines", []string{"order_id"}, "orders", []string{"code"})
		e := w.mustFail("INTERNAL_FK_COVERAGE")
		if !strings.Contains(e.Message, "[it references (code), which is not the PRIMARY KEY of orders (id); a foreign key to a unique key is not supported in 3.0]") {
			t.Errorf("message = %s", e.Message)
		}
	})
	t.Run("self-referencing", func(t *testing.T) {
		w := healthy(t, jobspec.Archive)
		w.src.fks = append(w.src.fks, fk("fk_orders_parent", "orders", []string{"id"}, "orders", []string{"id"}))
		w.mustPass()
	})
}
