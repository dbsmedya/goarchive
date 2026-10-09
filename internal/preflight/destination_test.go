package preflight

import (
	"strings"
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// stringKeyWorld is healthy with orders keyed by a character column whose charset and
// collation each side sets.
func stringKeyWorld(t *testing.T, srcCS, dstCS string) *world {
	t.Helper()
	w := healthy(t, jobspec.Archive)
	w.jobYAML = withoutNotes
	for _, side := range []struct {
		s  *fakeServer
		cs string
	}{{w.src, srcCS}, {w.dst, dstCS}} {
		setColumn(side.s, "orders", col("id", "varchar(36)", side.cs))
		setColumn(side.s, "order_lines", col("order_id", "varchar(36)", side.cs))
	}
	w.src.fks = w.src.fks[:1]
	return w
}

// TestDestinationKeyIdentityCharsets is R32: only utf8mb3_X to utf8mb4_X is allowed, and it
// is allowed under count verification too, because the key copies exactly.
func TestDestinationKeyIdentityCharsets(t *testing.T) {
	for _, tc := range []struct {
		name, src, dst string
		ok             bool
	}{
		{"identical", "utf8mb4 utf8mb4_0900_ai_ci", "utf8mb4 utf8mb4_0900_ai_ci", true},
		{"utf8mb3 general_ci twin", "utf8mb3 utf8mb3_general_ci", "utf8mb4 utf8mb4_general_ci", true},
		{"utf8mb3 bin twin", "utf8mb3 utf8mb3_bin", "utf8mb4 utf8mb4_bin", true},
		{"utf8mb3 unicode_ci twin", "utf8mb3 utf8mb3_unicode_ci", "utf8mb4 utf8mb4_unicode_ci", true},
		{"utf8mb3 to 0900_ai_ci", "utf8mb3 utf8mb3_general_ci", "utf8mb4 utf8mb4_0900_ai_ci", false},
		{"utf8mb4 to utf8mb3", "utf8mb4 utf8mb4_general_ci", "utf8mb3 utf8mb3_general_ci", false},
		{"latin1 to utf8mb4", "latin1 latin1_swedish_ci", "utf8mb4 utf8mb4_0900_ai_ci", false},
		{"collation only", "utf8mb4 utf8mb4_0900_ai_ci", "utf8mb4 utf8mb4_bin", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := stringKeyWorld(t, tc.src, tc.dst)
			if tc.ok {
				w.mustPass()
				return
			}
			e := w.mustFail("DEST_KEY_IDENTITY_CHECK")
			if !hasTable(e, "orders(key column id: source "+strings.Replace(tc.src, " ", " ", 1)) {
				t.Errorf("tables = %v", e.Tables)
			}
		})
	}
}

// TestDestinationKeyIdentityShape: the same columns, in order, of the same type.
func TestDestinationKeyIdentityShape(t *testing.T) {
	for _, tc := range []struct {
		name  string
		edit  func(w *world)
		shows string
	}{
		{"reordered", func(w *world) {
			w.dst.specs["order_lines"].Indexes[0].Parts[0].Column, w.dst.specs["order_lines"].Indexes[0].Parts[1].Column = "line_no", "order_id"
		}, "order_lines(the destination PRIMARY KEY is (line_no, order_id) but the source's is (order_id, line_no))"},
		{"a column fewer", func(w *world) {
			spec := w.dst.specs["order_lines"]
			spec.Indexes[0].Parts = spec.Indexes[0].Parts[:1]
			w.dst.specs["order_lines"] = spec
		}, "order_lines(the destination PRIMARY KEY is (order_id)"},
		{"another integer width", func(w *world) {
			setColumn(w.dst, "orders", col("id", "int unsigned", ""))
		}, "orders(key column id: source bigint unsigned, destination int unsigned)"},
		{"an unsupported destination key type", func(w *world) {
			setColumn(w.dst, "orders", col("id", "float", ""))
		}, "orders(destination key column id is float: approximate"},
		{"no primary key", func(w *world) {
			spec := w.dst.specs["line_notes"]
			spec.Indexes = spec.Indexes[1:]
			w.dst.specs["line_notes"] = spec
		}, "line_notes(the destination has no PRIMARY KEY"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := healthy(t, jobspec.Archive)
			w.dst.specs = healthySpecs() // a fresh copy the edit may change
			tc.edit(w)
			e := w.mustFail("DEST_KEY_IDENTITY_CHECK")
			if !hasTable(e, tc.shows) {
				t.Errorf("tables = %v, want %q", e.Tables, tc.shows)
			}
		})
	}
	t.Run("case of a key column name", func(t *testing.T) {
		w := healthy(t, jobspec.Archive)
		w.dst.specs["orders"] = table("orders", []validations.ColumnSpec{
			col("ID", "bigint unsigned", ""), col("code", "varchar(20)", "utf8mb4 utf8mb4_0900_ai_ci"),
		}, []string{"ID"}, index("uk_code", true, "code"))
		w.mustPass()
		if !w.warned("name differs only in letter case") {
			t.Error("no advisory for the key column's case")
		}
	})
}

// TestDestinationSchemaCompatibility is the 2.x policy carried over: the destination may
// be looser, never stricter.
func TestDestinationSchemaCompatibility(t *testing.T) {
	const sha256 = "sha256"
	for _, tc := range []struct {
		name   string
		method string
		edit   func(w *world)
		fatal  string // "" means it passes
		warns  string
	}{
		{"display width", "", func(w *world) { setColumn(w.dst, "order_lines", col("line_no", "smallint(6)", "")) }, "", ""},
		{"type", "", func(w *world) {
			setColumn(w.dst, "order_lines", col("sku", "varchar(16)", "utf8mb4 utf8mb4_0900_ai_ci"))
		},
			"order_lines(column sku: column type mismatch", ""},
		{"destination stricter on NULL", "", func(w *world) {
			c := col("sku", "varchar(32)", "utf8mb4 utf8mb4_0900_ai_ci")
			c.Nullable = true
			setColumn(w.src, "order_lines", c)
		}, "order_lines(column sku: destination is NOT NULL but source allows NULL", ""},
		{"destination looser on NULL", "", func(w *world) {
			c := col("sku", "varchar(32)", "utf8mb4 utf8mb4_0900_ai_ci")
			c.Nullable = true
			setColumn(w.dst, "order_lines", c)
		}, "", ""},
		{"charset under count", "", func(w *world) { setColumn(w.dst, "order_lines", col("sku", "varchar(32)", "latin1 latin1_swedish_ci")) },
			"order_lines(column sku: character set mismatch", ""},
		{"charset under sha256", sha256, func(w *world) {
			setColumn(w.dst, "order_lines", col("sku", "varchar(32)", "latin1 latin1_swedish_ci"))
		}, "", "charset differs"},
		{"collation", "", func(w *world) { setColumn(w.dst, "order_lines", col("sku", "varchar(32)", "utf8mb4 utf8mb4_bin")) },
			"", "collation differs"},
		{"generated destination column", "", func(w *world) {
			c := col("sku", "varchar(32)", "utf8mb4 utf8mb4_0900_ai_ci")
			c.Generated = validations.GeneratedStored
			setColumn(w.dst, "order_lines", c)
		}, "order_lines(column sku: destination column is generated", ""},
		{"destination unique index without a source equivalent", "", func(w *world) {
			spec := w.dst.specs["order_lines"]
			spec.Indexes = append(spec.Indexes, index("uk_sku", true, "sku"))
			w.dst.specs["order_lines"] = spec
		}, `order_lines(destination unique index "uk_sku" has no equivalent`, ""},
		{"an equivalent unique index under another name", "", func(w *world) {
			spec := w.dst.specs["orders"]
			spec.Indexes = []validations.IndexSpec{spec.Indexes[0], index("uk_code_renamed", true, "code")}
			w.dst.specs["orders"] = spec
		}, "", ""},
		{"a unique index whose collation differs", "", func(w *world) {
			setColumn(w.dst, "orders", col("code", "varchar(20)", "utf8mb4 utf8mb4_bin"))
			setColumn(w.src, "orders", col("code", "varchar(20)", "utf8mb4 utf8mb4_0900_ai_ci"))
		}, `orders(destination unique index "uk_code" has no equivalent`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := healthy(t, jobspec.Archive)
			w.dst.specs = healthySpecs()
			if tc.method != "" {
				w.cfg.Verification = jobspec.Verification(tc.method)
			}
			tc.edit(w)
			if tc.fatal == "" {
				w.mustPass()
			} else if e := w.mustFail("DEST_SCHEMA_COMPATIBILITY_CHECK"); !hasTable(e, tc.fatal) {
				t.Errorf("tables = %v, want %q", e.Tables, tc.fatal)
			}
			if tc.warns != "" && !w.warned(tc.warns) {
				t.Errorf("no warning containing %q", tc.warns)
			}
		})
	}
}

// TestKeyCharsetJudgedOnce: a twin key column is the key check's to allow; the column
// policy must not refuse it as a charset mismatch under count verification.
func TestKeyCharsetJudgedOnce(t *testing.T) {
	w := stringKeyWorld(t, "utf8mb3 utf8mb3_general_ci", "utf8mb4 utf8mb4_general_ci")
	w.mustPass()
	if w.warned("orders") && w.warned("charset differs") {
		t.Error("the twin key column was reported as a charset difference")
	}
	// A non-key column with the same widening is still fatal under count verification.
	setColumn(w.src, "orders", col("code", "varchar(20)", "utf8mb3 utf8mb3_general_ci"))
	setColumn(w.dst, "orders", col("code", "varchar(20)", "utf8mb4 utf8mb4_general_ci"))
	w.refused("DEST_SCHEMA_COMPATIBILITY_CHECK")
}

// TestDefaultVerificationRelaxesCharset: a job that sets no verification gets sha256
// (R41), so a non-key charset difference warns instead of failing; none and count are
// strict.
func TestDefaultVerificationRelaxesCharset(t *testing.T) {
	w := healthy(t, jobspec.Archive)
	w.dst.specs = healthySpecs()
	w.cfg.Verification = w.spec().EffectiveVerification(jobspec.VerifySHA256)
	setColumn(w.dst, "order_lines", col("sku", "varchar(32)", "latin1 latin1_swedish_ci"))
	w.mustPass()
	if !w.warned("charset differs") {
		t.Error("no charset advisory")
	}
	for _, v := range []jobspec.Verification{jobspec.VerifyNone, jobspec.VerifyCount} {
		w.cfg.Verification = v
		w.refused("DEST_SCHEMA_COMPATIBILITY_CHECK")
	}
}

// TestUniqueIndexOverATwinColumn: a unique index over a utf8mb3_X column copied into
// utf8mb4_X collides the same rows, so the R32 twin is not a stricter destination (plan
// review I2). Any other collation change on a unique column still is.
func TestUniqueIndexOverATwinColumn(t *testing.T) {
	const mb3, mb4 = "utf8mb3 utf8mb3_general_ci", "utf8mb4 utf8mb4_general_ci"
	for _, tc := range []struct {
		name, dst  string
		functional bool
		ok         bool
	}{
		{"twin", mb4, false, true},
		{"twin, functional index", mb4, true, true},
		{"another collation", "utf8mb4 utf8mb4_0900_ai_ci", false, false},
		{"another collation, functional index", "utf8mb4 utf8mb4_0900_ai_ci", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := stringKeyWorld(t, mb3, mb4)
			w.cfg.Verification = jobspec.VerifySHA256
			setColumn(w.src, "orders", col("code", "varchar(20)", mb3))
			setColumn(w.dst, "orders", col("code", "varchar(20)", tc.dst))
			if tc.functional {
				fn := validations.IndexSpec{Name: "uk_code", Unique: true, Visible: true,
					Parts: []validations.IndexPart{{Expression: "lower(`code`)"}}}
				for _, s := range []*fakeServer{w.src, w.dst} {
					spec := s.specs["orders"]
					spec.Indexes = []validations.IndexSpec{spec.Indexes[0], fn}
					s.specs["orders"] = spec
				}
			}
			if tc.ok {
				w.mustPass()
				return
			}
			if e := w.mustFail("DEST_SCHEMA_COMPATIBILITY_CHECK"); !hasTable(e, `"uk_code"`) {
				t.Errorf("tables = %v", e.Tables)
			}
		})
	}
}
