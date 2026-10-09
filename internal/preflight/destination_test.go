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
		// The looser destinations an archive commonly has. Each passes silently (final
		// review Important 1): a regression that refuses one blocks a legitimate job.
		{"destination default set", "", func(w *world) {
			c, d := col("sku", "varchar(32)", "utf8mb4 utf8mb4_0900_ai_ci"), "none"
			c.Default = &d
			setColumn(w.dst, "order_lines", c)
		}, "", ""},
		{"destination drops AUTO_INCREMENT", "", func(w *world) {
			c := col("note_id", "int", "")
			c.AutoIncrement = true
			setColumn(w.src, "line_notes", c)
		}, "", ""},
		{"destination drops ON UPDATE", "", func(w *world) {
			c := col("sku", "varchar(32)", "utf8mb4 utf8mb4_0900_ai_ci")
			c.OnUpdate = true
			setColumn(w.src, "order_lines", c)
		}, "", ""},
		{"destination column invisible", "", func(w *world) {
			c := col("sku", "varchar(32)", "utf8mb4 utf8mb4_0900_ai_ci")
			c.Invisible = true
			setColumn(w.dst, "order_lines", c)
		}, "", ""},
		{"source-only generated column", "", func(w *world) {
			c := col("sku", "varchar(32)", "utf8mb4 utf8mb4_0900_ai_ci")
			c.Generated, c.GenerationExpr = validations.GeneratedStored, "upper(`sku`)"
			setColumn(w.src, "order_lines", c)
		}, "", ""},
		{"table-level engine, charset and collation", "", func(w *world) {
			spec := w.dst.specs["order_lines"]
			spec.Engine, spec.Charset, spec.Collation = "MyISAM", "latin1", "latin1_swedish_ci"
			w.dst.specs["order_lines"] = spec
		}, "", ""},
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

// TestDisposeDiffCoversEveryKind: every diff kind the library publishes has a disposition
// here, and a kind this build does not know fails closed. A library release that adds a
// kind fails this test, not an operator's run.
func TestDisposeDiffCoversEveryKind(t *testing.T) {
	const pass, warn, fatal = "pass", "warn", "fatal"
	const integrity, unexpected = "PREFLIGHT_INSPECTION_INTEGRITY", "PREFLIGHT_UNEXPECTED_DIFF"
	type want struct{ disposition, a, b string }
	cases := map[validations.SpecDiffKind]want{
		validations.ColumnAbsent:                 {fatal, "", ""},
		validations.ColumnOrderMismatch:          {fatal, "", ""},
		validations.ColumnTypeMismatch:           {fatal, "int", "varchar(3)"},
		validations.ColumnNullabilityMismatch:    {fatal, "true", "false"},
		validations.ColumnNameCaseMismatch:       {warn, "Sku", "sku"},
		validations.ColumnCharsetMismatch:        {fatal, "utf8mb4", "latin1"},
		validations.ColumnCollationMismatch:      {warn, "utf8mb4_0900_ai_ci", "utf8mb4_bin"},
		validations.ColumnDefaultMismatch:        {pass, "", ""},
		validations.ColumnAutoIncrementMismatch:  {pass, "true", "false"},
		validations.ColumnOnUpdateMismatch:       {pass, "true", "false"},
		validations.ColumnGeneratedMismatch:      {pass, "", ""},
		validations.ColumnGenerationExprMismatch: {pass, "", ""},
		validations.ColumnVisibilityMismatch:     {pass, "false", "true"},
		validations.IndexAbsent:                  {pass, "", ""},
		validations.IndexPartsMismatch:           {pass, "", ""},
		validations.IndexUniquenessMismatch:      {pass, "", ""},
		validations.IndexTypeMismatch:            {pass, "", ""},
		validations.IndexVisibilityMismatch:      {pass, "", ""},
		validations.IndexNameCaseMismatch:        {pass, "", ""},
		validations.EngineMismatch:               {pass, "InnoDB", "MyISAM"},
		validations.CharsetMismatch:              {pass, "utf8mb4", "latin1"},
		validations.CollationMismatch:            {pass, "utf8mb4_0900_ai_ci", "latin1_swedish_ci"},
		validations.IndexUnconfirmed:             {integrity, "", ""},
		validations.CommentMismatch:              {unexpected, "", ""},
		validations.CommentUnconfirmed:           {unexpected, "", ""},
		validations.ConstraintUnconfirmed:        {unexpected, "", ""},
		validations.ConstraintKindUnconfirmed:    {unexpected, "", ""},
		validations.ConstraintAbsent:             {unexpected, "", ""},
		validations.ConstraintKindMismatch:       {unexpected, "", ""},
		validations.CheckClauseMismatch:          {unexpected, "", ""},
		validations.CheckEnforcementMismatch:     {unexpected, "", ""},
		validations.ForeignKeyColumnsMismatch:    {unexpected, "", ""},
		validations.ForeignKeyReferenceMismatch:  {unexpected, "", ""},
		validations.ForeignKeyRuleMismatch:       {unexpected, "", ""},
	}
	judge := func(diff validations.SpecDiff) string {
		d, err := disposeDiff(diff, true, map[string]bool{})
		switch {
		case err != nil:
			code, _, _ := strings.Cut(err.Error(), ":")
			return code
		case d.fatal != "":
			return fatal
		case d.warning != "":
			return warn
		}
		return pass
	}
	published := validations.AllSpecDiffKinds()
	for _, kind := range published {
		w, ok := cases[kind]
		if !ok {
			t.Errorf("%v: the library publishes this kind and the test has no expected disposition; judge it in disposeDiff", kind)
			continue
		}
		if got := judge(validations.SpecDiff{Kind: kind, Column: "sku", A: w.a, B: w.b}); got != w.disposition {
			t.Errorf("%v: %s, want %s", kind, got, w.disposition)
		}
	}
	if len(cases) != len(published) {
		t.Errorf("%d expected dispositions for %d published kinds", len(cases), len(published))
	}
	// The variants of the two kinds that depend on their values.
	if got := judge(validations.SpecDiff{Kind: validations.ColumnTypeMismatch, A: "int", B: "int(11)"}); got != pass {
		t.Errorf("a display-width-only type difference: %s, want pass", got)
	}
	if got := judge(validations.SpecDiff{Kind: validations.ColumnNullabilityMismatch, A: "false", B: "true"}); got != pass {
		t.Errorf("a destination that allows NULL: %s, want pass", got)
	}
	if got := judge(validations.SpecDiff{Kind: validations.SpecDiffKind(250)}); got != "PREFLIGHT_UNKNOWN_DIFF" {
		t.Errorf("an unpublished kind: %s, want PREFLIGHT_UNKNOWN_DIFF", got)
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
