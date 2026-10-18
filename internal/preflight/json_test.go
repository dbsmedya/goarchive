package preflight

import (
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// jsonWarning is the warning for mode v, job type typ and columns cols (JSN-1, spec §3.3).
func jsonWarning(v jobspec.Verification, typ jobspec.Type, cols string) string {
	seen := "count does not compare values"
	if v == jobspec.VerifyNone {
		seen = "none compares nothing"
	}
	effect := "an archive then deletes the original"
	if typ == jobspec.CopyOnly {
		effect = "a copy-only run keeps the changed copy"
	}
	return "WARNING: verification: " + string(v) + ": JSON columns " + cols + ": a JSON value is copied as MySQL's printed text, " +
		"so a member's type is not kept, a DECIMAL can lose digits, and an 8.0 or 8.4 destination can change a double " +
		"(MySQL Bug #116160). " + seen + ", so such a change goes unseen; " + effect +
		". See docs/README_COMPAT.md → JSON values that do not survive a text copy."
}

// TestJSONVerification (JSN-1): count and none cannot see a JSON value the destination's
// parser changed, and the operator chose them, so an archive or copy-only job names its JSON
// columns in one warning and runs. sha256 and purge are not checked.
func TestJSONVerification(t *testing.T) {
	both := func(w *world, table string, c validations.ColumnSpec) {
		putColumn(w.src, table, c)
		putColumn(w.dst, table, c)
	}
	text := col("note", "text", "utf8mb4 utf8mb4_0900_ai_ci")
	setup := func(t *testing.T, typ jobspec.Type, v jobspec.Verification) *world {
		w := healthy(t, typ)
		w.cfg.Verification = v
		return w
	}
	// warnings runs w, which must pass, and returns its JSON warnings.
	warnings := func(w *world) []string {
		w.t.Helper()
		w.mustPass()
		var out []string
		for _, e := range w.logs.FilterMessageSnippet("JSON columns").All() {
			out = append(out, e.Message)
		}
		return out
	}
	want := func(t *testing.T, got []string, msg string) {
		t.Helper()
		if len(got) != 1 || got[0] != msg {
			t.Errorf("warnings = %q,\nwant one: %q", got, msg)
		}
	}

	t.Run("warns", func(t *testing.T) {
		for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.CopyOnly} {
			for _, v := range []jobspec.Verification{jobspec.VerifyCount, jobspec.VerifyNone} {
				w := setup(t, typ, v)
				both(w, "orders", col("doc", "json", ""))
				want(t, warnings(w), jsonWarning(v, typ, "orders.doc"))
			}
		}
	})

	t.Run("child table", func(t *testing.T) {
		w := setup(t, jobspec.Archive, jobspec.VerifyCount)
		both(w, "order_lines", col("meta", "json", ""))
		want(t, warnings(w), jsonWarning(jobspec.VerifyCount, jobspec.Archive, "order_lines.meta"))
	})

	t.Run("two columns", func(t *testing.T) {
		w := setup(t, jobspec.Archive, jobspec.VerifyCount)
		both(w, "orders", col("a", "json", ""))
		both(w, "orders", col("b", "json", ""))
		want(t, warnings(w), jsonWarning(jobspec.VerifyCount, jobspec.Archive, "orders.a, orders.b"))
	})

	t.Run("uppercase type", func(t *testing.T) {
		w := setup(t, jobspec.Archive, jobspec.VerifyCount)
		both(w, "orders", col("doc", "JSON", ""))
		want(t, warnings(w), jsonWarning(jobspec.VerifyCount, jobspec.Archive, "orders.doc"))
	})

	t.Run("destination JSON only, under skip", func(t *testing.T) {
		w := setup(t, jobspec.Archive, jobspec.VerifyCount)
		w.cfg.SkipPolicy = true
		putColumn(w.src, "orders", text)
		putColumn(w.dst, "orders", col("note", "json", ""))
		want(t, warnings(w), jsonWarning(jobspec.VerifyCount, jobspec.Archive, "destination:orders.note"))
	})

	t.Run("column only on the destination", func(t *testing.T) {
		w := setup(t, jobspec.Archive, jobspec.VerifyCount)
		putColumn(w.dst, "orders", col("extra", "json", ""))
		if got := warnings(w); len(got) != 0 {
			t.Errorf("warnings = %q, want none: the column is never written", got)
		}
	})

	t.Run("under skip", func(t *testing.T) {
		w := setup(t, jobspec.Archive, jobspec.VerifyNone)
		w.cfg.SkipPolicy = true
		both(w, "orders", col("doc", "json", ""))
		want(t, warnings(w), jsonWarning(jobspec.VerifyNone, jobspec.Archive, "orders.doc"))
	})

	t.Run("quiet", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			typ  jobspec.Type
			v    jobspec.Verification
			c    validations.ColumnSpec
		}{
			{"sha256 archive", jobspec.Archive, jobspec.VerifySHA256, col("doc", "json", "")},
			{"sha256 copy-only", jobspec.CopyOnly, jobspec.VerifySHA256, col("doc", "json", "")},
			{"purge", jobspec.Purge, jobspec.VerifyCount, col("doc", "json", "")},
			{"text holding JSON", jobspec.Archive, jobspec.VerifyCount, text},
		} {
			w := setup(t, tc.typ, tc.v)
			both(w, "orders", tc.c)
			if got := warnings(w); len(got) != 0 {
				t.Errorf("%s: warnings = %q, want none", tc.name, got)
			}
		}
	})

	t.Run("banner callback", func(t *testing.T) {
		w := setup(t, jobspec.Archive, jobspec.VerifyCount)
		var got []string
		w.cfg.Banner = func(s string) { got = append(got, s) }
		both(w, "orders", col("doc", "json", ""))
		if logged := warnings(w); len(logged) != 0 {
			t.Errorf("logged %q, want the warning through Banner only", logged)
		}
		want(t, got, jsonWarning(jobspec.VerifyCount, jobspec.Archive, "orders.doc"))
	})
}
