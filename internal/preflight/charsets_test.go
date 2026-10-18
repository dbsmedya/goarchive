package preflight

import (
	"errors"
	"slices"
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

const columnCharsetMessage = "A job column uses a character set that GoArchive cannot read and write back exactly through its utf8mb4 session, so keys could merge and text change without notice"

// putColumn replaces column c.Name of table t on server s, as setColumn does, or appends c
// when the table has no such column.
func putColumn(s *fakeServer, t string, c validations.ColumnSpec) {
	spec := s.specs[t]
	cols := slices.Clone(spec.Columns)
	i := slices.IndexFunc(cols, func(x validations.ColumnSpec) bool { return x.Name == c.Name })
	if i < 0 {
		c.Ordinal = len(cols) + 1
		cols = append(cols, c)
	} else {
		c.Ordinal = cols[i].Ordinal
		cols[i] = c
	}
	spec.Columns = cols
	s.specs[t] = spec
}

// TestColumnCharsets (CHS-1): a job column the run reads or writes as text must use one of
// the character sets that survive the utf8mb4 session exactly. Archive and copy-only read
// every source column and write the destination's same-named ones; purge reads and binds
// only keys and foreign_key columns. No switch turns the check off.
func TestColumnCharsets(t *testing.T) {
	both := func(w *world, table string, c validations.ColumnSpec) {
		putColumn(w.src, table, c)
		putColumn(w.dst, table, c)
	}

	t.Run("cp932 key refuses every type", func(t *testing.T) {
		for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
			w := healthy(t, typ)
			w.jobYAML = withoutNotes
			both(w, "order_lines", col("line_no", "varchar(8)", "cp932 cp932_bin"))
			e := w.mustFail("COLUMN_CHARSET_CHECK")
			if !hasTable(e, "order_lines.line_no(cp932)") {
				t.Errorf("%s: tables = %v, want order_lines.line_no(cp932)", typ, e.Tables)
			}
		}
	})

	t.Run("sjis payload", func(t *testing.T) {
		for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.CopyOnly} {
			w := healthy(t, typ)
			both(w, "orders", col("body", "varchar(20)", "sjis sjis_japanese_ci"))
			e := w.mustFail("COLUMN_CHARSET_CHECK")
			if want := []string{"orders.body(sjis)", "destination:orders.body(sjis)"}; !slices.Equal(e.Tables, want) {
				t.Errorf("%s: tables = %v, want %v", typ, e.Tables, want)
			}
		}
		w := healthy(t, jobspec.Purge)
		both(w, "orders", col("body", "varchar(20)", "sjis sjis_japanese_ci"))
		w.mustPass()
	})

	t.Run("destination only", func(t *testing.T) {
		w := healthy(t, jobspec.Archive)
		putColumn(w.dst, "orders", col("code", "varchar(20)", "cp1250 cp1250_general_ci"))
		e := w.mustFail("COLUMN_CHARSET_CHECK")
		if want := []string{"destination:orders.code(cp1250)"}; !slices.Equal(e.Tables, want) {
			t.Errorf("tables = %v, want %v", e.Tables, want)
		}
	})

	t.Run("every supported set passes", func(t *testing.T) {
		for _, cs := range supportedCharsets {
			w := healthy(t, jobspec.Archive)
			coll := cs + "_bin"
			if cs == "binary" {
				coll = "binary"
			}
			both(w, "orders", col("code", "varchar(20)", cs+" "+coll))
			if _, err := w.run(); err != nil {
				t.Errorf("%s: run() = %v, want success", cs, err)
			}
		}
	})

	t.Run("collation is not judged", func(t *testing.T) {
		w := healthy(t, jobspec.Archive)
		putColumn(w.src, "orders", col("code", "varchar(20)", "latin1 latin1_bin"))
		putColumn(w.dst, "orders", col("code", "varchar(20)", "utf8mb4 utf8mb4_0900_bin"))
		_, err := w.run()
		var pe *Error
		if errors.As(err, &pe) && pe.Check == "COLUMN_CHARSET_CHECK" {
			t.Errorf("run() = %v; a supported set must pass whatever its collation", err)
		}
	})

	t.Run("destination name matched case-insensitively", func(t *testing.T) {
		w := healthy(t, jobspec.Archive)
		spec := w.dst.specs["orders"]
		spec.Columns = slices.Clone(spec.Columns)
		spec.Columns[1] = col("CODE", "varchar(20)", "cp932 cp932_bin")
		spec.Columns[1].Ordinal = 2
		w.dst.specs["orders"] = spec
		e := w.mustFail("COLUMN_CHARSET_CHECK")
		if want := []string{"destination:orders.CODE(cp932)"}; !slices.Equal(e.Tables, want) {
			t.Errorf("tables = %v, want %v", e.Tables, want)
		}
	})

	t.Run("purge reads only keys and foreign_key columns", func(t *testing.T) {
		w := healthy(t, jobspec.Purge)
		both(w, "order_lines", col("sku", "varchar(32)", "ujis ujis_bin"))
		w.mustPass()
	})

	t.Run("still refused under SkipPolicy", func(t *testing.T) {
		for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge} {
			w := healthy(t, typ)
			w.jobYAML = withoutNotes
			w.cfg.SkipPolicy = true
			both(w, "order_lines", col("line_no", "varchar(8)", "cp932 cp932_bin"))
			if e := w.mustFail("COLUMN_CHARSET_CHECK"); !hasTable(e, "order_lines.line_no(cp932)") {
				t.Errorf("%s under SkipPolicy: tables = %v, want order_lines.line_no(cp932)", typ, e.Tables)
			}
		}
	})

	t.Run("order", func(t *testing.T) {
		w := healthy(t, jobspec.Archive)
		putColumn(w.src, "orders", col("code", "varchar(20)", "cp932 cp932_bin"))
		putColumn(w.src, "line_notes", col("note_id", "varchar(8)", "sjis sjis_bin"))
		putColumn(w.dst, "order_lines", col("sku", "varchar(32)", "big5 big5_bin"))
		e := w.mustFail("COLUMN_CHARSET_CHECK")
		if want := []string{"orders.code(cp932)", "destination:order_lines.sku(big5)", "line_notes.note_id(sjis)"}; !slices.Equal(e.Tables, want) {
			t.Errorf("tables = %v, want %v", e.Tables, want)
		}
	})

	t.Run("message", func(t *testing.T) {
		w := healthy(t, jobspec.Archive)
		both(w, "orders", col("body", "varchar(20)", "sjis sjis_japanese_ci"))
		if e := w.mustFail("COLUMN_CHARSET_CHECK"); e.Message != columnCharsetMessage {
			t.Errorf("message = %q, want %q", e.Message, columnCharsetMessage)
		}
	})

	t.Run("JSON, ENUM, numbers", func(t *testing.T) {
		w := healthy(t, jobspec.Archive)
		both(w, "orders", col("doc", "json", ""))
		w.mustPass()
		w = healthy(t, jobspec.Archive)
		both(w, "orders", col("state", "enum('a','b')", "cp1251 cp1251_bin"))
		e := w.mustFail("COLUMN_CHARSET_CHECK")
		if want := []string{"orders.state(cp1251)", "destination:orders.state(cp1251)"}; !slices.Equal(e.Tables, want) {
			t.Errorf("tables = %v, want %v", e.Tables, want)
		}
	})
}
