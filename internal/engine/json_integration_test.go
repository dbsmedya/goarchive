//go:build integration

package engine

import (
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
)

// jsonHint is the start of the hint a sha256 mismatch on table jt, column j, carries (JSN-1).
const jsonHint = "jt has JSON columns (j): a JSON value copied as text can arrive changed"

// TestMySQLJSONVerification (JSN-1, Sonnet's F3): a JSON column reaches the client only as
// MySQL's printed text, and the destination's JSON parser rebuilds it. A member's type may change
// (shape, the operator's ruling): dates, times and binary arrive as strings, a DECIMAL as a
// double. A value may not: a long DECIMAL loses digits, and an 8.0 or 8.4 destination changes
// some doubles (MySQL Bug #116160). sha256 refuses a changed printed text and now says why;
// count and none cannot see it, and the operator chose them, so preflight names the JSON
// columns in a warning and the run proceeds.
func TestMySQLJSONVerification(t *testing.T) {
	const body = "\n    root_table: jt\n    primary_key: id\n    where: '1=1'\n"
	setup := func(t *testing.T, rows ...string) env {
		t.Helper()
		e := newEnv(t, "jsn")
		e.both(t, "CREATE TABLE jt (id INT PRIMARY KEY, j JSON) ENGINE=InnoDB")
		execAll(t, e.src, rows...)
		return e
	}
	archived := func(t *testing.T, e env, err error, rows int64) {
		t.Helper()
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		if s, d := count(t, e.src, "SELECT COUNT(*) FROM jt"), count(t, e.dst, "SELECT COUNT(*) FROM jt"); s != 0 || d != rows {
			t.Errorf("source %d, destination %d rows; want 0 and %d", s, d, rows)
		}
	}
	refusedWithHint := func(t *testing.T, e env, err error, rows int64) {
		t.Helper()
		ve := wantErr[*VerificationError](t, err)
		if msg := ve.Error(); !strings.Contains(msg, "hashes differ") || !strings.Contains(msg, jsonHint) {
			t.Errorf("error = %q, want hashes differ with %q", msg, jsonHint)
		}
		if s, d := count(t, e.src, "SELECT COUNT(*) FROM jt"), count(t, e.dst, "SELECT COUNT(*) FROM jt"); s != rows || d != 0 {
			t.Errorf("source %d, destination %d rows; want %d kept and none copied", s, d, rows)
		}
	}

	t.Run("plain document", func(t *testing.T) {
		e := setup(t, `INSERT INTO jt VALUES (1, '{"a": [1, "x", true, null], "b": {"c": "d"}, "n": 3}')`)
		_, err := e.run(t, e.config(t, job(t, jobspec.Archive, body)))
		archived(t, e, err, 1)
	})

	t.Run("dates, times and binary arrive as strings", func(t *testing.T) {
		e := setup(t, "INSERT INTO jt VALUES (1, JSON_OBJECT('t', CAST('2020-01-02 03:04:05.123456' AS DATETIME(6)), "+
			"'d', CAST('2020-01-02' AS DATE), 'tm', CAST('03:04:05' AS TIME), 'b', CAST('abc' AS BINARY)))")
		_, err := e.run(t, e.config(t, job(t, jobspec.Archive, body)))
		archived(t, e, err, 1)
		got := hexes(t, e.dst, "SELECT CONCAT_WS(',', JSON_TYPE(JSON_EXTRACT(j, '$.t')), JSON_TYPE(JSON_EXTRACT(j, '$.d')), "+
			"JSON_TYPE(JSON_EXTRACT(j, '$.tm')), JSON_TYPE(JSON_EXTRACT(j, '$.b'))) FROM jt")
		if !equal(got, []string{"STRING,STRING,STRING,STRING"}) {
			t.Errorf("destination member types %v, want every one STRING (the ruled shape)", got)
		}
	})

	t.Run("DECIMAL scale", func(t *testing.T) {
		e := setup(t, "INSERT INTO jt VALUES (1, JSON_OBJECT('m', 12.50))")
		_, err := e.run(t, e.config(t, job(t, jobspec.Archive, body)))
		refusedWithHint(t, e, err, 1)
	})

	t.Run("long DECIMAL", func(t *testing.T) {
		e := setup(t, "INSERT INTO jt VALUES (1, JSON_OBJECT('m', 1.234567890123456789012345678901))")
		_, err := e.run(t, e.config(t, job(t, jobspec.Archive, body)))
		refusedWithHint(t, e, err, 1)
	})

	t.Run("doubles follow the destination's parser", func(t *testing.T) {
		e := setup(t, "INSERT INTO jt VALUES (1, JSON_OBJECT('m', CAST(0.9057697559760601 AS DOUBLE))), "+
			"(2, JSON_OBJECT('m', CAST(9088544342.689999 AS DOUBLE)))")
		// 1 on 8.0.40 and 8.4.11, 0 on 9.7.1 (Task 0, jsn1_task0.out): the parser, not a version string.
		changes := count(t, e.dst, "SELECT CAST(CAST('[0.9057697559760601, 9088544342.689999]' AS JSON) AS CHAR) <> "+
			"'[0.9057697559760601, 9088544342.689999]'") == 1
		_, err := e.run(t, e.config(t, job(t, jobspec.Archive, body)))
		t.Logf("destination parser changes doubles: %v; run error %v", changes, err)
		if changes {
			refusedWithHint(t, e, err, 2)
			return
		}
		archived(t, e, err, 2)
		if got := hexes(t, e.dst, "SELECT CAST(j AS CHAR) FROM jt ORDER BY id"); !equal(got,
			[]string{`{"m": 0.9057697559760601}`, `{"m": 9088544342.689999}`}) {
			t.Errorf("destination %v, want both doubles unchanged", got)
		}
	})

	// warned runs cfg with an observed logger; preflight's JSON warning falls back to the
	// logger when no Banner is set, as in engine tests.
	warned := func(t *testing.T, e env, cfg Config) (*observer.ObservedLogs, error) {
		t.Helper()
		core, logs := observer.New(zapcore.WarnLevel)
		cfg.Logger = &logger.Logger{SugaredLogger: zap.New(core).Sugar()}
		_, err := e.run(t, cfg)
		return logs, err
	}

	for _, v := range []jobspec.Verification{jobspec.VerifyCount, jobspec.VerifyNone} {
		t.Run("archive warns under "+string(v), func(t *testing.T) {
			e := setup(t, "INSERT INTO jt VALUES (1, JSON_OBJECT('m', 12.50))")
			cfg := e.config(t, job(t, jobspec.Archive, body))
			cfg.Verification = v
			logs, err := warned(t, e, cfg)
			archived(t, e, err, 1) // the operator's choice: the run proceeds
			wantJSONWarning(t, logs, "WARNING: verification: "+string(v)+": JSON columns jt.j: ", "an archive then deletes the original")
		})
	}

	t.Run("copy-only warns under count", func(t *testing.T) {
		e := setup(t, "INSERT INTO jt VALUES (1, JSON_OBJECT('m', 12.50))")
		cfg := e.config(t, job(t, jobspec.CopyOnly, body))
		cfg.Verification = jobspec.VerifyCount
		logs, err := warned(t, e, cfg)
		if err != nil {
			t.Fatalf("copy-only: %v", err)
		}
		if s, d := count(t, e.src, "SELECT COUNT(*) FROM jt"), count(t, e.dst, "SELECT COUNT(*) FROM jt"); s != 1 || d != 1 {
			t.Errorf("source %d, destination %d rows; want 1 and 1", s, d)
		}
		wantJSONWarning(t, logs, "WARNING: verification: count: JSON columns jt.j: ", "a copy-only run keeps the changed copy")
	})

	t.Run("no JSON warning under sha256", func(t *testing.T) {
		e := setup(t, `INSERT INTO jt VALUES (1, '{"n": 3}')`)
		logs, err := warned(t, e, e.config(t, job(t, jobspec.Archive, body)))
		archived(t, e, err, 1)
		if n := logs.FilterMessageSnippet("JSON columns").Len(); n != 0 {
			t.Errorf("sha256 run logged %d JSON warning(s), want none: %v", n, logs.All())
		}
	})

	t.Run("purge is not checked", func(t *testing.T) {
		e := setup(t, "INSERT INTO jt VALUES (1, JSON_OBJECT('m', 12.50))")
		logs, err := warned(t, e, e.config(t, job(t, jobspec.Purge, body)))
		if err != nil {
			t.Fatalf("purge: %v", err)
		}
		if n := logs.FilterMessageSnippet("JSON columns").Len(); n != 0 {
			t.Errorf("purge logged %d JSON warning(s), want none", n)
		}
		if s := count(t, e.src, "SELECT COUNT(*) FROM jt"); s != 0 {
			t.Errorf("source %d rows, want 0 purged", s)
		}
	})
}

func wantJSONWarning(t *testing.T, logs *observer.ObservedLogs, parts ...string) {
	t.Helper()
	got := logs.FilterMessageSnippet("JSON columns jt.j").All()
	if len(got) != 1 {
		t.Fatalf("JSON warnings = %d, want 1: %v", len(got), logs.All())
	}
	for _, p := range parts {
		if !strings.Contains(got[0].Message, p) {
			t.Errorf("warning %q, want it to contain %q", got[0].Message, p)
		}
	}
	if !strings.Contains(got[0].Message, "MySQL Bug #116160") {
		t.Errorf("warning %q does not name MySQL Bug #116160", got[0].Message)
	}
}
