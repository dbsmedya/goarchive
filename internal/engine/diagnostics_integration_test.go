//go:build integration

package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// nonStrict replaces e's destination pool with one whose sessions run with an empty
// sql_mode, so a conversion is a warning on the INSERT instead of an error. params adds
// session variables.
func (e env) nonStrict(t *testing.T, params map[string]string) env {
	t.Helper()
	p := map[string]string{"sql_mode": "''"}
	for k, v := range params {
		p[k] = v
	}
	e.dst = openServer(t, "TEST_DEST", e.dstSchema, p)
	return e
}

// text reads one value of the destination as text, for a converted-value assertion.
func text(t *testing.T, e env, q string) string {
	t.Helper()
	var s string
	if err := e.dst.QueryRowContext(context.Background(), q).Scan(&s); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return s
}

// untouched asserts that a refused run left the source's n rows of table in place, the
// destination table empty and nothing in the job's log.
func untouched(t *testing.T, e env, table string, n int64) {
	t.Helper()
	if got := count(t, e.src, "SELECT COUNT(*) FROM "+table); got != n {
		t.Errorf("source %s = %d rows, want %d kept", table, got, n)
	}
	if got := count(t, e.dst, "SELECT COUNT(*) FROM "+table); got != 0 {
		t.Errorf("destination %s = %d rows, want 0: the batch's transaction rolls back", table, got)
	}
	if st := statuses(t, e.dst, e.dstSchema); len(st) != 0 {
		t.Errorf("log = %v, want nothing logged", st)
	}
}

// TestMySQLInsertDiagnostics (M7-3, G20): INSERT diagnostics on a real server. The
// destination session is non-strict and its column narrower than the source's, so the
// strict INSERT writes a converted value with one warning per row; SkipPolicy, because
// preflight refuses the type change. Two rows per batch, one INSERT.
// Pair: verification count or sha256 → INSERT_DIAGNOSTIC_REJECTED naming the code, the
// destination empty, nothing logged, the source kept; verification none, the one change →
// archived, Report.Conversions counts both warnings and the destination holds the converted
// value. For 1264 (out of range), 1265 (truncated) and 1366 (incorrect value).
func TestMySQLInsertDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		code     uint16
		src, dst string // the column n on each side
		seed     string // both rows' n
		want     string // the destination's n under verification none
	}{
		{1264, "n INT NOT NULL", "n TINYINT NOT NULL", "999", "127"},
		{1265, "n VARCHAR(10) NOT NULL", "n VARCHAR(3) NOT NULL", "'abcdefgh'", "abc"},
		{1366, "n VARCHAR(10) NOT NULL", "n INT NOT NULL", "'abc'", "0"},
	} {
		for _, v := range []jobspec.Verification{jobspec.VerifyCount, jobspec.VerifySHA256, jobspec.VerifyNone} {
			t.Run(fmt.Sprintf("%d/%s", tc.code, v), func(t *testing.T) {
				e := newEnv(t, "diag")
				execAll(t, e.src, "CREATE TABLE r (id BIGINT NOT NULL PRIMARY KEY, "+tc.src+") ENGINE=InnoDB",
					fmt.Sprintf("INSERT INTO r VALUES (1, %s), (2, %s)", tc.seed, tc.seed))
				execAll(t, e.dst, "CREATE TABLE r (id BIGINT NOT NULL PRIMARY KEY, "+tc.dst+") ENGINE=InnoDB")
				e = e.nonStrict(t, nil)
				cfg := e.config(t, job(t, jobspec.Archive, "\n    root_table: r\n    primary_key: id\n    where: \"1=1\"\n"))
				cfg.Verification, cfg.SkipPolicy = v, true
				rep, err := e.run(t, cfg)
				if v != jobspec.VerifyNone {
					de := wantErr[*insertDiagnosticError](t, err)
					if de.Identifier != "INSERT_DIAGNOSTIC_REJECTED" || de.Table != "r" || de.Count != 2 || de.Retained != 2 ||
						de.Reason != fmt.Sprintf("condition %d is not allowed", tc.code) ||
						len(de.Preview) != 2 || de.Preview[0].Code != tc.code || de.Preview[0].Level != "Warning" {
						t.Errorf("refusal = %+v, want INSERT_DIAGNOSTIC_REJECTED on r for two %d warnings", de, tc.code)
					}
					untouched(t, e, "r", 2)
					return
				}
				if err != nil {
					t.Fatalf("Run under verification none: %v", err)
				}
				if got := rep.Conversions["r"]; len(got) != 1 || got[tc.code] != 2 {
					t.Errorf("Report.Conversions[r] = %v, want {%d: 2}", got, tc.code)
				}
				if got := text(t, e, "SELECT GROUP_CONCAT(CAST(n AS CHAR) ORDER BY id) FROM r"); got != tc.want+","+tc.want {
					t.Errorf("destination n = %q, want %q twice", got, tc.want)
				}
				if got := count(t, e.src, "SELECT COUNT(*) FROM r"); got != 0 {
					t.Errorf("source r = %d rows, want 0 archived", got)
				}
				if st := statuses(t, e.dst, e.dstSchema); len(st) != 1 || st[2] != 2 {
					t.Errorf("log = %v, want 2 completed", st)
				}
			})
		}
	}
}

// TestMySQLInsertDiagnosticsLateTable (M7-3, G20): a conversion on the child table, whose
// INSERT runs after the root's in the same destination transaction, rolls the root's rows
// back too. Verification count, non-strict destination session, SkipPolicy.
// Pair: the child's value out of the destination's range → INSERT_DIAGNOSTIC_REJECTED on c,
// the destination's r and c empty, nothing logged; the value in range, the one change → the
// batch archived, both tables copied.
func TestMySQLInsertDiagnosticsLateTable(t *testing.T) {
	for _, tc := range []struct {
		name string
		n    int
	}{{"out of range", 999}, {"in range", 99}} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, "diaglate")
			execAll(t, e.src,
				"CREATE TABLE r (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB",
				"CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, r_id BIGINT NOT NULL, n INT NOT NULL, KEY ix_r (r_id), "+
					"CONSTRAINT fk_c_r FOREIGN KEY (r_id) REFERENCES r (id)) ENGINE=InnoDB",
				"INSERT INTO r VALUES (1), (2)",
				fmt.Sprintf("INSERT INTO c VALUES (11, 1, 1), (21, 2, %d)", tc.n))
			execAll(t, e.dst,
				"CREATE TABLE r (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB",
				"CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, r_id BIGINT NOT NULL, n TINYINT NOT NULL, KEY ix_r (r_id), "+
					"CONSTRAINT fk_c_r FOREIGN KEY (r_id) REFERENCES r (id)) ENGINE=InnoDB")
			e = e.nonStrict(t, nil)
			cfg := e.config(t, job(t, jobspec.Archive, `
    root_table: r
    primary_key: id
    where: "1=1"
    relations:
      - table: c
        primary_key: id
        foreign_key: r_id
`))
			cfg.Verification, cfg.SkipPolicy = jobspec.VerifyCount, true
			_, err := e.run(t, cfg)
			if tc.n > 127 {
				de := wantErr[*insertDiagnosticError](t, err)
				if de.Identifier != "INSERT_DIAGNOSTIC_REJECTED" || de.Table != "c" || de.Count != 1 || len(de.Preview) != 1 || de.Preview[0].Code != 1264 {
					t.Errorf("refusal = %+v, want INSERT_DIAGNOSTIC_REJECTED on c for one 1264", de)
				}
				untouched(t, e, "r", 2)
				untouched(t, e, "c", 2)
				return
			}
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if r, c := count(t, e.dst, "SELECT COUNT(*) FROM r"), count(t, e.dst, "SELECT COUNT(*) FROM c"); r != 2 || c != 2 {
				t.Errorf("destination r, c = %d, %d rows, want 2, 2", r, c)
			}
		})
	}
}

// TestMySQLInsertDiagnosticsRetention (M7-3, G21): the destination session's
// max_error_count, set per session, bounds the conditions an INSERT can prove. Each row
// carries two conversions (a and b out of TINYINT's range); verification none, so every
// retained condition would be accepted. Non-strict destination session, SkipPolicy.
// Pair: max_error_count 1 (one row per INSERT, two warnings, one retained) or 0 (no
// subdivision: both rows, four warnings, none retained) → INSERT_DIAGNOSTICS_UNPROVEN
// "incomplete diagnostic list", the destination empty, nothing logged; the server's default
// capacity, the one change → archived with four 1264 conversions. A third side keeps
// max_error_count 1 with one conversion per row: each INSERT carries one row, and all are
// accepted.
func TestMySQLInsertDiagnosticsRetention(t *testing.T) {
	for _, tc := range []struct {
		name     string
		capacity string // the session's max_error_count; empty is the server default
		b        int    // both rows' b
		warnings uint64 // the refused INSERT's warning count: one row under capacity 1, both under 0
		retained uint64
	}{
		{"capacity 1", "1", 999, 2, 1},
		{"capacity 0", "0", 999, 4, 0},
		{"default capacity", "", 999, 0, 0},
		{"capacity 1, one warning per row", "1", 9, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, "diagcap")
			execAll(t, e.src, "CREATE TABLE r (id BIGINT NOT NULL PRIMARY KEY, a INT NOT NULL, b INT NOT NULL) ENGINE=InnoDB",
				fmt.Sprintf("INSERT INTO r VALUES (1, 999, %d), (2, 999, %d)", tc.b, tc.b))
			execAll(t, e.dst, "CREATE TABLE r (id BIGINT NOT NULL PRIMARY KEY, a TINYINT NOT NULL, b TINYINT NOT NULL) ENGINE=InnoDB")
			params := map[string]string{}
			if tc.capacity != "" {
				params["max_error_count"] = tc.capacity
			}
			e = e.nonStrict(t, params)
			want := "1024"
			if tc.capacity != "" {
				want = tc.capacity
			}
			if got := text(t, e, "SELECT CAST(@@SESSION.max_error_count AS CHAR)"); got != want {
				t.Fatalf("destination session max_error_count = %s, want %s", got, want)
			}
			cfg := e.config(t, job(t, jobspec.Archive, "\n    root_table: r\n    primary_key: id\n    where: \"1=1\"\n"))
			cfg.Verification, cfg.SkipPolicy = jobspec.VerifyNone, true
			rep, err := e.run(t, cfg)
			if tc.b > 127 && tc.capacity != "" {
				de := wantErr[*insertDiagnosticError](t, err)
				if de.Identifier != "INSERT_DIAGNOSTICS_UNPROVEN" || de.Table != "r" || de.Count != tc.warnings || de.Retained != tc.retained ||
					!strings.HasPrefix(de.Reason, "incomplete diagnostic list") {
					t.Errorf("refusal = %+v, want INSERT_DIAGNOSTICS_UNPROVEN on r: %d warnings, %d retained", de, tc.warnings, tc.retained)
				}
				untouched(t, e, "r", 2)
				return
			}
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			conversions := uint64(2)
			if tc.b > 127 {
				conversions = 4
			}
			if got := rep.Conversions["r"]; len(got) != 1 || got[1264] != conversions {
				t.Errorf("Report.Conversions[r] = %v, want {1264: %d}", got, conversions)
			}
			if got := count(t, e.dst, "SELECT COUNT(*) FROM r WHERE a = 127"); got != 2 {
				t.Errorf("destination rows with a = 127: %d, want 2", got)
			}
		})
	}
}
