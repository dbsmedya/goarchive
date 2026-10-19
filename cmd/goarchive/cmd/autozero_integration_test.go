//go:build integration

package cmd

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

// autoZeroJobs are an archive and a copy-only job over t.
const autoZeroJobs = `
  archive_auto:
    job_type: archive
    root_table: t
    primary_key: id
    where: "1=1"
  copy_auto:
    job_type: copy-only
    root_table: t
    primary_key: id
    where: "1=1"
`

const autoZeroDDL = "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, seq BIGINT NOT NULL AUTO_INCREMENT, KEY ix_seq (seq)) ENGINE=InnoDB"

// onSession runs stmts on one session of db: the session variable a statement sets must
// hold for the next one.
func onSession(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	for _, q := range stmts {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
}

// TestMySQLAutoIncrementZero (M7-3, G18): an AUTO_INCREMENT column holding 0 survives the
// copy, because every production session (database.BuildDSN) adds NO_AUTO_VALUE_ON_ZERO to
// its sql_mode. The commands connect through the written config, so this is the production
// DSN; seq = 0 is seeded on a source session that has the mode.
// Pair: the archive or copy-only command's INSERT of seq = 0 → the destination keeps 0
// (archive also empties the source); the same INSERT of 0 into the same destination table
// on a session without NO_AUTO_VALUE_ON_ZERO, the one change → MySQL renumbers it to 1.
func TestMySQLAutoIncrementZero(t *testing.T) {
	for _, tc := range []struct{ command, job string }{{"archive", "archive_auto"}, {"copy-only", "copy_auto"}} {
		t.Run(tc.command, func(t *testing.T) {
			f := newFixture(t, "autozero", autoZeroJobs)
			mustExec(t, f.src, autoZeroDDL)
			mustExec(t, f.dst, autoZeroDDL)
			onSession(t, f.src, "SET SESSION sql_mode = CONCAT_WS(',', NULLIF(@@SESSION.sql_mode, ''), 'NO_AUTO_VALUE_ON_ZERO')",
				"INSERT INTO t (id, seq) VALUES (1, 0)")
			if got := scalar(t, f.src, "SELECT seq FROM t WHERE id = 1"); got != 0 {
				t.Fatalf("source seq = %d, want the seeded 0", got)
			}
			if out, err := execute(t, tc.command, "-c", f.config, "--job", tc.job); err != nil {
				t.Fatalf("%s: %v\n%s", tc.command, err, out)
			}
			if got := scalar(t, f.dst, "SELECT seq FROM t WHERE id = 1"); got != 0 {
				t.Errorf("destination seq = %d, want 0 kept", got)
			}
			want := int64(1)
			if tc.command == "archive" {
				want = 0
			}
			if got := scalar(t, f.src, "SELECT COUNT(*) FROM t"); got != want {
				t.Errorf("source t = %d rows, want %d", got, want)
			}

			// The same INSERT on a session without the mode: the fixture would catch a lost mode.
			// f.dst is goarchive's own pool, which pins the mode, so this is the one test that
			// drops a pin on purpose: one session of it goes back to the server's global mode.
			ctx := context.Background()
			conn, err := f.dst.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			if _, err := conn.ExecContext(ctx, "SET SESSION sql_mode = @@GLOBAL.sql_mode"); err != nil {
				t.Fatal(err)
			}
			var mode string
			if err := conn.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode").Scan(&mode); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(mode, "NO_AUTO_VALUE_ON_ZERO") {
				t.Fatalf("the assertion session's sql_mode %q already holds NO_AUTO_VALUE_ON_ZERO", mode)
			}
			if _, err := conn.ExecContext(ctx, "INSERT INTO t (id, seq) VALUES (2, 0)"); err != nil {
				t.Fatal(err)
			}
			if got := scalar(t, f.dst, "SELECT seq FROM t WHERE id = 2"); got != 1 {
				t.Errorf("a raw INSERT of seq = 0 without NO_AUTO_VALUE_ON_ZERO stored %d, want MySQL's renumbered 1", got)
			}
		})
	}
}
