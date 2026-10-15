//go:build integration

package engine

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// dedupWhere selects every dedup_payment row whose base_payment_id already has a row with a
// smaller id, the keeper, through a correlated subquery on the root table itself. Rows with
// a NULL key are never duplicates.
const dedupWhere = "dedup_payment.base_payment_id IS NOT NULL AND dedup_payment.id > (" +
	"SELECT keeper.id FROM dedup_payment AS keeper FORCE INDEX (idx_base_payment_id) " +
	"WHERE keeper.base_payment_id = dedup_payment.base_payment_id ORDER BY keeper.id LIMIT 1)"

// TestMySQLDedupArchive (M7-3, G26): removing duplicate rows with one job, keeping the
// minimum id per base_payment_id (docs/README_DUPLICATE_CLEANUP.md). Thirteen rows: keepers
// 1, 4, 5 and 10, NULL keys 7 and 8, duplicates interleaved across batch boundaries.
// Archive under sha256 with --progress, whose range count runs the where before the first
// batch; a second run counts what is left.
// Pair: batch_size 2 and batch_delsize 1, so every fetch after the first reads the root
// table through the subquery after deletes from it → the source keeps exactly [1 4 5 7 8
// 10], the destination holds [2 3 6 9 11 12 13], the range count is 7 before and 0 after;
// batch_size 100, the one change, so one fetch reads the range before any delete → the same
// id sets and counts.
func TestMySQLDedupArchive(t *testing.T) {
	for _, tc := range []struct {
		name       string
		batch, del int
		batches    int
	}{{"interleaved", 2, 1, 4}, {"one batch", 100, 100, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, "dedup")
			e.both(t, "CREATE TABLE dedup_payment (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, base_payment_id BIGINT NULL, "+
				"amount DECIMAL(10,2) NOT NULL, KEY idx_base_payment_id (base_payment_id)) ENGINE=InnoDB")
			execAll(t, e.src, "INSERT INTO dedup_payment (id, base_payment_id, amount) VALUES "+
				"(1, 100, 10.00), (2, 100, 10.00), (3, 100, 10.00), (4, 200, 20.00), (5, 300, 30.00), (6, 300, 30.00), "+
				"(7, NULL, 70.00), (8, NULL, 70.00), (9, 100, 10.00), "+
				"(10, 400, 40.00), (11, 400, 40.00), (12, 400, 40.00), (13, 400, 40.00)")
			// An independent count of the duplicates, not through the where.
			if got := count(t, e.src, "SELECT COALESCE(SUM(n - 1), 0) FROM (SELECT COUNT(*) AS n FROM dedup_payment "+
				"WHERE base_payment_id IS NOT NULL GROUP BY base_payment_id) AS g"); got != 7 {
				t.Fatalf("fixture has %d duplicates by GROUP BY, want 7", got)
			}
			spec := job(t, jobspec.Archive, "\n    root_table: dedup_payment\n    primary_key: id\n    where: \""+dedupWhere+"\"\n")
			run := func() (string, *Report) {
				var out strings.Builder
				cfg := e.config(t, spec)
				cfg.Processing.BatchSize, cfg.Processing.BatchDeleteSize = tc.batch, tc.del
				cfg.Progress, cfg.Out = time.Hour, &out
				rep := e.mustRun(t, cfg)
				return strings.TrimSpace(out.String()), rep
			}
			out, rep := run()
			if rep.Roots != 7 || rep.Batches != tc.batches {
				t.Errorf("report = %+v, want 7 roots in %d batches", rep, tc.batches)
			}
			if want := "progress: roots 7/~7 (100.0%) remaining=0 copied_rows=7 deleted_rows=7 "; !strings.HasPrefix(fields(out), want) {
				t.Errorf("progress = %q, want the range count 7: %q", out, want)
			}
			if got, want := texts(t, e.src, "SELECT CAST(id AS CHAR) FROM dedup_payment ORDER BY id"), []string{"1", "4", "5", "7", "8", "10"}; !equal(got, want) {
				t.Errorf("source ids = %v, want %v", got, want)
			}
			if got, want := texts(t, e.dst, "SELECT CAST(id AS CHAR) FROM dedup_payment ORDER BY id"), []string{"2", "3", "6", "9", "11", "12", "13"}; !equal(got, want) {
				t.Errorf("destination ids = %v, want %v", got, want)
			}
			if out, rep = run(); rep.Roots != 0 || !strings.HasPrefix(fields(out), "progress: roots 0/~0 ") {
				t.Errorf("second run: progress = %q, report = %+v, want the range count 0 and no roots", out, rep)
			}
		})
	}
}

// TestMySQLWhereAppendedStatement (M7-3, G27): --progress's range count (rangeCountSQL) is
// the one statement sent with no arguments, as text. A where that closes its parenthesis,
// ends the statement and appends a DELETE reaches MySQL as one statement, because no
// connection enables multiStatements.
// Pair: where "1=1) ; DELETE FROM victim WHERE (1=1" → MySQL 1064 from the --progress
// count, before any batch: victim keeps its 3 rows, r its 3 rows, the destination empty,
// nothing logged; where "1=1", the one change → the 3 roots archived, victim keeps its 3 rows.
func TestMySQLWhereAppendedStatement(t *testing.T) {
	for _, tc := range []struct{ name, where string }{
		{"appended statement", "1=1) ; DELETE FROM victim WHERE (1=1"},
		{"plain", "1=1"},
	} {
		where := tc.where
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, "multi")
			e.both(t, "CREATE TABLE r (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB")
			execAll(t, e.src, "CREATE TABLE victim (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB",
				"INSERT INTO victim VALUES (1), (2), (3)", "INSERT INTO r VALUES (1), (2), (3)")
			var out strings.Builder
			cfg := e.config(t, job(t, jobspec.Archive, "\n    root_table: r\n    primary_key: id\n    where: \""+where+"\"\n"))
			cfg.Progress, cfg.Out = time.Hour, &out
			rep, err := e.run(t, cfg)
			if got := count(t, e.src, "SELECT COUNT(*) FROM victim"); got != 3 {
				t.Errorf("victim = %d rows, want 3 kept", got)
			}
			if where != "1=1" {
				var me *mysql.MySQLError
				if !errors.As(err, &me) || me.Number != 1064 || !strings.Contains(err.Error(), "--progress count on r") {
					t.Fatalf("Run = %v, want MySQL error 1064 from the --progress count", err)
				}
				if rep == nil || rep.Batches != 0 {
					t.Errorf("report = %+v, want no batch", rep)
				}
				untouched(t, e, "r", 3)
				return
			}
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if rep.Roots != 3 || count(t, e.src, "SELECT COUNT(*) FROM r") != 0 || count(t, e.dst, "SELECT COUNT(*) FROM r") != 3 {
				t.Errorf("report = %+v, want 3 roots archived", rep)
			}
		})
	}
}
