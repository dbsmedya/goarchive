//go:build integration

package engine

import (
	"database/sql"
	"math"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// TestMySQLProgress (R50): with --progress the run estimates the where range after recovery and
// prints the 2.x line; an interval longer than the run leaves exactly the final line. Its rows
// are this run's, recovery's deletes included.
func TestMySQLProgress(t *testing.T) {
	for _, tc := range []struct {
		typ  jobspec.Type
		want string
	}{
		{jobspec.Archive, "progress: roots 3/~3 (100.0%) remaining=0 copied_rows=15 deleted_rows=15 "},
		{jobspec.CopyOnly, "progress: roots 3/~3 (100.0%) remaining=0 copied_rows=15 elapsed="},
		{jobspec.Purge, "progress: roots 3/~3 (100.0%) remaining=0 deleted_rows=15 elapsed="},
	} {
		t.Run(string(tc.typ), func(t *testing.T) {
			e := newEnv(t, "progress")
			e.both(t, orderTables("CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")...)
			seedOrders(t, e, 3)
			var out strings.Builder
			cfg := e.config(t, job(t, tc.typ, orderJob))
			cfg.Progress, cfg.Out = time.Hour, &out
			e.mustRun(t, cfg)
			if lines := strings.Split(strings.TrimSpace(out.String()), "\n"); len(lines) != 1 || !strings.HasPrefix(fields(lines[0]), tc.want) || !strings.HasSuffix(lines[0], " eta=0s") {
				t.Errorf("progress output = %q, want one line starting %q", out.String(), tc.want)
			}
		})
	}

	t.Run("after recovery", func(t *testing.T) {
		e := newEnv(t, "progress")
		e.both(t, orderTables("CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")...)
		seedOrders(t, e, 3)
		e.interrupted(t, e.config(t, job(t, jobspec.Archive, orderJob)), atCommitted)
		var out strings.Builder
		cfg := e.config(t, job(t, jobspec.Archive, orderJob))
		cfg.Progress, cfg.Out = time.Hour, &out
		e.mustRun(t, cfg)
		want := "progress: roots 1/~1 (100.0%) remaining=0 copied_rows=5 deleted_rows=15 "
		if got := fields(out.String()); !strings.HasPrefix(got, want) {
			t.Errorf("progress output = %q, want %q: the count follows recovery, the rows include its deletes", got, want)
		}
	})

	t.Run("a refused run prints nothing", func(t *testing.T) {
		e := newEnv(t, "progress")
		e.both(t, orderTables("CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")...)
		var out strings.Builder
		cfg := e.config(t, job(t, jobspec.Archive, strings.Replace(orderJob, "primary_key: id", "primary_key: code", 1)))
		cfg.Progress, cfg.Out = time.Second, &out
		if _, err := e.run(t, cfg); err == nil {
			t.Fatal("preflight passed a wrong primary_key")
		}
		if out.Len() != 0 {
			t.Errorf("a run refused at preflight printed %q", out.String())
		}
	})

	// PRC-1 (spec rev 3 §3.3, §4): the total is the optimizer's estimate of the count statement,
	// not the count. The dedup where's estimate differs from its 7 roots; the tables freeze their
	// statistics (STATS_AUTO_RECALC=0 in CREATE TABLE), so the test's EXPLAIN and the run's read
	// the same numbers. A stop after the first batch keeps the estimate on the closing line.
	t.Run("an estimate, not a count", func(t *testing.T) {
		e := newEnv(t, "progress")
		e.both(t, "CREATE TABLE dedup_payment (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, base_payment_id BIGINT NULL, "+
			"amount DECIMAL(10,2) NOT NULL, KEY idx_base_payment_id (base_payment_id)) ENGINE=InnoDB STATS_AUTO_RECALC=0")
		execAll(t, e.src, "INSERT INTO dedup_payment (id, base_payment_id, amount) VALUES "+
			"(1, 100, 10.00), (2, 100, 10.00), (3, 100, 10.00), (4, 200, 20.00), (5, 300, 30.00), (6, 300, 30.00), "+
			"(7, NULL, 70.00), (8, NULL, 70.00), (9, 100, 10.00), "+
			"(10, 400, 40.00), (11, 400, 40.00), (12, 400, 40.00), (13, 400, 40.00)")
		const roots = 7
		if got := count(t, e.src, "SELECT COUNT(*) FROM dedup_payment WHERE ("+dedupWhere+")"); got != roots {
			t.Fatalf("fixture range = %d roots, want %d", got, roots)
		}
		// The estimate, read from the raw plan: the fixture's select 1 has one row, the root.
		want := int64(-1)
		plan, err := e.src.Query("EXPLAIN FORMAT=TRADITIONAL SELECT COUNT(*) FROM `dedup_payment` WHERE (" + dedupWhere + "\n)")
		if err != nil {
			t.Fatal(err)
		}
		cols, _ := plan.Columns()
		for plan.Next() {
			vals := make([]sql.NullString, len(cols))
			dest := make([]any, len(cols))
			for i := range vals {
				dest[i] = &vals[i]
			}
			if err := plan.Scan(dest...); err != nil {
				t.Fatal(err)
			}
			col := map[string]string{}
			for i, c := range cols {
				col[c] = vals[i].String
			}
			if col["id"] != "1" {
				continue
			}
			if want != -1 || col["table"] != "dedup_payment" {
				t.Fatalf("select 1 of the plan = %v; want one row, the root", col)
			}
			n, _ := strconv.ParseFloat(col["rows"], 64)
			f, _ := strconv.ParseFloat(col["filtered"], 64)
			want = int64(math.Round(n * f / 100))
		}
		_ = plan.Close()
		if want == roots || want < 0 {
			t.Fatalf("the optimizer's estimate is %d for %d roots: the fixture cannot tell an estimate from a count", want, roots)
		}

		stop := make(chan struct{})
		var out strings.Builder
		cfg := e.config(t, job(t, jobspec.Archive, "\n    root_table: dedup_payment\n    primary_key: id\n    where: \""+dedupWhere+"\"\n"))
		cfg.Processing.BatchSize, cfg.Processing.BatchDeleteSize = 2, 1
		cfg.Progress, cfg.Out, cfg.Stop = time.Hour, &out, stop
		cfg.at = func(p point) {
			if p == atDeleted {
				select {
				case <-stop:
				default:
					close(stop)
				}
			}
		}
		if rep := e.mustRun(t, cfg); !rep.Stopped {
			t.Fatalf("report = %+v, want a stop after the first batch", rep)
		}
		m := regexp.MustCompile(`roots\s+(\d+)/~(\d+) `).FindStringSubmatch(out.String())
		if m == nil {
			t.Fatalf("progress output = %q, want one closing line", out.String())
		}
		if got, _ := strconv.ParseInt(m[2], 10, 64); got != want {
			t.Errorf("progress total = %d, want the optimizer's estimate %d (the range holds %d): %q", got, want, roots, out.String())
		}
	})
}

// fields folds a progress line's padding (spec §3.5 aligns its columns), so a test compares
// the values.
func fields(s string) string { return strings.Join(strings.Fields(s), " ") + " " }
