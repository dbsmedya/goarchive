//go:build integration

package engine

import (
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// TestMySQLProgress (R50): with --progress the run counts the where range after recovery and
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
			if lines := strings.Split(strings.TrimSpace(out.String()), "\n"); len(lines) != 1 || !strings.HasPrefix(lines[0], tc.want) || !strings.HasSuffix(lines[0], " eta=0s") {
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
		if got := strings.TrimSpace(out.String()); !strings.HasPrefix(got, want) {
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
}
