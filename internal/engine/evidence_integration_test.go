//go:build integration

package engine

import (
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// Evidence for two 2.2-series issues the 3.0 core closes by design. Both pass on 3.0; each is
// a pair (R56): the first half shows the condition the issue describes, the second half the
// outcome 3.0 gives it.

// TestMySQLRowEligibleLaterIsProcessed is GH #17's acceptance test (owner, 2026-09-06): rows
// o0 and o2 are eligible, a run takes them; o1, between them, becomes eligible afterwards and
// the next run takes it. 3.0 keeps no checkpoint: every run starts at the beginning of the
// where range (spec §6.5).
func TestMySQLRowEligibleLaterIsProcessed(t *testing.T) {
	cs := "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
	// RTRIM: under PAD_CHAR_TO_FULL_LENGTH a NO PAD collation compares the padded CHAR value.
	const inScope = "SELECT COUNT(*) FROM orders WHERE RTRIM(id) = 'o1'"
	setup := func(t *testing.T, name string, destination bool) env {
		e := newEnv(t, name)
		if destination {
			e.both(t, orderTables(cs)...)
		} else {
			execAll(t, e.src, orderTables(cs)...)
		}
		seedOrders(t, e, 3)
		// o1 is not eligible yet: the first run's where range passes over its key.
		execAll(t, e.src, "UPDATE orders SET placed = '2025-06-01' WHERE RTRIM(id) = 'o1'")
		return e
	}
	becomeEligible := func(t *testing.T, e env) {
		execAll(t, e.src, "UPDATE orders SET placed = '2019-06-01' WHERE RTRIM(id) = 'o1'")
	}

	t.Run("archive", func(t *testing.T) {
		e := setup(t, "elig_a", true)
		e.mustRun(t, e.config(t, job(t, jobspec.Archive, orderJob)))
		if count(t, e.src, inScope) != 1 || count(t, e.dst, inScope) != 0 {
			t.Fatal("the first run touched o1 while it was not eligible")
		}
		becomeEligible(t, e)
		rep := e.mustRun(t, e.config(t, job(t, jobspec.Archive, orderJob)))
		if rep.Roots != 1 || count(t, e.src, inScope) != 0 || count(t, e.dst, inScope) != 1 ||
			count(t, e.dst, "SELECT COUNT(*) FROM line_notes WHERE RTRIM(order_id) = 'o1'") != 2 {
			t.Errorf("second run = %+v, want o1's tree archived", rep)
		}
	})
	t.Run("purge", func(t *testing.T) {
		e := setup(t, "elig_p", false)
		e.mustRun(t, e.config(t, job(t, jobspec.Purge, orderJob)))
		if count(t, e.src, inScope) != 1 {
			t.Fatal("the first run purged o1 while it was not eligible")
		}
		becomeEligible(t, e)
		rep := e.mustRun(t, e.config(t, job(t, jobspec.Purge, orderJob)))
		if rep.Roots != 1 || count(t, e.src, inScope) != 0 ||
			count(t, e.src, "SELECT COUNT(*) FROM line_notes WHERE RTRIM(order_id) = 'o1'") != 0 {
			t.Errorf("second run = %+v, want o1's tree purged", rep)
		}
	})
	t.Run("copy-only with resume: true", func(t *testing.T) {
		e := setup(t, "elig_c", true)
		resume := orderJob + "    resume: true\n"
		e.mustRun(t, e.config(t, job(t, jobspec.CopyOnly, resume)))
		if count(t, e.dst, inScope) != 0 {
			t.Fatal("the first run copied o1 while it was not eligible")
		}
		becomeEligible(t, e)
		rep := e.mustRun(t, e.config(t, job(t, jobspec.CopyOnly, resume)))
		if rep.Roots != 1 || rep.Skipped != 2 || count(t, e.dst, inScope) != 1 {
			t.Errorf("second run = %+v, want o1 copied and o0, o2 skipped", rep)
		}
	})
}

// TestMySQLLoggedRootsNeverReplayedOnAnotherTable is GH #14's checklist item: a job
// interrupted with logged roots of orders, run again under the same name with another root
// table holding the same keys, is refused before anything is replayed or deleted; the job's
// own config then finishes the logged roots. The source-switch half is TestMySQLArchiveBinding.
func TestMySQLLoggedRootsNeverReplayedOnAnotherTable(t *testing.T) {
	cs := "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
	// orders2 holds orders' keys, so a replay of the logged roots against it would delete them.
	other := "CREATE TABLE orders2 (id CHAR(36) " + cs + " NOT NULL, placed DATE NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB"
	const otherJob = `
    root_table: orders2
    primary_key: id
    where: "placed < '2020-01-01'"
`
	const refusal = `exists for root table "orders"`
	seedOther := func(t *testing.T, e env) {
		execAll(t, e.src, "INSERT INTO orders2 VALUES ('o0', '2019-01-01'), ('o1', '2019-01-01'), ('o2', '2019-01-01')")
	}

	for _, tc := range []struct {
		name        string
		typ         jobspec.Type
		at          point
		destination bool
	}{
		{"archive with copied roots", jobspec.Archive, atCommitted, true},
		{"purge with deleting roots", jobspec.Purge, atLogged, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, "rootsw")
			ddl := append(orderTables(cs), other)
			if tc.destination {
				e.both(t, ddl...)
			} else {
				execAll(t, e.src, ddl...)
			}
			seedOrders(t, e, 3)
			seedOther(t, e)
			e.interrupted(t, e.config(t, job(t, tc.typ, orderJob)), tc.at)
			orders := count(t, e.src, "SELECT COUNT(*) FROM orders")

			_, err := e.run(t, e.config(t, job(t, tc.typ, otherJob)))
			if err == nil || !strings.Contains(err.Error(), refusal) {
				t.Fatalf("run with root orders2 = %v, want it refused (%s)", err, refusal)
			}
			if got := count(t, e.src, "SELECT COUNT(*) FROM orders2"); got != 3 {
				t.Errorf("orders2 rows = %d, want 3: the logged roots were replayed against it", got)
			}
			if got := count(t, e.src, "SELECT COUNT(*) FROM orders"); got != orders {
				t.Errorf("orders rows = %d, want %d: the refused run deleted", got, orders)
			}

			rep := e.mustRun(t, e.config(t, job(t, tc.typ, orderJob)))
			if rep.Recovered != 2 || count(t, e.src, "SELECT COUNT(*) FROM orders") != 1 {
				t.Errorf("the job's own config = %+v, want its 2 logged roots finished and only the new order left", rep)
			}
		})
	}
}
