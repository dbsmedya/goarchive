//go:build integration

package engine

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
)

// GH #7: archive recovery deletes only what the destination holds. A page whose
// re-discovered rows the destination lacks is refused before its first delete (spec
// 2026-10-13-gh7-recovery-destination-check-design.md rev 3).

// Test statements that name a CHAR key by its literal trim it: under PAD_CHAR_TO_FULL_LENGTH
// (an environment matrix row) a NO PAD collation compares the stored value padded to its full
// length, so id = 'o0' matches nothing (probe, 8.4.11).

// lateNote is a row written under o0's first line after o0 was copied.
const lateNote = "INSERT INTO line_notes VALUES (100, 'o0', 1, 'late')"

// orderTablesNoFK is orderTables without its declared foreign keys.
func orderTablesNoFK(cs string) []string {
	var out []string
	for _, ddl := range orderTables(cs) {
		if i := strings.Index(ddl, ", CONSTRAINT"); i >= 0 {
			ddl = ddl[:i] + ") ENGINE=InnoDB"
		}
		out = append(out, ddl)
	}
	return out
}

// treeCounts is the row count of each table of orderJob's tree on db.
func treeCounts(t *testing.T, db *sql.DB) string {
	t.Helper()
	var parts []string
	for _, table := range []string{"orders", "order_lines", "line_notes"} {
		parts = append(parts, fmt.Sprintf("%s=%d", table, count(t, db, "SELECT COUNT(*) FROM "+table)))
	}
	return strings.Join(parts, " ")
}

// missingByTable is an *UncopiedError's findings, by table.
func missingByTable(t *testing.T, err error) map[string]MissingRows {
	t.Helper()
	ue := wantErr[*UncopiedError](t, err)
	out := map[string]MissingRows{}
	for _, m := range ue.Missing {
		out[m.Table] = m
	}
	return out
}

// TestMySQLRecoveryRefusesRowsTheDestinationLacks is GH #7's crash window: an archive run
// stops after its copy commits, and a note is then written under a copied order. Recovery
// used to delete it uncopied. It now stops before any delete, under every verification
// method, and the lossless remedy (copy the row into the destination) lets the rerun finish.
func TestMySQLRecoveryRefusesRowsTheDestinationLacks(t *testing.T) {
	cs := "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
	for _, v := range []jobspec.Verification{jobspec.VerifySHA256, jobspec.VerifyNone} {
		t.Run(string(v), func(t *testing.T) {
			e := newEnv(t, "held")
			e.both(t, orderTablesNoFK(cs)...)
			seedOrders(t, e, 3)
			cfg := func() Config {
				c := e.config(t, job(t, jobspec.Archive, orderJob))
				c.Verification = v
				return c
			}
			e.interrupted(t, cfg(), atCommitted)
			execAll(t, e.src, lateNote)
			before := treeCounts(t, e.src)

			_, err := e.run(t, cfg())
			got := missingByTable(t, err)
			if m := got["line_notes"]; len(got) != 1 || m.Root || m.Count != 1 {
				t.Fatalf("refusal = %+v, want the one late line_notes row", got)
			}
			if after := treeCounts(t, e.src); after != before {
				t.Errorf("source %s, was %s: the refused recovery deleted", after, before)
			}
			if st := statuses(t, e.dst, e.dstSchema); st[int(tracking.StatusCopied)] != 2 {
				t.Errorf("log = %v, want the 2 roots still copied", st)
			}

			execAll(t, e.dst, lateNote) // the remedy: copy the row into the destination
			rep := e.mustRun(t, cfg())
			if rep.Recovered != 2 || count(t, e.src, "SELECT COUNT(*) FROM line_notes WHERE note_id = 100") != 0 ||
				count(t, e.dst, "SELECT COUNT(*) FROM line_notes WHERE note_id = 100") != 1 {
				t.Errorf("after the remedy: %+v, want both roots recovered and the late row archived", rep)
			}
		})
	}
}

// TestMySQLRecoveryAfter1451KeepsTheLateRow is GH #7's aggravation: a note written between a
// batch's copy and its deletes makes the line's delete fail with 1451, and the roots stay
// copied. Recovery used to delete the note uncopied; it now stops before any delete, and the
// other remedy (remove the row from the source) lets the rerun finish.
func TestMySQLRecoveryAfter1451KeepsTheLateRow(t *testing.T) {
	e := newEnv(t, "held1451")
	e.both(t, orderTables("CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")...)
	seedOrders(t, e, 3)
	cfg := e.config(t, job(t, jobspec.Archive, orderJob))
	written := false
	cfg.at = func(p point) {
		if p == atCommitted && !written {
			written = true
			execAll(t, e.src, lateNote)
		}
	}
	_, err := e.run(t, cfg)
	var me *mysql.MySQLError
	if !errors.As(err, &me) || me.Number != 1451 {
		t.Fatalf("first run = %v, want 1451 on the line the late note references", err)
	}
	if st := statuses(t, e.dst, e.dstSchema); st[int(tracking.StatusCopied)] != 2 {
		t.Fatalf("log = %v, want 2 copied", st)
	}
	before := treeCounts(t, e.src)

	_, err = e.run(t, e.config(t, job(t, jobspec.Archive, orderJob)))
	got := missingByTable(t, err)
	if m := got["line_notes"]; len(got) != 1 || m.Count != 1 {
		t.Fatalf("refusal = %+v, want the one late line_notes row", got)
	}
	if after := treeCounts(t, e.src); after != before {
		t.Errorf("source %s, was %s: the refused recovery deleted", after, before)
	}

	execAll(t, e.src, "DELETE FROM line_notes WHERE note_id = 100") // the remedy: the row is not wanted
	rep := e.mustRun(t, e.config(t, job(t, jobspec.Archive, orderJob)))
	if rep.Recovered != 2 || count(t, e.src, "SELECT COUNT(*) FROM orders") != 1 {
		t.Errorf("after the remedy: %+v, want both roots recovered and only the new order left", rep)
	}
}

// TestMySQLRecoveryRefusesARootTheDestinationLost: a copied root's tree removed from the
// destination (a restore, a retention job) is reported as lost, not deleted from the source.
func TestMySQLRecoveryRefusesARootTheDestinationLost(t *testing.T) {
	e := newEnv(t, "heldroot")
	e.both(t, orderTables("CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")...)
	seedOrders(t, e, 3)
	e.interrupted(t, e.config(t, job(t, jobspec.Archive, orderJob)), atCommitted)
	execAll(t, e.dst, "DELETE FROM line_notes WHERE RTRIM(order_id) = 'o0'", "DELETE FROM order_lines WHERE RTRIM(order_id) = 'o0'",
		"DELETE FROM orders WHERE RTRIM(id) = 'o0'")
	before := treeCounts(t, e.src)

	_, err := e.run(t, e.config(t, job(t, jobspec.Archive, orderJob)))
	got := missingByTable(t, err)
	want := map[string]struct {
		root  bool
		count int64
	}{"orders": {true, 1}, "order_lines": {false, 2}, "line_notes": {false, 2}}
	if len(got) != len(want) {
		t.Fatalf("refusal = %+v, want %v", got, want)
	}
	for table, w := range want {
		if m := got[table]; m.Root != w.root || m.Count != w.count {
			t.Errorf("%s: %+v, want root=%v count=%d", table, m, w.root, w.count)
		}
	}
	if after := treeCounts(t, e.src); after != before {
		t.Errorf("source %s, was %s: the refused recovery deleted", after, before)
	}
	if st := statuses(t, e.dst, e.dstSchema); st[int(tracking.StatusCopied)] != 2 {
		t.Errorf("log = %v, want the 2 roots still copied", st)
	}
}

// TestMySQLRecoveryWarnsOfARootGoneFromBoth: a run killed after its last delete chunk
// leaves its roots gone from the source but logged copied. If the destination has lost one
// too, its rows exist on neither server; recovery cannot help, so it warns and finishes.
func TestMySQLRecoveryWarnsOfARootGoneFromBoth(t *testing.T) {
	e := newEnv(t, "heldgone")
	e.both(t, orderTables("CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")...)
	seedOrders(t, e, 3)
	e.killAt(t, jobspec.Archive, orderJob, atDeleted, 5) // batch 1's fifth chunk deletes its two roots
	if st := statuses(t, e.dst, e.dstSchema); st[int(tracking.StatusCopied)] != 2 {
		t.Fatalf("log after the kill = %v, want 2 copied", st)
	}
	execAll(t, e.dst, "DELETE FROM line_notes WHERE RTRIM(order_id) = 'o0'", "DELETE FROM order_lines WHERE RTRIM(order_id) = 'o0'",
		"DELETE FROM orders WHERE RTRIM(id) = 'o0'")

	core, logs := observer.New(zapcore.WarnLevel)
	cfg := e.config(t, job(t, jobspec.Archive, orderJob))
	cfg.Logger = &logger.Logger{SugaredLogger: zap.New(core).Sugar()}
	rep := e.mustRun(t, cfg)
	if rep.Recovered != 2 {
		t.Errorf("report = %+v, want both logged roots finished", rep)
	}
	if n := logs.FilterMessageSnippet("1 logged roots of orders are gone from both the source and the destination").Len(); n != 1 {
		t.Errorf("warnings naming the root gone from both = %d, want 1: %v", n, logs.All())
	}
}
