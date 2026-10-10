//go:build integration

package engine

import (
	"fmt"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
)

// orderTables: a CHAR(36) root, lines on a composite key under it, notes on a composite
// foreign key under the lines. cs is the character columns' charset and collation.
func orderTables(cs string) []string {
	return []string{
		"CREATE TABLE orders (id CHAR(36) " + cs + " NOT NULL, placed DATE NOT NULL, code VARCHAR(20) " + cs + ", " +
			"PRIMARY KEY (id), UNIQUE KEY uk_code (code)) ENGINE=InnoDB",
		"CREATE TABLE order_lines (order_id CHAR(36) " + cs + " NOT NULL, line_no SMALLINT NOT NULL, sku VARCHAR(32), at DATETIME(6), " +
			"PRIMARY KEY (order_id, line_no), CONSTRAINT fk_lines_order FOREIGN KEY (order_id) REFERENCES orders (id)) ENGINE=InnoDB",
		"CREATE TABLE line_notes (note_id INT NOT NULL, order_id CHAR(36) " + cs + ", line_no SMALLINT, body TEXT, " +
			"PRIMARY KEY (note_id), KEY ix_line (order_id, line_no), CONSTRAINT fk_notes_line FOREIGN KEY (order_id, line_no) " +
			"REFERENCES order_lines (order_id, line_no)) ENGINE=InnoDB",
	}
}

const orderJob = `
    root_table: orders
    primary_key: id
    where: "placed < '2020-01-01'"
    relations:
      - table: order_lines
        primary_key: [order_id, line_no]
        foreign_key: order_id
        relations:
          - table: line_notes
            primary_key: note_id
            foreign_key: [order_id, line_no]
            references: [order_id, line_no]
`

// seedOrders writes n old orders and one new one, each with two lines, and a note on every
// line. Old order i has id "o<i>"; the new one is "new".
func seedOrders(t *testing.T, e env, n int) {
	t.Helper()
	note := 0
	add := func(id, placed string) {
		execAll(t, e.src, fmt.Sprintf("INSERT INTO orders VALUES ('%s', '%s', 'c-%s')", id, placed, id))
		for line := 1; line <= 2; line++ {
			note++
			execAll(t, e.src,
				fmt.Sprintf("INSERT INTO order_lines VALUES ('%s', %d, 'sku', '2019-05-05 10:00:00.123456')", id, line),
				fmt.Sprintf("INSERT INTO line_notes VALUES (%d, '%s', %d, 'n')", note, id, line))
		}
	}
	for i := 0; i < n; i++ {
		add(fmt.Sprintf("o%d", i), "2019-01-01")
	}
	add("new", "2025-01-01")
}

var orderOrder = map[string]string{"orders": "id", "order_lines": "order_id, line_no", "line_notes": "note_id"}

// oldRows is every row of the old orders' trees, as rows() reads them.
func oldRows(t *testing.T, e env) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for table, order := range orderOrder {
		for _, r := range rows(t, e.src, table, order) {
			if !strings.Contains(r, fmt.Sprintf("%X", "new")) {
				out[table] = append(out[table], r)
			}
		}
	}
	return out
}

// TestMySQLPhases runs each job type over five old orders in batches of two: archive and
// purge leave only the new order's tree in the source; archive and copy-only leave exactly
// the old trees in the destination; the log ends with every old root completed.
func TestMySQLPhases(t *testing.T) {
	cs := "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
		t.Run(string(typ), func(t *testing.T) {
			e := newEnv(t, "phases")
			if typ == jobspec.Purge {
				execAll(t, e.src, orderTables(cs)...)
			} else {
				e.both(t, orderTables(cs)...)
			}
			seedOrders(t, e, 5)
			old := oldRows(t, e)
			rep := e.mustRun(t, e.config(t, job(t, typ, orderJob)))

			if rep.Batches != 3 || rep.Roots != 5 || rep.Recovered != 0 || rep.Stopped {
				t.Errorf("report = %+v, want 3 batches of 5 roots", rep)
			}
			wantSource := int64(1 + 2 + 2) // the new order's tree
			if typ == jobspec.CopyOnly {
				wantSource = 6 + 12 + 12
			}
			if got := count(t, e.src, "SELECT (SELECT COUNT(*) FROM orders) + (SELECT COUNT(*) FROM order_lines) + (SELECT COUNT(*) FROM line_notes)"); got != wantSource {
				t.Errorf("source rows = %d, want %d", got, wantSource)
			}
			if typ != jobspec.Purge {
				for table, order := range orderOrder {
					if got := rows(t, e.dst, table, order); !equal(got, old[table]) {
						t.Errorf("destination %s =\n%v\nwant\n%v", table, got, old[table])
					}
				}
				if rep.Copied["line_notes"] != 10 {
					t.Errorf("copied = %v", rep.Copied)
				}
			}
			if got := statuses(t, e.dst, e.dstSchema); len(got) != 1 || got[int(tracking.StatusCompleted)] != 5 {
				t.Errorf("log = %v, want 5 completed", got)
			}
			if got := count(t, e.dst, "SELECT job_status FROM archiver_job WHERE job_name = 'j'"); got != int64(tracking.JobIdle) {
				t.Errorf("job_status = %d, want idle", got)
			}
		})
	}
}
