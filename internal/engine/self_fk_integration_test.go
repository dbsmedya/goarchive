//go:build integration

package engine

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
)

// TestMySQLSelfReferencingDeleteRules (beta audit A3): a job table whose foreign key
// references the same table makes InnoDB act on rows outside the batch when a batch row is
// deleted. CASCADE deletes them and SET NULL clears their parent, uncopied, and the run used to
// report success: archive and purge must refuse both before any row moves. RESTRICT and NO
// ACTION fail loudly (1451) and lose nothing, so they stay accepted; copy-only deletes nothing.
func TestMySQLSelfReferencingDeleteRules(t *testing.T) {
	for _, tc := range []struct {
		typ     jobspec.Type
		rule    string
		refused bool
	}{
		{jobspec.Archive, "CASCADE", true},
		{jobspec.Archive, "SET NULL", true},
		{jobspec.Purge, "CASCADE", true},
		{jobspec.Purge, "SET NULL", true},
		{jobspec.Archive, "RESTRICT", false},
		{jobspec.Archive, "NO ACTION", false},
		{jobspec.CopyOnly, "CASCADE", false},
	} {
		t.Run(fmt.Sprintf("%s %s", tc.typ, tc.rule), func(t *testing.T) {
			e := newEnv(t, "selffk")
			e.both(t, "CREATE TABLE comments (id BIGINT NOT NULL PRIMARY KEY, parent_id BIGINT NULL, created_at DATETIME NOT NULL, "+
				"KEY idx_parent (parent_id), CONSTRAINT fk_comments_parent FOREIGN KEY (parent_id) REFERENCES comments (id) "+
				"ON DELETE "+tc.rule+") ENGINE=InnoDB")
			// An old comment, inside the where range, and a new reply to it, outside it.
			execAll(t, e.src, "INSERT INTO comments VALUES (1, NULL, '2024-01-01 00:00:00'), (2, 1, '2026-10-01 00:00:00')")
			cfg := e.config(t, job(t, tc.typ, "\n    root_table: comments\n    primary_key: id\n    where: \"created_at < '2025-01-01'\"\n"))
			_, err := e.run(t, cfg)

			reply := count(t, e.src, "SELECT COUNT(*) FROM comments WHERE id = 2 AND parent_id = 1")
			old := count(t, e.src, "SELECT COUNT(*) FROM comments WHERE id = 1")
			t.Logf("%s, ON DELETE %s: run error %v; source: old %d, reply with its parent %d", tc.typ, tc.rule, err, old, reply)
			if reply != 1 {
				t.Errorf("the reply outside the where range was deleted or lost its parent (run error %v)", err)
			}
			switch {
			case tc.refused:
				wantSelfFKRefusal(t, err, tc.rule)
				if old != 1 {
					t.Errorf("old comment left %d, want 1: the refusal must come before any row moves", old)
				}
				if n := count(t, e.dst, "SELECT COUNT(*) FROM comments"); n != 0 {
					t.Errorf("destination comments = %d, want 0: the refusal must come before any row moves", n)
				}
			case tc.typ == jobspec.CopyOnly:
				if err != nil {
					t.Errorf("copy-only deletes nothing and must run: %v", err)
				}
			default:
				// Today's loud failure: deleting the old comment while its reply still
				// references it.
				var me *mysql.MySQLError
				if !errors.As(err, &me) || me.Number != 1451 {
					t.Errorf("run error = %v, want MySQL error 1451 from the delete", err)
				}
			}
		})
	}

	// The rule covers every job table, not just the root: here a child's comments reference
	// each other, and a reply under an order outside the range answers a comment under one
	// inside it.
	t.Run("a child table", func(t *testing.T) {
		e := newEnv(t, "selffkchild")
		e.both(t, "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, created_at DATETIME NOT NULL) ENGINE=InnoDB",
			"CREATE TABLE comments (id BIGINT NOT NULL PRIMARY KEY, order_id BIGINT NOT NULL, parent_id BIGINT NULL, "+
				"KEY idx_order (order_id), KEY idx_parent (parent_id), "+
				"CONSTRAINT fk_comments_order FOREIGN KEY (order_id) REFERENCES orders (id), "+
				"CONSTRAINT fk_comments_parent FOREIGN KEY (parent_id) REFERENCES comments (id) ON DELETE CASCADE) ENGINE=InnoDB")
		execAll(t, e.src, "INSERT INTO orders VALUES (1, '2024-01-01 00:00:00'), (2, '2026-10-01 00:00:00')",
			"INSERT INTO comments VALUES (1, 1, NULL), (2, 2, 1)")
		cfg := e.config(t, job(t, jobspec.Archive, `
    root_table: orders
    primary_key: id
    where: "created_at < '2025-01-01'"
    relations:
      - table: comments
        primary_key: id
        foreign_key: order_id
`))
		_, err := e.run(t, cfg)
		wantSelfFKRefusal(t, err, "CASCADE")
		if n := count(t, e.src, "SELECT COUNT(*) FROM comments"); n != 2 {
			t.Errorf("source comments = %d, want 2", n)
		}
		if n := count(t, e.src, "SELECT COUNT(*) FROM orders"); n != 2 {
			t.Errorf("source orders = %d, want 2", n)
		}
		if n := count(t, e.dst, "SELECT COUNT(*) FROM orders") + count(t, e.dst, "SELECT COUNT(*) FROM comments"); n != 0 {
			t.Errorf("destination rows = %d, want 0", n)
		}
	})
}

// wantSelfFKRefusal requires err to be preflight's SELF_FK_DELETE_RULE_CHECK naming the
// comments table's constraint and its rule.
func wantSelfFKRefusal(t *testing.T, err error, rule string) {
	t.Helper()
	var pe *preflight.Error
	if !errors.As(err, &pe) || pe.Check != "SELF_FK_DELETE_RULE_CHECK" {
		t.Fatalf("run error = %v, want preflight's SELF_FK_DELETE_RULE_CHECK", err)
	}
	for _, want := range []string{"comments", "fk_comments_parent", "ON DELETE " + rule} {
		if !strings.Contains(pe.Message, want) {
			t.Errorf("refusal does not name %q: %s", want, pe.Message)
		}
	}
}
