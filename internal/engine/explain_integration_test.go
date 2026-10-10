//go:build integration

package engine

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// TestMySQLExplain (spec §7.2, M6-9): dry-run's plans. The root fetch's EXPLAIN, then each
// child's discovery EXPLAIN bound with the first batch's keys, all in FORMAT=TRADITIONAL,
// which names its columns on every version (9.7 defaults to TREE). Nothing is locked, logged
// or deleted.
func TestMySQLExplain(t *testing.T) {
	e := newEnv(t, "explain")
	e.both(t, orderTables("CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")...)
	seedOrders(t, e, 3)
	before := rows(t, e.src, "line_notes", "note_id")
	ex, err := Explain(context.Background(), e.config(t, job(t, jobspec.Archive, orderJob)))
	if err != nil {
		t.Fatal(err)
	}
	if ex.Roots != 2 {
		t.Errorf("first batch = %d roots, want batch_size 2", ex.Roots)
	}
	if !strings.HasPrefix(ex.Fetch.Query, "EXPLAIN FORMAT=TRADITIONAL SELECT `id` FROM `orders` WHERE (") ||
		len(ex.Fetch.Rows) != 1 || ex.Fetch.Rows[0].Table != "orders" {
		t.Errorf("fetch plan = %+v", ex.Fetch)
	}
	if len(ex.Discovery) != 2 || ex.Discovery[0].Table != "order_lines" || ex.Discovery[1].Table != "line_notes" {
		t.Fatalf("discovery plans = %+v, want order_lines then line_notes", ex.Discovery)
	}
	if got := ex.Discovery[0].Rows[0].Key; got != "PRIMARY" {
		t.Errorf("order_lines discovery key = %q, want PRIMARY (order_id leads it)", got)
	}
	if got := ex.Discovery[1].Rows[0].Key; got != "ix_line" {
		t.Errorf("line_notes discovery key = %q, want ix_line", got)
	}
	if len(ex.Preflight.Caps) == 0 {
		t.Error("no statement caps reported")
	}
	if got := rows(t, e.src, "line_notes", "note_id"); !equal(got, before) {
		t.Error("dry-run changed the source")
	}
	var name string
	if err := e.dst.QueryRowContext(context.Background(), "SHOW TABLES LIKE 'archiver_job'").Scan(&name); !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("dry-run created the tracking tables (%q, %v)", name, err)
	}
}

// TestMySQLExplainEmptyRange: with nothing to fetch there are no keys to bind, so only the
// fetch is explained, and a note says why discovery is not.
func TestMySQLExplainEmptyRange(t *testing.T) {
	e := newEnv(t, "explainempty")
	e.both(t, orderTables("CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")...)
	ex, err := Explain(context.Background(), e.config(t, job(t, jobspec.Purge, orderJob)))
	if err != nil {
		t.Fatal(err)
	}
	if ex.Roots != 0 || len(ex.Fetch.Rows) == 0 || len(ex.Discovery) != 0 {
		t.Errorf("explanation = %+v", ex)
	}
	if !strings.Contains(strings.Join(ex.Notes, "\n"), "no rows match") {
		t.Errorf("notes = %q, want the empty-range note", ex.Notes)
	}
}

// TestMySQLExplainFilesort (M1-D1, second half): a key with a descending part walks
// correctly but sorts every batch. The fetch's plan shows the filesort, and a note names it.
func TestMySQLExplainFilesort(t *testing.T) {
	e := newEnv(t, "explainsort")
	e.both(t, "CREATE TABLE r (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a ASC, b DESC)) ENGINE=InnoDB")
	execAll(t, e.src, "INSERT INTO r VALUES (1, 1), (1, 2), (2, 1)")
	ex, err := Explain(context.Background(), e.config(t, job(t, jobspec.Purge, "\n    root_table: r\n    primary_key: [a, b]\n    where: \"1=1\"\n")))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(ex.Fetch.Rows[0].Extra, "Using filesort") {
		t.Fatalf("fetch plan = %+v, want a filesort", ex.Fetch.Rows)
	}
	if !strings.Contains(strings.Join(ex.Notes, "\n"), "sorts") {
		t.Errorf("notes = %q, want the filesort note", ex.Notes)
	}
}
