//go:build integration

package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
)

// notesAfterDrop runs DROP TABLE IF EXISTS on a table that does not exist, on one
// connection of db, and returns the codes SHOW WARNINGS then lists on it.
func notesAfterDrop(t *testing.T, db *sql.DB) []int {
	t.Helper()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	table := fmt.Sprintf("goarchive_m7c2_absent.t_%d", os.Getpid())
	if _, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
		t.Fatal(err)
	}
	rows, err := conn.QueryContext(ctx, "SHOW WARNINGS")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var codes []int
	for rows.Next() {
		var level, message string
		var code int
		if err := rows.Scan(&level, &code, &message); err != nil {
			t.Fatal(err)
		}
		codes = append(codes, code)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return codes
}

// TestMySQLSQLNotesPin (M7c-2, register G21): a session BuildDSN opens keeps notes whatever
// the server's global sql_notes. INSERT diagnostics read the session's warnings, notes
// included, so a global sql_notes=0 must not reach goarchive's sessions.
// Environment pair (R56, P7) on a server whose global sql_notes is 0, the environment
// matrix row sql_notes_0; elsewhere it skips. Side A: a BuildDSN session runs a statement
// that raises note 1051 and SHOW WARNINGS lists it. Side B, the one change: a session
// without the pin inherits sql_notes=0 and lists nothing.
func TestMySQLSQLNotesPin(t *testing.T) {
	ctx := context.Background()
	cfg := estateDatabaseConfig(t, "TEST_DEST")
	pinned, err := sql.Open("mysql", BuildDSN(&cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pinned.Close() })
	var global int
	if err := pinned.QueryRowContext(ctx, "SELECT @@GLOBAL.sql_notes").Scan(&global); err != nil {
		t.Fatal(err)
	}
	if global != 0 {
		t.Skipf("@@GLOBAL.sql_notes = %d: the pair needs 0, the environment matrix row sql_notes_0", global)
	}

	t.Run("side A: a BuildDSN session keeps note 1051", func(t *testing.T) {
		if got := notesAfterDrop(t, pinned); len(got) != 1 || got[0] != 1051 {
			t.Errorf("SHOW WARNINGS codes = %v, want [1051] under a global sql_notes=0", got)
		}
	})
	t.Run("side B: a session without the pin keeps none", func(t *testing.T) {
		if got := notesAfterDrop(t, rawPool(t, cfg, nil)); len(got) != 0 {
			t.Errorf("SHOW WARNINGS codes = %v, want none: the global sql_notes=0 should silence an unpinned session", got)
		}
	})
}
