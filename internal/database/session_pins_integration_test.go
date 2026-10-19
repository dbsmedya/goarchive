//go:build integration

package database

import (
	"context"
	"database/sql"
	"testing"
)

// TestMySQLSessionPins (M7c; beta audit A1): a session BuildDSN opens commits each statement
// on its own, whatever the server's global autocommit, and returns every row a read asks for,
// whatever the server's global sql_select_limit. Environment pair (R56): on the estate the
// global is the default (1); the M7c environment matrix sets it to 0 with SET GLOBAL on a
// throwaway server, and the session must still read 1.
func TestMySQLSessionPins(t *testing.T) {
	for _, prefix := range []string{"TEST_SOURCE", "TEST_DEST"} {
		t.Run(prefix, func(t *testing.T) {
			cfg := estateDatabaseConfig(t, prefix)
			db, err := sql.Open("mysql", BuildDSN(&cfg))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			var global, session int
			if err := db.QueryRowContext(context.Background(), "SELECT @@GLOBAL.autocommit, @@SESSION.autocommit").Scan(&global, &session); err != nil {
				t.Fatal(err)
			}
			if session != 1 {
				t.Errorf("session autocommit = %d with global %d, want 1", session, global)
			}
			// Beta audit A1: a global sql_select_limit caps every read without its own LIMIT;
			// the matrix row sql_select_limit_20 sets it to 20.
			var globalLimit, sessionLimit string
			if err := db.QueryRowContext(context.Background(), "SELECT CAST(@@GLOBAL.sql_select_limit AS CHAR), "+
				"CAST(@@SESSION.sql_select_limit AS CHAR)").Scan(&globalLimit, &sessionLimit); err != nil {
				t.Fatal(err)
			}
			if sessionLimit != "18446744073709551615" {
				t.Errorf("session sql_select_limit = %s with global %s, want 18446744073709551615", sessionLimit, globalLimit)
			}
		})
	}
}
