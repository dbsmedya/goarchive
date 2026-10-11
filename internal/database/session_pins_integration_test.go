//go:build integration

package database

import (
	"context"
	"database/sql"
	"testing"
)

// TestMySQLSessionPins (M7c): a session BuildDSN opens commits each statement on its own,
// whatever the server's global autocommit. Environment pair (R56): on the estate the
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
		})
	}
}
