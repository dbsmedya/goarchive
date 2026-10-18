//go:build integration

package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/config"
)

// TestMySQLSessionCharset (CHS-1): the session check refuses a session that is not utf8mb4 /
// utf8mb4_general_ci, as the server reports it, and every physical connection of Manager's
// source and destination pools passes it. Session variables are enough to narrow a real
// session, so no global changes.
func TestMySQLSessionCharset(t *testing.T) {
	ctx := context.Background()
	src := estateDatabaseConfig(t, "TEST_SOURCE")
	dst := estateDatabaseConfig(t, "TEST_DEST")

	t.Run("refuses a narrowed session", func(t *testing.T) {
		conn, err := rawPool(t, src, nil).Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		check := func(want string) {
			t.Helper()
			err := conn.Raw(func(dc any) error { return ensureSessionCharset(ctx, dc.(driver.Conn), "source") })
			var sce *SessionCharsetError
			if !errors.As(err, &sce) || !strings.Contains(err.Error(), want) {
				t.Errorf("ensureSessionCharset = %v, want SESSION_CHARSET_CHECK naming %s", err, want)
			}
		}
		for _, stmt := range []string{"SET NAMES latin1"} {
			if _, err := conn.ExecContext(ctx, stmt); err != nil {
				t.Fatal(err)
			}
		}
		check("results=latin1")
		for _, stmt := range []string{"SET NAMES utf8mb4 COLLATE utf8mb4_general_ci", "SET character_set_results = NULL"} {
			if _, err := conn.ExecContext(ctx, stmt); err != nil {
				t.Fatal(err)
			}
		}
		check("results=NULL")
	})

	t.Run("every physical connection", func(t *testing.T) {
		m := NewManager(&config.Config{Source: src, Destination: dst})
		if err := m.Connect(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = m.Close() })
		for name, db := range map[string]*sql.DB{"source": m.Source, "destination": m.Destination} {
			// Two connections held at once are two physical sessions.
			for i := 1; i <= 2; i++ {
				conn, err := db.Conn(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = conn.Close() }()
				var got [4]string
				if err := conn.QueryRowContext(ctx, sessionCharsetProbe).Scan(&got[0], &got[1], &got[2], &got[3]); err != nil {
					t.Fatal(err)
				}
				if want := [4]string{"utf8mb4", "utf8mb4", "utf8mb4", "utf8mb4_general_ci"}; got != want {
					t.Errorf("%s connection %d: session %q, want %q", name, i, got, want)
				}
			}
		}
	})
}
