//go:build integration

package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
)

// TestMySQLMaxErrorCount (M7c-F5): Manager.Connect's source and destination sessions keep
// warnings whatever the server's global max_error_count. A session at 0 keeps none, and
// goarchive reads warnings after every INSERT and every range-checked statement.
// Environment pair (R56, P7) on a server whose global max_error_count is 0, the environment
// matrix row max_error_count_0; elsewhere it skips. Side A: the test accounts hold
// SESSION_VARIABLES_ADMIN, and every connection of both pools reads 1024 and keeps the
// warning a statement raises (SELECT 1/0, warning 1365): SHOW COUNT(*) WARNINGS and SHOW
// WARNINGS agree, the property the INSERT diagnostics and the range check need. Side B, the
// one change: a destination account without the privilege is refused at Connect by name,
// with both remedies, and not retried (connectWithRetry's only retry path ends "failed after
// 3 retries").
func TestMySQLMaxErrorCount(t *testing.T) {
	ctx := context.Background()
	src := estateDatabaseConfig(t, "TEST_SOURCE")
	dst := estateDatabaseConfig(t, "TEST_DEST")
	admin := rawPool(t, dst, nil)
	var global int64
	if err := admin.QueryRowContext(ctx, "SELECT @@GLOBAL.max_error_count").Scan(&global); err != nil {
		t.Fatal(err)
	}
	if global != 0 {
		t.Skipf("@@GLOBAL.max_error_count = %d: the pair needs 0, the environment matrix row max_error_count_0", global)
	}

	t.Run("side A: every connection of both pools reads 1024", func(t *testing.T) {
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
				var got int64
				if err := conn.QueryRowContext(ctx, "SELECT @@SESSION.max_error_count").Scan(&got); err != nil {
					t.Fatal(err)
				}
				if got != 1024 {
					t.Errorf("%s connection %d: @@SESSION.max_error_count = %d under a global of 0, want 1024", name, i, got)
				}
				var quotient sql.NullInt64
				if err := conn.QueryRowContext(ctx, "SELECT 1/0").Scan(&quotient); err != nil {
					t.Fatal(err)
				}
				var raised int64
				if err := conn.QueryRowContext(ctx, "SHOW COUNT(*) WARNINGS").Scan(&raised); err != nil {
					t.Fatal(err)
				}
				if kept := keptWarnings(t, conn); raised != 1 || kept != 1 {
					t.Errorf("%s connection %d: SELECT 1/0 raised %d warning(s) and SHOW WARNINGS kept %d, want 1 and 1", name, i, raised, kept)
				}
			}
		}
	})

	t.Run("side B: an account without SESSION_VARIABLES_ADMIN is refused at Connect", func(t *testing.T) {
		user := fmt.Sprintf("ga_mec_%d", os.Getpid())
		pass := fmt.Sprintf("mec-%d-%d", os.Getpid(), time.Now().UnixNano())
		account := "'" + user + "'@'%'"
		for _, q := range []string{
			"DROP USER IF EXISTS " + account,
			"CREATE USER " + account + " IDENTIFIED BY '" + pass + "'",
			"GRANT SELECT ON `" + dst.Database + "`.* TO " + account,
		} {
			if _, err := admin.ExecContext(ctx, q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
		t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+account) })
		weak := dst
		weak.User, weak.Password = user, pass

		m := NewManager(&config.Config{Source: src, Destination: weak})
		t.Cleanup(func() { _ = m.Close() })
		err := m.Connect(ctx)
		if err == nil {
			t.Fatal("Connect succeeded; want MAX_ERROR_COUNT_CHECK on the destination")
		}
		for _, want := range []string{"destination", "MAX_ERROR_COUNT_CHECK", "global max_error_count", "SESSION_VARIABLES_ADMIN"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("Connect error = %q, want it to contain %q", err, want)
			}
		}
		if strings.Contains(err.Error(), "retries") {
			t.Errorf("Connect error = %q: a refusal is not transient and must not be retried", err)
		}
		if m.Source != nil || m.Destination != nil {
			t.Error("Connect left a pool open after the refusal")
		}
	})
}

// keptWarnings is the number of rows SHOW WARNINGS lists on conn.
func keptWarnings(t *testing.T, conn *sql.Conn) int {
	t.Helper()
	rows, err := conn.QueryContext(context.Background(), "SHOW WARNINGS")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	n := 0
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return n
}
