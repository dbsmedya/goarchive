//go:build integration

package database

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/lock"
)

// rawPool opens cfg's server without BuildDSN's session pins, with params as the only
// session settings.
func rawPool(t *testing.T, cfg config.DatabaseConfig, params map[string]string) *sql.DB {
	t.Helper()
	c := mysql.NewConfig()
	c.User, c.Passwd, c.Net, c.Addr = cfg.User, cfg.Password, "tcp", net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	c.Params = params
	connector, err := mysql.NewConnector(c)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// takeLock is another session's GET_LOCK(name, 0): 1 when it got the lock (then released),
// 0 when someone holds it.
func takeLock(t *testing.T, db *sql.DB, name string) int {
	t.Helper()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", name).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got.Int64 == 1 {
		_, _ = conn.ExecContext(ctx, "DO RELEASE_LOCK(?)", name)
	}
	return int(got.Int64)
}

// TestMySQLWaitTimeout (M7c-2 P4, register M7a-F2): a session BuildDSN opens keeps at least
// 8 h of idle time and never less than the server's own wait_timeout, so a server with a
// short wait_timeout does not close the job lock's dedicated session between the 30 s
// keep-alive checks (closing it releases GET_LOCK while the run goes on deleting), nor a
// source session pinned through a lag wait.
// Environment pair (R56, P7): side B, on every server, shows the hazard: a session whose
// wait_timeout is 2 s loses its job lock to another session after 3 s idle. Side A needs a
// server whose global wait_timeout is below 30 s, the environment matrix row
// wait_timeout_10, and skips elsewhere: a BuildDSN session holding the job lock idles past
// the global and still holds it, and a pinned connection still answers.
func TestMySQLWaitTimeout(t *testing.T) {
	ctx := context.Background()
	cfg := estateDatabaseConfig(t, "TEST_DEST")
	pinned, err := sql.Open("mysql", BuildDSN(&cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pinned.Close() })
	other := rawPool(t, cfg, nil)
	name := fmt.Sprintf("goarchive_m7c2_wait_timeout_%d", os.Getpid())

	var global, session int64
	if err := pinned.QueryRowContext(ctx, "SELECT @@GLOBAL.wait_timeout, @@SESSION.wait_timeout").Scan(&global, &session); err != nil {
		t.Fatal(err)
	}

	t.Run("the session keeps the larger of the server's value and 8 h", func(t *testing.T) {
		if want := max(global, 28800); session != want {
			t.Errorf("session wait_timeout = %d under a global of %d, want %d", session, global, want)
		}
	})

	t.Run("side B: a 2 s session loses its job lock while idle", func(t *testing.T) {
		l := lock.NewAdvisoryLock(rawPool(t, cfg, map[string]string{"wait_timeout": "2"}), name)
		if ok, err := l.AcquireLock(ctx, 0); err != nil || !ok {
			t.Fatalf("AcquireLock = %v, %v", ok, err)
		}
		t.Cleanup(func() { _, _ = l.ReleaseLock(ctx) })
		time.Sleep(3 * time.Second)
		if got := takeLock(t, other, name); got != 1 {
			t.Errorf("another session's GET_LOCK after the holder idled past its wait_timeout = %d, want 1 (the lock released)", got)
		}
	})

	t.Run("side A: a BuildDSN session keeps its job lock past the server's wait_timeout", func(t *testing.T) {
		if global >= 30 {
			t.Skipf("@@GLOBAL.wait_timeout = %d: side A needs one below the 30 s keep-alive, the environment matrix row wait_timeout_10", global)
		}
		l := lock.NewAdvisoryLock(pinned, name)
		if ok, err := l.AcquireLock(ctx, 0); err != nil || !ok {
			t.Fatalf("AcquireLock = %v, %v", ok, err)
		}
		conn, err := pinned.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		if err := conn.PingContext(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Duration(global+2) * time.Second)
		if got := takeLock(t, other, name); got != 0 {
			t.Errorf("another session's GET_LOCK after %d s idle = %d, want 0 (still held)", global+2, got)
		}
		if released, err := l.ReleaseLock(ctx); err != nil || !released {
			t.Errorf("ReleaseLock on the holder's own session = %v, %v; want true: the session that took the lock must still own it", released, err)
		}
		var one int
		if err := conn.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
			t.Errorf("a pinned connection after %d s idle: %v", global+2, err)
		}
	})
}
