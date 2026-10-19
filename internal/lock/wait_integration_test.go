//go:build integration

package lock

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

// holdOn takes name on a session of its own; it returns the session's connection id and a
// release, which the test's cleanup also runs.
func holdOn(t *testing.T, db *sql.DB, name string) (int64, func()) {
	t.Helper()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", name).Scan(&got); err != nil || got.Int64 != 1 {
		t.Fatalf("GET_LOCK(%s) = %v, %v", name, got, err)
	}
	var id int64
	if err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&id); err != nil {
		t.Fatal(err)
	}
	var once bool
	release := func() {
		if !once {
			once = true
			_, _ = conn.ExecContext(ctx, "DO RELEASE_LOCK(?)", name)
			_ = conn.Close()
		}
	}
	t.Cleanup(release)
	return id, release
}

// pendingOn reports whether a session waits for name (performance_schema.metadata_locks).
func pendingOn(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM performance_schema.metadata_locks "+
		"WHERE OBJECT_TYPE = 'USER LEVEL LOCK' AND OBJECT_NAME = ? AND LOCK_STATUS = 'PENDING'", name).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n > 0
}

// TestMySQLWait (TRK-1): Wait takes a free lock and its release frees it; a lock held
// throughout the wait is refused with its holder; a lock released during the wait is
// taken; a cancelled wait returns at once and leaves nothing waiting; a release after the
// caller's context ended still frees the lock (Fable F3).
func TestMySQLWait(t *testing.T) {
	db := pool(t)
	t.Run("free", func(t *testing.T) {
		name := unique(t, 1)[0]
		release, err := Wait(context.Background(), db, name, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if holder(t, db, name) == 0 {
			t.Fatal("Wait returned but no session holds the lock")
		}
		release()
		if h := holder(t, db, name); h != 0 {
			t.Errorf("held by %d after release", h)
		}
	})
	t.Run("held throughout", func(t *testing.T) {
		name := unique(t, 1)[0]
		id, _ := holdOn(t, db, name)
		start := time.Now()
		_, err := Wait(context.Background(), db, name, time.Second)
		var held *HeldError
		if !errors.As(err, &held) || held.Holder != id || held.Name != name {
			t.Fatalf("Wait() = %v, want a HeldError naming connection %d", err, id)
		}
		if d := time.Since(start); d < time.Second {
			t.Errorf("refused after %s, before the 1 s wait ran out", d)
		}
	})
	t.Run("released during the wait", func(t *testing.T) {
		name := unique(t, 1)[0]
		id, rel := holdOn(t, db, name)
		go func() { time.Sleep(300 * time.Millisecond); rel() }()
		release, err := Wait(context.Background(), db, name, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if h := holder(t, db, name); h == 0 || h == id {
			t.Errorf("holder = %d, want Wait's session", h)
		}
		release()
		if h := holder(t, db, name); h != 0 {
			t.Errorf("held by %d after release", h)
		}
	})
	t.Run("cancelled", func(t *testing.T) {
		name := unique(t, 1)[0]
		_, rel := holdOn(t, db, name)
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := Wait(ctx, db, name, 10*time.Second)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Wait() = %v, want the context's error", err)
		}
		if d := time.Since(start); d > 1500*time.Millisecond {
			t.Errorf("returned %s after the wait began; the context ended it at 500 ms", d)
		}
		deadline := time.Now().Add(time.Second)
		for pendingOn(t, db, name) {
			if time.Now().After(deadline) {
				t.Fatal("a session still waits for the lock 1 s after the cancelled Wait returned")
			}
			time.Sleep(50 * time.Millisecond)
		}
		rel()
		if h := holder(t, db, name); h != 0 {
			t.Errorf("held by %d after the holder released: the cancelled wait took it", h)
		}
	})
	t.Run("release after the context ended", func(t *testing.T) {
		name := unique(t, 1)[0]
		ctx, cancel := context.WithCancel(context.Background())
		release, err := Wait(ctx, db, name, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		release()
		if h := holder(t, db, name); h != 0 {
			t.Errorf("held by %d after a release on a cancelled context", h)
		}
	})
}

// TestMySQLWaitUnderAStatementTimer (TRK-1, Fable F2): a server whose max_execution_time is
// shorter than the wait does not abort it. The pool sets the timer on every new session, as
// an inherited global would; nothing global changes. The floor is 1.5 s, not 2 s: the server
// times GET_LOCK on its own clock and has returned 0.7 ms before the client's 2 s, while a
// wait the 1 s timer ended would return at about 1 s.
func TestMySQLWaitUnderAStatementTimer(t *testing.T) {
	timed := poolWith(t, map[string]string{"max_execution_time": "1000"})
	name := unique(t, 1)[0]
	id, _ := holdOn(t, pool(t), name)
	start := time.Now()
	_, err := Wait(context.Background(), timed, name, 2*time.Second)
	var held *HeldError
	if !errors.As(err, &held) || held.Holder != id {
		t.Fatalf("Wait() = %v, want a HeldError after the 2 s wait (error 3024 means the statement timer ended it)", err)
	}
	if d := time.Since(start); d < 1500*time.Millisecond {
		t.Errorf("refused after %s, well before the 2 s wait ran out", d)
	}
}
