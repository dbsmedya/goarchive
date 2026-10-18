//go:build integration

package lock

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/database"
)

// These tests run on TEST_SOURCE_* (db1). Each pool is the test's own, so its connection
// counts are the set's alone.
func pool(t *testing.T) *sql.DB { return poolWith(t, nil) }

// poolWith is pool with server globals the test emulates under the pins (database.Global).
func poolWith(t *testing.T, params map[string]string) *sql.DB {
	t.Helper()
	host, user, pass := os.Getenv("TEST_SOURCE_HOST"), os.Getenv("TEST_SOURCE_USER"), os.Getenv("TEST_SOURCE_PASSWORD")
	port, _ := strconv.Atoi(os.Getenv("TEST_SOURCE_PORT"))
	if host == "" || user == "" || port == 0 {
		t.Fatal("integration environment not loaded: set -a; source tests/.env; set +a")
	}
	cfg := config.DatabaseConfig{Host: host, Port: port, User: user, Password: pass, TLS: "disable"}
	var globals []database.Global
	for _, k := range slices.Sorted(maps.Keys(params)) {
		globals = append(globals, database.Global{Name: k, Value: params[k]})
	}
	// goarchive's own pool (database.OpenPool); the cap is lifted, as the suites race more
	// sessions than a run.
	db, err := database.OpenPool(t.Context(), &cfg, true, globals...)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(0)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// unique returns lock names no other test or run uses.
func unique(t *testing.T, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = TableLockName(fmt.Sprintf("ga3stl_%d", os.Getpid()), fmt.Sprintf("%s_%d", t.Name(), i))
	}
	return out
}

// holder returns the connection id holding name on db's server, or 0 when it is free.
func holder(t *testing.T, db *sql.DB, name string) int64 {
	t.Helper()
	var id sql.NullInt64
	if err := db.QueryRowContext(context.Background(), "SELECT IS_USED_LOCK(?)", name).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id.Int64
}

// TestMySQLSetLongNames (T2): the names of the longest identities config allows are
// acquired and released; the old constructors' names failed with error 4163 (probe L1).
func TestMySQLSetLongNames(t *testing.T) {
	ctx := context.Background()
	db, observer := pool(t), pool(t)
	names := []string{
		TableLockName(strings.Repeat("d", 64), strings.Repeat("t", 64)),
		JobLockName(strings.Repeat("s", 64), strings.Repeat("j", 255)),
	}
	s := NewSet(db, names...)
	if err := s.Acquire(ctx); err != nil {
		t.Fatalf("Acquire = %v", err)
	}
	for _, n := range names {
		if got := holder(t, observer, n); got != s.connID {
			t.Errorf("%s held by %d, want the set's session %d", n, got, s.connID)
		}
	}
	if err := s.Release(ctx); err != nil {
		t.Fatal(err)
	}
	for _, n := range names {
		if got := holder(t, observer, n); got != 0 {
			t.Errorf("%s still held by %d after Release", n, got)
		}
	}
}

// TestMySQLSetSession (T6): the lock session runs at wait_timeout 300, is discarded on
// release, and a new set gets a new session.
func TestMySQLSetSession(t *testing.T) {
	ctx := context.Background()
	db := pool(t)
	names := unique(t, 1)
	s := NewSet(db, names...)
	if err := s.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	var wt int64
	if err := s.conn.QueryRowContext(ctx, "SELECT @@SESSION.wait_timeout").Scan(&wt); err != nil {
		t.Fatal(err)
	}
	if wt != 300 {
		t.Errorf("lock session wait_timeout = %d, want 300", wt)
	}
	first := s.connID
	if err := s.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if got := db.Stats().OpenConnections; got != 0 {
		t.Errorf("%d open connections after Release; the lock session must not return to the pool", got)
	}
	again := NewSet(db, names...)
	if err := again.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = again.Release(ctx) }()
	if again.connID == first {
		t.Errorf("a new set reused connection %d", first)
	}
}

// TestMySQLSetTimeoutFreesLocks (T7): with no keep-alive, the server frees a silent set
// after the session's wait_timeout. The lost-host case itself is probe L4.
func TestMySQLSetTimeoutFreesLocks(t *testing.T) {
	saved := sessionWaitTimeout
	sessionWaitTimeout = 2
	t.Cleanup(func() { sessionWaitTimeout = saved })
	ctx := context.Background()
	db, observer := pool(t), pool(t)
	names := unique(t, 2)
	s := NewSet(db, names...)
	if err := s.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Release(ctx) }()
	time.Sleep(4 * time.Second)
	for _, n := range names {
		if got := holder(t, observer, n); got != 0 {
			t.Errorf("%s still held by %d after the session idled past its wait_timeout", n, got)
		}
	}
	if err := s.Check(ctx); !errors.Is(err, ErrLost) {
		t.Errorf("Check after the timeout = %v, want ErrLost", err)
	}
}

// TestMySQLSetRefusal: one held name refuses the whole set, names its holder, and leaves
// none of the set's other names held.
func TestMySQLSetRefusal(t *testing.T) {
	ctx := context.Background()
	db, observer := pool(t), pool(t)
	names := unique(t, 3)
	other := NewSet(observer, names[1])
	if err := other.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.Release(ctx) }()
	var held *HeldError
	if err := NewSet(db, names...).Acquire(ctx); !errors.As(err, &held) {
		t.Fatalf("Acquire = %v, want *HeldError", err)
	}
	if held.Name != names[1] || held.Holder != other.connID {
		t.Errorf("HeldError = %+v, want %s held by %d", held, names[1], other.connID)
	}
	for _, n := range []string{names[0], names[2]} {
		if got := holder(t, observer, n); got != 0 {
			t.Errorf("%s held by %d after the refusal", n, got)
		}
	}
}

// TestMySQLSetCancelledReleaseFreesLock (issue #79): a release on a cancelled context
// cannot run RELEASE_ALL_LOCKS, and the lock is still free at once.
func TestMySQLSetCancelledReleaseFreesLock(t *testing.T) {
	ctx := context.Background()
	db, observer := pool(t), pool(t)
	names := unique(t, 1)
	s := NewSet(db, names...)
	if err := s.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_ = s.Release(cancelled)
	deadline := time.Now().Add(3 * time.Second)
	for holder(t, observer, names[0]) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%s still held 3 s after a cancelled release", names[0])
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestMySQLLockNameFormula (probe L8): MySQL computes the same names, so an operator can
// find a job's lock from SQL (README_JOBS_SCHEMA.md, README_OPERATIONS.md).
func TestMySQLLockNameFormula(t *testing.T) {
	db := pool(t)
	for _, tc := range []struct{ prefix, a, b, want string }{
		{"goarchive:j:", "GoArchive", "Archive_Old_Orders", JobLockName("GoArchive", "Archive_Old_Orders")},
		{"goarchive:t:", "Sakila", "Payment", TableLockName("Sakila", "Payment")},
		{"goarchive:s:", "GoArchive", "", SchemaLockName("GoArchive")},
	} {
		var got string
		if err := db.QueryRowContext(context.Background(),
			"SELECT CONCAT(?, LEFT(SHA2(CONCAT(LOWER(?), CHAR(0), LOWER(?)), 256), 32))", tc.prefix, tc.a, tc.b).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Errorf("SQL formula(%s, %s) = %s, want %s", tc.a, tc.b, got, tc.want)
		}
	}
}
