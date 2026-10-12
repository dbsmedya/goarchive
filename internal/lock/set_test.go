package lock

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// acquired returns a set of names acquired on a mock session with connection id 7.
func acquired(t *testing.T, names ...string) (*Set, *sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectExec(regexp.QuoteMeta("SET SESSION wait_timeout = 300")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT CONNECTION_ID()")).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(7)))
	for range names {
		mock.ExpectQuery(regexp.QuoteMeta("SELECT GET_LOCK(?, 0)")).WillReturnRows(sqlmock.NewRows([]string{"l"}).AddRow(int64(1)))
	}
	s := NewSet(db, names...)
	if err := s.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire = %v", err)
	}
	return s, db, mock
}

// TestSetCheckTreatsNullAsLost (T5a): a NULL or a 0 from the ownership check is a lost
// lock, and the session is discarded.
func TestSetCheckTreatsNullAsLost(t *testing.T) {
	for _, tc := range []struct {
		name string
		row  any
	}{{"NULL", nil}, {"another holder", int64(0)}} {
		t.Run(tc.name, func(t *testing.T) {
			s, db, mock := acquired(t, "goarchive:t:a", "goarchive:t:b")
			mock.ExpectQuery(regexp.QuoteMeta("SELECT (IS_USED_LOCK(?) <=> CONNECTION_ID()) AND (IS_USED_LOCK(?) <=> CONNECTION_ID())")).
				WithArgs("goarchive:t:a", "goarchive:t:b").
				WillReturnRows(sqlmock.NewRows([]string{"ok"}).AddRow(tc.row))
			if err := s.Check(context.Background()); !errors.Is(err, ErrLost) {
				t.Fatalf("Check = %v, want ErrLost", err)
			}
			if got := db.Stats().OpenConnections; got != 0 {
				t.Errorf("%d open connections after a lost check; the session must be discarded", got)
			}
			if got := s.ConnectionID(); got != 7 {
				t.Errorf("ConnectionID after the loss = %d, want 7 (kept for the final-status check)", got)
			}
		})
	}
}

// TestSetRefusalWithoutHolder (F1 M3): a holder that released between GET_LOCK's 0 and the
// lookup leaves Holder 0, and the session is discarded.
func TestSetRefusalWithoutHolder(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectExec(regexp.QuoteMeta("SET SESSION wait_timeout = 300")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT CONNECTION_ID()")).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(7)))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT GET_LOCK(?, 0)")).WillReturnRows(sqlmock.NewRows([]string{"l"}).AddRow(int64(0)))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT IS_USED_LOCK(?)")).WillReturnRows(sqlmock.NewRows([]string{"h"}).AddRow(nil))
	mock.ExpectExec(regexp.QuoteMeta("DO RELEASE_ALL_LOCKS()")).WillReturnResult(sqlmock.NewResult(0, 0))
	var held *HeldError
	if err := NewSet(db, "goarchive:t:a").Acquire(context.Background()); !errors.As(err, &held) || held.Holder != 0 {
		t.Fatalf("Acquire = %v, want *HeldError with Holder 0", err)
	}
	if got := db.Stats().OpenConnections; got != 0 {
		t.Errorf("%d open connections after a refusal", got)
	}
}

// TestSetFailedReleaseDiscardsSession (issue #79, re-proven on the set): a RELEASE that
// fails still discards the session, so the lock cannot live on in the pool.
func TestSetFailedReleaseDiscardsSession(t *testing.T) {
	s, db, mock := acquired(t, "goarchive:j:a")
	mock.ExpectExec(regexp.QuoteMeta("DO RELEASE_ALL_LOCKS()")).WillReturnError(errors.New("synthetic release failure"))
	if err := s.Release(context.Background()); err == nil {
		t.Fatal("Release must report the failure")
	}
	if got := db.Stats().OpenConnections; got != 0 {
		t.Fatalf("failed release left %d open connection(s); the session must be discarded", got)
	}
}

// TestSetIsAcquiredOnce (T6, unit half): a set never reuses a session.
func TestSetIsAcquiredOnce(t *testing.T) {
	s, _, _ := acquired(t, "goarchive:j:a")
	if err := s.Acquire(context.Background()); err == nil {
		t.Fatal("a second Acquire on one set must fail")
	}
}

// TestCheckSQL (Review Focus 3): a wide tree's check carries one NULL-safe term per name,
// joined by AND, and nothing else.
func TestCheckSQL(t *testing.T) {
	want := "SELECT (IS_USED_LOCK(?) <=> CONNECTION_ID()) AND (IS_USED_LOCK(?) <=> CONNECTION_ID()) AND (IS_USED_LOCK(?) <=> CONNECTION_ID())"
	if got := checkSQL(3); got != want {
		t.Errorf("checkSQL(3) = %q, want %q", got, want)
	}
}
