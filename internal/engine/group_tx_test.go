package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
)

// The real transaction functions, on a driver that records what they send (spec §8, GR C3, Codex round 5.2): a
// statement that fails with 3231 ends its transaction with a GroupWriteSetError, the transaction is rolled
// back and never committed (MySQL leaves it open and a COMMIT would fail with 3100), and every other error
// the transaction ends with passes through as it was.

// txRecord is what a fake pool was asked to do, in order.
type txRecord struct {
	mu      sync.Mutex
	events  []string
	execErr error // what every statement returns, nil for success
}

func (r *txRecord) add(e string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *txRecord) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.events)
}

type fakeTxConnector struct{ rec *txRecord }

func (c fakeTxConnector) Connect(context.Context) (driver.Conn, error) {
	return &fakeTxConn{c.rec}, nil
}
func (c fakeTxConnector) Driver() driver.Driver { return fakeTxDriver{} }

type fakeTxDriver struct{}

func (fakeTxDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("fake driver: opened by name")
}

type fakeTxConn struct{ rec *txRecord }

func (c *fakeTxConn) Prepare(string) (driver.Stmt, error) { return &fakeTxStmt{c.rec}, nil }
func (c *fakeTxConn) Close() error                        { return nil }
func (c *fakeTxConn) Begin() (driver.Tx, error)           { c.rec.add("begin"); return fakeTx{c.rec}, nil }

type fakeTx struct{ rec *txRecord }

func (t fakeTx) Commit() error   { t.rec.add("commit"); return nil }
func (t fakeTx) Rollback() error { t.rec.add("rollback"); return nil }

type fakeTxStmt struct{ rec *txRecord }

func (s *fakeTxStmt) Close() error  { return nil }
func (s *fakeTxStmt) NumInput() int { return -1 }
func (s *fakeTxStmt) Exec([]driver.Value) (driver.Result, error) {
	s.rec.add("exec")
	if s.rec.execErr != nil {
		return nil, s.rec.execErr
	}
	return driver.RowsAffected(1), nil
}
func (s *fakeTxStmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, errors.New("fake driver: no queries")
}

// fakePool is a *sql.DB on the recording driver.
func fakePool(t *testing.T, execErr error) (*sql.DB, *txRecord) {
	t.Helper()
	rec := &txRecord{execErr: execErr}
	db := sql.OpenDB(fakeTxConnector{rec})
	t.Cleanup(func() { _ = db.Close() })
	return db, rec
}

// TestAFailedStatementIsRolledBackAndNeverCommitted: a log write whose statement fails with 3231 returns a
// GroupWriteSetError and the transaction is rolled back without a commit; any other error is returned as it
// was, and rolled back the same way.
func TestAFailedStatementIsRolledBackAndNeverCommitted(t *testing.T) {
	me := writeSet3231()
	other := &mysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock"}
	for _, tc := range []struct {
		name   string
		facts  preflight.Group
		fail   error
		commit Commit
		roots  tracking.Status
		typed  bool // becomes a GroupWriteSetError
	}{
		{"3231 in the log INSERT before the deletes", limitFacts("ONLINE", 65536),
			fmt.Errorf("tracking: log 2 root(s) as deleting for job %q: %w", "j", me), CommitLogBefore, tracking.StatusDeleting, true},
		{"3231 in the log UPDATE after the deletes", limitFacts("ONLINE", 65536),
			fmt.Errorf("tracking: mark 2 root(s) completed for job %q: %w", "j", me), CommitLogAfter, tracking.StatusCopied, true},
		{"3231 with a limit of 0", limitFacts("ONLINE", 0),
			fmt.Errorf("tracking: mark 2 root(s) completed for job %q: %w", "j", me), CommitLogAfter, tracking.StatusCopied, true},
		{"3231 on a side with no known limit", preflight.Group{}, me, CommitLogAfter, tracking.StatusCopied, false},
		{"another MySQL error", limitFacts("ONLINE", 65536), other, CommitLogAfter, tracking.StatusCopied, false},
		{"a 3100 returned by a statement is not a commit's", limitFacts("ONLINE", 65536), hook3100(), CommitLogAfter, tracking.StatusCopied, false},
		{"a plain error", limitFacts("ONLINE", 65536), errors.New("connection reset"), CommitLogAfter, tracking.StatusCopied, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, rec := fakePool(t, nil)
			r := limitRun(jobspec.Purge, tc.facts)
			r.cfg.Destination = db
			err := r.inDestinationTx(context.Background(), tc.commit, tc.roots, func(*sql.Tx) error { return tc.fail })
			var gwe *GroupWriteSetError
			switch {
			case tc.typed && !errors.As(err, &gwe):
				t.Fatalf("returned %v (%T), want a GroupWriteSetError", err, err)
			case !tc.typed && (err != tc.fail || errors.As(err, &gwe)):
				t.Fatalf("returned %v, want %v untouched", err, tc.fail)
			}
			if tc.typed && (gwe.Commit != tc.commit || gwe.Roots != tc.roots || !errors.Is(err, tc.fail)) {
				t.Errorf("GroupWriteSetError = commit %d roots %s, chain holds the statement's error %v", gwe.Commit, gwe.Roots, errors.Is(err, tc.fail))
			}
			if got, want := rec.seen(), []string{"begin", "rollback"}; !slices.Equal(got, want) {
				t.Errorf("the transaction did %v, want %v: a failed statement is rolled back and never committed", got, want)
			}
		})
	}
}

// TestADeleteChunkThatFailsIsRolledBackAndNeverCommitted is the same for a delete chunk on the real deleteOne:
// its DELETE fails with 3231 and the chunk's transaction is begun, the statement sent, and rolled back.
func TestADeleteChunkThatFailsIsRolledBackAndNeverCommitted(t *testing.T) {
	chunk := []key.Key{{int64(1)}, {int64(2)}}
	other := &mysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock"}
	for _, tc := range []struct {
		name  string
		facts preflight.Group
		fail  error
		typed bool
	}{
		{"3231", limitFacts("ONLINE", 65536), writeSet3231(), true},
		{"3231, a source with no known limit", preflight.Group{}, writeSet3231(), false},
		{"another MySQL error", limitFacts("ONLINE", 65536), other, false},
		{"a 3100", limitFacts("ONLINE", 65536), hook3100(), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, rec := fakePool(t, tc.fail)
			r := limitRun(jobspec.Archive, tc.facts)
			r.keys = map[string]key.Def{"order_items": {Table: "order_items", Columns: []key.Column{intCol("id")}}}
			err := r.deleteOne(context.Background(), db, "order_items", chunk)
			var gwe *GroupWriteSetError
			var gce *GroupCommitError
			if errors.As(err, &gce) {
				t.Fatalf("a statement's error became a GroupCommitError: %v", err)
			}
			switch {
			case tc.typed && !errors.As(err, &gwe):
				t.Fatalf("returned %v (%T), want a GroupWriteSetError", err, err)
			case !tc.typed && (errors.As(err, &gwe) || !errors.Is(err, tc.fail)):
				t.Fatalf("returned %v, want the statement's own error in the usual wrapper", err)
			}
			if tc.typed && (gwe.Commit != CommitDelete || gwe.Side != "source" || gwe.What != "delete from order_items" || gwe.Roots != tracking.StatusCopied) {
				t.Errorf("GroupWriteSetError = %+v, want a delete chunk of order_items on the source, roots copied", gwe)
			}
			if got, want := rec.seen(), []string{"begin", "exec", "rollback"}; !slices.Equal(got, want) {
				t.Errorf("the chunk did %v, want %v: a failed DELETE is rolled back and never committed", got, want)
			}
		})
	}
}
