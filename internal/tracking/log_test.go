package tracking

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
)

// execCounter is a Tx that records each statement's row count. Insert never reads.
type execCounter struct{ rows []int }

func (e *execCounter) ExecContext(_ context.Context, q string, _ ...any) (sql.Result, error) {
	e.rows = append(e.rows, strings.Count(q, "), (")+1)
	return nil, nil
}

func (e *execCounter) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errors.New("Insert does not read")
}

func intJob(t *testing.T) *Job {
	t.Helper()
	s := &Store{schema: "s", budget: 8 << 20}
	return s.newJob(7, JobInput{Name: "j", Type: jobspec.Purge,
		RootKey: key.Def{Table: "t", Columns: []key.Column{{Name: "id", Type: "int", Family: key.FamilySigned}}}})
}

// TestInsertChunks: an INSERT carries at most insertMax rows, so a batch over the cap is
// split and every key is sent once.
func TestInsertChunks(t *testing.T) {
	j := intJob(t)
	j.insertMax = 2
	var keys []key.Key
	for i := int64(1); i <= 5; i++ {
		keys = append(keys, key.Key{i})
	}
	var tx execCounter
	if err := j.Insert(context.Background(), &tx, StatusDeleting, keys); err != nil {
		t.Fatal(err)
	}
	if len(tx.rows) != 3 || tx.rows[0] != 2 || tx.rows[1] != 2 || tx.rows[2] != 1 {
		t.Errorf("statements carried %v rows, want [2 2 1]", tx.rows)
	}
}

// TestMembershipNeedsASession: Transition and Logged read warning 3170 after their
// statement, which proves nothing on a pool, where SHOW WARNINGS may run on another
// connection. They refuse a *sql.DB.
func TestMembershipNeedsASession(t *testing.T) {
	j := intJob(t)
	ctx, pool, keys := context.Background(), new(sql.DB), []key.Key{{int64(1)}}
	if err := j.Transition(ctx, pool, StatusDeleting, StatusCompleted, keys); err == nil || !strings.Contains(err.Error(), "not a pool") {
		t.Errorf("Transition on a pool = %v", err)
	}
	if _, err := j.Logged(ctx, pool, StatusCompleted, keys); err == nil || !strings.Contains(err.Error(), "not a pool") {
		t.Errorf("Logged on a pool = %v", err)
	}
}
