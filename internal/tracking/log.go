package tracking

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/dbsmedya/dbsgomysql/pkg/sqlutil"

	"github.com/dbsmedya/goarchive/v3/internal/key"
)

// Tx is the caller's destination transaction, or a pinned connection (Insert, Transition
// and Logged refuse a *sql.DB). Statuses are
// written in the transaction that does the work they record (spec §6.3), and warning
// 3170 is read on the same session as its statement (spec §7.1a).
type Tx interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (j *Job) keyColumns() []string { return j.log.Names() }

func logTableName(id int64) string { return fmt.Sprintf("archiver_job_log_%d", id) }

// PrimaryKeyName is the log's primary key as MySQL names it in a duplicate-entry error
// (table.key, MySQL 8.0.19 and later): a 1062 that names it is a root the log already holds.
func (j *Job) PrimaryKeyName() string { return logTableName(j.ID) + ".PRIMARY" }

func (j *Job) quotedKeyColumns() string {
	cols := j.keyColumns()
	for i, c := range cols {
		cols[i] = sqlutil.QuoteIdentifier(c)
	}
	return strings.Join(cols, ", ")
}

// session refuses a pool: a status written on a *sql.DB is outside the work's
// transaction, and SHOW WARNINGS on one may run on another connection and prove nothing.
func (j *Job) session(tx Tx, op string) error {
	if _, ok := tx.(*sql.DB); ok {
		return fmt.Errorf("tracking: job %q: %s needs a transaction or a pinned connection, not a pool", j.Name, op)
	}
	return nil
}

func (j *Job) validateKeys(keys []key.Key) error {
	for _, k := range keys {
		if err := j.log.Validate(k); err != nil {
			return fmt.Errorf("tracking: job %q: %w", j.Name, err)
		}
	}
	return nil
}

// Insert logs keys with status st in tx. The INSERT is strict: a key the log already
// holds fails the statement, and with it the caller's transaction (R25: a key collision
// fails loudly on the log's primary key).
func (j *Job) Insert(ctx context.Context, tx Tx, st Status, keys []key.Key) error {
	if !st.valid() {
		return fmt.Errorf("tracking: status %d", st)
	}
	if err := j.session(tx, "Insert"); err != nil {
		return err
	}
	if err := j.validateKeys(keys); err != nil {
		return err
	}
	phs := make([]string, len(j.log.Columns)+1)
	for i, c := range j.log.Columns {
		phs[i] = key.Placeholder(c)
	}
	phs[len(phs)-1] = "?"
	row := "(" + strings.Join(phs, ", ") + ")"
	for _, chunk := range key.Chunks(keys, j.insertMax) {
		rows := make([]string, len(chunk))
		args := make([]any, 0, len(chunk)*len(phs))
		for i, k := range chunk {
			rows[i] = row
			args = append(args, k...)
			args = append(args, st)
		}
		q := fmt.Sprintf("INSERT INTO %s (%s, %s) VALUES %s", j.logTable, j.quotedKeyColumns(), colStatus, strings.Join(rows, ", "))
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("tracking: log %d root(s) as %s for job %q: %w", len(chunk), st, j.Name, err)
		}
	}
	return nil
}

// Transition moves keys from status from to status to in tx: copied → completed after an
// archive's deletes, deleting → completed after a purge's. Every key must be in from:
// fewer rows changed fails, so the caller's transaction rolls back (spec §7.1a).
func (j *Job) Transition(ctx context.Context, tx Tx, from, to Status, keys []key.Key) error {
	if !from.valid() || !to.valid() || from == to {
		return fmt.Errorf("tracking: transition %s → %s", from, to)
	}
	if err := j.session(tx, "Transition"); err != nil {
		return err
	}
	if err := j.validateKeys(keys); err != nil {
		return err
	}
	for _, chunk := range key.Chunks(keys, j.maxTuples) {
		q := fmt.Sprintf("UPDATE %s SET %s = ? WHERE %s = ? AND %s", j.logTable, colStatus, colStatus,
			key.In(j.keyColumns(), j.log.Columns, len(chunk)))
		res, err := tx.ExecContext(ctx, q, append([]any{to, from}, key.Args(chunk)...)...)
		if err != nil {
			return fmt.Errorf("tracking: mark %d root(s) %s for job %q: %w", len(chunk), to, j.Name, err)
		}
		if err := key.CheckRangeWarning(ctx, tx); err != nil {
			return fmt.Errorf("tracking: mark root(s) %s for job %q: %w", to, j.Name, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("tracking: mark root(s) %s for job %q: %w", to, j.Name, err)
		}
		if n != int64(len(chunk)) {
			return fmt.Errorf("tracking: job %q: %d of %d root(s) were in status %s; refusing to mark them %s",
				j.Name, n, len(chunk), from, to)
		}
	}
	return nil
}

// Logged returns the keys among keys that the log holds in status st, in no particular
// order: copy-only resume: true drops a batch's roots this job already completed (spec
// §6.6).
func (j *Job) Logged(ctx context.Context, tx Tx, st Status, keys []key.Key) ([]key.Key, error) {
	if !st.valid() {
		return nil, fmt.Errorf("tracking: status %d", st)
	}
	if err := j.session(tx, "Logged"); err != nil {
		return nil, err
	}
	if err := j.validateKeys(keys); err != nil {
		return nil, err
	}
	var out []key.Key
	for _, chunk := range key.Chunks(keys, j.maxTuples) {
		q := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ? AND %s", key.Projection(j.log), j.logTable, colStatus,
			key.In(j.keyColumns(), j.log.Columns, len(chunk)))
		found, err := j.scan(ctx, tx, q, append([]any{st}, key.Args(chunk)...))
		if err != nil {
			return nil, err
		}
		if err := key.CheckRangeWarning(ctx, tx); err != nil {
			return nil, fmt.Errorf("tracking: read the log of job %q: %w", j.Name, err)
		}
		out = append(out, found...)
	}
	return out, nil
}

// Roots returns up to limit keys in status st, in key order, after the key after (nil:
// from the start). Recovery reads copied (archive) and deleting (purge) roots this way.
func (j *Job) Roots(ctx context.Context, tx Tx, st Status, after key.Key, limit int) ([]key.Key, error) {
	if !st.valid() || limit < 1 {
		return nil, fmt.Errorf("tracking: roots in status %d, limit %d", st, limit)
	}
	where, args := colStatus+" = ?", []any{st}
	if after != nil {
		if err := j.log.Validate(after); err != nil {
			return nil, fmt.Errorf("tracking: job %q: %w", j.Name, err)
		}
		where += " AND " + key.After(j.log)
		args = append(args, key.AfterArgs(after)...)
	}
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT %d", key.Projection(j.log), j.logTable, where,
		key.OrderBy(j.log), limit)
	return j.scan(ctx, tx, q, args)
}

func (j *Job) scan(ctx context.Context, tx Tx, q string, args []any) ([]key.Key, error) {
	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("tracking: read the log of job %q: %w", j.Name, err)
	}
	defer func() { _ = rows.Close() }()
	var out []key.Key
	for rows.Next() {
		k, err := j.log.Scan(rows)
		if err != nil {
			return nil, fmt.Errorf("tracking: read the log of job %q: %w", j.Name, err)
		}
		out = append(out, k)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("tracking: read the log of job %q: %w", j.Name, err)
	}
	return out, nil
}

// count is the number of roots in any of statuses.
func (j *Job) count(ctx context.Context, statuses []Status) (int64, error) {
	phs := make([]string, len(statuses))
	args := make([]any, len(statuses))
	for i, s := range statuses {
		phs[i], args[i] = "?", s
	}
	var n int64
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s IN (%s)", j.logTable, colStatus, strings.Join(phs, ", "))
	if err := j.store.db.QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("tracking: count the pending roots of job %q: %w", j.Name, err)
	}
	return n, nil
}
