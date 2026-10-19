package engine

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dbsmedya/goarchive/v3/internal/key"
)

// deleteTree deletes a batch's discovered rows from the source, children first (spec
// §7.5). Each chunk of batch_delete_size keys is one statement in its own transaction,
// which commits only when the rows affected equal the chunk size and MySQL kept range
// access (spec §7.1a). Every chunk first passes the replication lag gate, then takes a
// session of its own, so no session sits idle through a hold (LAG-1);
// delete_sleep_seconds separates consecutive statements.
func (r *run) deleteTree(ctx context.Context, t *tree) error {
	issued := false
	for _, n := range r.tree.DeleteOrder() {
		for _, chunk := range key.Chunks(t.keys[n.Table], r.deleteChunk(n.Table)) {
			if err := r.waitForLag(ctx); err != nil {
				return err
			}
			if issued {
				if err := interruptibleSleep(ctx, nil, seconds(r.cfg.Processing.DeleteSleepSeconds)); err != nil {
					return err
				}
			}
			if err := r.deleteOnSession(ctx, n.Table, chunk); err != nil {
				return err
			}
			issued = true
			r.report.Deleted[n.Table] += int64(len(chunk))
			r.reached(atDeleted)
		}
	}
	return nil
}

// deleteOnSession runs one chunk's delete on a source session taken for it from the pool:
// the pool retires a session idle for 5 minutes, and the driver's liveness check refuses
// one the server closed sooner, so a hold of any length leaves the chunk a live session.
func (r *run) deleteOnSession(ctx context.Context, table string, chunk []key.Key) error {
	conn, err := r.cfg.Source.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	return r.deleteOne(ctx, conn, table, chunk)
}

func (r *run) deleteOne(ctx context.Context, src connTx, table string, chunk []key.Key) error {
	st := statement{op: opDelete, table: table, query: deleteSQL(r.keys[table], len(chunk)), args: key.Args(chunk)}
	r.prepare(&st)
	tx, err := src.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx, st.query, st.args...)
	if err != nil {
		return fmt.Errorf("engine: delete from %s: %w", table, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("engine: delete from %s: %w", table, err)
	}
	if want := int64(len(chunk)); n != want {
		e := &KeySetError{Table: table, Statement: string(opDelete), Requested: want, Matched: min(n, want)}
		if n > want {
			e.Extra = n - want
		}
		return e
	}
	if err := key.CheckRangeWarning(ctx, tx); err != nil {
		return fmt.Errorf("engine: delete from %s: %w", table, err)
	}
	if err := r.commit(ctx, tx, atDeleteWritten); err != nil {
		return fmt.Errorf("engine: delete from %s: commit: %w", table, err)
	}
	return nil
}

// connTx is a source session a delete runs on, in its own transaction: a chunk's own
// session (deleteOnSession).
type connTx interface {
	queryer
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}
