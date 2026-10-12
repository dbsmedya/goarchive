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
// access (spec §7.1a). Every chunk first passes the replication lag gate, and
// delete_sleep_seconds separates consecutive statements.
func (r *run) deleteTree(ctx context.Context, src connTx, t *tree) error {
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
			if err := r.deleteOne(ctx, src, n.Table, chunk); err != nil {
				return err
			}
			issued = true
			r.report.Deleted[n.Table] += int64(len(chunk))
			r.reached(atDeleted)
		}
	}
	return nil
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

// connTx is a pinned source connection: deletes run in their own transactions on it, and
// the reads of the same batch run on it too.
type connTx interface {
	queryer
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}
