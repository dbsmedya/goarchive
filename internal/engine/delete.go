package engine

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dbsmedya/goarchive/v3/internal/key"
	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// deleteTree deletes a batch's discovered rows from the source, children first (spec
// §7.5). Each DELETE is one statement of at most batch_delete_size keys in its own
// transaction, which commits only when the rows affected equal the chunk size and MySQL kept
// range access (spec §7.1a). The chunks are taken one at a time by an offset into the table's
// keys, and each DELETE goes through the same five steps, in this order (D16):
//
//  1. the sleep, delete_sleep_seconds, only after a DELETE was already issued in this batch;
//     a first stop does not shorten it (nil stop). After it a healthy gate lets the batch
//     finish; a hold ends under LAG-1's rule, and the next run's recovery finishes the
//     unfinished batch;
//  2. the replication lag gate, whose decision is made after the sleep and just before the
//     statement, not before the sleep. The decision may be a pass still valid in the gate's
//     cache (cache_ttl), which lets the DELETE go without a new probe;
//  3. a source session taken for the chunk, so no session sits idle through a sleep or a hold
//     (LAG-1);
//  4. the chunk: min(batch_delete_size, the statement cap) keys from the offset, asked for
//     after the gate, so a size that changes during a sleep or a hold applies to this DELETE;
//  5. the DELETE.
//
// Recovery's replay shares this path. A purge's gate before its roots are logged is separate.
func (r *run) deleteTree(ctx context.Context, t *tree) error {
	r.reached(atDeleteTree)
	issued := false
	for _, n := range r.tree.DeleteOrder() {
		c := newChunkCursor(t.keys[n.Table])
		for !c.done() {
			if issued {
				r.progress.at("sleep", "")
				r.reached(atDeleteSleep)
				if err := followSleep(ctx, nil, r.tune, func(s tuning.Settings) float64 { return s.DeleteSleepSeconds }); err != nil {
					return err
				}
				r.reached(atDeleteSlept)
			}
			r.reached(atDeleteGate)
			if err := r.waitForLag(ctx); err != nil {
				return err
			}
			r.reached(atDeleteGated)
			r.progress.at("delete", n.Table)
			chunk, err := r.deleteNext(ctx, n.Table, c)
			if err != nil {
				return err
			}
			issued = true
			r.report.Deleted[n.Table] += int64(len(chunk))
			r.progress.deleted(n.Table, int64(len(chunk)))
			r.reached(atDeleted)
		}
	}
	return nil
}

// deleteNext is steps 3 to 5 of deleteTree: it takes a source session from the pool (the pool
// retires a session idle for 5 minutes, and the driver's liveness check refuses one the server
// closed sooner, so a hold of any length leaves the chunk a live session), then the next chunk
// of c, and deletes it. It returns the keys it deleted.
func (r *run) deleteNext(ctx context.Context, table string, c *chunkCursor) ([]key.Key, error) {
	conn, err := r.cfg.Source.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	r.reached(atDeleteSession)
	chunk := c.take(r.deleteChunk(table))
	r.reached(atDeleteIssue)
	if err := r.deleteOne(ctx, conn, table, chunk); err != nil {
		return nil, err
	}
	return chunk, nil
}

// chunkCursor hands out a table's keys one delete chunk at a time, by an offset. The size is
// asked for at each take, so a size that changes between two DELETEs applies to the next one;
// the offset advances by what was taken, so every key is handed out exactly once and in order.
type chunkCursor struct {
	keys   []key.Key
	offset int
}

func newChunkCursor(keys []key.Key) *chunkCursor { return &chunkCursor{keys: keys} }

// done reports whether every key has been handed out.
func (c *chunkCursor) done() bool { return c.offset >= len(c.keys) }

// take returns the next size keys, fewer at the end of the table. A size below one is one,
// as key.Chunks has it. The chunk has no spare capacity, so a statement that appends to it
// cannot write into the keys behind it.
func (c *chunkCursor) take(size int) []key.Key {
	n := min(max(size, 1), len(c.keys)-c.offset)
	end := c.offset + max(n, 0)
	chunk := c.keys[c.offset:end:end]
	c.offset = end
	return chunk
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
// session (deleteNext).
type connTx interface {
	queryer
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}
