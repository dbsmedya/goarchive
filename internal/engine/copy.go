package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
	"github.com/dbsmedya/goarchive/v3/internal/types"
)

const mysqlDuplicateEntry = 1062

// copy copies one batch's discovered rows into the destination, parents first, verifies
// them and logs the roots with status st, all in one destination transaction (spec §6.3,
// §7.4). It commits only when every step passed; otherwise nothing of the batch exists in
// the destination or the log.
func (r *run) copy(ctx context.Context, src queryer, t *tree, st tracking.Status) error {
	conn, err := r.cfg.Destination.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	session, err := readDiagnosticSession(ctx, conn)
	if err != nil {
		return err
	}
	if r.cfg.DisableForeignKeyChecks {
		// SET is not transactional: reset it before the connection returns to the pool,
		// whether the transaction committed or not.
		r.log.Warn("SAFETY: FOREIGN_KEY_CHECKS is DISABLED for this copy. Destination inserts will not validate FK constraints.")
		if _, err := conn.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 0"); err != nil {
			return err
		}
		defer func() {
			if _, err := conn.ExecContext(context.Background(), "SET FOREIGN_KEY_CHECKS = 1"); err != nil {
				r.log.Errorf("Failed to reset FOREIGN_KEY_CHECKS on the destination connection: %v", err)
				_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			}
		}()
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	copied := map[string]int64{}
	accepted := map[string]map[uint16]uint64{}
	for _, n := range r.tree.CopyOrder() {
		for _, chunk := range key.Chunks(t.keys[n.Table], r.byKeyChunk(n.Table)) {
			rows, err := r.read(ctx, src, n.Table, chunk)
			if err != nil {
				return err
			}
			if err := r.insert(ctx, tx, session, n.Table, rows, accepted); err != nil {
				return err
			}
			copied[n.Table] += int64(len(rows))
		}
	}
	if err := r.verify(ctx, src, tx, t); err != nil {
		return err
	}
	if err := r.job.Insert(ctx, tx, st, t.keys[r.root.Table]); err != nil {
		return r.logInsertError(err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("engine: commit the destination transaction: %w", err)
	}
	for table, n := range copied {
		r.report.Copied[table] += n
	}
	for table, codes := range accepted {
		if r.report.Conversions[table] == nil {
			r.report.Conversions[table] = map[uint16]uint64{}
		}
		for code, n := range codes {
			r.report.Conversions[table][code] += n
		}
	}
	return nil
}

// logInsertError explains a log INSERT that met a key the log already holds: a copy-only
// root copied before, whose destination rows a retention job has pruned since.
func (r *run) logInsertError(err error) error {
	var me *mysql.MySQLError
	if r.typ == jobspec.CopyOnly && errors.As(err, &me) && me.Number == mysqlDuplicateEntry {
		return fmt.Errorf("%w. The job copied this root before; to skip the roots it copied, set resume: true on the job", err)
	}
	return err
}

// read is the copy read of one chunk (spec §7.1a): exactly one row per requested key, each
// returned key's bytes equal to a requested key. Temporal values are carried as text.
func (r *run) read(ctx context.Context, src queryer, table string, chunk []key.Key) ([][]any, error) {
	d, meta := r.keys[table], r.meta[table]
	q, err := readSQL(d, meta, len(chunk))
	if err != nil {
		return nil, &types.TemporalReadContractError{Table: table, Operation: "copy", Detail: "metadata unavailable", Cause: err}
	}
	st := statement{op: opRead, table: table, query: q, args: key.Args(chunk)}
	r.prepare(&st)
	requested := key.NewSet()
	for _, k := range chunk {
		requested.Add(k)
	}
	rows, err := src.QueryContext(ctx, st.query, st.args...)
	if err != nil {
		return nil, fmt.Errorf("engine: copy read on %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	seen := key.NewSet()
	var out [][]any
	var extra int64
	nk, nc := len(d.Columns), len(meta.Names)
	raw := make([]sql.RawBytes, nk)
	for rows.Next() {
		values := make([]any, nc)
		dest := make([]any, 0, nk+nc)
		for i := range raw {
			dest = append(dest, &raw[i])
		}
		for i := range values {
			dest = append(dest, &values[i])
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("engine: copy read on %s: %w", table, err)
		}
		b := make([][]byte, nk)
		for i := range raw {
			b[i] = raw[i]
		}
		k, err := d.Decode(b)
		if err != nil {
			return nil, fmt.Errorf("engine: copy read on %s: %w", table, err)
		}
		if !requested.Has(k) || !seen.Add(k) {
			extra++
			continue
		}
		for i, col := range meta.Names {
			if meta.Kind(col) != types.TemporalNone && values[i] != nil {
				text, err := types.TemporalText(values[i])
				if err != nil {
					return nil, &types.TemporalReadContractError{Table: table, Operation: "copy", Detail: "temporal payload representation unavailable", Cause: err}
				}
				values[i] = text
			}
		}
		out = append(out, values)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("engine: copy read on %s: %w", table, err)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := key.CheckRangeWarning(ctx, src); err != nil {
		return nil, fmt.Errorf("engine: copy read on %s: %w", table, err)
	}
	if want := len(requested.Keys()); extra > 0 || len(out) != want {
		return nil, &KeySetError{Table: table, Statement: string(opRead), Requested: int64(want), Matched: int64(len(out)), Extra: extra}
	}
	return out, nil
}

// insert writes rows with strict multi-row INSERTs inside tx, and reads each statement's
// conditions back on tx (INSERT diagnostics).
func (r *run) insert(ctx context.Context, tx *sql.Tx, session diagnosticSession, table string, rows [][]any, accepted map[string]map[uint16]uint64) error {
	if len(rows) == 0 {
		return nil
	}
	cols := r.meta[table].Names
	per := insertRows(len(rows), len(cols), session.MaxErrorCount)
	for off := 0; off < len(rows); off += per {
		part := rows[off:min(off+per, len(rows))]
		args := make([]any, 0, len(part)*len(cols))
		for _, row := range part {
			args = append(args, row...)
		}
		res, err := tx.ExecContext(ctx, insertSQL(table, cols, len(part)), args...)
		if err != nil {
			var me *mysql.MySQLError
			if errors.As(err, &me) && me.Number == mysqlDuplicateEntry {
				return &DuplicateError{Table: table, Key: duplicateKey(me.Message), Raw: me.Message, CopyOnly: r.typ == jobspec.CopyOnly}
			}
			return fmt.Errorf("engine: insert into %s: %w", table, err)
		}
		if n, err := res.RowsAffected(); err != nil || n != int64(len(part)) {
			return fmt.Errorf("engine: insert into %s wrote %d of %d rows: %v", table, n, len(part), err)
		}
		d, err := collectInsertDiagnostics(ctx, tx, table)
		if err != nil {
			return err
		}
		codes, err := classifyInsertDiagnostics(table, r.cfg.Verification, d)
		if err != nil {
			return err
		}
		if len(codes) > 0 {
			if accepted[table] == nil {
				accepted[table] = map[uint16]uint64{}
			}
			for code, n := range codes {
				accepted[table][code] += n
			}
			r.log.Warnw("Observed accepted conversion warnings (uncommitted)", "table", table, "diagnostics", codes)
		}
	}
	return nil
}

// duplicateKey extracts the quoted value from MySQL's "Duplicate entry 'x' for key 'y'".
func duplicateKey(msg string) string {
	first := strings.IndexByte(msg, '\'')
	if first < 0 {
		return msg
	}
	last := strings.IndexByte(msg[first+1:], '\'')
	if last < 0 {
		return msg
	}
	return msg[first+1 : first+1+last]
}
