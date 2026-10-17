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

// MySQL refuses a long-data value larger than max_allowed_packet with ER_UNKNOWN_ERROR and a
// message naming the variable (probed on 8.0, 8.4 and 9.7, M7c-F1); a statement larger than
// the packet is ER_NET_PACKET_TOO_LARGE.
const (
	mysqlUnknownError   = 1105
	mysqlPacketTooLarge = 1153
)

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
		if _, err := conn.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 0"); err != nil {
			return err
		}
		defer func() {
			resetCtx, done := cleanupContext(ctx)
			defer done()
			if _, err := conn.ExecContext(resetCtx, "SET FOREIGN_KEY_CHECKS = 1"); err != nil {
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
	verified, err := r.verify(ctx, src, tx, t)
	if err != nil {
		return err
	}
	if err := r.job.Insert(ctx, tx, st, t.keys[r.root.Table]); err != nil {
		return r.logInsertError(err)
	}
	if err := r.commit(ctx, tx, atCopyWritten); err != nil {
		return fmt.Errorf("engine: commit the destination transaction: %w", err)
	}
	for table, n := range copied {
		r.report.Copied[table] += n
	}
	for table, n := range verified {
		r.report.Verified[table] += n
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

// logInsertError explains a log INSERT that met a key the log already holds (RKP-2): a root this
// job finished before. For archive or purge it is a root back in the where range; for copy-only,
// a root copied before whose destination rows are gone. Any other error passes through, a
// duplicate on another key or table included: only the log's primary key is a root.
func (r *run) logInsertError(err error) error {
	var me *mysql.MySQLError
	if errors.As(err, &me) && me.Number == mysqlDuplicateEntry {
		if value, key, ok := duplicateEntry(me.Message); ok && key == r.job.PrimaryKeyName() {
			return &RecurringRootError{Job: r.job.Name, Type: r.typ, Root: value, Err: me}
		}
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
			if pe := packetError(table, cols, part, err); pe != nil {
				return pe
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

// duplicateKey extracts the value from MySQL's "Duplicate entry 'x' for key 'y'", or returns msg
// when it has another form.
func duplicateKey(msg string) string {
	if value, _, ok := duplicateEntry(msg); ok {
		return value
	}
	return msg
}

// duplicateEntry splits MySQL's "Duplicate entry 'x' for key 'y'" into x and y. MySQL does not
// escape x, which may hold quotes (O'Reilly), so x ends at the last "' for key '"; a key name
// holds none.
func duplicateEntry(msg string) (value, key string, ok bool) {
	const head, mid = "Duplicate entry '", "' for key '"
	i := strings.LastIndex(msg, mid)
	if !strings.HasPrefix(msg, head) || i < len(head) || !strings.HasSuffix(msg, "'") || len(msg) < i+len(mid)+1 {
		return "", "", false
	}
	return msg[len(head):i], msg[i+len(mid) : len(msg)-1], true
}

// packetError names an INSERT of table that did not fit the destination's max_allowed_packet
// (M7c-F1): one value larger than it, refused as long data and named by the largest value of
// rows; or the statement as a whole, refused by the driver or the server. It is nil for any
// other error.
func packetError(table string, cols []string, rows [][]any, err error) *PacketError {
	var me *mysql.MySQLError
	isMySQL := errors.As(err, &me)
	switch {
	case isMySQL && me.Number == mysqlUnknownError && strings.Contains(me.Message, "max_allowed_packet"):
		e := &PacketError{Table: table}
		for _, row := range rows {
			for i, v := range row {
				n := 0
				switch v := v.(type) {
				case []byte:
					n = len(v)
				case string:
					n = len(v)
				}
				if n > e.Bytes {
					e.Column, e.Bytes = cols[i], n
				}
			}
		}
		return e
	case errors.Is(err, mysql.ErrPktTooLarge), isMySQL && me.Number == mysqlPacketTooLarge:
		return &PacketError{Table: table}
	}
	return nil
}
