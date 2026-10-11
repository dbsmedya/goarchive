package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"sort"
	"strconv"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
	"github.com/dbsmedya/goarchive/v3/internal/types"
)

// verify checks a batch's copy inside the destination transaction, before the log write
// (spec §7.4). count compares each server's row count with the number of requested keys,
// not only with each other (spec §7.1a). sha256 also hashes every row, read in key order on
// both servers, and compares the hashes; it detects a changed value. none skips both. It
// returns the rows it verified, by table.
func (r *run) verify(ctx context.Context, src, dst queryer, t *tree) (map[string]int64, error) {
	verified := map[string]int64{}
	if r.cfg.Verification == jobspec.VerifyNone {
		return verified, nil
	}
	for _, n := range r.tree.CopyOrder() {
		keys := t.keys[n.Table]
		if len(keys) == 0 {
			continue
		}
		var err error
		if r.cfg.Verification == jobspec.VerifyCount {
			err = r.verifyCount(ctx, src, dst, n.Table, keys)
		} else {
			err = r.verifyHash(ctx, src, dst, n.Table, keys)
		}
		if err != nil {
			return nil, err
		}
		verified[n.Table] += int64(len(keys))
	}
	return verified, nil
}

func (r *run) verifyCount(ctx context.Context, src, dst queryer, table string, keys []key.Key) error {
	d := r.keys[table]
	for _, chunk := range key.Chunks(keys, r.byKeyChunk(table)) {
		var n [2]int64
		for i, q := range []queryer{src, dst} {
			st := statement{op: opCount, destination: i == 1, table: table, query: countSQL(d, len(chunk)), args: key.Args(chunk)}
			r.prepare(&st)
			if err := q.QueryRowContext(ctx, st.query, st.args...).Scan(&n[i]); err != nil {
				return fmt.Errorf("engine: count verification on %s: %w", table, err)
			}
			if err := key.CheckRangeWarning(ctx, q); err != nil {
				return fmt.Errorf("engine: count verification on %s: %w", table, err)
			}
		}
		if want := int64(len(chunk)); n[0] != want || n[1] != want {
			return &VerificationError{Table: table, Method: "count", Requested: want, Source: n[0], Destination: n[1]}
		}
	}
	return nil
}

func (r *run) verifyHash(ctx context.Context, src, dst queryer, table string, keys []key.Key) error {
	d, meta := r.keys[table], r.meta[table]
	var sums [2]string
	var rows [2]int64
	for i, q := range []queryer{src, dst} {
		h := sha256.New()
		for _, chunk := range key.Chunks(keys, r.byKeyChunk(table)) {
			n, err := r.hashChunk(ctx, q, i == 1, d, meta, chunk, h)
			if err != nil {
				return err
			}
			rows[i] += n
		}
		sums[i] = hex.EncodeToString(h.Sum(nil))
	}
	want := int64(len(keys))
	switch {
	case rows[0] != want || rows[1] != want:
		return &VerificationError{Table: table, Method: "sha256", Requested: want, Source: rows[0], Destination: rows[1]}
	case sums[0] != sums[1]:
		return &VerificationError{Table: table, Method: "sha256", Requested: want, Source: rows[0], Destination: rows[1],
			Detail: fmt.Sprintf(", hashes differ (source %s…, destination %s…)", sums[0][:16], sums[1][:16])}
	}
	return nil
}

// hashChunk writes one chunk's rows to h and returns how many it read.
func (r *run) hashChunk(ctx context.Context, q queryer, destination bool, d key.Def, meta types.ColumnMetadata, chunk []key.Key, h hash.Hash) (int64, error) {
	query, err := hashSQL(d, meta, len(chunk))
	if err != nil {
		return 0, &types.TemporalReadContractError{Table: d.Table, Operation: "verify", Detail: "metadata unavailable", Cause: err}
	}
	st := statement{op: opHash, destination: destination, table: d.Table, query: query, args: key.Args(chunk)}
	r.prepare(&st)
	rows, err := q.QueryContext(ctx, st.query, st.args...)
	if err != nil {
		return 0, fmt.Errorf("engine: sha256 verification on %s: %w", d.Table, err)
	}
	defer func() { _ = rows.Close() }()
	ser := newRowSerializer(meta.Names)
	values := make([]any, len(meta.Names))
	ptrs := make([]any, len(values))
	for i := range values {
		ptrs[i] = &values[i]
	}
	var n int64
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return 0, fmt.Errorf("engine: sha256 verification on %s: %w", d.Table, err)
		}
		for i, col := range meta.Names {
			if meta.Kind(col) != types.TemporalNone && values[i] != nil {
				text, err := types.TemporalText(values[i])
				if err != nil {
					return 0, &types.TemporalReadContractError{Table: d.Table, Operation: "verify", Detail: "temporal payload representation unavailable", Cause: err}
				}
				values[i] = text
			}
		}
		h.Write(ser.appendRow(values))
		n++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("engine: sha256 verification on %s: %w", d.Table, err)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if err := key.CheckRangeWarning(ctx, q); err != nil {
		return 0, fmt.Errorf("engine: sha256 verification on %s: %w", d.Table, err)
	}
	return n, nil
}

// rowSerializer is the verifier's canonical row form, kept from 2.x (spec §9): name=value
// pairs sorted by column name, joined by NUL, one newline per row. Both servers' rows go
// through it, so only a difference in a value changes the hash.
type rowSerializer struct {
	order []int    // column indices in name order
	names []string // column names in select order
	buf   []byte   // reused; valid until the next appendRow
}

func newRowSerializer(columns []string) *rowSerializer {
	order := make([]int, len(columns))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool { return columns[order[a]] < columns[order[b]] })
	return &rowSerializer{order: order, names: columns}
}

func (s *rowSerializer) appendRow(values []any) []byte {
	s.buf = s.buf[:0]
	for n, idx := range s.order {
		if n > 0 {
			s.buf = append(s.buf, 0)
		}
		s.buf = append(s.buf, s.names[idx]...)
		s.buf = append(s.buf, '=')
		s.buf = appendValue(s.buf, values[idx])
	}
	return append(s.buf, '\n')
}

// appendValue formats a driver value: NULL, bytes as hex, numbers in decimal, text as is.
func appendValue(buf []byte, val any) []byte {
	switch v := val.(type) {
	case nil:
		return append(buf, "NULL"...)
	case []byte:
		buf = append(buf, '0', 'x')
		return hex.AppendEncode(buf, v)
	case int64:
		return strconv.AppendInt(buf, v, 10)
	case uint64:
		return strconv.AppendUint(buf, v, 10)
	case float64:
		return strconv.AppendFloat(buf, v, 'g', 17, 64)
	case float32:
		return strconv.AppendFloat(buf, float64(v), 'g', 9, 32)
	case bool:
		return strconv.AppendBool(buf, v)
	case string:
		return append(buf, v...)
	default:
		return fmt.Appendf(buf, "%v", v)
	}
}
