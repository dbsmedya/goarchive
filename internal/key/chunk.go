package key

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// maxPlaceholders is the most parameters one prepared statement accepts (MySQL protocol limit).
const maxPlaceholders = 65535

// keyPartsPerMiB is the measured safe density of the range optimizer for narrow parts
// (probe P2, 2026-10-07): 2 columns × 10,000 tuples stayed a range at 8 MiB, 2 × 20,000 did
// not.
const keyPartsPerMiB = 2000

// The range optimizer's memory also grows with each key part's byte width. On 8.0.46,
// 8.4.11 and 9.7.1 one part of b bytes costs about 230 + 1.05·b bytes of
// range_optimizer_max_mem_size (decisions/2026-10-09-m1-width-aware-statement-cap.md).
// MaxTuples charges partOverhead + partWidthFactor·b, about twice the measured cost, so a
// utf8mb4 VARCHAR(255) key gets 3,297 tuples at 8 MiB where MySQL's limit is about 6,400.
// For parts of up to 12 bytes the keyPartsPerMiB density is the tighter bound.
const (
	partOverhead    = 500
	partWidthFactor = 2
)

// MaxTuples is the largest number of key tuples one statement over the index on cols may
// carry (spec §4.3). It is the smallest of three bounds:
//   - tuples × len(cols) ≤ 2,000 × range_optimizer_max_mem_size in MiB;
//   - tuples × Σ(partOverhead + partWidthFactor × part bytes) ≤ range_optimizer_max_mem_size;
//   - tuples × len(cols) ≤ 65,535, the placeholders of one prepared statement.
//
// A budget of 0 (unlimited) leaves only the placeholder bound. The result is at least 1.
// cols are the columns of the index being addressed, in index order.
func MaxTuples(cols []Column, rangeOptimizerMaxMemSize uint64) int {
	parts := uint64(max(len(cols), 1))
	limit := uint64(maxPlaceholders) / parts
	if budget := rangeOptimizerMaxMemSize; budget > 0 {
		const mib = 1 << 20
		// Split the product so 2,000 × budget cannot overflow for budgets up to 2^64-1.
		byParts := (budget/mib*keyPartsPerMiB + budget%mib*keyPartsPerMiB/mib) / parts
		var perTuple uint64
		for _, c := range cols {
			perTuple += partOverhead + partWidthFactor*uint64(partBytes(c))
		}
		limit = min(limit, byParts, budget/max(perTuple, 1))
	}
	return int(max(limit, 1))
}

// StatementBytes is an upper bound on the larger of the two packets a key-addressed
// statement of tuples tuples over cols (named names) sends (M7c F3): COM_STMT_PREPARE,
// its text, which is the IN list plus other bytes outside it, and COM_STMT_EXECUTE, its
// bound values, each charged partBytes plus 11 bytes of type, length and null-bitmap
// overhead. Both must fit the server's max_allowed_packet.
func StatementBytes(names []string, cols []Column, tuples, other int) uint64 {
	text := uint64(len(In(names, cols, tuples)) + other)
	var perTuple uint64
	for _, c := range cols {
		perTuple += uint64(partBytes(c)) + 11
	}
	return max(text, uint64(tuples)*perTuple+64)
}

// partBytes is an upper bound on the bytes one key part of c occupies in an index key image:
// 8 for integer and temporal parts, an upper bound on the packed size for DECIMAL, and the declared length in
// bytes plus a 2-byte length prefix for string and binary parts.
func partBytes(c Column) int {
	switch c.Family {
	case FamilyDecimal:
		return c.Precision/2 + 2
	case FamilyString:
		return c.Length*charsetMaxBytes(c.Charset) + 2
	case FamilyBinary:
		return c.Length + 2
	default:
		return 8
	}
}

// charsetMaxBytes is the most bytes one character of charset takes. Charsets not listed are
// charged 4, the widest any MySQL charset uses, so an unknown charset gets a smaller cap,
// never a larger one.
func charsetMaxBytes(charset string) int {
	switch strings.ToLower(charset) {
	case "latin1", "ascii", "binary":
		return 1
	case "utf8mb3", "utf8":
		return 3
	default:
		return 4
	}
}

// Chunks splits keys into consecutive slices of at most n keys each. No chunk has spare
// capacity, so appending to one never writes into the next chunk or into keys itself.
func Chunks(keys []Key, n int) [][]Key {
	if n < 1 {
		n = 1
	}
	var out [][]Key
	for len(keys) > n {
		out = append(out, keys[:n:n])
		keys = keys[n:]
	}
	if len(keys) > 0 {
		out = append(out, keys[:len(keys):len(keys)])
	}
	return out
}

// WarnRangeMemory is MySQL warning 3170: the range optimizer exceeded
// range_optimizer_max_mem_size and fell back to a full scan.
const WarnRangeMemory = 3170

// ErrRangeScan reports that MySQL abandoned range access for a key-addressed statement.
var ErrRangeScan = errors.New("range optimizer memory exceeded; the statement fell back to a full scan")

// ErrWarningsUnread reports that the session kept fewer warnings than the statement raised
// (max_error_count), so a 3170 among the rest cannot be ruled out (M7c F2).
var ErrWarningsUnread = errors.New("the session kept fewer warnings than the statement raised; raise max_error_count")

// Queryer is satisfied by *sql.Conn and *sql.Tx. *sql.DB satisfies it too, but
// CheckRangeWarning refuses one.
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// CheckRangeWarning runs SHOW WARNINGS on q, which must be the connection or transaction that
// ran the statement, after its result set was fully read. It returns an error wrapping
// ErrRangeScan when warning 3170 is present (spec §7.1a). Sessions run with sql_notes=1.
// The session keeps at most max_error_count warnings while SHOW COUNT(*) WARNINGS counts
// them all, so a statement that raised more than were kept, and none of the kept ones is
// 3170, is refused with ErrWarningsUnread. Neither SHOW statement clears the list.
//
// A *sql.DB is refused: the pool may run SHOW WARNINGS on another connection than the
// statement's, where the warning is never seen. For a DELETE, q is the chunk's transaction,
// which a 3170 rolls back; the locks the full scan took are held until then.
func CheckRangeWarning(ctx context.Context, q Queryer) error {
	if _, ok := q.(*sql.DB); ok {
		return errors.New("key: CheckRangeWarning needs the statement's own connection or transaction, not a connection pool")
	}
	raised, err := warningCount(ctx, q)
	if err != nil {
		return err
	}
	rows, err := q.QueryContext(ctx, "SHOW WARNINGS")
	if err != nil {
		return fmt.Errorf("key: SHOW WARNINGS: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var kept uint64
	for rows.Next() {
		var level, msg string
		var code int
		if err := rows.Scan(&level, &code, &msg); err != nil {
			return fmt.Errorf("key: SHOW WARNINGS: %w", err)
		}
		if code == WarnRangeMemory {
			return fmt.Errorf("%w: %s", ErrRangeScan, msg)
		}
		kept++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("key: SHOW WARNINGS: %w", err)
	}
	if kept < raised {
		return fmt.Errorf("%w (max_error_count kept %d of %d)", ErrWarningsUnread, kept, raised)
	}
	return nil
}

// warningCount is SHOW COUNT(*) WARNINGS on q: every condition the last statement raised,
// including those max_error_count did not keep.
func warningCount(ctx context.Context, q Queryer) (uint64, error) {
	rows, err := q.QueryContext(ctx, "SHOW COUNT(*) WARNINGS")
	if err != nil {
		return 0, fmt.Errorf("key: SHOW COUNT(*) WARNINGS: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var n uint64
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf("key: SHOW COUNT(*) WARNINGS: %w", err)
		}
		return 0, fmt.Errorf("key: SHOW COUNT(*) WARNINGS returned no row")
	}
	if err := rows.Scan(&n); err != nil {
		return 0, fmt.Errorf("key: SHOW COUNT(*) WARNINGS: %w", err)
	}
	return n, rows.Close()
}
