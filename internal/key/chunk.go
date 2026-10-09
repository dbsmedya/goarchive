package key

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// maxPlaceholders is the most parameters one prepared statement accepts (MySQL protocol limit).
const maxPlaceholders = 65535

// keyPartsPerMiB is the measured safe density of the range optimizer (probe P2,
// 2026-10-07): 2 columns × 10,000 tuples stayed a range at 8 MiB, 2 × 20,000 did not.
const keyPartsPerMiB = 2000

// MaxTuples is the largest number of key tuples one statement may carry (spec §4.3):
// tuples × keyColumns ≤ 2,000 × range_optimizer_max_mem_size in MiB, and never more than
// the 65,535 placeholders of a prepared statement. A budget of 0 (unlimited) leaves only the
// placeholder limit. The result is at least 1.
func MaxTuples(keyColumns int, rangeOptimizerMaxMemSize uint64) int {
	if keyColumns < 1 {
		keyColumns = 1
	}
	limit := maxPlaceholders / keyColumns
	if rangeOptimizerMaxMemSize > 0 {
		byMemory := int(uint64(keyPartsPerMiB)*rangeOptimizerMaxMemSize/(1<<20)) / keyColumns
		if byMemory < limit {
			limit = byMemory
		}
	}
	if limit < 1 {
		limit = 1
	}
	return limit
}

// Chunks splits keys into consecutive slices of at most n keys each.
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
		out = append(out, keys)
	}
	return out
}

// WarnRangeMemory is MySQL warning 3170: the range optimizer exceeded
// range_optimizer_max_mem_size and fell back to a full scan.
const WarnRangeMemory = 3170

// ErrRangeScan reports that MySQL abandoned range access for a key-addressed statement.
var ErrRangeScan = errors.New("range optimizer memory exceeded; the statement fell back to a full scan")

// Queryer is satisfied by *sql.Conn and *sql.Tx.
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// CheckRangeWarning runs SHOW WARNINGS on q, which must be the connection or transaction that
// ran the statement, after its result set was fully read. It returns an error wrapping
// ErrRangeScan when warning 3170 is present (spec §7.1a). Sessions run with sql_notes=1.
func CheckRangeWarning(ctx context.Context, q Queryer) error {
	rows, err := q.QueryContext(ctx, "SHOW WARNINGS")
	if err != nil {
		return fmt.Errorf("key: SHOW WARNINGS: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var level, msg string
		var code int
		if err := rows.Scan(&level, &code, &msg); err != nil {
			return fmt.Errorf("key: SHOW WARNINGS: %w", err)
		}
		if code == WarnRangeMemory {
			return fmt.Errorf("%w: %s", ErrRangeScan, msg)
		}
	}
	return rows.Err()
}
