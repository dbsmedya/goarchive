package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// shouldUseStrictInsert decides whether the copy phase must use a plain INSERT
// (which fails loudly on a duplicate key) instead of INSERT IGNORE (which
// silently skips a conflicting row).
//
// Strict insert is REQUIRED whenever a silently-skipped row could later be
// deleted from the source without a faithful destination copy:
//
//   - count verification: a pre-existing destination PK would let the count
//     match while the content differs (the long-standing behavior).
//   - verification skipped: there is no post-copy safety net at all, so a
//     silent skip would go undetected before the source delete. This closes the
//     dangerous "--skip-verify + non-count method" asymmetry (review P0-1).
//   - destination carries a secondary UNIQUE index: INSERT IGNORE would skip a
//     row that collides on that unique key (not the PK) and again go undetected
//     before the delete (review P1-2).
//
// In every forced case a duplicate now aborts the copy (and therefore the
// delete) instead of silently dropping a row.
func shouldUseStrictInsert(method string, skipVerification, destHasUniqueIndex bool) bool {
	return method == "count" || skipVerification || destHasUniqueIndex
}

// destinationSecondaryUniqueIndexes returns "table.index" descriptors for every
// participating destination table that carries a non-PRIMARY UNIQUE index.
// Their presence forces strict insert (see shouldUseStrictInsert) because
// INSERT IGNORE can silently drop a row that collides on such an index — a
// silent partial copy that would precede a source delete.
//
// This deliberately uses information_schema.STATISTICS (full index definitions)
// rather than the per-column COLUMN_KEY, so a COMPOSITE unique index is detected
// too (review P1-2). It runs in the orchestrator rather than preflight so it is
// enforced even when preflight is skipped with --skip-validate-preflight.
func destinationSecondaryUniqueIndexes(ctx context.Context, db *sql.DB, schema string, tables []string) ([]string, error) {
	if db == nil || schema == "" || len(tables) == 0 {
		return nil, nil
	}

	placeholders := make([]string, len(tables))
	args := make([]interface{}, 0, len(tables)+1)
	args = append(args, schema)
	for i, t := range tables {
		placeholders[i] = "?"
		args = append(args, t)
	}

	query := fmt.Sprintf(`
		SELECT DISTINCT TABLE_NAME, INDEX_NAME
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ?
		  AND TABLE_NAME IN (%s)
		  AND NON_UNIQUE = 0
		  AND INDEX_NAME <> 'PRIMARY'
		ORDER BY TABLE_NAME, INDEX_NAME`, strings.Join(placeholders, ", "))

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect destination unique indexes: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var found []string
	for rows.Next() {
		var table, index string
		if err := rows.Scan(&table, &index); err != nil {
			return nil, fmt.Errorf("failed to scan destination unique index row: %w", err)
		}
		found = append(found, fmt.Sprintf("%s.%s", table, index))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating destination unique indexes: %w", err)
	}
	return found, nil
}
