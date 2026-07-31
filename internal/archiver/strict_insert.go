package archiver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
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
// The index fact comes from validations.TableSpec(WithIndexes), which reports full index
// definitions, so a COMPOSITE unique index is detected too (review P1-2). It runs in the
// orchestrator rather than preflight so it is enforced even when preflight is skipped
// with --skip-validate-preflight.
func destinationSecondaryUniqueIndexes(ctx context.Context, db *sql.DB, schema string, tables []string) ([]string, error) {
	if db == nil || schema == "" || len(tables) == 0 {
		return nil, nil
	}

	inspector := validations.NewInspector(db, schema)

	var found []string
	for _, table := range tables {
		spec, err := inspector.TableSpec(ctx, validations.Ref(schema, table), validations.WithIndexes())
		if err != nil {
			// An absent table, or an object that is not a base table, carries no unique
			// index for INSERT IGNORE to collide on. 1.8 read information_schema
			// .STATISTICS and got no rows for either shape; skipping reproduces that
			// outcome exactly.
			//
			// This is load-bearing, not defensive tidiness: the only caller that can
			// reach a missing destination table here is a run started with
			// --skip-validate-preflight, and that flag means "bypass preflight", not
			// "abort at startup for a new reason". Every other error still propagates.
			if errors.Is(err, validations.ErrTableNotFound) ||
				errors.Is(err, validations.ErrUnsupportedTableType) {
				continue
			}
			return nil, fmt.Errorf("failed to inspect destination unique indexes: %w", err)
		}
		for _, idx := range spec.Indexes {
			if idx.Unique && idx.Name != "PRIMARY" {
				found = append(found, fmt.Sprintf("%s.%s", table, idx.Name))
			}
		}
	}

	// 1.8 ordered by TABLE_NAME, INDEX_NAME in SQL. TableSpec orders indexes within one
	// table, but nothing orders across the per-table loop, so sort explicitly. Callers use
	// this list two ways: len(...) > 0 decides strict insert (order-independent) and
	// strings.Join(...) builds a log line (order-visible), so this affects diagnostics
	// only — an output improvement inside decision section 1.1, outside ledger scope.
	sort.Strings(found)
	return found, nil
}
