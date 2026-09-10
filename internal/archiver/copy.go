// Package archiver provides the copy phase implementation for GoArchive.
package archiver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/dbsgomysql/pkg/sqlutil"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/dbsmedya/goarchive/internal/types"
)

// CopyStats contains statistics about the copy operation.
// GA-P3-F3-T8: Copy stats logging
type CopyStats struct {
	TablesCopied  int           // Number of tables processed
	RowsCopied    int64         // Total rows copied across all tables
	Duration      time.Duration // Time taken for copy operation
	TablesSkipped int           // Tables with no rows to copy
	RowsPerTable  map[string]int64
}

// CopyPhase manages the transactional copy of discovered records from source to destination.
//
// GA-P3-F3-T1: Destination transaction
// GA-P3-F3-T2: FK checks configuration
// GA-P3-F3-T3: Copy root table
// GA-P3-F3-T4: Copy child tables
// GA-P3-F3-T5: Use INSERT IGNORE for idempotency
// GA-P3-F3-T6: Commit transaction
// GA-P3-F3-T7: Rollback on error
// GA-P3-F3-T8: Copy stats logging
type CopyPhase struct {
	sourceDB             *sql.DB
	destDB               *sql.DB
	graph                *graph.Graph
	safetyCfg            config.SafetyConfig
	logger               *logger.Logger
	strictInsert         bool
	diagnosticPolicy     config.VerificationConfig
	hasSecondaryUnique   bool
	policyConfigured     bool
	committedConversions map[string]map[uint16]uint64
	batchSize            int // fetch+insert chunk size; 0 => defaultCopyBatchSize

	// columnLists maps each graph table to its full source column list
	// (ordinal order, invisible and generated columns included), fetched once
	// per run via sourceColumnMetadata. copyChunk fails closed without it.
	columnMetadata map[string]types.ColumnMetadata
}

const defaultCopyBatchSize = 200
const mysqlErrDuplicateEntry = 1062
const maxPlaceholders = 65535

// ErrDestinationDuplicate is returned when strict INSERT sees a destination duplicate.
type ErrDestinationDuplicate struct {
	Table         string
	ConflictingPK string
	RawMySQLError string
}

func (e *ErrDestinationDuplicate) Error() string {
	return fmt.Sprintf(
		"Archive aborted: destination already contains a row in table %q with primary key %q (MySQL: %s).\n\n"+
			"This is unsafe under verification.method: count, which only checks row counts and cannot prove pre-existing destination rows match the source. Additional conflicting rows may exist beyond the one MySQL reported above.\n\n"+
			"To resolve:\n"+
			"  1. Switch this job to verification.method: sha256 (recommended), OR\n"+
			"  2. Manually remove the conflicting destination rows and re-run.\n\n"+
			"Source data has NOT been deleted.",
		e.Table, e.ConflictingPK, e.RawMySQLError)
}

// NewCopyPhase creates a new copy phase coordinator.
func NewCopyPhase(
	sourceDB *sql.DB,
	destDB *sql.DB,
	g *graph.Graph,
	safetyCfg config.SafetyConfig,
	log *logger.Logger,
) (*CopyPhase, error) {
	if sourceDB == nil {
		return nil, fmt.Errorf("source database is nil")
	}
	if destDB == nil {
		return nil, fmt.Errorf("destination database is nil")
	}
	if g == nil {
		return nil, fmt.Errorf("graph is nil")
	}
	if log == nil {
		log = logger.NewDefault()
	}

	return &CopyPhase{
		sourceDB:  sourceDB,
		destDB:    destDB,
		graph:     g,
		safetyCfg: safetyCfg,
		logger:    log,
	}, nil
}

// SetStrictInsert switches copy to plain INSERT. Used for destructive count verification.
func (cp *CopyPhase) SetStrictInsert(strict bool) {
	cp.strictInsert = strict
}

// StrictInsert reports whether the copy phase uses plain (strict) INSERT rather
// than INSERT IGNORE. Strict mode aborts on any duplicate, which means a pending
// batch whose destination copy already committed cannot be safely re-copied on
// resume — callers use this to gate crash-recovery replay.
func (cp *CopyPhase) StrictInsert() bool {
	return cp.strictInsert
}

// SetBatchSize sets the fetch+insert chunk size for the copy phase. Values <= 0
// are ignored. When never set, defaultCopyBatchSize is used.
func (cp *CopyPhase) SetBatchSize(n int) {
	if n > 0 {
		cp.batchSize = n
	}
}

// SetColumnMetadata installs the per-table explicit column lists used for the
// copy SELECT and INSERT. Rows are never fetched with SELECT *, which MySQL
// omits INVISIBLE columns from (issue #23).
func (cp *CopyPhase) SetColumnMetadata(metadata map[string]types.ColumnMetadata) {
	cp.columnMetadata = make(map[string]types.ColumnMetadata, len(metadata))
	for table, m := range metadata {
		n := types.ColumnMetadata{Names: append([]string(nil), m.Names...)}
		if m.Temporal != nil {
			n.Temporal = make(map[string]types.TemporalKind, len(m.Temporal))
			for c, k := range m.Temporal {
				n.Temporal[c] = k
			}
		}
		cp.columnMetadata[table] = n
	}
}

// effectiveBatchSize returns the configured chunk size or the default.
func (cp *CopyPhase) effectiveBatchSize() int {
	if cp.batchSize > 0 {
		return cp.batchSize
	}
	return defaultCopyBatchSize
}

// Copy executes the copy phase for the given discovered record set.
// It copies all tables in dependency order within a single destination transaction.
//
// When DisableForeignKeyChecks is true, the copy runs on a dedicated *sql.Conn
// so the SET FOREIGN_KEY_CHECKS=0 session variable cannot leak back into the
// connection pool. The variable is explicitly reset before the connection is
// returned to the pool, regardless of whether the transaction committed or
// rolled back. SET is not transactional in MySQL, so explicit reset is required.
//
// GA-P3-F3-T1: Uses destination transaction for atomicity
// GA-P3-F3-T6: Commits on success
// GA-P3-F3-T7: Rolls back on error
// GA-P3-F3-T8: Returns copy statistics
func (cp *CopyPhase) Copy(ctx context.Context, recordSet *RecordSet) (*CopyStats, error) {
	startTime := time.Now()
	if !cp.policyConfigured {
		return nil, &insertDiagnosticError{Identifier: "INSERT_DIAGNOSTICS_UNPROVEN", Reason: "diagnostic policy not configured"}
	}
	if _, err := classifyInsertDiagnostics(insertDiagnosticContext{UsesIgnore: !cp.StrictInsert(), SkipVerification: cp.diagnosticPolicy.SkipVerification, VerificationMethod: cp.diagnosticPolicy.Method, HasSecondaryUnique: cp.hasSecondaryUnique}, insertDiagnostics{}); err != nil {
		return nil, err
	}
	for table, keys := range recordSet.Records {
		if len(keys) > 0 {
			if _, err := cp.columnMetadata[table].ProjectColumn(cp.graph.GetPK(table)); err != nil {
				return nil, &types.TemporalReadContractError{Table: table, Operation: "copy", Detail: "metadata unavailable", Cause: err}
			}
		}
	}
	stats := &CopyStats{
		RowsPerTable: make(map[string]int64),
	}

	// Loud warning when FK checks are disabled. This is an advanced option that
	// can mask referential-integrity bugs in the copy order; operators should
	// see it in every run's log output.
	if cp.safetyCfg.DisableForeignKeyChecks {
		cp.logger.Warn("SAFETY: FOREIGN_KEY_CHECKS is DISABLED for this copy phase. " +
			"Destination inserts will not validate FK constraints. " +
			"Use only when you have verified the copy order and accept the risk.")
	}

	// Checkout a dedicated connection so any session state (FOREIGN_KEY_CHECKS)
	// is contained to this conn and cannot leak back to the pool.
	conn, err := cp.destDB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get destination connection: %w", err)
	}
	session, err := readDiagnosticSession(ctx, conn)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	op := &copyOperation{Session: session, Accepted: map[string]map[uint16]uint64{}}
	fkReset := false
	defer func() {
		// Reset FK checks before returning the connection to the pool, even if
		// the transaction rolled back — SET is not transactional in MySQL.
		if !fkReset && cp.safetyCfg.DisableForeignKeyChecks {
			if _, resetErr := conn.ExecContext(context.Background(),
				"SET FOREIGN_KEY_CHECKS = 1"); resetErr != nil {
				cp.logger.Errorf("Failed to reset FOREIGN_KEY_CHECKS on destination connection: %v", resetErr)
			}
		}
		if closeErr := conn.Close(); closeErr != nil {
			cp.logger.Warnf("Failed to close destination connection: %v", closeErr)
		}
	}()

	// GA-P3-F3-T1: Begin destination transaction on the dedicated connection
	cp.logger.Debug("Starting destination transaction")
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin destination transaction: %w", err)
	}

	// GA-P3-F3-T7: Ensure rollback on error
	defer func() {
		if tx != nil {
			// Transaction not yet committed - rollback
			cp.logger.Warn("Rolling back destination transaction due to error or panic")
			if rbErr := tx.Rollback(); rbErr != nil {
				cp.logger.Errorf("Failed to rollback transaction: %v", rbErr)
			}
		}
	}()

	// GA-P3-F3-T2: Configure foreign key checks on the dedicated connection.
	// Using tx.ExecContext vs conn.ExecContext both land on the same connection,
	// but SET is not rolled back by tx, so location does not matter for safety.
	if err := cp.setForeignKeyChecks(ctx, tx, cp.safetyCfg.DisableForeignKeyChecks); err != nil {
		return nil, fmt.Errorf("failed to configure FK checks: %w", err)
	}

	// Get copy order from dependency graph (parent tables first)
	copyOrder, err := cp.graph.CopyOrder()
	if err != nil {
		return nil, fmt.Errorf("failed to get copy order: %w", err)
	}

	cp.logger.Infof("Starting copy phase for %d tables in dependency order", len(copyOrder))

	// Copy tables in order: root table first, then children
	for _, table := range copyOrder {
		// Check context cancellation
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("copy interrupted: %w", err)
		}

		pks, exists := recordSet.Records[table]
		if !exists || len(pks) == 0 {
			// Table has no records to copy (not discovered or empty)
			cp.logger.Debugf("Skipping table %q (no records to copy)", table)
			stats.TablesSkipped++
			continue
		}

		// GA-P3-F3-T3 and GA-P3-F3-T4: Copy table (root or child)
		rowsCopied, err := cp.copyTable(ctx, tx, table, pks, op)
		if err != nil {
			return nil, fmt.Errorf("failed to copy table %s: %w", table, err)
		}

		stats.TablesCopied++
		stats.RowsCopied += rowsCopied
		stats.RowsPerTable[table] = rowsCopied

		cp.logger.Debugf("Copied %d rows from table %q", rowsCopied, table)
	}

	// Re-enable FK checks before commit so the reset is part of the same
	// session's linear statement stream and cannot be interleaved with any
	// later use of the connection. Belt-and-suspenders with the defer.
	if cp.safetyCfg.DisableForeignKeyChecks {
		if _, err := tx.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 1"); err != nil {
			return nil, fmt.Errorf("failed to reset FOREIGN_KEY_CHECKS before commit: %w", err)
		}
		fkReset = true
	}

	// GA-P3-F3-T6: Commit transaction on success
	cp.logger.Debug("Committing destination transaction")
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit destination transaction: %w", err)
	}

	if cp.committedConversions == nil {
		cp.committedConversions = map[string]map[uint16]uint64{}
	}
	for table, counts := range op.Accepted {
		if cp.committedConversions[table] == nil {
			cp.committedConversions[table] = map[uint16]uint64{}
		}
		for code, count := range counts {
			cp.committedConversions[table][code] += count
		}
	}
	// Mark transaction as committed (prevent defer rollback)
	tx = nil

	// GA-P3-F3-T8: Populate final statistics
	stats.Duration = time.Since(startTime)

	cp.logger.Infof("Copy phase complete: %d tables, %d rows, duration: %s",
		stats.TablesCopied,
		stats.RowsCopied,
		stats.Duration,
	)

	return stats, nil
}

// copyTable copies all specified records for one table, in batchSize-sized
// chunks. Each chunk is one SELECT (fetch) followed by one INSERT, all inside
// the caller's single destination transaction tx.
//
// GA-P3-F3-T5: Uses INSERT IGNORE for idempotent inserts (unless strictInsert)
func (cp *CopyPhase) copyTable(ctx context.Context, tx *sql.Tx, table string, pks []interface{}, op *copyOperation) (int64, error) {
	if len(pks) == 0 {
		return 0, nil
	}

	chunk := cp.effectiveBatchSize()
	var rowsCopied int64

	for start := 0; start < len(pks); start += chunk {
		if err := ctx.Err(); err != nil {
			return rowsCopied, fmt.Errorf("copy interrupted: %w", err)
		}
		end := start + chunk
		if end > len(pks) {
			end = len(pks)
		}
		copied, err := cp.copyChunk(ctx, tx, table, pks[start:end], op)
		if err != nil {
			return rowsCopied, err
		}
		rowsCopied += copied
	}
	return rowsCopied, nil
}

// copyChunk fetches one chunk of rows from source and inserts them into dest
// within tx. Rows are inserted via one or more INSERTs — split into
// sub-batches of at most maxRowsPerInsert(len(columns)) rows so no single
// statement exceeds MySQL's 65,535-placeholder limit.
func (cp *CopyPhase) copyChunk(ctx context.Context, tx *sql.Tx, table string, pks []interface{}, op *copyOperation) (int64, error) {
	pkColumn := cp.graph.GetPK(table)

	meta := cp.columnMetadata[table]
	columns := meta.Names
	if _, err := meta.ProjectColumn(pkColumn); err != nil {
		return 0, &types.TemporalReadContractError{Table: table, Operation: "copy", Detail: "primary-key metadata unavailable", Cause: err}
	}
	selectQuery, err := buildSelectColumnsQuery(table, pkColumn, meta, len(pks))
	if err != nil {
		return 0, err
	}
	var identitySet *types.TemporalIdentitySet
	if kind := meta.Kind(pkColumn); kind != types.TemporalNone {
		identitySet, err = types.NewTemporalIdentitySet(kind, table, "copy", pks)
		if err != nil {
			return 0, err
		}
		pks = identitySet.Keys()
		selectQuery, err = buildSelectColumnsQuery(table, pkColumn, meta, len(pks))
		if err != nil {
			return 0, err
		}
	}
	rows, err := cp.sourceDB.QueryContext(ctx, selectQuery, pks...)
	if err != nil {
		return 0, &types.TemporalReadContractError{Table: table, Operation: "copy", Detail: "source query failed", Cause: err}
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			cp.logger.Warnf("Failed to close rows: %v", cerr)
		}
	}()

	batchValues := make([]interface{}, 0, len(columns)*len(pks))
	rowsInBatch := 0
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return 0, &types.TemporalReadContractError{Table: table, Operation: "copy", Detail: "row scan failed", Cause: err}
		}
		for i, column := range columns {
			if meta.Kind(column) != types.TemporalNone && values[i] != nil {
				text, err := types.TemporalText(values[i])
				if err != nil {
					return 0, &types.TemporalReadContractError{Table: table, Operation: "copy", Detail: "temporal payload representation unavailable", Cause: err}
				}
				values[i] = text
			}
			if identitySet != nil && strings.EqualFold(column, pkColumn) {
				if err := identitySet.Observe(values[i]); err != nil {
					return 0, err
				}
			}
		}
		batchValues = append(batchValues, values...)
		rowsInBatch++
	}
	if err := rows.Err(); err != nil {
		return 0, &types.TemporalReadContractError{Table: table, Operation: "copy", Detail: "row iteration failed", Cause: err}
	}
	if identitySet != nil {
		if err := identitySet.Finish(); err != nil {
			return 0, err
		}
	}
	if rowsInBatch == 0 {
		return 0, nil
	}

	// MySQL hard-limits a single prepared statement to 65,535 placeholders.
	// A chunk whose columns*rowsInBatch would exceed that is split into
	// multiple INSERTs, each within the clamp, so wide tables never abort a
	// run that could otherwise succeed with smaller INSERTs.
	maxRows := effectiveInsertRows(cp.effectiveBatchSize(), len(columns), op.Session.MaxErrorCount)
	var rowsCopied int64
	for off := 0; off < rowsInBatch; off += maxRows {
		n := rowsInBatch - off
		if n > maxRows {
			n = maxRows
		}
		vals := batchValues[off*len(columns) : (off+n)*len(columns)]
		affected, err := cp.execInsertBatch(ctx, tx, table, columns, n, vals, op)
		if err != nil {
			return rowsCopied, err
		}
		rowsCopied += affected
	}
	return rowsCopied, nil
}

// maxRowsPerInsert returns the largest number of rows whose combined
// placeholders (rows × columnCount) stay within MySQL's 65,535-placeholder
// limit for a single prepared statement. Always returns at least 1 so a
// single row can still be inserted even for an (impossibly) wide table.
func maxRowsPerInsert(columnCount int) int {
	if columnCount <= 0 {
		return 1
	}
	n := maxPlaceholders / columnCount
	if n < 1 {
		return 1
	}
	return n
}

// buildSelectColumnsQuery builds the explicit-column chunk fetch:
// SELECT `c1`, `c2` FROM `t` WHERE `pk` IN (?, ?, ...). Explicit naming is
// what carries INVISIBLE columns, which SELECT * silently omits.
func buildSelectColumnsQuery(table, pkColumn string, meta types.ColumnMetadata, pkCount int) (string, error) {
	projection, err := meta.Projection()
	if err != nil {
		return "", err
	}
	placeholders := make([]string, pkCount)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s IN (%s)",
		projection,
		sqlutil.QuoteIdentifier(table),
		sqlutil.QuoteIdentifier(pkColumn),
		strings.Join(placeholders, ", "),
	), nil
}

// execInsertBatch inserts rowCount rows (values already flattened in
// row-major order, len == rowCount*len(columns)) into table within tx, using
// INSERT IGNORE or strict INSERT per cp.strictInsert, and maps a strict-mode
// duplicate to *ErrDestinationDuplicate. Returns RowsAffected.
func (cp *CopyPhase) execInsertBatch(ctx context.Context, tx *sql.Tx, table string, columns []string, rowCount int, values []interface{}, op *copyOperation) (int64, error) {
	if !cp.policyConfigured || op == nil || !op.Session.SQLNotes {
		return 0, &insertDiagnosticError{Identifier: "INSERT_DIAGNOSTICS_UNPROVEN", Table: table, Reason: "diagnostic policy/session not configured"}
	}
	diagnosticContext := insertDiagnosticContext{Table: table, UsesIgnore: !cp.StrictInsert(), SkipVerification: cp.diagnosticPolicy.SkipVerification, VerificationMethod: cp.diagnosticPolicy.Method, HasSecondaryUnique: cp.hasSecondaryUnique}
	if _, err := classifyInsertDiagnostics(diagnosticContext, insertDiagnostics{}); err != nil {
		return 0, err
	}
	insertQuery := cp.buildInsertIgnoreBatchQuery(table, columns, rowCount)
	if cp.strictInsert {
		insertQuery = cp.buildInsertBatchQuery(table, columns, rowCount)
	}
	result, err := tx.ExecContext(ctx, insertQuery, values...)
	if err != nil {
		if cp.strictInsert {
			var mysqlErr *mysql.MySQLError
			if errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDuplicateEntry {
				return 0, &ErrDestinationDuplicate{
					Table:         table,
					ConflictingPK: extractDuplicatePK(mysqlErr.Message),
					RawMySQLError: mysqlErr.Message,
				}
			}
		}
		return 0, fmt.Errorf("failed to insert batch into %s: %w", table, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("INSERT rows affected: %w", err)
	}
	diagnostics, err := collectInsertDiagnostics(ctx, tx, table)
	if err != nil {
		return 0, err
	}
	accepted, err := classifyInsertDiagnostics(diagnosticContext, diagnostics)
	if err != nil {
		return 0, err
	}
	if len(accepted) > 0 {
		if op.Accepted == nil {
			op.Accepted = map[string]map[uint16]uint64{}
		}
		if op.Accepted[table] == nil {
			op.Accepted[table] = map[uint16]uint64{}
		}
		for code, count := range accepted {
			op.Accepted[table][code] += count
		}
		cp.logger.Warnw("Observed accepted conversion warnings (uncommitted)", "table", table, "diagnostics", accepted, "scope", "uncommitted-copy")
	}
	return affected, nil
}

func (cp *CopyPhase) buildInsertIgnoreBatchQuery(table string, columns []string, rowCount int) string {
	// Column list: (`col1`, `col2`, `col3`)
	columnList := quotedColumnList(columns)

	// Placeholders: (?, ?, ?)
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	placeholderList := fmt.Sprintf("(%s)", strings.Join(placeholders, ", "))
	valueTuples := make([]string, rowCount)
	for i := 0; i < rowCount; i++ {
		valueTuples[i] = placeholderList
	}

	// GA-P3-F3-T5: INSERT IGNORE ensures idempotency
	return fmt.Sprintf(
		"INSERT IGNORE INTO %s (%s) VALUES %s",
		sqlutil.QuoteIdentifier(table),
		columnList,
		strings.Join(valueTuples, ", "),
	)
}

func (cp *CopyPhase) buildInsertBatchQuery(table string, columns []string, rowCount int) string {
	query := cp.buildInsertIgnoreBatchQuery(table, columns, rowCount)
	return strings.Replace(query, "INSERT IGNORE INTO", "INSERT INTO", 1)
}

func extractDuplicatePK(mysqlMsg string) string {
	first := strings.IndexByte(mysqlMsg, '\'')
	if first == -1 {
		return mysqlMsg
	}
	last := strings.IndexByte(mysqlMsg[first+1:], '\'')
	if last == -1 {
		return mysqlMsg
	}
	return mysqlMsg[first+1 : first+1+last]
}

// setForeignKeyChecks configures FOREIGN_KEY_CHECKS for the transaction.
//
// GA-P3-F3-T2: FK checks configuration
// Setting to 0 disables FK checks during inserts, allowing out-of-order inserts
// Setting to 1 (default) enforces FK constraints
func (cp *CopyPhase) setForeignKeyChecks(ctx context.Context, tx *sql.Tx, disable bool) error {
	value := 1
	if disable {
		value = 0
		cp.logger.Debug("Disabling FOREIGN_KEY_CHECKS for destination transaction")
	} else {
		cp.logger.Debug("FOREIGN_KEY_CHECKS enabled (default)")
	}

	query := fmt.Sprintf("SET FOREIGN_KEY_CHECKS = %d", value)
	if _, err := tx.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to set FOREIGN_KEY_CHECKS: %w", err)
	}

	return nil
}

type copyOperation struct {
	Session  diagnosticSession
	Accepted map[string]map[uint16]uint64
}

func (cp *CopyPhase) SetDiagnosticPolicy(v config.VerificationConfig, hasSecondaryUnique bool) {
	v.Method = v.EffectiveMethod()
	cp.diagnosticPolicy = v
	cp.hasSecondaryUnique = hasSecondaryUnique
	cp.policyConfigured = true
}
func (cp *CopyPhase) ConversionTotals() map[string]map[uint16]uint64 {
	out := map[string]map[uint16]uint64{}
	for table, counts := range cp.committedConversions {
		out[table] = map[uint16]uint64{}
		for code, n := range counts {
			out[table][code] = n
		}
	}
	return out
}
