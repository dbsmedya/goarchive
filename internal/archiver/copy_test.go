package archiver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	mysql "github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCopyPhase_Validation(t *testing.T) {
	sourceDB, _, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, _, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()

	g := createSimpleGraph()
	log := logger.NewDefault()
	safetyCfg := config.SafetyConfig{}

	tests := []struct {
		name      string
		sourceDB  *sql.DB
		destDB    *sql.DB
		graph     *graph.Graph
		expectErr bool
		errMsg    string
	}{
		{
			name:      "Valid inputs",
			sourceDB:  sourceDB,
			destDB:    destDB,
			graph:     g,
			expectErr: false,
		},
		{
			name:      "Nil source DB",
			sourceDB:  nil,
			destDB:    destDB,
			graph:     g,
			expectErr: true,
			errMsg:    "source database is nil",
		},
		{
			name:      "Nil destination DB",
			sourceDB:  sourceDB,
			destDB:    nil,
			graph:     g,
			expectErr: true,
			errMsg:    "destination database is nil",
		},
		{
			name:      "Nil graph",
			sourceDB:  sourceDB,
			destDB:    destDB,
			graph:     nil,
			expectErr: true,
			errMsg:    "graph is nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cp, err := NewCopyPhase(tt.sourceDB, tt.destDB, tt.graph, safetyCfg, log)
			if tt.expectErr {
				assert.Error(t, err)
				assert.Nil(t, cp)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, cp)
			}
		})
	}
}

func TestExtractDuplicatePK(t *testing.T) {
	tests := []struct {
		msg  string
		want string
	}{
		{"Duplicate entry '123' for key 'PRIMARY'", "123"},
		{"Duplicate entry 'abc-def' for key 'PRIMARY'", "abc-def"},
		{"unrelated error", "unrelated error"},
		{"no quotes 'only one", "no quotes 'only one"},
	}
	for _, tt := range tests {
		if got := extractDuplicatePK(tt.msg); got != tt.want {
			t.Fatalf("extractDuplicatePK(%q): want %q, got %q", tt.msg, tt.want, got)
		}
	}
}

func TestNewCopyPhase_NilLoggerDefaults(t *testing.T) {
	sourceDB, _, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, _, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()

	g := createSimpleGraph()
	cp, err := NewCopyPhase(sourceDB, destDB, g, config.SafetyConfig{}, nil)
	require.NoError(t, err)
	require.NotNil(t, cp)
	require.NotNil(t, cp.logger)
}

func TestCopyPhase_TransactionBeginError(t *testing.T) {
	sourceDB, _, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()

	g := createSimpleGraph()
	log := logger.NewDefault()
	cp, _ := NewCopyPhase(sourceDB, destDB, g, config.SafetyConfig{}, log)

	recordSet := &RecordSet{
		RootPKs: []interface{}{int64(1)},
		Records: map[string][]interface{}{
			"customers": {int64(1)},
		},
	}

	// Mock transaction begin failure
	destMock.ExpectBegin().WillReturnError(sql.ErrConnDone)

	ctx := context.Background()
	stats, err := cp.Copy(ctx, recordSet)

	assert.Error(t, err)
	assert.Nil(t, stats)
	assert.Contains(t, err.Error(), "failed to begin destination transaction")
	assert.NoError(t, destMock.ExpectationsWereMet())
}

func TestCopyPhase_SetForeignKeyChecksError(t *testing.T) {
	sourceDB, _, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()

	g := createSimpleGraph()
	log := logger.NewDefault()
	// DisableForeignKeyChecks: true → executes SET FOREIGN_KEY_CHECKS = 0
	cp, _ := NewCopyPhase(sourceDB, destDB, g, config.SafetyConfig{DisableForeignKeyChecks: true}, log)

	recordSet := &RecordSet{
		RootPKs: []interface{}{int64(1)},
		Records: map[string][]interface{}{
			"customers": {int64(1)},
		},
	}

	// Mock successful transaction begin but FK check disable failure
	destMock.ExpectBegin()
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 0").WillReturnError(sql.ErrTxDone)
	destMock.ExpectRollback()

	ctx := context.Background()
	stats, err := cp.Copy(ctx, recordSet)

	assert.Error(t, err)
	assert.Nil(t, stats)
	assert.Contains(t, err.Error(), "failed to configure FK checks")
	assert.NoError(t, destMock.ExpectationsWereMet())
}

func TestCopyPhase_CopySuccess_SingleTable(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()

	g := createSimpleGraph()
	log := logger.NewDefault()
	cp, _ := NewCopyPhase(sourceDB, destDB, g, config.SafetyConfig{DisableForeignKeyChecks: true}, log)

	recordSet := &RecordSet{
		RootPKs: []interface{}{int64(1)},
		Records: map[string][]interface{}{
			"customers": {int64(1)},
		},
	}

	// Mock destination transaction
	destMock.ExpectBegin()
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 0").WillReturnResult(sqlmock.NewResult(0, 0))

	// Mock source query for customers table
	sourceMock.ExpectQuery("SELECT \\* FROM `customers` WHERE `id` IN \\(\\?\\)").
		WithArgs(int64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "email"}).
			AddRow(1, "Alice", "alice@example.com"))

	// Mock destination insert
	destMock.ExpectExec("INSERT IGNORE INTO `customers`").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// FK checks are re-enabled before commit when DisableForeignKeyChecks=true
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	destMock.ExpectCommit()

	ctx := context.Background()
	stats, err := cp.Copy(ctx, recordSet)

	require.NoError(t, err)
	require.NotNil(t, stats)
	assert.Equal(t, 1, stats.TablesCopied)
	assert.Equal(t, int64(1), stats.RowsCopied)
	assert.Equal(t, 0, stats.TablesSkipped)
	assert.Equal(t, int64(1), stats.RowsPerTable["customers"])
	assert.NoError(t, sourceMock.ExpectationsWereMet())
	assert.NoError(t, destMock.ExpectationsWereMet())
}

func TestCopyPhase_CopySuccess_MultipleTablesWithOrder(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()

	g := createMultiLevelGraph()
	log := logger.NewDefault()
	cp, _ := NewCopyPhase(sourceDB, destDB, g, config.SafetyConfig{DisableForeignKeyChecks: true}, log)

	recordSet := &RecordSet{
		RootPKs: []interface{}{int64(1)},
		Records: map[string][]interface{}{
			"customers": {int64(1)},
			"orders":    {int64(101)},
		},
	}

	// Mock destination transaction
	destMock.ExpectBegin()
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 0").WillReturnResult(sqlmock.NewResult(0, 0))

	// Copy order: customers -> orders (parent-first via Kahn's algorithm)
	// Mock customers copy
	sourceMock.ExpectQuery("SELECT \\* FROM `customers` WHERE `id` IN \\(\\?\\)").
		WithArgs(int64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).
			AddRow(1, "Alice"))
	destMock.ExpectExec("INSERT IGNORE INTO `customers`").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Mock orders copy
	sourceMock.ExpectQuery("SELECT \\* FROM `orders` WHERE `id` IN \\(\\?\\)").
		WithArgs(int64(101)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "customer_id", "total"}).
			AddRow(101, 1, 100.50))
	destMock.ExpectExec("INSERT IGNORE INTO `orders`").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// FK checks are re-enabled before commit when DisableForeignKeyChecks=true
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	destMock.ExpectCommit()

	ctx := context.Background()
	stats, err := cp.Copy(ctx, recordSet)

	require.NoError(t, err)
	require.NotNil(t, stats)
	assert.Equal(t, 2, stats.TablesCopied)
	assert.Equal(t, int64(2), stats.RowsCopied)
	assert.NoError(t, sourceMock.ExpectationsWereMet())
	assert.NoError(t, destMock.ExpectationsWereMet())
}

func TestCopyPhase_EmptyTable_Skipped(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()

	g := createSimpleGraph()
	log := logger.NewDefault()
	cp, _ := NewCopyPhase(sourceDB, destDB, g, config.SafetyConfig{DisableForeignKeyChecks: true}, log)

	recordSet := &RecordSet{
		RootPKs: []interface{}{int64(1)},
		Records: map[string][]interface{}{
			"customers": {}, // Empty - no records to copy
		},
	}

	// Mock destination transaction
	destMock.ExpectBegin()
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 0").WillReturnResult(sqlmock.NewResult(0, 0))
	// No source query or insert expected for empty table
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	destMock.ExpectCommit()

	ctx := context.Background()
	stats, err := cp.Copy(ctx, recordSet)

	require.NoError(t, err)
	require.NotNil(t, stats)
	assert.Equal(t, 0, stats.TablesCopied)
	assert.Equal(t, int64(0), stats.RowsCopied)
	assert.Equal(t, 1, stats.TablesSkipped)
	assert.NoError(t, sourceMock.ExpectationsWereMet())
	assert.NoError(t, destMock.ExpectationsWereMet())
}

func TestCopyPhase_SourceQueryError_Rollback(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()

	g := createSimpleGraph()
	log := logger.NewDefault()
	cp, _ := NewCopyPhase(sourceDB, destDB, g, config.SafetyConfig{DisableForeignKeyChecks: true}, log)

	recordSet := &RecordSet{
		RootPKs: []interface{}{int64(1)},
		Records: map[string][]interface{}{
			"customers": {int64(1)},
		},
	}

	// Mock destination transaction
	destMock.ExpectBegin()
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 0").WillReturnResult(sqlmock.NewResult(0, 0))

	// Mock source query failure
	sourceMock.ExpectQuery("SELECT \\* FROM `customers` WHERE `id` IN \\(\\?\\)").
		WithArgs(int64(1)).
		WillReturnError(sql.ErrConnDone)
	destMock.ExpectRollback()

	ctx := context.Background()
	stats, err := cp.Copy(ctx, recordSet)

	assert.Error(t, err)
	assert.Nil(t, stats)
	assert.Contains(t, err.Error(), "failed to copy table")
	assert.NoError(t, sourceMock.ExpectationsWereMet())
	assert.NoError(t, destMock.ExpectationsWereMet())
}

func TestCopyPhase_InsertError_Rollback(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()

	g := createSimpleGraph()
	log := logger.NewDefault()
	cp, _ := NewCopyPhase(sourceDB, destDB, g, config.SafetyConfig{DisableForeignKeyChecks: true}, log)

	recordSet := &RecordSet{
		RootPKs: []interface{}{int64(1)},
		Records: map[string][]interface{}{
			"customers": {int64(1)},
		},
	}

	// Mock destination transaction
	destMock.ExpectBegin()
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 0").WillReturnResult(sqlmock.NewResult(0, 0))

	// Mock source query success
	sourceMock.ExpectQuery("SELECT \\* FROM `customers` WHERE `id` IN \\(\\?\\)").
		WithArgs(int64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).
			AddRow(1, "Alice"))

	// Mock exec failure on destination insert
	destMock.ExpectExec("INSERT IGNORE INTO `customers`").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(sql.ErrTxDone)
	destMock.ExpectRollback()

	ctx := context.Background()
	stats, err := cp.Copy(ctx, recordSet)

	assert.Error(t, err)
	assert.Nil(t, stats)
	assert.NoError(t, sourceMock.ExpectationsWereMet())
	assert.NoError(t, destMock.ExpectationsWereMet())
}

func TestCopyPhase_ContextCancellation(t *testing.T) {
	sourceDB, _, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()

	g := createSimpleGraph()
	log := logger.NewDefault()
	cp, _ := NewCopyPhase(sourceDB, destDB, g, config.SafetyConfig{}, log)

	recordSet := &RecordSet{
		RootPKs: []interface{}{int64(1)},
		Records: map[string][]interface{}{
			"customers": {int64(1)},
		},
	}

	// Cancel context before copy
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Mock transaction begin (may or may not happen depending on timing)
	destMock.ExpectBegin().WillReturnError(context.Canceled)

	stats, err := cp.Copy(ctx, recordSet)

	assert.Error(t, err)
	assert.Nil(t, stats)
}

func TestCopyPhase_CommitError(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()

	g := createSimpleGraph()
	log := logger.NewDefault()
	cp, _ := NewCopyPhase(sourceDB, destDB, g, config.SafetyConfig{DisableForeignKeyChecks: true}, log)

	recordSet := &RecordSet{
		RootPKs: []interface{}{int64(1)},
		Records: map[string][]interface{}{
			"customers": {int64(1)},
		},
	}

	// Mock successful copy but commit failure
	destMock.ExpectBegin()
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 0").WillReturnResult(sqlmock.NewResult(0, 0))
	sourceMock.ExpectQuery("SELECT \\* FROM `customers` WHERE `id` IN \\(\\?\\)").
		WithArgs(int64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).
			AddRow(1, "Alice"))
	destMock.ExpectExec("INSERT IGNORE INTO `customers`").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	destMock.ExpectCommit().WillReturnError(sql.ErrTxDone)
	// After failed commit, sqlmock tx is in done state.
	// The defer will attempt Rollback() which returns ErrTxDone (logged and swallowed).

	ctx := context.Background()
	stats, err := cp.Copy(ctx, recordSet)

	assert.Error(t, err)
	assert.Nil(t, stats)
	assert.Contains(t, err.Error(), "failed to commit destination transaction")
	assert.NoError(t, sourceMock.ExpectationsWereMet())
	assert.NoError(t, destMock.ExpectationsWereMet())
}

// TestCopyPhase_FKChecks_ResetAfterRollback verifies that when the copy fails
// after SET FOREIGN_KEY_CHECKS=0 but before commit, the dedicated connection
// receives an explicit SET FOREIGN_KEY_CHECKS=1 before being returned to the
// pool. SET is not transactional in MySQL, so rollback alone does not restore
// the session default.
func TestCopyPhase_FKChecks_ResetAfterRollback(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()

	g := createSimpleGraph()
	log := logger.NewDefault()
	cp, _ := NewCopyPhase(sourceDB, destDB, g, config.SafetyConfig{DisableForeignKeyChecks: true}, log)

	recordSet := &RecordSet{
		RootPKs: []interface{}{int64(1)},
		Records: map[string][]interface{}{
			"customers": {int64(1)},
		},
	}

	destMock.ExpectBegin()
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 0").WillReturnResult(sqlmock.NewResult(0, 0))

	// Force a source-side error to trigger rollback before the pre-commit
	// reset path runs. The defer must then restore FK_CHECKS=1 on the conn.
	sourceMock.ExpectQuery("SELECT \\* FROM `customers`").
		WithArgs(int64(1)).
		WillReturnError(sql.ErrConnDone)
	destMock.ExpectRollback()

	// Defer-driven reset via conn.ExecContext. Outside the tx.
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 1").WillReturnResult(sqlmock.NewResult(0, 0))

	ctx := context.Background()
	stats, err := cp.Copy(ctx, recordSet)

	assert.Error(t, err)
	assert.Nil(t, stats)
	assert.NoError(t, sourceMock.ExpectationsWereMet())
	assert.NoError(t, destMock.ExpectationsWereMet(),
		"deferred SET FOREIGN_KEY_CHECKS = 1 must run on rollback to avoid leaking into pool")
}

// TestCopyPhase_FKChecks_NoResetWhenNotDisabled verifies that when the feature
// is off, no SET FOREIGN_KEY_CHECKS statements are issued at all.
func TestCopyPhase_FKChecks_NoResetWhenNotDisabled(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()

	g := createSimpleGraph()
	log := logger.NewDefault()
	cp, _ := NewCopyPhase(sourceDB, destDB, g, config.SafetyConfig{DisableForeignKeyChecks: false}, log)

	recordSet := &RecordSet{
		RootPKs: []interface{}{int64(1)},
		Records: map[string][]interface{}{
			"customers": {int64(1)},
		},
	}

	destMock.ExpectBegin()
	// Implementation still issues SET FOREIGN_KEY_CHECKS = 1 (the safe default)
	// via setForeignKeyChecks even when not disabling; only this one SET is expected.
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 1").WillReturnResult(sqlmock.NewResult(0, 0))

	sourceMock.ExpectQuery("SELECT \\* FROM `customers`").
		WithArgs(int64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "Alice"))
	destMock.ExpectExec("INSERT IGNORE INTO `customers`").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	destMock.ExpectCommit()

	ctx := context.Background()
	stats, err := cp.Copy(ctx, recordSet)

	require.NoError(t, err)
	require.NotNil(t, stats)
	assert.NoError(t, sourceMock.ExpectationsWereMet())
	assert.NoError(t, destMock.ExpectationsWereMet())
}

func TestCopyTableChunksByBatchSize(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()

	g := createSimpleGraph() // table "customers", PK "id"
	log := logger.NewDefault()
	// SafetyConfig{} => FK checks NOT disabled => Copy issues "SET FOREIGN_KEY_CHECKS = 1".
	cp, _ := NewCopyPhase(sourceDB, destDB, g, config.SafetyConfig{}, log)
	cp.SetBatchSize(2) // force 2 PKs per chunk

	recordSet := &RecordSet{
		RootPKs: []interface{}{int64(1), int64(2), int64(3)},
		Records: map[string][]interface{}{
			"customers": {int64(1), int64(2), int64(3)}, // 3 PKs => 2 chunks: [1,2],[3]
		},
	}

	destMock.ExpectBegin()
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 1").WillReturnResult(sqlmock.NewResult(0, 0))

	// Chunk 1: SELECT ids (1,2) -> 2 rows, then one INSERT.
	sourceMock.ExpectQuery("SELECT \\* FROM `customers` WHERE `id` IN \\(\\?, \\?\\)").
		WithArgs(int64(1), int64(2)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).
			AddRow(1, "a").AddRow(2, "b"))
	destMock.ExpectExec("INSERT IGNORE INTO `customers`").
		WillReturnResult(sqlmock.NewResult(0, 2))

	// Chunk 2: SELECT id (3) -> 1 row, then one INSERT.
	sourceMock.ExpectQuery("SELECT \\* FROM `customers` WHERE `id` IN \\(\\?\\)").
		WithArgs(int64(3)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).
			AddRow(3, "c"))
	destMock.ExpectExec("INSERT IGNORE INTO `customers`").
		WillReturnResult(sqlmock.NewResult(0, 1))

	destMock.ExpectCommit()

	stats, err := cp.Copy(context.Background(), recordSet)
	require.NoError(t, err)
	assert.Equal(t, int64(3), stats.RowsCopied)
	assert.NoError(t, sourceMock.ExpectationsWereMet())
	assert.NoError(t, destMock.ExpectationsWereMet())
}

func TestBuildInsertIgnoreQuery_QuotesColumnNames(t *testing.T) {
	cp := &CopyPhase{}
	query := cp.buildInsertIgnoreQuery("orders", []string{"id", "order", "group"})

	expected := "INSERT IGNORE INTO `orders` (`id`, `order`, `group`) VALUES (?, ?, ?)"
	assert.Equal(t, expected, query)
}

// TestMaxRowsPerInsert covers the pure placeholder-clamp arithmetic:
// maxRowsPerInsert(columnCount) must always keep columnCount*rows <= 65535
// while returning at least 1 row.
func TestMaxRowsPerInsert(t *testing.T) {
	tests := []struct {
		columnCount int
		want        int
	}{
		{columnCount: 3, want: 21845}, // 65535 / 3
		{columnCount: 2, want: 32767}, // 65535 / 2
		{columnCount: 1, want: 65535}, // 65535 / 1
		{columnCount: 65535, want: 1}, // exactly one column's worth of rows
		{columnCount: 70000, want: 1}, // wider than the limit itself
		{columnCount: 0, want: 1},     // degenerate: never divide by zero
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("columnCount=%d", tt.columnCount), func(t *testing.T) {
			got := maxRowsPerInsert(tt.columnCount)
			assert.Equal(t, tt.want, got)
		})
	}
}

// anyArgValues returns n sqlmock.AnyArg() driver values, used to assert an
// exact per-exec argument count without pinning individual values.
func anyArgValues(n int) []driver.Value {
	args := make([]driver.Value, n)
	for i := range args {
		args[i] = sqlmock.AnyArg()
	}
	return args
}

// wideRowsSource builds a sqlmock.Rows with numCols synthetic columns and
// numRows synthetic rows, used by the placeholder-clamp split tests below.
func wideRowsSource(numCols, numRows int) *sqlmock.Rows {
	columns := make([]string, numCols)
	for i := range columns {
		columns[i] = fmt.Sprintf("col%d", i)
	}
	rows := sqlmock.NewRows(columns)
	for r := 0; r < numRows; r++ {
		rowVals := make([]driver.Value, numCols)
		for c := 0; c < numCols; c++ {
			rowVals[c] = int64(r*numCols + c)
		}
		rows.AddRow(rowVals...)
	}
	return rows
}

// TestCopyChunk_SplitsOversizedBatchIntoSubInserts proves that a chunk whose
// combined placeholder count (columns * rows) exceeds MySQL's 65,535 limit is
// split into multiple INSERTs instead of aborting. 1000 columns x 66 rows =
// 66,000 placeholders > 65,535; maxRowsPerInsert(1000) = 65, so the split
// must be a 65-row exec followed by a 1-row exec.
func TestCopyChunk_SplitsOversizedBatchIntoSubInserts(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()

	g := createSimpleGraph() // table "customers", PK "id"
	log := logger.NewDefault()
	cp, _ := NewCopyPhase(sourceDB, destDB, g, config.SafetyConfig{}, log)

	const numCols = 1000
	const numRows = 66

	pks := make([]interface{}, numRows)
	for i := range pks {
		pks[i] = int64(i + 1)
	}
	recordSet := &RecordSet{
		RootPKs: []interface{}{int64(1)},
		Records: map[string][]interface{}{
			"customers": pks,
		},
	}

	destMock.ExpectBegin()
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 1").WillReturnResult(sqlmock.NewResult(0, 0))

	sourceMock.ExpectQuery("SELECT \\* FROM `customers`").
		WillReturnRows(wideRowsSource(numCols, numRows))

	// First sub-batch: 65 rows x 1000 columns = 65000 args.
	destMock.ExpectExec("INSERT IGNORE INTO `customers`").
		WithArgs(anyArgValues(65 * numCols)...).
		WillReturnResult(sqlmock.NewResult(0, 65))

	// Second sub-batch: remaining 1 row x 1000 columns = 1000 args.
	destMock.ExpectExec("INSERT IGNORE INTO `customers`").
		WithArgs(anyArgValues(1 * numCols)...).
		WillReturnResult(sqlmock.NewResult(0, 1))

	destMock.ExpectCommit()

	stats, err := cp.Copy(context.Background(), recordSet)

	require.NoError(t, err)
	require.NotNil(t, stats)
	assert.Equal(t, int64(numRows), stats.RowsCopied)
	assert.NoError(t, sourceMock.ExpectationsWereMet())
	assert.NoError(t, destMock.ExpectationsWereMet())
}

// TestCopyChunk_StrictDuplicateOnLaterSubBatch proves that duplicate-error
// mapping to *ErrDestinationDuplicate applies per sub-batch exec, not just
// the first one: the second (later) INSERT in the split is the one that
// fails here.
func TestCopyChunk_StrictDuplicateOnLaterSubBatch(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()

	g := createSimpleGraph() // table "customers", PK "id"
	log := logger.NewDefault()
	cp, _ := NewCopyPhase(sourceDB, destDB, g, config.SafetyConfig{}, log)
	cp.SetStrictInsert(true)

	const numCols = 1000
	const numRows = 66

	pks := make([]interface{}, numRows)
	for i := range pks {
		pks[i] = int64(i + 1)
	}
	recordSet := &RecordSet{
		RootPKs: []interface{}{int64(1)},
		Records: map[string][]interface{}{
			"customers": pks,
		},
	}

	destMock.ExpectBegin()
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 1").WillReturnResult(sqlmock.NewResult(0, 0))

	sourceMock.ExpectQuery("SELECT \\* FROM `customers`").
		WillReturnRows(wideRowsSource(numCols, numRows))

	// First sub-batch succeeds.
	destMock.ExpectExec("INSERT INTO `customers`").
		WithArgs(anyArgValues(65 * numCols)...).
		WillReturnResult(sqlmock.NewResult(0, 65))

	// Second sub-batch hits a MySQL duplicate-key error.
	destMock.ExpectExec("INSERT INTO `customers`").
		WithArgs(anyArgValues(1 * numCols)...).
		WillReturnError(&mysql.MySQLError{Number: 1062, Message: "Duplicate entry '42' for key 'PRIMARY'"})

	destMock.ExpectRollback()

	stats, err := cp.Copy(context.Background(), recordSet)

	require.Error(t, err)
	assert.Nil(t, stats)

	var dupErr *ErrDestinationDuplicate
	require.ErrorAs(t, err, &dupErr)
	assert.Equal(t, "customers", dupErr.Table)
	assert.Equal(t, "42", dupErr.ConflictingPK)

	assert.NoError(t, sourceMock.ExpectationsWereMet())
	assert.NoError(t, destMock.ExpectationsWereMet())
}

// Helper functions

func createSimpleGraph() *graph.Graph {
	jobCfg := &config.JobConfig{
		RootTable:  "customers",
		PrimaryKey: "id",
		Relations:  []config.Relation{},
	}
	builder := graph.NewBuilder(jobCfg)
	g, _ := builder.Build()
	return g
}

func createMultiLevelGraph() *graph.Graph {
	jobCfg := &config.JobConfig{
		RootTable:  "customers",
		PrimaryKey: "id",
		Relations: []config.Relation{
			{
				Table:          "orders",
				PrimaryKey:     "id",
				ForeignKey:     "customer_id",
				DependencyType: "1-N",
			},
		},
	}
	builder := graph.NewBuilder(jobCfg)
	g, _ := builder.Build()
	return g
}
