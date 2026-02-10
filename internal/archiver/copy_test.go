package archiver

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
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

	// Mock destination prepare and insert
	destMock.ExpectPrepare("INSERT IGNORE INTO `customers`")
	destMock.ExpectExec("INSERT IGNORE INTO `customers`").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Mock commit (no second FK check - implementation only sets once)
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
	destMock.ExpectPrepare("INSERT IGNORE INTO `customers`")
	destMock.ExpectExec("INSERT IGNORE INTO `customers`").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Mock orders copy
	sourceMock.ExpectQuery("SELECT \\* FROM `orders` WHERE `id` IN \\(\\?\\)").
		WithArgs(int64(101)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "customer_id", "total"}).
			AddRow(101, 1, 100.50))
	destMock.ExpectPrepare("INSERT IGNORE INTO `orders`")
	destMock.ExpectExec("INSERT IGNORE INTO `orders`").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Mock commit
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

	// Mock prepare success but exec failure on destination insert
	destMock.ExpectPrepare("INSERT IGNORE INTO `customers`")
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
	destMock.ExpectPrepare("INSERT IGNORE INTO `customers`")
	destMock.ExpectExec("INSERT IGNORE INTO `customers`").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
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
