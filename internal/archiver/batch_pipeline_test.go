package archiver

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/dbsmedya/goarchive/internal/verifier"
	"github.com/stretchr/testify/require"
)

// newTestPipeline builds a batchPipeline over three sqlmock DBs, mirroring the
// component wiring of orchestrator_test.go:495. skipVerify=true keeps the
// verifier quiet so tests only choreograph copy/delete/bookkeeping SQL.
func newTestPipeline(t *testing.T, mainMode batchMode) (*batchPipeline, sqlmock.Sqlmock, sqlmock.Sqlmock, sqlmock.Sqlmock) {
	t.Helper()
	sourceDB, sourceMock, _ := sqlmock.New()
	t.Cleanup(func() { _ = sourceDB.Close() })
	destDB, destMock, _ := sqlmock.New()
	t.Cleanup(func() { _ = destDB.Close() })
	archDB, archMock, _ := sqlmock.New()
	t.Cleanup(func() { _ = archDB.Close() })

	g := createSimpleGraph() // root "customers", PK "id", leaf (no children)
	g.SetRootPKMeta("bigint", false)
	log := logger.NewDefault()

	discovery, _ := NewRecordDiscovery(g, sourceDB, 1000)
	copyPhase, _ := NewCopyPhase(sourceDB, destDB, g, config.SafetyConfig{}, log)
	dataVerifier, _ := verifier.NewVerifier(sourceDB, destDB, g, verifier.MethodSHA256, log)
	deletePhase, _ := NewDeletePhase(sourceDB, g, 1000, log)
	fetcher := NewRootIDFetcher(sourceDB, "customers", "id", "", 1000, nil)
	resumeMgr, _ := NewResumeManager(archDB, log, "testdb")
	resumeMgr.setJobID(7)

	p := &batchPipeline{
		jobName:       "job1",
		mainMode:      mainMode,
		graph:         g,
		logger:        log,
		processingCfg: config.ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 1000},
		skipVerify:    true,
		discovery:     discovery,
		copyPhase:     copyPhase,
		dataVerifier:  dataVerifier,
		deletePhase:   deletePhase,
		resumeMgr:     resumeMgr,
		fetcher:       fetcher,
	}
	return p, sourceMock, destMock, archMock
}

// TestProcessBatchCopyVerifySkipsDelete: the copy-only mode runs
// Discover -> Copy -> (verify skipped) -> MarkBatchCopied -> CompleteBatch and
// must never touch the delete phase or the lag monitor.
func TestProcessBatchCopyVerifySkipsDelete(t *testing.T) {
	p, sourceMock, destMock, archMock := newTestPipeline(t, batchCopyVerify)
	stub := &stubLagWaiter{}
	p.lagMonitor = stub // present but must NOT be consulted in batchCopyVerify

	sourceMock.ExpectQuery("SELECT \\* FROM `customers` WHERE `id` IN \\(\\?\\)").
		WithArgs(int64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "a"))
	destMock.ExpectBegin()
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	destMock.ExpectExec("INSERT IGNORE INTO `customers`").WillReturnResult(sqlmock.NewResult(0, 1))
	destMock.ExpectCommit()
	archMock.ExpectExec("UPDATE .*archiver_job_log_\\d+. SET log_status").
		WithArgs(LogStatusCopied, "1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	archMock.ExpectBegin()
	archMock.ExpectExec("UPDATE .*archiver_job_log_\\d+. SET log_status").
		WithArgs(LogStatusCompleted, "1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	archMock.ExpectExec("UPDATE .*archiver_job. SET last_processed_root_pk_id").
		WithArgs("1", "job1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	archMock.ExpectCommit()

	stats, err := p.processBatch(context.Background(), []interface{}{int64(1)},
		batchCopyVerify, int64(1), nil)
	require.NoError(t, err)
	require.Equal(t, 1, stats.RootsProcessed)
	require.Equal(t, 0, stub.calls) // no lag check without a delete phase

	// No DELETE was expected on sourceMock; ExpectationsWereMet + sqlmock's
	// unexpected-call errors together prove delete never fired.
	require.NoError(t, sourceMock.ExpectationsWereMet())
	require.NoError(t, destMock.ExpectationsWereMet())
	require.NoError(t, archMock.ExpectationsWereMet())
}

// TestProcessBatchErrorLeavesNoStatusWrites: on a phase error the engine
// returns the error and performs NO tracking-table writes — no
// MarkBatchCopied, no CompleteBatch, and (the #18 contract) never a
// log_status=3 write.
//
// NOTE: createSimpleGraph is a leaf graph, and leaf discovery issues NO SQL
// (Discover seeds result.Records[root] = rootPKs directly, discovery.go:74-81)
// — so the error is injected into the COPY phase's source chunk SELECT
// (copy.go:321), which runs after the destination transaction has begun.
// Expected destination choreography: Begin -> SET FK=1 -> Rollback. There is
// NO second FK reset: the connection-cleanup reset (copy.go:167-175) only
// fires when SafetyConfig.DisableForeignKeyChecks is true, and newTestPipeline
// uses config.SafetyConfig{}.
func TestProcessBatchErrorLeavesNoStatusWrites(t *testing.T) {
	p, sourceMock, destMock, archMock := newTestPipeline(t, batchCopyVerify)

	destMock.ExpectBegin()
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	sourceMock.ExpectQuery("SELECT \\* FROM `customers` WHERE `id` IN").
		WillReturnError(errors.New("source gone"))
	destMock.ExpectRollback()

	_, err := p.processBatch(context.Background(), []interface{}{int64(1), int64(2), int64(3)},
		batchCopyVerify, int64(3), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "copy failed")

	// archMock had zero expectations: any tracking write (including failed=3)
	// would surface as an unexpected-call error here.
	require.NoError(t, sourceMock.ExpectationsWereMet())
	require.NoError(t, destMock.ExpectationsWereMet())
	require.NoError(t, archMock.ExpectationsWereMet())
}
