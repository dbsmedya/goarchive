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

// TestRecoverRefusesLegacyFailedRows: rows marked failed (log_status=3) by a
// pre-1.8 release block resume with recovery guidance, for every mode.
func TestRecoverRefusesLegacyFailedRows(t *testing.T) {
	for _, mode := range []batchMode{batchFull, batchCopyVerify, batchDeleteOnly} {
		p, sourceMock, destMock, archMock := newTestPipeline(t, mode)

		archMock.ExpectQuery("SELECT root_pk_id FROM .*archiver_job_log_\\d+. WHERE log_status = \\?").
			WithArgs(LogStatusFailed).
			WillReturnRows(sqlmock.NewRows([]string{"root_pk_id"}).AddRow("7"))

		_, err := p.recover(context.Background(), nil)
		require.Error(t, err, "mode %v", mode)
		require.Contains(t, err.Error(), "log_status=3")
		require.Contains(t, err.Error(), "SET log_status=0") // requeue guidance
		require.Contains(t, err.Error(), "SET log_status=2") // acknowledge guidance
		require.NoError(t, sourceMock.ExpectationsWereMet())
		require.NoError(t, destMock.ExpectationsWereMet())
		require.NoError(t, archMock.ExpectationsWereMet())
	}
}

// TestRecoverCopyOnlyPromotesCopied: copy-only 'copied' rows complete WITHOUT
// re-copy or re-verify (issue #1), advancing the checkpoint atomically. The
// promotion runs through recoverChunks in batchPromote mode, which skips
// discovery/copy/verify/delete and executes only processBatch's completion
// tail — so the ONLY SQL is the CompleteBatch transaction below.
func TestRecoverCopyOnlyPromotesCopied(t *testing.T) {
	p, sourceMock, destMock, archMock := newTestPipeline(t, batchCopyVerify)

	archMock.ExpectQuery("SELECT root_pk_id FROM .*archiver_job_log_\\d+. WHERE log_status = \\?").
		WithArgs(LogStatusFailed).
		WillReturnRows(sqlmock.NewRows([]string{"root_pk_id"}))
	archMock.ExpectQuery("SELECT root_pk_id FROM .*archiver_job_log_\\d+. WHERE log_status = \\?").
		WithArgs(LogStatusCopied).
		WillReturnRows(sqlmock.NewRows([]string{"root_pk_id"}).AddRow("10").AddRow("9"))
	archMock.ExpectQuery("SELECT root_pk_id FROM .*archiver_job_log_\\d+. WHERE log_status = \\?").
		WithArgs(LogStatusPending).
		WillReturnRows(sqlmock.NewRows([]string{"root_pk_id"}))
	// One promote chunk (batchSize=1000 > 2): CompleteBatch(9,10, checkpoint=10),
	// numerically sorted (9 before 10 — lexicographic would order "10" first).
	archMock.ExpectBegin()
	archMock.ExpectExec("UPDATE .*archiver_job_log_\\d+. SET log_status").
		WithArgs(LogStatusCompleted, "9", "10").
		WillReturnResult(sqlmock.NewResult(0, 2))
	archMock.ExpectExec("UPDATE .*archiver_job. SET last_processed_root_pk_id").
		WithArgs("10", "job1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	archMock.ExpectCommit()

	stats, err := p.recover(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, 2, stats.RootsProcessed)
	// source/dest untouched: no re-discovery, no re-copy, no re-verify.
	require.NoError(t, sourceMock.ExpectationsWereMet())
	require.NoError(t, destMock.ExpectationsWereMet())
	require.NoError(t, archMock.ExpectationsWereMet())
}

// TestRecoverCopyOnlyPendingAdvancesCheckpointPerChunk: pending replay for
// copy-only advances the checkpoint with each ascending chunk (spec §Checkpoint
// rules) — the checkpoint lands atomically with each chunk's completion, and
// only after the whole chunk is terminal (issue #18's no-hole rule).
func TestRecoverCopyOnlyPendingAdvancesCheckpointPerChunk(t *testing.T) {
	p, sourceMock, destMock, archMock := newTestPipeline(t, batchCopyVerify)
	p.processingCfg.BatchSize = 2 // pending {1,2,3} -> chunks [1,2], [3]

	archMock.ExpectQuery("SELECT root_pk_id FROM .*archiver_job_log_\\d+. WHERE log_status = \\?").
		WithArgs(LogStatusFailed).
		WillReturnRows(sqlmock.NewRows([]string{"root_pk_id"}))
	archMock.ExpectQuery("SELECT root_pk_id FROM .*archiver_job_log_\\d+. WHERE log_status = \\?").
		WithArgs(LogStatusCopied).
		WillReturnRows(sqlmock.NewRows([]string{"root_pk_id"}))
	archMock.ExpectQuery("SELECT root_pk_id FROM .*archiver_job_log_\\d+. WHERE log_status = \\?").
		WithArgs(LogStatusPending).
		WillReturnRows(sqlmock.NewRows([]string{"root_pk_id"}).AddRow("3").AddRow("1").AddRow("2"))

	// Chunk [1,2]: copy pipeline then CompleteBatch with checkpoint "2".
	sourceMock.ExpectQuery("SELECT \\* FROM `customers` WHERE `id` IN \\(\\?, \\?\\)").
		WithArgs(int64(1), int64(2)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "a").AddRow(2, "b"))
	destMock.ExpectBegin()
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	destMock.ExpectExec("INSERT IGNORE INTO `customers`").WillReturnResult(sqlmock.NewResult(0, 2))
	destMock.ExpectCommit()
	archMock.ExpectExec("UPDATE .*archiver_job_log_\\d+. SET log_status").
		WithArgs(LogStatusCopied, "1", "2").
		WillReturnResult(sqlmock.NewResult(0, 2))
	archMock.ExpectBegin()
	archMock.ExpectExec("UPDATE .*archiver_job_log_\\d+. SET log_status").
		WithArgs(LogStatusCompleted, "1", "2").
		WillReturnResult(sqlmock.NewResult(0, 2))
	archMock.ExpectExec("UPDATE .*archiver_job. SET last_processed_root_pk_id").
		WithArgs("2", "job1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	archMock.ExpectCommit()

	// Chunk [3]: same shape, checkpoint "3".
	sourceMock.ExpectQuery("SELECT \\* FROM `customers` WHERE `id` IN \\(\\?\\)").
		WithArgs(int64(3)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(3, "c"))
	destMock.ExpectBegin()
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	destMock.ExpectExec("INSERT IGNORE INTO `customers`").WillReturnResult(sqlmock.NewResult(0, 1))
	destMock.ExpectCommit()
	archMock.ExpectExec("UPDATE .*archiver_job_log_\\d+. SET log_status").
		WithArgs(LogStatusCopied, "3").
		WillReturnResult(sqlmock.NewResult(0, 1))
	archMock.ExpectBegin()
	archMock.ExpectExec("UPDATE .*archiver_job_log_\\d+. SET log_status").
		WithArgs(LogStatusCompleted, "3").
		WillReturnResult(sqlmock.NewResult(0, 1))
	archMock.ExpectExec("UPDATE .*archiver_job. SET last_processed_root_pk_id").
		WithArgs("3", "job1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	archMock.ExpectCommit()

	stats, err := p.recover(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, 3, stats.RootsProcessed)
	require.NoError(t, sourceMock.ExpectationsWereMet())
	require.NoError(t, destMock.ExpectationsWereMet())
	require.NoError(t, archMock.ExpectationsWereMet())
}

// TestRecoverCopyOnlyMixedStatusesAscendGlobally: copied={10}, pending={9}
// must process 9 (full pipeline, checkpoint 9) BEFORE promoting 10
// (checkpoint 10). Two independent per-status phases would advance the
// checkpoint to 10 while 9 is still pending and then regress it — the merged
// ascending schedule keeps the checkpoint monotonic and never past a
// non-terminal row (review 2026-07-24, finding 2).
func TestRecoverCopyOnlyMixedStatusesAscendGlobally(t *testing.T) {
	p, sourceMock, destMock, archMock := newTestPipeline(t, batchCopyVerify)

	archMock.ExpectQuery("SELECT root_pk_id FROM .*archiver_job_log_\\d+. WHERE log_status = \\?").
		WithArgs(LogStatusFailed).
		WillReturnRows(sqlmock.NewRows([]string{"root_pk_id"}))
	archMock.ExpectQuery("SELECT root_pk_id FROM .*archiver_job_log_\\d+. WHERE log_status = \\?").
		WithArgs(LogStatusCopied).
		WillReturnRows(sqlmock.NewRows([]string{"root_pk_id"}).AddRow("10"))
	archMock.ExpectQuery("SELECT root_pk_id FROM .*archiver_job_log_\\d+. WHERE log_status = \\?").
		WithArgs(LogStatusPending).
		WillReturnRows(sqlmock.NewRows([]string{"root_pk_id"}).AddRow("9"))

	// Run 1 — pending [9]: full copy pipeline, then CompleteBatch(checkpoint=9).
	sourceMock.ExpectQuery("SELECT \\* FROM `customers` WHERE `id` IN \\(\\?\\)").
		WithArgs(int64(9)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(9, "i"))
	destMock.ExpectBegin()
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	destMock.ExpectExec("INSERT IGNORE INTO `customers`").WillReturnResult(sqlmock.NewResult(0, 1))
	destMock.ExpectCommit()
	archMock.ExpectExec("UPDATE .*archiver_job_log_\\d+. SET log_status").
		WithArgs(LogStatusCopied, "9").
		WillReturnResult(sqlmock.NewResult(0, 1))
	archMock.ExpectBegin()
	archMock.ExpectExec("UPDATE .*archiver_job_log_\\d+. SET log_status").
		WithArgs(LogStatusCompleted, "9").
		WillReturnResult(sqlmock.NewResult(0, 1))
	archMock.ExpectExec("UPDATE .*archiver_job. SET last_processed_root_pk_id").
		WithArgs("9", "job1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	archMock.ExpectCommit()

	// Run 2 — copied [10]: batchPromote (no re-discovery/re-copy/re-verify),
	// CompleteBatch(checkpoint=10).
	archMock.ExpectBegin()
	archMock.ExpectExec("UPDATE .*archiver_job_log_\\d+. SET log_status").
		WithArgs(LogStatusCompleted, "10").
		WillReturnResult(sqlmock.NewResult(0, 1))
	archMock.ExpectExec("UPDATE .*archiver_job. SET last_processed_root_pk_id").
		WithArgs("10", "job1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	archMock.ExpectCommit()

	stats, err := p.recover(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, 2, stats.RootsProcessed)
	require.NoError(t, sourceMock.ExpectationsWereMet())
	require.NoError(t, destMock.ExpectationsWereMet())
	require.NoError(t, archMock.ExpectationsWereMet())
}

// TestRecoverCopyOnlyPromoteHonorsStopChannel: a cooperative stop halts
// promotion at the chunk boundary with nil error and NO bookkeeping —
// remaining rows stay 'copied' for the next run. Promotion is now recoverChunks
// in batchPromote mode, so this is recoverChunks' own stop contract.
func TestRecoverCopyOnlyPromoteHonorsStopChannel(t *testing.T) {
	p, sourceMock, destMock, archMock := newTestPipeline(t, batchCopyVerify)
	stopped := make(chan struct{})
	close(stopped)
	p.stopCh = stopped

	archMock.ExpectQuery("SELECT root_pk_id FROM .*archiver_job_log_\\d+. WHERE log_status = \\?").
		WithArgs(LogStatusFailed).
		WillReturnRows(sqlmock.NewRows([]string{"root_pk_id"}))
	archMock.ExpectQuery("SELECT root_pk_id FROM .*archiver_job_log_\\d+. WHERE log_status = \\?").
		WithArgs(LogStatusCopied).
		WillReturnRows(sqlmock.NewRows([]string{"root_pk_id"}).AddRow("1").AddRow("2"))
	archMock.ExpectQuery("SELECT root_pk_id FROM .*archiver_job_log_\\d+. WHERE log_status = \\?").
		WithArgs(LogStatusPending).
		WillReturnRows(sqlmock.NewRows([]string{"root_pk_id"}))

	stats, err := p.recover(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, 0, stats.RootsProcessed) // no chunk started
	require.NoError(t, sourceMock.ExpectationsWereMet())
	require.NoError(t, destMock.ExpectationsWereMet())
	require.NoError(t, archMock.ExpectationsWereMet()) // no CompleteBatch fired
}

// TestRecoverCopyOnlyRespectsCheckpointFloor: requeuing a legacy failed row
// BELOW the current checkpoint (checkpoint=100, pending={9}) must complete
// the row WITHOUT touching the checkpoint — CompleteBatch has no monotonic
// guard, so writing 9 would regress the checkpoint from 100 and re-open
// completed roots 10..100 to the forward scan.
func TestRecoverCopyOnlyRespectsCheckpointFloor(t *testing.T) {
	p, sourceMock, destMock, archMock := newTestPipeline(t, batchCopyVerify)
	p.checkpointFloor = "100"

	archMock.ExpectQuery("SELECT root_pk_id FROM .*archiver_job_log_\\d+. WHERE log_status = \\?").
		WithArgs(LogStatusFailed).
		WillReturnRows(sqlmock.NewRows([]string{"root_pk_id"}))
	archMock.ExpectQuery("SELECT root_pk_id FROM .*archiver_job_log_\\d+. WHERE log_status = \\?").
		WithArgs(LogStatusCopied).
		WillReturnRows(sqlmock.NewRows([]string{"root_pk_id"}))
	archMock.ExpectQuery("SELECT root_pk_id FROM .*archiver_job_log_\\d+. WHERE log_status = \\?").
		WithArgs(LogStatusPending).
		WillReturnRows(sqlmock.NewRows([]string{"root_pk_id"}).AddRow("9"))

	sourceMock.ExpectQuery("SELECT \\* FROM `customers` WHERE `id` IN \\(\\?\\)").
		WithArgs(int64(9)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(9, "i"))
	destMock.ExpectBegin()
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	destMock.ExpectExec("INSERT IGNORE INTO `customers`").WillReturnResult(sqlmock.NewResult(0, 1))
	destMock.ExpectCommit()
	archMock.ExpectExec("UPDATE .*archiver_job_log_\\d+. SET log_status").
		WithArgs(LogStatusCopied, "9").
		WillReturnResult(sqlmock.NewResult(0, 1))
	// CompleteBatch WITHOUT the job-table checkpoint UPDATE: 9 <= floor 100.
	archMock.ExpectBegin()
	archMock.ExpectExec("UPDATE .*archiver_job_log_\\d+. SET log_status").
		WithArgs(LogStatusCompleted, "9").
		WillReturnResult(sqlmock.NewResult(0, 1))
	archMock.ExpectCommit()

	stats, err := p.recover(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, 1, stats.RootsProcessed)
	require.NoError(t, sourceMock.ExpectationsWereMet())
	require.NoError(t, destMock.ExpectationsWereMet())
	require.NoError(t, archMock.ExpectationsWereMet())
}
