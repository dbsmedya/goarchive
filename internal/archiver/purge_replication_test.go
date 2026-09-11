package archiver

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/dbsmedya/goarchive/internal/types"
	"github.com/stretchr/testify/require"
)

type scriptedLagWaiter struct {
	errors []error
	calls  int
}

func (w *scriptedLagWaiter) WaitForLag(context.Context) error {
	w.calls++
	if w.calls <= len(w.errors) {
		return w.errors[w.calls-1]
	}
	return nil
}

func newPurgeRecoveryUnitPipeline(t *testing.T, waiter lagWaiter) (*batchPipeline, sqlmock.Sqlmock, sqlmock.Sqlmock) {
	t.Helper()

	sourceDB, sourceMock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sourceDB.Close() })
	archDB, archMock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = archDB.Close() })

	g := createMultiLevelGraph()
	g.SetRootPKMeta("bigint", false)
	log := logger.NewDefault()
	discovery, err := NewRecordDiscovery(g, sourceDB, 1)
	require.NoError(t, err)
	discovery.SetColumnMetadata(map[string]types.ColumnMetadata{
		"customers": {Names: []string{"id"}, Temporal: map[string]types.TemporalKind{}},
		"orders":    {Names: []string{"id", "customer_id"}, Temporal: map[string]types.TemporalKind{}},
	})
	deletePhase, err := NewDeletePhase(sourceDB, g, 10, log)
	require.NoError(t, err)
	deletePhase.SetColumnMetadata(testNonTemporalMetadata(g))
	resumeMgr, err := NewResumeManager(archDB, log, "testdb")
	require.NoError(t, err)
	resumeMgr.setJobID(7)

	return &batchPipeline{
		jobName:       "purge_recovery_unit",
		mainMode:      batchDeleteOnly,
		graph:         g,
		logger:        log,
		processingCfg: config.ProcessingConfig{BatchSize: 1, BatchDeleteSize: 10},
		discovery:     discovery,
		deletePhase:   deletePhase,
		resumeMgr:     resumeMgr,
		lagMonitor:    waiter,
	}, sourceMock, archMock
}

func expectPurgeRecoveryChunk(sourceMock sqlmock.Sqlmock, archMock sqlmock.Sqlmock, root int64, rootPK string, order int64) {
	sourceMock.ExpectQuery("SELECT `id` FROM `orders` WHERE `customer_id` IN").
		WithArgs(root).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(order))
	sourceMock.ExpectExec("DELETE FROM `orders` WHERE `id` IN").
		WithArgs(order).
		WillReturnResult(sqlmock.NewResult(0, 1))
	sourceMock.ExpectExec("DELETE FROM `customers` WHERE `id` IN").
		WithArgs(root).
		WillReturnResult(sqlmock.NewResult(0, 1))
	archMock.ExpectBegin()
	archMock.ExpectExec("UPDATE .*archiver_job_log_\\d+. SET log_status").
		WithArgs(LogStatusCompleted, rootPK).
		WillReturnResult(sqlmock.NewResult(0, 1))
	archMock.ExpectCommit()
}

func TestPurgeRecoveryChunkGate(t *testing.T) {
	sentinel := errors.New("outer recovery gate sentinel")
	waiter := &scriptedLagWaiter{errors: []error{sentinel}}
	pipeline, sourceMock, archMock := newPurgeRecoveryUnitPipeline(t, waiter)
	// This deliberately optional alternate-path fixture keeps the M17 mutant
	// valid: without the outer gate, discovery succeeds and the same sentinel
	// is returned by the surviving pre-delete gate. On the healthy baseline the
	// exact remaining expectation below proves discovery never started.
	sourceMock.ExpectQuery("SELECT `id` FROM `orders` WHERE `customer_id` IN").
		WithArgs(int64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(101)))

	agg := &BatchStats{}
	err := pipeline.recoverChunks(context.Background(), []string{"1", "2"}, batchDeleteOnly, false, nil, agg)
	require.ErrorIs(t, err, sentinel)
	require.EqualError(t, err, "lag monitor error: outer recovery gate sentinel")
	require.Equal(t, 1, waiter.calls)
	require.Zero(t, agg.RootsProcessed)
	sourceErr := sourceMock.ExpectationsWereMet()
	require.Error(t, sourceErr, "outer gate must leave the alternate discovery fixture unused")
	require.Contains(t, sourceErr.Error(), "SELECT `id` FROM `orders` WHERE `customer_id` IN")
	require.NoError(t, archMock.ExpectationsWereMet())
}

func TestPurgeRecoveryChunkGateHealthy(t *testing.T) {
	waiter := &scriptedLagWaiter{}
	pipeline, sourceMock, archMock := newPurgeRecoveryUnitPipeline(t, waiter)
	expectPurgeRecoveryChunk(sourceMock, archMock, 1, "1", 101)
	expectPurgeRecoveryChunk(sourceMock, archMock, 2, "2", 201)

	agg := &BatchStats{}
	err := pipeline.recoverChunks(context.Background(), []string{"2", "1"}, batchDeleteOnly, false, nil, agg)
	require.NoError(t, err)
	require.Equal(t, 4, waiter.calls, "outer and pre-delete gate must run for both chunks")
	require.Equal(t, 2, agg.RootsProcessed)
	require.EqualValues(t, 4, agg.RecordsDeleted)
	require.NoError(t, sourceMock.ExpectationsWereMet())
	require.NoError(t, archMock.ExpectationsWereMet())
}
