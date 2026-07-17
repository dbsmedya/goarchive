package archiver

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewResumeManager_Validation(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer func() { _ = db.Close() }()
	log := logger.NewDefault()

	tests := []struct {
		name      string
		db        interface{}
		log       *logger.Logger
		expectErr bool
		errMsg    string
	}{
		{
			name:      "Valid inputs",
			db:        db,
			log:       log,
			expectErr: false,
		},
		{
			name:      "Nil database",
			db:        nil,
			log:       log,
			expectErr: true,
			errMsg:    "database connection is nil",
		},
		{
			name:      "Nil logger with valid DB",
			db:        db,
			log:       nil,
			expectErr: false, // Creates default logger
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rm *ResumeManager
			var err error
			if tt.db == nil {
				rm, err = NewResumeManager(nil, tt.log, "testdb")
			} else {
				rm, err = NewResumeManager(db, tt.log, "testdb")
			}

			if tt.expectErr {
				assert.Error(t, err)
				assert.Nil(t, rm)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, rm)
			}
		})
	}
}

func TestResumeManager_InitializeTables_Success(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault(), "testdb")

	// Legacy probe: archiver_job does not yet exist (fresh schema).
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.tables").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	// Create archiver_job only (per-job log table is created later).
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS .*archiver_job`").WillReturnResult(sqlmock.NewResult(0, 0))

	ctx := context.Background()
	err := rm.InitializeTables(ctx)

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeManager_HeartbeatAndStaleness(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()
	rm, _ := NewResumeManager(db, logger.NewDefault(), "testdb")

	mock.ExpectExec("UPDATE .*archiver_job` SET last_heartbeat_at = NOW\\(\\) WHERE job_name = \\?").
		WithArgs("job1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := rm.Heartbeat(context.Background(), "job1"); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	mock.ExpectQuery("SELECT TIMESTAMPDIFF\\(SECOND, last_heartbeat_at, NOW\\(\\)\\)").
		WithArgs("job1").
		WillReturnRows(sqlmock.NewRows([]string{"age_seconds"}).AddRow(int64(5)))
	stale, age, err := rm.IsHeartbeatStale(context.Background(), "job1", time.Minute)
	if err != nil {
		t.Fatalf("IsHeartbeatStale fresh: %v", err)
	}
	if stale || age < 5*time.Second {
		t.Fatalf("fresh heartbeat: stale=%v age=%v", stale, age)
	}

	mock.ExpectQuery("SELECT TIMESTAMPDIFF\\(SECOND, last_heartbeat_at, NOW\\(\\)\\)").
		WithArgs("job1").
		WillReturnRows(sqlmock.NewRows([]string{"age_seconds"}).AddRow(int64(120)))
	stale, age, err = rm.IsHeartbeatStale(context.Background(), "job1", time.Minute)
	if err != nil {
		t.Fatalf("IsHeartbeatStale stale: %v", err)
	}
	if !stale || age < 120*time.Second {
		t.Fatalf("stale heartbeat: stale=%v age=%v", stale, age)
	}
}

func TestResumeManager_InitializeTables_JobTableError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault(), "testdb")

	// Legacy probe (fresh schema), then job table creation failure.
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.tables").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS .*archiver_job`").WillReturnError(assert.AnError)

	ctx := context.Background()
	err := rm.InitializeTables(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create archiver_job in schema")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeManager_InitializeTables_LegacyShapeRejected(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault(), "testdb")

	// archiver_job exists but lacks the new integer id PRIMARY KEY -> legacy.
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.tables").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.columns").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	ctx := context.Background()
	err := rm.InitializeTables(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "legacy GoArchive tracking tables detected")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeManager_UpdateJobStatus_Success(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault(), "testdb")

	mock.ExpectExec("UPDATE .*archiver_job` SET job_status").
		WithArgs(JobStatusRunning, "test_job").
		WillReturnResult(sqlmock.NewResult(0, 1))

	ctx := context.Background()
	err := rm.UpdateJobStatus(ctx, "test_job", JobStatusRunning)

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeManager_UpdateJobStatus_Error(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault(), "testdb")

	mock.ExpectExec("UPDATE .*archiver_job` SET job_status").
		WithArgs(JobStatusFailed, "test_job").
		WillReturnError(assert.AnError)

	ctx := context.Background()
	err := rm.UpdateJobStatus(ctx, "test_job", JobStatusFailed)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to update job status")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeManager_UpdateCheckpoint_Success(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault(), "testdb")

	mock.ExpectExec("UPDATE .*archiver_job` SET last_processed_root_pk_id").
		WithArgs("500", "test_job").
		WillReturnResult(sqlmock.NewResult(0, 1))

	ctx := context.Background()
	err := rm.UpdateCheckpoint(ctx, "test_job", 500)

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeManager_UpdateCheckpoint_Error(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault(), "testdb")

	mock.ExpectExec("UPDATE .*archiver_job` SET last_processed_root_pk_id").
		WithArgs("250", "test_job").
		WillReturnError(assert.AnError)

	ctx := context.Background()
	err := rm.UpdateCheckpoint(ctx, "test_job", 250)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to update checkpoint")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeManager_LogBatchPending_Success(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault(), "testdb")
	rm.setJobID(7)

	rootPKs := []interface{}{int64(1), int64(2), int64(3)}

	// Default chunk size is 1000, so all 3 PKs go in one multi-row INSERT.
	mock.ExpectExec("INSERT IGNORE INTO .*archiver_job_log_\\d+").
		WithArgs("1", LogStatusPending, "2", LogStatusPending, "3", LogStatusPending).
		WillReturnResult(sqlmock.NewResult(3, 3))

	ctx := context.Background()
	err := rm.LogBatchPending(ctx, "test_job", rootPKs)

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeManager_LogBatchPending_EmptyBatch(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault(), "testdb")

	ctx := context.Background()
	err := rm.LogBatchPending(ctx, "test_job", []interface{}{})

	assert.NoError(t, err) // Should succeed with no-op for empty batch
}

func TestResumeManager_MarkCompleted_Success(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault(), "testdb")
	rm.setJobID(7)

	mock.ExpectExec("UPDATE .*archiver_job_log_\\d+. SET log_status").
		WithArgs(LogStatusCompleted, "1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	ctx := context.Background()
	err := rm.MarkCompleted(ctx, "test_job", 1)

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeManager_MarkFailed_Success(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault(), "testdb")
	rm.setJobID(7)

	mock.ExpectExec("UPDATE .*archiver_job_log_\\d+. SET log_status").
		WithArgs(LogStatusFailed, "test error", "2").
		WillReturnResult(sqlmock.NewResult(0, 1))

	ctx := context.Background()
	err := rm.MarkFailed(ctx, "test_job", 2, "test error")

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeManager_GetPendingPKs_Success(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault(), "testdb")
	rm.setJobID(7)

	rows := sqlmock.NewRows([]string{"root_pk_id"}).
		AddRow("10").
		AddRow("20").
		AddRow("30")

	mock.ExpectQuery("SELECT root_pk_id FROM .*archiver_job_log_\\d+. WHERE log_status = \\?").
		WithArgs(LogStatusPending).
		WillReturnRows(rows)

	ctx := context.Background()
	pks, err := rm.GetPendingPKs(ctx, "test_job")

	require.NoError(t, err)
	require.Len(t, pks, 3)
	assert.Equal(t, "10", pks[0])
	assert.Equal(t, "20", pks[1])
	assert.Equal(t, "30", pks[2])
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeManager_GetPendingPKs_Empty(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault(), "testdb")
	rm.setJobID(7)

	rows := sqlmock.NewRows([]string{"root_pk_id"})

	mock.ExpectQuery("SELECT root_pk_id FROM .*archiver_job_log_\\d+. WHERE log_status = \\?").
		WithArgs(LogStatusPending).
		WillReturnRows(rows)

	ctx := context.Background()
	pks, err := rm.GetPendingPKs(ctx, "test_job")

	require.NoError(t, err)
	assert.Len(t, pks, 0)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeManager_ShouldResume_True(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault(), "testdb")
	rm.setJobID(7)

	// ShouldResume checks existing checkpoint directly
	jobRows := sqlmock.NewRows([]string{"last_processed_root_pk_id"}).
		AddRow("")
	mock.ExpectQuery("SELECT last_processed_root_pk_id FROM .*archiver_job` WHERE job_name = \\?").
		WithArgs("test_job").
		WillReturnRows(jobRows)

	// Then mock non-terminal count > 0 (pending OR copied) triggers resume.
	countRows := sqlmock.NewRows([]string{"count"}).AddRow(5)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM .*archiver_job_log_\\d+. WHERE log_status IN").
		WithArgs(LogStatusPending, LogStatusCopied).
		WillReturnRows(countRows)

	ctx := context.Background()
	shouldResume, err := rm.ShouldResume(ctx, "test_job")

	require.NoError(t, err)
	assert.True(t, shouldResume)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeManager_ShouldResume_False(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault(), "testdb")
	rm.setJobID(7)

	// ShouldResume checks existing checkpoint directly
	jobRows := sqlmock.NewRows([]string{"last_processed_root_pk_id"}).
		AddRow("")
	mock.ExpectQuery("SELECT last_processed_root_pk_id FROM .*archiver_job` WHERE job_name = \\?").
		WithArgs("test_job").
		WillReturnRows(jobRows)

	// Non-terminal count = 0 (no resume needed).
	countRows := sqlmock.NewRows([]string{"count"}).AddRow(0)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM .*archiver_job_log_\\d+. WHERE log_status IN").
		WithArgs(LogStatusPending, LogStatusCopied).
		WillReturnRows(countRows)

	ctx := context.Background()
	shouldResume, err := rm.ShouldResume(ctx, "test_job")

	require.NoError(t, err)
	assert.False(t, shouldResume)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLogBatchPendingMultiRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	rm, err := NewResumeManager(db, logger.NewDefault(), "testdb")
	if err != nil {
		t.Fatalf("NewResumeManager: %v", err)
	}
	rm.setJobID(7)
	rm.SetChunkSize(2) // force chunking at 2

	mock.ExpectExec("INSERT IGNORE INTO .*archiver_job_log_\\d+").
		WithArgs("1", LogStatusPending, "2", LogStatusPending).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec("INSERT IGNORE INTO .*archiver_job_log_\\d+").
		WithArgs("3", LogStatusPending).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = rm.LogBatchPending(context.Background(), "job1",
		[]interface{}{int64(1), int64(2), int64(3)})
	if err != nil {
		t.Fatalf("LogBatchPending: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestCompleteBatchAtomicWithCheckpoint(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	rm, _ := NewResumeManager(db, logger.NewDefault(), "testdb")
	rm.setJobID(7)
	rm.SetChunkSize(10)

	cp := interface{}(int64(42))

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE .*archiver_job_log_\\d+. SET log_status").
		WithArgs(LogStatusCompleted, "1", "2").
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec("UPDATE .*archiver_job` SET last_processed_root_pk_id").
		WithArgs("42", "job1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err = rm.CompleteBatch(context.Background(), "job1",
		[]interface{}{int64(1), int64(2)}, cp)
	if err != nil {
		t.Fatalf("CompleteBatch: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestCompleteBatchNoCheckpointForReplay(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	rm, _ := NewResumeManager(db, logger.NewDefault(), "testdb")
	rm.setJobID(7)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE .*archiver_job_log_\\d+. SET log_status").
		WithArgs(LogStatusCompleted, "1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err = rm.CompleteBatch(context.Background(), "job1",
		[]interface{}{int64(1)}, nil)
	if err != nil {
		t.Fatalf("CompleteBatch (replay): %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestMarkBatchCopiedChunked(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	rm, _ := NewResumeManager(db, logger.NewDefault(), "testdb")
	rm.setJobID(7)
	rm.SetChunkSize(2)

	mock.ExpectExec("UPDATE .*archiver_job_log_\\d+. SET log_status").
		WithArgs(LogStatusCopied, "1", "2").
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec("UPDATE .*archiver_job_log_\\d+. SET log_status").
		WithArgs(LogStatusCopied, "3").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := rm.MarkBatchCopied(context.Background(), "job1",
		[]interface{}{int64(1), int64(2), int64(3)}); err != nil {
		t.Fatalf("MarkBatchCopied: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestGetRootPKsByStatus(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	rm, _ := NewResumeManager(db, logger.NewDefault(), "testdb")
	rm.setJobID(7)

	mock.ExpectQuery("SELECT root_pk_id FROM .*archiver_job_log_\\d+. WHERE log_status = \\?").
		WithArgs(LogStatusCopied).
		WillReturnRows(sqlmock.NewRows([]string{"root_pk_id"}).AddRow("5").AddRow("6"))

	got, err := rm.GetRootPKsByStatus(context.Background(), "job1", LogStatusCopied)
	if err != nil {
		t.Fatalf("GetRootPKsByStatus: %v", err)
	}
	if len(got) != 2 || got[0] != "5" || got[1] != "6" {
		t.Fatalf("got %v, want [5 6]", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

// TestRequireLogTableGuard verifies that every log-table method returns a clear
// error when called before GetOrCreateJob(WithType) has resolved the per-job
// log table name.
func TestRequireLogTableGuard(t *testing.T) {
	ctx := context.Background()

	// ShouldResume only hits the guard AFTER the checkpoint SELECT on
	// archiver_job succeeds; we prime that query to return an empty checkpoint
	// so execution reaches requireLogTable().
	makeMockWithCheckpointSelect := func(t *testing.T) (*ResumeManager, sqlmock.Sqlmock) {
		t.Helper()
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		rm, err := NewResumeManager(db, logger.NewDefault(), "testdb")
		require.NoError(t, err)
		// rm.logTable intentionally left empty (setJobID never called)
		return rm, mock
	}

	// For pure log-only methods the guard fires before any DB query, so no
	// sqlmock expectations are needed. We share a single instance for those.
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	rm, err := NewResumeManager(db, logger.NewDefault(), "testdb")
	require.NoError(t, err)

	t.Run("LogBatchPending", func(t *testing.T) {
		err := rm.LogBatchPending(ctx, "job", []interface{}{1})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "per-job log table not resolved")
	})

	t.Run("MarkBatchCopied", func(t *testing.T) {
		err := rm.MarkBatchCopied(ctx, "job", []interface{}{1})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "per-job log table not resolved")
	})

	t.Run("MarkCompleted", func(t *testing.T) {
		err := rm.MarkCompleted(ctx, "job", 1)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "per-job log table not resolved")
	})

	t.Run("MarkFailed", func(t *testing.T) {
		err := rm.MarkFailed(ctx, "job", 1, "some error")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "per-job log table not resolved")
	})

	t.Run("CompleteBatch", func(t *testing.T) {
		// Non-empty rootPKs forces past CompleteBatch's early-return (which
		// fires only when both rootPKs and checkpointPK are empty/nil) so the
		// requireLogTable guard is reached.
		err := rm.CompleteBatch(ctx, "job", []interface{}{1}, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "per-job log table not resolved")
	})

	t.Run("GetRootPKsByStatus", func(t *testing.T) {
		_, err := rm.GetRootPKsByStatus(ctx, "job", LogStatusPending)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "per-job log table not resolved")
	})

	// Verify no unexpected DB calls were made for the pure guard methods above.
	assert.NoError(t, mock.ExpectationsWereMet())

	// ShouldResume: requires a successful checkpoint SELECT first, then hits the guard.
	t.Run("ShouldResume", func(t *testing.T) {
		rmS, mockS := makeMockWithCheckpointSelect(t)
		// Prime the checkpoint SELECT to return a row with an empty checkpoint
		// so ShouldResume proceeds past the first branch into requireLogTable().
		mockS.ExpectQuery("SELECT last_processed_root_pk_id FROM .*archiver_job` WHERE job_name = \\?").
			WithArgs("job").
			WillReturnRows(sqlmock.NewRows([]string{"last_processed_root_pk_id"}).AddRow(nil))

		_, err := rmS.ShouldResume(ctx, "job")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "per-job log table not resolved")
		assert.NoError(t, mockS.ExpectationsWereMet())
	})
}

func TestShouldResumeTriggersOnCopied(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	rm, _ := NewResumeManager(db, logger.NewDefault(), "testdb")
	rm.setJobID(7)

	// No checkpoint set.
	mock.ExpectQuery("SELECT last_processed_root_pk_id FROM .*archiver_job` WHERE job_name = \\?").
		WithArgs("job1").
		WillReturnRows(sqlmock.NewRows([]string{"last_processed_root_pk_id"}).AddRow(nil))
	// No pending, but one copied -> must still resume.
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM .*archiver_job_log_\\d+. WHERE log_status IN \\(\\?, \\?\\)").
		WithArgs(LogStatusPending, LogStatusCopied).
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(1))

	should, err := rm.ShouldResume(context.Background(), "job1")
	if err != nil {
		t.Fatalf("ShouldResume: %v", err)
	}
	if !should {
		t.Fatal("expected ShouldResume=true when only 'copied' entries exist")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}
