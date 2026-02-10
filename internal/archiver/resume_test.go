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
				rm, err = NewResumeManager(nil, tt.log)
			} else {
				rm, err = NewResumeManager(db, tt.log)
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

	rm, _ := NewResumeManager(db, logger.NewDefault())

	// Mock successful table creation
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS archiver_job").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS archiver_job_log").WillReturnResult(sqlmock.NewResult(0, 0))

	ctx := context.Background()
	err := rm.InitializeTables(ctx)

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeManager_InitializeTables_JobTableError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault())

	// Mock job table creation failure
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS archiver_job").WillReturnError(assert.AnError)

	ctx := context.Background()
	err := rm.InitializeTables(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create archiver_job table")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeManager_InitializeTables_LogTableError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault())

	// Mock successful job table but log table failure
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS archiver_job").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS archiver_job_log").WillReturnError(assert.AnError)

	ctx := context.Background()
	err := rm.InitializeTables(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create archiver_job_log table")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeManager_GetOrCreateJob_CreateNew(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	log := logger.NewDefault()
	_, _ = NewResumeManager(db, log)

	// Mock: Job doesn't exist (ErrNoRows is tricky with sqlmock, skip this complex test)
	t.Skip("Complex sqlmock behavior with sql.ErrNoRows - covered by integration tests")
}

func TestResumeManager_GetOrCreateJob_ExistingWithCheckpoint(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault())

	// Mock: Existing job with checkpoint (use time.Time values for Scan compatibility)
	rows := sqlmock.NewRows([]string{"job_name", "root_table", "last_processed_root_pk_id", "job_status", "created_at", "updated_at"}).
		AddRow("test_job", "customers", 100, JobStatusIdle, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC))

	mock.ExpectQuery("SELECT job_name, root_table").
		WithArgs("test_job").
		WillReturnRows(rows)

	ctx := context.Background()
	state, err := rm.GetOrCreateJob(ctx, "test_job", "customers")

	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, "test_job", state.JobName)
	assert.Equal(t, "customers", state.RootTable)
	assert.Equal(t, int64(100), state.LastProcessedRootPKID)
	assert.Equal(t, JobStatusIdle, state.Status)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeManager_GetOrCreateJob_ExistingNoCheckpoint(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault())

	// Mock: Existing job without checkpoint (checkpoint = 0, use time.Time values)
	rows := sqlmock.NewRows([]string{"job_name", "root_table", "last_processed_root_pk_id", "job_status", "created_at", "updated_at"}).
		AddRow("test_job", "orders", 0, JobStatusIdle, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	mock.ExpectQuery("SELECT job_name, root_table").
		WithArgs("test_job").
		WillReturnRows(rows)

	ctx := context.Background()
	state, err := rm.GetOrCreateJob(ctx, "test_job", "orders")

	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, int64(0), state.LastProcessedRootPKID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeManager_UpdateJobStatus_Success(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault())

	mock.ExpectExec("UPDATE archiver_job SET job_status").
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

	rm, _ := NewResumeManager(db, logger.NewDefault())

	mock.ExpectExec("UPDATE archiver_job SET job_status").
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

	rm, _ := NewResumeManager(db, logger.NewDefault())

	mock.ExpectExec("UPDATE archiver_job SET last_processed_root_pk_id").
		WithArgs(int64(500), "test_job").
		WillReturnResult(sqlmock.NewResult(0, 1))

	ctx := context.Background()
	err := rm.UpdateCheckpoint(ctx, "test_job", 500)

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeManager_UpdateCheckpoint_Error(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault())

	mock.ExpectExec("UPDATE archiver_job SET last_processed_root_pk_id").
		WithArgs(int64(250), "test_job").
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

	rm, _ := NewResumeManager(db, logger.NewDefault())

	rootPKs := []interface{}{int64(1), int64(2), int64(3)}

	mock.ExpectPrepare("INSERT IGNORE INTO archiver_job_log")
	for _, pk := range rootPKs {
		mock.ExpectExec("INSERT IGNORE INTO archiver_job_log").
			WithArgs("test_job", pk, LogStatusPending).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}

	ctx := context.Background()
	err := rm.LogBatchPending(ctx, "test_job", rootPKs)

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeManager_LogBatchPending_EmptyBatch(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault())

	ctx := context.Background()
	err := rm.LogBatchPending(ctx, "test_job", []interface{}{})

	assert.NoError(t, err) // Should succeed with no-op for empty batch
}

func TestResumeManager_MarkCompleted_Success(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault())

	mock.ExpectExec("UPDATE archiver_job_log SET log_status").
		WithArgs(LogStatusCompleted, "test_job", int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	ctx := context.Background()
	err := rm.MarkCompleted(ctx, "test_job", 1)

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeManager_MarkFailed_Success(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault())

	mock.ExpectExec("UPDATE archiver_job_log SET log_status").
		WithArgs(LogStatusFailed, "test error", "test_job", int64(2)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	ctx := context.Background()
	err := rm.MarkFailed(ctx, "test_job", 2, "test error")

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeManager_GetPendingPKs_Success(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault())

	rows := sqlmock.NewRows([]string{"root_pk_id"}).
		AddRow(10).
		AddRow(20).
		AddRow(30)

	mock.ExpectQuery("SELECT root_pk_id FROM archiver_job_log WHERE job_name").
		WithArgs("test_job", LogStatusPending).
		WillReturnRows(rows)

	ctx := context.Background()
	pks, err := rm.GetPendingPKs(ctx, "test_job")

	require.NoError(t, err)
	require.Len(t, pks, 3)
	assert.Equal(t, int64(10), pks[0])
	assert.Equal(t, int64(20), pks[1])
	assert.Equal(t, int64(30), pks[2])
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeManager_GetPendingPKs_Empty(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault())

	rows := sqlmock.NewRows([]string{"root_pk_id"})

	mock.ExpectQuery("SELECT root_pk_id FROM archiver_job_log WHERE job_name").
		WithArgs("test_job", LogStatusPending).
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

	rm, _ := NewResumeManager(db, logger.NewDefault())

	// ShouldResume calls GetOrCreateJob first - mock it returning checkpoint=0
	jobRows := sqlmock.NewRows([]string{"job_name", "root_table", "last_processed_root_pk_id", "job_status", "created_at", "updated_at"}).
		AddRow("test_job", "", int64(0), JobStatusIdle, time.Now(), time.Now())
	mock.ExpectQuery("SELECT job_name, root_table").
		WithArgs("test_job").
		WillReturnRows(jobRows)

	// Then mock pending count > 0 (triggers resume)
	countRows := sqlmock.NewRows([]string{"count"}).AddRow(5)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM archiver_job_log WHERE job_name").
		WithArgs("test_job", LogStatusPending).
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

	rm, _ := NewResumeManager(db, logger.NewDefault())

	// ShouldResume calls GetOrCreateJob first - mock it returning checkpoint=0
	jobRows := sqlmock.NewRows([]string{"job_name", "root_table", "last_processed_root_pk_id", "job_status", "created_at", "updated_at"}).
		AddRow("test_job", "", int64(0), JobStatusIdle, time.Now(), time.Now())
	mock.ExpectQuery("SELECT job_name, root_table").
		WithArgs("test_job").
		WillReturnRows(jobRows)

	// Then mock pending count = 0 (no resume needed)
	countRows := sqlmock.NewRows([]string{"count"}).AddRow(0)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM archiver_job_log WHERE job_name").
		WithArgs("test_job", LogStatusPending).
		WillReturnRows(countRows)

	ctx := context.Background()
	shouldResume, err := rm.ShouldResume(ctx, "test_job")

	require.NoError(t, err)
	assert.False(t, shouldResume)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeManager_GetStats_Success(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	rm, _ := NewResumeManager(db, logger.NewDefault())

	rows := sqlmock.NewRows([]string{"log_status", "count"}).
		AddRow(string(LogStatusPending), 5).
		AddRow(string(LogStatusCompleted), 95).
		AddRow(string(LogStatusFailed), 2)

	mock.ExpectQuery("SELECT log_status, COUNT\\(\\*\\) FROM archiver_job_log WHERE job_name").
		WithArgs("test_job").
		WillReturnRows(rows)

	ctx := context.Background()
	pending, completed, failed, err := rm.GetStats(ctx, "test_job")

	require.NoError(t, err)
	assert.Equal(t, 5, pending)
	assert.Equal(t, 95, completed)
	assert.Equal(t, 2, failed)
	assert.NoError(t, mock.ExpectationsWereMet())
}
