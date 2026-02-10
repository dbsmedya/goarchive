package archiver

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLagMonitor_Disabled(t *testing.T) {
	log := logger.NewDefault()
	cfg := config.SafetyConfig{}

	// Create with nil DB (disabled mode)
	lm, err := NewLagMonitor(nil, cfg, log)

	require.NoError(t, err)
	require.NotNil(t, lm)
	assert.False(t, lm.enabled)
	assert.Nil(t, lm.db)
}

func TestNewLagMonitor_Enabled_DefaultValues(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer func() { _ = db.Close() }()
	log := logger.NewDefault()

	// Create with empty config (should use defaults)
	cfg := config.SafetyConfig{}

	lm, err := NewLagMonitor(db, cfg, log)

	require.NoError(t, err)
	require.NotNil(t, lm)
	assert.True(t, lm.enabled)
	assert.NotNil(t, lm.db)
	assert.Equal(t, 10, lm.threshold)             // Default threshold
	assert.Greater(t, lm.interval.Seconds(), 0.0) // Default interval
}

func TestNewLagMonitor_Enabled_CustomValues(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer func() { _ = db.Close() }()
	log := logger.NewDefault()

	cfg := config.SafetyConfig{
		LagThreshold:  30,
		CheckInterval: 15,
	}

	lm, err := NewLagMonitor(db, cfg, log)

	require.NoError(t, err)
	require.NotNil(t, lm)
	assert.True(t, lm.enabled)
	assert.Equal(t, 30, lm.threshold)
	assert.Equal(t, 15.0, lm.interval.Seconds())
}

func TestNewLagMonitor_NilLogger(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	cfg := config.SafetyConfig{}

	// Should create default logger
	lm, err := NewLagMonitor(db, cfg, nil)

	require.NoError(t, err)
	require.NotNil(t, lm)
	assert.NotNil(t, lm.logger)
}

func TestLagMonitor_GetReplicationStatus_Disabled(t *testing.T) {
	log := logger.NewDefault()
	lm, _ := NewLagMonitor(nil, config.SafetyConfig{}, log)

	ctx := context.Background()
	status, err := lm.GetReplicationStatus(ctx)

	assert.NoError(t, err)
	assert.Nil(t, status) // Disabled returns nil status
}

func TestLagMonitor_GetReplicationStatus_Success(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	log := logger.NewDefault()
	cfg := config.SafetyConfig{LagThreshold: 10}
	lm, _ := NewLagMonitor(db, cfg, log)

	// Mock SHOW REPLICA STATUS result
	rows := sqlmock.NewRows([]string{
		"Seconds_Behind_Master", "Slave_IO_Running", "Slave_SQL_Running", "Last_Error",
	}).AddRow(5, "Yes", "Yes", "")

	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(rows)

	ctx := context.Background()
	status, err := lm.GetReplicationStatus(ctx)

	require.NoError(t, err)
	require.NotNil(t, status)
	assert.True(t, status.SecondsBehindMaster.Valid)
	assert.Equal(t, int64(5), status.SecondsBehindMaster.Int64)
	assert.Equal(t, "Yes", status.SlaveIORunning)
	assert.Equal(t, "Yes", status.SlaveSQLRunning)
	assert.Empty(t, status.LastError)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLagMonitor_GetReplicationStatus_FallbackToSlave(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	log := logger.NewDefault()
	cfg := config.SafetyConfig{LagThreshold: 10}
	lm, _ := NewLagMonitor(db, cfg, log)

	// Mock SHOW REPLICA STATUS failure, fallback to SHOW SLAVE STATUS
	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnError(sql.ErrNoRows)

	rows := sqlmock.NewRows([]string{
		"Seconds_Behind_Master", "Slave_IO_Running", "Slave_SQL_Running", "Last_Error",
	}).AddRow(3, "Yes", "Yes", "")

	mock.ExpectQuery("SHOW SLAVE STATUS").WillReturnRows(rows)

	ctx := context.Background()
	status, err := lm.GetReplicationStatus(ctx)

	require.NoError(t, err)
	require.NotNil(t, status)
	assert.Equal(t, int64(3), status.SecondsBehindMaster.Int64)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLagMonitor_GetReplicationStatus_NoReplication(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	log := logger.NewDefault()
	cfg := config.SafetyConfig{LagThreshold: 10}
	lm, _ := NewLagMonitor(db, cfg, log)

	// Mock: Query succeeds but returns no rows (replication not configured)
	rows := sqlmock.NewRows([]string{"Seconds_Behind_Master"})
	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(rows)

	ctx := context.Background()
	status, err := lm.GetReplicationStatus(ctx)

	assert.Error(t, err)
	assert.Nil(t, status)
	assert.Contains(t, err.Error(), "replication not configured")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLagMonitor_GetReplicationStatus_QueryError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	log := logger.NewDefault()
	cfg := config.SafetyConfig{LagThreshold: 10}
	lm, _ := NewLagMonitor(db, cfg, log)

	// Mock both queries failing
	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnError(sql.ErrConnDone)
	mock.ExpectQuery("SHOW SLAVE STATUS").WillReturnError(sql.ErrConnDone)

	ctx := context.Background()
	status, err := lm.GetReplicationStatus(ctx)

	assert.Error(t, err)
	assert.Nil(t, status)
	assert.Contains(t, err.Error(), "failed to query replication status")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLagMonitor_CheckLag_Disabled(t *testing.T) {
	log := logger.NewDefault()
	lm, _ := NewLagMonitor(nil, config.SafetyConfig{}, log)

	ctx := context.Background()
	ok, lag, err := lm.CheckLag(ctx)

	assert.NoError(t, err)
	assert.True(t, ok)      // Always OK when disabled
	assert.Equal(t, 0, lag) // Lag reported as 0
}

func TestLagMonitor_CheckLag_WithinThreshold(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	log := logger.NewDefault()
	cfg := config.SafetyConfig{LagThreshold: 10}
	lm, _ := NewLagMonitor(db, cfg, log)

	// Mock: Lag is 5 seconds (within 10 second threshold)
	rows := sqlmock.NewRows([]string{
		"Seconds_Behind_Master", "Slave_IO_Running", "Slave_SQL_Running", "Last_Error",
	}).AddRow(5, "Yes", "Yes", "")

	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(rows)

	ctx := context.Background()
	ok, lag, err := lm.CheckLag(ctx)

	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, 5, lag)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLagMonitor_CheckLag_ExceedsThreshold(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	log := logger.NewDefault()
	cfg := config.SafetyConfig{LagThreshold: 10}
	lm, _ := NewLagMonitor(db, cfg, log)

	// Mock: Lag is 25 seconds (exceeds 10 second threshold)
	rows := sqlmock.NewRows([]string{
		"Seconds_Behind_Master", "Slave_IO_Running", "Slave_SQL_Running", "Last_Error",
	}).AddRow(25, "Yes", "Yes", "")

	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(rows)

	ctx := context.Background()
	ok, lag, err := lm.CheckLag(ctx)

	require.NoError(t, err)
	assert.False(t, ok) // Lag exceeds threshold
	assert.Equal(t, 25, lag)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLagMonitor_CheckLag_ReplicaStopped(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	log := logger.NewDefault()
	cfg := config.SafetyConfig{LagThreshold: 10}
	lm, _ := NewLagMonitor(db, cfg, log)

	// Mock: Replica stopped (Seconds_Behind_Master is NULL, IO/SQL not running)
	rows := sqlmock.NewRows([]string{
		"Seconds_Behind_Master", "Slave_IO_Running", "Slave_SQL_Running", "Last_Error",
	}).AddRow(nil, "No", "No", "")

	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(rows)

	ctx := context.Background()
	ok, _, err := lm.CheckLag(ctx)

	assert.Error(t, err)
	assert.False(t, ok)
	assert.Contains(t, err.Error(), "replication is not running")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLagMonitor_CheckLag_IOThreadStopped(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	log := logger.NewDefault()
	cfg := config.SafetyConfig{LagThreshold: 10}
	lm, _ := NewLagMonitor(db, cfg, log)

	// Mock: IO thread stopped
	rows := sqlmock.NewRows([]string{
		"Seconds_Behind_Master", "Slave_IO_Running", "Slave_SQL_Running", "Last_Error",
	}).AddRow(5, "No", "Yes", "")

	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(rows)

	ctx := context.Background()
	ok, _, err := lm.CheckLag(ctx)

	assert.Error(t, err)
	assert.False(t, ok)
	assert.Contains(t, err.Error(), "replication is not running")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLagMonitor_CheckLag_SQLThreadStopped(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	log := logger.NewDefault()
	cfg := config.SafetyConfig{LagThreshold: 10}
	lm, _ := NewLagMonitor(db, cfg, log)

	// Mock: SQL thread stopped
	rows := sqlmock.NewRows([]string{
		"Seconds_Behind_Master", "Slave_IO_Running", "Slave_SQL_Running", "Last_Error",
	}).AddRow(5, "Yes", "No", "Connection refused")

	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(rows)

	ctx := context.Background()
	ok, _, err := lm.CheckLag(ctx)

	assert.Error(t, err)
	assert.False(t, ok)
	assert.Contains(t, err.Error(), "replication is not running")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLagMonitor_CheckLag_StatusQueryError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	log := logger.NewDefault()
	cfg := config.SafetyConfig{LagThreshold: 10}
	lm, _ := NewLagMonitor(db, cfg, log)

	// Mock query error
	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnError(sql.ErrConnDone)
	mock.ExpectQuery("SHOW SLAVE STATUS").WillReturnError(sql.ErrConnDone)

	ctx := context.Background()
	ok, lag, err := lm.CheckLag(ctx)

	assert.Error(t, err)
	assert.False(t, ok)
	assert.Equal(t, -1, lag)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLagMonitor_WaitForLag_Disabled(t *testing.T) {
	log := logger.NewDefault()
	lm, _ := NewLagMonitor(nil, config.SafetyConfig{}, log)

	ctx := context.Background()
	err := lm.WaitForLag(ctx)

	assert.NoError(t, err) // Immediately returns when disabled
}

func TestLagMonitor_WaitForLag_AlreadyWithinThreshold(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	log := logger.NewDefault()
	cfg := config.SafetyConfig{LagThreshold: 10, CheckInterval: 1}
	lm, _ := NewLagMonitor(db, cfg, log)

	// Mock: Lag within threshold on first check
	rows := sqlmock.NewRows([]string{
		"Seconds_Behind_Master", "Slave_IO_Running", "Slave_SQL_Running", "Last_Error",
	}).AddRow(5, "Yes", "Yes", "")

	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(rows)

	ctx := context.Background()
	err := lm.WaitForLag(ctx)

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLagMonitor_WaitForLag_ContextCancellation(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	log := logger.NewDefault()
	cfg := config.SafetyConfig{LagThreshold: 10, CheckInterval: 1}
	lm, _ := NewLagMonitor(db, cfg, log)

	// Mock: Lag exceeds threshold
	rows := sqlmock.NewRows([]string{
		"Seconds_Behind_Master", "Slave_IO_Running", "Slave_SQL_Running", "Last_Error",
	}).AddRow(30, "Yes", "Yes", "")

	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(rows)

	// Cancel context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := lm.WaitForLag(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context canceled")
}
