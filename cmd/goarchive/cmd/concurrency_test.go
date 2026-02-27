package cmd

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/archiver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckConcurrentJobsByRootTable_NoConflicts(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	rows := sqlmock.NewRows([]string{"job_name"})
	mock.ExpectQuery("SELECT job_name FROM archiver_job").
		WithArgs("customers", archiver.JobStatusRunning, "job_a").
		WillReturnRows(rows)

	err = checkConcurrentJobsByRootTable(context.Background(), db, "customers", "job_a", "archive")
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCheckConcurrentJobsByRootTable_WithConflicts(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	rows := sqlmock.NewRows([]string{"job_name"}).AddRow("copy_job")
	mock.ExpectQuery("SELECT job_name FROM archiver_job").
		WithArgs("customers", archiver.JobStatusRunning, "job_a").
		WillReturnRows(rows)

	err = checkConcurrentJobsByRootTable(context.Background(), db, "customers", "job_a", "archive")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot run archive")
	assert.NoError(t, mock.ExpectationsWereMet())
}
