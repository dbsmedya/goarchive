package archiver

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestCheckSameRootConcurrency(t *testing.T) {
	t.Run("fresh heartbeat blocks", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer func() { _ = db.Close() }()
		mock.ExpectQuery("SELECT job_name, TIMESTAMPDIFF").
			WithArgs("users", JobStatusRunning, "myjob").
			WillReturnRows(sqlmock.NewRows([]string{"job_name", "age_seconds"}).AddRow("other_job", int64(5)))
		err := CheckSameRootConcurrency(context.Background(), db, "users", "myjob", "archive")
		if err == nil || !strings.Contains(err.Error(), "live") {
			t.Fatalf("expected live conflict, got %v", err)
		}
	})

	t.Run("stale heartbeat blocks with stale message", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer func() { _ = db.Close() }()
		mock.ExpectQuery("SELECT job_name, TIMESTAMPDIFF").
			WithArgs("users", JobStatusRunning, "myjob").
			WillReturnRows(sqlmock.NewRows([]string{"job_name", "age_seconds"}).AddRow("dead_job", int64(120)))
		err := CheckSameRootConcurrency(context.Background(), db, "users", "myjob", "archive")
		if err == nil || !strings.Contains(err.Error(), "stale") {
			t.Fatalf("expected stale conflict, got %v", err)
		}
	})

	t.Run("no conflict passes", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer func() { _ = db.Close() }()
		mock.ExpectQuery("SELECT job_name, TIMESTAMPDIFF").
			WithArgs("users", JobStatusRunning, "myjob").
			WillReturnRows(sqlmock.NewRows([]string{"job_name", "age_seconds"}))
		if err := CheckSameRootConcurrency(context.Background(), db, "users", "myjob", "archive"); err != nil {
			t.Fatalf("expected no conflict, got %v", err)
		}
	})
}
