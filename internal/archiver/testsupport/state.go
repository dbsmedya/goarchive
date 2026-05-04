//go:build integration
// +build integration

package testsupport

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// CleanupArchiverState wipes resume metadata for a job as integration-test cleanup.
func CleanupArchiverState(t *testing.T, db *sql.DB, jobName string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := db.ExecContext(ctx, "DELETE FROM archiver_job_log WHERE job_name = ?", jobName); err != nil {
			t.Logf("CleanupArchiverState archiver_job_log: %v", err)
		}
		if _, err := db.ExecContext(ctx, "DELETE FROM archiver_job WHERE job_name = ?", jobName); err != nil {
			t.Logf("CleanupArchiverState archiver_job: %v", err)
		}
	})
}
