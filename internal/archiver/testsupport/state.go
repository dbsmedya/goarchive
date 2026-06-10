//go:build integration
// +build integration

package testsupport

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

// CleanupArchiverState wipes resume metadata for a job as integration-test cleanup.
func CleanupArchiverState(t *testing.T, db *sql.DB, jobName string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var id int64
		err := db.QueryRowContext(ctx, "SELECT id FROM archiver_job WHERE job_name = ?", jobName).Scan(&id)
		if err == nil {
			drop := fmt.Sprintf("DROP TABLE IF EXISTS `archiver_job_log_%d`", id)
			if _, derr := db.ExecContext(ctx, drop); derr != nil {
				t.Logf("CleanupArchiverState drop log table: %v", derr)
			}
		} else if err != sql.ErrNoRows {
			t.Logf("CleanupArchiverState resolve id: %v", err)
		}
		if _, err := db.ExecContext(ctx, "DELETE FROM archiver_job WHERE job_name = ?", jobName); err != nil {
			t.Logf("CleanupArchiverState archiver_job: %v", err)
		}
	})
}
