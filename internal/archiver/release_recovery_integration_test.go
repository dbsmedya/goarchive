//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"
)

// resetReleaseRecoveryData clears the deterministic graph child-first using
// DELETE. It deliberately avoids pooled session SET statements: a SET on one
// pooled connection does not govern DELETEs executed on another.
func resetReleaseRecoveryData(t *testing.T, sourceDB, destDB *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, db := range []*sql.DB{sourceDB, destDB} {
		for _, table := range []string{"order_payments", "order_items", "orders", "customers"} {
			if _, err := db.ExecContext(ctx, "DELETE FROM `"+table+"`"); err != nil {
				t.Fatalf("clear %s child-first: %v", table, err)
			}
		}
	}
	seedResumeScenarioData(t, sourceDB, resumeScenarioRoots...)
}

func assertRecoveryLogState(t *testing.T, destDB *sql.DB, logTable string, want map[string]LogStatus) {
	t.Helper()
	if got := readLogStatuses(t, destDB, logTable); !reflect.DeepEqual(got, want) {
		t.Fatalf("recovery log state = %v, want %v", got, want)
	}
}

// dropReleaseRecoverySecondaryUniqueIndexes scopes the supported loose-index
// fixture to one subtest and restores the customer fixture for repeated runs.
func dropReleaseRecoverySecondaryUniqueIndexes(t *testing.T, destDB *sql.DB, schema string) {
	t.Helper()
	dropDestinationSecondaryUniqueIndexes(t, destDB, schema)
	t.Cleanup(func() {
		if _, err := destDB.Exec("ALTER TABLE `customers` ADD UNIQUE INDEX `email` (`email`)"); err != nil {
			t.Errorf("restore destination customers.email unique index: %v", err)
		}
	})
}

func assertRecoveryTrackingGone(t *testing.T, observer *sql.DB, jobName, logTableName string) {
	t.Helper()
	var jobs int
	if err := observer.QueryRow("SELECT COUNT(*) FROM archiver_job WHERE job_name = ?", jobName).Scan(&jobs); err != nil {
		t.Fatalf("observe job cleanup: %v", err)
	}
	if jobs != 0 {
		t.Fatalf("job rows after cleanup = %d, want 0", jobs)
	}
	rows, err := observer.Query("SHOW TABLES LIKE '" + logTableName + "'")
	if err != nil {
		t.Fatalf("observe log-table cleanup: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		t.Fatalf("log table %q remains after cleanup", logTableName)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("observe log-table cleanup rows: %v", err)
	}
}

// TestRecoveryFixtureCleanup_Integration proves bootstrapJobTracking's
// registered cleanup is repeatable and runs while its database handles remain
// live. The observer is independent of both nested scenario lifetimes.
func TestRecoveryFixtureCleanup_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, _ := SetupIntegrationTest(t)
	t.Cleanup(setup.Close)
	_, destDB := resumeScenarioDBs(t, setup)
	destSchema := getDestSchema(setup)
	observer := getVerificationDB(t, setup, "destination")
	t.Cleanup(func() { _ = observer.Close() })
	const jobName = "release_recovery_cleanup"

	for run := 1; run <= 2; run++ {
		var logTableName string
		t.Run(fmt.Sprintf("run-%d", run), func(t *testing.T) {
			var before int
			if err := observer.QueryRow("SELECT COUNT(*) FROM archiver_job WHERE job_name = ?", jobName).Scan(&before); err != nil {
				t.Fatalf("observe clean start: %v", err)
			}
			if before != 0 {
				t.Fatalf("job rows at run start = %d, want 0", before)
			}

			logTable := bootstrapJobTracking(t, destDB, destSchema, jobName, "customers", JobTypeArchive)
			seedLogStatus(t, destDB, logTable, LogStatusPending, "1")
			var jobID int64
			if err := destDB.QueryRow("SELECT id FROM archiver_job WHERE job_name = ?", jobName).Scan(&jobID); err != nil {
				t.Fatalf("read cleanup job id: %v", err)
			}
			logTableName = fmt.Sprintf("archiver_job_log_%d", jobID)
		})

		assertRecoveryTrackingGone(t, observer, jobName, logTableName)
	}
}
