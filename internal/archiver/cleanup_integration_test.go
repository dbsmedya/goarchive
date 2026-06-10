//go:build integration
// +build integration

package archiver

import (
	"database/sql"
	"fmt"
	"testing"
)

// dropAllJobLogTables drops archiver_job_log_<id> for every row in archiver_job.
// Best-effort teardown: all errors are logged, never fatal.
func dropAllJobLogTables(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.Query("SELECT id FROM archiver_job")
	if err != nil {
		t.Logf("dropAllJobLogTables: query error: %v", err)
		return
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if serr := rows.Scan(&id); serr != nil {
			t.Logf("dropAllJobLogTables: scan error: %v", serr)
			continue
		}
		ids = append(ids, id)
	}
	if rerr := rows.Err(); rerr != nil {
		t.Logf("dropAllJobLogTables: row iteration error: %v", rerr)
	}
	_ = rows.Close()
	for _, id := range ids {
		if _, derr := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `archiver_job_log_%d`", id)); derr != nil {
			t.Logf("dropAllJobLogTables: drop %d: %v", id, derr)
		}
	}
}
