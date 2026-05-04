package archiver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	mysql "github.com/go-sql-driver/mysql"
)

const concurrencyStaleThresholdSeconds = 60
const mysqlErrTableNotFound = 1146

// CheckSameRootConcurrency blocks when another Running job exists on the same root table.
func CheckSameRootConcurrency(ctx context.Context, db *sql.DB, rootTable, currentJob, commandName string) error {
	if db == nil {
		return fmt.Errorf("destination database is nil")
	}
	const query = `
		SELECT job_name, TIMESTAMPDIFF(SECOND, last_heartbeat_at, NOW())
		FROM archiver_job
		WHERE root_table = ?
		  AND job_status = ?
		  AND job_name != ?
	`
	rows, err := db.QueryContext(ctx, query, rootTable, JobStatusRunning, currentJob)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrTableNotFound {
			return nil
		}
		return fmt.Errorf("failed to query concurrent jobs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	type conflict struct {
		jobName string
		stale   bool
		ageSec  int64
	}
	var conflicts []conflict
	for rows.Next() {
		var name string
		var age sql.NullInt64
		if err := rows.Scan(&name, &age); err != nil {
			return fmt.Errorf("failed to scan concurrent job: %w", err)
		}
		ageSec := int64(0)
		if age.Valid {
			ageSec = age.Int64
		}
		conflicts = append(conflicts, conflict{
			jobName: name,
			stale:   !age.Valid || age.Int64 > concurrencyStaleThresholdSeconds,
			ageSec:  ageSec,
		})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate concurrent jobs: %w", err)
	}
	if len(conflicts) == 0 {
		return nil
	}

	var live, stale []conflict
	for _, c := range conflicts {
		if c.stale {
			stale = append(stale, c)
		} else {
			live = append(live, c)
		}
	}
	if len(live) > 0 {
		names := make([]string, len(live))
		for i, c := range live {
			names[i] = c.jobName
		}
		return fmt.Errorf("cannot run %s: live job(s) running on root_table %q: %v", commandName, rootTable, names)
	}
	names := make([]string, len(stale))
	for i, c := range stale {
		names[i] = fmt.Sprintf("%s (heartbeat %ds ago)", c.jobName, c.ageSec)
	}
	return fmt.Errorf(
		"cannot run %s on root_table %q: stale running job(s) detected: %v. "+
			"This indicates a prior crashed run. Manually inspect and clear with:\n"+
			"  UPDATE archiver_job SET job_status = 0 WHERE job_name = '<name>';\n"+
			"(0 = JobStatusIdle. See JobStatus constants in internal/archiver/resume.go.)",
		commandName, rootTable, names)
}
