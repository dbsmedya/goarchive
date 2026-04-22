package cmd

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/internal/archiver"
)

// mysqlErrTableNotFound is the error code MySQL returns when SELECT targets a
// table that does not exist (ER_NO_SUCH_TABLE).
const mysqlErrTableNotFound = 1146

func checkConcurrentJobsByRootTable(ctx context.Context, db *sql.DB, rootTable, currentJob, commandName string) error {
	if db == nil {
		return fmt.Errorf("destination database is nil")
	}

	const query = `
		SELECT job_name FROM archiver_job
		WHERE root_table = ?
		AND job_status = ?
		AND job_name != ?
	`

	rows, err := db.QueryContext(ctx, query, rootTable, archiver.JobStatusRunning, currentJob)
	if err != nil {
		// On a fresh destination, archiver_job is created lazily by the
		// orchestrator's InitializeTables call. If it does not exist yet there
		// cannot be any concurrent jobs, so treat ER_NO_SUCH_TABLE as "no
		// conflicts" rather than a hard failure.
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrTableNotFound {
			return nil
		}
		return fmt.Errorf("failed to query concurrent jobs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var conflicts []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("failed to scan concurrent job: %w", err)
		}
		conflicts = append(conflicts, name)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate concurrent jobs: %w", err)
	}
	if len(conflicts) > 0 {
		return fmt.Errorf("cannot run %s: job(s) already running on root_table %q: %v", commandName, rootTable, conflicts)
	}

	return nil
}
