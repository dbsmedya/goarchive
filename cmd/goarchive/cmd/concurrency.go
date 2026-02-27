package cmd

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dbsmedya/goarchive/internal/archiver"
)

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
