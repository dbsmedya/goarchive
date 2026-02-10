// Package archiver provides resume/recovery functionality for GoArchive.
package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/dbsmedya/goarchive/internal/logger"
)

// JobStatus represents the state of an archive job.
type JobStatus int

const (
	JobStatusIdle    JobStatus = 0
	JobStatusRunning JobStatus = 1
	JobStatusPaused  JobStatus = 2
	JobStatusFailed  JobStatus = 3
)

// LogStatus represents the processing status of a single root PK.
type LogStatus string

const (
	LogStatusPending   LogStatus = "pending"
	LogStatusCompleted LogStatus = "completed"
	LogStatusFailed    LogStatus = "failed"
)

// GA-P3-F4-T1: Create archiver_job table
const createJobTableSQL = `
CREATE TABLE IF NOT EXISTS archiver_job (
	job_name VARCHAR(255) PRIMARY KEY,
	root_table VARCHAR(255) NOT NULL,
	last_processed_root_pk_id BIGINT DEFAULT 0,
	job_status TINYINT NOT NULL DEFAULT 0,
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	INDEX idx_status (job_status),
	INDEX idx_updated (updated_at)
) ENGINE=InnoDB;
`

// GA-P3-F4-T2: Create archiver_job_log table
const createJobLogTableSQL = `
CREATE TABLE IF NOT EXISTS archiver_job_log (
	id BIGINT AUTO_INCREMENT PRIMARY KEY,
	job_name VARCHAR(255) NOT NULL,
	root_pk_id BIGINT NOT NULL,
	log_status VARCHAR(20) NOT NULL DEFAULT 'pending',
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	error_message TEXT,
	UNIQUE KEY uk_job_pk (job_name, root_pk_id),
	INDEX idx_job_status (job_name, log_status),
	INDEX idx_status (log_status),
	FOREIGN KEY (job_name) REFERENCES archiver_job(job_name) ON DELETE CASCADE
) ENGINE=InnoDB;
`

// JobState represents a job's current state.
//
// GA-P3-F4-T6: Checkpoint query support
// GA-P3-F4-T7: Resume detection
type JobState struct {
	JobName               string
	RootTable             string
	LastProcessedRootPKID int64
	Status                JobStatus
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

// LogEntry represents a single root PK processing log entry.
//
// GA-P3-F4-T3: Insert pending log entries
// GA-P3-F4-T4: Update to completed
// GA-P3-F4-T5: Update to failed
type LogEntry struct {
	ID           int64
	JobName      string
	RootPKID     int64
	LogStatus    LogStatus
	CreatedAt    time.Time
	UpdatedAt    time.Time
	ErrorMessage string
}

// ResumeManager handles job state persistence and crash recovery.
//
// Responsibilities:
// - Initialize resume log tables (archiver_job, archiver_job_log)
// - Track job checkpoints for resumption
// - Log per-PK processing status (pending/completed/failed)
// - Detect interrupted jobs and resume from checkpoint
//
// GA-P3-F4: Resume Logging System
type ResumeManager struct {
	db     *sql.DB
	logger *logger.Logger
}

// NewResumeManager creates a new resume manager for job state tracking.
func NewResumeManager(db *sql.DB, log *logger.Logger) (*ResumeManager, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}
	if log == nil {
		log = logger.NewDefault()
	}

	return &ResumeManager{
		db:     db,
		logger: log,
	}, nil
}

// InitializeTables creates resume log tables if they don't exist.
//
// This method is idempotent and safe to call on every startup.
//
// GA-P3-F4-T1: Create archiver_job table
// GA-P3-F4-T2: Create archiver_job_log table
func (r *ResumeManager) InitializeTables(ctx context.Context) error {
	r.logger.Debug("Initializing resume log tables")

	// GA-P3-F4-T1: Create archiver_job table (idempotent)
	if _, err := r.db.ExecContext(ctx, createJobTableSQL); err != nil {
		return fmt.Errorf("failed to create archiver_job table: %w", err)
	}

	// GA-P3-F4-T2: Create archiver_job_log table (idempotent)
	if _, err := r.db.ExecContext(ctx, createJobLogTableSQL); err != nil {
		return fmt.Errorf("failed to create archiver_job_log table: %w", err)
	}

	r.logger.Info("Resume log tables initialized")
	return nil
}

// GetOrCreateJob retrieves an existing job or creates a new one.
//
// If the job exists and has a checkpoint, it indicates resumption capability.
// If the job is new, it starts with checkpoint 0.
//
// GA-P3-F4-T6: Checkpoint query
// GA-P3-F4-T7: Resume detection
func (r *ResumeManager) GetOrCreateJob(ctx context.Context, jobName, rootTable string) (*JobState, error) {
	// Try to get existing job
	var state JobState
	err := r.db.QueryRowContext(ctx,
		"SELECT job_name, root_table, last_processed_root_pk_id, job_status, created_at, updated_at FROM archiver_job WHERE job_name = ?",
		jobName,
	).Scan(&state.JobName, &state.RootTable, &state.LastProcessedRootPKID, &state.Status, &state.CreatedAt, &state.UpdatedAt)

	if err == sql.ErrNoRows {
		// GA-P3-F4-T7: No existing job - create new one
		r.logger.Infof("Creating new job %q for root table %q", jobName, rootTable)
		_, err = r.db.ExecContext(ctx,
			"INSERT INTO archiver_job (job_name, root_table, job_status) VALUES (?, ?, ?)",
			jobName, rootTable, JobStatusIdle,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create job: %w", err)
		}

		return &JobState{
			JobName:               jobName,
			RootTable:             rootTable,
			LastProcessedRootPKID: 0,
			Status:                JobStatusIdle,
		}, nil
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	// GA-P3-F4-T7: Existing job found - resume detection
	if state.LastProcessedRootPKID > 0 {
		r.logger.Infof("Resuming job %q from checkpoint PK=%d", jobName, state.LastProcessedRootPKID)
	} else {
		r.logger.Infof("Job %q exists with no checkpoint (starting from beginning)", jobName)
	}

	return &state, nil
}

// UpdateJobStatus updates the job's status.
func (r *ResumeManager) UpdateJobStatus(ctx context.Context, jobName string, status JobStatus) error {
	_, err := r.db.ExecContext(ctx,
		"UPDATE archiver_job SET job_status = ?, updated_at = CURRENT_TIMESTAMP WHERE job_name = ?",
		status, jobName,
	)
	if err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}

	r.logger.Debugf("Job %q status updated to %d", jobName, status)
	return nil
}

// UpdateCheckpoint updates the last processed root PK for resumption.
//
// GA-P3-F4-T6: Checkpoint query
// GA-P3-F4-T8: Resume from checkpoint
func (r *ResumeManager) UpdateCheckpoint(ctx context.Context, jobName string, lastPKID int64) error {
	_, err := r.db.ExecContext(ctx,
		"UPDATE archiver_job SET last_processed_root_pk_id = ?, updated_at = CURRENT_TIMESTAMP WHERE job_name = ?",
		lastPKID, jobName,
	)
	if err != nil {
		return fmt.Errorf("failed to update checkpoint: %w", err)
	}

	r.logger.Debugf("Job %q checkpoint updated to PK=%d", jobName, lastPKID)
	return nil
}

// LogBatchPending inserts log entries for a batch of root PKs with status 'pending'.
//
// This is called before processing each batch to enable idempotent reprocessing
// on crash recovery.
//
// GA-P3-F4-T3: Insert pending log entries
func (r *ResumeManager) LogBatchPending(ctx context.Context, jobName string, rootPKs []interface{}) error {
	if len(rootPKs) == 0 {
		return nil
	}

	// Use INSERT IGNORE to make this idempotent (safe for retries)
	stmt, err := r.db.PrepareContext(ctx,
		"INSERT IGNORE INTO archiver_job_log (job_name, root_pk_id, log_status) VALUES (?, ?, ?)",
	)
	if err != nil {
		return fmt.Errorf("failed to prepare log insert: %w", err)
	}
	defer func() {
		if err := stmt.Close(); err != nil {
			r.logger.Warnf("Failed to close statement: %v", err)
		}
	}()

	// GA-P3-F4-T3: Insert pending entries for each root PK
	for _, pk := range rootPKs {
		// Convert interface{} to int64 (assuming numeric PKs)
		var pkID int64
		switch v := pk.(type) {
		case int64:
			pkID = v
		case int:
			pkID = int64(v)
		default:
			return fmt.Errorf("unsupported PK type: %T", pk)
		}

		if _, err := stmt.ExecContext(ctx, jobName, pkID, LogStatusPending); err != nil {
			return fmt.Errorf("failed to log pending PK=%d: %w", pkID, err)
		}
	}

	r.logger.Debugf("Logged %d pending entries for job %q", len(rootPKs), jobName)
	return nil
}

// MarkCompleted updates a log entry to 'completed' status.
//
// GA-P3-F4-T4: Update to completed
func (r *ResumeManager) MarkCompleted(ctx context.Context, jobName string, rootPKID int64) error {
	_, err := r.db.ExecContext(ctx,
		"UPDATE archiver_job_log SET log_status = ?, updated_at = CURRENT_TIMESTAMP WHERE job_name = ? AND root_pk_id = ?",
		LogStatusCompleted, jobName, rootPKID,
	)
	if err != nil {
		return fmt.Errorf("failed to mark PK=%d completed: %w", rootPKID, err)
	}

	r.logger.Debugf("Marked PK=%d completed for job %q", rootPKID, jobName)
	return nil
}

// MarkFailed updates a log entry to 'failed' status with error message.
//
// GA-P3-F4-T5: Update to failed
func (r *ResumeManager) MarkFailed(ctx context.Context, jobName string, rootPKID int64, errorMsg string) error {
	_, err := r.db.ExecContext(ctx,
		"UPDATE archiver_job_log SET log_status = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP WHERE job_name = ? AND root_pk_id = ?",
		LogStatusFailed, errorMsg, jobName, rootPKID,
	)
	if err != nil {
		return fmt.Errorf("failed to mark PK=%d failed: %w", rootPKID, err)
	}

	r.logger.Warnf("Marked PK=%d failed for job %q: %s", rootPKID, jobName, errorMsg)
	return nil
}

// GetPendingPKs retrieves all pending root PKs for a job (for reprocessing).
//
// GA-P3-F4-T9: Reprocess pending
func (r *ResumeManager) GetPendingPKs(ctx context.Context, jobName string) ([]int64, error) {
	rows, err := r.db.QueryContext(ctx,
		"SELECT root_pk_id FROM archiver_job_log WHERE job_name = ? AND log_status = ? ORDER BY root_pk_id ASC",
		jobName, LogStatusPending,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending PKs: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			r.logger.Warnf("Failed to close rows: %v", err)
		}
	}()

	var pks []int64
	for rows.Next() {
		var pk int64
		if err := rows.Scan(&pk); err != nil {
			return nil, fmt.Errorf("failed to scan pending PK: %w", err)
		}
		pks = append(pks, pk)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pending PKs: %w", err)
	}

	// GA-P3-F4-T9: Return pending PKs for reprocessing
	if len(pks) > 0 {
		r.logger.Infof("Found %d pending PKs for job %q (requires reprocessing)", len(pks), jobName)
	}

	return pks, nil
}

// GetCheckpoint retrieves the last processed root PK for a job.
//
// GA-P3-F4-T6: Checkpoint query
// GA-P3-F4-T8: Resume from checkpoint
func (r *ResumeManager) GetCheckpoint(ctx context.Context, jobName string) (int64, error) {
	var checkpoint int64
	err := r.db.QueryRowContext(ctx,
		"SELECT last_processed_root_pk_id FROM archiver_job WHERE job_name = ?",
		jobName,
	).Scan(&checkpoint)

	if err == sql.ErrNoRows {
		// No job exists - start from 0
		return 0, nil
	}

	if err != nil {
		return 0, fmt.Errorf("failed to get checkpoint: %w", err)
	}

	return checkpoint, nil
}

// ShouldResume checks if a job needs resumption (has pending work).
//
// A job should resume if:
// 1. It has a checkpoint > 0 (was interrupted mid-processing)
// 2. It has pending log entries (incomplete batches)
//
// GA-P3-F4-T7: Resume detection
// GA-P3-F4-T8: Resume from checkpoint
// GA-P3-F4-T9: Reprocess pending
func (r *ResumeManager) ShouldResume(ctx context.Context, jobName string) (bool, error) {
	state, err := r.GetOrCreateJob(ctx, jobName, "")
	if err != nil {
		return false, err
	}

	// Check for checkpoint
	if state.LastProcessedRootPKID > 0 {
		r.logger.Infof("Job %q has checkpoint PK=%d, resumption required", jobName, state.LastProcessedRootPKID)
		return true, nil
	}

	// Check for pending entries
	pendingCount := 0
	err = r.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM archiver_job_log WHERE job_name = ? AND log_status = ?",
		jobName, LogStatusPending,
	).Scan(&pendingCount)

	if err != nil {
		return false, fmt.Errorf("failed to count pending entries: %w", err)
	}

	if pendingCount > 0 {
		r.logger.Infof("Job %q has %d pending entries, reprocessing required", jobName, pendingCount)
		return true, nil
	}

	r.logger.Infof("Job %q has no pending work, starting fresh", jobName)
	return false, nil
}

// GetStats returns current job statistics.
func (r *ResumeManager) GetStats(ctx context.Context, jobName string) (pending, completed, failed int, err error) {
	rows, err := r.db.QueryContext(ctx,
		"SELECT log_status, COUNT(*) FROM archiver_job_log WHERE job_name = ? GROUP BY log_status",
		jobName,
	)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to get stats: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			r.logger.Warnf("Failed to close rows: %v", err)
		}
	}()

	for rows.Next() {
		var status LogStatus
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return 0, 0, 0, fmt.Errorf("failed to scan stats: %w", err)
		}

		switch status {
		case LogStatusPending:
			pending = count
		case LogStatusCompleted:
			completed = count
		case LogStatusFailed:
			failed = count
		}
	}

	return pending, completed, failed, rows.Err()
}

// SetLogger sets a custom logger for the resume manager.
func (r *ResumeManager) SetLogger(log *logger.Logger) {
	r.logger = log
}
