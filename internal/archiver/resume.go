// Package archiver provides resume/recovery functionality for GoArchive.
package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
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

const (
	JobTypeArchive  = "archive"
	JobTypePurge    = "purge"
	JobTypeCopyOnly = "copy-only"
)

// GA-P3-F4-T1: Create archiver_job table
const createJobTableSQL = `
CREATE TABLE IF NOT EXISTS archiver_job (
	job_name VARCHAR(255) PRIMARY KEY,
	root_table VARCHAR(255) NOT NULL,
	job_type VARCHAR(32) NOT NULL DEFAULT 'archive',
	last_processed_root_pk_id VARCHAR(255) DEFAULT NULL,
	job_status TINYINT NOT NULL DEFAULT 0,
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	INDEX idx_status (job_status),
	INDEX idx_updated (updated_at)
) ENGINE=InnoDB;
`

const checkJobTypeColumnSQL = `
SELECT COUNT(*) FROM information_schema.columns 
WHERE table_schema = DATABASE() 
AND table_name = 'archiver_job' 
AND column_name = 'job_type'
`

const addJobTypeColumnSQL = `
ALTER TABLE archiver_job
ADD COLUMN job_type VARCHAR(32) NOT NULL DEFAULT 'archive'
`

// GA-P3-F4-T2: Create archiver_job_log table
const createJobLogTableSQL = `
CREATE TABLE IF NOT EXISTS archiver_job_log (
	id BIGINT AUTO_INCREMENT PRIMARY KEY,
	job_name VARCHAR(255) NOT NULL,
	root_pk_id VARCHAR(255) NOT NULL,
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

const checkCheckpointColumnTypeSQL = `
SELECT DATA_TYPE FROM information_schema.columns
WHERE table_schema = DATABASE()
AND table_name = 'archiver_job'
AND column_name = 'last_processed_root_pk_id'
`

const alterCheckpointColumnTypeSQL = `
ALTER TABLE archiver_job
MODIFY COLUMN last_processed_root_pk_id VARCHAR(255) DEFAULT NULL
`

const checkLogPKColumnTypeSQL = `
SELECT DATA_TYPE FROM information_schema.columns
WHERE table_schema = DATABASE()
AND table_name = 'archiver_job_log'
AND column_name = 'root_pk_id'
`

const alterLogPKColumnTypeSQL = `
ALTER TABLE archiver_job_log
MODIFY COLUMN root_pk_id VARCHAR(255) NOT NULL
`

// JobState represents a job's current state.
//
// GA-P3-F4-T6: Checkpoint query support
// GA-P3-F4-T7: Resume detection
type JobState struct {
	JobName               string
	RootTable             string
	JobType               string
	LastProcessedRootPKID string
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
	RootPKID     string
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

	// Check if job_type column exists, add if not
	var columnExists int
	err := r.db.QueryRowContext(ctx, checkJobTypeColumnSQL).Scan(&columnExists)
	if err != nil {
		return fmt.Errorf("failed to check job_type column: %w", err)
	}
	if columnExists == 0 {
		if _, err := r.db.ExecContext(ctx, addJobTypeColumnSQL); err != nil {
			return fmt.Errorf("failed to migrate archiver_job.job_type: %w", err)
		}
		r.logger.Debug("Added job_type column to archiver_job table")
	}

	var checkpointType string
	if err := r.db.QueryRowContext(ctx, checkCheckpointColumnTypeSQL).Scan(&checkpointType); err != nil {
		return fmt.Errorf("failed to check checkpoint column type: %w", err)
	}
	if checkpointType != "varchar" {
		if _, err := r.db.ExecContext(ctx, alterCheckpointColumnTypeSQL); err != nil {
			return fmt.Errorf("failed to migrate checkpoint column type: %w", err)
		}
	}

	// GA-P3-F4-T2: Create archiver_job_log table (idempotent)
	if _, err := r.db.ExecContext(ctx, createJobLogTableSQL); err != nil {
		return fmt.Errorf("failed to create archiver_job_log table: %w", err)
	}

	var logPKType string
	if err := r.db.QueryRowContext(ctx, checkLogPKColumnTypeSQL).Scan(&logPKType); err != nil {
		return fmt.Errorf("failed to check log root_pk_id column type: %w", err)
	}
	if logPKType != "varchar" {
		if _, err := r.db.ExecContext(ctx, alterLogPKColumnTypeSQL); err != nil {
			return fmt.Errorf("failed to migrate log root_pk_id column type: %w", err)
		}
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
	return r.GetOrCreateJobWithType(ctx, jobName, rootTable, JobTypeArchive)
}

// GetOrCreateJobWithType retrieves an existing job or creates a new one with expected job type.
func (r *ResumeManager) GetOrCreateJobWithType(ctx context.Context, jobName, rootTable, jobType string) (*JobState, error) {
	if jobType == "" {
		jobType = JobTypeArchive
	}

	// Try to get existing job
	var state JobState
	err := r.db.QueryRowContext(ctx,
		"SELECT job_name, root_table, job_type, last_processed_root_pk_id, job_status, created_at, updated_at FROM archiver_job WHERE job_name = ?",
		jobName,
	).Scan(&state.JobName, &state.RootTable, &state.JobType, &state.LastProcessedRootPKID, &state.Status, &state.CreatedAt, &state.UpdatedAt)

	if err == sql.ErrNoRows {
		// GA-P3-F4-T7: No existing job - create new one
		r.logger.Infof("Creating new job %q for root table %q", jobName, rootTable)
		_, err = r.db.ExecContext(ctx,
			"INSERT INTO archiver_job (job_name, root_table, job_type, job_status) VALUES (?, ?, ?, ?)",
			jobName, rootTable, jobType, JobStatusIdle,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create job: %w", err)
		}

		return &JobState{
			JobName:               jobName,
			RootTable:             rootTable,
			JobType:               jobType,
			LastProcessedRootPKID: "",
			Status:                JobStatusIdle,
		}, nil
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}
	if state.JobType == "" {
		state.JobType = JobTypeArchive
	}
	if state.JobType != jobType {
		return nil, fmt.Errorf("job %q exists with type %q, expected %q", jobName, state.JobType, jobType)
	}

	// GA-P3-F4-T7: Existing job found - resume detection
	if state.LastProcessedRootPKID != "" {
		r.logger.Infof("Resuming job %q from checkpoint PK=%q", jobName, state.LastProcessedRootPKID)
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
func (r *ResumeManager) UpdateCheckpoint(ctx context.Context, jobName string, lastPKID interface{}) error {
	pk, err := formatPK(lastPKID)
	if err != nil {
		return fmt.Errorf("invalid checkpoint PK: %w", err)
	}

	_, err = r.db.ExecContext(ctx,
		"UPDATE archiver_job SET last_processed_root_pk_id = ?, updated_at = CURRENT_TIMESTAMP WHERE job_name = ?",
		pk, jobName,
	)
	if err != nil {
		return fmt.Errorf("failed to update checkpoint: %w", err)
	}

	r.logger.Debugf("Job %q checkpoint updated to PK=%q", jobName, pk)
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
		pkID, err := formatPK(pk)
		if err != nil {
			return fmt.Errorf("unsupported PK type %T: %w", pk, err)
		}

		if _, err := stmt.ExecContext(ctx, jobName, pkID, LogStatusPending); err != nil {
			return fmt.Errorf("failed to log pending PK=%q: %w", pkID, err)
		}
	}

	r.logger.Debugf("Logged %d pending entries for job %q", len(rootPKs), jobName)
	return nil
}

// MarkCompleted updates a log entry to 'completed' status.
//
// GA-P3-F4-T4: Update to completed
func (r *ResumeManager) MarkCompleted(ctx context.Context, jobName string, rootPKID interface{}) error {
	pk, err := formatPK(rootPKID)
	if err != nil {
		return fmt.Errorf("invalid completion PK: %w", err)
	}

	_, err = r.db.ExecContext(ctx,
		"UPDATE archiver_job_log SET log_status = ?, updated_at = CURRENT_TIMESTAMP WHERE job_name = ? AND root_pk_id = ?",
		LogStatusCompleted, jobName, pk,
	)
	if err != nil {
		return fmt.Errorf("failed to mark PK=%q completed: %w", pk, err)
	}

	r.logger.Debugf("Marked PK=%q completed for job %q", pk, jobName)
	return nil
}

// MarkFailed updates a log entry to 'failed' status with error message.
//
// GA-P3-F4-T5: Update to failed
func (r *ResumeManager) MarkFailed(ctx context.Context, jobName string, rootPKID interface{}, errorMsg string) error {
	pk, err := formatPK(rootPKID)
	if err != nil {
		return fmt.Errorf("invalid failed PK: %w", err)
	}

	_, err = r.db.ExecContext(ctx,
		"UPDATE archiver_job_log SET log_status = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP WHERE job_name = ? AND root_pk_id = ?",
		LogStatusFailed, errorMsg, jobName, pk,
	)
	if err != nil {
		return fmt.Errorf("failed to mark PK=%q failed: %w", pk, err)
	}

	r.logger.Warnf("Marked PK=%q failed for job %q: %s", pk, jobName, errorMsg)
	return nil
}

// GetPendingPKs retrieves all pending root PKs for a job (for reprocessing).
//
// GA-P3-F4-T9: Reprocess pending
func (r *ResumeManager) GetPendingPKs(ctx context.Context, jobName string) ([]string, error) {
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

	var pks []string
	for rows.Next() {
		var pk string
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
func (r *ResumeManager) GetCheckpoint(ctx context.Context, jobName string) (string, error) {
	var checkpoint sql.NullString
	err := r.db.QueryRowContext(ctx,
		"SELECT last_processed_root_pk_id FROM archiver_job WHERE job_name = ?",
		jobName,
	).Scan(&checkpoint)

	if err == sql.ErrNoRows {
		// No job exists - start from 0
		return "", nil
	}

	if err != nil {
		return "", fmt.Errorf("failed to get checkpoint: %w", err)
	}

	return checkpoint.String, nil
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
	var checkpoint sql.NullString
	err := r.db.QueryRowContext(ctx,
		"SELECT last_processed_root_pk_id FROM archiver_job WHERE job_name = ?",
		jobName,
	).Scan(&checkpoint)
	if err == sql.ErrNoRows {
		r.logger.Infof("Job %q has no existing resume metadata, starting fresh", jobName)
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to get job state for resume check: %w", err)
	}

	// Check for checkpoint
	if checkpoint.Valid && checkpoint.String != "" {
		r.logger.Infof("Job %q has checkpoint PK=%q, resumption required", jobName, checkpoint.String)
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

func formatPK(pk interface{}) (string, error) {
	switch v := pk.(type) {
	case nil:
		return "", fmt.Errorf("pk is nil")
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case int:
		return strconv.Itoa(v), nil
	case int8:
		return strconv.FormatInt(int64(v), 10), nil
	case int16:
		return strconv.FormatInt(int64(v), 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case fmt.Stringer:
		return v.String(), nil
	default:
		return "", fmt.Errorf("unsupported pk type: %T", pk)
	}
}
