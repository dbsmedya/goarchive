// Package archiver provides resume/recovery functionality for GoArchive.
package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/dbsmedya/goarchive/internal/sqlutil"
)

// JobStatus represents the state of an archive job.
type JobStatus int

const (
	JobStatusIdle    JobStatus = 0
	JobStatusRunning JobStatus = 1
	JobStatusPaused  JobStatus = 2
	JobStatusFailed  JobStatus = 3
)

// LogStatus is the per-root-PK processing status, stored as TINYINT.
type LogStatus int8

const (
	LogStatusPending   LogStatus = 0
	LogStatusCopied    LogStatus = 1
	LogStatusCompleted LogStatus = 2
	LogStatusFailed    LogStatus = 3
)

const defaultResumeChunkSize = 1000

// String renders the status name for logs (e.g. "copied"), not the raw int.
func (s LogStatus) String() string {
	switch s {
	case LogStatusPending:
		return "pending"
	case LogStatusCopied:
		return "copied"
	case LogStatusCompleted:
		return "completed"
	case LogStatusFailed:
		return "failed"
	default:
		return fmt.Sprintf("LogStatus(%d)", int8(s))
	}
}

const (
	JobTypeArchive  = "archive"
	JobTypePurge    = "purge"
	JobTypeCopyOnly = "copy-only"
)

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

// ResumeManager handles job state persistence and crash recovery.
//
// Responsibilities:
// - Initialize the archiver_job table and the per-job archiver_job_log_<id> table
// - Track job checkpoints for resumption
// - Log per-PK processing status (pending/copied/completed/failed)
// - Detect interrupted jobs and resume from checkpoint
//
// GA-P3-F4: Resume Logging System
type ResumeManager struct {
	db        *sql.DB
	logger    *logger.Logger
	chunkSize int

	jobSchema string // resolved tracking schema (defaults to destination DB); never empty
	jobTable  string // quoted qualified name, e.g. `goarchive`.`archiver_job`
	jobID     int64  // resolved in GetOrCreateJobWithType
	logTable  string // quoted qualified name, e.g. `goarchive`.`archiver_job_log_42`; empty until resolved
}

// NewResumeManager creates a resume manager. jobSchema is the schema holding
// tracking tables (caller passes cfg.Destination.EffectiveJobSchema()).
func NewResumeManager(db *sql.DB, log *logger.Logger, jobSchema string) (*ResumeManager, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}
	if log == nil {
		log = logger.NewDefault()
	}
	if !sqlutil.IsValidIdentifier(jobSchema) {
		return nil, fmt.Errorf("invalid job_schema %q: must contain only alphanumeric characters and underscores", jobSchema)
	}
	return &ResumeManager{
		db:        db,
		logger:    log,
		jobSchema: jobSchema,
		jobTable:  sqlutil.QuoteIdentifier(jobSchema) + "." + sqlutil.QuoteIdentifier("archiver_job"),
	}, nil
}

// setJobID caches the resolved job id and derives the per-job log table name.
func (r *ResumeManager) setJobID(id int64) {
	r.jobID = id
	r.logTable = sqlutil.QuoteIdentifier(r.jobSchema) + "." +
		sqlutil.QuoteIdentifier("archiver_job_log_"+strconv.FormatInt(id, 10))
}

// LogTableName returns the fully-qualified per-job log table name (empty until
// GetOrCreateJobWithType has resolved the job id).
func (r *ResumeManager) LogTableName() string { return r.logTable }

// requireLogTable guards log-table methods against being called before the job
// id is resolved (would otherwise emit malformed SQL). The call graph never
// hits this, but a direct/out-of-order caller gets a clear error, not a crash.
func (r *ResumeManager) requireLogTable() error {
	if r.logTable == "" {
		return fmt.Errorf("per-job log table not resolved; call GetOrCreateJob(WithType) before log operations")
	}
	return nil
}

// SetChunkSize sets the multi-row statement chunk size for batch bookkeeping.
// Values <= 0 are ignored. Defaults to defaultResumeChunkSize.
func (r *ResumeManager) SetChunkSize(n int) {
	if n > 0 {
		r.chunkSize = n
	}
}

func (r *ResumeManager) effectiveChunkSize() int {
	if r.chunkSize > 0 {
		return r.chunkSize
	}
	return defaultResumeChunkSize
}

func (r *ResumeManager) createJobTableSQL() string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	id BIGINT AUTO_INCREMENT PRIMARY KEY,
	job_name VARCHAR(255) NOT NULL,
	root_table VARCHAR(255) NOT NULL,
	job_type VARCHAR(32) NOT NULL DEFAULT 'archive',
	last_processed_root_pk_id VARCHAR(255) DEFAULT NULL,
	job_status TINYINT NOT NULL DEFAULT 0,
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	last_heartbeat_at DATETIME NULL,
	UNIQUE KEY uk_job_name (job_name),
	INDEX idx_status (job_status),
	INDEX idx_updated (updated_at)
) ENGINE=InnoDB`, r.jobTable)
}

func (r *ResumeManager) createLogTableSQL() string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	id BIGINT AUTO_INCREMENT PRIMARY KEY,
	root_pk_id VARCHAR(255) NOT NULL,
	log_status TINYINT NOT NULL DEFAULT 0,
	error_message TEXT,
	UNIQUE KEY uk_pk (root_pk_id),
	INDEX idx_status (log_status)
) ENGINE=InnoDB`, r.logTable)
}

// checkLegacySchema rejects pre-1.2.x tracking tables (archiver_job without the
// new integer id PK). Replaces the old in-place ALTER migrations.
func (r *ResumeManager) checkLegacySchema(ctx context.Context) error {
	var tableCount int
	if err := r.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = 'archiver_job'",
		r.jobSchema,
	).Scan(&tableCount); err != nil {
		return fmt.Errorf("failed to probe for legacy archiver_job: %w", err)
	}
	if tableCount == 0 {
		return nil // fresh schema
	}
	// New-shape requires `id` to exist AND be the PRIMARY KEY. This single
	// COLUMN_KEY='PRI' check rejects BOTH "lacks id" (old job_name-PK shape)
	// and the partially-migrated "has id but job_name is still PK" shape
	// (id present but COLUMN_KEY != 'PRI').
	var idPKCount int
	if err := r.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = ? AND table_name = 'archiver_job' AND column_name = 'id' AND column_key = 'PRI'",
		r.jobSchema,
	).Scan(&idPKCount); err != nil {
		return fmt.Errorf("failed to probe legacy archiver_job columns: %w", err)
	}
	if idPKCount == 0 {
		return fmt.Errorf(
			"legacy GoArchive tracking tables detected in schema %q (archiver_job lacks the new 'id' column).\n"+
				"This release reshapes tracking tables and is not state-compatible with prior versions.\n"+
				"Drain in-flight jobs, then drop the old tables:\n"+
				"  DROP TABLE IF EXISTS `%s`.archiver_job_log;\n"+
				"  DROP TABLE IF EXISTS `%s`.archiver_job;\n"+
				"They are recreated automatically on next run.",
			r.jobSchema, r.jobSchema, r.jobSchema)
	}
	return nil
}

// InitializeTables creates the archiver_job table if it doesn't exist (and
// probes for legacy-shape tables). Per-job log tables are created lazily in
// GetOrCreateJobWithType, once the integer job id is known.
//
// This method is idempotent and safe to call on every startup.
//
// GA-P3-F4-T1: Create archiver_job table
func (r *ResumeManager) InitializeTables(ctx context.Context) error {
	r.logger.Debug("Initializing resume log tables")
	if err := r.checkLegacySchema(ctx); err != nil {
		return err
	}
	if _, err := r.db.ExecContext(ctx, r.createJobTableSQL()); err != nil {
		// Most likely cause when job_schema is isolated and the DBA hasn't
		// created it (MySQL ER 1049 "Unknown database"). Surface guidance —
		// this is the first place an absent schema is hit when preflight is
		// skipped via --skip-validate-preflight.
		return fmt.Errorf("failed to create archiver_job in schema %q (does the schema exist and does the account hold CREATE? a DBA must `CREATE DATABASE %s` and grant CREATE,SELECT,INSERT,UPDATE): %w", r.jobSchema, r.jobSchema, err)
	}
	r.logger.Info("Resume job table initialized")
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

	var state JobState
	var id int64
	var checkpoint sql.NullString
	err := r.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT id, job_name, root_table, job_type, last_processed_root_pk_id, job_status, created_at, updated_at FROM %s WHERE job_name = ?", r.jobTable),
		jobName,
	).Scan(&id, &state.JobName, &state.RootTable, &state.JobType, &checkpoint, &state.Status, &state.CreatedAt, &state.UpdatedAt)
	if checkpoint.Valid {
		state.LastProcessedRootPKID = checkpoint.String
	}

	if err == sql.ErrNoRows {
		r.logger.Infof("Creating new job %q for root table %q", jobName, rootTable)
		res, ierr := r.db.ExecContext(ctx,
			fmt.Sprintf("INSERT INTO %s (job_name, root_table, job_type, job_status) VALUES (?, ?, ?, ?)", r.jobTable),
			jobName, rootTable, jobType, JobStatusIdle,
		)
		if ierr != nil {
			return nil, fmt.Errorf("failed to create job: %w", ierr)
		}
		newID, lerr := res.LastInsertId()
		if lerr != nil {
			return nil, fmt.Errorf("failed to read new job id: %w", lerr)
		}
		r.setJobID(newID)
		if _, cerr := r.db.ExecContext(ctx, r.createLogTableSQL()); cerr != nil {
			return nil, fmt.Errorf("failed to create per-job log table: %w", cerr)
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

	r.setJobID(id)
	if _, cerr := r.db.ExecContext(ctx, r.createLogTableSQL()); cerr != nil {
		return nil, fmt.Errorf("failed to ensure per-job log table: %w", cerr)
	}

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
		fmt.Sprintf("UPDATE %s SET job_status = ?, updated_at = CURRENT_TIMESTAMP WHERE job_name = ?", r.jobTable),
		status, jobName,
	)
	if err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}

	r.logger.Debugf("Job %q status updated to %d", jobName, status)
	return nil
}

// Heartbeat updates the job heartbeat to the database server's current time.
func (r *ResumeManager) Heartbeat(ctx context.Context, jobName string) error {
	_, err := r.db.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET last_heartbeat_at = NOW() WHERE job_name = ?", r.jobTable), jobName)
	if err != nil {
		return fmt.Errorf("failed to update heartbeat for job %q: %w", jobName, err)
	}
	return nil
}

// IsHeartbeatStale reports whether a job row is missing, has no heartbeat, or is older than threshold.
func (r *ResumeManager) IsHeartbeatStale(ctx context.Context, jobName string, threshold time.Duration) (bool, time.Duration, error) {
	query := fmt.Sprintf("SELECT TIMESTAMPDIFF(SECOND, last_heartbeat_at, NOW()) FROM %s WHERE job_name = ?", r.jobTable)
	var ageSeconds sql.NullInt64
	err := r.db.QueryRowContext(ctx, query, jobName).Scan(&ageSeconds)
	if err == sql.ErrNoRows {
		return true, 0, nil
	}
	if err != nil {
		return false, 0, fmt.Errorf("failed to query heartbeat for job %q: %w", jobName, err)
	}
	if !ageSeconds.Valid {
		return true, 0, nil
	}
	age := time.Duration(ageSeconds.Int64) * time.Second
	return age > threshold, age, nil
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
		fmt.Sprintf("UPDATE %s SET last_processed_root_pk_id = ?, updated_at = CURRENT_TIMESTAMP WHERE job_name = ?", r.jobTable),
		pk, jobName,
	)
	if err != nil {
		return fmt.Errorf("failed to update checkpoint: %w", err)
	}

	r.logger.Debugf("Job %q checkpoint updated to PK=%q", jobName, pk)
	return nil
}

// LogBatchPending inserts 'pending' log entries for a batch of root PKs using
// chunked multi-row INSERT IGNORE (idempotent on retry).
// jobName is informational only (used for log messages); query scoping is via the resolved per-job log table.
//
// GA-P3-F4-T3: Insert pending log entries
func (r *ResumeManager) LogBatchPending(ctx context.Context, jobName string, rootPKs []interface{}) error {
	if len(rootPKs) == 0 {
		return nil
	}
	if err := r.requireLogTable(); err != nil {
		return err
	}
	chunk := r.effectiveChunkSize()
	for start := 0; start < len(rootPKs); start += chunk {
		end := start + chunk
		if end > len(rootPKs) {
			end = len(rootPKs)
		}
		group := rootPKs[start:end]

		tuples := make([]string, len(group))
		args := make([]interface{}, 0, len(group)*2)
		for i, pk := range group {
			pkID, err := formatPK(pk)
			if err != nil {
				return fmt.Errorf("unsupported PK type %T: %w", pk, err)
			}
			tuples[i] = "(?, ?)"
			args = append(args, pkID, LogStatusPending)
		}
		query := fmt.Sprintf("INSERT IGNORE INTO %s (root_pk_id, log_status) VALUES %s",
			r.logTable, strings.Join(tuples, ", "))
		if _, err := r.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("failed to log pending batch: %w", err)
		}
	}
	r.logger.Debugf("Logged %d pending entries for job %q", len(rootPKs), jobName)
	return nil
}

// MarkBatchCopied transitions a batch's log rows pending -> copied, recording
// that copy (and verify, if enabled) succeeded and only deletion remains.
// jobName is informational only (used for log messages); query scoping is via the resolved per-job log table.
// Chunked multi-row UPDATE.
func (r *ResumeManager) MarkBatchCopied(ctx context.Context, jobName string, rootPKs []interface{}) error {
	if len(rootPKs) == 0 {
		return nil
	}
	if err := r.requireLogTable(); err != nil {
		return err
	}
	chunk := r.effectiveChunkSize()
	for start := 0; start < len(rootPKs); start += chunk {
		end := start + chunk
		if end > len(rootPKs) {
			end = len(rootPKs)
		}
		group := rootPKs[start:end]

		placeholders := make([]string, len(group))
		args := make([]interface{}, 0, len(group)+1)
		args = append(args, LogStatusCopied)
		for i, pk := range group {
			pkID, err := formatPK(pk)
			if err != nil {
				return fmt.Errorf("invalid copied PK: %w", err)
			}
			placeholders[i] = "?"
			args = append(args, pkID)
		}
		query := fmt.Sprintf("UPDATE %s SET log_status = ? WHERE root_pk_id IN (%s)",
			r.logTable, strings.Join(placeholders, ", "))
		if _, err := r.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("failed to mark batch copied: %w", err)
		}
	}
	return nil
}

// MarkCompleted updates a log entry to 'completed' status.
// jobName is informational only (used for log messages); query scoping is via the resolved per-job log table.
//
// GA-P3-F4-T4: Update to completed
func (r *ResumeManager) MarkCompleted(ctx context.Context, jobName string, rootPKID interface{}) error {
	if err := r.requireLogTable(); err != nil {
		return err
	}
	pk, err := formatPK(rootPKID)
	if err != nil {
		return fmt.Errorf("invalid completion PK: %w", err)
	}

	_, err = r.db.ExecContext(ctx,
		fmt.Sprintf("UPDATE %s SET log_status = ? WHERE root_pk_id = ?", r.logTable),
		LogStatusCompleted, pk,
	)
	if err != nil {
		return fmt.Errorf("failed to mark PK=%q completed: %w", pk, err)
	}

	r.logger.Debugf("Marked PK=%q completed for job %q", pk, jobName)
	return nil
}

// MarkFailed updates a log entry to 'failed' status with error message.
// jobName is informational only (used for log messages); query scoping is via the resolved per-job log table.
//
// GA-P3-F4-T5: Update to failed
func (r *ResumeManager) MarkFailed(ctx context.Context, jobName string, rootPKID interface{}, errorMsg string) error {
	if err := r.requireLogTable(); err != nil {
		return err
	}
	pk, err := formatPK(rootPKID)
	if err != nil {
		return fmt.Errorf("invalid failed PK: %w", err)
	}

	_, err = r.db.ExecContext(ctx,
		fmt.Sprintf("UPDATE %s SET log_status = ?, error_message = ? WHERE root_pk_id = ?", r.logTable),
		LogStatusFailed, errorMsg, pk,
	)
	if err != nil {
		return fmt.Errorf("failed to mark PK=%q failed: %w", pk, err)
	}

	r.logger.Warnf("Marked PK=%q failed for job %q: %s", pk, jobName, errorMsg)
	return nil
}

// CompleteBatch atomically marks all rootPKs 'completed' and, when checkpointPK
// is non-nil, advances the job checkpoint to it — all in one transaction (the
// T3 invariant: never "checkpoint advanced but rows still pending").
// jobName is informational only (used for log messages); log row scoping is via the resolved per-job log table.
//
// Pass checkpointPK == nil on the resume/replay path so the main checkpoint is
// not derived from a (lexicographically-ordered) pending list.
func (r *ResumeManager) CompleteBatch(ctx context.Context, jobName string, rootPKs []interface{}, checkpointPK interface{}) error {
	if len(rootPKs) == 0 && checkpointPK == nil {
		return nil
	}
	if err := r.requireLogTable(); err != nil {
		return err
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin completion tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			if rbErr := tx.Rollback(); rbErr != nil {
				r.logger.Warnf("Failed to rollback completion tx: %v", rbErr)
			}
		}
	}()

	chunk := r.effectiveChunkSize()
	for start := 0; start < len(rootPKs); start += chunk {
		end := start + chunk
		if end > len(rootPKs) {
			end = len(rootPKs)
		}
		group := rootPKs[start:end]

		placeholders := make([]string, len(group))
		args := make([]interface{}, 0, len(group)+1)
		args = append(args, LogStatusCompleted)
		for i, pk := range group {
			pkID, err := formatPK(pk)
			if err != nil {
				return fmt.Errorf("invalid completion PK: %w", err)
			}
			placeholders[i] = "?"
			args = append(args, pkID)
		}
		query := fmt.Sprintf("UPDATE %s SET log_status = ? WHERE root_pk_id IN (%s)",
			r.logTable, strings.Join(placeholders, ", "))
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("failed to mark batch completed: %w", err)
		}
	}

	if checkpointPK != nil {
		pk, err := formatPK(checkpointPK)
		if err != nil {
			return fmt.Errorf("invalid checkpoint PK: %w", err)
		}
		if _, err := tx.ExecContext(ctx,
			fmt.Sprintf("UPDATE %s SET last_processed_root_pk_id = ?, updated_at = CURRENT_TIMESTAMP WHERE job_name = ?", r.jobTable),
			pk, jobName,
		); err != nil {
			return fmt.Errorf("failed to update checkpoint: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit completion tx: %w", err)
	}
	committed = true
	return nil
}

// GetRootPKsByStatus returns root_pk_ids for a job filtered by log_status,
// ordered lexicographically by the VARCHAR column (callers re-sort numerically).
// jobName is informational only (used for log messages); query scoping is via the resolved per-job log table.
func (r *ResumeManager) GetRootPKsByStatus(ctx context.Context, jobName string, status LogStatus) ([]string, error) {
	if err := r.requireLogTable(); err != nil {
		return nil, err
	}
	rows, err := r.db.QueryContext(ctx,
		fmt.Sprintf("SELECT root_pk_id FROM %s WHERE log_status = ? ORDER BY root_pk_id ASC", r.logTable),
		status,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query status %s PKs: %w", status, err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			r.logger.Warnf("Failed to close rows: %v", cerr)
		}
	}()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan status %s PK: %w", status, err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating status %s PKs: %w", status, err)
	}
	return ids, nil
}

// GetPendingPKs retrieves all pending root PKs for a job (for reprocessing).
//
// GA-P3-F4-T9: Reprocess pending
func (r *ResumeManager) GetPendingPKs(ctx context.Context, jobName string) ([]string, error) {
	pks, err := r.GetRootPKsByStatus(ctx, jobName, LogStatusPending)
	if err != nil {
		return nil, err
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
		fmt.Sprintf("SELECT last_processed_root_pk_id FROM %s WHERE job_name = ?", r.jobTable),
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
		fmt.Sprintf("SELECT last_processed_root_pk_id FROM %s WHERE job_name = ?", r.jobTable),
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

	// Check for non-terminal entries (pending OR copied) left by a crash.
	if err := r.requireLogTable(); err != nil {
		return false, err
	}
	nonTerminal := 0
	err = r.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE log_status IN (?, ?)", r.logTable),
		LogStatusPending, LogStatusCopied,
	).Scan(&nonTerminal)
	if err != nil {
		return false, fmt.Errorf("failed to count non-terminal entries: %w", err)
	}
	if nonTerminal > 0 {
		r.logger.Infof("Job %q has %d non-terminal entries, reprocessing required", jobName, nonTerminal)
		return true, nil
	}

	r.logger.Infof("Job %q has no pending work, starting fresh", jobName)
	return false, nil
}

// GetStats returns per-status counts for the manager's current job. jobName is
// accepted for API symmetry but is not used: the resolved per-job log table
// (r.logTable) already scopes results to this job.
// NOTE: not yet surfaced in any run summary; currently exercised only by tests.
func (r *ResumeManager) GetStats(ctx context.Context, jobName string) (pending, copied, completed, failed int, err error) {
	if err := r.requireLogTable(); err != nil {
		return 0, 0, 0, 0, err
	}
	rows, err := r.db.QueryContext(ctx,
		fmt.Sprintf("SELECT log_status, COUNT(*) FROM %s GROUP BY log_status", r.logTable),
	)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("failed to get stats: %w", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			r.logger.Warnf("Failed to close rows: %v", cerr)
		}
	}()

	for rows.Next() {
		var status LogStatus
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return 0, 0, 0, 0, fmt.Errorf("failed to scan stats: %w", err)
		}
		switch status {
		case LogStatusPending:
			pending = count
		case LogStatusCopied:
			copied = count
		case LogStatusCompleted:
			completed = count
		case LogStatusFailed:
			failed = count
		}
	}
	return pending, copied, completed, failed, rows.Err()
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
