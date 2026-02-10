// Package lock provides MySQL advisory locking functionality for GoArchive.
package lock

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrLockTimeout is returned when lock acquisition times out because
// another instance is holding the lock.
var ErrLockTimeout = errors.New("lock acquisition timed out")

// Common timeout values for lock acquisition (in seconds).
const (
	// TimeoutImmediate returns immediately if lock cannot be acquired (no wait).
	TimeoutImmediate = 0

	// TimeoutShort is suitable for fast-failing duplicate job detection.
	// Use this when you want to quickly determine if another instance is running.
	TimeoutShort = 1

	// TimeoutMedium provides a reasonable wait for transient conflicts.
	// Use this for typical job coordination scenarios.
	TimeoutMedium = 10

	// TimeoutLong allows extended waiting for lock acquisition.
	// Use this when you want to queue behind a running job.
	TimeoutLong = 60

	// TimeoutInfinite waits indefinitely until the lock is acquired.
	// Note: MySQL treats negative values as infinite wait.
	TimeoutInfinite = -1
)

// AdvisoryLock represents a MySQL advisory lock for preventing concurrent job execution.
// It uses MySQL's GET_LOCK() function to acquire a named lock that is automatically
// released when the connection closes or RELEASE_LOCK() is called.
type AdvisoryLock struct {
	db       *sql.DB
	lockName string
	held     bool
}

// NewAdvisoryLock creates a new advisory lock with the given name.
// The lock is not acquired until AcquireLock is called.
func NewAdvisoryLock(db *sql.DB, lockName string) *AdvisoryLock {
	return &AdvisoryLock{
		db:       db,
		lockName: lockName,
		held:     false,
	}
}

// AcquireLock attempts to acquire the advisory lock with the specified timeout.
// Returns true if the lock was acquired, false if timeout was reached.
// Returns an error if the database query fails.
//
// MySQL GET_LOCK() return values:
//   - 1: Lock was obtained successfully
//   - 0: Timeout was reached without obtaining the lock
//   - NULL: An error occurred (e.g., out of memory, thread killed)
//
// Timeout is specified in seconds. Use 0 for no timeout (infinite wait).
func (a *AdvisoryLock) AcquireLock(ctx context.Context, timeoutSeconds int) (bool, error) {
	if a.held {
		return true, nil // Already holding the lock
	}

	query := "SELECT GET_LOCK(?, ?)"
	var result sql.NullInt64

	err := a.db.QueryRowContext(ctx, query, a.lockName, timeoutSeconds).Scan(&result)
	if err != nil {
		return false, fmt.Errorf("failed to execute GET_LOCK: %w", err)
	}

	// Check if result is NULL (error case)
	if !result.Valid {
		return false, fmt.Errorf("GET_LOCK returned NULL for lock %q (possible database error)", a.lockName)
	}

	// Check result value
	switch result.Int64 {
	case 1:
		a.held = true
		return true, nil
	case 0:
		// Timeout reached - another instance is holding the lock
		return false, nil
	default:
		return false, fmt.Errorf("unexpected GET_LOCK return value: %d", result.Int64)
	}
}

// ReleaseLock releases the advisory lock.
// Returns true if the lock was released successfully, false if the lock was not held.
// Returns an error if the database query fails.
//
// MySQL RELEASE_LOCK() return values:
//   - 1: Lock was released successfully
//   - 0: Lock was not established by this thread (not held)
//   - NULL: Named lock did not exist
//
// Note: Locks are automatically released when the database connection closes,
// but explicit release is recommended for proper cleanup.
func (a *AdvisoryLock) ReleaseLock(ctx context.Context) (bool, error) {
	if !a.held {
		return false, nil // Not holding the lock
	}

	query := "SELECT RELEASE_LOCK(?)"
	var result sql.NullInt64

	err := a.db.QueryRowContext(ctx, query, a.lockName).Scan(&result)
	if err != nil {
		return false, fmt.Errorf("failed to execute RELEASE_LOCK: %w", err)
	}

	// Check if result is NULL (lock didn't exist)
	if !result.Valid {
		a.held = false // Update state even if NULL
		return false, fmt.Errorf("RELEASE_LOCK returned NULL for lock %q (lock did not exist)", a.lockName)
	}

	// Check result value
	switch result.Int64 {
	case 1:
		a.held = false
		return true, nil
	case 0:
		// Lock was not established by this thread
		a.held = false // Update state to reflect reality
		return false, nil
	default:
		return false, fmt.Errorf("unexpected RELEASE_LOCK return value: %d", result.Int64)
	}
}

// IsHeld returns true if this lock is currently held by this instance.
func (a *AdvisoryLock) IsHeld() bool {
	return a.held
}

// LockName returns the name of the advisory lock.
func (a *AdvisoryLock) LockName() string {
	return a.lockName
}

// TryAcquire attempts to acquire the lock immediately without waiting.
// Returns true if acquired, false if the lock is already held by another instance.
// Returns an error only if there is a database failure.
//
// This is equivalent to AcquireLock(ctx, TimeoutImmediate) but with clearer intent.
// Use this for fast duplicate job detection.
func (a *AdvisoryLock) TryAcquire(ctx context.Context) (bool, error) {
	return a.AcquireLock(ctx, TimeoutImmediate)
}

// AcquireOrFail attempts to acquire the lock with a short timeout.
// Returns nil if lock is acquired successfully.
// Returns ErrLockTimeout if another instance is holding the lock.
// Returns other errors for database failures.
//
// This is a convenience method for the common pattern of failing fast when
// a duplicate job is detected. Uses TimeoutShort (1 second) by default.
func (a *AdvisoryLock) AcquireOrFail(ctx context.Context) error {
	acquired, err := a.AcquireLock(ctx, TimeoutShort)
	if err != nil {
		return err
	}
	if !acquired {
		return fmt.Errorf("%w: lock %q is held by another instance", ErrLockTimeout, a.lockName)
	}
	return nil
}

// GenerateJobLockName creates a consistent lock name for a GoArchive job.
// Lock names follow the format: "goarchive:job:{jobName}"
//
// This ensures:
//   - Consistent naming across all GoArchive instances
//   - Namespacing to avoid conflicts with other MySQL locks
//   - Easy identification in MySQL's lock tables
//
// Example: GenerateJobLockName("archive_old_orders") → "goarchive:job:archive_old_orders"
func GenerateJobLockName(jobName string) string {
	// Sanitize job name to prevent injection or lock name conflicts
	// Replace any problematic characters with underscores
	sanitized := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			return r
		}
		return '_'
	}, jobName)

	return fmt.Sprintf("goarchive:job:%s", sanitized)
}

// NewJobLock creates a new advisory lock for a specific GoArchive job.
// The lock name is automatically generated using GenerateJobLockName.
//
// This is the recommended way to create locks for job execution to ensure
// consistent lock naming and prevent duplicate job execution.
//
// Example:
//
//	lock := NewJobLock(db, "archive_old_orders")
//	if err := lock.AcquireOrFail(ctx); err != nil {
//	    if errors.Is(err, ErrLockTimeout) {
//	        log.Error("Job is already running")
//	    }
//	    return err
//	}
//	defer lock.ReleaseLock(ctx)
func NewJobLock(db *sql.DB, jobName string) *AdvisoryLock {
	lockName := GenerateJobLockName(jobName)
	return NewAdvisoryLock(db, lockName)
}

// IsJobRunning checks if a specific job is currently running by attempting
// to acquire its lock immediately without waiting.
//
// Returns:
//   - true, nil: Job is currently running (lock is held by another instance)
//   - false, nil: Job is not running (lock is available)
//   - false, error: Database error occurred while checking
//
// This is useful for pre-flight checks or status reporting without actually
// acquiring the lock. Note that this check is not atomic - the job state
// could change immediately after this function returns.
//
// Example:
//
//	running, err := IsJobRunning(ctx, db, "archive_old_orders")
//	if err != nil {
//	    return fmt.Errorf("failed to check job status: %w", err)
//	}
//	if running {
//	    log.Info("Job is currently running, skipping")
//	    return nil
//	}
func IsJobRunning(ctx context.Context, db *sql.DB, jobName string) (bool, error) {
	lock := NewJobLock(db, jobName)

	// Try to acquire lock immediately
	acquired, err := lock.TryAcquire(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to check if job %q is running: %w", jobName, err)
	}

	// If we acquired the lock, the job was not running
	// Release it immediately since we were just checking
	if acquired {
		if _, releaseErr := lock.ReleaseLock(ctx); releaseErr != nil {
			// Log warning but don't fail - the lock will auto-release on connection close
			// In production, this should be logged via the zap logger
			_ = releaseErr // Suppress unused variable warning
		}
		return false, nil
	}

	// Lock was not acquired - job is running
	return true, nil
}

// WithLock executes a function while holding an advisory lock, ensuring
// automatic release even if the function panics.
//
// This provides crash-safe lock management by using defer to guarantee
// lock release regardless of how the function exits (normal return, error,
// or panic). The lock is acquired with the specified timeout before
// executing the function.
//
// Returns:
//   - ErrLockTimeout if lock cannot be acquired within timeout
//   - Any error returned by the function
//   - Any panic from the function is re-raised after releasing the lock
//
// Example:
//
//	lock := lock.NewJobLock(db, "archive_old_orders")
//	err := lock.WithLock(ctx, lock.TimeoutShort, func() error {
//	    // Critical section - lock is held
//	    // Lock will be released even if this panics
//	    return processJob()
//	})
func (a *AdvisoryLock) WithLock(ctx context.Context, timeoutSeconds int, fn func() error) error {
	// Acquire the lock
	acquired, err := a.AcquireLock(ctx, timeoutSeconds)
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	if !acquired {
		return fmt.Errorf("%w: lock %q is held by another instance", ErrLockTimeout, a.lockName)
	}

	// Ensure lock is released even on panic
	defer func() {
		// Release lock in a separate context to avoid cancellation issues
		// Use background context with short timeout for cleanup
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if _, releaseErr := a.ReleaseLock(releaseCtx); releaseErr != nil {
			// Lock will auto-release when connection closes
			// In production, this should be logged via zap logger
			_ = releaseErr // Suppress unused variable warning
		}
	}()

	// Execute the protected function
	return fn()
}

// WithJobLock executes a function while holding a job-specific advisory lock.
// This is a convenience wrapper around WithLock that automatically generates
// the job lock name and provides crash-safe execution.
//
// The lock is acquired with TimeoutShort (1 second) by default for fast
// duplicate detection. If you need a different timeout, create a lock with
// NewJobLock and use WithLock directly.
//
// Returns:
//   - ErrLockTimeout if another instance is running the same job
//   - Any error returned by the function
//   - Any panic from the function is re-raised after releasing the lock
//
// Example:
//
//	err := lock.WithJobLock(ctx, db, "archive_old_orders", func() error {
//	    // Job execution - protected by advisory lock
//	    // Lock automatically released even if this panics
//	    return runArchiveJob()
//	})
//	if errors.Is(err, lock.ErrLockTimeout) {
//	    log.Info("Job already running, skipping")
//	    return nil
//	}
func WithJobLock(ctx context.Context, db *sql.DB, jobName string, fn func() error) error {
	lock := NewJobLock(db, jobName)
	return lock.WithLock(ctx, TimeoutShort, fn)
}
