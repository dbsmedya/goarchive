// Package lock provides MySQL advisory locking functionality for GoArchive.
package lock

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
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
	db              *sql.DB
	conn            *sql.Conn
	lockName        string
	connID          int64
	held            bool
	keepAliveCancel context.CancelFunc
	keepAliveDone   chan struct{}
	mu              sync.Mutex
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
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.held {
		return true, nil // Already holding the lock
	}
	if a.db == nil {
		return false, fmt.Errorf("database is nil")
	}

	conn, err := a.db.Conn(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get dedicated connection: %w", err)
	}

	query := "SELECT GET_LOCK(?, ?)"
	var result sql.NullInt64

	err = conn.QueryRowContext(ctx, query, a.lockName, timeoutSeconds).Scan(&result)
	if err != nil {
		_ = conn.Close()
		return false, fmt.Errorf("failed to execute GET_LOCK: %w", err)
	}

	// Check if result is NULL (error case)
	if !result.Valid {
		_ = conn.Close()
		return false, fmt.Errorf("GET_LOCK returned NULL for lock %q (possible database error)", a.lockName)
	}

	// Check result value
	switch result.Int64 {
	case 1:
		var connID int64
		if err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&connID); err != nil {
			_ = conn.Close()
			return false, fmt.Errorf("failed to read advisory lock connection id: %w", err)
		}
		a.conn = conn
		a.connID = connID
		a.held = true
		return true, nil
	case 0:
		// Timeout reached - another instance is holding the lock
		_ = conn.Close()
		return false, nil
	default:
		_ = conn.Close()
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
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.held {
		if a.conn != nil {
			closeErr := a.conn.Close()
			a.conn = nil
			a.connID = 0
			if closeErr != nil {
				return false, fmt.Errorf("failed to close unheld lock connection: %w", closeErr)
			}
		}
		return false, nil // Not holding the lock
	}
	if a.conn == nil {
		a.held = false
		return false, fmt.Errorf("lock %q marked as held but dedicated connection is nil", a.lockName)
	}

	query := "SELECT RELEASE_LOCK(?)"
	var result sql.NullInt64

	err := a.conn.QueryRowContext(ctx, query, a.lockName).Scan(&result)
	closeErr := a.conn.Close()
	a.conn = nil
	a.connID = 0
	a.held = false

	if err != nil {
		if closeErr != nil {
			return false, fmt.Errorf("failed to execute RELEASE_LOCK: %w (failed to close connection: %v)", err, closeErr)
		}
		return false, fmt.Errorf("failed to execute RELEASE_LOCK: %w", err)
	}

	// Check if result is NULL (lock didn't exist)
	if !result.Valid {
		if closeErr != nil {
			return false, fmt.Errorf("RELEASE_LOCK returned NULL for lock %q (lock did not exist); failed to close connection: %v", a.lockName, closeErr)
		}
		return false, fmt.Errorf("RELEASE_LOCK returned NULL for lock %q (lock did not exist)", a.lockName)
	}

	// Check result value
	switch result.Int64 {
	case 1:
		if closeErr != nil {
			return true, fmt.Errorf("failed to close connection after RELEASE_LOCK: %w", closeErr)
		}
		return true, nil
	case 0:
		// Lock was not established by this thread
		if closeErr != nil {
			return false, fmt.Errorf("failed to close connection after RELEASE_LOCK: %w", closeErr)
		}
		return false, nil
	default:
		if closeErr != nil {
			return false, fmt.Errorf("unexpected RELEASE_LOCK return value: %d (failed to close connection: %v)", result.Int64, closeErr)
		}
		return false, fmt.Errorf("unexpected RELEASE_LOCK return value: %d", result.Int64)
	}
}

// StartKeepAlive periodically verifies that this instance still owns the lock.
func (a *AdvisoryLock) StartKeepAlive(ctx context.Context, interval time.Duration) <-chan error {
	lost := make(chan error, 1)
	if interval <= 0 {
		interval = 30 * time.Second
	}

	a.mu.Lock()
	if a.keepAliveCancel != nil {
		a.mu.Unlock()
		return lost
	}
	keepAliveCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	a.keepAliveCancel = cancel
	a.keepAliveDone = done
	a.mu.Unlock()

	go func() {
		defer close(done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-keepAliveCtx.Done():
				return
			case <-t.C:
				if err := a.checkOwnership(keepAliveCtx); err != nil {
					// Don't report loss if we're shutting down; a cancelled keepalive
					// context is normal cleanup, not a lost lock.
					if keepAliveCtx.Err() != nil {
						return
					}
					select {
					case lost <- err:
					default:
					}
					return
				}
			}
		}
	}()

	return lost
}

// StopKeepAlive stops the advisory lock keepalive goroutine.
func (a *AdvisoryLock) StopKeepAlive() {
	a.mu.Lock()
	cancel := a.keepAliveCancel
	done := a.keepAliveDone
	a.keepAliveCancel = nil
	a.keepAliveDone = nil
	a.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (a *AdvisoryLock) checkOwnership(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.held {
		return fmt.Errorf("lock %q is not held", a.lockName)
	}
	if a.conn == nil {
		a.held = false
		return fmt.Errorf("lock %q marked as held but dedicated connection is nil", a.lockName)
	}

	var owner sql.NullInt64
	if err := a.conn.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?)", a.lockName).Scan(&owner); err != nil {
		// A cancelled context means the keepalive is shutting down (job completing or
		// aborting via the shared run context), not that we lost ownership. Don't tear
		// down the lock or report loss; let normal cleanup release it.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		a.markLostLocked()
		return fmt.Errorf("failed to verify ownership for lock %q: %w", a.lockName, err)
	}
	if !owner.Valid {
		a.markLostLocked()
		return fmt.Errorf("lock %q is no longer used", a.lockName)
	}
	if owner.Int64 != a.connID {
		a.markLostLocked()
		return fmt.Errorf("lock %q owner changed: got connection %d, want %d", a.lockName, owner.Int64, a.connID)
	}
	return nil
}

func (a *AdvisoryLock) markLostLocked() {
	a.held = false
	if a.conn != nil {
		_ = a.conn.Close()
		a.conn = nil
	}
	a.connID = 0
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
//	acquired, err := lock.TryAcquire(ctx)
//	if err != nil {
//	    return err
//	}
//	if !acquired {
//	    log.Error("Job is already running")
//	    return nil
//	}
//	defer lock.ReleaseLock(ctx)
func NewJobLock(db *sql.DB, jobName string) *AdvisoryLock {
	lockName := GenerateJobLockName(jobName)
	return NewAdvisoryLock(db, lockName)
}

// GenerateRootTableLockName creates a consistent lock name for serializing startup on a root table.
func GenerateRootTableLockName(rootTable string) string {
	sanitized := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			return r
		}
		return '_'
	}, rootTable)
	return fmt.Sprintf("goarchive:root:%s", sanitized)
}

// NewRootTableLock creates an advisory lock keyed on a root table.
func NewRootTableLock(db *sql.DB, rootTable string) *AdvisoryLock {
	return NewAdvisoryLock(db, GenerateRootTableLockName(rootTable))
}
