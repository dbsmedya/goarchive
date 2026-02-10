// Package lock provides MySQL advisory locking functionality for GoArchive.
package lock

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// ============================================================================
// GenerateJobLockName Tests
// ============================================================================

func TestGenerateJobLockName_Format(t *testing.T) {
	tests := []struct {
		jobName  string
		expected string
	}{
		{"archive_orders", "goarchive:job:archive_orders"},
		{"job-123", "goarchive:job:job-123"},
		{"my_job_name", "goarchive:job:my_job_name"},
		{"UPPERCASE_JOB", "goarchive:job:UPPERCASE_JOB"},
		{"MixedCase_Job-123", "goarchive:job:MixedCase_Job-123"},
	}

	for _, tt := range tests {
		t.Run(tt.jobName, func(t *testing.T) {
			result := GenerateJobLockName(tt.jobName)
			if result != tt.expected {
				t.Errorf("GenerateJobLockName(%q) = %q, expected %q", tt.jobName, result, tt.expected)
			}
		})
	}
}

func TestGenerateJobLockName_Sanitization(t *testing.T) {
	tests := []struct {
		jobName  string
		expected string
	}{
		// Special characters should be replaced with underscores
		{"job.with.dots", "goarchive:job:job_with_dots"},
		{"job/with/slashes", "goarchive:job:job_with_slashes"},
		{"job@with@ats", "goarchive:job:job_with_ats"},
		{"job with spaces", "goarchive:job:job_with_spaces"},
		{"job#with#hash", "goarchive:job:job_with_hash"},
		{"job$with$dollar", "goarchive:job:job_with_dollar"},
		{"job%with%percent", "goarchive:job:job_with_percent"},
		{"job&ampersand", "goarchive:job:job_ampersand"},
		{"job*star*", "goarchive:job:job_star_"},
		{"job(paren)", "goarchive:job:job_paren_"},
		{"job+plus+", "goarchive:job:job_plus_"},
		{"job=equals=", "goarchive:job:job_equals_"},
		{"job?question?", "goarchive:job:job_question_"},
		{"job!exclaim!", "goarchive:job:job_exclaim_"},
		{"job[bracket]", "goarchive:job:job_bracket_"},
		{"job{brace}", "goarchive:job:job_brace_"},
		{"job:colon:", "goarchive:job:job_colon_"},
		{"job;semi;", "goarchive:job:job_semi_"},
		{"job'quote'", "goarchive:job:job_quote_"},
		{`job"double"`, "goarchive:job:job_double_"},
		{"job<less>", "goarchive:job:job_less_"},
		{"job>greater>", "goarchive:job:job_greater_"},
		{"job|pipe|", "goarchive:job:job_pipe_"},
		{"job^caret^", "goarchive:job:job_caret_"},
		{"job~tilde~", "goarchive:job:job_tilde_"},
		{"job`backtick`", "goarchive:job:job_backtick_"},
		// Multiple special characters
		{"job!@#$%", "goarchive:job:job_____"},
		// Mixed valid and invalid characters
		{"job_name.with-dots", "goarchive:job:job_name_with-dots"},
	}

	for _, tt := range tests {
		t.Run(tt.jobName, func(t *testing.T) {
			result := GenerateJobLockName(tt.jobName)
			if result != tt.expected {
				t.Errorf("GenerateJobLockName(%q) = %q, expected %q", tt.jobName, result, tt.expected)
			}
		})
	}
}

func TestGenerateJobLockName_EdgeCases(t *testing.T) {
	// Empty job name
	result := GenerateJobLockName("")
	if result != "goarchive:job:" {
		t.Errorf("Empty job name: got %q, expected %q", result, "goarchive:job:")
	}

	// Single character
	result = GenerateJobLockName("a")
	if result != "goarchive:job:a" {
		t.Errorf("Single char: got %q, expected %q", result, "goarchive:job:a")
	}

	// Only special characters (all become underscores)
	result = GenerateJobLockName("!@#$%")
	if result != "goarchive:job:_____" {
		t.Errorf("Only special chars: got %q, expected %q", result, "goarchive:job:_____")
	}

	// Long job name (should not be truncated by our function, but MySQL limits to 64 chars)
	longName := strings.Repeat("a", 100)
	result = GenerateJobLockName(longName)
	expectedPrefix := "goarchive:job:"
	if !strings.HasPrefix(result, expectedPrefix) {
		t.Errorf("Long name should have prefix %q, got %q", expectedPrefix, result)
	}
}

func TestGenerateJobLockName_Consistency(t *testing.T) {
	// Same input should always produce same output
	jobName := "my_test_job"
	result1 := GenerateJobLockName(jobName)
	result2 := GenerateJobLockName(jobName)
	result3 := GenerateJobLockName(jobName)

	if result1 != result2 || result2 != result3 {
		t.Error("GenerateJobLockName should be consistent for same input")
	}
}

// ============================================================================
// NewJobLock Tests
// ============================================================================

func TestNewJobLock(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	jobName := "test_archive_job"
	lock := NewJobLock(db, jobName)

	if lock == nil {
		t.Fatal("NewJobLock returned nil")
	}

	expectedLockName := GenerateJobLockName(jobName)
	if lock.LockName() != expectedLockName {
		t.Errorf("Lock name = %q, expected %q", lock.LockName(), expectedLockName)
	}

	if lock.db != db {
		t.Error("Lock should store database connection")
	}

	if lock.IsHeld() {
		t.Error("New lock should not be held")
	}
}

func TestNewJobLock_EmptyJobName(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lock := NewJobLock(db, "")
	if lock == nil {
		t.Fatal("NewJobLock should not return nil for empty job name")
	}

	expectedLockName := "goarchive:job:"
	if lock.LockName() != expectedLockName {
		t.Errorf("Empty job lock name = %q, expected %q", lock.LockName(), expectedLockName)
	}
}

func TestNewJobLock_SanitizedName(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	// Job name with special characters that get sanitized
	jobName := "my.job@name"
	lock := NewJobLock(db, jobName)

	expectedLockName := "goarchive:job:my_job_name"
	if lock.LockName() != expectedLockName {
		t.Errorf("Sanitized lock name = %q, expected %q", lock.LockName(), expectedLockName)
	}
}

// ============================================================================
// IsJobRunning Tests
// ============================================================================

func TestIsJobRunning_NotRunning(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	ctx := context.Background()
	jobName := generateUniqueLockName(t) + "_not_running"

	running, err := IsJobRunning(ctx, db, jobName)
	if err != nil {
		t.Fatalf("IsJobRunning failed: %v", err)
	}

	if running {
		t.Error("IsJobRunning should return false when job is not running")
	}
}

func TestIsJobRunning_ActuallyRunning(t *testing.T) {
	db1 := connectToTestDB(t)
	defer db1.Close()

	db2 := connectToTestDB(t)
	defer db2.Close()

	ctx := context.Background()
	jobName := generateUniqueLockName(t) + "_running"

	// First connection acquires the lock
	lock := NewJobLock(db1, jobName)
	acquired, err := lock.AcquireLock(ctx, 1)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}
	if !acquired {
		t.Fatal("Expected to acquire lock")
	}

	// Second connection checks if job is running
	running, err := IsJobRunning(ctx, db2, jobName)
	if err != nil {
		t.Fatalf("IsJobRunning failed: %v", err)
	}

	if !running {
		t.Error("IsJobRunning should return true when job is running")
	}

	// Cleanup
	lock.ReleaseLock(ctx)
}

func TestIsJobRunning_DoesNotLeaveLock(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	ctx := context.Background()
	jobName := generateUniqueLockName(t) + "_check_cleanup"

	// Check if job is running (should acquire and release)
	running, err := IsJobRunning(ctx, db, jobName)
	if err != nil {
		t.Fatalf("IsJobRunning failed: %v", err)
	}

	if running {
		t.Error("Job should not be running initially")
	}

	// Now try to acquire the lock ourselves - should succeed
	lock := NewJobLock(db, jobName)
	acquired, err := lock.TryAcquire(ctx)
	if err != nil {
		t.Fatalf("TryAcquire failed: %v", err)
	}

	if !acquired {
		t.Error("Should be able to acquire lock after IsJobRunning - lock was not released")
	}

	// Cleanup
	lock.ReleaseLock(ctx)
}

// ============================================================================
// WithLock Tests
// ============================================================================

func TestWithLock_Success(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockName := generateUniqueLockName(t)
	lock := NewAdvisoryLock(db, lockName)
	ctx := context.Background()

	executed := false
	err := lock.WithLock(ctx, 5, func() error {
		executed = true
		if !lock.IsHeld() {
			t.Error("Lock should be held during function execution")
		}
		return nil
	})

	if err != nil {
		t.Fatalf("WithLock failed: %v", err)
	}

	if !executed {
		t.Error("Function should have been executed")
	}

	// Lock should be released after function completes
	if lock.IsHeld() {
		t.Error("Lock should be released after WithLock completes")
	}

	// Verify lock is actually released by trying to acquire it
	lock2 := NewAdvisoryLock(db, lockName)
	acquired, _ := lock2.TryAcquire(ctx)
	if !acquired {
		t.Error("Lock should be available after WithLock")
	}
	lock2.ReleaseLock(ctx)
}

func TestWithLock_ErrorReturn(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockName := generateUniqueLockName(t)
	lock := NewAdvisoryLock(db, lockName)
	ctx := context.Background()

	expectedErr := errors.New("function error")
	err := lock.WithLock(ctx, 5, func() error {
		return expectedErr
	})

	if err == nil {
		t.Fatal("WithLock should return error from function")
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}

	// Lock should still be released even if function returns error
	if lock.IsHeld() {
		t.Error("Lock should be released after function returns error")
	}

	// Verify lock is released
	lock2 := NewAdvisoryLock(db, lockName)
	acquired, _ := lock2.TryAcquire(ctx)
	if !acquired {
		t.Error("Lock should be available after error return")
	}
	lock2.ReleaseLock(ctx)
}

func TestWithLock_PanicRecovery(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockName := generateUniqueLockName(t)
	lock := NewAdvisoryLock(db, lockName)
	ctx := context.Background()

	// Use a separate lock to verify release after panic
	panicOccurred := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicOccurred = true
			}
		}()
		lock.WithLock(ctx, 5, func() error {
			panic("intentional panic")
		})
	}()

	if !panicOccurred {
		t.Fatal("Panic should have occurred")
	}

	// Lock should be released even after panic
	// Wait a moment for the defer to complete
	time.Sleep(100 * time.Millisecond)

	if lock.IsHeld() {
		t.Error("Lock should be released after panic")
	}

	// Verify lock is released by trying to acquire it with new connection
	lock2 := NewAdvisoryLock(db, lockName)
	acquired, _ := lock2.TryAcquire(ctx)
	if !acquired {
		t.Error("Lock should be available after panic recovery")
	}
	lock2.ReleaseLock(ctx)
}

func TestWithLock_Timeout(t *testing.T) {
	db1 := connectToTestDB(t)
	defer db1.Close()

	db2 := connectToTestDB(t)
	defer db2.Close()

	lockName := generateUniqueLockName(t)

	// First connection acquires the lock
	lock1 := NewAdvisoryLock(db1, lockName)
	ctx := context.Background()
	acquired, err := lock1.AcquireLock(ctx, 5)
	if err != nil {
		t.Fatalf("First acquire failed: %v", err)
	}
	if !acquired {
		t.Fatal("Expected to acquire lock first")
	}

	// Second connection tries to use WithLock with short timeout
	lock2 := NewAdvisoryLock(db2, lockName)
	executed := false
	start := time.Now()
	err = lock2.WithLock(ctx, 1, func() error {
		executed = true
		return nil
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Error("WithLock should return error on timeout")
	}

	if executed {
		t.Error("Function should not have been executed due to timeout")
	}

	if !errors.Is(err, ErrLockTimeout) {
		t.Errorf("Expected ErrLockTimeout, got: %v", err)
	}

	// Should have waited approximately 1 second
	if elapsed < 900*time.Millisecond || elapsed > 1500*time.Millisecond {
		t.Errorf("Expected ~1s timeout, took %v", elapsed)
	}

	// Cleanup
	lock1.ReleaseLock(ctx)
}

func TestWithLock_AcquireError(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockName := generateUniqueLockName(t)
	lock := NewAdvisoryLock(db, lockName)

	// Cancel context before acquiring
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	executed := false
	err := lock.WithLock(ctx, 5, func() error {
		executed = true
		return nil
	})

	if err == nil {
		t.Error("WithLock should return error when context is cancelled")
	}

	if executed {
		t.Error("Function should not have been executed")
	}
}

// ============================================================================
// WithJobLock Tests
// ============================================================================

func TestWithJobLock_Success(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	ctx := context.Background()
	jobName := generateUniqueLockName(t) + "_job"

	executed := false
	err := WithJobLock(ctx, db, jobName, func() error {
		executed = true
		return nil
	})

	if err != nil {
		t.Fatalf("WithJobLock failed: %v", err)
	}

	if !executed {
		t.Error("Function should have been executed")
	}
}

func TestWithJobLock_DuplicateJob(t *testing.T) {
	db1 := connectToTestDB(t)
	defer db1.Close()

	db2 := connectToTestDB(t)
	defer db2.Close()

	ctx := context.Background()
	jobName := generateUniqueLockName(t) + "_duplicate"

	// First instance acquires the lock
	lock1 := NewJobLock(db1, jobName)
	acquired, err := lock1.AcquireLock(ctx, 5)
	if err != nil {
		t.Fatalf("First acquire failed: %v", err)
	}
	if !acquired {
		t.Fatal("Expected first lock to be acquired")
	}

	// Second instance tries to run the same job
	executed := false
	err = WithJobLock(ctx, db2, jobName, func() error {
		executed = true
		return nil
	})

	if err == nil {
		t.Error("WithJobLock should return error when job is already running")
	}

	if executed {
		t.Error("Function should not have been executed when job is running")
	}

	if !errors.Is(err, ErrLockTimeout) {
		t.Errorf("Expected ErrLockTimeout, got: %v", err)
	}

	// Cleanup
	lock1.ReleaseLock(ctx)
}

func TestWithJobLock_ErrorPropagation(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	ctx := context.Background()
	jobName := generateUniqueLockName(t) + "_error"

	expectedErr := errors.New("job execution error")
	err := WithJobLock(ctx, db, jobName, func() error {
		return expectedErr
	})

	if err == nil {
		t.Fatal("WithJobLock should return error from function")
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}
}

// ============================================================================
// Integration Tests
// ============================================================================

func TestJobLock_FullWorkflow(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	ctx := context.Background()
	jobName := generateUniqueLockName(t) + "_workflow"

	// Step 1: Check if job is running (should be false)
	running, err := IsJobRunning(ctx, db, jobName)
	if err != nil {
		t.Fatalf("IsJobRunning failed: %v", err)
	}
	if running {
		t.Error("Job should not be running initially")
	}

	// Step 2: Execute job with WithJobLock
	executed := false
	err = WithJobLock(ctx, db, jobName, func() error {
		executed = true

		// While job is running, check again (should be true from another connection)
		db2 := connectToTestDB(t)
		defer db2.Close()

		running2, _ := IsJobRunning(ctx, db2, jobName)
		if !running2 {
			t.Error("Job should be detected as running from another connection")
		}

		return nil
	})

	if err != nil {
		t.Fatalf("WithJobLock failed: %v", err)
	}

	if !executed {
		t.Error("Job function should have been executed")
	}

	// Step 3: Verify job is no longer running
	running, err = IsJobRunning(ctx, db, jobName)
	if err != nil {
		t.Fatalf("IsJobRunning failed: %v", err)
	}
	if running {
		t.Error("Job should not be running after completion")
	}
}

func TestJobLock_ConcurrentJobs(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	ctx := context.Background()
	jobName1 := generateUniqueLockName(t) + "_job1"
	jobName2 := generateUniqueLockName(t) + "_job2"

	// Both jobs should be able to run concurrently (different lock names)
	executed1 := false
	executed2 := false

	err1 := WithJobLock(ctx, db, jobName1, func() error {
		executed1 = true
		return nil
	})

	err2 := WithJobLock(ctx, db, jobName2, func() error {
		executed2 = true
		return nil
	})

	if err1 != nil {
		t.Errorf("Job1 failed: %v", err1)
	}
	if err2 != nil {
		t.Errorf("Job2 failed: %v", err2)
	}

	if !executed1 {
		t.Error("Job1 should have been executed")
	}
	if !executed2 {
		t.Error("Job2 should have been executed")
	}
}
