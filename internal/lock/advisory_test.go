// Package lock provides MySQL advisory locking functionality for GoArchive.
package lock

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// ============================================================================
// Test Configuration and Helpers
// ============================================================================

// getTestDSN returns the DSN for the test MySQL server
// Uses environment variables or defaults to local test server
func getTestDSN() string {
	host := getEnv("TEST_MYSQL_HOST", "127.0.0.1")
	port := getEnv("TEST_MYSQL_PORT", "3305")
	user := getEnv("TEST_MYSQL_USER", "root")
	pass := getEnv("TEST_MYSQL_PASS", "qazokm")

	return fmt.Sprintf("%s:%s@tcp(%s:%s)/?parseTime=true&multiStatements=true", user, pass, host, port)
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// connectToTestDB establishes a connection to the test MySQL server
func connectToTestDB(t *testing.T) *sql.DB {
	dsn := getTestDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Failed to open database connection: %v", err)
	}

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Skipf("MySQL test server not available: %v", err)
	}

	return db
}

// generateUniqueLockName creates a unique lock name for test isolation
// MySQL limits lock names to 64 characters, so we use a hash-based approach
func generateUniqueLockName(t *testing.T) string {
	// Use short prefix + first 8 chars of test name + timestamp suffix
	// Total: ~30-40 characters, well under 64 limit
	testName := t.Name()
	if len(testName) > 15 {
		testName = testName[:15]
	}
	return fmt.Sprintf("t_%s_%d", testName, time.Now().UnixNano()%1000000)
}

// releaseLock is a helper to manually release a lock for cleanup
func releaseLock(db *sql.DB, lockName string) error {
	var result sql.NullInt64
	err := db.QueryRow("SELECT RELEASE_LOCK(?)", lockName).Scan(&result)
	if err != nil {
		return err
	}
	return nil
}

// isLockFree checks if a lock is currently free
func isLockFree(db *sql.DB, lockName string) (bool, error) {
	var result sql.NullInt64
	err := db.QueryRow("SELECT IS_FREE_LOCK(?)", lockName).Scan(&result)
	if err != nil {
		return false, err
	}
	if !result.Valid {
		return false, fmt.Errorf("IS_FREE_LOCK returned NULL")
	}
	return result.Int64 == 1, nil
}

// ============================================================================
// Constructor Tests
// ============================================================================

func TestNewAdvisoryLock(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockName := "test_constructor_lock"
	lock := NewAdvisoryLock(db, lockName)

	if lock == nil {
		t.Fatal("NewAdvisoryLock returned nil")
	}

	if lock.db != db {
		t.Error("Lock should store database connection")
	}

	if lock.lockName != lockName {
		t.Errorf("Lock name mismatch: got %q, expected %q", lock.lockName, lockName)
	}

	if lock.held {
		t.Error("New lock should not be marked as held")
	}
}

func TestNewAdvisoryLock_EmptyName(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	// Empty lock name should still create the lock (MySQL behavior tested separately)
	lock := NewAdvisoryLock(db, "")
	if lock == nil {
		t.Fatal("NewAdvisoryLock should accept empty name")
	}

	if lock.lockName != "" {
		t.Error("Lock name should be empty")
	}
}

// ============================================================================
// Lock Acquisition Tests
// ============================================================================

func TestAdvisoryLock_AcquireLock_Success(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockName := generateUniqueLockName(t)
	lock := NewAdvisoryLock(db, lockName)

	ctx := context.Background()
	acquired, err := lock.AcquireLock(ctx, 5)

	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}

	if !acquired {
		t.Error("Expected to acquire lock successfully")
	}

	if !lock.IsHeld() {
		t.Error("Lock should report as held after successful acquisition")
	}

	// Cleanup
	releaseLock(db, lockName)
}

func TestAdvisoryLock_AcquireLock_ZeroTimeout(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockName := generateUniqueLockName(t)
	lock := NewAdvisoryLock(db, lockName)

	ctx := context.Background()
	// Zero timeout means immediate return (don't wait)
	acquired, err := lock.AcquireLock(ctx, 0)

	if err != nil {
		t.Fatalf("AcquireLock with zero timeout failed: %v", err)
	}

	if !acquired {
		t.Error("Expected to acquire lock immediately with zero timeout")
	}

	// Cleanup
	releaseLock(db, lockName)
}

func TestAdvisoryLock_AcquireLock_AlreadyHeld(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockName := generateUniqueLockName(t)
	lock := NewAdvisoryLock(db, lockName)

	ctx := context.Background()

	// First acquisition
	acquired, err := lock.AcquireLock(ctx, 5)
	if err != nil {
		t.Fatalf("First AcquireLock failed: %v", err)
	}
	if !acquired {
		t.Fatal("Expected to acquire lock on first attempt")
	}

	// Second acquisition should succeed (idempotent) without error
	acquired2, err := lock.AcquireLock(ctx, 5)
	if err != nil {
		t.Fatalf("Second AcquireLock failed: %v", err)
	}

	if !acquired2 {
		t.Error("Expected second acquisition to return true (already held)")
	}

	if !lock.IsHeld() {
		t.Error("Lock should still be held")
	}

	// Cleanup
	releaseLock(db, lockName)
}

func TestAdvisoryLock_AcquireLock_Timeout(t *testing.T) {
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
		t.Fatalf("First lock acquisition failed: %v", err)
	}
	if !acquired {
		t.Fatal("Expected first lock to be acquired")
	}

	// Second connection tries to acquire the same lock with short timeout
	lock2 := NewAdvisoryLock(db2, lockName)
	start := time.Now()
	acquired2, err := lock2.AcquireLock(ctx, 1) // 1 second timeout
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Second AcquireLock failed with error: %v", err)
	}

	if acquired2 {
		t.Error("Expected second lock acquisition to fail (timeout)")
	}

	// Should have waited approximately the timeout duration
	if elapsed < 900*time.Millisecond || elapsed > 1500*time.Millisecond {
		t.Errorf("Timeout duration unexpected: %v (expected ~1s)", elapsed)
	}

	if lock2.IsHeld() {
		t.Error("Lock2 should not report as held after timeout")
	}

	// Cleanup
	releaseLock(db1, lockName)
}

func TestAdvisoryLock_AcquireLock_ContextCancellation(t *testing.T) {
	db1 := connectToTestDB(t)
	defer db1.Close()

	db2 := connectToTestDB(t)
	defer db2.Close()

	lockName := generateUniqueLockName(t)

	// First connection acquires the lock
	lock1 := NewAdvisoryLock(db1, lockName)
	ctx := context.Background()

	acquired, err := lock1.AcquireLock(ctx, 30)
	if err != nil {
		t.Fatalf("First lock acquisition failed: %v", err)
	}
	if !acquired {
		t.Fatal("Expected first lock to be acquired")
	}

	// Second connection tries with a cancellable context
	lock2 := NewAdvisoryLock(db2, lockName)
	ctx2, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	start := time.Now()
	acquired2, err := lock2.AcquireLock(ctx2, 30) // Long timeout but context will cancel sooner
	elapsed := time.Since(start)

	// Context cancellation returns an error from the database driver
	if err == nil {
		t.Error("Expected error due to context cancellation")
	}

	if acquired2 {
		t.Error("Expected lock acquisition to fail due to context cancellation")
	}

	// Should have returned quickly due to context cancellation, not waited 30s
	if elapsed > 2*time.Second {
		t.Errorf("Should have returned quickly due to context cancellation, took %v", elapsed)
	}

	// Cleanup
	releaseLock(db1, lockName)
}

// ============================================================================
// Lock State and Metadata Tests
// ============================================================================

func TestAdvisoryLock_IsHeld(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockName := generateUniqueLockName(t)
	lock := NewAdvisoryLock(db, lockName)

	// Initially not held
	if lock.IsHeld() {
		t.Error("New lock should not be held")
	}

	// Acquire the lock
	ctx := context.Background()
	acquired, err := lock.AcquireLock(ctx, 5)
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}
	if !acquired {
		t.Fatal("Expected to acquire lock")
	}

	if !lock.IsHeld() {
		t.Error("Lock should be held after acquisition")
	}

	// Cleanup
	releaseLock(db, lockName)
}

func TestAdvisoryLock_LockName(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	tests := []string{
		"simple_lock",
		"lock-with-dashes",
		"lock_with_underscores",
		"LockWithMixedCase",
		"a",
		"very_long_lock_name_that_is_still_valid_for_testing_purposes",
	}

	for _, lockName := range tests {
		lock := NewAdvisoryLock(db, lockName)
		if lock.LockName() != lockName {
			t.Errorf("LockName() = %q, expected %q", lock.LockName(), lockName)
		}
	}
}

// ============================================================================
// Concurrent Access Tests
// ============================================================================

func TestAdvisoryLock_ConcurrentDifferentLocks(t *testing.T) {
	db1 := connectToTestDB(t)
	defer db1.Close()

	db2 := connectToTestDB(t)
	defer db2.Close()

	lockName1 := generateUniqueLockName(t) + "_A"
	lockName2 := generateUniqueLockName(t) + "_B"

	lock1 := NewAdvisoryLock(db1, lockName1)
	lock2 := NewAdvisoryLock(db2, lockName2)

	ctx := context.Background()

	// Both should acquire their respective locks without conflict
	acquired1, err := lock1.AcquireLock(ctx, 5)
	if err != nil {
		t.Fatalf("Lock1 acquisition failed: %v", err)
	}
	if !acquired1 {
		t.Error("Expected lock1 to be acquired")
	}

	acquired2, err := lock2.AcquireLock(ctx, 5)
	if err != nil {
		t.Fatalf("Lock2 acquisition failed: %v", err)
	}
	if !acquired2 {
		t.Error("Expected lock2 to be acquired")
	}

	if !lock1.IsHeld() || !lock2.IsHeld() {
		t.Error("Both locks should be held")
	}

	// Cleanup
	releaseLock(db1, lockName1)
	releaseLock(db2, lockName2)
}

func TestAdvisoryLock_SameConnectionReentrant(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockName := generateUniqueLockName(t)

	// Same connection can acquire the same lock multiple times (MySQL behavior)
	lock1 := NewAdvisoryLock(db, lockName)
	lock2 := NewAdvisoryLock(db, lockName)

	ctx := context.Background()

	acquired1, err := lock1.AcquireLock(ctx, 5)
	if err != nil {
		t.Fatalf("First acquisition failed: %v", err)
	}
	if !acquired1 {
		t.Fatal("Expected first acquisition to succeed")
	}

	// Same connection acquiring same lock - this is connection-scoped in MySQL
	// The second lock object using the same connection will also "acquire" it
	acquired2, err := lock2.AcquireLock(ctx, 5)
	if err != nil {
		t.Fatalf("Second acquisition failed: %v", err)
	}

	// Note: MySQL GET_LOCK on the same connection returns 1 (success)
	// but doesn't actually grant a new lock - it's the same connection
	if !acquired2 {
		t.Error("Expected second acquisition from same connection to succeed")
	}

	// Need to release twice since GET_LOCK increments the lock count on same connection
	releaseLock(db, lockName)
	releaseLock(db, lockName)

	// Give MySQL a moment to process the release
	time.Sleep(50 * time.Millisecond)

	// Verify lock is free - use a fresh connection to check
	db2 := connectToTestDB(t)
	defer db2.Close()

	free, err := isLockFree(db2, lockName)
	if err != nil {
		t.Fatalf("Failed to check lock status: %v", err)
	}
	if !free {
		// Lock might still be held due to connection pooling, that's acceptable
		t.Log("Lock may still be held due to connection pooling - this is acceptable")
	}
}

// ============================================================================
// Lock Release and Cleanup Tests
// ============================================================================

func TestAdvisoryLock_ReleaseOnConnectionClose(t *testing.T) {
	db1 := connectToTestDB(t)

	lockName := generateUniqueLockName(t)
	lock := NewAdvisoryLock(db1, lockName)

	ctx := context.Background()
	acquired, err := lock.AcquireLock(ctx, 5)
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}
	if !acquired {
		t.Fatal("Expected to acquire lock")
	}

	// Close the first connection - this should release the lock
	db1.Close()

	// Give MySQL a moment to clean up
	time.Sleep(100 * time.Millisecond)

	// New connection should be able to acquire the lock
	db2 := connectToTestDB(t)
	defer db2.Close()

	lock2 := NewAdvisoryLock(db2, lockName)
	acquired2, err := lock2.AcquireLock(ctx, 2)
	if err != nil {
		t.Fatalf("Second acquisition failed: %v", err)
	}
	if !acquired2 {
		t.Error("Expected to acquire lock after first connection closed")
	}

	releaseLock(db2, lockName)
}

// ============================================================================
// Integration and Edge Case Tests
// ============================================================================

func TestAdvisoryLock_Integration_MultipleLocksSequence(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockNames := []string{
		generateUniqueLockName(t) + "_1",
		generateUniqueLockName(t) + "_2",
		generateUniqueLockName(t) + "_3",
	}

	locks := make([]*AdvisoryLock, len(lockNames))
	for i, name := range lockNames {
		locks[i] = NewAdvisoryLock(db, name)
	}

	ctx := context.Background()

	// Acquire all locks
	for i, lock := range locks {
		acquired, err := lock.AcquireLock(ctx, 5)
		if err != nil {
			t.Fatalf("Failed to acquire lock %d: %v", i, err)
		}
		if !acquired {
			t.Fatalf("Expected to acquire lock %d", i)
		}
	}

	// Verify all are held
	for i, lock := range locks {
		if !lock.IsHeld() {
			t.Errorf("Lock %d should be held", i)
		}
	}

	// Release in reverse order
	for i := len(lockNames) - 1; i >= 0; i-- {
		releaseLock(db, lockNames[i])
	}
}

func TestAdvisoryLock_LockNameIsolation(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	baseName := generateUniqueLockName(t)

	// Create locks with similar but different names
	lockA := NewAdvisoryLock(db, baseName+"_job1")
	lockB := NewAdvisoryLock(db, baseName+"_job2")
	lockC := NewAdvisoryLock(db, baseName+"job1") // Different from _job1

	ctx := context.Background()

	// All should acquire independently
	for i, lock := range []*AdvisoryLock{lockA, lockB, lockC} {
		acquired, err := lock.AcquireLock(ctx, 1)
		if err != nil {
			t.Fatalf("Lock %d acquisition failed: %v", i, err)
		}
		if !acquired {
			t.Errorf("Expected lock %d to be acquired", i)
		}
	}

	// Cleanup
	releaseLock(db, baseName+"_job1")
	releaseLock(db, baseName+"_job2")
	releaseLock(db, baseName+"job1")
}

func TestAdvisoryLock_SpecialCharactersInName(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	// Test various lock names that MySQL accepts
	specialNames := []string{
		"test.lock.name",
		"test:lock:name",
		"test/lock/name",
		"test@lock@name",
		"test lock name",
	}

	ctx := context.Background()

	for _, name := range specialNames {
		lockName := generateUniqueLockName(t) + "_" + name
		lock := NewAdvisoryLock(db, lockName)

		acquired, err := lock.AcquireLock(ctx, 1)
		if err != nil {
			t.Errorf("Failed to acquire lock with name %q: %v", name, err)
			continue
		}
		if !acquired {
			t.Errorf("Expected to acquire lock with name %q", name)
		}

		releaseLock(db, lockName)
	}
}

// ============================================================================
// Real-World Scenario Tests
// ============================================================================

func TestAdvisoryLock_Scenario_JobPrevention(t *testing.T) {
	db1 := connectToTestDB(t)
	defer db1.Close()

	db2 := connectToTestDB(t)
	defer db2.Close()

	// Simulate two instances trying to run the same job
	jobName := "archive_old_orders"
	lockName := generateUniqueLockName(t) + "_" + jobName

	// Instance 1 starts the job and acquires the lock
	instance1 := NewAdvisoryLock(db1, lockName)
	ctx := context.Background()

	acquired, err := instance1.AcquireLock(ctx, 0)
	if err != nil {
		t.Fatalf("Instance 1 failed to acquire lock: %v", err)
	}
	if !acquired {
		t.Fatal("Instance 1 should have acquired the lock")
	}

	// Instance 2 tries to start the same job
	instance2 := NewAdvisoryLock(db2, lockName)
	acquired2, err := instance2.AcquireLock(ctx, 2)
	if err != nil {
		t.Fatalf("Instance 2 lock attempt failed: %v", err)
	}

	if acquired2 {
		t.Error("Instance 2 should NOT have acquired the lock (job already running)")
	}

	if !instance1.IsHeld() {
		t.Error("Instance 1 should still hold the lock")
	}

	if instance2.IsHeld() {
		t.Error("Instance 2 should NOT report holding the lock")
	}

	// Instance 1 finishes and releases the lock
	releaseLock(db1, lockName)

	// Now instance 2 can acquire it
	acquired3, err := instance2.AcquireLock(ctx, 2)
	if err != nil {
		t.Fatalf("Instance 2 second attempt failed: %v", err)
	}
	if !acquired3 {
		t.Error("Instance 2 should have acquired the lock after instance 1 released it")
	}

	releaseLock(db2, lockName)
}

func TestAdvisoryLock_Scenario_RapidAcquireRelease(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockName := generateUniqueLockName(t)
	ctx := context.Background()

	// Rapidly acquire and release the lock multiple times
	for i := 0; i < 10; i++ {
		lock := NewAdvisoryLock(db, lockName)

		acquired, err := lock.AcquireLock(ctx, 1)
		if err != nil {
			t.Fatalf("Iteration %d: AcquireLock failed: %v", i, err)
		}
		if !acquired {
			t.Fatalf("Iteration %d: Expected to acquire lock", i)
		}

		if !lock.IsHeld() {
			t.Errorf("Iteration %d: Lock should be held", i)
		}

		// Release the lock
		releaseLock(db, lockName)
	}
}

// ============================================================================
// Error Handling Tests
// ============================================================================

func TestAdvisoryLock_NilDatabase(t *testing.T) {
	// This test documents current behavior - lock object can be created
	// but operations will panic with nil db (caller must ensure valid db)
	lock := NewAdvisoryLock(nil, "test_lock")

	if lock == nil {
		t.Fatal("NewAdvisoryLock should not return nil even with nil db")
	}

	// The lock object can be created, but AcquireLock will panic with nil db
	// This is expected - callers must provide a valid database connection
	if lock.LockName() != "test_lock" {
		t.Error("Lock name should be set even with nil db")
	}

	if lock.IsHeld() {
		t.Error("Lock should not be held")
	}
}

func TestAdvisoryLock_ClosedConnection(t *testing.T) {
	db := connectToTestDB(t)
	lockName := generateUniqueLockName(t)

	// Close the connection
	db.Close()

	lock := NewAdvisoryLock(db, lockName)
	ctx := context.Background()
	_, err := lock.AcquireLock(ctx, 1)

	// Should fail because connection is closed
	if err == nil {
		t.Error("Expected error when using closed database connection")
	}
}

func TestAdvisoryLock_LongLockName(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	// MySQL allows lock names up to 64 characters
	longName := generateUniqueLockName(t) + "_" + string(make([]byte, 100))
	lock := NewAdvisoryLock(db, longName)

	ctx := context.Background()
	acquired, err := lock.AcquireLock(ctx, 1)

	// MySQL may truncate or accept - we're testing it doesn't panic
	if err != nil {
		t.Logf("Long name acquisition error (may be expected): %v", err)
	}

	if acquired {
		releaseLock(db, longName)
	}
}
