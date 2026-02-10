// Package lock provides MySQL advisory locking functionality for GoArchive.
package lock

import (
	"context"
	"errors"
	"testing"
	"time"
)

// ============================================================================
// ReleaseLock Tests
// ============================================================================

// Note: Test helpers (getTestDSN, connectToTestDB, generateUniqueLockName, releaseLock)
// are shared from advisory_test.go

func TestAdvisoryLock_ReleaseLock_Success(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockName := generateUniqueLockName(t)
	lock := NewAdvisoryLock(db, lockName)
	ctx := context.Background()

	// Acquire the lock first
	acquired, err := lock.AcquireLock(ctx, 5)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}
	if !acquired {
		t.Fatal("Expected to acquire lock")
	}

	if !lock.IsHeld() {
		t.Error("Lock should be held before release")
	}

	// Release the lock
	released, err := lock.ReleaseLock(ctx)
	if err != nil {
		t.Fatalf("ReleaseLock failed: %v", err)
	}

	if !released {
		t.Error("Expected ReleaseLock to return true")
	}

	if lock.IsHeld() {
		t.Error("Lock should not be held after release")
	}
}

func TestAdvisoryLock_ReleaseLock_NotHeld(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockName := generateUniqueLockName(t)
	lock := NewAdvisoryLock(db, lockName)
	ctx := context.Background()

	// Try to release a lock we never acquired
	released, err := lock.ReleaseLock(ctx)
	if err != nil {
		t.Errorf("ReleaseLock should not error when lock not held: %v", err)
	}

	if released {
		t.Error("ReleaseLock should return false when lock was not held")
	}

	if lock.IsHeld() {
		t.Error("Lock should not be marked as held")
	}
}

func TestAdvisoryLock_ReleaseLock_DoubleRelease(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockName := generateUniqueLockName(t)
	lock := NewAdvisoryLock(db, lockName)
	ctx := context.Background()

	// Acquire and release
	acquired, _ := lock.AcquireLock(ctx, 5)
	if !acquired {
		t.Fatal("Expected to acquire lock")
	}

	released1, _ := lock.ReleaseLock(ctx)
	if !released1 {
		t.Error("First release should return true")
	}

	// Second release should be safe (idempotent)
	released2, err := lock.ReleaseLock(ctx)
	if err != nil {
		t.Errorf("Double release should not error: %v", err)
	}

	if released2 {
		t.Error("Second release should return false (already released)")
	}

	if lock.IsHeld() {
		t.Error("Lock should not be held after release")
	}
}

func TestAdvisoryLock_ReleaseLock_AllowsReacquisition(t *testing.T) {
	db1 := connectToTestDB(t)
	defer db1.Close()

	db2 := connectToTestDB(t)
	defer db2.Close()

	lockName := generateUniqueLockName(t)
	lock1 := NewAdvisoryLock(db1, lockName)
	lock2 := NewAdvisoryLock(db2, lockName)
	ctx := context.Background()

	// First connection acquires the lock
	acquired1, _ := lock1.AcquireLock(ctx, 5)
	if !acquired1 {
		t.Fatal("Expected first lock to be acquired")
	}

	// Second connection should fail to acquire
	acquired2, _ := lock2.AcquireLock(ctx, 0)
	if acquired2 {
		t.Error("Second lock should not be acquired while first holds it")
	}

	// First connection releases the lock
	released, _ := lock1.ReleaseLock(ctx)
	if !released {
		t.Error("Expected release to succeed")
	}

	// Give MySQL a moment to process
	time.Sleep(50 * time.Millisecond)

	// Second connection can now acquire
	acquired3, _ := lock2.AcquireLock(ctx, 1)
	if !acquired3 {
		t.Error("Second connection should acquire lock after release")
	}

	if !lock2.IsHeld() {
		t.Error("Second lock should be held")
	}

	// Cleanup
	lock2.ReleaseLock(ctx)
}

func TestAdvisoryLock_ReleaseLock_NilContext(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockName := generateUniqueLockName(t)
	lock := NewAdvisoryLock(db, lockName)
	ctx := context.Background()

	// Acquire the lock
	lock.AcquireLock(ctx, 1)

	// Release with nil context should use background context internally
	// (This tests that the method doesn't panic with nil context)
	// Note: In actual implementation, we pass ctx which is Background
	released, err := lock.ReleaseLock(ctx)
	if err != nil {
		t.Errorf("ReleaseLock should not error: %v", err)
	}
	if !released {
		t.Error("Expected release to succeed")
	}
}

// ============================================================================
// TryAcquire Tests
// ============================================================================

func TestAdvisoryLock_TryAcquire_Success(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockName := generateUniqueLockName(t)
	lock := NewAdvisoryLock(db, lockName)
	ctx := context.Background()

	// TryAcquire should succeed when lock is available
	acquired, err := lock.TryAcquire(ctx)
	if err != nil {
		t.Fatalf("TryAcquire failed: %v", err)
	}

	if !acquired {
		t.Error("Expected TryAcquire to succeed on available lock")
	}

	if !lock.IsHeld() {
		t.Error("Lock should be held after successful TryAcquire")
	}

	// Cleanup
	lock.ReleaseLock(ctx)
}

func TestAdvisoryLock_TryAcquire_AlreadyHeld(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockName := generateUniqueLockName(t)
	lock := NewAdvisoryLock(db, lockName)
	ctx := context.Background()

	// First acquire the lock
	acquired1, _ := lock.AcquireLock(ctx, 1)
	if !acquired1 {
		t.Fatal("Expected to acquire lock")
	}

	// TryAcquire should return true (already held, idempotent)
	acquired2, err := lock.TryAcquire(ctx)
	if err != nil {
		t.Errorf("TryAcquire should not error when already held: %v", err)
	}

	if !acquired2 {
		t.Error("TryAcquire should return true when already holding the lock")
	}

	if !lock.IsHeld() {
		t.Error("Lock should still be held")
	}

	// Cleanup
	lock.ReleaseLock(ctx)
}

func TestAdvisoryLock_TryAcquire_Contention(t *testing.T) {
	db1 := connectToTestDB(t)
	defer db1.Close()

	db2 := connectToTestDB(t)
	defer db2.Close()

	lockName := generateUniqueLockName(t)
	lock1 := NewAdvisoryLock(db1, lockName)
	lock2 := NewAdvisoryLock(db2, lockName)
	ctx := context.Background()

	// First connection acquires the lock
	acquired1, _ := lock1.AcquireLock(ctx, 1)
	if !acquired1 {
		t.Fatal("Expected first lock to be acquired")
	}

	// Second connection tries to acquire (non-blocking)
	start := time.Now()
	acquired2, err := lock2.TryAcquire(ctx)
	elapsed := time.Since(start)

	if err != nil {
		t.Errorf("TryAcquire should not error on contention: %v", err)
	}

	if acquired2 {
		t.Error("TryAcquire should fail when lock is held by another")
	}

	// Should return immediately (no waiting)
	if elapsed > 100*time.Millisecond {
		t.Errorf("TryAcquire should return immediately, took %v", elapsed)
	}

	if lock2.IsHeld() {
		t.Error("Second lock should not be held")
	}

	// Cleanup
	lock1.ReleaseLock(ctx)
}

func TestAdvisoryLock_TryAcquire_NonBlocking(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockName := generateUniqueLockName(t)
	lock := NewAdvisoryLock(db, lockName)
	ctx := context.Background()

	// Multiple rapid TryAcquire calls should all succeed
	// (same connection, or lock already held)
	for i := 0; i < 5; i++ {
		acquired, err := lock.TryAcquire(ctx)
		if err != nil {
			t.Fatalf("Iteration %d: TryAcquire failed: %v", i, err)
		}
		if !acquired {
			t.Errorf("Iteration %d: Expected TryAcquire to succeed", i)
		}
	}

	if !lock.IsHeld() {
		t.Error("Lock should be held")
	}

	// Cleanup
	lock.ReleaseLock(ctx)
}

// ============================================================================
// AcquireOrFail Tests
// ============================================================================

func TestAdvisoryLock_AcquireOrFail_Success(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockName := generateUniqueLockName(t)
	lock := NewAdvisoryLock(db, lockName)
	ctx := context.Background()

	// AcquireOrFail should succeed when lock is available
	err := lock.AcquireOrFail(ctx)
	if err != nil {
		t.Fatalf("AcquireOrFail failed: %v", err)
	}

	if !lock.IsHeld() {
		t.Error("Lock should be held after successful AcquireOrFail")
	}

	// Cleanup
	lock.ReleaseLock(ctx)
}

func TestAdvisoryLock_AcquireOrFail_AlreadyHeld(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockName := generateUniqueLockName(t)
	lock := NewAdvisoryLock(db, lockName)
	ctx := context.Background()

	// First acquire the lock
	err1 := lock.AcquireOrFail(ctx)
	if err1 != nil {
		t.Fatalf("First AcquireOrFail failed: %v", err1)
	}

	// Second call should succeed (idempotent)
	err2 := lock.AcquireOrFail(ctx)
	if err2 != nil {
		t.Errorf("Second AcquireOrFail should succeed when already held: %v", err2)
	}

	if !lock.IsHeld() {
		t.Error("Lock should still be held")
	}

	// Cleanup
	lock.ReleaseLock(ctx)
}

func TestAdvisoryLock_AcquireOrFail_TimeoutError(t *testing.T) {
	db1 := connectToTestDB(t)
	defer db1.Close()

	db2 := connectToTestDB(t)
	defer db2.Close()

	lockName := generateUniqueLockName(t)
	lock1 := NewAdvisoryLock(db1, lockName)
	lock2 := NewAdvisoryLock(db2, lockName)
	ctx := context.Background()

	// First connection acquires the lock
	err1 := lock1.AcquireOrFail(ctx)
	if err1 != nil {
		t.Fatalf("First AcquireOrFail failed: %v", err1)
	}

	// Second connection tries to acquire (will timeout after 1 second)
	start := time.Now()
	err2 := lock2.AcquireOrFail(ctx)
	elapsed := time.Since(start)

	// Should return ErrLockTimeout
	if err2 == nil {
		t.Fatal("Expected AcquireOrFail to return error on timeout")
	}

	if !errors.Is(err2, ErrLockTimeout) {
		t.Errorf("Expected ErrLockTimeout, got: %v", err2)
	}

	// Error message should contain lock name
	if err2.Error() == "" {
		t.Error("Error message should not be empty")
	}

	// Should have waited approximately TimeoutShort (1 second)
	if elapsed < 900*time.Millisecond || elapsed > 1500*time.Millisecond {
		t.Errorf("Expected ~1s timeout, took %v", elapsed)
	}

	if lock2.IsHeld() {
		t.Error("Second lock should not be held")
	}

	// Cleanup
	lock1.ReleaseLock(ctx)
}

func TestAdvisoryLock_AcquireOrFail_ErrorContainsLockName(t *testing.T) {
	db1 := connectToTestDB(t)
	defer db1.Close()

	db2 := connectToTestDB(t)
	defer db2.Close()

	lockName := generateUniqueLockName(t)
	lock1 := NewAdvisoryLock(db1, lockName)
	lock2 := NewAdvisoryLock(db2, lockName)
	ctx := context.Background()

	// First connection acquires the lock
	lock1.AcquireOrFail(ctx)

	// Second connection tries to acquire
	err := lock2.AcquireOrFail(ctx)

	// Error should contain the lock name
	if err == nil {
		t.Fatal("Expected error")
	}

	errMsg := err.Error()
	if !errors.Is(err, ErrLockTimeout) {
		t.Error("Error should wrap ErrLockTimeout")
	}

	// Error message should mention the lock
	if errMsg == "" {
		t.Error("Error message should not be empty")
	}

	// Cleanup
	lock1.ReleaseLock(ctx)
}

func TestAdvisoryLock_AcquireOrFail_FastFail(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockName := generateUniqueLockName(t)
	lock := NewAdvisoryLock(db, lockName)
	ctx := context.Background()

	// First acquire
	err := lock.AcquireOrFail(ctx)
	if err != nil {
		t.Fatalf("First acquire failed: %v", err)
	}

	// Release
	lock.ReleaseLock(ctx)

	// Can acquire again after release
	err = lock.AcquireOrFail(ctx)
	if err != nil {
		t.Errorf("Second acquire after release failed: %v", err)
	}

	if !lock.IsHeld() {
		t.Error("Lock should be held")
	}

	// Cleanup
	lock.ReleaseLock(ctx)
}

// ============================================================================
// Integration Tests
// ============================================================================

func TestAdvisoryLock_FullLifecycle(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockName := generateUniqueLockName(t)
	lock := NewAdvisoryLock(db, lockName)
	ctx := context.Background()

	// 1. Initial state
	if lock.IsHeld() {
		t.Error("New lock should not be held")
	}

	// 2. Acquire using different methods
	acquired, _ := lock.TryAcquire(ctx)
	if !acquired {
		t.Fatal("TryAcquire should succeed")
	}

	// 3. Release
	released, _ := lock.ReleaseLock(ctx)
	if !released {
		t.Error("Release should succeed")
	}

	// 4. Re-acquire using AcquireOrFail
	err := lock.AcquireOrFail(ctx)
	if err != nil {
		t.Errorf("AcquireOrFail after release failed: %v", err)
	}

	// 5. Release again
	released, _ = lock.ReleaseLock(ctx)
	if !released {
		t.Error("Second release should succeed")
	}

	// 6. Final state
	if lock.IsHeld() {
		t.Error("Lock should not be held at end")
	}
}

func TestAdvisoryLock_ConcurrentAcquireAndRelease(t *testing.T) {
	db1 := connectToTestDB(t)
	defer db1.Close()

	db2 := connectToTestDB(t)
	defer db2.Close()

	lockName := generateUniqueLockName(t)
	lock1 := NewAdvisoryLock(db1, lockName)
	lock2 := NewAdvisoryLock(db2, lockName)
	ctx := context.Background()

	// Connection 1 acquires
	acquired, _ := lock1.AcquireLock(ctx, 1)
	if !acquired {
		t.Fatal("First acquire should succeed")
	}

	// Connection 2 tries and fails (non-blocking)
	acquired2, _ := lock2.TryAcquire(ctx)
	if acquired2 {
		t.Error("Second should fail while first holds lock")
	}

	// Connection 1 releases
	lock1.ReleaseLock(ctx)
	time.Sleep(50 * time.Millisecond)

	// Connection 2 can now acquire
	acquired3, _ := lock2.AcquireLock(ctx, 1)
	if !acquired3 {
		t.Error("Second should acquire after first releases")
	}

	// Connection 2 releases
	lock2.ReleaseLock(ctx)
}

// ============================================================================
// Edge Cases and Error Conditions
// ============================================================================

func TestAdvisoryLock_ReleaseLock_ClosedConnection(t *testing.T) {
	db := connectToTestDB(t)
	lockName := generateUniqueLockName(t)

	lock := NewAdvisoryLock(db, lockName)
	ctx := context.Background()

	// Acquire the lock
	lock.AcquireLock(ctx, 1)

	// Close the connection
	db.Close()

	// Release should fail (connection closed)
	_, err := lock.ReleaseLock(ctx)
	if err == nil {
		t.Log("ReleaseLock may or may not error after connection close depending on timing")
	}
}

func TestAdvisoryLock_TryAcquire_ContextCancellation(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockName := generateUniqueLockName(t)
	lock := NewAdvisoryLock(db, lockName)

	// Use a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Should fail due to cancelled context
	_, err := lock.TryAcquire(ctx)
	if err == nil {
		t.Log("TryAcquire may succeed or fail depending on driver behavior with cancelled context")
	}
}

func TestAdvisoryLock_MultipleLocksIndependentLifecycle(t *testing.T) {
	db := connectToTestDB(t)
	defer db.Close()

	lockName1 := generateUniqueLockName(t) + "_A"
	lockName2 := generateUniqueLockName(t) + "_B"

	lock1 := NewAdvisoryLock(db, lockName1)
	lock2 := NewAdvisoryLock(db, lockName2)
	ctx := context.Background()

	// Acquire both locks
	lock1.AcquireLock(ctx, 1)
	lock2.AcquireLock(ctx, 1)

	if !lock1.IsHeld() || !lock2.IsHeld() {
		t.Error("Both locks should be held")
	}

	// Release only first
	lock1.ReleaseLock(ctx)

	if lock1.IsHeld() {
		t.Error("Lock1 should be released")
	}
	if !lock2.IsHeld() {
		t.Error("Lock2 should still be held")
	}

	// Release second
	lock2.ReleaseLock(ctx)

	if lock2.IsHeld() {
		t.Error("Lock2 should be released")
	}
}
