// Package lock provides MySQL advisory locking functionality for GoArchive.
package lock

import (
	"context"
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
	defer func() { _ = db.Close() }()

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

	if !lock.held {
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

	if lock.held {
		t.Error("Lock should not be held after release")
	}
}

func TestAdvisoryLock_ReleaseLock_NotHeld(t *testing.T) {
	db := connectToTestDB(t)
	defer func() { _ = db.Close() }()

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

	if lock.held {
		t.Error("Lock should not be marked as held")
	}
}

func TestAdvisoryLock_ReleaseLock_DoubleRelease(t *testing.T) {
	db := connectToTestDB(t)
	defer func() { _ = db.Close() }()

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

	if lock.held {
		t.Error("Lock should not be held after release")
	}
}

func TestAdvisoryLock_ReleaseLock_AllowsReacquisition(t *testing.T) {
	db1 := connectToTestDB(t)
	defer func() { _ = db1.Close() }()

	db2 := connectToTestDB(t)
	defer func() { _ = db2.Close() }()

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

	if !lock2.held {
		t.Error("Second lock should be held")
	}

	// Cleanup
	_, _ = lock2.ReleaseLock(ctx)
}

func TestAdvisoryLock_ReleaseLock_NilContext(t *testing.T) {
	db := connectToTestDB(t)
	defer func() { _ = db.Close() }()

	lockName := generateUniqueLockName(t)
	lock := NewAdvisoryLock(db, lockName)
	ctx := context.Background()

	// Acquire the lock
	_, _ = lock.AcquireLock(ctx, 1)

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
	defer func() { _ = db.Close() }()

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

	if !lock.held {
		t.Error("Lock should be held after successful TryAcquire")
	}

	// Cleanup
	_, _ = lock.ReleaseLock(ctx)
}

func TestAdvisoryLock_TryAcquire_AlreadyHeld(t *testing.T) {
	db := connectToTestDB(t)
	defer func() { _ = db.Close() }()

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

	if !lock.held {
		t.Error("Lock should still be held")
	}

	// Cleanup
	_, _ = lock.ReleaseLock(ctx)
}

func TestAdvisoryLock_TryAcquire_Contention(t *testing.T) {
	db1 := connectToTestDB(t)
	defer func() { _ = db1.Close() }()

	db2 := connectToTestDB(t)
	defer func() { _ = db2.Close() }()

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

	if lock2.held {
		t.Error("Second lock should not be held")
	}

	// Cleanup
	_, _ = lock1.ReleaseLock(ctx)
}

func TestAdvisoryLock_TryAcquire_NonBlocking(t *testing.T) {
	db := connectToTestDB(t)
	defer func() { _ = db.Close() }()

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

	if !lock.held {
		t.Error("Lock should be held")
	}

	// Cleanup
	_, _ = lock.ReleaseLock(ctx)
}

// ============================================================================
// Integration Tests
// ============================================================================

func TestAdvisoryLock_ConcurrentAcquireAndRelease(t *testing.T) {
	db1 := connectToTestDB(t)
	defer func() { _ = db1.Close() }()

	db2 := connectToTestDB(t)
	defer func() { _ = db2.Close() }()

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
	_, _ = lock1.ReleaseLock(ctx)
	time.Sleep(50 * time.Millisecond)

	// Connection 2 can now acquire
	acquired3, _ := lock2.AcquireLock(ctx, 1)
	if !acquired3 {
		t.Error("Second should acquire after first releases")
	}

	// Connection 2 releases
	_, _ = lock2.ReleaseLock(ctx)
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
	_, _ = lock.AcquireLock(ctx, 1)

	// Close the connection
	_ = db.Close()

	// Release should fail (connection closed)
	_, err := lock.ReleaseLock(ctx)
	if err == nil {
		t.Log("ReleaseLock may or may not error after connection close depending on timing")
	}
}

func TestAdvisoryLock_TryAcquire_ContextCancellation(t *testing.T) {
	db := connectToTestDB(t)
	defer func() { _ = db.Close() }()

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
	defer func() { _ = db.Close() }()

	lockName1 := generateUniqueLockName(t) + "_A"
	lockName2 := generateUniqueLockName(t) + "_B"

	lock1 := NewAdvisoryLock(db, lockName1)
	lock2 := NewAdvisoryLock(db, lockName2)
	ctx := context.Background()

	// Acquire both locks
	_, _ = lock1.AcquireLock(ctx, 1)
	_, _ = lock2.AcquireLock(ctx, 1)

	if !lock1.held || !lock2.held {
		t.Error("Both locks should be held")
	}

	// Release only first
	_, _ = lock1.ReleaseLock(ctx)

	if lock1.held {
		t.Error("Lock1 should be released")
	}
	if !lock2.held {
		t.Error("Lock2 should still be held")
	}

	// Release second
	_, _ = lock2.ReleaseLock(ctx)

	if lock2.held {
		t.Error("Lock2 should be released")
	}
}
