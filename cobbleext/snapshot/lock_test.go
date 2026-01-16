// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package snapshot

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func TestDistributedLock_BasicAcquireRelease(t *testing.T) {
	ctx := context.Background()
	storage := newTrackedStorage()

	lock := NewDistributedLock(storage, "test/", "mylock")

	// Acquire lock
	err := lock.Acquire(ctx, LockOptions{
		TTL:     1 * time.Minute,
		Purpose: "test operation",
	})
	require.NoError(t, err)

	// Verify we hold the lock
	require.True(t, lock.IsHeldByUs(ctx))

	// Get lock info
	info, err := lock.GetInfo(ctx)
	require.NoError(t, err)
	require.Equal(t, lock.holderID, info.HolderID)
	require.Equal(t, "test operation", info.Purpose)
	require.False(t, info.IsExpired())

	// Release lock
	err = lock.Release(ctx)
	require.NoError(t, err)

	// Verify lock is released
	require.False(t, lock.IsHeldByUs(ctx))
}

func TestDistributedLock_AcquireWhenHeldByOther(t *testing.T) {
	ctx := context.Background()
	storage := newTrackedStorage()

	lock1 := NewDistributedLock(storage, "test/", "mylock")
	lock2 := NewDistributedLock(storage, "test/", "mylock")

	// First lock acquires
	err := lock1.Acquire(ctx, LockOptions{TTL: 1 * time.Minute})
	require.NoError(t, err)

	// Second lock should fail immediately without retries
	err = lock2.Acquire(ctx, LockOptions{
		TTL:        1 * time.Minute,
		MaxRetries: 0,
	})
	require.ErrorIs(t, err, ErrLockHeld)

	// Release first lock
	err = lock1.Release(ctx)
	require.NoError(t, err)

	// Now second lock should succeed
	err = lock2.Acquire(ctx, LockOptions{TTL: 1 * time.Minute})
	require.NoError(t, err)

	err = lock2.Release(ctx)
	require.NoError(t, err)
}

func TestDistributedLock_AcquireWithRetry(t *testing.T) {
	ctx := context.Background()
	storage := newTrackedStorage()

	lock1 := NewDistributedLock(storage, "test/", "mylock")
	lock2 := NewDistributedLock(storage, "test/", "mylock")

	// First lock acquires
	err := lock1.Acquire(ctx, LockOptions{TTL: 1 * time.Minute})
	require.NoError(t, err)

	// Start a goroutine to release the lock after a short delay
	go func() {
		time.Sleep(200 * time.Millisecond)
		lock1.Release(ctx)
	}()

	// Second lock should succeed after retry
	start := time.Now()
	err = lock2.Acquire(ctx, LockOptions{
		TTL:           1 * time.Minute,
		RetryInterval: 50 * time.Millisecond,
		MaxRetries:    10,
	})
	require.NoError(t, err)
	require.True(t, time.Since(start) >= 150*time.Millisecond, "should have waited for retry")

	err = lock2.Release(ctx)
	require.NoError(t, err)
}

func TestDistributedLock_Expiration(t *testing.T) {
	ctx := context.Background()
	storage := newTrackedStorage()

	lock1 := NewDistributedLock(storage, "test/", "mylock")
	lock2 := NewDistributedLock(storage, "test/", "mylock")

	// First lock acquires with very short TTL
	err := lock1.Acquire(ctx, LockOptions{TTL: 100 * time.Millisecond})
	require.NoError(t, err)

	// Wait for lock to expire
	time.Sleep(150 * time.Millisecond)

	// Lock info should show expired
	info, err := lock1.GetInfo(ctx)
	require.NoError(t, err)
	require.True(t, info.IsExpired())

	// Second lock should be able to acquire (expired lock)
	err = lock2.Acquire(ctx, LockOptions{
		TTL:        1 * time.Minute,
		MaxRetries: 0,
	})
	require.NoError(t, err)

	err = lock2.Release(ctx)
	require.NoError(t, err)
}

func TestDistributedLock_Refresh(t *testing.T) {
	ctx := context.Background()
	storage := newTrackedStorage()

	lock := NewDistributedLock(storage, "test/", "mylock")

	// Acquire lock with short TTL
	err := lock.Acquire(ctx, LockOptions{TTL: 200 * time.Millisecond})
	require.NoError(t, err)

	// Get initial expiration
	info1, err := lock.GetInfo(ctx)
	require.NoError(t, err)
	initialExpiry := info1.ExpiresAt

	// Wait a bit
	time.Sleep(50 * time.Millisecond)

	// Refresh the lock
	err = lock.Refresh(ctx)
	require.NoError(t, err)

	// Verify expiration was extended
	info2, err := lock.GetInfo(ctx)
	require.NoError(t, err)
	require.True(t, info2.ExpiresAt.After(initialExpiry), "expiry should be extended")

	err = lock.Release(ctx)
	require.NoError(t, err)
}

func TestDistributedLock_RefreshNotHeld(t *testing.T) {
	ctx := context.Background()
	storage := newTrackedStorage()

	lock1 := NewDistributedLock(storage, "test/", "mylock")
	lock2 := NewDistributedLock(storage, "test/", "mylock")

	// First lock acquires
	err := lock1.Acquire(ctx, LockOptions{TTL: 1 * time.Minute})
	require.NoError(t, err)

	// Second lock tries to refresh - should fail
	err = lock2.Refresh(ctx)
	require.ErrorIs(t, err, ErrLockNotHeld)

	err = lock1.Release(ctx)
	require.NoError(t, err)
}

func TestDistributedLock_ReleaseNotHeld(t *testing.T) {
	ctx := context.Background()
	storage := newTrackedStorage()

	lock1 := NewDistributedLock(storage, "test/", "mylock")
	lock2 := NewDistributedLock(storage, "test/", "mylock")

	// First lock acquires
	err := lock1.Acquire(ctx, LockOptions{TTL: 1 * time.Minute})
	require.NoError(t, err)

	// Second lock tries to release - should fail
	err = lock2.Release(ctx)
	require.ErrorIs(t, err, ErrLockNotHeld)

	err = lock1.Release(ctx)
	require.NoError(t, err)
}

func TestDistributedLock_ForceRelease(t *testing.T) {
	ctx := context.Background()
	storage := newTrackedStorage()

	lock1 := NewDistributedLock(storage, "test/", "mylock")
	lock2 := NewDistributedLock(storage, "test/", "mylock")

	// First lock acquires
	err := lock1.Acquire(ctx, LockOptions{TTL: 1 * time.Minute})
	require.NoError(t, err)

	// Second lock force releases
	err = lock2.ForceRelease(ctx)
	require.NoError(t, err)

	// First lock is no longer held
	require.False(t, lock1.IsHeldByUs(ctx))

	// Either lock can now acquire
	err = lock2.Acquire(ctx, LockOptions{TTL: 1 * time.Minute})
	require.NoError(t, err)

	err = lock2.Release(ctx)
	require.NoError(t, err)
}

func TestDistributedLock_ReacquireSameHolder(t *testing.T) {
	ctx := context.Background()
	storage := newTrackedStorage()

	lock := NewDistributedLock(storage, "test/", "mylock")

	// Acquire lock
	err := lock.Acquire(ctx, LockOptions{TTL: 1 * time.Minute})
	require.NoError(t, err)

	// Reacquire same lock (should succeed and refresh)
	err = lock.Acquire(ctx, LockOptions{TTL: 2 * time.Minute})
	require.NoError(t, err)

	// Still held by us
	require.True(t, lock.IsHeldByUs(ctx))

	err = lock.Release(ctx)
	require.NoError(t, err)
}

func TestDistributedLock_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	storage := newTrackedStorage()

	lock1 := NewDistributedLock(storage, "test/", "mylock")
	lock2 := NewDistributedLock(storage, "test/", "mylock")

	// First lock acquires
	err := lock1.Acquire(ctx, LockOptions{TTL: 1 * time.Minute})
	require.NoError(t, err)

	// Cancel context after a short delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	// Second lock should fail due to context cancellation
	err = lock2.Acquire(ctx, LockOptions{
		TTL:           1 * time.Minute,
		RetryInterval: 50 * time.Millisecond,
		MaxRetries:    100, // Would take a long time without cancellation
	})
	require.ErrorIs(t, err, context.Canceled)

	// Release using background context
	err = lock1.Release(context.Background())
	require.NoError(t, err)
}

func TestDistributedLock_ConcurrentAcquire(t *testing.T) {
	ctx := context.Background()
	storage := newTrackedStorage()

	const numGoroutines = 10
	var successCount atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			lock := NewDistributedLock(storage, "test/", "mylock")
			err := lock.Acquire(ctx, LockOptions{
				TTL:        1 * time.Minute,
				MaxRetries: 0, // No retries - only one should succeed
			})
			if err == nil {
				successCount.Add(1)
				// Hold lock briefly then release
				time.Sleep(10 * time.Millisecond)
				lock.Release(ctx)
			}
		}()
	}

	wg.Wait()

	// Exactly one should have succeeded on first attempt
	// (others may have succeeded after the first released)
	require.GreaterOrEqual(t, successCount.Load(), int32(1))
}

func TestDistributedLock_MultipleLocks(t *testing.T) {
	ctx := context.Background()
	storage := newTrackedStorage()

	lockA := NewDistributedLock(storage, "test/", "lock-a")
	lockB := NewDistributedLock(storage, "test/", "lock-b")

	// Both locks can be acquired simultaneously
	err := lockA.Acquire(ctx, LockOptions{TTL: 1 * time.Minute})
	require.NoError(t, err)

	err = lockB.Acquire(ctx, LockOptions{TTL: 1 * time.Minute})
	require.NoError(t, err)

	require.True(t, lockA.IsHeldByUs(ctx))
	require.True(t, lockB.IsHeldByUs(ctx))

	err = lockA.Release(ctx)
	require.NoError(t, err)

	err = lockB.Release(ctx)
	require.NoError(t, err)
}

func TestDistributedLock_ReleaseNonexistent(t *testing.T) {
	ctx := context.Background()
	storage := newTrackedStorage()

	lock := NewDistributedLock(storage, "test/", "mylock")

	// Releasing a lock that was never acquired should not error
	err := lock.Release(ctx)
	require.NoError(t, err)
}

// =============================================================================
// Test the WithLock wrapper functions
// =============================================================================

func TestCreateCASSnapshotWithLock(t *testing.T) {
	ctx := context.Background()
	storage := newTrackedStorage()
	tmpDir := t.TempDir()

	// Create a real Pebble database
	dbPath := filepath.Join(tmpDir, "testdb")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	require.NoError(t, err)
	defer db.Close()

	// Write some data
	require.NoError(t, db.Set([]byte("key"), []byte("value"), nil))
	require.NoError(t, db.Flush())

	adapter := &realDBAdapter{db: db, path: dbPath}

	// Create snapshot with lock
	manifest, err := CreateCASSnapshotWithLock(ctx, adapter, LockedCASOptions{
		CASSnapshotOptions: CASSnapshotOptions{
			Storage:    storage,
			Prefix:     "cas/",
			SnapshotID: "test-snap",
		},
		LockTTL:           1 * time.Minute,
		LockRetryInterval: 100 * time.Millisecond,
		LockMaxRetries:    3,
	})
	require.NoError(t, err)
	require.NotNil(t, manifest)
	require.Equal(t, "test-snap", manifest.ID)

	// Lock should be released after function returns
	lock := NewDistributedLock(storage, "cas/", "catalog")
	err = lock.Acquire(ctx, LockOptions{TTL: 1 * time.Minute, MaxRetries: 0})
	require.NoError(t, err)
	err = lock.Release(ctx)
	require.NoError(t, err)
}

func TestDeleteCASSnapshotWithLock(t *testing.T) {
	ctx := context.Background()
	storage := newTrackedStorage()
	tmpDir := t.TempDir()

	// Create a real Pebble database
	dbPath := filepath.Join(tmpDir, "testdb")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	require.NoError(t, err)
	defer db.Close()

	// Write some data and create a snapshot
	require.NoError(t, db.Set([]byte("key"), []byte("value"), nil))
	require.NoError(t, db.Flush())

	adapter := &realDBAdapter{db: db, path: dbPath}

	manifest, err := CreateCASSnapshot(ctx, adapter, CASSnapshotOptions{
		Storage:    storage,
		Prefix:     "cas/",
		SnapshotID: "test-snap",
	})
	require.NoError(t, err)
	require.NotNil(t, manifest)

	// Delete with lock
	err = DeleteCASSnapshotWithLock(ctx, storage, "cas/", "test-snap", LockOptions{
		TTL:        1 * time.Minute,
		MaxRetries: 3,
	})
	require.NoError(t, err)

	// Verify snapshot is deleted from catalog
	catalog, err := LoadCASCatalog(ctx, storage, "cas/")
	require.NoError(t, err)
	_, found := catalog.GetSnapshot("test-snap")
	require.False(t, found)
}

func TestGarbageCollectCASWithLock(t *testing.T) {
	ctx := context.Background()
	storage := newTrackedStorage()
	tmpDir := t.TempDir()

	// Create a real Pebble database
	dbPath := filepath.Join(tmpDir, "testdb")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	require.NoError(t, err)
	defer db.Close()

	adapter := &realDBAdapter{db: db, path: dbPath}

	// Create multiple snapshots
	for i := 0; i < 3; i++ {
		require.NoError(t, db.Set([]byte("key"), []byte("value"), nil))
		require.NoError(t, db.Flush())

		_, err := CreateCASSnapshot(ctx, adapter, CASSnapshotOptions{
			Storage:    storage,
			Prefix:     "cas/",
			SnapshotID: time.Now().Format("20060102T150405.000000000Z"),
		})
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond) // Ensure unique IDs
	}

	// Run GC with lock - keep only 1 snapshot
	deleted, orphaned, err := GarbageCollectCASWithLock(ctx, CASGCOptions{
		Storage:  storage,
		Prefix:   "cas/",
		KeepLast: 1,
	}, LockOptions{
		TTL:        1 * time.Minute,
		MaxRetries: 3,
	})
	require.NoError(t, err)
	require.Len(t, deleted, 2) // 2 snapshots deleted
	// Orphaned blobs depend on whether snapshots share blobs -
	// since each snapshot adds new data, there will be orphaned blobs
	t.Logf("GC deleted %d snapshots, %d orphaned blobs", len(deleted), len(orphaned))
}

func TestCompactCASSnapshotWithLock(t *testing.T) {
	ctx := context.Background()
	storage := newTrackedStorage()
	tmpDir := t.TempDir()

	// Create a real Pebble database
	dbPath := filepath.Join(tmpDir, "testdb")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	require.NoError(t, err)
	defer db.Close()

	adapter := &realDBAdapter{db: db, path: dbPath}

	// Create base snapshot
	require.NoError(t, db.Set([]byte("key1"), []byte("value1"), nil))
	require.NoError(t, db.Flush())

	base, err := CreateCASSnapshot(ctx, adapter, CASSnapshotOptions{
		Storage:    storage,
		Prefix:     "cas/",
		SnapshotID: "base",
	})
	require.NoError(t, err)

	// Create another snapshot with a parent reference
	require.NoError(t, db.Set([]byte("key2"), []byte("value2"), nil))
	require.NoError(t, db.Flush())

	_, err = CreateCASSnapshot(ctx, adapter, CASSnapshotOptions{
		Storage:    storage,
		Prefix:     "cas/",
		SnapshotID: "child",
		ParentID:   base.ID,
	})
	require.NoError(t, err)

	// Compact with lock
	compacted, err := CompactCASSnapshotWithLock(ctx, CompactCASOptions{
		Storage:    storage,
		Prefix:     "cas/",
		SnapshotID: "child",
	}, LockOptions{
		TTL:        1 * time.Minute,
		MaxRetries: 3,
	})
	require.NoError(t, err)
	require.NotNil(t, compacted)
	require.Empty(t, compacted.ParentID, "compacted snapshot should have no parent")
}

func TestWithLock_LockContention(t *testing.T) {
	ctx := context.Background()
	storage := newTrackedStorage()
	tmpDir := t.TempDir()

	// Pre-acquire the lock
	existingLock := NewDistributedLock(storage, "cas/", "catalog")
	err := existingLock.Acquire(ctx, LockOptions{TTL: 1 * time.Minute})
	require.NoError(t, err)

	// Create a real Pebble database
	dbPath := filepath.Join(tmpDir, "testdb")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	require.NoError(t, err)
	defer db.Close()

	adapter := &realDBAdapter{db: db, path: dbPath}

	// Try to create snapshot - should fail because lock is held
	_, err = CreateCASSnapshotWithLock(ctx, adapter, LockedCASOptions{
		CASSnapshotOptions: CASSnapshotOptions{
			Storage:    storage,
			Prefix:     "cas/",
			SnapshotID: "test",
		},
		LockTTL:           100 * time.Millisecond,
		LockRetryInterval: 50 * time.Millisecond,
		LockMaxRetries:    2, // Only 2 retries
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to acquire lock")

	err = existingLock.Release(ctx)
	require.NoError(t, err)
}
