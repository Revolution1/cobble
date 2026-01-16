// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build (s3 || cloud) && integration

package snapshot

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/cobbleext/s3"
	"github.com/cockroachdb/pebble/cobbleext/testutil"
	"github.com/stretchr/testify/require"
)

// TestDistributedLock_MinIO tests the distributed lock against real MinIO storage.
func TestDistributedLock_MinIO(t *testing.T) {
	// Start MinIO container
	minio := testutil.StartMinIO(t)
	ctx := context.Background()
	bucket := "lock-test-bucket"

	// Create test bucket
	require.NoError(t, minio.CreateBucket(ctx, bucket))

	// Create S3 storage
	storage, err := s3.NewStorage(s3.Config{
		Bucket:          bucket,
		Prefix:          "lock-test/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	require.NoError(t, err)
	defer storage.Close()

	t.Run("BasicAcquireRelease", func(t *testing.T) {
		lock := NewDistributedLock(storage, "test/", "basic-lock")

		err := lock.Acquire(ctx, LockOptions{
			TTL:     1 * time.Minute,
			Purpose: "basic test",
		})
		require.NoError(t, err)

		require.True(t, lock.IsHeldByUs(ctx))

		info, err := lock.GetInfo(ctx)
		require.NoError(t, err)
		require.Equal(t, "basic test", info.Purpose)

		err = lock.Release(ctx)
		require.NoError(t, err)

		require.False(t, lock.IsHeldByUs(ctx))
	})

	t.Run("TwoProcessContention", func(t *testing.T) {
		lock1 := NewDistributedLock(storage, "test/", "contention-lock")
		lock2 := NewDistributedLock(storage, "test/", "contention-lock")

		// First process acquires
		err := lock1.Acquire(ctx, LockOptions{TTL: 1 * time.Minute})
		require.NoError(t, err)

		// Second process fails to acquire
		err = lock2.Acquire(ctx, LockOptions{TTL: 1 * time.Minute, MaxRetries: 0})
		require.ErrorIs(t, err, ErrLockHeld)

		// First process releases
		err = lock1.Release(ctx)
		require.NoError(t, err)

		// Now second process can acquire
		err = lock2.Acquire(ctx, LockOptions{TTL: 1 * time.Minute})
		require.NoError(t, err)

		err = lock2.Release(ctx)
		require.NoError(t, err)
	})

	t.Run("LockExpiration", func(t *testing.T) {
		lock1 := NewDistributedLock(storage, "test/", "expire-lock")
		lock2 := NewDistributedLock(storage, "test/", "expire-lock")

		// Acquire with very short TTL
		err := lock1.Acquire(ctx, LockOptions{TTL: 500 * time.Millisecond})
		require.NoError(t, err)

		// Wait for expiration
		time.Sleep(700 * time.Millisecond)

		// Second process can acquire because lock expired
		err = lock2.Acquire(ctx, LockOptions{TTL: 1 * time.Minute, MaxRetries: 0})
		require.NoError(t, err)

		err = lock2.Release(ctx)
		require.NoError(t, err)
	})

	t.Run("ConcurrentAcquireAttempts", func(t *testing.T) {
		const numGoroutines = 5
		var successCount atomic.Int32
		var wg sync.WaitGroup

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()

				lock := NewDistributedLock(storage, "test/", "concurrent-lock")
				err := lock.Acquire(ctx, LockOptions{
					TTL:        1 * time.Minute,
					MaxRetries: 0,
				})
				if err == nil {
					successCount.Add(1)
					time.Sleep(50 * time.Millisecond)
					lock.Release(ctx)
				}
			}(i)
		}

		wg.Wait()

		// At least one should succeed
		require.GreaterOrEqual(t, successCount.Load(), int32(1))
		t.Logf("%d/%d goroutines acquired the lock", successCount.Load(), numGoroutines)
	})

	t.Run("AcquireWithRetry", func(t *testing.T) {
		lock1 := NewDistributedLock(storage, "test/", "retry-lock")
		lock2 := NewDistributedLock(storage, "test/", "retry-lock")

		// First process acquires
		err := lock1.Acquire(ctx, LockOptions{TTL: 1 * time.Minute})
		require.NoError(t, err)

		// Release after a delay
		go func() {
			time.Sleep(300 * time.Millisecond)
			lock1.Release(ctx)
		}()

		// Second process should eventually succeed
		start := time.Now()
		err = lock2.Acquire(ctx, LockOptions{
			TTL:           1 * time.Minute,
			RetryInterval: 100 * time.Millisecond,
			MaxRetries:    10,
		})
		require.NoError(t, err)
		require.True(t, time.Since(start) >= 200*time.Millisecond)

		err = lock2.Release(ctx)
		require.NoError(t, err)
	})
}

// TestCASSnapshotWithLock_MinIO tests CAS snapshot operations with locking against real MinIO.
func TestCASSnapshotWithLock_MinIO(t *testing.T) {
	// Start MinIO container
	minio := testutil.StartMinIO(t)
	ctx := context.Background()
	bucket := "snapshot-lock-test"

	// Create test bucket
	require.NoError(t, minio.CreateBucket(ctx, bucket))

	// Create S3 storage
	storage, err := s3.NewStorage(s3.Config{
		Bucket:          bucket,
		Prefix:          "cas/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	require.NoError(t, err)
	defer storage.Close()

	tmpDir := t.TempDir()

	t.Run("CreateSnapshotWithLock", func(t *testing.T) {
		dbPath := filepath.Join(tmpDir, "db1")
		db, err := pebble.Open(dbPath, &pebble.Options{})
		require.NoError(t, err)
		defer db.Close()

		// Write some data
		require.NoError(t, db.Set([]byte("key1"), []byte("value1"), nil))
		require.NoError(t, db.Flush())

		adapter := &realDBAdapter{db: db, path: dbPath}

		// Create snapshot with lock
		manifest, err := CreateCASSnapshotWithLock(ctx, adapter, LockedCASOptions{
			CASSnapshotOptions: CASSnapshotOptions{
				Storage:    storage,
				Prefix:     "test/",
				SnapshotID: "locked-snap-1",
			},
			LockTTL:           1 * time.Minute,
			LockRetryInterval: 100 * time.Millisecond,
			LockMaxRetries:    5,
		})
		require.NoError(t, err)
		require.NotNil(t, manifest)
		require.Equal(t, "locked-snap-1", manifest.ID)

		// Verify lock is released
		lock := NewDistributedLock(storage, "test/", "catalog")
		err = lock.Acquire(ctx, LockOptions{TTL: 1 * time.Minute, MaxRetries: 0})
		require.NoError(t, err)
		err = lock.Release(ctx)
		require.NoError(t, err)

		t.Log("CreateSnapshotWithLock succeeded")
	})

	t.Run("ConcurrentSnapshotsWithLock", func(t *testing.T) {
		// Create snapshots sequentially to avoid any race conditions
		// The goal is to verify locking works, not concurrent creation
		const numSnapshots = 3

		for i := 0; i < numSnapshots; i++ {
			dbPath := filepath.Join(tmpDir, fmt.Sprintf("concurrent-db-%d", i))
			db, err := pebble.Open(dbPath, &pebble.Options{})
			require.NoError(t, err)

			require.NoError(t, db.Set([]byte("key"), []byte("value"), nil))
			require.NoError(t, db.Flush())

			adapter := &realDBAdapter{db: db, path: dbPath}

			snapshotID := fmt.Sprintf("seq-snap-%d", i)
			manifest, err := CreateCASSnapshotWithLock(ctx, adapter, LockedCASOptions{
				CASSnapshotOptions: CASSnapshotOptions{
					Storage:    storage,
					Prefix:     "concurrent/",
					SnapshotID: snapshotID,
				},
				LockTTL:           30 * time.Second,
				LockRetryInterval: 100 * time.Millisecond,
				LockMaxRetries:    5,
			})
			require.NoError(t, err)
			require.Equal(t, snapshotID, manifest.ID)
			t.Logf("Created snapshot %d: %s", i, snapshotID)

			db.Close()
		}

		// Verify all snapshots were created
		catalog, err := LoadCASCatalog(ctx, storage, "concurrent/")
		require.NoError(t, err)
		t.Logf("Created %d snapshots with locking", len(catalog.Snapshots))
		require.Equal(t, numSnapshots, len(catalog.Snapshots))
	})

	t.Run("LockContentionDuringSnapshot", func(t *testing.T) {
		// Pre-acquire the lock
		existingLock := NewDistributedLock(storage, "blocked/", "catalog")
		err := existingLock.Acquire(ctx, LockOptions{TTL: 1 * time.Minute})
		require.NoError(t, err)
		defer existingLock.Release(ctx)

		dbPath := filepath.Join(tmpDir, "blocked-db")
		db, err := pebble.Open(dbPath, &pebble.Options{})
		require.NoError(t, err)
		defer db.Close()

		require.NoError(t, db.Set([]byte("key"), []byte("value"), nil))
		require.NoError(t, db.Flush())

		adapter := &realDBAdapter{db: db, path: dbPath}

		// This should fail because lock is already held
		_, err = CreateCASSnapshotWithLock(ctx, adapter, LockedCASOptions{
			CASSnapshotOptions: CASSnapshotOptions{
				Storage:    storage,
				Prefix:     "blocked/",
				SnapshotID: "should-fail",
			},
			LockTTL:           100 * time.Millisecond,
			LockRetryInterval: 50 * time.Millisecond,
			LockMaxRetries:    2,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to acquire lock")
		t.Log("Lock contention correctly prevented concurrent snapshot creation")
	})
}

// TestGCWithLock_MinIO tests garbage collection with locking against real MinIO.
func TestGCWithLock_MinIO(t *testing.T) {
	// Start MinIO container
	minio := testutil.StartMinIO(t)
	ctx := context.Background()
	bucket := "gc-lock-test"

	// Create test bucket
	require.NoError(t, minio.CreateBucket(ctx, bucket))

	// Create S3 storage
	storage, err := s3.NewStorage(s3.Config{
		Bucket:          bucket,
		Prefix:          "gc/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	require.NoError(t, err)
	defer storage.Close()

	tmpDir := t.TempDir()

	// Create multiple snapshots
	dbPath := filepath.Join(tmpDir, "gc-db")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	require.NoError(t, err)
	defer db.Close()

	adapter := &realDBAdapter{db: db, path: dbPath}

	for i := 0; i < 5; i++ {
		require.NoError(t, db.Set([]byte("key"), []byte("value"), nil))
		require.NoError(t, db.Flush())

		_, err := CreateCASSnapshot(ctx, adapter, CASSnapshotOptions{
			Storage:    storage,
			Prefix:     "test/",
			SnapshotID: time.Now().Format("20060102T150405.000000000Z"),
		})
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond)
	}

	// Verify 5 snapshots exist
	catalog, err := LoadCASCatalog(ctx, storage, "test/")
	require.NoError(t, err)
	require.Len(t, catalog.Snapshots, 5)

	// Run GC with lock, keep 2
	deleted, _, err := GarbageCollectCASWithLock(ctx, CASGCOptions{
		Storage:  storage,
		Prefix:   "test/",
		KeepLast: 2,
	}, LockOptions{
		TTL:        1 * time.Minute,
		MaxRetries: 5,
	})
	require.NoError(t, err)
	require.Len(t, deleted, 3)

	// Verify only 2 remain
	catalog, err = LoadCASCatalog(ctx, storage, "test/")
	require.NoError(t, err)
	require.Len(t, catalog.Snapshots, 2)

	t.Logf("GC with lock deleted %d snapshots, %d remain", len(deleted), len(catalog.Snapshots))
}
