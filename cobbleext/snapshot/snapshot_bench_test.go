// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build (s3 || cloud) && integration

package snapshot

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/cobbleext/s3"
	"github.com/cockroachdb/pebble/cobbleext/testutil"
)

// =============================================================================
// Snapshot Benchmarks
// =============================================================================

// BenchmarkSnapshotCreate measures snapshot creation performance.
func BenchmarkSnapshotCreate(b *testing.B) {
	minio := testutil.StartMinIO(b)
	ctx := context.Background()
	bucket := "bench-snapshot-create"

	if err := minio.CreateBucket(ctx, bucket); err != nil {
		b.Fatalf("failed to create bucket: %v", err)
	}

	storage, err := s3.NewStorage(s3.Config{
		Bucket:          bucket,
		Prefix:          "snapshot/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	tmpDir := b.TempDir()

	// Create database with different data sizes
	dataSizes := []struct {
		name    string
		numKeys int
		valSize int
	}{
		{"Small_100Keys_1KB", 100, 1024},
		{"Medium_1000Keys_1KB", 1000, 1024},
		{"Large_10000Keys_1KB", 10000, 1024},
		{"Large_1000Keys_10KB", 1000, 10240},
	}

	for _, ds := range dataSizes {
		b.Run(ds.name, func(b *testing.B) {
			// Create fresh DB for each benchmark
			dbPath := filepath.Join(tmpDir, fmt.Sprintf("db-%s", ds.name))
			db, err := pebble.Open(dbPath, &pebble.Options{})
			if err != nil {
				b.Fatalf("failed to open db: %v", err)
			}
			defer db.Close()

			// Write data
			value := make([]byte, ds.valSize)
			for i := 0; i < ds.numKeys; i++ {
				key := fmt.Sprintf("key-%08d", i)
				if err := db.Set([]byte(key), value, nil); err != nil {
					b.Fatalf("failed to write: %v", err)
				}
			}
			if err := db.Flush(); err != nil {
				b.Fatalf("failed to flush: %v", err)
			}

			adapter := &realDBAdapter{db: db, path: dbPath}

			// Calculate expected data size
			expectedBytes := int64(ds.numKeys * (len("key-00000000") + ds.valSize))
			b.SetBytes(expectedBytes)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				snapshotID := fmt.Sprintf("%s-%d", ds.name, i)
				prefix := fmt.Sprintf("bench/%s/", ds.name)

				_, err := CreateCASSnapshot(ctx, adapter, CASSnapshotOptions{
					Storage:    storage,
					Prefix:     prefix,
					SnapshotID: snapshotID,
				})
				if err != nil {
					b.Fatalf("CreateCASSnapshot failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkSnapshotRestore measures snapshot restore performance.
func BenchmarkSnapshotRestore(b *testing.B) {
	minio := testutil.StartMinIO(b)
	ctx := context.Background()
	bucket := "bench-snapshot-restore"

	if err := minio.CreateBucket(ctx, bucket); err != nil {
		b.Fatalf("failed to create bucket: %v", err)
	}

	storage, err := s3.NewStorage(s3.Config{
		Bucket:          bucket,
		Prefix:          "snapshot/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	tmpDir := b.TempDir()

	// Create database and snapshot first
	dataSizes := []struct {
		name    string
		numKeys int
		valSize int
	}{
		{"Small_100Keys_1KB", 100, 1024},
		{"Medium_1000Keys_1KB", 1000, 1024},
		{"Large_5000Keys_1KB", 5000, 1024},
	}

	for _, ds := range dataSizes {
		// Setup: Create DB and snapshot
		dbPath := filepath.Join(tmpDir, fmt.Sprintf("db-%s", ds.name))
		db, err := pebble.Open(dbPath, &pebble.Options{})
		if err != nil {
			b.Fatalf("failed to open db: %v", err)
		}

		value := make([]byte, ds.valSize)
		for i := 0; i < ds.numKeys; i++ {
			key := fmt.Sprintf("key-%08d", i)
			if err := db.Set([]byte(key), value, nil); err != nil {
				b.Fatalf("failed to write: %v", err)
			}
		}
		if err := db.Flush(); err != nil {
			b.Fatalf("failed to flush: %v", err)
		}

		adapter := &realDBAdapter{db: db, path: dbPath}
		prefix := fmt.Sprintf("restore/%s/", ds.name)
		snapshotID := fmt.Sprintf("restore-test-%s", ds.name)

		_, err = CreateCASSnapshot(ctx, adapter, CASSnapshotOptions{
			Storage:    storage,
			Prefix:     prefix,
			SnapshotID: snapshotID,
		})
		if err != nil {
			b.Fatalf("CreateCASSnapshot failed: %v", err)
		}
		db.Close()

		// Benchmark restore
		b.Run(ds.name, func(b *testing.B) {
			expectedBytes := int64(ds.numKeys * (len("key-00000000") + ds.valSize))
			b.SetBytes(expectedBytes)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				restoreDir := filepath.Join(tmpDir, fmt.Sprintf("restore-%s-%d", ds.name, i))

				err := RestoreCASSnapshot(ctx, restoreDir, CASRestoreOptions{
					Storage:    storage,
					Prefix:     prefix,
					SnapshotID: snapshotID,
				})
				if err != nil {
					b.Fatalf("RestoreCASSnapshot failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkSnapshotWithLock measures snapshot creation with distributed locking.
func BenchmarkSnapshotWithLock(b *testing.B) {
	minio := testutil.StartMinIO(b)
	ctx := context.Background()
	bucket := "bench-snapshot-lock"

	if err := minio.CreateBucket(ctx, bucket); err != nil {
		b.Fatalf("failed to create bucket: %v", err)
	}

	storage, err := s3.NewStorage(s3.Config{
		Bucket:          bucket,
		Prefix:          "snapshot/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	tmpDir := b.TempDir()

	// Create database
	dbPath := filepath.Join(tmpDir, "db")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		b.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	numKeys := 500
	valSize := 1024
	value := make([]byte, valSize)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%08d", i)
		if err := db.Set([]byte(key), value, nil); err != nil {
			b.Fatalf("failed to write: %v", err)
		}
	}
	if err := db.Flush(); err != nil {
		b.Fatalf("failed to flush: %v", err)
	}

	adapter := &realDBAdapter{db: db, path: dbPath}
	expectedBytes := int64(numKeys * (len("key-00000000") + valSize))

	b.Run("WithoutLock", func(b *testing.B) {
		b.SetBytes(expectedBytes)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			snapshotID := fmt.Sprintf("nolock-%d", i)
			_, err := CreateCASSnapshot(ctx, adapter, CASSnapshotOptions{
				Storage:    storage,
				Prefix:     "nolock/",
				SnapshotID: snapshotID,
			})
			if err != nil {
				b.Fatalf("CreateCASSnapshot failed: %v", err)
			}
		}
	})

	b.Run("WithLock", func(b *testing.B) {
		b.SetBytes(expectedBytes)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			snapshotID := fmt.Sprintf("withlock-%d", i)
			_, err := CreateCASSnapshotWithLock(ctx, adapter, LockedCASOptions{
				CASSnapshotOptions: CASSnapshotOptions{
					Storage:    storage,
					Prefix:     "withlock/",
					SnapshotID: snapshotID,
				},
				LockTTL:           1 * time.Minute,
				LockRetryInterval: 100 * time.Millisecond,
				LockMaxRetries:    3,
			})
			if err != nil {
				b.Fatalf("CreateCASSnapshotWithLock failed: %v", err)
			}
		}
	})
}

// BenchmarkLockAcquireRelease measures lock acquisition and release performance.
func BenchmarkLockAcquireRelease(b *testing.B) {
	minio := testutil.StartMinIO(b)
	ctx := context.Background()
	bucket := "bench-lock"

	if err := minio.CreateBucket(ctx, bucket); err != nil {
		b.Fatalf("failed to create bucket: %v", err)
	}

	storage, err := s3.NewStorage(s3.Config{
		Bucket:          bucket,
		Prefix:          "lock/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	b.Run("AcquireRelease", func(b *testing.B) {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			lock := NewDistributedLock(storage, "bench/", fmt.Sprintf("lock-%d", i))

			err := lock.Acquire(ctx, LockOptions{
				TTL:        1 * time.Minute,
				MaxRetries: 0,
			})
			if err != nil {
				b.Fatalf("Acquire failed: %v", err)
			}

			err = lock.Release(ctx)
			if err != nil {
				b.Fatalf("Release failed: %v", err)
			}
		}
	})

	b.Run("AcquireRefreshRelease", func(b *testing.B) {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			lock := NewDistributedLock(storage, "bench/", fmt.Sprintf("refresh-lock-%d", i))

			err := lock.Acquire(ctx, LockOptions{
				TTL:        1 * time.Minute,
				MaxRetries: 0,
			})
			if err != nil {
				b.Fatalf("Acquire failed: %v", err)
			}

			err = lock.Refresh(ctx)
			if err != nil {
				b.Fatalf("Refresh failed: %v", err)
			}

			err = lock.Release(ctx)
			if err != nil {
				b.Fatalf("Release failed: %v", err)
			}
		}
	})
}

// BenchmarkCatalogOperations measures catalog read/write performance.
func BenchmarkCatalogOperations(b *testing.B) {
	minio := testutil.StartMinIO(b)
	ctx := context.Background()
	bucket := "bench-catalog"

	if err := minio.CreateBucket(ctx, bucket); err != nil {
		b.Fatalf("failed to create bucket: %v", err)
	}

	storage, err := s3.NewStorage(s3.Config{
		Bucket:          bucket,
		Prefix:          "catalog/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	tmpDir := b.TempDir()

	// Create database and snapshots to build up catalog
	catalogSizes := []int{5, 20, 50}

	for _, numSnapshots := range catalogSizes {
		prefix := fmt.Sprintf("catalog-%d/", numSnapshots)

		// Create snapshots to build catalog
		for i := 0; i < numSnapshots; i++ {
			dbPath := filepath.Join(tmpDir, fmt.Sprintf("db-catalog-%d-%d", numSnapshots, i))
			db, err := pebble.Open(dbPath, &pebble.Options{})
			if err != nil {
				b.Fatalf("failed to open db: %v", err)
			}

			db.Set([]byte("key"), []byte("value"), nil)
			db.Flush()

			adapter := &realDBAdapter{db: db, path: dbPath}
			_, err = CreateCASSnapshot(ctx, adapter, CASSnapshotOptions{
				Storage:    storage,
				Prefix:     prefix,
				SnapshotID: fmt.Sprintf("snap-%d", i),
			})
			if err != nil {
				b.Fatalf("CreateCASSnapshot failed: %v", err)
			}
			db.Close()
		}

		b.Run(fmt.Sprintf("LoadCatalog_%dSnapshots", numSnapshots), func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := LoadCASCatalog(ctx, storage, prefix)
				if err != nil {
					b.Fatalf("LoadCASCatalog failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkGarbageCollect measures garbage collection performance.
func BenchmarkGarbageCollect(b *testing.B) {
	minio := testutil.StartMinIO(b)
	ctx := context.Background()
	bucket := "bench-gc"

	if err := minio.CreateBucket(ctx, bucket); err != nil {
		b.Fatalf("failed to create bucket: %v", err)
	}

	storage, err := s3.NewStorage(s3.Config{
		Bucket:          bucket,
		Prefix:          "gc/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	tmpDir := b.TempDir()

	// Create database and multiple snapshots
	numSnapshots := 10
	dbPath := filepath.Join(tmpDir, "gc-db")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		b.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	adapter := &realDBAdapter{db: db, path: dbPath}

	b.Run("GC_10Snapshots_Keep2", func(b *testing.B) {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			prefix := fmt.Sprintf("gc-run-%d/", i)

			// Create snapshots
			for j := 0; j < numSnapshots; j++ {
				db.Set([]byte(fmt.Sprintf("key-%d", j)), []byte("value"), nil)
				db.Flush()

				_, err := CreateCASSnapshot(ctx, adapter, CASSnapshotOptions{
					Storage:    storage,
					Prefix:     prefix,
					SnapshotID: fmt.Sprintf("snap-%d", j),
				})
				if err != nil {
					b.Fatalf("CreateCASSnapshot failed: %v", err)
				}
			}

			// Run GC
			deleted, orphaned, err := GarbageCollectCAS(ctx, CASGCOptions{
				Storage:  storage,
				Prefix:   prefix,
				KeepLast: 2,
			})
			if err != nil {
				b.Fatalf("GarbageCollectCAS failed: %v", err)
			}
			_ = deleted
			_ = orphaned
		}
	})
}

// =============================================================================
// Detailed Latency Test (not benchmark, but measures latency distribution)
// =============================================================================

// TestSnapshotLatencyProfile runs detailed latency analysis for snapshot operations.
func TestSnapshotLatencyProfile(t *testing.T) {
	minio := testutil.StartMinIO(t)
	ctx := context.Background()
	bucket := "latency-profile"

	if err := minio.CreateBucket(ctx, bucket); err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	storage, err := s3.NewStorage(s3.Config{
		Bucket:          bucket,
		Prefix:          "profile/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	tmpDir := t.TempDir()

	// Create database
	dbPath := filepath.Join(tmpDir, "db")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Write data
	numKeys := 1000
	valSize := 1024
	value := make([]byte, valSize)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%08d", i)
		if err := db.Set([]byte(key), value, nil); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	adapter := &realDBAdapter{db: db, path: dbPath}
	numIterations := 10

	// Measure snapshot creation latency
	t.Run("SnapshotCreate", func(t *testing.T) {
		var latencies []time.Duration

		for i := 0; i < numIterations; i++ {
			start := time.Now()
			_, err := CreateCASSnapshot(ctx, adapter, CASSnapshotOptions{
				Storage:    storage,
				Prefix:     "create-latency/",
				SnapshotID: fmt.Sprintf("snap-%d", i),
			})
			if err != nil {
				t.Fatalf("CreateCASSnapshot failed: %v", err)
			}
			latencies = append(latencies, time.Since(start))
		}

		reportLatencies(t, "Snapshot Create", latencies)
	})

	// Measure snapshot restore latency
	t.Run("SnapshotRestore", func(t *testing.T) {
		// First create a snapshot
		_, err := CreateCASSnapshot(ctx, adapter, CASSnapshotOptions{
			Storage:    storage,
			Prefix:     "restore-latency/",
			SnapshotID: "restore-test",
		})
		if err != nil {
			t.Fatalf("CreateCASSnapshot failed: %v", err)
		}

		var latencies []time.Duration

		for i := 0; i < numIterations; i++ {
			restoreDir := filepath.Join(tmpDir, fmt.Sprintf("restore-%d", i))
			start := time.Now()
			err := RestoreCASSnapshot(ctx, restoreDir, CASRestoreOptions{
				Storage:    storage,
				Prefix:     "restore-latency/",
				SnapshotID: "restore-test",
			})
			if err != nil {
				t.Fatalf("RestoreCASSnapshot failed: %v", err)
			}
			latencies = append(latencies, time.Since(start))
		}

		reportLatencies(t, "Snapshot Restore", latencies)
	})

	// Measure lock acquisition latency
	t.Run("LockAcquire", func(t *testing.T) {
		var latencies []time.Duration

		for i := 0; i < numIterations; i++ {
			lock := NewDistributedLock(storage, "lock-latency/", fmt.Sprintf("lock-%d", i))

			start := time.Now()
			err := lock.Acquire(ctx, LockOptions{
				TTL:        1 * time.Minute,
				MaxRetries: 0,
			})
			latencies = append(latencies, time.Since(start))

			if err != nil {
				t.Fatalf("Acquire failed: %v", err)
			}
			lock.Release(ctx)
		}

		reportLatencies(t, "Lock Acquire", latencies)
	})
}

func reportLatencies(t *testing.T, name string, latencies []time.Duration) {
	if len(latencies) == 0 {
		return
	}

	// Sort
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	for i := 0; i < len(sorted)-1; i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j] < sorted[i] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	var total time.Duration
	for _, d := range sorted {
		total += d
	}

	avg := total / time.Duration(len(sorted))
	p50 := sorted[len(sorted)*50/100]
	p90 := sorted[len(sorted)*90/100]
	p99 := sorted[len(sorted)*99/100]
	min := sorted[0]
	max := sorted[len(sorted)-1]

	t.Logf("%s: n=%d min=%v avg=%v p50=%v p90=%v p99=%v max=%v",
		name, len(sorted), min, avg, p50, p90, p99, max)
}
