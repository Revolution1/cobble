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
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Multi-Version Tree Structure Tests with MinIO
// =============================================================================

// TestBranchingTree_MinIO tests a complex branching snapshot tree:
//
//	A1 (genesis)
//	├── A2 (node-A continues)
//	│   └── A3
//	├── B1 (node-B forks from A1)
//	│   └── B2
//	└── C1 (node-C also forks from A1)
//
// This simulates a blockchain scenario where different nodes fork from the same point.
func TestBranchingTree_MinIO(t *testing.T) {
	minio := testutil.StartMinIO(t)
	ctx := context.Background()
	bucket := "branching-test"

	require.NoError(t, minio.CreateBucket(ctx, bucket))

	storage, err := s3.NewStorage(s3.Config{
		Bucket:          bucket,
		Prefix:          "tree/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	require.NoError(t, err)
	defer storage.Close()

	tmpDir := t.TempDir()

	// ==========================================================================
	// Phase 1: Create genesis snapshot (A1)
	// ==========================================================================
	t.Log("=== Phase 1: Creating genesis snapshot A1 ===")

	nodeAPath := filepath.Join(tmpDir, "node-a")
	nodeA, err := pebble.Open(nodeAPath, &pebble.Options{})
	require.NoError(t, err)

	// Write genesis blocks 1-100
	for i := 1; i <= 100; i++ {
		key := fmt.Sprintf("block:%05d", i)
		value := fmt.Sprintf(`{"number":%d,"hash":"genesis-%05d"}`, i, i)
		require.NoError(t, nodeA.Set([]byte(key), []byte(value), nil))
	}
	require.NoError(t, nodeA.Flush())

	adapterA := &realDBAdapter{db: nodeA, path: nodeAPath}

	manifestA1, err := CreateCASSnapshotWithLock(ctx, adapterA, LockedCASOptions{
		CASSnapshotOptions: CASSnapshotOptions{
			Storage:     storage,
			Prefix:      "main/",
			SnapshotID:  "A1-genesis",
			NodeID:      "node-A",
			BranchName:  "main",
			Description: "Genesis blocks 1-100",
			Labels:      map[string]string{"type": "genesis"},
		},
		LockTTL:        1 * time.Minute,
		LockMaxRetries: 3,
	})
	require.NoError(t, err)
	require.Equal(t, "A1-genesis", manifestA1.ID)
	t.Logf("A1 created: %d blobs, %d bytes", len(manifestA1.Blobs), manifestA1.TotalSize)

	// ==========================================================================
	// Phase 2: Node A continues with A2 and A3
	// ==========================================================================
	t.Log("\n=== Phase 2: Node A continues (A2, A3) ===")

	// Write blocks 101-200
	for i := 101; i <= 200; i++ {
		key := fmt.Sprintf("block:%05d", i)
		value := fmt.Sprintf(`{"number":%d,"hash":"nodeA-%05d"}`, i, i)
		require.NoError(t, nodeA.Set([]byte(key), []byte(value), nil))
	}
	require.NoError(t, nodeA.Flush())

	manifestA2, err := CreateCASSnapshotWithLock(ctx, adapterA, LockedCASOptions{
		CASSnapshotOptions: CASSnapshotOptions{
			Storage:     storage,
			Prefix:      "main/",
			SnapshotID:  "A2-blocks-200",
			NodeID:      "node-A",
			ParentID:    "A1-genesis",
			BranchName:  "main",
			Description: "Blocks 101-200",
		},
		LockTTL:        1 * time.Minute,
		LockMaxRetries: 3,
	})
	require.NoError(t, err)
	require.Equal(t, "A1-genesis", manifestA2.ParentID)
	t.Logf("A2 created: %d blobs, parent=%s", len(manifestA2.Blobs), manifestA2.ParentID)

	// Write blocks 201-300
	for i := 201; i <= 300; i++ {
		key := fmt.Sprintf("block:%05d", i)
		value := fmt.Sprintf(`{"number":%d,"hash":"nodeA-%05d"}`, i, i)
		require.NoError(t, nodeA.Set([]byte(key), []byte(value), nil))
	}
	require.NoError(t, nodeA.Flush())

	manifestA3, err := CreateCASSnapshotWithLock(ctx, adapterA, LockedCASOptions{
		CASSnapshotOptions: CASSnapshotOptions{
			Storage:     storage,
			Prefix:      "main/",
			SnapshotID:  "A3-blocks-300",
			NodeID:      "node-A",
			ParentID:    "A2-blocks-200",
			BranchName:  "main",
			Description: "Blocks 201-300",
		},
		LockTTL:        1 * time.Minute,
		LockMaxRetries: 3,
	})
	require.NoError(t, err)
	require.Equal(t, "A2-blocks-200", manifestA3.ParentID)
	t.Logf("A3 created: %d blobs, parent=%s", len(manifestA3.Blobs), manifestA3.ParentID)

	nodeA.Close()

	// ==========================================================================
	// Phase 3: Node B forks from A1 (creates B1, B2)
	// ==========================================================================
	t.Log("\n=== Phase 3: Node B forks from A1 ===")

	nodeBPath := filepath.Join(tmpDir, "node-b")
	require.NoError(t, RestoreCASSnapshot(ctx, nodeBPath, CASRestoreOptions{
		Storage:         storage,
		Prefix:          "main/",
		SnapshotID:      "A1-genesis",
		VerifyChecksums: true,
	}))

	nodeB, err := pebble.Open(nodeBPath, &pebble.Options{})
	require.NoError(t, err)

	// Verify genesis data is correct
	val, closer, err := nodeB.Get([]byte("block:00050"))
	require.NoError(t, err)
	require.Contains(t, string(val), "genesis-00050")
	closer.Close()

	// Node B writes DIFFERENT blocks 101-150 (fork!)
	for i := 101; i <= 150; i++ {
		key := fmt.Sprintf("block:%05d", i)
		value := fmt.Sprintf(`{"number":%d,"hash":"nodeB-FORK-%05d"}`, i, i)
		require.NoError(t, nodeB.Set([]byte(key), []byte(value), nil))
	}
	require.NoError(t, nodeB.Flush())

	adapterB := &realDBAdapter{db: nodeB, path: nodeBPath}

	manifestB1, err := CreateCASSnapshotWithLock(ctx, adapterB, LockedCASOptions{
		CASSnapshotOptions: CASSnapshotOptions{
			Storage:     storage,
			Prefix:      "main/",
			SnapshotID:  "B1-fork",
			NodeID:      "node-B",
			ParentID:    "A1-genesis", // Fork from A1, not A2!
			BranchName:  "fork-b",
			Description: "Node B fork blocks 101-150",
			Labels:      map[string]string{"fork": "true"},
		},
		LockTTL:        1 * time.Minute,
		LockMaxRetries: 3,
	})
	require.NoError(t, err)
	require.Equal(t, "A1-genesis", manifestB1.ParentID)
	t.Logf("B1 created: parent=%s, branch=%s", manifestB1.ParentID, manifestB1.BranchName)

	// Continue with B2
	for i := 151; i <= 200; i++ {
		key := fmt.Sprintf("block:%05d", i)
		value := fmt.Sprintf(`{"number":%d,"hash":"nodeB-FORK-%05d"}`, i, i)
		require.NoError(t, nodeB.Set([]byte(key), []byte(value), nil))
	}
	require.NoError(t, nodeB.Flush())

	manifestB2, err := CreateCASSnapshotWithLock(ctx, adapterB, LockedCASOptions{
		CASSnapshotOptions: CASSnapshotOptions{
			Storage:     storage,
			Prefix:      "main/",
			SnapshotID:  "B2-fork",
			NodeID:      "node-B",
			ParentID:    "B1-fork",
			BranchName:  "fork-b",
			Description: "Node B fork blocks 151-200",
		},
		LockTTL:        1 * time.Minute,
		LockMaxRetries: 3,
	})
	require.NoError(t, err)
	require.Equal(t, "B1-fork", manifestB2.ParentID)
	t.Logf("B2 created: parent=%s", manifestB2.ParentID)

	nodeB.Close()

	// ==========================================================================
	// Phase 4: Node C also forks from A1 (creates C1)
	// ==========================================================================
	t.Log("\n=== Phase 4: Node C also forks from A1 ===")

	nodeCPath := filepath.Join(tmpDir, "node-c")
	require.NoError(t, RestoreCASSnapshot(ctx, nodeCPath, CASRestoreOptions{
		Storage:         storage,
		Prefix:          "main/",
		SnapshotID:      "A1-genesis",
		VerifyChecksums: true,
	}))

	nodeC, err := pebble.Open(nodeCPath, &pebble.Options{})
	require.NoError(t, err)

	// Node C writes yet ANOTHER different set of blocks
	for i := 101; i <= 120; i++ {
		key := fmt.Sprintf("block:%05d", i)
		value := fmt.Sprintf(`{"number":%d,"hash":"nodeC-FORK-%05d"}`, i, i)
		require.NoError(t, nodeC.Set([]byte(key), []byte(value), nil))
	}
	require.NoError(t, nodeC.Flush())

	adapterC := &realDBAdapter{db: nodeC, path: nodeCPath}

	manifestC1, err := CreateCASSnapshotWithLock(ctx, adapterC, LockedCASOptions{
		CASSnapshotOptions: CASSnapshotOptions{
			Storage:     storage,
			Prefix:      "main/",
			SnapshotID:  "C1-fork",
			NodeID:      "node-C",
			ParentID:    "A1-genesis", // Also forks from A1
			BranchName:  "fork-c",
			Description: "Node C fork blocks 101-120",
		},
		LockTTL:        1 * time.Minute,
		LockMaxRetries: 3,
	})
	require.NoError(t, err)
	require.Equal(t, "A1-genesis", manifestC1.ParentID)
	t.Logf("C1 created: parent=%s, branch=%s", manifestC1.ParentID, manifestC1.BranchName)

	nodeC.Close()

	// ==========================================================================
	// Phase 5: Verify the tree structure
	// ==========================================================================
	t.Log("\n=== Phase 5: Verifying tree structure ===")

	catalog, err := LoadCASCatalog(ctx, storage, "main/")
	require.NoError(t, err)
	require.Len(t, catalog.Snapshots, 6) // A1, A2, A3, B1, B2, C1

	// Build parent-child map
	children := make(map[string][]string)
	for _, snap := range catalog.Snapshots {
		if snap.ParentID != "" {
			children[snap.ParentID] = append(children[snap.ParentID], snap.ID)
		}
	}

	// A1 should have 3 children: A2, B1, C1
	require.Len(t, children["A1-genesis"], 3)
	t.Logf("A1-genesis children: %v", children["A1-genesis"])

	// A2 should have 1 child: A3
	require.Len(t, children["A2-blocks-200"], 1)
	require.Equal(t, "A3-blocks-300", children["A2-blocks-200"][0])

	// B1 should have 1 child: B2
	require.Len(t, children["B1-fork"], 1)
	require.Equal(t, "B2-fork", children["B1-fork"][0])

	// ==========================================================================
	// Phase 6: Verify each branch restores correctly
	// ==========================================================================
	t.Log("\n=== Phase 6: Verifying branch restores ===")

	// Restore A3 (main branch tip)
	restoreA3Path := filepath.Join(tmpDir, "restore-a3")
	require.NoError(t, RestoreCASSnapshot(ctx, restoreA3Path, CASRestoreOptions{
		Storage:    storage,
		Prefix:     "main/",
		SnapshotID: "A3-blocks-300",
	}))
	dbA3, err := pebble.Open(restoreA3Path, &pebble.Options{})
	require.NoError(t, err)
	val, closer, err = dbA3.Get([]byte("block:00250"))
	require.NoError(t, err)
	require.Contains(t, string(val), "nodeA-00250") // A branch data
	closer.Close()
	dbA3.Close()

	// Restore B2 (fork-b branch tip)
	restoreB2Path := filepath.Join(tmpDir, "restore-b2")
	require.NoError(t, RestoreCASSnapshot(ctx, restoreB2Path, CASRestoreOptions{
		Storage:    storage,
		Prefix:     "main/",
		SnapshotID: "B2-fork",
	}))
	dbB2, err := pebble.Open(restoreB2Path, &pebble.Options{})
	require.NoError(t, err)
	val, closer, err = dbB2.Get([]byte("block:00150"))
	require.NoError(t, err)
	require.Contains(t, string(val), "nodeB-FORK-00150") // B branch data
	closer.Close()
	dbB2.Close()

	// Restore C1 (fork-c branch tip)
	restoreC1Path := filepath.Join(tmpDir, "restore-c1")
	require.NoError(t, RestoreCASSnapshot(ctx, restoreC1Path, CASRestoreOptions{
		Storage:    storage,
		Prefix:     "main/",
		SnapshotID: "C1-fork",
	}))
	dbC1, err := pebble.Open(restoreC1Path, &pebble.Options{})
	require.NoError(t, err)
	val, closer, err = dbC1.Get([]byte("block:00110"))
	require.NoError(t, err)
	require.Contains(t, string(val), "nodeC-FORK-00110") // C branch data
	closer.Close()
	dbC1.Close()

	t.Log("=== All branches verified successfully! ===")

	// Print final tree structure
	t.Log("\nSnapshot tree structure:")
	t.Log("A1-genesis (main)")
	t.Log("├── A2-blocks-200 (main)")
	t.Log("│   └── A3-blocks-300 (main)")
	t.Log("├── B1-fork (fork-b)")
	t.Log("│   └── B2-fork (fork-b)")
	t.Log("└── C1-fork (fork-c)")
}

// TestCompaction_MinIO tests snapshot compaction against MinIO.
func TestCompaction_MinIO(t *testing.T) {
	minio := testutil.StartMinIO(t)
	ctx := context.Background()
	bucket := "compaction-test"

	require.NoError(t, minio.CreateBucket(ctx, bucket))

	storage, err := s3.NewStorage(s3.Config{
		Bucket:          bucket,
		Prefix:          "compact/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	require.NoError(t, err)
	defer storage.Close()

	tmpDir := t.TempDir()

	// ==========================================================================
	// Phase 1: Create a chain of incremental snapshots
	// ==========================================================================
	t.Log("=== Phase 1: Creating snapshot chain ===")

	dbPath := filepath.Join(tmpDir, "db")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	require.NoError(t, err)

	adapter := &realDBAdapter{db: db, path: dbPath}

	// Create 5 incremental snapshots
	var lastSnapshotID string
	for i := 1; i <= 5; i++ {
		// Write some data for each snapshot
		for j := 0; j < 100; j++ {
			key := fmt.Sprintf("key-%d-%05d", i, j)
			value := fmt.Sprintf("value-%d-%05d", i, j)
			require.NoError(t, db.Set([]byte(key), []byte(value), nil))
		}
		require.NoError(t, db.Flush())

		snapshotID := fmt.Sprintf("snap-%d", i)
		manifest, err := CreateCASSnapshotWithLock(ctx, adapter, LockedCASOptions{
			CASSnapshotOptions: CASSnapshotOptions{
				Storage:     storage,
				Prefix:      "chain/",
				SnapshotID:  snapshotID,
				NodeID:      "test-node",
				ParentID:    lastSnapshotID,
				BranchName:  "main",
				Description: fmt.Sprintf("Snapshot %d", i),
			},
			LockTTL:        1 * time.Minute,
			LockMaxRetries: 3,
		})
		require.NoError(t, err)
		t.Logf("Created %s: %d blobs, parent=%s", snapshotID, len(manifest.Blobs), manifest.ParentID)
		lastSnapshotID = snapshotID
	}

	db.Close()

	// ==========================================================================
	// Phase 2: Verify chain structure
	// ==========================================================================
	t.Log("\n=== Phase 2: Verifying chain structure ===")

	catalog, err := LoadCASCatalog(ctx, storage, "chain/")
	require.NoError(t, err)
	require.Len(t, catalog.Snapshots, 5)

	// Find snap-5 and trace back to root
	var chain []string
	currentID := "snap-5"
	for currentID != "" {
		chain = append(chain, currentID)
		manifest, found := catalog.GetSnapshot(currentID)
		require.True(t, found)
		currentID = manifest.ParentID
	}
	require.Len(t, chain, 5) // snap-5 -> snap-4 -> snap-3 -> snap-2 -> snap-1
	t.Logf("Chain: %v", chain)

	// ==========================================================================
	// Phase 3: Compact snap-5 to create an independent snapshot
	// ==========================================================================
	t.Log("\n=== Phase 3: Compacting snap-5 ===")

	compactedManifest, err := CompactCASSnapshotWithLock(ctx, CompactCASOptions{
		Storage:       storage,
		Prefix:        "chain/",
		SnapshotID:    "snap-5",
		NewSnapshotID: "snap-5-compacted",
	}, LockOptions{
		TTL:        1 * time.Minute,
		MaxRetries: 3,
	})
	require.NoError(t, err)
	require.Equal(t, "snap-5-compacted", compactedManifest.ID)
	require.Empty(t, compactedManifest.ParentID) // Compacted snapshot has no parent
	require.Contains(t, compactedManifest.Description, "Compacted from snap-5")

	t.Logf("Compacted snapshot: %s, %d blobs, parent=%q",
		compactedManifest.ID, len(compactedManifest.Blobs), compactedManifest.ParentID)

	// Verify blob references are preserved
	originalManifest, err := LoadCASManifest(ctx, storage, "chain/", "snap-5")
	require.NoError(t, err)
	require.Equal(t, len(originalManifest.Blobs), len(compactedManifest.Blobs))

	// ==========================================================================
	// Phase 4: Verify compacted snapshot restores correctly
	// ==========================================================================
	t.Log("\n=== Phase 4: Verifying compacted snapshot restore ===")

	restorePath := filepath.Join(tmpDir, "restore-compacted")
	require.NoError(t, RestoreCASSnapshot(ctx, restorePath, CASRestoreOptions{
		Storage:         storage,
		Prefix:          "chain/",
		SnapshotID:      "snap-5-compacted",
		VerifyChecksums: true,
	}))

	restoredDB, err := pebble.Open(restorePath, &pebble.Options{})
	require.NoError(t, err)

	// Verify data from all 5 snapshots is present
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("key-%d-00050", i)
		val, closer, err := restoredDB.Get([]byte(key))
		require.NoError(t, err, "key %s should exist", key)
		require.Contains(t, string(val), fmt.Sprintf("value-%d-00050", i))
		closer.Close()
	}

	restoredDB.Close()

	// ==========================================================================
	// Phase 5: Delete old snapshots and run GC
	// ==========================================================================
	t.Log("\n=== Phase 5: Deleting old snapshots and running GC ===")

	// Delete old chain (snap-1 to snap-4)
	for i := 1; i <= 4; i++ {
		snapshotID := fmt.Sprintf("snap-%d", i)
		err := DeleteCASSnapshotWithLock(ctx, storage, "chain/", snapshotID, LockOptions{
			TTL:        1 * time.Minute,
			MaxRetries: 3,
		})
		require.NoError(t, err)
		t.Logf("Deleted %s", snapshotID)
	}

	// Verify only snap-5 and snap-5-compacted remain
	catalog, err = LoadCASCatalog(ctx, storage, "chain/")
	require.NoError(t, err)
	require.Len(t, catalog.Snapshots, 2) // snap-5 and snap-5-compacted

	// Run GC to clean up orphaned blobs, keeping the 2 remaining snapshots
	deleted, orphaned, err := GarbageCollectCASWithLock(ctx, CASGCOptions{
		Storage:       storage,
		Prefix:        "chain/",
		KeepSnapshots: []string{"snap-5", "snap-5-compacted"}, // Explicitly keep these
		DryRun:        false,
	}, LockOptions{
		TTL:        1 * time.Minute,
		MaxRetries: 3,
	})
	require.NoError(t, err)
	t.Logf("GC: deleted %d snapshots, %d orphaned blobs cleaned", len(deleted), len(orphaned))

	// ==========================================================================
	// Phase 6: Verify compacted snapshot still works after GC
	// ==========================================================================
	t.Log("\n=== Phase 6: Verifying compacted snapshot after GC ===")

	restoreAfterGCPath := filepath.Join(tmpDir, "restore-after-gc")
	require.NoError(t, RestoreCASSnapshot(ctx, restoreAfterGCPath, CASRestoreOptions{
		Storage:         storage,
		Prefix:          "chain/",
		SnapshotID:      "snap-5-compacted",
		VerifyChecksums: true,
	}))

	dbAfterGC, err := pebble.Open(restoreAfterGCPath, &pebble.Options{})
	require.NoError(t, err)

	// Verify data is still intact
	val, closer, err := dbAfterGC.Get([]byte("key-3-00050"))
	require.NoError(t, err)
	require.Contains(t, string(val), "value-3-00050")
	closer.Close()

	dbAfterGC.Close()

	t.Log("=== Compaction test completed successfully! ===")
}

// TestBlobDeduplication_MinIO tests that blobs are deduplicated across snapshots.
func TestBlobDeduplication_MinIO(t *testing.T) {
	minio := testutil.StartMinIO(t)
	ctx := context.Background()
	bucket := "dedup-test"

	require.NoError(t, minio.CreateBucket(ctx, bucket))

	storage, err := s3.NewStorage(s3.Config{
		Bucket:          bucket,
		Prefix:          "dedup/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	require.NoError(t, err)
	defer storage.Close()

	tmpDir := t.TempDir()

	// Create database with some data
	dbPath := filepath.Join(tmpDir, "db")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	require.NoError(t, err)

	// Write initial data
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%05d", i)
		value := fmt.Sprintf("value-%05d-%s", i, "initial-data-padding-for-size")
		require.NoError(t, db.Set([]byte(key), []byte(value), nil))
	}
	require.NoError(t, db.Flush())

	adapter := &realDBAdapter{db: db, path: dbPath}

	// Create first snapshot
	manifest1, err := CreateCASSnapshotWithLock(ctx, adapter, LockedCASOptions{
		CASSnapshotOptions: CASSnapshotOptions{
			Storage:    storage,
			Prefix:     "test/",
			SnapshotID: "snap-1",
		},
		LockTTL:        1 * time.Minute,
		LockMaxRetries: 3,
	})
	require.NoError(t, err)
	t.Logf("Snapshot 1: %d blobs, %d bytes", len(manifest1.Blobs), manifest1.TotalSize)

	// Create second snapshot WITHOUT modifying data (should reuse all blobs)
	manifest2, err := CreateCASSnapshotWithLock(ctx, adapter, LockedCASOptions{
		CASSnapshotOptions: CASSnapshotOptions{
			Storage:    storage,
			Prefix:     "test/",
			SnapshotID: "snap-2",
			ParentID:   "snap-1",
		},
		LockTTL:        1 * time.Minute,
		LockMaxRetries: 3,
	})
	require.NoError(t, err)
	t.Logf("Snapshot 2: %d blobs, %d bytes", len(manifest2.Blobs), manifest2.TotalSize)

	// Both snapshots should have the same blobs (deduplication)
	require.Equal(t, len(manifest1.Blobs), len(manifest2.Blobs))

	// All blob keys should match
	keys1 := make(map[string]bool)
	for _, blob := range manifest1.Blobs {
		keys1[blob.Key] = true
	}
	for _, blob := range manifest2.Blobs {
		require.True(t, keys1[blob.Key], "blob %s should be deduplicated", blob.Key)
	}

	// Add some new data
	for i := 1000; i < 1100; i++ {
		key := fmt.Sprintf("key-%05d", i)
		value := fmt.Sprintf("value-%05d-new-data", i)
		require.NoError(t, db.Set([]byte(key), []byte(value), nil))
	}
	require.NoError(t, db.Flush())

	// Create third snapshot (should have some new blobs)
	manifest3, err := CreateCASSnapshotWithLock(ctx, adapter, LockedCASOptions{
		CASSnapshotOptions: CASSnapshotOptions{
			Storage:    storage,
			Prefix:     "test/",
			SnapshotID: "snap-3",
			ParentID:   "snap-2",
		},
		LockTTL:        1 * time.Minute,
		LockMaxRetries: 3,
	})
	require.NoError(t, err)
	t.Logf("Snapshot 3: %d blobs, %d bytes", len(manifest3.Blobs), manifest3.TotalSize)

	db.Close()

	// List all objects in blob storage to check actual blob count
	objects, err := storage.List("test/blobs/", "")
	require.NoError(t, err)
	t.Logf("Total unique blobs in storage: %d", len(objects))

	// Verify deduplication: should have fewer blobs than 3 * manifest1.Blobs
	require.Less(t, len(objects), len(manifest1.Blobs)*3,
		"blobs should be deduplicated")

	t.Logf("Deduplication verified: %d blobs instead of %d (without dedup)",
		len(objects), len(manifest1.Blobs)+len(manifest2.Blobs)+len(manifest3.Blobs))
}

// TestGCWithBranches_MinIO tests garbage collection with a branching structure.
func TestGCWithBranches_MinIO(t *testing.T) {
	minio := testutil.StartMinIO(t)
	ctx := context.Background()
	bucket := "gc-branches-test"

	require.NoError(t, minio.CreateBucket(ctx, bucket))

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

	// Create database
	dbPath := filepath.Join(tmpDir, "db")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	require.NoError(t, err)

	adapter := &realDBAdapter{db: db, path: dbPath}

	// Create initial data
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%05d", i)
		value := fmt.Sprintf("value-%05d", i)
		require.NoError(t, db.Set([]byte(key), []byte(value), nil))
	}
	require.NoError(t, db.Flush())

	// Create snapshots in a tree structure:
	// snap-1 -> snap-2 -> snap-3
	//        -> snap-2b (branch)

	manifest1, err := CreateCASSnapshotWithLock(ctx, adapter, LockedCASOptions{
		CASSnapshotOptions: CASSnapshotOptions{
			Storage:    storage,
			Prefix:     "test/",
			SnapshotID: "snap-1",
		},
		LockTTL: 1 * time.Minute,
	})
	require.NoError(t, err)

	// Add more data
	for i := 500; i < 600; i++ {
		key := fmt.Sprintf("key-%05d", i)
		value := fmt.Sprintf("value-%05d", i)
		require.NoError(t, db.Set([]byte(key), []byte(value), nil))
	}
	require.NoError(t, db.Flush())

	manifest2, err := CreateCASSnapshotWithLock(ctx, adapter, LockedCASOptions{
		CASSnapshotOptions: CASSnapshotOptions{
			Storage:    storage,
			Prefix:     "test/",
			SnapshotID: "snap-2",
			ParentID:   "snap-1",
		},
		LockTTL: 1 * time.Minute,
	})
	require.NoError(t, err)

	// Add even more data
	for i := 600; i < 700; i++ {
		key := fmt.Sprintf("key-%05d", i)
		value := fmt.Sprintf("value-%05d", i)
		require.NoError(t, db.Set([]byte(key), []byte(value), nil))
	}
	require.NoError(t, db.Flush())

	manifest3, err := CreateCASSnapshotWithLock(ctx, adapter, LockedCASOptions{
		CASSnapshotOptions: CASSnapshotOptions{
			Storage:    storage,
			Prefix:     "test/",
			SnapshotID: "snap-3",
			ParentID:   "snap-2",
		},
		LockTTL: 1 * time.Minute,
	})
	require.NoError(t, err)

	// Create a branch from snap-1
	db2Path := filepath.Join(tmpDir, "db2")
	require.NoError(t, RestoreCASSnapshot(ctx, db2Path, CASRestoreOptions{
		Storage:    storage,
		Prefix:     "test/",
		SnapshotID: "snap-1",
	}))
	db2, err := pebble.Open(db2Path, &pebble.Options{})
	require.NoError(t, err)

	// Different data for branch
	for i := 500; i < 550; i++ {
		key := fmt.Sprintf("key-%05d", i)
		value := fmt.Sprintf("branch-value-%05d", i)
		require.NoError(t, db2.Set([]byte(key), []byte(value), nil))
	}
	require.NoError(t, db2.Flush())

	adapter2 := &realDBAdapter{db: db2, path: db2Path}
	manifest2b, err := CreateCASSnapshotWithLock(ctx, adapter2, LockedCASOptions{
		CASSnapshotOptions: CASSnapshotOptions{
			Storage:    storage,
			Prefix:     "test/",
			SnapshotID: "snap-2b",
			ParentID:   "snap-1",
			BranchName: "branch",
		},
		LockTTL: 1 * time.Minute,
	})
	require.NoError(t, err)

	db.Close()
	db2.Close()

	t.Logf("Created snapshots:")
	t.Logf("  snap-1: %d blobs", len(manifest1.Blobs))
	t.Logf("  snap-2: %d blobs", len(manifest2.Blobs))
	t.Logf("  snap-3: %d blobs", len(manifest3.Blobs))
	t.Logf("  snap-2b: %d blobs", len(manifest2b.Blobs))

	// Delete snap-2 (middle of chain)
	err = DeleteCASSnapshotWithLock(ctx, storage, "test/", "snap-2", LockOptions{
		TTL: 1 * time.Minute,
	})
	require.NoError(t, err)

	// Run GC - explicitly keep snap-1, snap-3, and snap-2b
	deleted, orphaned, err := GarbageCollectCASWithLock(ctx, CASGCOptions{
		Storage:       storage,
		Prefix:        "test/",
		KeepSnapshots: []string{"snap-1", "snap-3", "snap-2b"},
		DryRun:        false,
	}, LockOptions{
		TTL: 1 * time.Minute,
	})
	require.NoError(t, err)
	t.Logf("GC deleted %d snapshots, %d orphaned blobs", len(deleted), len(orphaned))

	// Verify remaining snapshots still work
	catalog, err := LoadCASCatalog(ctx, storage, "test/")
	require.NoError(t, err)
	require.Len(t, catalog.Snapshots, 3) // snap-1, snap-3, snap-2b

	// Verify snap-3 still restores (even though snap-2 is deleted)
	restorePath := filepath.Join(tmpDir, "restore-snap-3")
	require.NoError(t, RestoreCASSnapshot(ctx, restorePath, CASRestoreOptions{
		Storage:         storage,
		Prefix:          "test/",
		SnapshotID:      "snap-3",
		VerifyChecksums: true,
	}))

	restoredDB, err := pebble.Open(restorePath, &pebble.Options{})
	require.NoError(t, err)
	val, closer, err := restoredDB.Get([]byte("key-00650"))
	require.NoError(t, err)
	require.Contains(t, string(val), "value-00650")
	closer.Close()
	restoredDB.Close()

	t.Log("=== GC with branches test completed successfully! ===")
}
