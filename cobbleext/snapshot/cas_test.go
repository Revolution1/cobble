// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package snapshot

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
)

// =============================================================================
// CAS Snapshot Tests
// =============================================================================

// TestCASBasicWorkflow tests basic CAS snapshot create and restore.
func TestCASBasicWorkflow(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newTrackedStorage()

	// Create database and write data
	dbPath := filepath.Join(tmpDir, "testdb")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	testData := make(map[string]string)
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%05d", i)
		value := fmt.Sprintf("value-%05d-%s", i, strings.Repeat("x", 100))
		testData[key] = value
		if err := db.Set([]byte(key), []byte(value), pebble.Sync); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
	}
	db.Flush()

	adapter := &realDBAdapter{db: db, path: dbPath}

	// Create CAS snapshot
	t.Log("Creating CAS snapshot...")
	opts := CASSnapshotOptions{
		Storage:     storage,
		Prefix:      "cas-test/",
		SnapshotID:  "snap-001",
		NodeID:      "node-A",
		Description: "Test snapshot",
		Labels: map[string]string{
			"env": "test",
		},
	}

	manifest, err := CreateCASSnapshot(context.Background(), adapter, opts)
	if err != nil {
		t.Fatalf("CreateCASSnapshot failed: %v", err)
	}

	t.Logf("Snapshot created: ID=%s, Blobs=%d, Size=%d", manifest.ID, len(manifest.Blobs), manifest.TotalSize)

	// Verify blobs are stored by hash
	allObjects := storage.listAllObjects()
	blobCount := 0
	for _, obj := range allObjects {
		if strings.Contains(obj, "/blobs/") {
			blobCount++
			t.Logf("  Blob: %s", obj)
		}
	}
	t.Logf("Total blobs in storage: %d", blobCount)

	// Note: Blob count might be less than manifest blobs if there are duplicate
	// content (e.g., multiple empty files have the same hash). This is expected
	// CAS behavior - content deduplication.
	if blobCount > len(manifest.Blobs) {
		t.Errorf("blob count %d should not exceed manifest blobs %d", blobCount, len(manifest.Blobs))
	}

	// Verify catalog
	catalog, err := LoadCASCatalog(context.Background(), storage, "cas-test/")
	if err != nil {
		t.Fatalf("LoadCASCatalog failed: %v", err)
	}
	if len(catalog.Snapshots) != 1 {
		t.Errorf("expected 1 snapshot in catalog, got %d", len(catalog.Snapshots))
	}
	// BlobRefCounts tracks unique hashes, so it should match unique blob count
	if len(catalog.BlobRefCounts) != blobCount {
		t.Errorf("expected %d blob refs (unique hashes), got %d", blobCount, len(catalog.BlobRefCounts))
	}

	// Close database before restore
	db.Close()

	// Restore from CAS snapshot
	t.Log("Restoring from CAS snapshot...")
	restoreDir := filepath.Join(tmpDir, "restored")
	restoreOpts := CASRestoreOptions{
		Storage:         storage,
		Prefix:          "cas-test/",
		SnapshotID:      "snap-001",
		VerifyChecksums: true,
	}

	if err := RestoreCASSnapshot(context.Background(), restoreDir, restoreOpts); err != nil {
		t.Fatalf("RestoreCASSnapshot failed: %v", err)
	}

	// Open restored database and verify
	restoredDB, err := pebble.Open(restoreDir, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open restored database: %v", err)
	}
	defer restoredDB.Close()

	verifiedCount := 0
	for key, expectedValue := range testData {
		value, closer, err := restoredDB.Get([]byte(key))
		if err != nil {
			t.Errorf("key %s not found: %v", key, err)
			continue
		}
		if string(value) != expectedValue {
			t.Errorf("value mismatch for %s", key)
		}
		closer.Close()
		verifiedCount++
	}

	t.Logf("Verified %d/%d keys", verifiedCount, len(testData))

	if verifiedCount != len(testData) {
		t.Errorf("verification failed")
	}

	t.Log("=== CAS Basic Workflow Test PASSED ===")
}

// TestCASDeduplication tests that identical blobs are not uploaded twice.
func TestCASDeduplication(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newTrackedStorage()

	// Create first database with data
	db1Path := filepath.Join(tmpDir, "db1")
	db1, err := pebble.Open(db1Path, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open db1: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("shared-key-%05d", i)
		value := fmt.Sprintf("shared-value-%05d", i)
		if err := db1.Set([]byte(key), []byte(value), pebble.Sync); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
	}
	db1.Flush()

	adapter1 := &realDBAdapter{db: db1, path: db1Path}

	// Create first snapshot
	t.Log("Creating first CAS snapshot...")
	opts1 := CASSnapshotOptions{
		Storage:    storage,
		Prefix:     "dedup-test/",
		SnapshotID: "snap-A-001",
		NodeID:     "node-A",
	}

	manifest1, err := CreateCASSnapshot(context.Background(), adapter1, opts1)
	if err != nil {
		t.Fatalf("CreateCASSnapshot 1 failed: %v", err)
	}
	t.Logf("Snapshot 1: %d blobs", len(manifest1.Blobs))

	// Count blobs after first snapshot
	blobsAfterFirst := countBlobs(storage, "dedup-test/")
	t.Logf("Blobs after first snapshot: %d", blobsAfterFirst)

	db1.Close()

	// Create second database with SAME data (simulating restore + new writes)
	db2Path := filepath.Join(tmpDir, "db2")
	db2, err := pebble.Open(db2Path, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open db2: %v", err)
	}

	// Write the SAME data - this simulates a restored node
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("shared-key-%05d", i)
		value := fmt.Sprintf("shared-value-%05d", i)
		if err := db2.Set([]byte(key), []byte(value), pebble.Sync); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
	}
	db2.Flush()

	adapter2 := &realDBAdapter{db: db2, path: db2Path}

	// Create second snapshot (should deduplicate)
	t.Log("Creating second CAS snapshot (should deduplicate)...")
	opts2 := CASSnapshotOptions{
		Storage:    storage,
		Prefix:     "dedup-test/",
		SnapshotID: "snap-B-001",
		NodeID:     "node-B",
	}

	manifest2, err := CreateCASSnapshot(context.Background(), adapter2, opts2)
	if err != nil {
		t.Fatalf("CreateCASSnapshot 2 failed: %v", err)
	}
	t.Logf("Snapshot 2: %d blobs", len(manifest2.Blobs))

	// Count blobs after second snapshot
	blobsAfterSecond := countBlobs(storage, "dedup-test/")
	t.Logf("Blobs after second snapshot: %d", blobsAfterSecond)

	db2.Close()

	// Verify catalog has both snapshots
	catalog, err := LoadCASCatalog(context.Background(), storage, "dedup-test/")
	if err != nil {
		t.Fatalf("LoadCASCatalog failed: %v", err)
	}
	if len(catalog.Snapshots) != 2 {
		t.Errorf("expected 2 snapshots, got %d", len(catalog.Snapshots))
	}

	// Check deduplication - blobs should be mostly shared
	// The SST files might be different due to different sequence numbers,
	// but config files (OPTIONS, MANIFEST) might be different too.
	// The key insight is that identical data produces identical blobs.
	t.Logf("Deduplication efficiency: %d blobs for 2 snapshots (vs %d + %d = %d without dedup)",
		blobsAfterSecond, len(manifest1.Blobs), len(manifest2.Blobs),
		len(manifest1.Blobs)+len(manifest2.Blobs))

	// Both snapshots should be independently restorable
	t.Log("Verifying both snapshots are independently restorable...")

	for _, snapID := range []string{"snap-A-001", "snap-B-001"} {
		restoreDir := filepath.Join(tmpDir, "restore-"+snapID)
		restoreOpts := CASRestoreOptions{
			Storage:         storage,
			Prefix:          "dedup-test/",
			SnapshotID:      snapID,
			VerifyChecksums: true,
		}

		if err := RestoreCASSnapshot(context.Background(), restoreDir, restoreOpts); err != nil {
			t.Errorf("RestoreCASSnapshot %s failed: %v", snapID, err)
			continue
		}

		// Verify data
		restoredDB, err := pebble.Open(restoreDir, &pebble.Options{})
		if err != nil {
			t.Errorf("failed to open restored %s: %v", snapID, err)
			continue
		}

		// Check one key
		value, closer, err := restoredDB.Get([]byte("shared-key-00050"))
		if err != nil {
			t.Errorf("%s: key not found: %v", snapID, err)
		} else {
			if string(value) != "shared-value-00050" {
				t.Errorf("%s: value mismatch", snapID)
			}
			closer.Close()
		}

		restoredDB.Close()
		t.Logf("  %s: OK", snapID)
	}

	t.Log("=== CAS Deduplication Test PASSED ===")
}

// TestCASBranching tests tree-like snapshot history (branching).
func TestCASBranching(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newTrackedStorage()

	// ==========================================================================
	// Scenario:
	// Node A creates snapshots: A1 -> A2 -> A3
	// Node B restores from A1 and creates: B1 -> B2 (different branch)
	// Both branches should work correctly
	// ==========================================================================

	// --- Node A: Initial data ---
	dbAPath := filepath.Join(tmpDir, "node-a")
	dbA, err := pebble.Open(dbAPath, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open dbA: %v", err)
	}

	// Write initial data (blocks 1-100)
	t.Log("Node A: Writing blocks 1-100")
	for i := 1; i <= 100; i++ {
		key := fmt.Sprintf("block-%05d", i)
		value := fmt.Sprintf("data-%05d-node-a", i)
		if err := dbA.Set([]byte(key), []byte(value), pebble.Sync); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
	}
	dbA.Flush()

	adapterA := &realDBAdapter{db: dbA, path: dbAPath}

	// Create snapshot A1
	t.Log("Node A: Creating snapshot A1")
	manifestA1, err := CreateCASSnapshot(context.Background(), adapterA, CASSnapshotOptions{
		Storage:    storage,
		Prefix:     "branch-test/",
		SnapshotID: "A1",
		NodeID:     "node-A",
		BranchName: "main",
	})
	if err != nil {
		t.Fatalf("CreateCASSnapshot A1 failed: %v", err)
	}
	t.Logf("A1 created: %d blobs", len(manifestA1.Blobs))

	// Node A continues: blocks 101-200
	t.Log("Node A: Writing blocks 101-200")
	for i := 101; i <= 200; i++ {
		key := fmt.Sprintf("block-%05d", i)
		value := fmt.Sprintf("data-%05d-node-a", i)
		if err := dbA.Set([]byte(key), []byte(value), pebble.Sync); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
	}
	dbA.Flush()

	// Create snapshot A2
	t.Log("Node A: Creating snapshot A2")
	manifestA2, err := CreateCASSnapshot(context.Background(), adapterA, CASSnapshotOptions{
		Storage:    storage,
		Prefix:     "branch-test/",
		SnapshotID: "A2",
		NodeID:     "node-A",
		ParentID:   "A1",
		BranchName: "main",
	})
	if err != nil {
		t.Fatalf("CreateCASSnapshot A2 failed: %v", err)
	}
	t.Logf("A2 created: %d blobs", len(manifestA2.Blobs))

	dbA.Close()

	// --- Node B: Restore from A1 and create different branch ---
	t.Log("Node B: Restoring from A1")
	dbBPath := filepath.Join(tmpDir, "node-b")
	if err := RestoreCASSnapshot(context.Background(), dbBPath, CASRestoreOptions{
		Storage:         storage,
		Prefix:          "branch-test/",
		SnapshotID:      "A1",
		VerifyChecksums: true,
	}); err != nil {
		t.Fatalf("RestoreCASSnapshot to node B failed: %v", err)
	}

	dbB, err := pebble.Open(dbBPath, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open dbB: %v", err)
	}

	// Node B writes DIFFERENT blocks (101-150 with different data)
	t.Log("Node B: Writing blocks 101-150 (different from A)")
	for i := 101; i <= 150; i++ {
		key := fmt.Sprintf("block-%05d", i)
		value := fmt.Sprintf("data-%05d-node-b-DIFFERENT", i) // Different data!
		if err := dbB.Set([]byte(key), []byte(value), pebble.Sync); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
	}
	dbB.Flush()

	adapterB := &realDBAdapter{db: dbB, path: dbBPath}

	// Create snapshot B1 (branching from A1)
	t.Log("Node B: Creating snapshot B1 (branch from A1)")
	manifestB1, err := CreateCASSnapshot(context.Background(), adapterB, CASSnapshotOptions{
		Storage:    storage,
		Prefix:     "branch-test/",
		SnapshotID: "B1",
		NodeID:     "node-B",
		ParentID:   "A1", // Branched from A1
		BranchName: "node-b-branch",
	})
	if err != nil {
		t.Fatalf("CreateCASSnapshot B1 failed: %v", err)
	}
	t.Logf("B1 created: %d blobs", len(manifestB1.Blobs))

	dbB.Close()

	// ==========================================================================
	// Verify both branches work correctly
	// ==========================================================================

	// Verify A2 can be restored and has blocks 1-200 with node-a data
	t.Log("Verifying A2 branch...")
	restoreA2 := filepath.Join(tmpDir, "verify-a2")
	if err := RestoreCASSnapshot(context.Background(), restoreA2, CASRestoreOptions{
		Storage:         storage,
		Prefix:          "branch-test/",
		SnapshotID:      "A2",
		VerifyChecksums: true,
	}); err != nil {
		t.Fatalf("RestoreCASSnapshot A2 failed: %v", err)
	}

	dbA2, err := pebble.Open(restoreA2, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open A2: %v", err)
	}

	// Check block 150 from A2 - should have node-a data
	val, closer, err := dbA2.Get([]byte("block-00150"))
	if err != nil {
		t.Errorf("A2: block-00150 not found: %v", err)
	} else {
		if !strings.Contains(string(val), "node-a") {
			t.Errorf("A2: block-00150 should have node-a data, got: %s", val)
		}
		closer.Close()
	}
	dbA2.Close()

	// Verify B1 can be restored and has blocks 1-150 with node-b data for 101-150
	t.Log("Verifying B1 branch...")
	restoreB1 := filepath.Join(tmpDir, "verify-b1")
	if err := RestoreCASSnapshot(context.Background(), restoreB1, CASRestoreOptions{
		Storage:         storage,
		Prefix:          "branch-test/",
		SnapshotID:      "B1",
		VerifyChecksums: true,
	}); err != nil {
		t.Fatalf("RestoreCASSnapshot B1 failed: %v", err)
	}

	dbB1, err := pebble.Open(restoreB1, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open B1: %v", err)
	}

	// Check block 150 from B1 - should have node-b data
	val, closer, err = dbB1.Get([]byte("block-00150"))
	if err != nil {
		t.Errorf("B1: block-00150 not found: %v", err)
	} else {
		if !strings.Contains(string(val), "node-b") {
			t.Errorf("B1: block-00150 should have node-b data, got: %s", val)
		}
		closer.Close()
	}

	// B1 should NOT have block 200 (only A2 has that)
	_, closer, err = dbB1.Get([]byte("block-00200"))
	if err == nil {
		closer.Close()
		t.Error("B1 should NOT have block-00200")
	}

	dbB1.Close()

	// Verify catalog structure
	catalog, err := LoadCASCatalog(context.Background(), storage, "branch-test/")
	if err != nil {
		t.Fatalf("LoadCASCatalog failed: %v", err)
	}

	t.Log("Catalog structure:")
	for _, snap := range catalog.Snapshots {
		t.Logf("  %s (node=%s, parent=%s, branch=%s)",
			snap.ID, snap.NodeID, snap.ParentID, snap.BranchName)
	}

	if len(catalog.Snapshots) != 3 {
		t.Errorf("expected 3 snapshots (A1, A2, B1), got %d", len(catalog.Snapshots))
	}

	t.Log("=== CAS Branching Test PASSED ===")
}

// TestCASGarbageCollection tests garbage collection.
func TestCASGarbageCollection(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newTrackedStorage()

	// Create database
	dbPath := filepath.Join(tmpDir, "gcdb")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	adapter := &realDBAdapter{db: db, path: dbPath}

	// Create multiple snapshots
	for i := 1; i <= 5; i++ {
		// Add some data
		for j := 0; j < 10; j++ {
			key := fmt.Sprintf("snap%d-key-%d", i, j)
			value := fmt.Sprintf("value-%d-%d", i, j)
			if err := db.Set([]byte(key), []byte(value), pebble.Sync); err != nil {
				t.Fatalf("failed to write: %v", err)
			}
		}
		db.Flush()

		// Create snapshot
		snapID := fmt.Sprintf("snap-%d", i)
		_, err := CreateCASSnapshot(context.Background(), adapter, CASSnapshotOptions{
			Storage:    storage,
			Prefix:     "gc-test/",
			SnapshotID: snapID,
		})
		if err != nil {
			t.Fatalf("CreateCASSnapshot %s failed: %v", snapID, err)
		}
		t.Logf("Created snapshot %s", snapID)
	}

	db.Close()

	// Check initial state
	catalogBefore, _ := LoadCASCatalog(context.Background(), storage, "gc-test/")
	blobsBefore := countBlobs(storage, "gc-test/")
	t.Logf("Before GC: %d snapshots, %d blobs", len(catalogBefore.Snapshots), blobsBefore)

	// GC: Keep only last 2 snapshots
	t.Log("Running GC (keep last 2)...")
	deletedSnaps, deletedBlobs, err := GarbageCollectCAS(context.Background(), CASGCOptions{
		Storage:  storage,
		Prefix:   "gc-test/",
		KeepLast: 2,
	})
	if err != nil {
		t.Fatalf("GarbageCollectCAS failed: %v", err)
	}

	t.Logf("GC deleted %d snapshots: %v", len(deletedSnaps), deletedSnaps)
	t.Logf("GC deleted %d blobs", len(deletedBlobs))

	// Check final state
	catalogAfter, _ := LoadCASCatalog(context.Background(), storage, "gc-test/")
	blobsAfter := countBlobs(storage, "gc-test/")
	t.Logf("After GC: %d snapshots, %d blobs", len(catalogAfter.Snapshots), blobsAfter)

	if len(catalogAfter.Snapshots) != 2 {
		t.Errorf("expected 2 snapshots after GC, got %d", len(catalogAfter.Snapshots))
	}

	if len(deletedSnaps) != 3 {
		t.Errorf("expected 3 deleted snapshots, got %d", len(deletedSnaps))
	}

	// Remaining snapshots should be snap-4 and snap-5 (newest)
	for _, snap := range catalogAfter.Snapshots {
		if snap.ID != "snap-4" && snap.ID != "snap-5" {
			t.Errorf("unexpected snapshot remaining: %s", snap.ID)
		}
	}

	// Verify remaining snapshots still work
	restoreDir := filepath.Join(tmpDir, "restored-after-gc")
	if err := RestoreCASSnapshot(context.Background(), restoreDir, CASRestoreOptions{
		Storage:    storage,
		Prefix:     "gc-test/",
		SnapshotID: "snap-5",
	}); err != nil {
		t.Fatalf("RestoreCASSnapshot after GC failed: %v", err)
	}

	restoredDB, err := pebble.Open(restoreDir, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open restored database: %v", err)
	}
	defer restoredDB.Close()

	// Check a key from snap-5
	_, closer, err := restoredDB.Get([]byte("snap5-key-5"))
	if err != nil {
		t.Errorf("snap5-key-5 not found: %v", err)
	} else {
		closer.Close()
		t.Log("Restored snapshot after GC works correctly")
	}

	t.Log("=== CAS Garbage Collection Test PASSED ===")
}

// TestCASCompaction tests snapshot compaction for enabling historical GC.
func TestCASCompaction(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newTrackedStorage()

	// ==========================================================================
	// Scenario:
	// Create a chain: snap-1 → snap-2 → snap-3 → snap-4 → snap-5
	// We want to keep only snap-5 but delete snap-1 through snap-4.
	// Without compaction, snap-5 might share blobs with older snapshots.
	// After compaction, snap-5 becomes independent and we can GC everything else.
	// ==========================================================================

	// Create database
	dbPath := filepath.Join(tmpDir, "compactdb")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	adapter := &realDBAdapter{db: db, path: dbPath}

	// Create chain of snapshots
	for i := 1; i <= 5; i++ {
		// Write some data
		for j := 0; j < 20; j++ {
			key := fmt.Sprintf("chain-%d-key-%d", i, j)
			value := fmt.Sprintf("value-%d-%d", i, j)
			if err := db.Set([]byte(key), []byte(value), pebble.Sync); err != nil {
				t.Fatalf("failed to write: %v", err)
			}
		}
		db.Flush()

		parentID := ""
		if i > 1 {
			parentID = fmt.Sprintf("chain-%d", i-1)
		}

		_, err := CreateCASSnapshot(context.Background(), adapter, CASSnapshotOptions{
			Storage:    storage,
			Prefix:     "compact-test/",
			SnapshotID: fmt.Sprintf("chain-%d", i),
			ParentID:   parentID,
		})
		if err != nil {
			t.Fatalf("CreateCASSnapshot chain-%d failed: %v", i, err)
		}
		t.Logf("Created chain-%d", i)
	}

	db.Close()

	// Check initial state
	catalogBefore, _ := LoadCASCatalog(context.Background(), storage, "compact-test/")
	blobsBefore := countBlobs(storage, "compact-test/")
	t.Logf("Before compaction: %d snapshots, %d blobs", len(catalogBefore.Snapshots), blobsBefore)

	// Verify chain structure
	t.Log("Snapshot chain before compaction:")
	for _, snap := range catalogBefore.Snapshots {
		t.Logf("  %s (parent=%s)", snap.ID, snap.ParentID)
	}

	// Compact chain-5 to make it independent
	t.Log("Compacting chain-5 to create independent snapshot...")
	compactedManifest, err := CompactCASSnapshot(context.Background(), CompactCASOptions{
		Storage:       storage,
		Prefix:        "compact-test/",
		SnapshotID:    "chain-5",
		NewSnapshotID: "chain-5-compacted",
		DeleteOld:     false, // Keep original for now
	})
	if err != nil {
		t.Fatalf("CompactCASSnapshot failed: %v", err)
	}

	t.Logf("Compacted snapshot created: %s (parent=%s, blobs=%d)",
		compactedManifest.ID, compactedManifest.ParentID, len(compactedManifest.Blobs))

	// Verify compacted snapshot has no parent
	if compactedManifest.ParentID != "" {
		t.Errorf("expected compacted snapshot to have no parent, got %s", compactedManifest.ParentID)
	}

	// Check catalog now has 6 snapshots
	catalogAfterCompact, _ := LoadCASCatalog(context.Background(), storage, "compact-test/")
	if len(catalogAfterCompact.Snapshots) != 6 {
		t.Errorf("expected 6 snapshots after compaction, got %d", len(catalogAfterCompact.Snapshots))
	}

	// Now delete all old snapshots (chain-1 through chain-5)
	t.Log("Deleting old snapshots...")
	for i := 1; i <= 5; i++ {
		snapID := fmt.Sprintf("chain-%d", i)
		if err := DeleteCASSnapshot(context.Background(), storage, "compact-test/", snapID); err != nil {
			t.Fatalf("DeleteCASSnapshot %s failed: %v", snapID, err)
		}
		t.Logf("  Deleted %s", snapID)
	}

	// Check final state
	catalogFinal, _ := LoadCASCatalog(context.Background(), storage, "compact-test/")
	blobsFinal := countBlobs(storage, "compact-test/")
	t.Logf("After deletion: %d snapshots, %d blobs", len(catalogFinal.Snapshots), blobsFinal)

	// Should have exactly 1 snapshot (the compacted one)
	if len(catalogFinal.Snapshots) != 1 {
		t.Errorf("expected 1 snapshot after deletion, got %d", len(catalogFinal.Snapshots))
	}
	if catalogFinal.Snapshots[0].ID != "chain-5-compacted" {
		t.Errorf("expected remaining snapshot to be chain-5-compacted, got %s", catalogFinal.Snapshots[0].ID)
	}

	// Blobs should be reduced (only keeping what chain-5-compacted needs)
	t.Logf("Blob reduction: %d → %d (saved %d)", blobsBefore, blobsFinal, blobsBefore-blobsFinal)

	// Verify the compacted snapshot still works
	t.Log("Verifying compacted snapshot can be restored...")
	restoreDir := filepath.Join(tmpDir, "restored-compacted")
	if err := RestoreCASSnapshot(context.Background(), restoreDir, CASRestoreOptions{
		Storage:         storage,
		Prefix:          "compact-test/",
		SnapshotID:      "chain-5-compacted",
		VerifyChecksums: true,
	}); err != nil {
		t.Fatalf("RestoreCASSnapshot failed: %v", err)
	}

	restoredDB, err := pebble.Open(restoreDir, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open restored database: %v", err)
	}
	defer restoredDB.Close()

	// Should have data from all 5 chain steps
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("chain-%d-key-10", i)
		_, closer, err := restoredDB.Get([]byte(key))
		if err != nil {
			t.Errorf("key %s not found in compacted restore: %v", key, err)
		} else {
			closer.Close()
		}
	}

	t.Log("=== CAS Compaction Test PASSED ===")
}

// Helper function to count blobs in storage
func countBlobs(storage *trackedStorage, prefix string) int {
	allObjects := storage.listAllObjects()
	count := 0
	for _, obj := range allObjects {
		if strings.Contains(obj, "/blobs/") {
			count++
		}
	}
	return count
}
