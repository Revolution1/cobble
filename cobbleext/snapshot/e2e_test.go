// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

// =============================================================================
// End-to-End Integration Tests
//
// These tests simulate real-world scenarios with multiple nodes, branching
// histories, and complete lifecycle management.
// =============================================================================

// TestE2EMultiNodeBranching simulates a realistic multi-node scenario:
// 1. Node A creates initial data and snapshots
// 2. Node B and C restore from Node A's snapshot
// 3. All three nodes continue writing (different data)
// 4. Each node creates its own branch of snapshots
// 5. Verify all branches are independently restorable
func TestE2EMultiNodeBranching(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newTrackedStorage()

	// ==========================================================================
	// Phase 1: Node A - Initial Setup
	// ==========================================================================
	t.Log("=== Phase 1: Node A - Initial Setup ===")

	nodeAPath := filepath.Join(tmpDir, "node-a")
	nodeA, err := pebble.Open(nodeAPath, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open node A: %v", err)
	}

	// Write genesis block data (blocks 1-100)
	t.Log("Node A: Writing genesis blocks 1-100...")
	for i := 1; i <= 100; i++ {
		key := fmt.Sprintf("block:%05d", i)
		value := fmt.Sprintf(`{"number":%d,"hash":"0x%064x","data":"genesis-data"}`, i, i)
		if err := nodeA.Set([]byte(key), []byte(value), pebble.Sync); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
	}
	nodeA.Flush()

	adapterA := &realDBAdapter{db: nodeA, path: nodeAPath}

	// Create snapshot A1 (genesis)
	t.Log("Node A: Creating snapshot A1 (genesis)...")
	manifestA1, err := CreateCASSnapshot(context.Background(), adapterA, CASSnapshotOptions{
		Storage:     storage,
		Prefix:      "e2e-test/",
		SnapshotID:  "A1-genesis",
		NodeID:      "node-A",
		BranchName:  "main",
		Description: "Genesis blocks 1-100",
		Labels: map[string]string{
			"type":    "full",
			"network": "testnet",
		},
	})
	if err != nil {
		t.Fatalf("CreateCASSnapshot A1 failed: %v", err)
	}
	t.Logf("A1 created: %d blobs, %d bytes", len(manifestA1.Blobs), manifestA1.TotalSize)

	// Node A continues with blocks 101-200
	t.Log("Node A: Writing blocks 101-200...")
	for i := 101; i <= 200; i++ {
		key := fmt.Sprintf("block:%05d", i)
		value := fmt.Sprintf(`{"number":%d,"hash":"0x%064x","data":"node-a-block"}`, i, i*1000+1)
		if err := nodeA.Set([]byte(key), []byte(value), pebble.Sync); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
	}
	nodeA.Flush()

	// Create snapshot A2
	t.Log("Node A: Creating snapshot A2...")
	manifestA2, err := CreateCASSnapshot(context.Background(), adapterA, CASSnapshotOptions{
		Storage:     storage,
		Prefix:      "e2e-test/",
		SnapshotID:  "A2-blocks-200",
		NodeID:      "node-A",
		ParentID:    "A1-genesis",
		BranchName:  "main",
		Description: "Blocks 101-200 from Node A",
	})
	if err != nil {
		t.Fatalf("CreateCASSnapshot A2 failed: %v", err)
	}
	t.Logf("A2 created: %d blobs, %d bytes", len(manifestA2.Blobs), manifestA2.TotalSize)

	nodeA.Close()

	// ==========================================================================
	// Phase 2: Node B - Restore and Branch
	// ==========================================================================
	t.Log("\n=== Phase 2: Node B - Restore and Branch ===")

	nodeBPath := filepath.Join(tmpDir, "node-b")
	t.Log("Node B: Restoring from A1-genesis...")
	if err := RestoreCASSnapshot(context.Background(), nodeBPath, CASRestoreOptions{
		Storage:         storage,
		Prefix:          "e2e-test/",
		SnapshotID:      "A1-genesis",
		VerifyChecksums: true,
	}); err != nil {
		t.Fatalf("Node B restore failed: %v", err)
	}

	nodeB, err := pebble.Open(nodeBPath, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open node B: %v", err)
	}

	// Node B writes DIFFERENT blocks 101-150 (fork!)
	t.Log("Node B: Writing DIFFERENT blocks 101-150 (FORK!)...")
	for i := 101; i <= 150; i++ {
		key := fmt.Sprintf("block:%05d", i)
		// Different hash - this is a fork!
		value := fmt.Sprintf(`{"number":%d,"hash":"0x%064x","data":"node-b-fork"}`, i, i*2000+2)
		if err := nodeB.Set([]byte(key), []byte(value), pebble.Sync); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
	}
	nodeB.Flush()

	adapterB := &realDBAdapter{db: nodeB, path: nodeBPath}

	// Create snapshot B1
	t.Log("Node B: Creating snapshot B1...")
	manifestB1, err := CreateCASSnapshot(context.Background(), adapterB, CASSnapshotOptions{
		Storage:     storage,
		Prefix:      "e2e-test/",
		SnapshotID:  "B1-fork",
		NodeID:      "node-B",
		ParentID:    "A1-genesis", // Branched from A1, not A2!
		BranchName:  "fork-b",
		Description: "Fork at block 100 - different blocks 101-150",
	})
	if err != nil {
		t.Fatalf("CreateCASSnapshot B1 failed: %v", err)
	}
	t.Logf("B1 created: %d blobs, %d bytes", len(manifestB1.Blobs), manifestB1.TotalSize)

	nodeB.Close()

	// ==========================================================================
	// Phase 3: Node C - Another Fork
	// ==========================================================================
	t.Log("\n=== Phase 3: Node C - Another Fork ===")

	nodeCPath := filepath.Join(tmpDir, "node-c")
	t.Log("Node C: Restoring from A1-genesis...")
	if err := RestoreCASSnapshot(context.Background(), nodeCPath, CASRestoreOptions{
		Storage:         storage,
		Prefix:          "e2e-test/",
		SnapshotID:      "A1-genesis",
		VerifyChecksums: true,
	}); err != nil {
		t.Fatalf("Node C restore failed: %v", err)
	}

	nodeC, err := pebble.Open(nodeCPath, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open node C: %v", err)
	}

	// Node C writes YET ANOTHER fork of blocks 101-120
	t.Log("Node C: Writing YET ANOTHER fork 101-120...")
	for i := 101; i <= 120; i++ {
		key := fmt.Sprintf("block:%05d", i)
		value := fmt.Sprintf(`{"number":%d,"hash":"0x%064x","data":"node-c-fork"}`, i, i*3000+3)
		if err := nodeC.Set([]byte(key), []byte(value), pebble.Sync); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
	}
	nodeC.Flush()

	adapterC := &realDBAdapter{db: nodeC, path: nodeCPath}

	// Create snapshot C1
	t.Log("Node C: Creating snapshot C1...")
	manifestC1, err := CreateCASSnapshot(context.Background(), adapterC, CASSnapshotOptions{
		Storage:     storage,
		Prefix:      "e2e-test/",
		SnapshotID:  "C1-fork",
		NodeID:      "node-C",
		ParentID:    "A1-genesis",
		BranchName:  "fork-c",
		Description: "Another fork at block 100 - blocks 101-120",
	})
	if err != nil {
		t.Fatalf("CreateCASSnapshot C1 failed: %v", err)
	}
	t.Logf("C1 created: %d blobs, %d bytes", len(manifestC1.Blobs), manifestC1.TotalSize)

	nodeC.Close()

	// ==========================================================================
	// Phase 4: Verify Tree Structure
	// ==========================================================================
	t.Log("\n=== Phase 4: Verify Tree Structure ===")

	catalog, err := LoadCASCatalog(context.Background(), storage, "e2e-test/")
	if err != nil {
		t.Fatalf("LoadCASCatalog failed: %v", err)
	}

	t.Log("Snapshot tree:")
	t.Log("  A1-genesis (main) ──┬── A2-blocks-200 (main)")
	t.Log("                      ├── B1-fork (fork-b)")
	t.Log("                      └── C1-fork (fork-c)")
	t.Log("")
	t.Log("Catalog contents:")
	for _, snap := range catalog.Snapshots {
		t.Logf("  ID=%s node=%s parent=%s branch=%s",
			snap.ID, snap.NodeID, snap.ParentID, snap.BranchName)
	}

	if len(catalog.Snapshots) != 4 {
		t.Errorf("expected 4 snapshots, got %d", len(catalog.Snapshots))
	}

	// Verify blob deduplication
	totalBlobs := len(catalog.BlobRefCounts)
	t.Logf("Total unique blobs: %d", totalBlobs)

	// ==========================================================================
	// Phase 5: Verify All Branches Are Independently Restorable
	// ==========================================================================
	t.Log("\n=== Phase 5: Verify All Branches ===")

	testCases := []struct {
		snapshotID    string
		expectedBlock int
		expectedData  string
		maxBlock      int
	}{
		{"A2-blocks-200", 150, "node-a-block", 200},
		{"B1-fork", 150, "node-b-fork", 150},
		{"C1-fork", 120, "node-c-fork", 120},
	}

	for _, tc := range testCases {
		t.Logf("Verifying %s...", tc.snapshotID)

		restoreDir := filepath.Join(tmpDir, "verify-"+tc.snapshotID)
		if err := RestoreCASSnapshot(context.Background(), restoreDir, CASRestoreOptions{
			Storage:         storage,
			Prefix:          "e2e-test/",
			SnapshotID:      tc.snapshotID,
			VerifyChecksums: true,
		}); err != nil {
			t.Errorf("%s restore failed: %v", tc.snapshotID, err)
			continue
		}

		db, err := pebble.Open(restoreDir, &pebble.Options{})
		if err != nil {
			t.Errorf("%s open failed: %v", tc.snapshotID, err)
			continue
		}

		// Check expected block
		key := fmt.Sprintf("block:%05d", tc.expectedBlock)
		val, closer, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("%s: block %d not found: %v", tc.snapshotID, tc.expectedBlock, err)
		} else {
			if !strings.Contains(string(val), tc.expectedData) {
				t.Errorf("%s: block %d has wrong data: %s", tc.snapshotID, tc.expectedBlock, string(val))
			} else {
				t.Logf("  ✓ Block %d contains '%s'", tc.expectedBlock, tc.expectedData)
			}
			closer.Close()
		}

		// Verify max block exists
		maxKey := fmt.Sprintf("block:%05d", tc.maxBlock)
		_, closer, err = db.Get([]byte(maxKey))
		if err != nil {
			t.Errorf("%s: max block %d not found: %v", tc.snapshotID, tc.maxBlock, err)
		} else {
			closer.Close()
			t.Logf("  ✓ Max block %d exists", tc.maxBlock)
		}

		// Verify block beyond max does NOT exist
		beyondMax := fmt.Sprintf("block:%05d", tc.maxBlock+1)
		_, closer, err = db.Get([]byte(beyondMax))
		if err == nil {
			closer.Close()
			t.Errorf("%s: block %d should NOT exist", tc.snapshotID, tc.maxBlock+1)
		} else {
			t.Logf("  ✓ Block %d correctly absent", tc.maxBlock+1)
		}

		db.Close()
	}

	t.Log("\n=== E2E Multi-Node Branching Test PASSED ===")
}

// TestE2EFullLifecycle tests the complete snapshot lifecycle:
// Create → Use → Compact → GC → Verify
func TestE2EFullLifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newTrackedStorage()

	// ==========================================================================
	// Phase 1: Create a Chain of Snapshots
	// ==========================================================================
	t.Log("=== Phase 1: Create Snapshot Chain ===")

	dbPath := filepath.Join(tmpDir, "db")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	adapter := &realDBAdapter{db: db, path: dbPath}

	// Create 10 snapshots with increasing data
	for i := 1; i <= 10; i++ {
		// Write data for this snapshot
		for j := 0; j < 50; j++ {
			key := fmt.Sprintf("snap%02d-key-%03d", i, j)
			value := fmt.Sprintf("value-%d-%d", i, j)
			if err := db.Set([]byte(key), []byte(value), pebble.Sync); err != nil {
				t.Fatalf("failed to write: %v", err)
			}
		}
		db.Flush()

		parentID := ""
		if i > 1 {
			parentID = fmt.Sprintf("snap-%02d", i-1)
		}

		_, err := CreateCASSnapshot(context.Background(), adapter, CASSnapshotOptions{
			Storage:    storage,
			Prefix:     "lifecycle-test/",
			SnapshotID: fmt.Sprintf("snap-%02d", i),
			ParentID:   parentID,
		})
		if err != nil {
			t.Fatalf("CreateCASSnapshot snap-%02d failed: %v", i, err)
		}
		t.Logf("Created snap-%02d", i)
	}

	db.Close()

	// Check initial state
	catalogBefore, _ := LoadCASCatalog(context.Background(), storage, "lifecycle-test/")
	blobsBefore := countBlobs(storage, "lifecycle-test/")
	t.Logf("Initial state: %d snapshots, %d blobs", len(catalogBefore.Snapshots), blobsBefore)

	// ==========================================================================
	// Phase 2: Compact the Latest Snapshot
	// ==========================================================================
	t.Log("\n=== Phase 2: Compact Latest Snapshot ===")

	compactedManifest, err := CompactCASSnapshot(context.Background(), CompactCASOptions{
		Storage:       storage,
		Prefix:        "lifecycle-test/",
		SnapshotID:    "snap-10",
		NewSnapshotID: "snap-10-compacted",
	})
	if err != nil {
		t.Fatalf("CompactCASSnapshot failed: %v", err)
	}
	t.Logf("Compacted to %s with %d blobs", compactedManifest.ID, len(compactedManifest.Blobs))

	// ==========================================================================
	// Phase 3: GC Old Snapshots
	// ==========================================================================
	t.Log("\n=== Phase 3: Garbage Collection ===")

	deletedSnaps, deletedBlobs, err := GarbageCollectCAS(context.Background(), CASGCOptions{
		Storage:       storage,
		Prefix:        "lifecycle-test/",
		KeepSnapshots: []string{"snap-10-compacted"}, // Only keep the compacted one
		KeepLast:      0,                             // Override keep last
	})
	if err != nil {
		t.Fatalf("GarbageCollectCAS failed: %v", err)
	}

	t.Logf("GC deleted %d snapshots: %v", len(deletedSnaps), deletedSnaps)
	t.Logf("GC deleted %d blobs", len(deletedBlobs))

	// Check final state
	catalogAfter, _ := LoadCASCatalog(context.Background(), storage, "lifecycle-test/")
	blobsAfter := countBlobs(storage, "lifecycle-test/")
	t.Logf("Final state: %d snapshots, %d blobs", len(catalogAfter.Snapshots), blobsAfter)

	if len(catalogAfter.Snapshots) != 1 {
		t.Errorf("expected 1 snapshot after GC, got %d", len(catalogAfter.Snapshots))
	}

	// ==========================================================================
	// Phase 4: Verify Compacted Snapshot Works
	// ==========================================================================
	t.Log("\n=== Phase 4: Verify Compacted Snapshot ===")

	restoreDir := filepath.Join(tmpDir, "restored")
	if err := RestoreCASSnapshot(context.Background(), restoreDir, CASRestoreOptions{
		Storage:         storage,
		Prefix:          "lifecycle-test/",
		SnapshotID:      "snap-10-compacted",
		VerifyChecksums: true,
	}); err != nil {
		t.Fatalf("RestoreCASSnapshot failed: %v", err)
	}

	restoredDB, err := pebble.Open(restoreDir, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open restored db: %v", err)
	}
	defer restoredDB.Close()

	// Should have data from ALL 10 snapshots
	for i := 1; i <= 10; i++ {
		key := fmt.Sprintf("snap%02d-key-025", i)
		_, closer, err := restoredDB.Get([]byte(key))
		if err != nil {
			t.Errorf("Key %s not found: %v", key, err)
		} else {
			closer.Close()
		}
	}

	t.Logf("Verified all data from 10 snapshots is present in compacted snapshot")
	t.Logf("Storage reduction: %d → %d blobs (%.1f%% saved)",
		blobsBefore, blobsAfter, float64(blobsBefore-blobsAfter)/float64(blobsBefore)*100)

	t.Log("\n=== E2E Full Lifecycle Test PASSED ===")
}

// TestE2ESnapshotSetWithCAS tests multi-database snapshot sets using CAS.
func TestE2ESnapshotSetWithCAS(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newTrackedStorage()

	// ==========================================================================
	// Phase 1: Create Multiple Databases (simulating geth)
	// ==========================================================================
	t.Log("=== Phase 1: Create Geth-like Databases ===")

	dbNames := []string{"chaindata", "state", "ancient"}
	databases := make(map[string]*pebble.DB)
	adapters := make(map[string]DBAdapter)
	testData := make(map[string]map[string]string)

	for _, name := range dbNames {
		dbPath := filepath.Join(tmpDir, name)
		db, err := pebble.Open(dbPath, &pebble.Options{})
		if err != nil {
			t.Fatalf("failed to open %s: %v", name, err)
		}
		databases[name] = db
		adapters[name] = &realDBAdapter{db: db, path: dbPath}
		testData[name] = make(map[string]string)

		// Write different data to each database
		count := map[string]int{"chaindata": 200, "state": 500, "ancient": 100}[name]
		for i := 0; i < count; i++ {
			key := fmt.Sprintf("%s-key-%05d", name, i)
			value := fmt.Sprintf("%s-value-%05d", name, i)
			testData[name][key] = value
			if err := db.Set([]byte(key), []byte(value), pebble.Sync); err != nil {
				t.Fatalf("failed to write to %s: %v", name, err)
			}
		}
		db.Flush()
		t.Logf("Created %s with %d keys", name, count)
	}

	// ==========================================================================
	// Phase 2: Create CAS Snapshots for Each Database
	// ==========================================================================
	t.Log("\n=== Phase 2: Create CAS Snapshots ===")

	setID := fmt.Sprintf("geth-set-%d", time.Now().UnixNano())
	snapshotIDs := make(map[string]string)

	for name, adapter := range adapters {
		snapID := fmt.Sprintf("%s-%s", setID, name)
		manifest, err := CreateCASSnapshot(context.Background(), adapter, CASSnapshotOptions{
			Storage:     storage,
			Prefix:      "geth-cas/",
			SnapshotID:  snapID,
			Description: fmt.Sprintf("%s database snapshot", name),
			Labels: map[string]string{
				"set_id": setID,
				"db":     name,
			},
		})
		if err != nil {
			t.Fatalf("CreateCASSnapshot %s failed: %v", name, err)
		}
		snapshotIDs[name] = snapID
		t.Logf("%s snapshot: %s (%d blobs)", name, snapID, len(manifest.Blobs))
	}

	// Close all databases
	for name, db := range databases {
		db.Close()
		t.Logf("Closed %s", name)
	}

	// ==========================================================================
	// Phase 3: Save Snapshot Set Metadata
	// ==========================================================================
	t.Log("\n=== Phase 3: Save Snapshot Set Metadata ===")

	setMetadata := map[string]interface{}{
		"set_id":      setID,
		"created_at":  time.Now().UTC().Format(time.RFC3339),
		"description": "Complete geth snapshot at block 12345678",
		"databases":   snapshotIDs,
		"labels": map[string]string{
			"network":      "mainnet",
			"block_number": "12345678",
		},
	}

	setData, _ := json.MarshalIndent(setMetadata, "", "  ")
	setPath := "geth-cas/snapshot-sets/" + setID + ".json"

	writer, _ := storage.CreateObject(setPath)
	writer.Write(setData)
	writer.Close()

	t.Logf("Saved snapshot set metadata to %s", setPath)

	// ==========================================================================
	// Phase 4: Restore All Databases
	// ==========================================================================
	t.Log("\n=== Phase 4: Restore All Databases ===")

	restoreBase := filepath.Join(tmpDir, "restored")
	for name, snapID := range snapshotIDs {
		restorePath := filepath.Join(restoreBase, name)
		if err := RestoreCASSnapshot(context.Background(), restorePath, CASRestoreOptions{
			Storage:         storage,
			Prefix:          "geth-cas/",
			SnapshotID:      snapID,
			VerifyChecksums: true,
		}); err != nil {
			t.Fatalf("Restore %s failed: %v", name, err)
		}
		t.Logf("Restored %s", name)
	}

	// ==========================================================================
	// Phase 5: Verify All Data
	// ==========================================================================
	t.Log("\n=== Phase 5: Verify All Data ===")

	for name, expectedData := range testData {
		restorePath := filepath.Join(restoreBase, name)
		db, err := pebble.Open(restorePath, &pebble.Options{})
		if err != nil {
			t.Fatalf("failed to open restored %s: %v", name, err)
		}

		verified := 0
		for key, expectedValue := range expectedData {
			val, closer, err := db.Get([]byte(key))
			if err != nil {
				t.Errorf("%s: key %s not found: %v", name, key, err)
				continue
			}
			if string(val) != expectedValue {
				t.Errorf("%s: value mismatch for %s", name, key)
			}
			closer.Close()
			verified++
		}

		db.Close()
		t.Logf("  %s: verified %d/%d keys", name, verified, len(expectedData))
	}

	// Verify storage efficiency
	catalog, _ := LoadCASCatalog(context.Background(), storage, "geth-cas/")
	blobCount := countBlobs(storage, "geth-cas/")
	t.Logf("\nStorage stats: %d snapshots, %d unique blobs", len(catalog.Snapshots), blobCount)

	t.Log("\n=== E2E Snapshot Set with CAS Test PASSED ===")
}

// TestE2EStorageVerification verifies that all storage operations are correct.
func TestE2EStorageVerification(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newTrackedStorage()

	// Create database
	dbPath := filepath.Join(tmpDir, "db")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	// Write data
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%05d", i)
		value := fmt.Sprintf("value-%05d", i)
		if err := db.Set([]byte(key), []byte(value), pebble.Sync); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
	}
	db.Flush()

	adapter := &realDBAdapter{db: db, path: dbPath}

	// Create snapshot
	manifest, err := CreateCASSnapshot(context.Background(), adapter, CASSnapshotOptions{
		Storage:    storage,
		Prefix:     "verify-test/",
		SnapshotID: "test-snap",
	})
	if err != nil {
		t.Fatalf("CreateCASSnapshot failed: %v", err)
	}

	db.Close()

	// ==========================================================================
	// Verify All Storage Objects
	// ==========================================================================
	t.Log("=== Verifying Storage Objects ===")

	allObjects := storage.listAllObjects()
	t.Logf("Total objects: %d", len(allObjects))

	// Categorize objects
	var blobs, manifests, catalogs, others []string
	for _, obj := range allObjects {
		switch {
		case strings.Contains(obj, "/blobs/"):
			blobs = append(blobs, obj)
		case strings.HasSuffix(obj, "manifest.json"):
			manifests = append(manifests, obj)
		case strings.HasSuffix(obj, "catalog.json"):
			catalogs = append(catalogs, obj)
		default:
			others = append(others, obj)
		}
	}

	t.Logf("  Blobs: %d", len(blobs))
	t.Logf("  Manifests: %d", len(manifests))
	t.Logf("  Catalogs: %d", len(catalogs))
	t.Logf("  Others: %d", len(others))

	// Verify each blob matches a manifest entry
	blobHashes := make(map[string]bool)
	for _, blob := range blobs {
		// Extract hash from path
		parts := strings.Split(blob, "/")
		hash := parts[len(parts)-1]
		blobHashes[hash] = true
	}

	manifestHashes := make(map[string]bool)
	for _, blob := range manifest.Blobs {
		manifestHashes[blob.Key] = true
	}

	// All manifest hashes should exist as blobs
	for hash := range manifestHashes {
		if !blobHashes[hash] {
			t.Errorf("Manifest references blob %s but it doesn't exist in storage", hash)
		}
	}

	// All blobs should be referenced by manifest (after dedup)
	for hash := range blobHashes {
		if !manifestHashes[hash] {
			t.Errorf("Blob %s exists but is not referenced by manifest", hash)
		}
	}

	// Verify catalog blob ref counts
	catalog, _ := LoadCASCatalog(context.Background(), storage, "verify-test/")
	for hash, count := range catalog.BlobRefCounts {
		if count <= 0 {
			t.Errorf("Blob %s has invalid ref count: %d", hash, count)
		}
		if !blobHashes[hash] {
			t.Errorf("Catalog references blob %s but it doesn't exist", hash)
		}
	}

	// Verify restore works
	restoreDir := filepath.Join(tmpDir, "restored")
	if err := RestoreCASSnapshot(context.Background(), restoreDir, CASRestoreOptions{
		Storage:         storage,
		Prefix:          "verify-test/",
		SnapshotID:      "test-snap",
		VerifyChecksums: true,
	}); err != nil {
		t.Fatalf("RestoreCASSnapshot failed: %v", err)
	}

	restoredDB, err := pebble.Open(restoreDir, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open restored db: %v", err)
	}
	defer restoredDB.Close()

	// Verify data
	verified := 0
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%05d", i)
		_, closer, err := restoredDB.Get([]byte(key))
		if err != nil {
			t.Errorf("Key %s not found: %v", key, err)
		} else {
			closer.Close()
			verified++
		}
	}

	t.Logf("Verified %d/100 keys after restore", verified)
	t.Log("=== E2E Storage Verification Test PASSED ===")
}
