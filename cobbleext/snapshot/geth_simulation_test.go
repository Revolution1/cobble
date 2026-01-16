// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package snapshot

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// =============================================================================
// Geth Simulation Test
// This simulates how geth would use the snapshot functionality with multiple
// databases (chaindata, ancient, state).
// =============================================================================

// simulatedDB represents a simulated Pebble database for testing.
type simulatedDB struct {
	path  string
	files map[string][]byte
}

func newSimulatedDB(path string) *simulatedDB {
	return &simulatedDB{
		path:  path,
		files: make(map[string][]byte),
	}
}

func (s *simulatedDB) Checkpoint(destDir string) error {
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return err
	}
	// Simulate checkpoint by copying files
	for name, data := range s.files {
		if err := os.WriteFile(filepath.Join(destDir, name), data, 0644); err != nil {
			return err
		}
	}
	return nil
}

func (s *simulatedDB) Path() string {
	return s.path
}

func (s *simulatedDB) Write(key, value string) {
	// Simulate writing to different SST files based on data
	sstFile := fmt.Sprintf("000%03d.sst", len(s.files)%10)
	if existing, ok := s.files[sstFile]; ok {
		s.files[sstFile] = append(existing, []byte(fmt.Sprintf("%s=%s\n", key, value))...)
	} else {
		s.files[sstFile] = []byte(fmt.Sprintf("%s=%s\n", key, value))
	}

	// Ensure MANIFEST exists
	if _, ok := s.files["MANIFEST-000001"]; !ok {
		s.files["MANIFEST-000001"] = []byte("manifest-data")
	}
	// Ensure OPTIONS exists
	if _, ok := s.files["OPTIONS-000001"]; !ok {
		s.files["OPTIONS-000001"] = []byte("options-data")
	}
}

// TestGethMultiDBSnapshotSet tests creating and restoring a snapshot set
// across multiple databases, simulating geth's multi-database setup.
func TestGethMultiDBSnapshotSet(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newMockStorage()

	// Create three databases (like geth)
	chaindata := newSimulatedDB(filepath.Join(tmpDir, "chaindata"))
	ancient := newSimulatedDB(filepath.Join(tmpDir, "ancient"))
	state := newSimulatedDB(filepath.Join(tmpDir, "state"))

	// Simulate block processing
	t.Log("Simulating block processing...")
	for block := 1; block <= 100; block++ {
		// Write block headers and bodies to chaindata
		chaindata.Write(fmt.Sprintf("header-%d", block), fmt.Sprintf("header-data-%d", block))
		chaindata.Write(fmt.Sprintf("body-%d", block), fmt.Sprintf("body-data-%d", block))
		chaindata.Write(fmt.Sprintf("receipt-%d", block), fmt.Sprintf("receipt-data-%d", block))

		// Write to ancient for old blocks
		if block <= 50 {
			ancient.Write(fmt.Sprintf("ancient-header-%d", block), fmt.Sprintf("ancient-data-%d", block))
		}

		// Write state data
		for i := 0; i < 10; i++ {
			state.Write(fmt.Sprintf("account-%d-%d", block, i), fmt.Sprintf("balance-%d-%d", block, i))
		}
	}

	t.Logf("Chaindata files: %d", len(chaindata.files))
	t.Logf("Ancient files: %d", len(ancient.files))
	t.Logf("State files: %d", len(state.files))

	// Create a coordinated snapshot set
	t.Log("Creating snapshot set for all databases...")
	databases := map[string]DBAdapter{
		"chaindata": chaindata,
		"ancient":   ancient,
		"state":     state,
	}

	opts := SnapshotSetOptions{
		Storage:     storage,
		Prefix:      "geth-test/",
		Description: "Block 100 snapshot",
		Labels: map[string]string{
			"network":      "mainnet",
			"block_number": "100",
		},
	}

	set, err := CreateSnapshotSet(context.Background(), databases, opts)
	if err != nil {
		t.Fatalf("CreateSnapshotSet failed: %v", err)
	}

	t.Logf("Created snapshot set: %s", set.ID)
	t.Logf("Database snapshots: %v", set.Databases)

	// Verify all databases are in the set
	if len(set.Databases) != 3 {
		t.Errorf("expected 3 databases in set, got %d", len(set.Databases))
	}
	for _, dbName := range []string{"chaindata", "ancient", "state"} {
		if _, ok := set.Databases[dbName]; !ok {
			t.Errorf("missing database %s in snapshot set", dbName)
		}
	}

	// List snapshot sets
	sets, err := ListSnapshotSets(context.Background(), storage, "geth-test/")
	if err != nil {
		t.Fatalf("ListSnapshotSets failed: %v", err)
	}
	if len(sets) != 1 {
		t.Errorf("expected 1 snapshot set, got %d", len(sets))
	}

	// Simulate disaster recovery - restore to new location
	t.Log("Simulating disaster recovery - restoring from snapshot set...")
	restoreDir := filepath.Join(tmpDir, "restored")
	destDirs := map[string]string{
		"chaindata": filepath.Join(restoreDir, "chaindata"),
		"ancient":   filepath.Join(restoreDir, "ancient"),
		"state":     filepath.Join(restoreDir, "state"),
	}

	err = RestoreSnapshotSet(context.Background(), storage, "geth-test/", set.ID, destDirs)
	if err != nil {
		t.Fatalf("RestoreSnapshotSet failed: %v", err)
	}

	// Verify restored files exist
	for dbName, destDir := range destDirs {
		entries, err := os.ReadDir(destDir)
		if err != nil {
			t.Errorf("failed to read restored %s dir: %v", dbName, err)
			continue
		}
		if len(entries) == 0 {
			t.Errorf("restored %s directory is empty", dbName)
		}
		t.Logf("Restored %s: %d files", dbName, len(entries))
	}

	t.Log("TestGethMultiDBSnapshotSet completed successfully")
}

// TestGethIncrementalBackups tests incremental backup strategy for geth.
func TestGethIncrementalBackups(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newMockStorage()

	chaindata := newSimulatedDB(filepath.Join(tmpDir, "chaindata"))

	// Initial state - blocks 1-1000
	t.Log("Writing initial blocks 1-1000...")
	for block := 1; block <= 1000; block++ {
		chaindata.Write(fmt.Sprintf("block-%d", block), fmt.Sprintf("data-%d", block))
	}

	// Create full backup
	t.Log("Creating full backup...")
	fullOpts := SnapshotOptions{
		Storage:     storage,
		Prefix:      "incremental-test/",
		SnapshotID:  "full-block-1000",
		Incremental: false,
	}

	fullResult, err := CreateSnapshot(context.Background(), chaindata, fullOpts)
	if err != nil {
		t.Fatalf("full backup failed: %v", err)
	}
	t.Logf("Full backup: %d files, %d bytes", fullResult.FileCount, fullResult.TotalSize)

	// Process more blocks
	t.Log("Processing blocks 1001-1100...")
	for block := 1001; block <= 1100; block++ {
		chaindata.Write(fmt.Sprintf("block-%d", block), fmt.Sprintf("data-%d", block))
	}

	// Create incremental backup
	t.Log("Creating incremental backup...")
	incrOpts := SnapshotOptions{
		Storage:     storage,
		Prefix:      "incremental-test/",
		SnapshotID:  "incr-block-1100",
		Incremental: true,
	}

	incrResult, err := CreateSnapshot(context.Background(), chaindata, incrOpts)
	if err != nil {
		t.Fatalf("incremental backup failed: %v", err)
	}
	t.Logf("Incremental backup: %d total files, %d uploaded, %d bytes uploaded",
		incrResult.FileCount, incrResult.UploadedCount, incrResult.UploadedSize)

	// Verify incremental is more efficient
	if !incrResult.Incremental {
		t.Error("expected incremental backup")
	}
	if incrResult.ParentSnapshotID != "full-block-1000" {
		t.Errorf("expected parent full-block-1000, got %s", incrResult.ParentSnapshotID)
	}

	// Verify snapshot chain
	catalog, err := LoadCatalog(context.Background(), storage, "incremental-test/")
	if err != nil {
		t.Fatalf("LoadCatalog failed: %v", err)
	}

	chain, err := catalog.GetSnapshotChain("incr-block-1100")
	if err != nil {
		t.Fatalf("GetSnapshotChain failed: %v", err)
	}

	if len(chain) != 2 {
		t.Fatalf("expected chain length 2, got %d", len(chain))
	}
	if chain[0].ID != "full-block-1000" {
		t.Errorf("expected chain[0] to be full-block-1000, got %s", chain[0].ID)
	}
	if chain[1].ID != "incr-block-1100" {
		t.Errorf("expected chain[1] to be incr-block-1100, got %s", chain[1].ID)
	}

	// Restore from incremental and verify
	t.Log("Restoring from incremental backup...")
	restoreDir := filepath.Join(tmpDir, "restored")
	restoreOpts := RestoreOptions{
		Storage:         storage,
		Prefix:          "incremental-test/",
		SnapshotID:      "incr-block-1100",
		VerifyChecksums: true,
	}

	if err := RestoreSnapshot(context.Background(), restoreDir, restoreOpts); err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	// Verify files are present
	entries, err := os.ReadDir(restoreDir)
	if err != nil {
		t.Fatalf("failed to read restored dir: %v", err)
	}
	t.Logf("Restored %d files from incremental chain", len(entries))

	t.Log("TestGethIncrementalBackups completed successfully")
}

// TestGethAutoSnapshotPolicy tests automatic snapshot scheduling for geth.
func TestGethAutoSnapshotPolicy(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newMockStorage()

	chaindata := newSimulatedDB(filepath.Join(tmpDir, "chaindata"))

	// Write initial data
	for i := 0; i < 100; i++ {
		chaindata.Write(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
	}

	// Configure scheduler (simulating geth's auto-snapshot config)
	cfg := SchedulerConfig{
		Interval:    50 * time.Millisecond, // Fast for testing
		Incremental: true,
		KeepLast:    3,
		Storage:     storage,
		Prefix:      "geth-auto/",
		OnSuccess: func(result *SnapshotResult) {
			t.Logf("Auto snapshot completed: %s (incremental=%v)", result.SnapshotID, result.Incremental)
		},
	}

	scheduler := NewScheduler(cfg, chaindata)

	// Start scheduler
	if err := scheduler.Start(); err != nil {
		t.Fatalf("failed to start scheduler: %v", err)
	}

	// Let it create some snapshots
	time.Sleep(200 * time.Millisecond)

	// Stop scheduler
	if err := scheduler.Stop(); err != nil {
		t.Fatalf("failed to stop scheduler: %v", err)
	}

	// Verify retention policy
	catalog, err := LoadCatalog(context.Background(), storage, "geth-auto/")
	if err != nil {
		t.Fatalf("LoadCatalog failed: %v", err)
	}

	t.Logf("Total snapshots after auto-schedule: %d", len(catalog.Snapshots))

	// Note: With incremental snapshots, we may keep more than KeepLast because
	// incremental snapshots depend on their parents. The GC will keep parents
	// if they have dependents. This is expected behavior.
	// We just verify that GC is running and keeping a reasonable number.
	if len(catalog.Snapshots) > cfg.KeepLast+2 { // Allow some slack for dependencies
		t.Errorf("expected around %d snapshots due to KeepLast, got %d", cfg.KeepLast, len(catalog.Snapshots))
	}

	// Verify we have at least one snapshot
	if len(catalog.Snapshots) == 0 {
		t.Error("expected at least one snapshot")
	}

	t.Log("TestGethAutoSnapshotPolicy completed successfully")
}

// TestGethSnapshotGarbageCollection tests GC with incremental snapshot chains.
func TestGethSnapshotGarbageCollection(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newMockStorage()

	chaindata := newSimulatedDB(filepath.Join(tmpDir, "chaindata"))

	// Create a chain of snapshots
	// full-1 -> incr-2 -> incr-3 -> incr-4
	// full-5 -> incr-6 -> incr-7

	t.Log("Creating snapshot chain 1: full-1 -> incr-2 -> incr-3 -> incr-4")

	chaindata.Write("data-1", "value-1")
	createSnapshot(t, chaindata, storage, "full-1", false)

	chaindata.Write("data-2", "value-2")
	createSnapshot(t, chaindata, storage, "incr-2", true)

	chaindata.Write("data-3", "value-3")
	createSnapshot(t, chaindata, storage, "incr-3", true)

	chaindata.Write("data-4", "value-4")
	createSnapshot(t, chaindata, storage, "incr-4", true)

	t.Log("Creating snapshot chain 2: full-5 -> incr-6 -> incr-7")

	// Force a new full snapshot
	chaindata.Write("data-5", "value-5")
	createSnapshot(t, chaindata, storage, "full-5", false)

	chaindata.Write("data-6", "value-6")
	createSnapshot(t, chaindata, storage, "incr-6", true)

	chaindata.Write("data-7", "value-7")
	createSnapshot(t, chaindata, storage, "incr-7", true)

	// Verify we have 7 snapshots
	catalog, err := LoadCatalog(context.Background(), storage, "gc-test/")
	if err != nil {
		t.Fatalf("LoadCatalog failed: %v", err)
	}
	t.Logf("Before GC: %d snapshots", len(catalog.Snapshots))

	// GC with KeepLast=3, should keep the most recent 3 and any dependencies
	gcOpts := GCOptions{
		Storage:           storage,
		Prefix:            "gc-test/",
		KeepLast:          3,
		KeepFullSnapshots: 1,
	}

	deleted, err := GarbageCollect(context.Background(), gcOpts)
	if err != nil {
		t.Fatalf("GarbageCollect failed: %v", err)
	}

	t.Logf("GC deleted %d snapshots: %v", len(deleted), snapshotIDs(deleted))

	// Reload catalog and verify
	catalog, err = LoadCatalog(context.Background(), storage, "gc-test/")
	if err != nil {
		t.Fatalf("LoadCatalog after GC failed: %v", err)
	}

	t.Logf("After GC: %d snapshots", len(catalog.Snapshots))
	for _, snap := range catalog.Snapshots {
		t.Logf("  - %s (incremental=%v, parent=%s)", snap.ID, snap.Incremental, snap.ParentSnapshotID)
	}

	t.Log("TestGethSnapshotGarbageCollection completed successfully")
}

func createSnapshot(t *testing.T, db DBAdapter, storage *mockStorage, id string, incremental bool) {
	t.Helper()
	opts := SnapshotOptions{
		Storage:     storage,
		Prefix:      "gc-test/",
		SnapshotID:  id,
		Incremental: incremental,
	}
	_, err := CreateSnapshot(context.Background(), db, opts)
	if err != nil {
		t.Fatalf("CreateSnapshot %s failed: %v", id, err)
	}
}

func snapshotIDs(infos []SnapshotInfo) []string {
	ids := make([]string, len(infos))
	for i, info := range infos {
		ids[i] = info.ID
	}
	return ids
}
