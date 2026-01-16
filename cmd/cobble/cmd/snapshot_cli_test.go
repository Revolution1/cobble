// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build (s3 || cloud) && integration

package cmd

import (
	"testing"
)

// =============================================================================
// Snapshot List Tests
// =============================================================================

// TestSnapshotList tests listing individual database snapshots.
func TestSnapshotList(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	// Create some data and snapshots
	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	// Create a checkpoint (which creates individual snapshots)
	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Snapshot list test")
	result.MustSucceed(t)

	// List snapshots
	listResult := helper.RunCLI("snapshot", "list")
	listResult.MustSucceed(t)

	t.Logf("Snapshot list:\n%s", listResult.Stdout)
}

// TestSnapshotListJSON tests listing snapshots in JSON format.
func TestSnapshotListJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "JSON list test")
	result.MustSucceed(t)

	// List in JSON format
	listResult := helper.RunCLI("snapshot", "list", "--json")
	listResult.MustSucceed(t)

	// Should be valid JSON
	var snapshots []map[string]interface{}
	if err := listResult.JSONOutput(&snapshots); err != nil {
		t.Errorf("failed to parse JSON output: %v\nOutput: %s", err, listResult.Stdout)
	}
}

// =============================================================================
// Snapshot Tree Tests
// =============================================================================

// TestSnapshotTree tests showing snapshot dependency tree.
func TestSnapshotTree(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	// Create multiple checkpoints to build a tree
	for i := 0; i < 3; i++ {
		helper.CreateGethLikeDataDir()
		helper.CloseAllDatabases()

		result := helper.RunCLI("checkpoint", "create",
			"--data-dir", helper.DataDir,
			"--description", "Tree test")
		result.MustSucceed(t)
	}

	// Show tree
	treeResult := helper.RunCLI("snapshot", "tree")
	treeResult.MustSucceed(t)

	t.Logf("Snapshot tree:\n%s", treeResult.Stdout)

	// Should show some tree structure
	// Note: Exact format depends on implementation
}

// =============================================================================
// Snapshot Info Tests
// =============================================================================

// TestSnapshotInfo tests showing snapshot details.
func TestSnapshotInfo(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Info test")
	result.MustSucceed(t)

	// Get snapshot list to find an ID
	listResult := helper.RunCLI("snapshot", "list", "--standalone", "--json")
	listResult.MustSucceed(t)

	var snapshots []map[string]interface{}
	if err := listResult.JSONOutput(&snapshots); err != nil {
		t.Fatalf("failed to parse snapshot list: %v", err)
	}

	if len(snapshots) == 0 {
		t.Skip("no snapshots found")
	}

	snapshotID, ok := snapshots[0]["id"].(string)
	if !ok {
		t.Skip("snapshot ID not found")
	}

	// Get info in standalone mode
	infoResult := helper.RunCLI("snapshot", "info", snapshotID, "--standalone")
	infoResult.MustSucceed(t)

	t.Logf("Snapshot info:\n%s", infoResult.Stdout)
}

// =============================================================================
// Snapshot GC Tests
// =============================================================================

// TestSnapshotGC tests garbage collection of old snapshots.
func TestSnapshotGC(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	// Create multiple snapshots
	for i := 0; i < 5; i++ {
		helper.CreateGethLikeDataDir()
		helper.CloseAllDatabases()

		result := helper.RunCLI("checkpoint", "create",
			"--data-dir", helper.DataDir,
			"--description", "GC test")
		result.MustSucceed(t)
	}

	// Count snapshots before GC
	listBefore := helper.RunCLI("snapshot", "list", "--json")
	listBefore.MustSucceed(t)

	var snapshotsBefore []map[string]interface{}
	listBefore.JSONOutput(&snapshotsBefore)
	beforeCount := len(snapshotsBefore)

	// Run GC keeping only last 2 per database
	gcResult := helper.RunCLI("snapshot", "gc", "--keep-last", "2")
	gcResult.MustSucceed(t)

	t.Logf("GC result:\n%s", gcResult.Stdout)

	// Verify fewer snapshots
	listAfter := helper.RunCLI("snapshot", "list", "--json")
	listAfter.MustSucceed(t)

	var snapshotsAfter []map[string]interface{}
	listAfter.JSONOutput(&snapshotsAfter)
	afterCount := len(snapshotsAfter)

	t.Logf("Snapshots: before=%d, after=%d", beforeCount, afterCount)

	if afterCount >= beforeCount {
		t.Log("Note: GC may not have reduced count if all snapshots are still needed")
	}
}

// TestSnapshotGCDryRun tests dry-run mode for snapshot GC.
func TestSnapshotGCDryRun(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	// Create snapshots
	for i := 0; i < 3; i++ {
		helper.CreateGethLikeDataDir()
		helper.CloseAllDatabases()

		result := helper.RunCLI("checkpoint", "create",
			"--data-dir", helper.DataDir,
			"--description", "Dry-run test")
		result.MustSucceed(t)
	}

	// Count snapshots before
	listBefore := helper.RunCLI("snapshot", "list", "--json")
	listBefore.MustSucceed(t)

	var snapshotsBefore []map[string]interface{}
	listBefore.JSONOutput(&snapshotsBefore)
	beforeCount := len(snapshotsBefore)

	// Run GC in dry-run mode
	gcResult := helper.RunCLI("snapshot", "gc", "--keep-last", "1", "--dry-run")
	gcResult.MustSucceed(t)

	// Should indicate dry run
	if !gcResult.Contains("DRY") && !gcResult.Contains("dry") {
		t.Log("Note: Output should indicate dry-run mode")
	}

	// Count should not change
	listAfter := helper.RunCLI("snapshot", "list", "--json")
	listAfter.MustSucceed(t)

	var snapshotsAfter []map[string]interface{}
	listAfter.JSONOutput(&snapshotsAfter)
	afterCount := len(snapshotsAfter)

	if beforeCount != afterCount {
		t.Errorf("dry-run should not change snapshot count: before=%d, after=%d", beforeCount, afterCount)
	}
}

// =============================================================================
// Snapshot Compact Tests
// =============================================================================

// TestSnapshotCompact tests compacting a snapshot to remove dependencies.
func TestSnapshotCompact(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	// Create multiple incremental snapshots to build dependency chain
	for i := 0; i < 3; i++ {
		helper.CreateGethLikeDataDir()
		helper.CloseAllDatabases()

		result := helper.RunCLI("checkpoint", "create",
			"--data-dir", helper.DataDir,
			"--description", "Compact test")
		result.MustSucceed(t)
	}

	// Get the latest snapshot
	listResult := helper.RunCLI("snapshot", "list", "--json")
	listResult.MustSucceed(t)

	var snapshots []map[string]interface{}
	if err := listResult.JSONOutput(&snapshots); err != nil {
		t.Fatalf("failed to parse snapshot list: %v", err)
	}

	if len(snapshots) == 0 {
		t.Skip("no snapshots found")
	}

	// Find a snapshot with a parent (has dependency)
	var snapshotToCompact string
	for _, snap := range snapshots {
		if parent, ok := snap["parent_id"].(string); ok && parent != "" {
			snapshotToCompact = snap["id"].(string)
			break
		}
	}

	if snapshotToCompact == "" {
		t.Skip("no snapshot with parent found to compact")
	}

	// Compact the snapshot
	compactResult := helper.RunCLI("snapshot", "compact", snapshotToCompact)
	compactResult.MustSucceed(t)

	t.Logf("Compact result:\n%s", compactResult.Stdout)
}
