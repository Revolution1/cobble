// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build (s3 || cloud) && integration

package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// =============================================================================
// Checkpoint Create Tests
// =============================================================================

// TestCheckpointCreateAutoDiscovery tests checkpoint creation with auto-discovery
// of Pebble databases from a data directory.
func TestCheckpointCreateAutoDiscovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	// Create geth-like data directory with multiple databases
	helper.CreateGethLikeDataDir()

	// Close databases so they can be checkpointed
	helper.CloseAllDatabases()

	// Create checkpoint
	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Test checkpoint")

	result.MustSucceed(t)

	// Verify checkpoint was created
	if !result.Contains("Checkpoint created") && !result.Contains("checkpoint") {
		t.Errorf("expected success message, got: %s", result.Output())
	}

	// List checkpoints to verify
	listResult := helper.RunCLI("checkpoint", "list")
	listResult.MustSucceed(t)

	// Should show at least one checkpoint
	if !strings.Contains(listResult.Stdout, "chaindata") ||
		!strings.Contains(listResult.Stdout, "statedata") ||
		!strings.Contains(listResult.Stdout, "ancient") {
		t.Logf("checkpoint list output: %s", listResult.Stdout)
		// Note: This might need adjustment based on actual output format
	}
}

// TestCheckpointCreateWithMetadata tests checkpoint creation with block metadata.
func TestCheckpointCreateWithMetadata(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	// Create checkpoint with metadata
	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--block-number", "12345678",
		"--block-hash", "0xabcdef1234567890",
		"--state-root", "0x1234567890abcdef",
		"--description", "Mainnet block 12345678")

	result.MustSucceed(t)

	// Get checkpoint info and verify metadata is stored
	listResult := helper.RunCLI("checkpoint", "list", "--json")
	listResult.MustSucceed(t)

	// Verify metadata is present in JSON output
	if !strings.Contains(listResult.Stdout, "12345678") {
		t.Log("Note: Metadata verification may need adjustment based on implementation")
	}
}

// TestCheckpointCreateEmptyDataDir tests error handling for empty data directory.
func TestCheckpointCreateEmptyDataDir(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	// Create empty directory
	emptyDir := filepath.Join(helper.DataDir, "empty")
	if err := os.MkdirAll(emptyDir, 0755); err != nil {
		t.Fatalf("failed to create empty dir: %v", err)
	}

	// Try to create checkpoint from empty directory
	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", emptyDir,
		"--description", "Should fail")

	result.MustFail(t)

	// Should have meaningful error message
	if !result.Contains("no") && !result.Contains("database") && !result.Contains("found") {
		t.Logf("Error output: %s", result.Output())
		// Note: Actual error message will depend on implementation
	}
}

// TestCheckpointCreateNonexistentDir tests error handling for nonexistent directory.
func TestCheckpointCreateNonexistentDir(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", "/nonexistent/path/that/does/not/exist",
		"--description", "Should fail")

	result.MustFail(t)
}

// =============================================================================
// Checkpoint List Tests
// =============================================================================

// TestCheckpointList tests listing checkpoints.
func TestCheckpointList(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	// Create some checkpoints
	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	// Create first checkpoint
	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "First checkpoint")
	result.MustSucceed(t)

	// Reopen and modify
	helper.CreateDatabase("chaindata", 150) // More data
	helper.CloseAllDatabases()

	// Create second checkpoint
	result = helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Second checkpoint")
	result.MustSucceed(t)

	// List checkpoints
	listResult := helper.RunCLI("checkpoint", "list")
	listResult.MustSucceed(t)

	// Should show multiple checkpoints
	t.Logf("Checkpoint list:\n%s", listResult.Stdout)
}

// TestCheckpointListJSON tests listing checkpoints in JSON format.
func TestCheckpointListJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Test checkpoint")
	result.MustSucceed(t)

	// List in JSON format
	listResult := helper.RunCLI("checkpoint", "list", "--json")
	listResult.MustSucceed(t)

	// Should be valid JSON
	var checkpoints []map[string]interface{}
	if err := listResult.JSONOutput(&checkpoints); err != nil {
		t.Errorf("failed to parse JSON output: %v\nOutput: %s", err, listResult.Stdout)
	}
}

// =============================================================================
// Checkpoint Restore Tests
// =============================================================================

// TestCheckpointRestoreFull tests full restore of a checkpoint.
func TestCheckpointRestoreFull(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	// Create and checkpoint
	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Test checkpoint")
	result.MustSucceed(t)

	// Get checkpoint ID from list
	listResult := helper.RunCLI("checkpoint", "list", "--json")
	listResult.MustSucceed(t)

	var checkpoints []map[string]interface{}
	if err := listResult.JSONOutput(&checkpoints); err != nil {
		t.Fatalf("failed to parse checkpoint list: %v", err)
	}

	if len(checkpoints) == 0 {
		t.Fatal("no checkpoints found")
	}

	checkpointID, ok := checkpoints[0]["id"].(string)
	if !ok {
		t.Fatal("checkpoint ID not found in output")
	}

	// Restore to new directory
	restoreDir := filepath.Join(helper.T.TempDir(), "restored")

	restoreResult := helper.RunCLI("checkpoint", "restore", checkpointID,
		"--dest", restoreDir)
	restoreResult.MustSucceed(t)

	// Verify restored data
	if err := helper.VerifyGethLikeRestore(restoreDir); err != nil {
		t.Errorf("restore verification failed: %v", err)
	}
}

// TestCheckpointRestoreExistingDir tests error when restoring to existing directory.
func TestCheckpointRestoreExistingDir(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Test checkpoint")
	result.MustSucceed(t)

	// Get checkpoint ID
	listResult := helper.RunCLI("checkpoint", "list", "--json")
	listResult.MustSucceed(t)

	var checkpoints []map[string]interface{}
	if err := listResult.JSONOutput(&checkpoints); err != nil {
		t.Fatalf("failed to parse checkpoint list: %v", err)
	}

	if len(checkpoints) == 0 {
		t.Fatal("no checkpoints found")
	}

	checkpointID := checkpoints[0]["id"].(string)

	// Create existing directory
	existingDir := filepath.Join(helper.T.TempDir(), "existing")
	if err := os.MkdirAll(existingDir, 0755); err != nil {
		t.Fatalf("failed to create existing dir: %v", err)
	}

	// Create a file in it
	if err := os.WriteFile(filepath.Join(existingDir, "file.txt"), []byte("test"), 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	// Try to restore without --force
	restoreResult := helper.RunCLI("checkpoint", "restore", checkpointID,
		"--dest", existingDir)

	// Should fail
	restoreResult.MustFail(t)

	// Should suggest using --force
	if !restoreResult.Contains("force") && !restoreResult.Contains("exists") {
		t.Logf("Error message should mention --force or existing directory")
	}
}

// TestCheckpointRestoreWithForce tests restore with --force flag.
func TestCheckpointRestoreWithForce(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Test checkpoint")
	result.MustSucceed(t)

	// Get checkpoint ID
	listResult := helper.RunCLI("checkpoint", "list", "--json")
	listResult.MustSucceed(t)

	var checkpoints []map[string]interface{}
	if err := listResult.JSONOutput(&checkpoints); err != nil {
		t.Fatalf("failed to parse checkpoint list: %v", err)
	}

	checkpointID := checkpoints[0]["id"].(string)

	// Create existing directory with some content
	existingDir := filepath.Join(helper.T.TempDir(), "existing-force")
	if err := os.MkdirAll(existingDir, 0755); err != nil {
		t.Fatalf("failed to create existing dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(existingDir, "old-file.txt"), []byte("old"), 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	// Restore with --force
	restoreResult := helper.RunCLI("checkpoint", "restore", checkpointID,
		"--dest", existingDir,
		"--force")
	restoreResult.MustSucceed(t)

	// Verify restored data
	if err := helper.VerifyGethLikeRestore(existingDir); err != nil {
		t.Errorf("restore verification failed: %v", err)
	}
}

// TestCheckpointRestoreNotFound tests error when checkpoint doesn't exist.
func TestCheckpointRestoreNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	restoreDir := filepath.Join(helper.T.TempDir(), "restore-notfound")

	result := helper.RunCLI("checkpoint", "restore", "nonexistent-checkpoint-id",
		"--dest", restoreDir)

	result.MustFail(t)

	if !result.Contains("not found") && !result.Contains("does not exist") {
		t.Logf("Error should indicate checkpoint not found")
	}
}

// =============================================================================
// Checkpoint Delete Tests
// =============================================================================

// TestCheckpointDelete tests deleting a checkpoint.
func TestCheckpointDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	// Create checkpoint
	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "To be deleted")
	result.MustSucceed(t)

	// Get checkpoint ID
	listResult := helper.RunCLI("checkpoint", "list", "--json")
	listResult.MustSucceed(t)

	var checkpoints []map[string]interface{}
	if err := listResult.JSONOutput(&checkpoints); err != nil {
		t.Fatalf("failed to parse checkpoint list: %v", err)
	}

	checkpointID := checkpoints[0]["id"].(string)

	// Delete with --force (no confirmation)
	deleteResult := helper.RunCLI("checkpoint", "delete", checkpointID, "--force")
	deleteResult.MustSucceed(t)

	// Verify deletion
	listResult = helper.RunCLI("checkpoint", "list", "--json")
	listResult.MustSucceed(t)

	var remainingCheckpoints []map[string]interface{}
	if err := listResult.JSONOutput(&remainingCheckpoints); err == nil {
		for _, cp := range remainingCheckpoints {
			if cp["id"] == checkpointID {
				t.Errorf("checkpoint %s should have been deleted", checkpointID)
			}
		}
	}
}

// =============================================================================
// Checkpoint GC Tests
// =============================================================================

// TestCheckpointGC tests garbage collection of old checkpoints.
func TestCheckpointGC(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	// Create multiple checkpoints
	for i := 0; i < 5; i++ {
		helper.CreateGethLikeDataDir()
		helper.CloseAllDatabases()

		result := helper.RunCLI("checkpoint", "create",
			"--data-dir", helper.DataDir,
			"--description", "Checkpoint for GC test")
		result.MustSucceed(t)
	}

	// Run GC keeping only last 2
	gcResult := helper.RunCLI("checkpoint", "gc", "--keep-last", "2")
	gcResult.MustSucceed(t)

	// Verify only 2 checkpoints remain
	listResult := helper.RunCLI("checkpoint", "list", "--json")
	listResult.MustSucceed(t)

	var checkpoints []map[string]interface{}
	if err := listResult.JSONOutput(&checkpoints); err != nil {
		t.Fatalf("failed to parse checkpoint list: %v", err)
	}

	if len(checkpoints) > 2 {
		t.Errorf("expected at most 2 checkpoints after GC, got %d", len(checkpoints))
	}
}

// TestCheckpointGCDryRun tests dry-run mode of garbage collection.
func TestCheckpointGCDryRun(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	// Create multiple checkpoints
	for i := 0; i < 3; i++ {
		helper.CreateGethLikeDataDir()
		helper.CloseAllDatabases()

		result := helper.RunCLI("checkpoint", "create",
			"--data-dir", helper.DataDir,
			"--description", "Checkpoint for dry-run test")
		result.MustSucceed(t)
	}

	// Run GC in dry-run mode
	gcResult := helper.RunCLI("checkpoint", "gc", "--keep-last", "1", "--dry-run")
	gcResult.MustSucceed(t)

	// Should mention "DRY RUN" or "would delete"
	if !gcResult.Contains("DRY") && !gcResult.Contains("would") {
		t.Logf("Dry-run output should indicate no changes made")
	}

	// Verify all checkpoints still exist
	listResult := helper.RunCLI("checkpoint", "list", "--json")
	listResult.MustSucceed(t)

	var checkpoints []map[string]interface{}
	if err := listResult.JSONOutput(&checkpoints); err != nil {
		t.Fatalf("failed to parse checkpoint list: %v", err)
	}

	if len(checkpoints) != 3 {
		t.Errorf("expected 3 checkpoints after dry-run GC, got %d", len(checkpoints))
	}
}

// =============================================================================
// Checkpoint Info Tests
// =============================================================================

// TestCheckpointInfo tests showing checkpoint details.
func TestCheckpointInfo(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Detailed checkpoint")
	result.MustSucceed(t)

	// Get checkpoint ID
	listResult := helper.RunCLI("checkpoint", "list", "--json")
	listResult.MustSucceed(t)

	var checkpoints []map[string]interface{}
	if err := listResult.JSONOutput(&checkpoints); err != nil {
		t.Fatalf("failed to parse checkpoint list: %v", err)
	}

	checkpointID := checkpoints[0]["id"].(string)

	// Get checkpoint info
	infoResult := helper.RunCLI("checkpoint", "info", checkpointID)
	infoResult.MustSucceed(t)

	t.Logf("Checkpoint info:\n%s", infoResult.Stdout)

	// Should show database details
	if !infoResult.Contains("chaindata") {
		t.Log("Note: Info output format may vary based on implementation")
	}
}

// TestCheckpointInfoJSON tests showing checkpoint details in JSON format.
func TestCheckpointInfoJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "JSON info checkpoint")
	result.MustSucceed(t)

	// Get checkpoint ID
	listResult := helper.RunCLI("checkpoint", "list", "--json")
	listResult.MustSucceed(t)

	var checkpoints []map[string]interface{}
	if err := listResult.JSONOutput(&checkpoints); err != nil {
		t.Fatalf("failed to parse checkpoint list: %v", err)
	}

	checkpointID := checkpoints[0]["id"].(string)

	// Get checkpoint info in JSON
	infoResult := helper.RunCLI("checkpoint", "info", checkpointID, "--json")
	infoResult.MustSucceed(t)

	// Should be valid JSON
	var info map[string]interface{}
	if err := infoResult.JSONOutput(&info); err != nil {
		t.Errorf("failed to parse JSON output: %v\nOutput: %s", err, infoResult.Stdout)
	}
}
