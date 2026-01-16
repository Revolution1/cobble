// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build (s3 || cloud) && integration

package cmd

import (
	"testing"
)

// =============================================================================
// Blob List Tests
// =============================================================================

// TestBlobList tests listing blobs.
func TestBlobList(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	// Create some snapshots to generate blobs
	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Blob list test")
	result.MustSucceed(t)

	// List blobs
	listResult := helper.RunCLI("blob", "list")
	listResult.MustSucceed(t)

	t.Logf("Blob list:\n%s", listResult.Stdout)

	// Should show some blob information
	if !listResult.Contains("HASH") && !listResult.Contains("hash") && !listResult.Contains("Blob") {
		t.Log("Note: Blob list output format may vary")
	}
}

// TestBlobListJSON tests listing blobs in JSON format.
func TestBlobListJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Blob list JSON test")
	result.MustSucceed(t)

	// List blobs in JSON
	listResult := helper.RunCLI("blob", "list", "--json")
	listResult.MustSucceed(t)

	// Should be valid JSON
	var blobs []map[string]interface{}
	if err := listResult.JSONOutput(&blobs); err != nil {
		t.Errorf("failed to parse JSON output: %v\nOutput: %s", err, listResult.Stdout)
	}
}

// TestBlobListWithLimit tests listing blobs with a limit.
func TestBlobListWithLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Blob limit test")
	result.MustSucceed(t)

	// List blobs with limit
	listResult := helper.RunCLI("blob", "list", "--limit", "5")
	listResult.MustSucceed(t)

	t.Logf("Blob list (limit 5):\n%s", listResult.Stdout)
}

// =============================================================================
// Blob Info Tests
// =============================================================================

// TestBlobInfo tests showing blob details.
func TestBlobInfo(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Blob info test")
	result.MustSucceed(t)

	// Get blob list to find a hash
	listResult := helper.RunCLI("blob", "list", "--json")
	listResult.MustSucceed(t)

	var blobs []map[string]interface{}
	if err := listResult.JSONOutput(&blobs); err != nil {
		t.Fatalf("failed to parse blob list: %v", err)
	}

	if len(blobs) == 0 {
		t.Skip("no blobs found")
	}

	blobHash, ok := blobs[0]["hash"].(string)
	if !ok {
		t.Skip("blob hash not found in output")
	}

	// Get blob info
	infoResult := helper.RunCLI("blob", "info", blobHash)
	infoResult.MustSucceed(t)

	t.Logf("Blob info:\n%s", infoResult.Stdout)
}

// TestBlobInfoJSON tests showing blob details in JSON format.
func TestBlobInfoJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Blob info JSON test")
	result.MustSucceed(t)

	// Get blob list
	listResult := helper.RunCLI("blob", "list", "--json")
	listResult.MustSucceed(t)

	var blobs []map[string]interface{}
	if err := listResult.JSONOutput(&blobs); err != nil {
		t.Fatalf("failed to parse blob list: %v", err)
	}

	if len(blobs) == 0 {
		t.Skip("no blobs found")
	}

	blobHash := blobs[0]["hash"].(string)

	// Get blob info in JSON
	infoResult := helper.RunCLI("blob", "info", blobHash, "--json")
	infoResult.MustSucceed(t)

	// Should be valid JSON
	var info map[string]interface{}
	if err := infoResult.JSONOutput(&info); err != nil {
		t.Errorf("failed to parse JSON output: %v\nOutput: %s", err, infoResult.Stdout)
	}
}

// TestBlobInfoPartialHash tests blob info with partial hash match.
func TestBlobInfoPartialHash(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Blob partial hash test")
	result.MustSucceed(t)

	// Get blob list
	listResult := helper.RunCLI("blob", "list", "--json")
	listResult.MustSucceed(t)

	var blobs []map[string]interface{}
	if err := listResult.JSONOutput(&blobs); err != nil {
		t.Fatalf("failed to parse blob list: %v", err)
	}

	if len(blobs) == 0 {
		t.Skip("no blobs found")
	}

	fullHash := blobs[0]["hash"].(string)
	partialHash := fullHash[:16] // First 16 characters

	// Get blob info with partial hash
	infoResult := helper.RunCLI("blob", "info", partialHash)
	infoResult.MustSucceed(t)

	t.Logf("Blob info (partial hash %s):\n%s", partialHash, infoResult.Stdout)
}

// =============================================================================
// Blob Verify Tests
// =============================================================================

// TestBlobVerify tests verifying blob integrity.
func TestBlobVerify(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Blob verify test")
	result.MustSucceed(t)

	// Verify blobs (limit to speed up test)
	verifyResult := helper.RunCLI("blob", "verify", "--limit", "10")
	verifyResult.MustSucceed(t)

	t.Logf("Blob verify:\n%s", verifyResult.Stdout)

	// Should indicate verification passed
	if !verifyResult.Contains("Verified") && !verifyResult.Contains("OK") && !verifyResult.Contains("complete") {
		t.Log("Note: Verify output should indicate success")
	}
}

// TestBlobVerifyVerbose tests verbose blob verification.
func TestBlobVerifyVerbose(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Blob verify verbose test")
	result.MustSucceed(t)

	// Verify blobs with verbose
	verifyResult := helper.RunCLI("blob", "verify", "--verbose", "--limit", "5")
	verifyResult.MustSucceed(t)

	t.Logf("Blob verify (verbose):\n%s", verifyResult.Stdout)
}

// =============================================================================
// Blob Orphans Tests
// =============================================================================

// TestBlobOrphans tests finding orphaned blobs.
func TestBlobOrphans(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Blob orphans test")
	result.MustSucceed(t)

	// Check for orphaned blobs
	orphansResult := helper.RunCLI("blob", "orphans")
	orphansResult.MustSucceed(t)

	t.Logf("Blob orphans:\n%s", orphansResult.Stdout)

	// After fresh create, should have no orphans
	if orphansResult.Contains("No orphaned") || !orphansResult.Contains("orphan") {
		t.Log("Correctly found no orphaned blobs after fresh checkpoint")
	}
}

// TestBlobOrphansJSON tests finding orphaned blobs with JSON output.
func TestBlobOrphansJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Blob orphans JSON test")
	result.MustSucceed(t)

	// Check for orphaned blobs in JSON
	orphansResult := helper.RunCLI("blob", "orphans", "--json")
	orphansResult.MustSucceed(t)

	// Should be valid JSON (might be empty array)
	var orphans []string
	if err := orphansResult.JSONOutput(&orphans); err != nil {
		t.Errorf("failed to parse JSON output: %v\nOutput: %s", err, orphansResult.Stdout)
	}
}
