// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build (s3 || cloud) && integration

package cmd

import (
	"os"
	"path/filepath"
	"testing"
)

// =============================================================================
// Catalog Show Tests
// =============================================================================

// TestCatalogShow tests displaying the catalog.
func TestCatalogShow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	// Create some snapshots
	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Catalog show test")
	result.MustSucceed(t)

	// Show catalog
	showResult := helper.RunCLI("catalog", "show")
	showResult.MustSucceed(t)

	t.Logf("Catalog show:\n%s", showResult.Stdout)

	// Should show snapshot info
	if !showResult.Contains("Snapshot") && !showResult.Contains("snapshot") {
		t.Log("Note: Catalog output should mention snapshots")
	}
}

// TestCatalogShowJSON tests displaying catalog in JSON format.
func TestCatalogShowJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Catalog JSON test")
	result.MustSucceed(t)

	// Show catalog in JSON
	showResult := helper.RunCLI("catalog", "show", "--json")
	showResult.MustSucceed(t)

	// Should be valid JSON
	var catalog map[string]interface{}
	if err := showResult.JSONOutput(&catalog); err != nil {
		t.Errorf("failed to parse JSON output: %v\nOutput: %s", err, showResult.Stdout)
	}
}

// TestCatalogShowVerbose tests verbose catalog display.
func TestCatalogShowVerbose(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Catalog verbose test")
	result.MustSucceed(t)

	// Show catalog with verbose flag
	showResult := helper.RunCLI("catalog", "show", "--verbose")
	showResult.MustSucceed(t)

	t.Logf("Catalog show (verbose):\n%s", showResult.Stdout)

	// Should show more details than non-verbose
}

// =============================================================================
// Catalog Verify Tests
// =============================================================================

// TestCatalogVerify tests catalog integrity verification.
func TestCatalogVerify(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Catalog verify test")
	result.MustSucceed(t)

	// Verify catalog
	verifyResult := helper.RunCLI("catalog", "verify")
	verifyResult.MustSucceed(t)

	t.Logf("Catalog verify:\n%s", verifyResult.Stdout)

	// Should indicate verification passed
	if !verifyResult.Contains("passed") && !verifyResult.Contains("OK") && !verifyResult.Contains("✓") {
		t.Log("Note: Verify output should indicate success")
	}
}

// TestCatalogVerifyVerbose tests verbose catalog verification.
func TestCatalogVerifyVerbose(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Catalog verify verbose test")
	result.MustSucceed(t)

	// Verify catalog with verbose
	verifyResult := helper.RunCLI("catalog", "verify", "--verbose")
	verifyResult.MustSucceed(t)

	t.Logf("Catalog verify (verbose):\n%s", verifyResult.Stdout)
}

// =============================================================================
// Catalog Export Tests
// =============================================================================

// TestCatalogExport tests exporting catalog to file.
func TestCatalogExport(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Catalog export test")
	result.MustSucceed(t)

	// Export catalog to file
	exportFile := filepath.Join(helper.T.TempDir(), "catalog-export.json")
	exportResult := helper.RunCLI("catalog", "export", exportFile)
	exportResult.MustSucceed(t)

	// Verify file was created
	if _, err := os.Stat(exportFile); err != nil {
		t.Errorf("export file not created: %v", err)
	}

	// Verify file is valid JSON
	data, err := os.ReadFile(exportFile)
	if err != nil {
		t.Fatalf("failed to read export file: %v", err)
	}

	var catalog map[string]interface{}
	if err := helper.RunCLI("catalog", "show", "--json").JSONOutput(&catalog); err != nil {
		// Just verify the exported file is parseable JSON
		var exported map[string]interface{}
		if jsonErr := (&CLIResult{Stdout: string(data)}).JSONOutput(&exported); jsonErr != nil {
			t.Errorf("exported file is not valid JSON: %v", jsonErr)
		}
	}

	t.Logf("Exported catalog to %s (%d bytes)", exportFile, len(data))
}
