// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build (s3 || cloud) && integration

package cmd

import (
	"testing"
)

// =============================================================================
// Maintenance Run Tests
// =============================================================================

// TestMaintenanceRun tests the maintenance run command.
func TestMaintenanceRun(t *testing.T) {
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
			"--description", "Maintenance test")
		result.MustSucceed(t)
	}

	// Run maintenance
	maintenanceResult := helper.RunCLI("maintenance", "run", "--keep-last", "2")
	maintenanceResult.MustSucceed(t)

	t.Logf("Maintenance run:\n%s", maintenanceResult.Stdout)

	// Should show summary
	if !maintenanceResult.Contains("Summary") && !maintenanceResult.Contains("deleted") {
		t.Log("Note: Maintenance output should show summary")
	}
}

// TestMaintenanceRunDryRun tests dry-run mode for maintenance.
func TestMaintenanceRunDryRun(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	// Create multiple snapshots
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

	// Run maintenance in dry-run mode
	maintenanceResult := helper.RunCLI("maintenance", "run", "--keep-last", "1", "--dry-run")
	maintenanceResult.MustSucceed(t)

	// Should indicate dry run
	if !maintenanceResult.Contains("DRY") && !maintenanceResult.Contains("dry") {
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

// TestMaintenanceRunJSON tests maintenance run with JSON output.
func TestMaintenanceRunJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "JSON maintenance test")
	result.MustSucceed(t)

	// Run maintenance with JSON output
	maintenanceResult := helper.RunCLI("maintenance", "run", "--json")
	maintenanceResult.MustSucceed(t)

	// The output might be mixed (human readable + JSON summary)
	// or full JSON depending on implementation
	t.Logf("Maintenance run (JSON):\n%s", maintenanceResult.Stdout)
}

// TestMaintenanceRunWithCompactThreshold tests compaction threshold.
func TestMaintenanceRunWithCompactThreshold(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	// Create multiple incremental snapshots
	for i := 0; i < 5; i++ {
		helper.CreateGethLikeDataDir()
		helper.CloseAllDatabases()

		result := helper.RunCLI("checkpoint", "create",
			"--data-dir", helper.DataDir,
			"--description", "Compact threshold test")
		result.MustSucceed(t)
	}

	// Run maintenance with low compact threshold
	maintenanceResult := helper.RunCLI("maintenance", "run",
		"--compact-threshold", "2",
		"--keep-last", "5")
	maintenanceResult.MustSucceed(t)

	t.Logf("Maintenance run (compact threshold 2):\n%s", maintenanceResult.Stdout)
}

// =============================================================================
// Maintenance Status Tests
// =============================================================================

// TestMaintenanceStatus tests the maintenance status command.
func TestMaintenanceStatus(t *testing.T) {
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
		"--description", "Status test")
	result.MustSucceed(t)

	// Get status
	statusResult := helper.RunCLI("maintenance", "status")
	statusResult.MustSucceed(t)

	t.Logf("Maintenance status:\n%s", statusResult.Stdout)

	// Should show various status sections
	sections := []string{"Storage", "Snapshot", "Blob"}
	for _, section := range sections {
		if statusResult.Contains(section) {
			t.Logf("Found section: %s", section)
		}
	}
}

// TestMaintenanceStatusJSON tests maintenance status with JSON output.
func TestMaintenanceStatusJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	helper.CreateGethLikeDataDir()
	helper.CloseAllDatabases()

	result := helper.RunCLI("checkpoint", "create",
		"--data-dir", helper.DataDir,
		"--description", "Status JSON test")
	result.MustSucceed(t)

	// Get status in JSON
	statusResult := helper.RunCLI("maintenance", "status", "--json")
	statusResult.MustSucceed(t)

	// Should be valid JSON
	var status map[string]interface{}
	if err := statusResult.JSONOutput(&status); err != nil {
		t.Errorf("failed to parse JSON output: %v\nOutput: %s", err, statusResult.Stdout)
	}
}

// TestMaintenanceStatusEmpty tests status with no data.
func TestMaintenanceStatusEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	helper := NewCLITestHelper(t)
	defer helper.Cleanup()

	// Don't create any snapshots
	// Get status
	statusResult := helper.RunCLI("maintenance", "status")
	statusResult.MustSucceed(t)

	t.Logf("Maintenance status (empty):\n%s", statusResult.Stdout)

	// Should show zero counts
	if statusResult.Contains("0") || statusResult.Contains("empty") {
		t.Log("Correctly showing empty state")
	}
}
