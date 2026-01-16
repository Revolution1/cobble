// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/pebble/cobbleext/snapshot"
	"github.com/spf13/cobra"
)

var maintenanceCmd = &cobra.Command{
	Use:   "maintenance",
	Short: "Database maintenance operations",
	Long: `Maintenance commands for managing snapshots and storage.

These commands operate directly on S3/GCS storage without requiring
a running admin server (standalone mode).`,
}

var maintenanceRunCmd = &cobra.Command{
	Use:   "run",
	Short: "Run all maintenance tasks",
	Long: `Run all maintenance tasks including:
  - Garbage collect old snapshots
  - Clean up orphaned blobs
  - Compact long dependency chains

Examples:
  cobble maintenance run --keep-last 5
  cobble maintenance run --compact-threshold 10 --dry-run`,
	RunE: runMaintenanceRun,
}

var maintenanceStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show storage and snapshot status",
	Long:  `Display current status of snapshots and storage health.`,
	RunE:  runMaintenanceStatus,
}

var (
	maintenanceKeepLast         int
	maintenanceCompactThreshold int
	maintenanceDryRun           bool
	maintenanceJSON             bool
)

func init() {
	rootCmd.AddCommand(maintenanceCmd)

	maintenanceCmd.AddCommand(maintenanceRunCmd)
	maintenanceCmd.AddCommand(maintenanceStatusCmd)

	// Run command flags
	maintenanceRunCmd.Flags().IntVar(&maintenanceKeepLast, "keep-last", 5, "Number of recent snapshots to keep")
	maintenanceRunCmd.Flags().IntVar(&maintenanceCompactThreshold, "compact-threshold", 10, "Compact snapshots with dependency chains longer than this")
	maintenanceRunCmd.Flags().BoolVar(&maintenanceDryRun, "dry-run", false, "Only show what would be done")
	maintenanceRunCmd.Flags().BoolVar(&maintenanceJSON, "json", false, "Output in JSON format")

	// Status command flags
	maintenanceStatusCmd.Flags().BoolVar(&maintenanceJSON, "json", false, "Output in JSON format")
}

func runMaintenanceRun(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	storage, prefix, err := getStorageFromConfig()
	if err != nil {
		return err
	}
	defer storage.Close()

	if maintenanceDryRun {
		fmt.Println("DRY RUN - no changes will be made")
	}

	result := MaintenanceResult{
		DryRun:    maintenanceDryRun,
		StartTime: time.Now(),
	}

	// Step 1: Load catalog and analyze
	fmt.Println("=== Analyzing snapshots ===")
	catalog, err := snapshot.LoadCASCatalog(ctx, storage, prefix)
	if err != nil {
		return fmt.Errorf("failed to load catalog: %w", err)
	}

	result.TotalSnapshots = len(catalog.Snapshots)
	result.TotalBlobs = len(catalog.BlobRefCounts)

	fmt.Printf("  Found %d snapshots, %d unique blobs\n", result.TotalSnapshots, result.TotalBlobs)

	// Step 2: Find snapshots with long dependency chains
	fmt.Println("\n=== Checking dependency chains ===")
	chainsToCompact := findLongChains(catalog, maintenanceCompactThreshold)
	result.ChainsFound = len(chainsToCompact)

	if len(chainsToCompact) > 0 {
		fmt.Printf("  Found %d snapshots with chains > %d\n", len(chainsToCompact), maintenanceCompactThreshold)
		for _, chain := range chainsToCompact {
			fmt.Printf("    - %s (chain length: %d)\n", chain.ID, chain.Length)
		}

		if !maintenanceDryRun {
			for _, chain := range chainsToCompact {
				outputID := chain.ID + "-compacted"
				fmt.Printf("  Compacting %s -> %s...\n", chain.ID, outputID)

				_, err := snapshot.CompactCASSnapshotWithLock(ctx, snapshot.CompactCASOptions{
					Storage:       storage,
					Prefix:        prefix,
					SnapshotID:    chain.ID,
					NewSnapshotID: outputID,
				}, snapshot.LockOptions{
					TTL:        5 * time.Minute,
					MaxRetries: 3,
				})
				if err != nil {
					fmt.Printf("    Warning: compaction failed: %v\n", err)
				} else {
					result.SnapshotsCompacted++
				}
			}
		}
	} else {
		fmt.Println("  No long dependency chains found")
	}

	// Step 3: Garbage collection
	fmt.Println("\n=== Running garbage collection ===")
	deleted, orphaned, err := snapshot.GarbageCollectCASWithLock(ctx, snapshot.CASGCOptions{
		Storage:  storage,
		Prefix:   prefix,
		KeepLast: maintenanceKeepLast,
		DryRun:   maintenanceDryRun,
	}, snapshot.LockOptions{
		TTL:        5 * time.Minute,
		MaxRetries: 3,
	})
	if err != nil {
		return fmt.Errorf("garbage collection failed: %w", err)
	}

	result.SnapshotsDeleted = len(deleted)
	result.OrphanedBlobsCleaned = len(orphaned)

	if len(deleted) > 0 {
		fmt.Printf("  Deleted %d snapshots\n", len(deleted))
	}
	if len(orphaned) > 0 {
		fmt.Printf("  Cleaned %d orphaned blobs\n", len(orphaned))
	}
	if len(deleted) == 0 && len(orphaned) == 0 {
		fmt.Println("  Nothing to clean up")
	}

	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime).String()

	// Summary
	fmt.Println("\n=== Summary ===")
	if maintenanceJSON {
		output, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(output))
	} else {
		fmt.Printf("  Duration: %s\n", result.Duration)
		fmt.Printf("  Snapshots compacted: %d\n", result.SnapshotsCompacted)
		fmt.Printf("  Snapshots deleted: %d\n", result.SnapshotsDeleted)
		fmt.Printf("  Orphaned blobs cleaned: %d\n", result.OrphanedBlobsCleaned)
	}

	return nil
}

func runMaintenanceStatus(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	storage, prefix, err := getStorageFromConfig()
	if err != nil {
		return err
	}
	defer storage.Close()

	catalog, err := snapshot.LoadCASCatalog(ctx, storage, prefix)
	if err != nil {
		return fmt.Errorf("failed to load catalog: %w", err)
	}

	status := StorageStatus{
		Prefix:          prefix,
		SnapshotCount:   len(catalog.Snapshots),
		UniqueBlobCount: len(catalog.BlobRefCounts),
		LastUpdated:     catalog.UpdatedAt,
	}

	// Calculate total size
	for _, count := range catalog.BlobRefCounts {
		status.TotalBlobRefs += count
	}

	// Find snapshots by age
	now := time.Now()
	for _, snap := range catalog.Snapshots {
		age := now.Sub(snap.CreatedAt)
		if age < 24*time.Hour {
			status.SnapshotsLast24h++
		} else if age < 7*24*time.Hour {
			status.SnapshotsLastWeek++
		} else if age < 30*24*time.Hour {
			status.SnapshotsLastMonth++
		} else {
			status.SnapshotsOlder++
		}
		status.TotalSize += snap.TotalSize
	}

	// Calculate average chain length
	chainLengths := calculateChainLengths(catalog)
	if len(chainLengths) > 0 {
		var total int
		for _, l := range chainLengths {
			total += l
			if l > status.MaxChainLength {
				status.MaxChainLength = l
			}
		}
		status.AvgChainLength = float64(total) / float64(len(chainLengths))
	}

	if maintenanceJSON {
		output, _ := json.MarshalIndent(status, "", "  ")
		fmt.Println(string(output))
		return nil
	}

	// Human-readable output
	fmt.Println("Storage Status")
	fmt.Println("==============")
	fmt.Printf("  Prefix:          %s\n", status.Prefix)
	fmt.Printf("  Last Updated:    %s\n", status.LastUpdated.Format(time.RFC3339))
	fmt.Println()
	fmt.Println("Snapshots")
	fmt.Println("---------")
	fmt.Printf("  Total:           %d\n", status.SnapshotCount)
	fmt.Printf("  Last 24 hours:   %d\n", status.SnapshotsLast24h)
	fmt.Printf("  Last week:       %d\n", status.SnapshotsLastWeek)
	fmt.Printf("  Last month:      %d\n", status.SnapshotsLastMonth)
	fmt.Printf("  Older:           %d\n", status.SnapshotsOlder)
	fmt.Printf("  Total Size:      %s\n", formatBytes(status.TotalSize))
	fmt.Println()
	fmt.Println("Blobs")
	fmt.Println("-----")
	fmt.Printf("  Unique Blobs:    %d\n", status.UniqueBlobCount)
	fmt.Printf("  Total Refs:      %d\n", status.TotalBlobRefs)
	fmt.Printf("  Dedup Ratio:     %.2fx\n", float64(status.TotalBlobRefs)/float64(max(status.UniqueBlobCount, 1)))
	fmt.Println()
	fmt.Println("Dependency Chains")
	fmt.Println("-----------------")
	fmt.Printf("  Max Chain:       %d\n", status.MaxChainLength)
	fmt.Printf("  Avg Chain:       %.1f\n", status.AvgChainLength)

	return nil
}

// MaintenanceResult holds the result of a maintenance run.
type MaintenanceResult struct {
	DryRun               bool      `json:"dry_run"`
	StartTime            time.Time `json:"start_time"`
	EndTime              time.Time `json:"end_time"`
	Duration             string    `json:"duration"`
	TotalSnapshots       int       `json:"total_snapshots"`
	TotalBlobs           int       `json:"total_blobs"`
	ChainsFound          int       `json:"chains_found"`
	SnapshotsCompacted   int       `json:"snapshots_compacted"`
	SnapshotsDeleted     int       `json:"snapshots_deleted"`
	OrphanedBlobsCleaned int       `json:"orphaned_blobs_cleaned"`
}

// StorageStatus holds storage status information.
type StorageStatus struct {
	Prefix             string    `json:"prefix"`
	LastUpdated        time.Time `json:"last_updated"`
	SnapshotCount      int       `json:"snapshot_count"`
	SnapshotsLast24h   int       `json:"snapshots_last_24h"`
	SnapshotsLastWeek  int       `json:"snapshots_last_week"`
	SnapshotsLastMonth int       `json:"snapshots_last_month"`
	SnapshotsOlder     int       `json:"snapshots_older"`
	TotalSize          int64     `json:"total_size"`
	UniqueBlobCount    int       `json:"unique_blob_count"`
	TotalBlobRefs      int       `json:"total_blob_refs"`
	MaxChainLength     int       `json:"max_chain_length"`
	AvgChainLength     float64   `json:"avg_chain_length"`
}

// ChainInfo holds information about a snapshot's dependency chain.
type ChainInfo struct {
	ID     string
	Length int
}

func findLongChains(catalog *snapshot.CASCatalog, threshold int) []ChainInfo {
	chainLengths := calculateChainLengths(catalog)

	var result []ChainInfo
	for id, length := range chainLengths {
		if length > threshold {
			result = append(result, ChainInfo{ID: id, Length: length})
		}
	}

	// Sort by chain length (longest first)
	sort.Slice(result, func(i, j int) bool {
		return result[i].Length > result[j].Length
	})

	return result
}

func calculateChainLengths(catalog *snapshot.CASCatalog) map[string]int {
	// Build ID -> snapshot map
	snapshotByID := make(map[string]snapshot.CASManifest)
	for _, snap := range catalog.Snapshots {
		snapshotByID[snap.ID] = snap
	}

	// Calculate chain length for each snapshot
	chainLengths := make(map[string]int)
	var getChainLength func(id string) int
	getChainLength = func(id string) int {
		if length, ok := chainLengths[id]; ok {
			return length
		}
		snap, ok := snapshotByID[id]
		if !ok {
			return 0
		}
		if snap.ParentID == "" {
			chainLengths[id] = 1
			return 1
		}
		length := 1 + getChainLength(snap.ParentID)
		chainLengths[id] = length
		return length
	}

	for _, snap := range catalog.Snapshots {
		getChainLength(snap.ID)
	}

	return chainLengths
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
