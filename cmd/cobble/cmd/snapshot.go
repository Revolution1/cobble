// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/pebble/cobbleext/snapshot"
	"github.com/spf13/cobra"
)

var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Manage database snapshots",
	Long:  `Commands for creating, listing, and restoring database snapshots.`,
}

var snapshotListCmd = &cobra.Command{
	Use:   "list",
	Short: "List available snapshots",
	Long: `List all available snapshots in the storage.

In standalone mode (without admin server), directly accesses S3 storage.
Configure via environment variables or flags:
  COBBLE_S3_BUCKET, COBBLE_S3_ENDPOINT, etc.`,
	RunE: runSnapshotList,
}

var snapshotCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new snapshot",
	RunE:  runSnapshotCreate,
}

var snapshotRestoreCmd = &cobra.Command{
	Use:   "restore [snapshot-id] [dest-dir]",
	Short: "Restore a snapshot to a directory",
	Long: `Restore a snapshot to the specified directory.

In standalone mode, directly accesses S3 storage to download the snapshot.`,
	Args: cobra.ExactArgs(2),
	RunE: runSnapshotRestore,
}

var snapshotDeleteCmd = &cobra.Command{
	Use:   "delete [snapshot-id]",
	Short: "Delete a snapshot",
	Long: `Delete a snapshot from storage.

Note: This only removes the snapshot manifest. Use 'snapshot gc' to clean up
orphaned blobs after deletion.`,
	Args: cobra.ExactArgs(1),
	RunE: runSnapshotDelete,
}

var snapshotInfoCmd = &cobra.Command{
	Use:   "info [snapshot-id]",
	Short: "Show detailed information about a snapshot",
	Args:  cobra.ExactArgs(1),
	RunE:  runSnapshotInfo,
}

var snapshotGCCmd = &cobra.Command{
	Use:   "gc",
	Short: "Garbage collect old snapshots and orphaned blobs",
	Long: `Garbage collect old snapshots and orphaned blobs.

This command removes old snapshots based on retention policy and cleans up
any orphaned blobs that are no longer referenced by any snapshot.

Examples:
  cobble snapshot gc --keep-last 5
  cobble snapshot gc --keep snap-1,snap-2 --dry-run
  cobble snapshot gc --older-than 7d`,
	RunE: runSnapshotGC,
}

var snapshotCompactCmd = &cobra.Command{
	Use:   "compact [snapshot-id]",
	Short: "Compact a snapshot to eliminate dependencies",
	Long: `Compact a snapshot by copying all referenced blobs into a new standalone snapshot.

After compaction, the new snapshot has no parent dependencies and can be
restored independently. This is useful for:
  - Creating portable snapshots for distribution
  - Breaking long dependency chains
  - Preparing for deletion of parent snapshots

Examples:
  cobble snapshot compact snap-10
  cobble snapshot compact snap-10 --output snap-10-standalone`,
	Args: cobra.ExactArgs(1),
	RunE: runSnapshotCompact,
}

var snapshotTreeCmd = &cobra.Command{
	Use:   "tree",
	Short: "Show snapshot dependency tree",
	Long: `Display the snapshot dependency tree showing parent-child relationships.

This helps visualize which snapshots depend on others and plan for
compaction or deletion operations.`,
	RunE: runSnapshotTree,
}

var (
	snapshotIncremental bool
	snapshotKeepLast    int
	snapshotDryRun      bool
	snapshotDBName      string
	snapshotKeepList    string
	snapshotOlderThan   string
	snapshotCompactOut  string
	snapshotTreeVerbose bool
	snapshotStandalone  bool
	snapshotJSON        bool
)

func init() {
	rootCmd.AddCommand(snapshotCmd)

	snapshotCmd.AddCommand(snapshotListCmd)
	snapshotCmd.AddCommand(snapshotCreateCmd)
	snapshotCmd.AddCommand(snapshotRestoreCmd)
	snapshotCmd.AddCommand(snapshotDeleteCmd)
	snapshotCmd.AddCommand(snapshotInfoCmd)
	snapshotCmd.AddCommand(snapshotGCCmd)
	snapshotCmd.AddCommand(snapshotCompactCmd)
	snapshotCmd.AddCommand(snapshotTreeCmd)

	// Persistent flags for snapshot commands
	snapshotCmd.PersistentFlags().BoolVar(&snapshotStandalone, "standalone", false, "Use standalone mode (direct S3 access)")
	snapshotCmd.PersistentFlags().BoolVar(&snapshotJSON, "json", false, "Output in JSON format")

	// Create command flags
	snapshotCreateCmd.Flags().BoolVar(&snapshotIncremental, "incremental", false, "Create incremental snapshot")
	snapshotCreateCmd.Flags().StringVar(&snapshotDBName, "db", "", "Database name (for multi-db snapshot sets)")

	// GC command flags
	snapshotGCCmd.Flags().IntVar(&snapshotKeepLast, "keep-last", 0, "Number of recent snapshots to keep")
	snapshotGCCmd.Flags().StringVar(&snapshotKeepList, "keep", "", "Comma-separated list of snapshot IDs to keep")
	snapshotGCCmd.Flags().StringVar(&snapshotOlderThan, "older-than", "", "Delete snapshots older than duration (e.g., 7d, 24h)")
	snapshotGCCmd.Flags().BoolVar(&snapshotDryRun, "dry-run", false, "Only show what would be deleted")

	// Compact command flags
	snapshotCompactCmd.Flags().StringVar(&snapshotCompactOut, "output", "", "Output snapshot ID (default: <id>-compacted)")

	// Tree command flags
	snapshotTreeCmd.Flags().BoolVarP(&snapshotTreeVerbose, "verbose", "v", false, "Show detailed information")
}

func runSnapshotList(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Try standalone mode first if flag is set or admin client fails
	if snapshotStandalone {
		return runSnapshotListStandalone(ctx)
	}

	client, err := getAdminClient()
	if err != nil {
		// Fall back to standalone mode
		fmt.Fprintln(os.Stderr, "Admin server not available, using standalone mode")
		return runSnapshotListStandalone(ctx)
	}

	resp, err := client.Get(ctx, "/snapshots")
	if err != nil {
		// Fall back to standalone mode
		fmt.Fprintln(os.Stderr, "Admin server not available, using standalone mode")
		return runSnapshotListStandalone(ctx)
	}

	var result struct {
		Snapshots []struct {
			ID          string    `json:"id"`
			CreatedAt   time.Time `json:"created_at"`
			TotalSize   int64     `json:"total_size"`
			FileCount   int       `json:"file_count"`
			Incremental bool      `json:"incremental"`
		} `json:"snapshots"`
	}

	if err := json.Unmarshal(resp, &result); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	if len(result.Snapshots) == 0 {
		fmt.Println("No snapshots found")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tCREATED\tSIZE\tFILES\tTYPE")
	for _, snap := range result.Snapshots {
		snapType := "full"
		if snap.Incremental {
			snapType = "incremental"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\n",
			snap.ID,
			snap.CreatedAt.Format("2006-01-02 15:04:05"),
			formatBytes(snap.TotalSize),
			snap.FileCount,
			snapType,
		)
	}
	w.Flush()

	return nil
}

func runSnapshotListStandalone(ctx context.Context) error {
	storage, prefix, err := getStorageFromConfig()
	if err != nil {
		return err
	}
	defer storage.Close()

	catalog, err := snapshot.LoadCASCatalog(ctx, storage, prefix)
	if err != nil {
		return fmt.Errorf("failed to load catalog: %w", err)
	}

	if len(catalog.Snapshots) == 0 {
		fmt.Println("No snapshots found")
		return nil
	}

	// Sort by creation time (newest first)
	snapshots := make([]snapshot.CASManifest, len(catalog.Snapshots))
	copy(snapshots, catalog.Snapshots)
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].CreatedAt.After(snapshots[j].CreatedAt)
	})

	if snapshotJSON {
		output, _ := json.MarshalIndent(snapshots, "", "  ")
		fmt.Println(string(output))
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tCREATED\tSIZE\tBLOBS\tPARENT")
	for _, snap := range snapshots {
		parent := "-"
		if snap.ParentID != "" {
			parent = snap.ParentID
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\n",
			snap.ID,
			snap.CreatedAt.Format("2006-01-02 15:04:05"),
			formatBytes(snap.TotalSize),
			len(snap.Blobs),
			parent,
		)
	}
	w.Flush()

	return nil
}

func runSnapshotCreate(cmd *cobra.Command, args []string) error {
	client, err := getAdminClient()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	body := map[string]interface{}{
		"incremental": snapshotIncremental,
	}
	if snapshotDBName != "" {
		body["db_name"] = snapshotDBName
	}

	resp, err := client.Post(ctx, "/snapshots", body)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}

	var result struct {
		SnapshotID string `json:"snapshot_id"`
		Status     string `json:"status"`
		Message    string `json:"message,omitempty"`
	}

	if err := json.Unmarshal(resp, &result); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	if result.Status == "not_implemented" {
		fmt.Println("Snapshot creation is not yet implemented via admin API")
		fmt.Println("Use the snapshot package directly or wait for the feature to be implemented")
		return nil
	}

	fmt.Printf("Snapshot created: %s\n", result.SnapshotID)
	return nil
}

func runSnapshotRestore(cmd *cobra.Command, args []string) error {
	snapshotID := args[0]
	destDir := args[1]

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	if snapshotStandalone {
		return runSnapshotRestoreStandalone(ctx, snapshotID, destDir)
	}

	// Try admin client first
	fmt.Printf("Restoring snapshot %s to %s...\n", snapshotID, destDir)
	fmt.Println("Note: Admin API restore not yet implemented. Trying standalone mode...")
	return runSnapshotRestoreStandalone(ctx, snapshotID, destDir)
}

func runSnapshotRestoreStandalone(ctx context.Context, snapshotID, destDir string) error {
	storage, prefix, err := getStorageFromConfig()
	if err != nil {
		return err
	}
	defer storage.Close()

	fmt.Printf("Restoring snapshot %s to %s...\n", snapshotID, destDir)

	err = snapshot.RestoreCASSnapshot(ctx, destDir, snapshot.CASRestoreOptions{
		Storage:    storage,
		Prefix:     prefix,
		SnapshotID: snapshotID,
		ProgressFn: func(p snapshot.CASProgress) {
			if p.BytesTotal > 0 {
				fmt.Printf("\r  %s: %d/%d files (%.1f%%)",
					p.Phase,
					p.FilesCompleted,
					p.FilesTotal,
					float64(p.BytesCompleted)/float64(p.BytesTotal)*100,
				)
			}
		},
	})
	if err != nil {
		return fmt.Errorf("failed to restore snapshot: %w", err)
	}

	fmt.Println("\nRestore complete!")
	return nil
}

func runSnapshotDelete(cmd *cobra.Command, args []string) error {
	snapshotID := args[0]
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if snapshotStandalone {
		return runSnapshotDeleteStandalone(ctx, snapshotID)
	}

	client, err := getAdminClient()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Admin server not available, using standalone mode")
		return runSnapshotDeleteStandalone(ctx, snapshotID)
	}

	_, err = client.Delete(ctx, "/snapshots/"+snapshotID)
	if err != nil {
		return fmt.Errorf("failed to delete snapshot: %w", err)
	}

	fmt.Printf("Snapshot %s deleted\n", snapshotID)
	return nil
}

func runSnapshotDeleteStandalone(ctx context.Context, snapshotID string) error {
	storage, prefix, err := getStorageFromConfig()
	if err != nil {
		return err
	}
	defer storage.Close()

	err = snapshot.DeleteCASSnapshotWithLock(ctx, storage, prefix, snapshotID, snapshot.LockOptions{
		TTL:        1 * time.Minute,
		MaxRetries: 3,
	})
	if err != nil {
		return fmt.Errorf("failed to delete snapshot: %w", err)
	}

	fmt.Printf("Snapshot %s deleted\n", snapshotID)
	fmt.Println("Note: Run 'cobble snapshot gc' to clean up orphaned blobs")
	return nil
}

func runSnapshotInfo(cmd *cobra.Command, args []string) error {
	snapshotID := args[0]
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if snapshotStandalone {
		return runSnapshotInfoStandalone(ctx, snapshotID)
	}

	client, err := getAdminClient()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Admin server not available, using standalone mode")
		return runSnapshotInfoStandalone(ctx, snapshotID)
	}

	resp, err := client.Get(ctx, "/snapshots/"+snapshotID)
	if err != nil {
		return fmt.Errorf("failed to get snapshot info: %w", err)
	}

	// Pretty print the JSON
	var result map[string]interface{}
	if err := json.Unmarshal(resp, &result); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	output, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(output))

	return nil
}

func runSnapshotInfoStandalone(ctx context.Context, snapshotID string) error {
	storage, prefix, err := getStorageFromConfig()
	if err != nil {
		return err
	}
	defer storage.Close()

	manifest, err := snapshot.LoadCASManifest(ctx, storage, prefix, snapshotID)
	if err != nil {
		return fmt.Errorf("failed to load manifest: %w", err)
	}

	if snapshotJSON {
		output, _ := json.MarshalIndent(manifest, "", "  ")
		fmt.Println(string(output))
		return nil
	}

	// Human-readable format
	fmt.Printf("Snapshot: %s\n", manifest.ID)
	fmt.Printf("  Created:    %s\n", manifest.CreatedAt.Format(time.RFC3339))
	fmt.Printf("  Parent:     %s\n", valueOrDash(manifest.ParentID))
	fmt.Printf("  Node:       %s\n", valueOrDash(manifest.NodeID))
	fmt.Printf("  Blobs:      %d\n", len(manifest.Blobs))

	var totalSize int64
	for _, blob := range manifest.Blobs {
		totalSize += blob.Size
	}
	fmt.Printf("  Total Size: %s\n", formatBytes(totalSize))

	if len(manifest.Blobs) > 0 && snapshotTreeVerbose {
		fmt.Println("\n  Files:")
		for _, blob := range manifest.Blobs {
			keyPreview := blob.Key
			if len(keyPreview) > 20 {
				keyPreview = keyPreview[:20] + "..."
			}
			fmt.Printf("    %s (%s) [%s]\n", blob.OriginalName, formatBytes(blob.Size), keyPreview)
		}
	}

	return nil
}

func runSnapshotGC(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	storage, prefix, err := getStorageFromConfig()
	if err != nil {
		return err
	}
	defer storage.Close()

	// Build keep list
	var keepSnapshots []string
	if snapshotKeepList != "" {
		keepSnapshots = strings.Split(snapshotKeepList, ",")
		for i, s := range keepSnapshots {
			keepSnapshots[i] = strings.TrimSpace(s)
		}
	}

	opts := snapshot.CASGCOptions{
		Storage:       storage,
		Prefix:        prefix,
		KeepLast:      snapshotKeepLast,
		KeepSnapshots: keepSnapshots,
		DryRun:        snapshotDryRun,
	}

	if snapshotDryRun {
		fmt.Println("DRY RUN - no changes will be made")
	}

	deleted, orphaned, err := snapshot.GarbageCollectCASWithLock(ctx, opts, snapshot.LockOptions{
		TTL:        5 * time.Minute,
		MaxRetries: 3,
	})
	if err != nil {
		return fmt.Errorf("garbage collection failed: %w", err)
	}

	if snapshotJSON {
		result := map[string]interface{}{
			"deleted_snapshots": deleted,
			"orphaned_blobs":    orphaned,
			"dry_run":           snapshotDryRun,
		}
		output, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(output))
		return nil
	}

	if len(deleted) > 0 {
		fmt.Printf("Deleted %d snapshots:\n", len(deleted))
		for _, id := range deleted {
			fmt.Printf("  - %s\n", id)
		}
	}

	if len(orphaned) > 0 {
		fmt.Printf("Cleaned up %d orphaned blobs\n", len(orphaned))
	}

	if len(deleted) == 0 && len(orphaned) == 0 {
		fmt.Println("Nothing to clean up")
	}

	return nil
}

func runSnapshotCompact(cmd *cobra.Command, args []string) error {
	snapshotID := args[0]
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	storage, prefix, err := getStorageFromConfig()
	if err != nil {
		return err
	}
	defer storage.Close()

	outputID := snapshotCompactOut
	if outputID == "" {
		outputID = snapshotID + "-compacted"
	}

	fmt.Printf("Compacting snapshot %s -> %s...\n", snapshotID, outputID)

	newManifest, err := snapshot.CompactCASSnapshotWithLock(ctx, snapshot.CompactCASOptions{
		Storage:       storage,
		Prefix:        prefix,
		SnapshotID:    snapshotID,
		NewSnapshotID: outputID,
	}, snapshot.LockOptions{
		TTL:        5 * time.Minute,
		MaxRetries: 3,
	})
	if err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}

	if snapshotJSON {
		output, _ := json.MarshalIndent(newManifest, "", "  ")
		fmt.Println(string(output))
		return nil
	}

	fmt.Printf("Compaction complete!\n")
	fmt.Printf("  New snapshot: %s\n", newManifest.ID)
	fmt.Printf("  Blobs:        %d\n", len(newManifest.Blobs))
	fmt.Printf("  Parent:       %s (was: %s)\n", valueOrDash(newManifest.ParentID), snapshotID)

	return nil
}

func runSnapshotTree(cmd *cobra.Command, args []string) error {
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

	if len(catalog.Snapshots) == 0 {
		fmt.Println("No snapshots found")
		return nil
	}

	if snapshotJSON {
		// Build tree structure for JSON
		tree := buildSnapshotTree(catalog)
		output, _ := json.MarshalIndent(tree, "", "  ")
		fmt.Println(string(output))
		return nil
	}

	// Print ASCII tree
	printSnapshotTree(catalog)
	return nil
}

type treeNode struct {
	ID        string      `json:"id"`
	CreatedAt time.Time   `json:"created_at"`
	Size      int64       `json:"size"`
	BlobCount int         `json:"blob_count"`
	Children  []*treeNode `json:"children,omitempty"`
}

func buildSnapshotTree(catalog *snapshot.CASCatalog) []*treeNode {
	// Build parent -> children map
	children := make(map[string][]snapshot.CASManifest)
	roots := make([]snapshot.CASManifest, 0)

	for _, snap := range catalog.Snapshots {
		if snap.ParentID == "" {
			roots = append(roots, snap)
		} else {
			children[snap.ParentID] = append(children[snap.ParentID], snap)
		}
	}

	// Sort roots by creation time
	sort.Slice(roots, func(i, j int) bool {
		return roots[i].CreatedAt.Before(roots[j].CreatedAt)
	})

	// Build tree recursively
	var buildNode func(snap snapshot.CASManifest) *treeNode
	buildNode = func(snap snapshot.CASManifest) *treeNode {
		node := &treeNode{
			ID:        snap.ID,
			CreatedAt: snap.CreatedAt,
			Size:      snap.TotalSize,
			BlobCount: len(snap.Blobs),
		}
		if childSnaps, ok := children[snap.ID]; ok {
			sort.Slice(childSnaps, func(i, j int) bool {
				return childSnaps[i].CreatedAt.Before(childSnaps[j].CreatedAt)
			})
			for _, child := range childSnaps {
				node.Children = append(node.Children, buildNode(child))
			}
		}
		return node
	}

	result := make([]*treeNode, 0, len(roots))
	for _, root := range roots {
		result = append(result, buildNode(root))
	}
	return result
}

func printSnapshotTree(catalog *snapshot.CASCatalog) {
	// Build parent -> children map
	children := make(map[string][]snapshot.CASManifest)
	roots := make([]snapshot.CASManifest, 0)

	for _, snap := range catalog.Snapshots {
		if snap.ParentID == "" {
			roots = append(roots, snap)
		} else {
			children[snap.ParentID] = append(children[snap.ParentID], snap)
		}
	}

	// Sort roots by creation time
	sort.Slice(roots, func(i, j int) bool {
		return roots[i].CreatedAt.Before(roots[j].CreatedAt)
	})

	// Print tree recursively
	var printNode func(snap snapshot.CASManifest, prefix string, isLast bool)
	printNode = func(snap snapshot.CASManifest, prefix string, isLast bool) {
		connector := "├── "
		if isLast {
			connector = "└── "
		}

		info := fmt.Sprintf("%s (%s, %d blobs)",
			snap.ID,
			snap.CreatedAt.Format("2006-01-02"),
			len(snap.Blobs),
		)
		if snapshotTreeVerbose {
			info = fmt.Sprintf("%s (%s, %s, %d blobs)",
				snap.ID,
				snap.CreatedAt.Format("2006-01-02 15:04:05"),
				formatBytes(snap.TotalSize),
				len(snap.Blobs),
			)
		}

		fmt.Printf("%s%s%s\n", prefix, connector, info)

		childPrefix := prefix
		if isLast {
			childPrefix += "    "
		} else {
			childPrefix += "│   "
		}

		if childSnaps, ok := children[snap.ID]; ok {
			sort.Slice(childSnaps, func(i, j int) bool {
				return childSnaps[i].CreatedAt.Before(childSnaps[j].CreatedAt)
			})
			for i, child := range childSnaps {
				printNode(child, childPrefix, i == len(childSnaps)-1)
			}
		}
	}

	fmt.Println("Snapshot Tree:")
	for i, root := range roots {
		printNode(root, "", i == len(roots)-1)
	}
}

func valueOrDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
