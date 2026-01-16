// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

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
	RunE:  runSnapshotList,
}

var snapshotCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new snapshot",
	RunE:  runSnapshotCreate,
}

var snapshotRestoreCmd = &cobra.Command{
	Use:   "restore [snapshot-id] [dest-dir]",
	Short: "Restore a snapshot to a directory",
	Args:  cobra.ExactArgs(2),
	RunE:  runSnapshotRestore,
}

var snapshotDeleteCmd = &cobra.Command{
	Use:   "delete [snapshot-id]",
	Short: "Delete a snapshot",
	Args:  cobra.ExactArgs(1),
	RunE:  runSnapshotDelete,
}

var snapshotInfoCmd = &cobra.Command{
	Use:   "info [snapshot-id]",
	Short: "Show detailed information about a snapshot",
	Args:  cobra.ExactArgs(1),
	RunE:  runSnapshotInfo,
}

var snapshotGCCmd = &cobra.Command{
	Use:   "gc",
	Short: "Garbage collect old snapshots",
	RunE:  runSnapshotGC,
}

var (
	snapshotIncremental bool
	snapshotKeepLast    int
	snapshotDryRun      bool
	snapshotDBName      string
)

func init() {
	rootCmd.AddCommand(snapshotCmd)

	snapshotCmd.AddCommand(snapshotListCmd)
	snapshotCmd.AddCommand(snapshotCreateCmd)
	snapshotCmd.AddCommand(snapshotRestoreCmd)
	snapshotCmd.AddCommand(snapshotDeleteCmd)
	snapshotCmd.AddCommand(snapshotInfoCmd)
	snapshotCmd.AddCommand(snapshotGCCmd)

	snapshotCreateCmd.Flags().BoolVar(&snapshotIncremental, "incremental", false, "Create incremental snapshot")
	snapshotCreateCmd.Flags().StringVar(&snapshotDBName, "db", "", "Database name (for multi-db snapshot sets)")

	snapshotGCCmd.Flags().IntVar(&snapshotKeepLast, "keep-last", 5, "Number of snapshots to keep")
	snapshotGCCmd.Flags().BoolVar(&snapshotDryRun, "dry-run", false, "Only show what would be deleted")
}

func runSnapshotList(cmd *cobra.Command, args []string) error {
	client, err := getAdminClient()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := client.Get(ctx, "/snapshots")
	if err != nil {
		return fmt.Errorf("failed to list snapshots: %w", err)
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

	fmt.Printf("Restoring snapshot %s to %s...\n", snapshotID, destDir)
	fmt.Println("Note: Direct restore via CLI is not yet implemented.")
	fmt.Println("Use the snapshot package's RestoreSnapshot function directly.")

	return nil
}

func runSnapshotDelete(cmd *cobra.Command, args []string) error {
	snapshotID := args[0]

	client, err := getAdminClient()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err = client.Delete(ctx, "/snapshots/"+snapshotID)
	if err != nil {
		return fmt.Errorf("failed to delete snapshot: %w", err)
	}

	fmt.Printf("Snapshot %s deleted\n", snapshotID)
	return nil
}

func runSnapshotInfo(cmd *cobra.Command, args []string) error {
	snapshotID := args[0]

	client, err := getAdminClient()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

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

func runSnapshotGC(cmd *cobra.Command, args []string) error {
	fmt.Printf("Garbage collecting snapshots (keep-last=%d, dry-run=%v)...\n",
		snapshotKeepLast, snapshotDryRun)
	fmt.Println("Note: GC via CLI is not yet implemented.")
	fmt.Println("Use the snapshot package's GarbageCollect function directly.")

	return nil
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
