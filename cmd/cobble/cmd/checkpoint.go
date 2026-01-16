// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/cobbleext/snapshot"
	"github.com/spf13/cobra"
)

var (
	checkpointCmd = &cobra.Command{
		Use:   "checkpoint",
		Short: "Create and manage checkpoints backed by CAS snapshots",
		Long:  "Create, list, restore, delete, and garbage collect checkpoints stored in content-addressed storage.",
	}

	checkpointCreateCmd = &cobra.Command{
		Use:   "create",
		Short: "Create a checkpoint from a data directory",
		RunE:  runCheckpointCreate,
	}

	checkpointListCmd = &cobra.Command{
		Use:   "list",
		Short: "List checkpoints",
		RunE:  runCheckpointList,
	}

	checkpointRestoreCmd = &cobra.Command{
		Use:   "restore [checkpoint-id]",
		Short: "Restore a checkpoint to a directory",
		Args:  cobra.ExactArgs(1),
		RunE:  runCheckpointRestore,
	}

	checkpointDeleteCmd = &cobra.Command{
		Use:   "delete [checkpoint-id]",
		Short: "Delete a checkpoint",
		Args:  cobra.ExactArgs(1),
		RunE:  runCheckpointDelete,
	}

	checkpointGCCmd = &cobra.Command{
		Use:   "gc",
		Short: "Garbage collect old checkpoints",
		RunE:  runCheckpointGC,
	}

	checkpointInfoCmd = &cobra.Command{
		Use:   "info [checkpoint-id]",
		Short: "Show checkpoint details",
		Args:  cobra.ExactArgs(1),
		RunE:  runCheckpointInfo,
	}

	checkpointDataDir   string
	checkpointDesc      string
	checkpointBlockNum  string
	checkpointBlockHash string
	checkpointStateRoot string
	checkpointJSON      bool
	checkpointForce     bool
	checkpointKeepLast  int
	checkpointDryRun    bool
	checkpointDestDir   string
)

func init() {
	rootCmd.AddCommand(checkpointCmd)

	checkpointCmd.AddCommand(checkpointCreateCmd)
	checkpointCmd.AddCommand(checkpointListCmd)
	checkpointCmd.AddCommand(checkpointRestoreCmd)
	checkpointCmd.AddCommand(checkpointDeleteCmd)
	checkpointCmd.AddCommand(checkpointGCCmd)
	checkpointCmd.AddCommand(checkpointInfoCmd)

	checkpointCreateCmd.Flags().StringVar(&checkpointDataDir, "data-dir", "", "Path to the data directory containing one or more Pebble databases")
	checkpointCreateCmd.Flags().StringVar(&checkpointDesc, "description", "", "Description for the checkpoint")
	checkpointCreateCmd.Flags().StringVar(&checkpointBlockNum, "block-number", "", "Optional block number metadata")
	checkpointCreateCmd.Flags().StringVar(&checkpointBlockHash, "block-hash", "", "Optional block hash metadata")
	checkpointCreateCmd.Flags().StringVar(&checkpointStateRoot, "state-root", "", "Optional state root metadata")
	checkpointCreateCmd.MarkFlagRequired("data-dir")

	checkpointListCmd.Flags().BoolVar(&checkpointJSON, "json", false, "Output in JSON format")

	checkpointRestoreCmd.Flags().StringVar(&checkpointDestDir, "dest", "", "Destination directory for restore")
	checkpointRestoreCmd.Flags().BoolVar(&checkpointForce, "force", false, "Overwrite destination if it exists")
	checkpointRestoreCmd.MarkFlagRequired("dest")

	checkpointDeleteCmd.Flags().BoolVar(&checkpointForce, "force", false, "Skip confirmation (always on in tests)")

	checkpointGCCmd.Flags().IntVar(&checkpointKeepLast, "keep-last", 1, "Number of recent checkpoints to keep")
	checkpointGCCmd.Flags().BoolVar(&checkpointDryRun, "dry-run", false, "Show actions without deleting")

	checkpointInfoCmd.Flags().BoolVar(&checkpointJSON, "json", false, "Output in JSON format")
}

type checkpointDB struct {
	Name       string    `json:"name"`
	SnapshotID string    `json:"snapshot_id"`
	CreatedAt  time.Time `json:"created_at"`
	TotalSize  int64     `json:"total_size"`
	BlobCount  int       `json:"blob_count"`
}

type checkpointEntry struct {
	ID          string         `json:"id"`
	Description string         `json:"description,omitempty"`
	BlockNumber string         `json:"block_number,omitempty"`
	BlockHash   string         `json:"block_hash,omitempty"`
	StateRoot   string         `json:"state_root,omitempty"`
	CreatedAt   time.Time      `json:"created_at"`
	Databases   []checkpointDB `json:"databases"`
}

func runCheckpointCreate(cmd *cobra.Command, args []string) error {
	info, err := os.Stat(checkpointDataDir)
	if err != nil {
		return fmt.Errorf("data-dir not found: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("data-dir must be a directory")
	}

	storage, prefix, err := getStorageFromConfig()
	if err != nil {
		return err
	}
	defer storage.Close()

	dbDirs, err := findPebbleDBs(checkpointDataDir)
	if err != nil {
		return err
	}
	if len(dbDirs) == 0 {
		return fmt.Errorf("no Pebble databases found in data-dir (searched %s)", checkpointDataDir)
	}

	checkpointID := time.Now().UTC().Format("20060102T150405.000000000Z")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	var created []checkpointDB
	for _, dbDir := range dbDirs {
		dbName := filepath.Base(dbDir)
		db, err := pebble.Open(dbDir, &pebble.Options{})
		if err != nil {
			return fmt.Errorf("failed to open %s: %w", dbDir, err)
		}

		adapter := &pebbleAdapter{db: db, path: dbDir}
		manifest, err := snapshot.CreateCASSnapshot(ctx, adapter, snapshot.CASSnapshotOptions{
			Storage:     storage,
			Prefix:      prefix,
			SnapshotID:  fmt.Sprintf("%s-%s", checkpointID, dbName),
			Description: checkpointDesc,
			Labels: map[string]string{
				"checkpoint_id": checkpointID,
				"db_name":       dbName,
				"block_number":  checkpointBlockNum,
				"block_hash":    checkpointBlockHash,
				"state_root":    checkpointStateRoot,
			},
		})
		db.Close()
		if err != nil {
			return fmt.Errorf("failed to create snapshot for %s: %w", dbName, err)
		}

		created = append(created, checkpointDB{
			Name:       dbName,
			SnapshotID: manifest.ID,
			CreatedAt:  manifest.CreatedAt,
			TotalSize:  manifest.TotalSize,
			BlobCount:  len(manifest.Blobs),
		})
	}

	fmt.Printf("Checkpoint created: %s (%d databases)\n", checkpointID, len(created))
	return nil
}

func runCheckpointList(cmd *cobra.Command, args []string) error {
	entries, err := loadCheckpoints()
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		fmt.Println("No checkpoints found")
		return nil
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].CreatedAt.After(entries[j].CreatedAt) })

	if checkpointJSON {
		output, _ := json.MarshalIndent(entries, "", "  ")
		fmt.Println(string(output))
		return nil
	}

	for _, cp := range entries {
		fmt.Printf("%s  (%s)  dbs: %d\n", cp.ID, cp.CreatedAt.Format(time.RFC3339), len(cp.Databases))
		if cp.Description != "" {
			fmt.Printf("  desc: %s\n", cp.Description)
		}
		if cp.BlockNumber != "" || cp.BlockHash != "" || cp.StateRoot != "" {
			fmt.Printf("  block: %s hash: %s state: %s\n", cp.BlockNumber, cp.BlockHash, cp.StateRoot)
		}
		for _, db := range cp.Databases {
			fmt.Printf("  - %s (%s, %d blobs)\n", db.Name, formatBytes(db.TotalSize), db.BlobCount)
		}
		fmt.Println()
	}

	return nil
}

func runCheckpointRestore(cmd *cobra.Command, args []string) error {
	checkpointID := args[0]

	if checkpointDestDir == "" {
		return errors.New("--dest is required")
	}

	if err := validateDest(checkpointDestDir, checkpointForce); err != nil {
		return err
	}

	entries, err := loadCheckpoints()
	if err != nil {
		return err
	}

	var target *checkpointEntry
	for i := range entries {
		if entries[i].ID == checkpointID {
			target = &entries[i]
			break
		}
	}
	if target == nil {
		return fmt.Errorf("checkpoint %s not found", checkpointID)
	}

	storage, prefix, err := getStorageFromConfig()
	if err != nil {
		return err
	}
	defer storage.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	for _, db := range target.Databases {
		dest := filepath.Join(checkpointDestDir, db.Name)
		if err := os.MkdirAll(dest, 0755); err != nil {
			return fmt.Errorf("failed to create dest %s: %w", dest, err)
		}

		err = snapshot.RestoreCASSnapshot(ctx, dest, snapshot.CASRestoreOptions{
			Storage:    storage,
			Prefix:     prefix,
			SnapshotID: db.SnapshotID,
		})
		if err != nil {
			return fmt.Errorf("failed to restore %s: %w", db.Name, err)
		}
	}

	fmt.Printf("Checkpoint %s restored to %s\n", checkpointID, checkpointDestDir)
	return nil
}

func runCheckpointDelete(cmd *cobra.Command, args []string) error {
	checkpointID := args[0]

	entries, err := loadCheckpoints()
	if err != nil {
		return err
	}

	storage, prefix, err := getStorageFromConfig()
	if err != nil {
		return err
	}
	defer storage.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	deleted := 0
	for _, cp := range entries {
		if cp.ID != checkpointID {
			continue
		}
		for _, db := range cp.Databases {
			if err := snapshot.DeleteCASSnapshotWithLock(ctx, storage, prefix, db.SnapshotID, snapshot.LockOptions{TTL: time.Minute, MaxRetries: 3}); err != nil {
				return fmt.Errorf("failed to delete snapshot %s: %w", db.SnapshotID, err)
			}
		}
		deleted++
	}

	if deleted == 0 {
		return fmt.Errorf("checkpoint %s not found", checkpointID)
	}

	fmt.Printf("Checkpoint %s deleted\n", checkpointID)
	return nil
}

func runCheckpointGC(cmd *cobra.Command, args []string) error {
	entries, err := loadCheckpoints()
	if err != nil {
		return err
	}

	if checkpointKeepLast < 0 {
		checkpointKeepLast = 0
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].CreatedAt.After(entries[j].CreatedAt) })

	var toDelete []checkpointEntry
	if len(entries) > checkpointKeepLast {
		toDelete = entries[checkpointKeepLast:]
	}

	if len(toDelete) == 0 {
		fmt.Println("Nothing to delete")
		return nil
	}

	if checkpointDryRun {
		fmt.Println("DRY RUN - would delete:")
		for _, cp := range toDelete {
			fmt.Printf("  %s (created %s)\n", cp.ID, cp.CreatedAt.Format(time.RFC3339))
		}
		return nil
	}

	storage, prefix, err := getStorageFromConfig()
	if err != nil {
		return err
	}
	defer storage.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	for _, cp := range toDelete {
		for _, db := range cp.Databases {
			if err := snapshot.DeleteCASSnapshotWithLock(ctx, storage, prefix, db.SnapshotID, snapshot.LockOptions{TTL: 2 * time.Minute, MaxRetries: 3}); err != nil {
				return fmt.Errorf("failed to delete snapshot %s: %w", db.SnapshotID, err)
			}
		}
	}

	fmt.Printf("Deleted %d checkpoints\n", len(toDelete))
	return nil
}

func runCheckpointInfo(cmd *cobra.Command, args []string) error {
	checkpointID := args[0]
	entries, err := loadCheckpoints()
	if err != nil {
		return err
	}

	var target *checkpointEntry
	for i := range entries {
		if entries[i].ID == checkpointID {
			target = &entries[i]
			break
		}
	}
	if target == nil {
		return fmt.Errorf("checkpoint %s not found", checkpointID)
	}

	if checkpointJSON {
		output, _ := json.MarshalIndent(target, "", "  ")
		fmt.Println(string(output))
		return nil
	}

	fmt.Printf("Checkpoint %s\n", target.ID)
	if target.Description != "" {
		fmt.Printf("Description: %s\n", target.Description)
	}
	if target.BlockNumber != "" || target.BlockHash != "" || target.StateRoot != "" {
		fmt.Printf("Block: %s\nHash: %s\nState: %s\n", target.BlockNumber, target.BlockHash, target.StateRoot)
	}
	fmt.Printf("Created: %s\n", target.CreatedAt.Format(time.RFC3339))
	fmt.Println("Databases:")
	for _, db := range target.Databases {
		fmt.Printf("  - %s (snapshot %s, %d blobs, %s)\n", db.Name, db.SnapshotID, db.BlobCount, formatBytes(db.TotalSize))
	}
	return nil
}

func findPebbleDBs(dir string) ([]string, error) {
	found := make(map[string]struct{})
	if err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		// Check for MANIFEST file (Pebble DB marker)
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil
		}
		for _, entry := range entries {
			if !entry.IsDir() && len(entry.Name()) > 8 && entry.Name()[:8] == "MANIFEST" {
				found[path] = struct{}{}
				// Skip descending into this DB directory
				return filepath.SkipDir
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	var dbDirs []string
	for dir := range found {
		dbDirs = append(dbDirs, dir)
	}

	sort.Strings(dbDirs)
	return dbDirs, nil
}

func loadCheckpoints() ([]checkpointEntry, error) {
	storage, prefix, err := getStorageFromConfig()
	if err != nil {
		return nil, err
	}
	defer storage.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	catalog, err := snapshot.LoadCASCatalog(ctx, storage, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to load catalog: %w", err)
	}

	groups := make(map[string]*checkpointEntry)
	for _, m := range catalog.Snapshots {
		cpID := m.Labels["checkpoint_id"]
		if cpID == "" {
			continue
		}
		dbName := m.Labels["db_name"]
		entry, ok := groups[cpID]
		if !ok {
			entry = &checkpointEntry{ID: cpID, Description: m.Description, BlockNumber: m.Labels["block_number"], BlockHash: m.Labels["block_hash"], StateRoot: m.Labels["state_root"], CreatedAt: m.CreatedAt}
			groups[cpID] = entry
		}
		if m.CreatedAt.Before(entry.CreatedAt) {
			entry.CreatedAt = m.CreatedAt
		}
		entry.Databases = append(entry.Databases, checkpointDB{
			Name:       dbName,
			SnapshotID: m.ID,
			CreatedAt:  m.CreatedAt,
			TotalSize:  m.TotalSize,
			BlobCount:  len(m.Blobs),
		})
	}

	var entries []checkpointEntry
	for _, e := range groups {
		entries = append(entries, *e)
	}
	return entries, nil
}

func validateDest(dest string, force bool) error {
	info, err := os.Stat(dest)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("destination exists and is not a directory")
	}
	entries, err := os.ReadDir(dest)
	if err != nil {
		return err
	}
	if len(entries) > 0 && !force {
		return fmt.Errorf("destination directory exists; use --force to overwrite")
	}
	if force {
		return os.RemoveAll(dest)
	}
	return nil
}

// pebbleAdapter wraps pebble.DB to satisfy snapshot.DBAdapter.
type pebbleAdapter struct {
	db   *pebble.DB
	path string
}

func (p *pebbleAdapter) Checkpoint(destDir string) error {
	return p.db.Checkpoint(destDir, pebble.WithFlushedWAL())
}

func (p *pebbleAdapter) Path() string {
	return p.path
}
