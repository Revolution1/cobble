// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package snapshot

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble/objstorage/remote"
)

// DBAdapter is an interface for database operations needed for snapshots.
type DBAdapter interface {
	// Checkpoint creates a checkpoint of the database at the given directory.
	Checkpoint(destDir string) error

	// Path returns the path to the database directory.
	Path() string
}

// CreateSnapshot creates a snapshot of the database and uploads it to remote storage.
func CreateSnapshot(ctx context.Context, db DBAdapter, opts SnapshotOptions) (*SnapshotResult, error) {
	if opts.Storage == nil {
		return nil, fmt.Errorf("storage is required")
	}

	startTime := time.Now()

	// Generate snapshot ID if not provided
	snapshotID := opts.SnapshotID
	if snapshotID == "" {
		// Use nanoseconds to ensure uniqueness
		snapshotID = time.Now().UTC().Format("20060102T150405.000000000Z")
	}

	prefix := opts.Prefix
	if prefix == "" {
		prefix = "snapshots/"
	}

	// Report progress: starting
	if opts.ProgressFn != nil {
		opts.ProgressFn(SnapshotProgress{
			Phase: "checkpoint",
		})
	}

	// Create temporary directory for checkpoint
	tmpDir, err := os.MkdirTemp("", "cobble-checkpoint-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	checkpointDir := filepath.Join(tmpDir, "checkpoint")

	// Create checkpoint
	if err := db.Checkpoint(checkpointDir); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint: %w", err)
	}

	// Report progress: scanning files
	if opts.ProgressFn != nil {
		opts.ProgressFn(SnapshotProgress{
			Phase: "scan",
		})
	}

	// Scan checkpoint files
	files, err := scanCheckpointFiles(checkpointDir)
	if err != nil {
		return nil, fmt.Errorf("failed to scan checkpoint files: %w", err)
	}

	// Load existing catalog
	catalog, err := LoadCatalog(ctx, opts.Storage, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to load catalog: %w", err)
	}

	// Handle incremental snapshots
	var (
		parentSnapshot   *SnapshotInfo
		baseSnapshotID   string
		parentSnapshotID string
		inheritedFiles   []string
		newFiles         []string
		filesToUpload    []string
	)

	if opts.Incremental {
		// Find parent snapshot
		if opts.ParentSnapshotID != "" {
			ps, ok := catalog.GetSnapshot(opts.ParentSnapshotID)
			if !ok {
				return nil, fmt.Errorf("parent snapshot %s not found", opts.ParentSnapshotID)
			}
			parentSnapshot = ps
		} else {
			ps, ok := catalog.GetLatestSnapshot()
			if ok {
				parentSnapshot = ps
			}
		}

		if parentSnapshot != nil {
			parentSnapshotID = parentSnapshot.ID
			if parentSnapshot.Incremental {
				baseSnapshotID = parentSnapshot.BaseSnapshotID
			} else {
				baseSnapshotID = parentSnapshot.ID
			}

			// Load parent manifest to determine inherited files
			parentManifest, err := LoadManifest(ctx, opts.Storage, prefix, parentSnapshotID)
			if err != nil {
				return nil, fmt.Errorf("failed to load parent manifest: %w", err)
			}

			// Build set of parent file names
			parentFiles := make(map[string]bool)
			for _, f := range parentManifest.Files {
				parentFiles[f.Name] = true
			}

			// Determine which files to upload vs inherit
			for _, f := range files {
				if parentFiles[f] {
					inheritedFiles = append(inheritedFiles, f)
				} else {
					newFiles = append(newFiles, f)
					filesToUpload = append(filesToUpload, f)
				}
			}
		} else {
			// No parent found, do a full snapshot
			filesToUpload = files
			newFiles = files
		}
	} else {
		filesToUpload = files
	}

	// Upload files
	uploader := newUploader(opts.Storage, prefix, snapshotID, opts.Parallelism, opts.ProgressFn)
	uploadedFiles, err := uploader.uploadFiles(ctx, checkpointDir, filesToUpload)
	if err != nil {
		return nil, fmt.Errorf("failed to upload files: %w", err)
	}

	// Calculate total size
	var totalSize int64
	allFiles := make([]FileInfo, 0, len(uploadedFiles)+len(inheritedFiles))

	// Add uploaded files
	for _, f := range uploadedFiles {
		totalSize += f.Size
		allFiles = append(allFiles, f)
	}

	// For incremental, inherit file info from parent
	if parentSnapshot != nil && len(inheritedFiles) > 0 {
		parentManifest, _ := LoadManifest(ctx, opts.Storage, prefix, parentSnapshotID)
		if parentManifest != nil {
			parentFileMap := make(map[string]FileInfo)
			for _, f := range parentManifest.Files {
				parentFileMap[f.Name] = f
			}
			for _, name := range inheritedFiles {
				if f, ok := parentFileMap[name]; ok {
					totalSize += f.Size
					allFiles = append(allFiles, f)
				}
			}
		}
	}

	// Report progress: finalizing
	if opts.ProgressFn != nil {
		opts.ProgressFn(SnapshotProgress{
			Phase: "finalize",
		})
	}

	// Create manifest
	manifest := &SnapshotManifest{
		SnapshotInfo: SnapshotInfo{
			ID:               snapshotID,
			CreatedAt:        time.Now().UTC(),
			TotalSize:        totalSize,
			FileCount:        len(allFiles),
			Incremental:      opts.Incremental && parentSnapshot != nil,
			BaseSnapshotID:   baseSnapshotID,
			ParentSnapshotID: parentSnapshotID,
		},
		Files: allFiles,
	}

	if opts.Incremental && parentSnapshot != nil {
		manifest.NewFiles = uploadedFiles
		manifest.InheritedFiles = inheritedFiles

		// Determine deleted files
		if parentManifest, _ := LoadManifest(ctx, opts.Storage, prefix, parentSnapshotID); parentManifest != nil {
			currentFiles := make(map[string]bool)
			for _, f := range files {
				currentFiles[f] = true
			}
			for _, f := range parentManifest.Files {
				if !currentFiles[f.Name] {
					manifest.DeletedFiles = append(manifest.DeletedFiles, f.Name)
				}
			}
		}
	}

	// Save manifest
	if err := SaveManifest(ctx, opts.Storage, prefix, manifest); err != nil {
		return nil, fmt.Errorf("failed to save manifest: %w", err)
	}

	// Update catalog
	catalog.AddSnapshot(manifest.SnapshotInfo)
	if err := SaveCatalog(ctx, opts.Storage, prefix, catalog); err != nil {
		return nil, fmt.Errorf("failed to save catalog: %w", err)
	}

	// Calculate uploaded size
	var uploadedSize int64
	for _, f := range uploadedFiles {
		uploadedSize += f.Size
	}

	return &SnapshotResult{
		SnapshotID:       snapshotID,
		CreatedAt:        manifest.CreatedAt,
		TotalSize:        totalSize,
		FileCount:        len(allFiles),
		UploadedSize:     uploadedSize,
		UploadedCount:    len(uploadedFiles),
		Duration:         time.Since(startTime),
		Incremental:      manifest.Incremental,
		ParentSnapshotID: parentSnapshotID,
	}, nil
}

// ListSnapshots returns all available snapshots.
func List(ctx context.Context, storage remote.Storage, prefix string) ([]SnapshotInfo, error) {
	return ListSnapshots(ctx, storage, prefix)
}

// GetSnapshot returns information about a specific snapshot.
func Get(ctx context.Context, storage remote.Storage, prefix, snapshotID string) (*SnapshotManifest, error) {
	return LoadManifest(ctx, storage, prefix, snapshotID)
}
