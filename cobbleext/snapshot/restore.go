// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/objstorage/remote"
)

// RestoreSnapshot restores a snapshot from remote storage to a local directory.
func RestoreSnapshot(ctx context.Context, destDir string, opts RestoreOptions) error {
	if opts.Storage == nil {
		return fmt.Errorf("storage is required")
	}

	prefix := opts.Prefix
	if prefix == "" {
		prefix = "snapshots/"
	}

	parallelism := opts.Parallelism
	if parallelism <= 0 {
		parallelism = runtime.NumCPU()
	}

	// Report progress: fetching manifest
	if opts.ProgressFn != nil {
		opts.ProgressFn(RestoreProgress{
			Phase: "fetch_manifest",
		})
	}

	// Determine snapshot ID
	snapshotID := opts.SnapshotID
	if snapshotID == "" {
		// Get latest snapshot
		catalog, err := LoadCatalog(ctx, opts.Storage, prefix)
		if err != nil {
			return fmt.Errorf("failed to load catalog: %w", err)
		}
		latest, ok := catalog.GetLatestSnapshot()
		if !ok {
			return fmt.Errorf("no snapshots available")
		}
		snapshotID = latest.ID
	}

	// Load catalog to get snapshot chain if needed
	catalog, err := LoadCatalog(ctx, opts.Storage, prefix)
	if err != nil {
		return fmt.Errorf("failed to load catalog: %w", err)
	}

	// Get snapshot chain (for incremental snapshots)
	chain, err := catalog.GetSnapshotChain(snapshotID)
	if err != nil {
		return fmt.Errorf("failed to get snapshot chain: %w", err)
	}

	// Collect all files to download from the chain
	// For incremental chains, we need files from all snapshots in the chain
	filesToDownload := make(map[string]downloadInfo)

	for _, snapInfo := range chain {
		manifest, err := LoadManifest(ctx, opts.Storage, prefix, snapInfo.ID)
		if err != nil {
			return fmt.Errorf("failed to load manifest for %s: %w", snapInfo.ID, err)
		}

		// For full snapshots or the target incremental snapshot
		if !snapInfo.Incremental {
			// Full snapshot: include all files
			for _, f := range manifest.Files {
				filesToDownload[f.Name] = downloadInfo{
					file:       f,
					snapshotID: snapInfo.ID,
				}
			}
		} else {
			// Incremental snapshot: only include new files
			for _, f := range manifest.NewFiles {
				filesToDownload[f.Name] = downloadInfo{
					file:       f,
					snapshotID: snapInfo.ID,
				}
			}
			// Remove deleted files
			for _, name := range manifest.DeletedFiles {
				delete(filesToDownload, name)
			}
		}
	}

	// Create destination directory
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	// Calculate total size
	var totalSize int64
	downloadList := make([]downloadInfo, 0, len(filesToDownload))
	for _, info := range filesToDownload {
		totalSize += info.file.Size
		downloadList = append(downloadList, info)
	}

	// Report progress: starting download
	if opts.ProgressFn != nil {
		opts.ProgressFn(RestoreProgress{
			Phase:      "download",
			FilesTotal: len(downloadList),
			BytesTotal: totalSize,
		})
	}

	// Download files in parallel
	downloader := &downloader{
		storage:        opts.Storage,
		prefix:         prefix,
		destDir:        destDir,
		parallelism:    parallelism,
		verifyChecksum: opts.VerifyChecksums,
		progressFn:     opts.ProgressFn,
		totalFiles:     len(downloadList),
		totalSize:      totalSize,
	}

	if err := downloader.downloadFiles(ctx, downloadList); err != nil {
		return fmt.Errorf("failed to download files: %w", err)
	}

	// Report progress: finalizing
	if opts.ProgressFn != nil {
		opts.ProgressFn(RestoreProgress{
			Phase:          "finalize",
			FilesTotal:     len(downloadList),
			FilesCompleted: len(downloadList),
			BytesTotal:     totalSize,
			BytesCompleted: totalSize,
		})
	}

	return nil
}

// downloadInfo contains information about a file to download.
type downloadInfo struct {
	file       FileInfo
	snapshotID string
}

// downloader handles parallel file downloads from remote storage.
type downloader struct {
	storage        remote.Storage
	prefix         string
	destDir        string
	parallelism    int
	verifyChecksum bool
	progressFn     func(RestoreProgress)
	totalFiles     int
	totalSize      int64
}

// downloadFiles downloads files in parallel.
func (d *downloader) downloadFiles(ctx context.Context, files []downloadInfo) error {
	if len(files) == 0 {
		return nil
	}

	// Create work channel
	workCh := make(chan downloadInfo, len(files))
	for _, f := range files {
		workCh <- f
	}
	close(workCh)

	// Progress tracking
	var filesCompleted atomic.Int32
	var bytesCompleted atomic.Int64

	// Error collection
	var mu sync.Mutex
	var errs []error

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < d.parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for info := range workCh {
				select {
				case <-ctx.Done():
					mu.Lock()
					errs = append(errs, ctx.Err())
					mu.Unlock()
					continue
				default:
				}

				if err := d.downloadFile(ctx, info); err != nil {
					mu.Lock()
					errs = append(errs, fmt.Errorf("download %s: %w", info.file.Name, err))
					mu.Unlock()
					continue
				}

				filesCompleted.Add(1)
				bytesCompleted.Add(info.file.Size)

				if d.progressFn != nil {
					d.progressFn(RestoreProgress{
						Phase:          "download",
						FilesTotal:     d.totalFiles,
						FilesCompleted: int(filesCompleted.Load()),
						BytesTotal:     d.totalSize,
						BytesCompleted: bytesCompleted.Load(),
						CurrentFile:    info.file.Name,
					})
				}
			}
		}()
	}

	wg.Wait()

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// downloadFile downloads a single file.
func (d *downloader) downloadFile(ctx context.Context, info downloadInfo) error {
	srcPath := path.Join(d.prefix, info.snapshotID, info.file.Name)
	destPath := filepath.Join(d.destDir, info.file.Name)

	// Get file size
	size, err := d.storage.Size(srcPath)
	if err != nil {
		return fmt.Errorf("failed to get size: %w", err)
	}

	// Open reader
	reader, objSize, err := d.storage.ReadObject(ctx, srcPath)
	if err != nil {
		return fmt.Errorf("failed to open: %w", err)
	}
	defer reader.Close()

	if objSize > 0 {
		size = objSize
	}

	// Create destination file
	f, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	// Read and optionally verify checksum
	var hash = sha256.New()
	data := make([]byte, size)
	if err := reader.ReadAt(ctx, data, 0); err != nil {
		return fmt.Errorf("failed to read: %w", err)
	}

	if d.verifyChecksum {
		hash.Write(data)
	}

	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("failed to write: %w", err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to sync: %w", err)
	}

	// Verify checksum
	if d.verifyChecksum && info.file.Checksum != "" {
		expected := info.file.Checksum
		if strings.HasPrefix(expected, "sha256:") {
			expected = strings.TrimPrefix(expected, "sha256:")
		}
		actual := hex.EncodeToString(hash.Sum(nil))
		if actual != expected {
			os.Remove(destPath)
			return fmt.Errorf("checksum mismatch: expected %s, got %s", expected, actual)
		}
	}

	return nil
}

// DeleteSnapshot deletes a snapshot and its files from remote storage.
func DeleteSnapshot(ctx context.Context, storage remote.Storage, prefix, snapshotID string) error {
	if prefix == "" {
		prefix = "snapshots/"
	}

	// Load catalog
	catalog, err := LoadCatalog(ctx, storage, prefix)
	if err != nil {
		return fmt.Errorf("failed to load catalog: %w", err)
	}

	// Check if snapshot exists
	snapshot, ok := catalog.GetSnapshot(snapshotID)
	if !ok {
		return fmt.Errorf("snapshot %s not found", snapshotID)
	}

	// Check for dependents
	if catalog.HasDependents(snapshotID) {
		return fmt.Errorf("snapshot %s has dependent snapshots", snapshotID)
	}

	// Load manifest to get file list
	manifest, err := LoadManifest(ctx, storage, prefix, snapshotID)
	if err != nil {
		return fmt.Errorf("failed to load manifest: %w", err)
	}

	// For incremental snapshots, only delete new files
	filesToDelete := manifest.Files
	if snapshot.Incremental {
		filesToDelete = manifest.NewFiles
	}

	// Delete files
	for _, f := range filesToDelete {
		objPath := path.Join(prefix, snapshotID, f.Name)
		if err := storage.Delete(objPath); err != nil && !storage.IsNotExistError(err) {
			return fmt.Errorf("failed to delete %s: %w", f.Name, err)
		}
	}

	// Delete manifest
	manifestPath := path.Join(prefix, snapshotID, manifestFileName)
	if err := storage.Delete(manifestPath); err != nil && !storage.IsNotExistError(err) {
		return fmt.Errorf("failed to delete manifest: %w", err)
	}

	// Remove from catalog
	catalog.RemoveSnapshot(snapshotID)
	if err := SaveCatalog(ctx, storage, prefix, catalog); err != nil {
		return fmt.Errorf("failed to save catalog: %w", err)
	}

	return nil
}

// GarbageCollect removes old snapshots based on the GC options.
func GarbageCollect(ctx context.Context, opts GCOptions) ([]SnapshotInfo, error) {
	if opts.Storage == nil {
		return nil, fmt.Errorf("storage is required")
	}

	prefix := opts.Prefix
	if prefix == "" {
		prefix = "snapshots/"
	}

	// Load catalog
	catalog, err := LoadCatalog(ctx, opts.Storage, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to load catalog: %w", err)
	}

	if len(catalog.Snapshots) == 0 {
		return nil, nil
	}

	// Determine which snapshots to keep
	var toDelete []SnapshotInfo
	keptCount := 0
	keptFullCount := 0

	// Snapshots are sorted newest first
	for _, snap := range catalog.Snapshots {
		keep := false

		// Keep if within KeepLast
		if opts.KeepLast > 0 && keptCount < opts.KeepLast {
			keep = true
		}

		// Keep full snapshots up to KeepFullSnapshots
		if !snap.Incremental && opts.KeepFullSnapshots > 0 && keptFullCount < opts.KeepFullSnapshots {
			keep = true
		}

		// Keep if other snapshots depend on this one
		if catalog.HasDependents(snap.ID) {
			keep = true
		}

		if keep {
			keptCount++
			if !snap.Incremental {
				keptFullCount++
			}
		} else {
			toDelete = append(toDelete, snap)
		}
	}

	// Delete snapshots
	if !opts.DryRun {
		for _, snap := range toDelete {
			if err := DeleteSnapshot(ctx, opts.Storage, prefix, snap.ID); err != nil {
				return toDelete, fmt.Errorf("failed to delete snapshot %s: %w", snap.ID, err)
			}
		}
	}

	return toDelete, nil
}
