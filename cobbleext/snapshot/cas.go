// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package snapshot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/objstorage/remote"
)

// =============================================================================
// Content-Addressable Storage (CAS) based Snapshot System
//
// This implementation stores files by their content hash, enabling:
// - Deduplication across snapshots and nodes
// - Reliable incremental snapshots (not dependent on file names)
// - Support for tree-like snapshot histories (branching)
// - Efficient storage when multiple nodes share common data
// =============================================================================

const (
	casBlobPrefix     = "blobs/"
	casSnapshotPrefix = "snapshots/"
	casManifestFile   = "manifest.json"
	casCatalogFile    = "catalog.json"
)

// CASBlobRef represents a reference to a content-addressed blob.
type CASBlobRef struct {
	// Hash is the SHA256 hash of the blob content (hex encoded).
	Hash string `json:"hash"`

	// Size is the size of the blob in bytes.
	Size int64 `json:"size"`

	// OriginalName is the original file name (for restoration).
	OriginalName string `json:"original_name"`

	// Type is the file type (sst, manifest, options, wal, marker, etc.)
	Type string `json:"type,omitempty"`
}

// CASManifest represents a snapshot manifest using content-addressed blobs.
type CASManifest struct {
	// ID is the unique identifier for this snapshot.
	ID string `json:"id"`

	// CreatedAt is when the snapshot was created.
	CreatedAt time.Time `json:"created_at"`

	// NodeID identifies which node created this snapshot (optional).
	NodeID string `json:"node_id,omitempty"`

	// Description is an optional description.
	Description string `json:"description,omitempty"`

	// Labels are optional key-value pairs.
	Labels map[string]string `json:"labels,omitempty"`

	// Blobs is the list of blob references that make up this snapshot.
	Blobs []CASBlobRef `json:"blobs"`

	// TotalSize is the total size of all blobs.
	TotalSize int64 `json:"total_size"`

	// ParentID is the parent snapshot ID (for tracking lineage, not for restore).
	// This is informational only - restore doesn't depend on parent.
	ParentID string `json:"parent_id,omitempty"`

	// BranchName is an optional branch name for organizing snapshots.
	BranchName string `json:"branch_name,omitempty"`
}

// CASCatalog tracks all snapshots and blob references.
type CASCatalog struct {
	// Snapshots is the list of all snapshots, newest first.
	Snapshots []CASManifest `json:"snapshots"`

	// BlobRefCounts tracks how many snapshots reference each blob.
	// This is used for garbage collection.
	BlobRefCounts map[string]int `json:"blob_ref_counts"`

	// UpdatedAt is when the catalog was last updated.
	UpdatedAt time.Time `json:"updated_at"`
}

// CASSnapshotOptions configures CAS snapshot creation.
type CASSnapshotOptions struct {
	// Storage is the remote storage backend.
	Storage remote.Storage

	// Prefix is the base prefix for all objects.
	Prefix string

	// SnapshotID is an optional custom snapshot ID.
	SnapshotID string

	// NodeID identifies this node (for multi-node tracking).
	NodeID string

	// Description is an optional description.
	Description string

	// Labels are optional key-value pairs.
	Labels map[string]string

	// ParentID is the parent snapshot ID (informational).
	ParentID string

	// BranchName is an optional branch name.
	BranchName string

	// Parallelism is the number of parallel uploads.
	Parallelism int

	// ProgressFn is called to report progress.
	ProgressFn func(CASProgress)
}

// CASProgress reports snapshot progress.
type CASProgress struct {
	Phase          string
	FilesTotal     int
	FilesCompleted int
	BytesTotal     int64
	BytesCompleted int64
	BlobsUploaded  int
	BlobsSkipped   int
	CurrentFile    string
}

// CASRestoreOptions configures CAS snapshot restoration.
type CASRestoreOptions struct {
	// Storage is the remote storage backend.
	Storage remote.Storage

	// Prefix is the base prefix for all objects.
	Prefix string

	// SnapshotID is the snapshot to restore.
	SnapshotID string

	// VerifyChecksums enables checksum verification during restore.
	VerifyChecksums bool

	// Parallelism is the number of parallel downloads.
	Parallelism int

	// ProgressFn is called to report progress.
	ProgressFn func(CASProgress)
}

// CASGCOptions configures garbage collection.
type CASGCOptions struct {
	// Storage is the remote storage backend.
	Storage remote.Storage

	// Prefix is the base prefix for all objects.
	Prefix string

	// KeepSnapshots is the list of snapshot IDs to keep.
	// All other snapshots will be deleted.
	KeepSnapshots []string

	// KeepLast keeps the N most recent snapshots.
	KeepLast int

	// DryRun only reports what would be deleted.
	DryRun bool
}

// CreateCASSnapshot creates a snapshot using content-addressable storage.
func CreateCASSnapshot(ctx context.Context, db DBAdapter, opts CASSnapshotOptions) (*CASManifest, error) {
	if opts.Storage == nil {
		return nil, fmt.Errorf("storage is required")
	}

	startTime := time.Now()

	snapshotID := opts.SnapshotID
	if snapshotID == "" {
		snapshotID = time.Now().UTC().Format("20060102T150405.000000000Z")
	}

	prefix := opts.Prefix
	if prefix == "" {
		prefix = "cas/"
	}

	parallelism := opts.Parallelism
	if parallelism <= 0 {
		parallelism = runtime.NumCPU()
	}

	// Report progress: checkpoint
	if opts.ProgressFn != nil {
		opts.ProgressFn(CASProgress{Phase: "checkpoint"})
	}

	// Create temporary directory for checkpoint
	tmpDir, err := os.MkdirTemp("", "cobble-cas-checkpoint-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	checkpointDir := filepath.Join(tmpDir, "checkpoint")

	// Create checkpoint
	if err := db.Checkpoint(checkpointDir); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint: %w", err)
	}

	// Report progress: scanning
	if opts.ProgressFn != nil {
		opts.ProgressFn(CASProgress{Phase: "scan"})
	}

	// Scan and hash all files
	files, err := scanAndHashFiles(checkpointDir)
	if err != nil {
		return nil, fmt.Errorf("failed to scan files: %w", err)
	}

	// Load existing catalog to check for existing blobs
	catalog, err := LoadCASCatalog(ctx, opts.Storage, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to load catalog: %w", err)
	}

	// Determine which blobs need to be uploaded
	var toUpload []fileHashInfo
	var existing []fileHashInfo

	for _, f := range files {
		if catalog.BlobRefCounts[f.hash] > 0 {
			// Blob already exists
			existing = append(existing, f)
		} else {
			// Need to upload
			toUpload = append(toUpload, f)
		}
	}

	// Report progress: uploading
	if opts.ProgressFn != nil {
		var totalSize int64
		for _, f := range toUpload {
			totalSize += f.size
		}
		opts.ProgressFn(CASProgress{
			Phase:        "upload",
			FilesTotal:   len(toUpload),
			BytesTotal:   totalSize,
			BlobsSkipped: len(existing),
		})
	}

	// Upload new blobs
	uploader := &casUploader{
		storage:     opts.Storage,
		prefix:      prefix,
		parallelism: parallelism,
		progressFn:  opts.ProgressFn,
	}

	if err := uploader.uploadBlobs(ctx, checkpointDir, toUpload); err != nil {
		return nil, fmt.Errorf("failed to upload blobs: %w", err)
	}

	// Create blob references
	blobs := make([]CASBlobRef, 0, len(files))
	var totalSize int64
	for _, f := range files {
		blobs = append(blobs, CASBlobRef{
			Hash:         f.hash,
			Size:         f.size,
			OriginalName: f.name,
			Type:         detectFileType(f.name),
		})
		totalSize += f.size
	}

	// Create manifest
	manifest := CASManifest{
		ID:          snapshotID,
		CreatedAt:   time.Now().UTC(),
		NodeID:      opts.NodeID,
		Description: opts.Description,
		Labels:      opts.Labels,
		Blobs:       blobs,
		TotalSize:   totalSize,
		ParentID:    opts.ParentID,
		BranchName:  opts.BranchName,
	}

	// Report progress: finalizing
	if opts.ProgressFn != nil {
		opts.ProgressFn(CASProgress{
			Phase:         "finalize",
			BlobsUploaded: len(toUpload),
			BlobsSkipped:  len(existing),
		})
	}

	// Save manifest
	if err := SaveCASManifest(ctx, opts.Storage, prefix, &manifest); err != nil {
		return nil, fmt.Errorf("failed to save manifest: %w", err)
	}

	// Update catalog
	catalog.AddSnapshot(manifest)
	if err := SaveCASCatalog(ctx, opts.Storage, prefix, catalog); err != nil {
		return nil, fmt.Errorf("failed to save catalog: %w", err)
	}

	_ = startTime // Could log duration

	return &manifest, nil
}

// RestoreCASSnapshot restores a snapshot from content-addressable storage.
func RestoreCASSnapshot(ctx context.Context, destDir string, opts CASRestoreOptions) error {
	if opts.Storage == nil {
		return fmt.Errorf("storage is required")
	}

	prefix := opts.Prefix
	if prefix == "" {
		prefix = "cas/"
	}

	parallelism := opts.Parallelism
	if parallelism <= 0 {
		parallelism = runtime.NumCPU()
	}

	// Report progress: loading manifest
	if opts.ProgressFn != nil {
		opts.ProgressFn(CASProgress{Phase: "load_manifest"})
	}

	// Load manifest
	manifest, err := LoadCASManifest(ctx, opts.Storage, prefix, opts.SnapshotID)
	if err != nil {
		return fmt.Errorf("failed to load manifest: %w", err)
	}

	// Create destination directory
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	// Report progress: downloading
	if opts.ProgressFn != nil {
		opts.ProgressFn(CASProgress{
			Phase:      "download",
			FilesTotal: len(manifest.Blobs),
			BytesTotal: manifest.TotalSize,
		})
	}

	// Download blobs
	downloader := &casDownloader{
		storage:        opts.Storage,
		prefix:         prefix,
		destDir:        destDir,
		parallelism:    parallelism,
		verifyChecksum: opts.VerifyChecksums,
		progressFn:     opts.ProgressFn,
		totalFiles:     len(manifest.Blobs),
		totalSize:      manifest.TotalSize,
	}

	if err := downloader.downloadBlobs(ctx, manifest.Blobs); err != nil {
		return fmt.Errorf("failed to download blobs: %w", err)
	}

	// Report progress: done
	if opts.ProgressFn != nil {
		opts.ProgressFn(CASProgress{
			Phase:          "finalize",
			FilesTotal:     len(manifest.Blobs),
			FilesCompleted: len(manifest.Blobs),
			BytesTotal:     manifest.TotalSize,
			BytesCompleted: manifest.TotalSize,
		})
	}

	return nil
}

// CompactCASOptions configures snapshot compaction.
type CompactCASOptions struct {
	// Storage is the remote storage backend.
	Storage remote.Storage

	// Prefix is the base prefix for all objects.
	Prefix string

	// SnapshotID is the snapshot to compact.
	SnapshotID string

	// NewSnapshotID is the ID for the compacted snapshot (optional).
	// If empty, generates a new ID.
	NewSnapshotID string

	// DeleteOld deletes the old snapshot after compaction.
	DeleteOld bool
}

// CompactCASSnapshot creates a new snapshot that is fully independent,
// with its own blob reference counts. This allows old snapshots in the
// history chain to be deleted through garbage collection.
//
// Use this when you want to:
// 1. Keep a snapshot but allow older snapshots to be garbage collected
// 2. Create a "checkpoint" that doesn't depend on any history
// 3. Reduce storage dependencies before major cleanup
//
// Note: This doesn't copy blob data (blobs are immutable), but it does
// create a new snapshot entry with its own reference counts.
func CompactCASSnapshot(ctx context.Context, opts CompactCASOptions) (*CASManifest, error) {
	if opts.Storage == nil {
		return nil, fmt.Errorf("storage is required")
	}

	prefix := opts.Prefix
	if prefix == "" {
		prefix = "cas/"
	}

	// Load the source manifest
	manifest, err := LoadCASManifest(ctx, opts.Storage, prefix, opts.SnapshotID)
	if err != nil {
		return nil, fmt.Errorf("failed to load manifest: %w", err)
	}

	newID := opts.NewSnapshotID
	if newID == "" {
		newID = time.Now().UTC().Format("20060102T150405.000000000Z")
	}

	// Create new manifest with same blobs but new ID and no parent
	// This makes the snapshot "independent" - it doesn't rely on any
	// historical snapshots, even though it shares blobs with them.
	newManifest := CASManifest{
		ID:          newID,
		CreatedAt:   time.Now().UTC(),
		NodeID:      manifest.NodeID,
		Description: fmt.Sprintf("Compacted from %s: %s", opts.SnapshotID, manifest.Description),
		Labels:      manifest.Labels,
		Blobs:       manifest.Blobs, // Same blobs (content-addressed, immutable)
		TotalSize:   manifest.TotalSize,
		ParentID:    "", // No parent - fully independent
		BranchName:  manifest.BranchName,
	}

	// Save the new manifest
	if err := SaveCASManifest(ctx, opts.Storage, prefix, &newManifest); err != nil {
		return nil, fmt.Errorf("failed to save compacted manifest: %w", err)
	}

	// Update catalog: add new snapshot with its own blob references
	catalog, err := LoadCASCatalog(ctx, opts.Storage, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to load catalog: %w", err)
	}

	catalog.AddSnapshot(newManifest)

	if err := SaveCASCatalog(ctx, opts.Storage, prefix, catalog); err != nil {
		return nil, fmt.Errorf("failed to save catalog: %w", err)
	}

	// Optionally delete the old snapshot
	if opts.DeleteOld {
		if err := DeleteCASSnapshot(ctx, opts.Storage, prefix, opts.SnapshotID); err != nil {
			return nil, fmt.Errorf("failed to delete old snapshot: %w", err)
		}
	}

	return &newManifest, nil
}

// DeleteCASSnapshot deletes a snapshot and decrements its blob reference counts.
// Blobs that are no longer referenced by any snapshot will be deleted.
func DeleteCASSnapshot(ctx context.Context, storage remote.Storage, prefix, snapshotID string) error {
	if prefix == "" {
		prefix = "cas/"
	}

	// Load manifest to get blob list
	manifest, err := LoadCASManifest(ctx, storage, prefix, snapshotID)
	if err != nil {
		return fmt.Errorf("failed to load manifest: %w", err)
	}

	// Load catalog
	catalog, err := LoadCASCatalog(ctx, storage, prefix)
	if err != nil {
		return fmt.Errorf("failed to load catalog: %w", err)
	}

	// Decrement blob reference counts and collect blobs to delete
	var blobsToDelete []string
	for _, blob := range manifest.Blobs {
		catalog.BlobRefCounts[blob.Hash]--
		if catalog.BlobRefCounts[blob.Hash] <= 0 {
			blobsToDelete = append(blobsToDelete, blob.Hash)
			delete(catalog.BlobRefCounts, blob.Hash)
		}
	}

	// Remove snapshot from catalog
	var newSnapshots []CASManifest
	for _, s := range catalog.Snapshots {
		if s.ID != snapshotID {
			newSnapshots = append(newSnapshots, s)
		}
	}
	catalog.Snapshots = newSnapshots

	// Delete manifest file
	manifestPath := path.Join(prefix, casSnapshotPrefix, snapshotID, casManifestFile)
	if err := storage.Delete(manifestPath); err != nil && !storage.IsNotExistError(err) {
		return fmt.Errorf("failed to delete manifest: %w", err)
	}

	// Delete unreferenced blobs
	for _, hash := range blobsToDelete {
		blobPath := path.Join(prefix, casBlobPrefix, hash)
		if err := storage.Delete(blobPath); err != nil && !storage.IsNotExistError(err) {
			return fmt.Errorf("failed to delete blob %s: %w", hash, err)
		}
	}

	// Save updated catalog
	if err := SaveCASCatalog(ctx, storage, prefix, catalog); err != nil {
		return fmt.Errorf("failed to save catalog: %w", err)
	}

	return nil
}

// GarbageCollectCAS removes unreferenced blobs and old snapshots.
func GarbageCollectCAS(ctx context.Context, opts CASGCOptions) (deletedSnapshots []string, deletedBlobs []string, err error) {
	if opts.Storage == nil {
		return nil, nil, fmt.Errorf("storage is required")
	}

	prefix := opts.Prefix
	if prefix == "" {
		prefix = "cas/"
	}

	// Load catalog
	catalog, err := LoadCASCatalog(ctx, opts.Storage, prefix)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load catalog: %w", err)
	}

	// Determine which snapshots to keep
	keepSet := make(map[string]bool)
	for _, id := range opts.KeepSnapshots {
		keepSet[id] = true
	}

	// Also keep the most recent N snapshots
	if opts.KeepLast > 0 {
		for i := 0; i < opts.KeepLast && i < len(catalog.Snapshots); i++ {
			keepSet[catalog.Snapshots[i].ID] = true
		}
	}

	// Find snapshots to delete
	var toDelete []CASManifest
	var toKeep []CASManifest
	for _, snap := range catalog.Snapshots {
		if keepSet[snap.ID] {
			toKeep = append(toKeep, snap)
		} else {
			toDelete = append(toDelete, snap)
		}
	}

	// Collect blobs that are still referenced
	referencedBlobs := make(map[string]int)
	for _, snap := range toKeep {
		for _, blob := range snap.Blobs {
			referencedBlobs[blob.Hash]++
		}
	}

	// Find blobs to delete (no longer referenced)
	var blobsToDelete []string
	for hash := range catalog.BlobRefCounts {
		if referencedBlobs[hash] == 0 {
			blobsToDelete = append(blobsToDelete, hash)
		}
	}

	if opts.DryRun {
		for _, snap := range toDelete {
			deletedSnapshots = append(deletedSnapshots, snap.ID)
		}
		deletedBlobs = blobsToDelete
		return deletedSnapshots, deletedBlobs, nil
	}

	// Delete snapshots
	for _, snap := range toDelete {
		manifestPath := path.Join(prefix, casSnapshotPrefix, snap.ID, casManifestFile)
		if err := opts.Storage.Delete(manifestPath); err != nil && !opts.Storage.IsNotExistError(err) {
			return deletedSnapshots, deletedBlobs, fmt.Errorf("failed to delete manifest %s: %w", snap.ID, err)
		}
		deletedSnapshots = append(deletedSnapshots, snap.ID)
	}

	// Delete unreferenced blobs
	for _, hash := range blobsToDelete {
		blobPath := path.Join(prefix, casBlobPrefix, hash)
		if err := opts.Storage.Delete(blobPath); err != nil && !opts.Storage.IsNotExistError(err) {
			return deletedSnapshots, deletedBlobs, fmt.Errorf("failed to delete blob %s: %w", hash, err)
		}
		deletedBlobs = append(deletedBlobs, hash)
	}

	// Update catalog
	catalog.Snapshots = toKeep
	catalog.BlobRefCounts = referencedBlobs
	if err := SaveCASCatalog(ctx, opts.Storage, prefix, catalog); err != nil {
		return deletedSnapshots, deletedBlobs, fmt.Errorf("failed to save catalog: %w", err)
	}

	return deletedSnapshots, deletedBlobs, nil
}

// =============================================================================
// Catalog and Manifest Operations
// =============================================================================

// LoadCASCatalog loads the CAS catalog from storage.
func LoadCASCatalog(ctx context.Context, storage remote.Storage, prefix string) (*CASCatalog, error) {
	catalogPath := path.Join(prefix, casCatalogFile)

	size, err := storage.Size(catalogPath)
	if err != nil {
		if storage.IsNotExistError(err) {
			return &CASCatalog{
				Snapshots:     []CASManifest{},
				BlobRefCounts: make(map[string]int),
				UpdatedAt:     time.Now(),
			}, nil
		}
		return nil, fmt.Errorf("failed to get catalog size: %w", err)
	}

	reader, objSize, err := storage.ReadObject(ctx, catalogPath)
	if err != nil {
		if storage.IsNotExistError(err) {
			return &CASCatalog{
				Snapshots:     []CASManifest{},
				BlobRefCounts: make(map[string]int),
				UpdatedAt:     time.Now(),
			}, nil
		}
		return nil, fmt.Errorf("failed to read catalog: %w", err)
	}
	defer reader.Close()

	if objSize > 0 {
		size = objSize
	}

	data := make([]byte, size)
	if err := reader.ReadAt(ctx, data, 0); err != nil {
		return nil, fmt.Errorf("failed to read catalog data: %w", err)
	}

	var catalog CASCatalog
	if err := json.Unmarshal(data, &catalog); err != nil {
		return nil, fmt.Errorf("failed to parse catalog: %w", err)
	}

	if catalog.BlobRefCounts == nil {
		catalog.BlobRefCounts = make(map[string]int)
	}

	return &catalog, nil
}

// SaveCASCatalog saves the CAS catalog to storage.
func SaveCASCatalog(ctx context.Context, storage remote.Storage, prefix string, catalog *CASCatalog) error {
	catalog.UpdatedAt = time.Now()

	data, err := json.MarshalIndent(catalog, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal catalog: %w", err)
	}

	catalogPath := path.Join(prefix, casCatalogFile)
	writer, err := storage.CreateObject(catalogPath)
	if err != nil {
		return fmt.Errorf("failed to create catalog: %w", err)
	}

	if _, err := io.Copy(writer, bytes.NewReader(data)); err != nil {
		writer.Close()
		return fmt.Errorf("failed to write catalog: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close catalog writer: %w", err)
	}

	return nil
}

// AddSnapshot adds a snapshot to the catalog.
func (c *CASCatalog) AddSnapshot(manifest CASManifest) {
	// Add to front (newest first)
	c.Snapshots = append([]CASManifest{manifest}, c.Snapshots...)

	// Update blob reference counts
	for _, blob := range manifest.Blobs {
		c.BlobRefCounts[blob.Hash]++
	}
}

// GetSnapshot returns a snapshot by ID.
func (c *CASCatalog) GetSnapshot(snapshotID string) (*CASManifest, bool) {
	for i := range c.Snapshots {
		if c.Snapshots[i].ID == snapshotID {
			return &c.Snapshots[i], true
		}
	}
	return nil, false
}

// LoadCASManifest loads a snapshot manifest.
func LoadCASManifest(ctx context.Context, storage remote.Storage, prefix, snapshotID string) (*CASManifest, error) {
	manifestPath := path.Join(prefix, casSnapshotPrefix, snapshotID, casManifestFile)

	size, err := storage.Size(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get manifest size: %w", err)
	}

	reader, objSize, err := storage.ReadObject(ctx, manifestPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest: %w", err)
	}
	defer reader.Close()

	if objSize > 0 {
		size = objSize
	}

	data := make([]byte, size)
	if err := reader.ReadAt(ctx, data, 0); err != nil {
		return nil, fmt.Errorf("failed to read manifest data: %w", err)
	}

	var manifest CASManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("failed to parse manifest: %w", err)
	}

	return &manifest, nil
}

// SaveCASManifest saves a snapshot manifest.
func SaveCASManifest(ctx context.Context, storage remote.Storage, prefix string, manifest *CASManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	manifestPath := path.Join(prefix, casSnapshotPrefix, manifest.ID, casManifestFile)
	writer, err := storage.CreateObject(manifestPath)
	if err != nil {
		return fmt.Errorf("failed to create manifest: %w", err)
	}

	if _, err := io.Copy(writer, bytes.NewReader(data)); err != nil {
		writer.Close()
		return fmt.Errorf("failed to write manifest: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close manifest writer: %w", err)
	}

	return nil
}

// =============================================================================
// Internal Helpers
// =============================================================================

type fileHashInfo struct {
	name string
	hash string
	size int64
}

// scanAndHashFiles scans a directory and computes SHA256 hashes for all files.
func scanAndHashFiles(dir string) ([]fileHashInfo, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	var files []fileHashInfo
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filePath := filepath.Join(dir, entry.Name())
		hash, size, err := hashFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to hash %s: %w", entry.Name(), err)
		}

		files = append(files, fileHashInfo{
			name: entry.Name(),
			hash: hash,
			size: size,
		})
	}

	return files, nil
}

// hashFile computes the SHA256 hash of a file.
func hashFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return "", 0, err
	}

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", 0, err
	}

	return hex.EncodeToString(h.Sum(nil)), stat.Size(), nil
}

// casUploader handles parallel blob uploads.
type casUploader struct {
	storage     remote.Storage
	prefix      string
	parallelism int
	progressFn  func(CASProgress)
}

func (u *casUploader) uploadBlobs(ctx context.Context, srcDir string, files []fileHashInfo) error {
	if len(files) == 0 {
		return nil
	}

	// Calculate total size
	var totalSize int64
	for _, f := range files {
		totalSize += f.size
	}

	// Create work channel
	workCh := make(chan fileHashInfo, len(files))
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
	for i := 0; i < u.parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range workCh {
				select {
				case <-ctx.Done():
					mu.Lock()
					errs = append(errs, ctx.Err())
					mu.Unlock()
					continue
				default:
				}

				if err := u.uploadBlob(ctx, srcDir, f); err != nil {
					mu.Lock()
					errs = append(errs, fmt.Errorf("upload %s: %w", f.name, err))
					mu.Unlock()
					continue
				}

				filesCompleted.Add(1)
				bytesCompleted.Add(f.size)

				if u.progressFn != nil {
					u.progressFn(CASProgress{
						Phase:          "upload",
						FilesTotal:     len(files),
						FilesCompleted: int(filesCompleted.Load()),
						BytesTotal:     totalSize,
						BytesCompleted: bytesCompleted.Load(),
						CurrentFile:    f.name,
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

func (u *casUploader) uploadBlob(ctx context.Context, srcDir string, f fileHashInfo) error {
	srcPath := filepath.Join(srcDir, f.name)
	destPath := path.Join(u.prefix, casBlobPrefix, f.hash)

	// Check if blob already exists (double-check)
	if _, err := u.storage.Size(destPath); err == nil {
		// Blob already exists, skip
		return nil
	}

	// Open source file
	file, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("failed to open: %w", err)
	}
	defer file.Close()

	// Create remote object
	writer, err := u.storage.CreateObject(destPath)
	if err != nil {
		return fmt.Errorf("failed to create object: %w", err)
	}

	// Copy content
	if _, err := io.Copy(writer, file); err != nil {
		writer.Close()
		return fmt.Errorf("failed to copy: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	return nil
}

// casDownloader handles parallel blob downloads.
type casDownloader struct {
	storage        remote.Storage
	prefix         string
	destDir        string
	parallelism    int
	verifyChecksum bool
	progressFn     func(CASProgress)
	totalFiles     int
	totalSize      int64
}

func (d *casDownloader) downloadBlobs(ctx context.Context, blobs []CASBlobRef) error {
	if len(blobs) == 0 {
		return nil
	}

	// Create work channel
	workCh := make(chan CASBlobRef, len(blobs))
	for _, b := range blobs {
		workCh <- b
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
			for blob := range workCh {
				select {
				case <-ctx.Done():
					mu.Lock()
					errs = append(errs, ctx.Err())
					mu.Unlock()
					continue
				default:
				}

				if err := d.downloadBlob(ctx, blob); err != nil {
					mu.Lock()
					errs = append(errs, fmt.Errorf("download %s: %w", blob.OriginalName, err))
					mu.Unlock()
					continue
				}

				filesCompleted.Add(1)
				bytesCompleted.Add(blob.Size)

				if d.progressFn != nil {
					d.progressFn(CASProgress{
						Phase:          "download",
						FilesTotal:     d.totalFiles,
						FilesCompleted: int(filesCompleted.Load()),
						BytesTotal:     d.totalSize,
						BytesCompleted: bytesCompleted.Load(),
						CurrentFile:    blob.OriginalName,
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

func (d *casDownloader) downloadBlob(ctx context.Context, blob CASBlobRef) error {
	srcPath := path.Join(d.prefix, casBlobPrefix, blob.Hash)
	destPath := filepath.Join(d.destDir, blob.OriginalName)

	// Get size
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

	// Read data
	data := make([]byte, size)
	if err := reader.ReadAt(ctx, data, 0); err != nil {
		return fmt.Errorf("failed to read: %w", err)
	}

	// Verify checksum
	if d.verifyChecksum {
		hash := sha256.Sum256(data)
		actualHash := hex.EncodeToString(hash[:])
		if actualHash != blob.Hash {
			return fmt.Errorf("checksum mismatch: expected %s, got %s", blob.Hash, actualHash)
		}
	}

	// Write to destination
	if err := os.WriteFile(destPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write: %w", err)
	}

	return nil
}
