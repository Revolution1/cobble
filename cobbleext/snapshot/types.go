// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package snapshot

import (
	"time"
)

// SnapshotInfo contains metadata about a snapshot.
type SnapshotInfo struct {
	// ID is the unique identifier for this snapshot.
	ID string `json:"id"`

	// CreatedAt is when the snapshot was created.
	CreatedAt time.Time `json:"created_at"`

	// FormatVersion is the Pebble format version.
	FormatVersion uint64 `json:"format_version"`

	// VisibleSeqNum is the sequence number visible in this snapshot.
	VisibleSeqNum uint64 `json:"visible_seq_num"`

	// TotalSize is the total size of all files in bytes.
	TotalSize int64 `json:"total_size"`

	// FileCount is the number of files in the snapshot.
	FileCount int `json:"file_count"`

	// Incremental indicates if this is an incremental snapshot.
	Incremental bool `json:"incremental"`

	// BaseSnapshotID is the base snapshot for incremental chains.
	// For full snapshots, this is empty.
	BaseSnapshotID string `json:"base_snapshot_id,omitempty"`

	// ParentSnapshotID is the immediate parent for incremental snapshots.
	ParentSnapshotID string `json:"parent_snapshot_id,omitempty"`
}

// SnapshotManifest contains the full metadata for a snapshot.
type SnapshotManifest struct {
	SnapshotInfo

	// Files lists all files in this snapshot.
	Files []FileInfo `json:"files"`

	// NewFiles lists files added in this incremental snapshot.
	// Only populated for incremental snapshots.
	NewFiles []FileInfo `json:"new_files,omitempty"`

	// DeletedFiles lists files removed since the parent snapshot.
	// Only populated for incremental snapshots.
	DeletedFiles []string `json:"deleted_files,omitempty"`

	// InheritedFiles lists files inherited from the parent snapshot.
	// Only populated for incremental snapshots.
	InheritedFiles []string `json:"inherited_files,omitempty"`
}

// FileInfo contains metadata about a single file.
type FileInfo struct {
	// Name is the file name (e.g., "000001.sst").
	Name string `json:"name"`

	// Size is the file size in bytes.
	Size int64 `json:"size"`

	// Checksum is the SHA-256 checksum of the file.
	Checksum string `json:"checksum"`

	// Type is the file type: "sst", "wal", "manifest", "options", "marker".
	Type string `json:"type"`

	// RemotePath is set if the file is already in remote storage.
	// This avoids re-uploading files that are part of tiered storage.
	RemotePath string `json:"remote_path,omitempty"`
}

// SnapshotProgress reports progress during snapshot operations.
type SnapshotProgress struct {
	// Phase is the current phase: "checkpoint", "scan", "upload", "finalize".
	Phase string

	// FilesTotal is the total number of files to process.
	FilesTotal int

	// FilesCompleted is the number of files completed.
	FilesCompleted int

	// BytesTotal is the total bytes to transfer.
	BytesTotal int64

	// BytesCompleted is the bytes transferred so far.
	BytesCompleted int64

	// CurrentFile is the file currently being processed.
	CurrentFile string

	// Error is set if an error occurred during this file.
	Error error
}

// RestoreProgress reports progress during restore operations.
type RestoreProgress struct {
	// Phase is the current phase: "fetch_manifest", "download", "verify", "finalize".
	Phase string

	// FilesTotal is the total number of files to download.
	FilesTotal int

	// FilesCompleted is the number of files downloaded.
	FilesCompleted int

	// BytesTotal is the total bytes to download.
	BytesTotal int64

	// BytesCompleted is the bytes downloaded so far.
	BytesCompleted int64

	// CurrentFile is the file currently being downloaded.
	CurrentFile string

	// Error is set if an error occurred during this file.
	Error error
}

// SnapshotResult contains the result of a snapshot operation.
type SnapshotResult struct {
	// SnapshotID is the unique identifier for this snapshot.
	SnapshotID string

	// CreatedAt is when the snapshot was created.
	CreatedAt time.Time

	// TotalSize is the total size of all files in the snapshot.
	TotalSize int64

	// FileCount is the total number of files in the snapshot.
	FileCount int

	// UploadedSize is the size of files actually uploaded.
	// For incremental snapshots, this is less than TotalSize.
	UploadedSize int64

	// UploadedCount is the number of files actually uploaded.
	UploadedCount int

	// Duration is how long the snapshot operation took.
	Duration time.Duration

	// Incremental indicates if this was an incremental snapshot.
	Incremental bool

	// ParentSnapshotID is the parent snapshot for incremental snapshots.
	ParentSnapshotID string
}

// Catalog tracks all snapshots in a storage location.
type Catalog struct {
	// Snapshots is the list of all snapshots, newest first.
	Snapshots []SnapshotInfo `json:"snapshots"`

	// UpdatedAt is when the catalog was last updated.
	UpdatedAt time.Time `json:"updated_at"`
}
