// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package snapshot

import (
	"runtime"

	"github.com/cockroachdb/pebble/objstorage/remote"
)

// SnapshotOptions configures snapshot behavior.
type SnapshotOptions struct {
	// Storage is the remote storage to use for the snapshot.
	Storage remote.Storage

	// Prefix is the path prefix in the bucket for snapshots.
	// Default is "snapshots/".
	Prefix string

	// Incremental enables incremental snapshots.
	// If true and a previous snapshot exists, only changed files are uploaded.
	Incremental bool

	// ParentSnapshotID specifies the parent for incremental snapshots.
	// If empty and Incremental is true, uses the latest snapshot.
	ParentSnapshotID string

	// Parallelism is the number of concurrent uploads.
	// Default is runtime.NumCPU().
	Parallelism int

	// ProgressFn is called periodically with progress updates.
	ProgressFn func(SnapshotProgress)

	// FlushWAL ensures all committed writes are included in the snapshot.
	FlushWAL bool

	// SnapshotID allows specifying a custom snapshot ID.
	// If empty, a timestamp-based ID is generated.
	SnapshotID string
}

// DefaultSnapshotOptions returns default snapshot options.
func DefaultSnapshotOptions() SnapshotOptions {
	return SnapshotOptions{
		Prefix:      "snapshots/",
		Parallelism: runtime.NumCPU(),
		FlushWAL:    true,
	}
}

// RestoreOptions configures restore behavior.
type RestoreOptions struct {
	// Storage is the remote storage to restore from.
	Storage remote.Storage

	// Prefix is the path prefix in the bucket for snapshots.
	// Default is "snapshots/".
	Prefix string

	// SnapshotID is the snapshot to restore.
	// If empty, restores the latest snapshot.
	SnapshotID string

	// Parallelism is the number of concurrent downloads.
	// Default is runtime.NumCPU().
	Parallelism int

	// ProgressFn is called periodically with progress updates.
	ProgressFn func(RestoreProgress)

	// VerifyChecksums verifies file checksums after download.
	// Default is true.
	VerifyChecksums bool
}

// DefaultRestoreOptions returns default restore options.
func DefaultRestoreOptions() RestoreOptions {
	return RestoreOptions{
		Prefix:          "snapshots/",
		Parallelism:     runtime.NumCPU(),
		VerifyChecksums: true,
	}
}

// GCOptions configures garbage collection of old snapshots.
type GCOptions struct {
	// Storage is the remote storage.
	Storage remote.Storage

	// Prefix is the path prefix in the bucket for snapshots.
	Prefix string

	// KeepLast keeps the N most recent snapshots.
	// If 0, this constraint is not applied.
	KeepLast int

	// KeepFullSnapshots keeps at least N full (non-incremental) snapshots.
	// This ensures incremental chains can be resolved.
	KeepFullSnapshots int

	// DryRun only reports what would be deleted without actually deleting.
	DryRun bool
}
