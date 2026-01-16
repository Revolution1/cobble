# Snapshot to Object Storage Design

## Overview

This document describes the design for snapshotting Pebble databases to object storage (S3/GCS), enabling:
- Full database backups to cloud storage
- Incremental backups (only changed files)
- Point-in-time restore from any snapshot
- Integration with existing tiered storage

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        User API                                 │
│  db.SnapshotToCloud(ctx, dest, opts...)                        │
│  cobbleext.RestoreFromCloud(ctx, src, localDir, opts...)       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   SnapshotManager                               │
│  - Coordinates checkpoint creation                              │
│  - Manages upload/download                                      │
│  - Tracks snapshot metadata                                     │
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ Local Checkpoint│  │ Cloud Uploader  │  │ Snapshot Catalog│
│ (existing)      │  │ (parallel)      │  │ (metadata)      │
└─────────────────┘  └─────────────────┘  └─────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              remote.Storage (S3/GCS)                            │
│  - cobbleext/s3 or cobbleext/gcs implementation                 │
└─────────────────────────────────────────────────────────────────┘
```

## Snapshot Structure in Object Storage

```
s3://bucket/prefix/snapshots/
├── catalog.json                    # List of all snapshots
├── {snapshot-id}/
│   ├── manifest.json               # Snapshot metadata
│   ├── MANIFEST-000001             # Pebble manifest
│   ├── OPTIONS-000001              # Pebble options
│   ├── marker.format-version.000002.XXXXXXXX
│   ├── 000001.sst                  # SST files
│   ├── 000002.sst
│   ├── ...
│   ├── 000001.log                  # WAL files
│   └── 000002.log
└── {snapshot-id-2}/
    └── ...
```

## Snapshot Metadata (manifest.json)

```json
{
  "id": "20240115-120000-abc123",
  "created_at": "2024-01-15T12:00:00Z",
  "format_version": 18,
  "visible_seq_num": 12345678,
  "parent_snapshot_id": null,
  "files": [
    {
      "name": "000001.sst",
      "size": 67108864,
      "checksum": "sha256:abc123...",
      "type": "sst"
    },
    {
      "name": "000001.log",
      "size": 1048576,
      "checksum": "sha256:def456...",
      "type": "wal"
    }
  ],
  "total_size": 1073741824,
  "file_count": 42,
  "incremental": false,
  "base_snapshot_id": null
}
```

## Incremental Snapshots

For incremental snapshots, we only upload files that have changed since the parent snapshot:

```json
{
  "id": "20240116-120000-def456",
  "created_at": "2024-01-16T12:00:00Z",
  "incremental": true,
  "base_snapshot_id": "20240115-120000-abc123",
  "parent_snapshot_id": "20240115-120000-abc123",
  "new_files": [
    {"name": "000005.sst", "size": 67108864, "checksum": "sha256:..."}
  ],
  "deleted_files": ["000001.sst"],
  "inherited_files": ["000002.sst", "000003.sst", "000004.sst"]
}
```

## API Design

### Snapshot Creation

```go
// SnapshotOptions configures snapshot behavior.
type SnapshotOptions struct {
    // Storage is the remote storage to use.
    Storage remote.Storage

    // Prefix is the path prefix in the bucket.
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

    // FlushWAL ensures all committed writes are included.
    FlushWAL bool
}

type SnapshotProgress struct {
    Phase           string  // "checkpoint", "upload", "finalize"
    FilesTotal      int
    FilesCompleted  int
    BytesTotal      int64
    BytesCompleted  int64
    CurrentFile     string
}

type SnapshotResult struct {
    SnapshotID      string
    CreatedAt       time.Time
    TotalSize       int64
    FileCount       int
    UploadedSize    int64  // For incremental, only new files
    UploadedCount   int
    Duration        time.Duration
}

// SnapshotToCloud creates a snapshot of the database to cloud storage.
func (d *DB) SnapshotToCloud(ctx context.Context, opts SnapshotOptions) (*SnapshotResult, error)
```

### Snapshot Restoration

```go
// RestoreOptions configures restore behavior.
type RestoreOptions struct {
    // Storage is the remote storage to use.
    Storage remote.Storage

    // Prefix is the path prefix in the bucket.
    Prefix string

    // SnapshotID is the snapshot to restore.
    // If empty, restores the latest snapshot.
    SnapshotID string

    // Parallelism is the number of concurrent downloads.
    Parallelism int

    // ProgressFn is called periodically with progress updates.
    ProgressFn func(RestoreProgress)
}

type RestoreProgress struct {
    Phase           string  // "download", "verify", "finalize"
    FilesTotal      int
    FilesCompleted  int
    BytesTotal      int64
    BytesCompleted  int64
    CurrentFile     string
}

// RestoreFromCloud restores a database from a cloud snapshot.
// The localDir must be empty or not exist.
func RestoreFromCloud(ctx context.Context, localDir string, opts RestoreOptions) error
```

### Snapshot Management

```go
// ListSnapshots returns all snapshots in the cloud storage.
func ListSnapshots(ctx context.Context, storage remote.Storage, prefix string) ([]SnapshotInfo, error)

// DeleteSnapshot deletes a snapshot from cloud storage.
// If the snapshot has dependents (incremental children), returns an error.
func DeleteSnapshot(ctx context.Context, storage remote.Storage, prefix, snapshotID string) error

// GetSnapshotInfo returns information about a specific snapshot.
func GetSnapshotInfo(ctx context.Context, storage remote.Storage, prefix, snapshotID string) (*SnapshotInfo, error)
```

## Implementation Details

### 1. Checkpoint Phase

```go
func (d *DB) SnapshotToCloud(ctx context.Context, opts SnapshotOptions) (*SnapshotResult, error) {
    // 1. Create local checkpoint to temp directory
    tmpDir, err := os.MkdirTemp("", "pebble-snapshot-*")
    if err != nil {
        return nil, err
    }
    defer os.RemoveAll(tmpDir)

    checkpointOpts := []CheckpointOption{}
    if opts.FlushWAL {
        checkpointOpts = append(checkpointOpts, WithFlushedWAL())
    }

    if err := d.Checkpoint(tmpDir, checkpointOpts...); err != nil {
        return nil, err
    }

    // 2. Scan checkpoint directory for files
    files, err := scanCheckpointFiles(tmpDir)
    if err != nil {
        return nil, err
    }

    // 3. For incremental, determine which files are new
    if opts.Incremental {
        files, err = filterNewFiles(ctx, opts.Storage, opts.Prefix, opts.ParentSnapshotID, files)
        if err != nil {
            return nil, err
        }
    }

    // 4. Upload files in parallel
    if err := uploadFiles(ctx, opts, tmpDir, files); err != nil {
        return nil, err
    }

    // 5. Write manifest and update catalog
    return finalizeSnapshot(ctx, opts, files)
}
```

### 2. Parallel Upload with Progress

```go
func uploadFiles(ctx context.Context, opts SnapshotOptions, srcDir string, files []FileInfo) error {
    parallelism := opts.Parallelism
    if parallelism <= 0 {
        parallelism = runtime.NumCPU()
    }

    sem := make(chan struct{}, parallelism)
    errCh := make(chan error, len(files))

    var wg sync.WaitGroup
    var completed atomic.Int64
    var bytesCompleted atomic.Int64

    for _, f := range files {
        wg.Add(1)
        go func(f FileInfo) {
            defer wg.Done()

            select {
            case sem <- struct{}{}:
                defer func() { <-sem }()
            case <-ctx.Done():
                errCh <- ctx.Err()
                return
            }

            if err := uploadFile(ctx, opts.Storage, srcDir, f); err != nil {
                errCh <- err
                return
            }

            completed.Add(1)
            bytesCompleted.Add(f.Size)

            if opts.ProgressFn != nil {
                opts.ProgressFn(SnapshotProgress{
                    Phase:          "upload",
                    FilesTotal:     len(files),
                    FilesCompleted: int(completed.Load()),
                    BytesTotal:     totalSize(files),
                    BytesCompleted: bytesCompleted.Load(),
                    CurrentFile:    f.Name,
                })
            }
        }(f)
    }

    wg.Wait()
    close(errCh)

    for err := range errCh {
        if err != nil {
            return err
        }
    }
    return nil
}
```

### 3. Restore Implementation

```go
func RestoreFromCloud(ctx context.Context, localDir string, opts RestoreOptions) error {
    // 1. Get snapshot manifest
    snapshotID := opts.SnapshotID
    if snapshotID == "" {
        latest, err := getLatestSnapshot(ctx, opts.Storage, opts.Prefix)
        if err != nil {
            return err
        }
        snapshotID = latest.ID
    }

    manifest, err := getSnapshotManifest(ctx, opts.Storage, opts.Prefix, snapshotID)
    if err != nil {
        return err
    }

    // 2. For incremental, resolve full file list
    files := manifest.Files
    if manifest.Incremental {
        files, err = resolveIncrementalFiles(ctx, opts.Storage, opts.Prefix, manifest)
        if err != nil {
            return err
        }
    }

    // 3. Create local directory
    if err := os.MkdirAll(localDir, 0755); err != nil {
        return err
    }

    // 4. Download files in parallel
    if err := downloadFiles(ctx, opts, localDir, files); err != nil {
        return err
    }

    // 5. Verify checksums
    if err := verifyChecksums(localDir, files); err != nil {
        return err
    }

    return nil
}
```

## Integration with Tiered Storage

When tiered storage is enabled, some SST files may already be in remote storage. The snapshot system can leverage this:

```go
func (d *DB) SnapshotToCloud(ctx context.Context, opts SnapshotOptions) (*SnapshotResult, error) {
    // ... checkpoint creation ...

    for _, f := range files {
        // Check if file is already in remote storage
        if meta, err := d.objProvider.Lookup(f.Type, f.FileNum); err == nil && meta.IsRemote() {
            // File already in cloud - just record reference
            files[i].RemoteRef = meta.Remote.Path
            files[i].NeedUpload = false
        } else {
            files[i].NeedUpload = true
        }
    }

    // Only upload files not already in cloud
    // ...
}
```

## Environment Variable Configuration

```bash
# Snapshot destination
COBBLE_SNAPSHOT_BUCKET=my-backup-bucket
COBBLE_SNAPSHOT_PREFIX=pebble-snapshots/
COBBLE_SNAPSHOT_REGION=us-east-1

# Or use the same config as tiered storage
COBBLE_SNAPSHOT_USE_TIERED_CONFIG=true
```

## Error Handling and Resumability

For large databases, upload failures should be resumable:

```go
type SnapshotState struct {
    SnapshotID      string
    Phase           string
    CompletedFiles  map[string]bool
    TempDir         string
}

// ResumeSnapshot continues a previously failed snapshot.
func ResumeSnapshot(ctx context.Context, state SnapshotState, opts SnapshotOptions) (*SnapshotResult, error)
```

## Garbage Collection

Old snapshots should be cleaned up to avoid storage costs:

```go
type GCOptions struct {
    // KeepLast keeps the N most recent snapshots.
    KeepLast int

    // KeepDuration keeps snapshots newer than this duration.
    KeepDuration time.Duration

    // DryRun only reports what would be deleted.
    DryRun bool
}

// GCSnapshots removes old snapshots based on retention policy.
func GCSnapshots(ctx context.Context, storage remote.Storage, prefix string, opts GCOptions) ([]string, error)
```

## File Structure

```
cobbleext/
├── snapshot/
│   ├── snapshot.go         # Main snapshot logic
│   ├── restore.go          # Restore logic
│   ├── catalog.go          # Snapshot catalog management
│   ├── upload.go           # Parallel upload
│   ├── download.go         # Parallel download
│   ├── incremental.go      # Incremental snapshot logic
│   ├── gc.go               # Garbage collection
│   └── snapshot_test.go    # Tests
```

## Testing Strategy

1. **Unit tests**: Mock remote.Storage for fast tests
2. **Integration tests**: Use MinIO for S3-compatible testing
3. **Large file tests**: Test with multi-GB databases
4. **Failure tests**: Test resumability after network failures
5. **Incremental tests**: Test incremental snapshot chains
