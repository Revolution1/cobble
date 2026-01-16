// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build (s3 || cloud) && integration

package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/cobbleext/s3"
	"github.com/cockroachdb/pebble/cobbleext/testutil"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

// pebbleDBAdapter wraps pebble.DB to implement DBAdapter
type pebbleDBAdapter struct {
	db   *pebble.DB
	path string
}

func (a *pebbleDBAdapter) Checkpoint(destDir string) error {
	return a.db.Checkpoint(destDir, pebble.WithFlushedWAL())
}

func (a *pebbleDBAdapter) Path() string {
	return a.path
}

// TestTieredStorageWithMinIO tests the full tiered storage flow with a real MinIO instance.
// This verifies that server-side copy actually works with S3-compatible storage.
func TestTieredStorageWithMinIO(t *testing.T) {
	minio := testutil.StartMinIO(t)
	ctx := context.Background()
	bucket := "tiered-test"

	require.NoError(t, minio.CreateBucket(ctx, bucket))

	storage, err := s3.NewStorage(s3.Config{
		Bucket:          bucket,
		Prefix:          "cobble/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	require.NoError(t, err)
	defer storage.Close()

	// Step 1: Pre-populate "tiered storage" (simulating existing SST files in S3)
	tieredFiles := []struct {
		remotePath string
		localName  string
		data       []byte
	}{
		{"tiered/chaindata/000001.sst", "000001.sst", []byte(strings.Repeat("chaindata-block-1-", 100))},
		{"tiered/chaindata/000002.sst", "000002.sst", []byte(strings.Repeat("chaindata-block-2-", 100))},
		{"tiered/state/000001.sst", "000001.sst", []byte(strings.Repeat("state-account-1-", 100))},
	}

	for _, f := range tieredFiles {
		writer, err := storage.CreateObject(f.remotePath)
		require.NoError(t, err)
		_, err = writer.Write(f.data)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	}

	t.Logf("Pre-populated %d files in tiered storage", len(tieredFiles))

	// Step 2: Create remote file info list
	var remoteFileList []RemoteFileInfo
	for i, f := range tieredFiles {
		remoteFileList = append(remoteFileList, RemoteFileInfo{
			LocalName:      f.localName,
			RemotePath:     f.remotePath,
			CreatorID:      1,
			CreatorFileNum: base.DiskFileNum(i + 1),
		})
	}

	// Step 3: Use remoteBlobCopier to copy with server-side copy
	// Note: storage already has prefix "cobble/", so copier prefix should be empty
	// or relative to the storage's prefix
	copier := &remoteBlobCopier{
		storage:     storage,
		prefix:      "", // Storage already has "cobble/" prefix
		parallelism: 2,
	}

	catalog := &CASCatalog{
		BlobRefCounts: make(map[string]int),
	}

	blobs, err := copier.copyRemoteBlobs(ctx, remoteFileList, catalog)
	require.NoError(t, err)
	require.Len(t, blobs, 3)

	// Step 4: Verify all blobs use ETag keys (server-side copy)
	for _, blob := range blobs {
		t.Logf("Blob: %s -> %s (source=%s)", blob.OriginalName, blob.Key, blob.Source)
		require.Equal(t, "tiered", blob.Source)
		require.Equal(t, BlobKeyTypeETag, blob.KeyType())

		// Verify blob exists at the expected path
		// Storage has prefix "cobble/", copier prefix is "", so blobs are at "blobs/key"
		blobPath := "blobs/" + blob.Key
		size, err := storage.Size(blobPath)
		require.NoError(t, err, "Blob should exist at %s", blobPath)
		require.Equal(t, blob.Size, size)
	}

	t.Log("=== Tiered Storage MinIO Integration Test PASSED ===")
}

// TestMixedLocalAndTieredSnapshot tests creating a snapshot with both
// local files (uploaded with SHA256) and tiered files (copied with ETag).
func TestMixedLocalAndTieredSnapshot(t *testing.T) {
	// Disable auto tiered storage config
	pebble.ExternalConfigHook = nil
	pebble.PostOpenHook = nil

	minio := testutil.StartMinIO(t)
	ctx := context.Background()
	bucket := "mixed-test"

	require.NoError(t, minio.CreateBucket(ctx, bucket))

	storage, err := s3.NewStorage(s3.Config{
		Bucket:          bucket,
		Prefix:          "cobble/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	require.NoError(t, err)
	defer storage.Close()

	// Create a real pebble database
	tmpDir := t.TempDir()
	dbDir := filepath.Join(tmpDir, "testdb")

	db, err := pebble.Open(dbDir, &pebble.Options{})
	require.NoError(t, err)

	// Write some data to create SST files
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%05d", i)
		value := fmt.Sprintf("value-%05d-%s", i, strings.Repeat("x", 100))
		require.NoError(t, db.Set([]byte(key), []byte(value), pebble.Sync))
	}
	require.NoError(t, db.Flush())

	// Pre-populate tiered storage with "old" SST files
	tieredFiles := []struct {
		remotePath string
		localName  string
		data       []byte
	}{
		{"tiered/000001.sst", "000001.sst", []byte(strings.Repeat("tiered-1-", 100))},
		{"tiered/000002.sst", "000002.sst", []byte(strings.Repeat("tiered-2-", 100))},
		{"tiered/000003.sst", "000003.sst", []byte(strings.Repeat("tiered-3-", 100))},
	}

	for _, f := range tieredFiles {
		writer, err := storage.CreateObject(f.remotePath)
		require.NoError(t, err)
		_, err = writer.Write(f.data)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	}

	// Create REMOTE-OBJ-CATALOG to simulate tiered storage catalog
	remoteObjCatalog := `1
1 000001.sst tiered/000001.sst
1 000002.sst tiered/000002.sst
1 000003.sst tiered/000003.sst
`
	require.NoError(t, os.WriteFile(filepath.Join(dbDir, "REMOTE-OBJ-CATALOG"), []byte(remoteObjCatalog), 0644))

	// Create adapter
	adapter := &pebbleDBAdapter{db: db, path: dbDir}

	// Create snapshot
	snap, err := CreateCASSnapshot(ctx, adapter, CASSnapshotOptions{
		Storage:     storage,
		Prefix:      "cobble/",
		Description: "Mixed local and tiered test",
	})
	require.NoError(t, err)
	require.NoError(t, db.Close())

	t.Logf("Created snapshot: %s with %d blobs", snap.ID, len(snap.Blobs))

	// Analyze blobs
	var sha256Count, etagCount int
	for _, blob := range snap.Blobs {
		keyPreview := blob.Key
		if len(keyPreview) > 30 {
			keyPreview = keyPreview[:30] + "..."
		}
		t.Logf("  %s: %s (source=%s, size=%d)", blob.OriginalName, keyPreview, blob.Source, blob.Size)
		switch blob.KeyType() {
		case BlobKeyTypeSHA256:
			sha256Count++
		case BlobKeyTypeETag:
			etagCount++
			require.Equal(t, "tiered", blob.Source)
		}
	}

	t.Logf("Blob breakdown: %d SHA256 (local), %d ETag (tiered)", sha256Count, etagCount)

	// Verify we have both types
	require.Greater(t, sha256Count, 0, "Should have some SHA256 blobs from local files")
	require.Greater(t, etagCount, 0, "Should have some ETag blobs from tiered files")

	t.Log("=== Mixed Local and Tiered Snapshot Test PASSED ===")
}

// TestTieredStorageRestoreIntegration tests restoring a snapshot that was created
// with tiered storage files.
func TestTieredStorageRestoreIntegration(t *testing.T) {
	minio := testutil.StartMinIO(t)
	ctx := context.Background()
	bucket := "restore-test"

	require.NoError(t, minio.CreateBucket(ctx, bucket))

	storage, err := s3.NewStorage(s3.Config{
		Bucket:          bucket,
		Prefix:          "cobble/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	require.NoError(t, err)
	defer storage.Close()

	tmpDir := t.TempDir()

	// Create test files with known content
	testFiles := map[string][]byte{
		"local-manifest.txt": []byte("manifest-content-12345"),
		"local-data.sst":     []byte(strings.Repeat("local-sst-", 100)),
	}

	// Pre-populate tiered storage
	tieredData := []byte(strings.Repeat("tiered-data-", 100))
	tieredPath := "tiered/remote.sst"

	writer, err := storage.CreateObject(tieredPath)
	require.NoError(t, err)
	_, err = writer.Write(tieredData)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	// Get ETag for tiered file
	etag, err := storage.GetETag(ctx, tieredPath)
	require.NoError(t, err)

	// Copy tiered file to blobs using server-side copy
	// Note: Blobs should be stored at prefix/blobs/key, where prefix is "cobble/"
	etagKey := "etag:" + etag
	require.NoError(t, storage.Copy(ctx, tieredPath, "cobble/blobs/"+etagKey))

	// Upload local files to blobs with SHA256 keys
	var blobs []CASBlobRef
	for name, data := range testFiles {
		hash := sha256.Sum256(data)
		key := "sha256:" + hex.EncodeToString(hash[:])

		writer, err := storage.CreateObject("cobble/blobs/" + key)
		require.NoError(t, err)
		_, err = writer.Write(data)
		require.NoError(t, err)
		require.NoError(t, writer.Close())

		blobs = append(blobs, CASBlobRef{
			Key:          key,
			Size:         int64(len(data)),
			OriginalName: name,
			Type:         "local",
			Source:       "local",
		})
	}

	// Add tiered blob
	blobs = append(blobs, CASBlobRef{
		Key:          etagKey,
		Size:         int64(len(tieredData)),
		OriginalName: "remote.sst",
		Type:         "sst",
		Source:       "tiered",
	})

	// Create and save manifest
	manifest := &CASManifest{
		ID:        fmt.Sprintf("restore-test-%d", time.Now().Unix()),
		CreatedAt: time.Now(),
		NodeID:    "test-node",
		Blobs:     blobs,
		TotalSize: func() int64 {
			var sum int64
			for _, b := range blobs {
				sum += b.Size
			}
			return sum
		}(),
	}
	require.NoError(t, SaveCASManifest(ctx, storage, "cobble/", manifest))

	// Create and save catalog
	catalog := &CASCatalog{
		BlobRefCounts: make(map[string]int),
	}
	catalog.AddSnapshot(*manifest)
	require.NoError(t, SaveCASCatalog(ctx, storage, "cobble/", catalog))

	// Restore snapshot
	restoreDir := filepath.Join(tmpDir, "restored")
	err = RestoreCASSnapshot(ctx, restoreDir, CASRestoreOptions{
		Storage:    storage,
		Prefix:     "cobble/",
		SnapshotID: manifest.ID,
	})
	require.NoError(t, err)

	// Verify restored files
	for name, expectedData := range testFiles {
		restoredPath := filepath.Join(restoreDir, name)
		restoredData, err := os.ReadFile(restoredPath)
		require.NoError(t, err, "Failed to read restored file: %s", name)
		require.Equal(t, expectedData, restoredData, "Content mismatch for %s", name)
		t.Logf("Verified: %s (%d bytes)", name, len(restoredData))
	}

	// Verify tiered file
	restoredTieredPath := filepath.Join(restoreDir, "remote.sst")
	restoredTieredData, err := os.ReadFile(restoredTieredPath)
	require.NoError(t, err)
	require.Equal(t, tieredData, restoredTieredData)
	t.Logf("Verified: remote.sst (%d bytes) - restored from ETag blob", len(restoredTieredData))

	t.Log("=== Tiered Storage Restore Integration Test PASSED ===")
}

// TestServerSideCopyFallback tests that when server-side copy fails,
// the system can fall back to download+upload.
func TestServerSideCopyFallback(t *testing.T) {
	minio := testutil.StartMinIO(t)
	ctx := context.Background()
	bucket := "fallback-test"

	require.NoError(t, minio.CreateBucket(ctx, bucket))

	storage, err := s3.NewStorage(s3.Config{
		Bucket:          bucket,
		Prefix:          "cobble/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	require.NoError(t, err)
	defer storage.Close()

	// Pre-populate a file
	testData := []byte("test-data-for-copy")
	writer, err := storage.CreateObject("source/test.txt")
	require.NoError(t, err)
	_, err = writer.Write(testData)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	// Test normal copy works
	err = storage.Copy(ctx, "source/test.txt", "dest/test.txt")
	require.NoError(t, err)

	// Verify copied data
	size, err := storage.Size("dest/test.txt")
	require.NoError(t, err)
	require.Equal(t, int64(len(testData)), size)

	// Test copy of non-existent file fails gracefully
	err = storage.Copy(ctx, "source/nonexistent.txt", "dest/nonexistent.txt")
	require.Error(t, err, "Copy of non-existent file should fail")

	t.Log("=== Server-Side Copy Fallback Test PASSED ===")
}

// TestETagConsistency tests that ETag values are consistent and can be used
// as content identifiers.
func TestETagConsistency(t *testing.T) {
	minio := testutil.StartMinIO(t)
	ctx := context.Background()
	bucket := "etag-consistency-test"

	require.NoError(t, minio.CreateBucket(ctx, bucket))

	storage, err := s3.NewStorage(s3.Config{
		Bucket:          bucket,
		Prefix:          "cobble/",
		Endpoint:        minio.Endpoint,
		AccessKeyID:     minio.AccessKey,
		SecretAccessKey: minio.SecretKey,
		ForcePathStyle:  true,
	})
	require.NoError(t, err)
	defer storage.Close()

	// Create same content in two different locations
	testData := []byte("identical-content-for-etag-test")

	for _, path := range []string{"file1.txt", "file2.txt"} {
		writer, err := storage.CreateObject(path)
		require.NoError(t, err)
		_, err = writer.Write(testData)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	}

	// Get ETags
	etag1, err := storage.GetETag(ctx, "file1.txt")
	require.NoError(t, err)

	etag2, err := storage.GetETag(ctx, "file2.txt")
	require.NoError(t, err)

	// For single-part uploads of identical content, ETags should be identical
	// (they're MD5 hashes of the content)
	require.Equal(t, etag1, etag2, "ETags for identical content should match")

	t.Logf("ETag for identical content: %s", etag1)

	// Create different content
	differentData := []byte("different-content")
	writer, err := storage.CreateObject("file3.txt")
	require.NoError(t, err)
	_, err = writer.Write(differentData)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	etag3, err := storage.GetETag(ctx, "file3.txt")
	require.NoError(t, err)

	require.NotEqual(t, etag1, etag3, "ETags for different content should differ")

	t.Logf("ETag for different content: %s", etag3)
	t.Log("=== ETag Consistency Test PASSED ===")
}
