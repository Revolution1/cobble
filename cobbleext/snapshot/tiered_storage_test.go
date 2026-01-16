// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package snapshot

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Tiered Storage Integration Tests
// These tests simulate the Geth scenario where most data is already in S3
// via tiered storage, and snapshot should use server-side copy instead of
// re-uploading.
// =============================================================================

// tieredMockStorage extends trackedStorage with Copy and GetETag support
// to simulate S3-like storage with server-side copy capability.
type tieredMockStorage struct {
	mu         sync.RWMutex
	objects    map[string][]byte
	etags      map[string]string // objName -> ETag
	operations []tieredOp
	copyCount  int // Track server-side copies
	uploadSize int64
}

type tieredOp struct {
	opType string
	path   string
	size   int64
	time   time.Time
}

func newTieredMockStorage() *tieredMockStorage {
	return &tieredMockStorage{
		objects:    make(map[string][]byte),
		etags:      make(map[string]string),
		operations: make([]tieredOp, 0),
	}
}

func (s *tieredMockStorage) Close() error { return nil }

func (s *tieredMockStorage) CreateObject(objName string) (io.WriteCloser, error) {
	s.mu.Lock()
	s.operations = append(s.operations, tieredOp{opType: "create", path: objName, time: time.Now()})
	s.mu.Unlock()
	return &tieredWriter{storage: s, name: objName}, nil
}

func (s *tieredMockStorage) ReadObject(ctx context.Context, objName string) (remote.ObjectReader, int64, error) {
	s.mu.RLock()
	data, ok := s.objects[objName]
	s.mu.RUnlock()
	if !ok {
		return nil, 0, fmt.Errorf("object not found: %s", objName)
	}
	return &tieredReader{data: data}, int64(len(data)), nil
}

func (s *tieredMockStorage) List(prefix, delimiter string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []string
	seen := make(map[string]bool)

	for name := range s.objects {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		rest := name[len(prefix):]
		if delimiter != "" {
			if idx := strings.Index(rest, delimiter); idx >= 0 {
				dir := rest[:idx+1]
				if !seen[dir] {
					results = append(results, dir)
					seen[dir] = true
				}
				continue
			}
		}
		results = append(results, rest)
	}
	return results, nil
}

func (s *tieredMockStorage) Delete(objName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, objName)
	delete(s.etags, objName)
	return nil
}

func (s *tieredMockStorage) Size(objName string) (int64, error) {
	s.mu.RLock()
	data, ok := s.objects[objName]
	s.mu.RUnlock()
	if !ok {
		return 0, fmt.Errorf("object not found: %s", objName)
	}
	return int64(len(data)), nil
}

func (s *tieredMockStorage) IsNotExistError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "object not found")
}

// Copy implements server-side copy (StorageCopier interface)
func (s *tieredMockStorage) Copy(ctx context.Context, srcName, dstName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, ok := s.objects[srcName]
	if !ok {
		return fmt.Errorf("source object not found: %s", srcName)
	}

	// Server-side copy - no data transfer through client
	s.objects[dstName] = data
	s.etags[dstName] = s.etags[srcName]
	s.copyCount++
	s.operations = append(s.operations, tieredOp{opType: "copy", path: fmt.Sprintf("%s -> %s", srcName, dstName), time: time.Now()})
	return nil
}

// GetETag implements StorageETagGetter interface
func (s *tieredMockStorage) GetETag(ctx context.Context, objName string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	etag, ok := s.etags[objName]
	if !ok {
		return "", fmt.Errorf("object not found: %s", objName)
	}
	return etag, nil
}

// Helper to add an object with auto-generated ETag
func (s *tieredMockStorage) addObject(name string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.objects[name] = data
	// Generate MD5-based ETag (like S3)
	hash := md5.Sum(data)
	s.etags[name] = hex.EncodeToString(hash[:])
}

// Helper to list all objects
func (s *tieredMockStorage) listAllObjects() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var names []string
	for name := range s.objects {
		names = append(names, name)
	}
	return names
}

// Helper to get stats
func (s *tieredMockStorage) getStats() (copyCount int, uploadSize int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.copyCount, s.uploadSize
}

type tieredWriter struct {
	storage *tieredMockStorage
	name    string
	buf     []byte
}

func (w *tieredWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

func (w *tieredWriter) Close() error {
	w.storage.mu.Lock()
	defer w.storage.mu.Unlock()

	w.storage.objects[w.name] = w.buf
	w.storage.uploadSize += int64(len(w.buf))

	// Generate MD5-based ETag
	hash := md5.Sum(w.buf)
	w.storage.etags[w.name] = hex.EncodeToString(hash[:])
	return nil
}

type tieredReader struct {
	data []byte
}

func (r *tieredReader) ReadAt(ctx context.Context, p []byte, offset int64) error {
	if offset < 0 || offset >= int64(len(r.data)) {
		return io.EOF
	}
	copy(p, r.data[offset:])
	return nil
}

func (r *tieredReader) Close() error { return nil }

// =============================================================================
// Tests
// =============================================================================

// TestTieredStorageServerSideCopy tests the remoteBlobCopier directly,
// verifying that it uses server-side copy when available.
func TestTieredStorageServerSideCopy(t *testing.T) {
	storage := newTieredMockStorage()

	// Simulate tiered storage: add SST files that are "already in S3"
	tieredFiles := []struct {
		name string
		data []byte
	}{
		{"tiered/000001.sst", []byte(strings.Repeat("sst-data-1-", 1000))},
		{"tiered/000002.sst", []byte(strings.Repeat("sst-data-2-", 1000))},
		{"tiered/000003.sst", []byte(strings.Repeat("sst-data-3-", 1000))},
	}

	for _, f := range tieredFiles {
		storage.addObject(f.name, f.data)
	}

	t.Logf("Pre-populated tiered storage with %d SST files", len(tieredFiles))

	// Create remote file info list (simulating what LoadRemoteObjectCatalog would return)
	remoteFileList := []RemoteFileInfo{
		{LocalName: "000001.sst", RemotePath: "tiered/000001.sst", CreatorID: 1, CreatorFileNum: 1},
		{LocalName: "000002.sst", RemotePath: "tiered/000002.sst", CreatorID: 1, CreatorFileNum: 2},
		{LocalName: "000003.sst", RemotePath: "tiered/000003.sst", CreatorID: 1, CreatorFileNum: 3},
	}

	// Create a copier
	copier := &remoteBlobCopier{
		storage:     storage,
		prefix:      "snapshot/",
		parallelism: 2,
	}

	// Create empty catalog
	catalog := &CASCatalog{
		BlobRefCounts: make(map[string]int),
	}

	// Copy remote blobs
	t.Log("Copying remote blobs using server-side copy...")
	blobs, err := copier.copyRemoteBlobs(context.Background(), remoteFileList, catalog)
	require.NoError(t, err)

	t.Logf("Copied %d blobs", len(blobs))

	// Verify results
	require.Len(t, blobs, 3)

	for _, blob := range blobs {
		t.Logf("  Blob: %s -> %s (source=%s)", blob.OriginalName, blob.Key, blob.Source)
		require.Equal(t, "tiered", blob.Source)
		require.Equal(t, BlobKeyTypeETag, blob.KeyType())
	}

	// Verify server-side copies were used
	copyCount, uploadSize := storage.getStats()
	t.Logf("Server-side copies: %d, Upload size: %d bytes", copyCount, uploadSize)

	require.Equal(t, 3, copyCount, "Expected 3 server-side copies")
	require.Equal(t, int64(0), uploadSize, "No data should be uploaded (server-side copy)")

	// Verify blobs exist in storage at the expected paths
	for _, blob := range blobs {
		blobPath := "snapshot/blobs/" + blob.Key
		_, err := storage.Size(blobPath)
		require.NoError(t, err, "Blob should exist at %s", blobPath)
	}

	t.Log("=== Tiered Storage Server-Side Copy Test PASSED ===")
}

// TestGethTieredStorageScenario simulates a realistic Geth scenario where
// 80% of database is in tiered storage and snapshot should be efficient.
// This test directly exercises the remoteBlobCopier with a large number of files.
func TestGethTieredStorageScenario(t *testing.T) {
	storage := newTieredMockStorage()

	// Simulate Geth scenario:
	// - 100 SST files total
	// - 80 (80%) already in S3 tiered storage (old SST files)
	// - 20 (20%) local (recent SST files)

	// Create "tiered" SST files
	tieredDataSize := int64(0)
	var remoteFileList []RemoteFileInfo

	for i := 1; i <= 80; i++ {
		name := fmt.Sprintf("tiered/%06d.sst", i)
		data := []byte(fmt.Sprintf("sst-file-%d-data-%s", i, strings.Repeat("x", 1000)))
		storage.addObject(name, data)
		tieredDataSize += int64(len(data))

		remoteFileList = append(remoteFileList, RemoteFileInfo{
			LocalName:      fmt.Sprintf("%06d.sst", i),
			RemotePath:     name,
			CreatorID:      1,
			CreatorFileNum: base.DiskFileNum(i),
		})
	}

	t.Logf("Tiered storage: 80 SST files, %d bytes", tieredDataSize)

	// Create the copier
	copier := &remoteBlobCopier{
		storage:     storage,
		prefix:      "geth-snapshot/",
		parallelism: 4,
	}

	// Create empty catalog
	catalog := &CASCatalog{
		BlobRefCounts: make(map[string]int),
	}

	// Record initial state
	_, initialUploadSize := storage.getStats()

	// Copy remote blobs
	t.Log("Copying remote blobs (simulating Geth with 80% tiered storage)...")
	startTime := time.Now()

	blobs, err := copier.copyRemoteBlobs(context.Background(), remoteFileList, catalog)
	require.NoError(t, err)

	elapsed := time.Since(startTime)
	t.Logf("Copy completed in %v", elapsed)

	// Analyze efficiency
	copyCount, totalUploadSize := storage.getStats()
	actualUploadSize := totalUploadSize - initialUploadSize

	var totalBlobSize int64
	for _, blob := range blobs {
		totalBlobSize += blob.Size
	}

	t.Log("=== Snapshot Efficiency Report ===")
	t.Logf("Total blobs copied: %d", len(blobs))
	t.Logf("Total blob size: %d bytes", totalBlobSize)
	t.Logf("Server-side copies: %d", copyCount)
	t.Logf("Actual data uploaded: %d bytes", actualUploadSize)

	// Calculate savings
	if totalBlobSize > 0 {
		savedPercent := float64(totalBlobSize-actualUploadSize) / float64(totalBlobSize) * 100
		t.Logf("Upload savings: %.1f%% (used server-side copy)", savedPercent)
	}

	// Verify expectations
	require.Equal(t, 80, copyCount, "Should have 80 server-side copies")
	require.Len(t, blobs, 80, "Should have 80 blobs")

	// Verify no data was uploaded (all server-side copies)
	require.Equal(t, int64(0), actualUploadSize, "No data should be uploaded")

	// Verify all blobs have correct source and key type
	for _, blob := range blobs {
		require.Equal(t, "tiered", blob.Source)
		require.Equal(t, BlobKeyTypeETag, blob.KeyType())
	}

	t.Log("=== Geth Tiered Storage Scenario Test PASSED ===")
}

// TestTieredStorageRestore tests restoring a snapshot that contains both
// SHA256 and ETag keyed blobs.
func TestTieredStorageRestore(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newTieredMockStorage()

	// Create test data with proper SHA256 hashes
	localData1 := []byte("sha256-content-1")
	localData2 := []byte("sha256-content-2")
	tieredData1 := []byte("etag-content-1")
	tieredData2 := []byte("etag-content-2")

	// Compute actual SHA256 hashes for local data
	hash1 := sha256.Sum256(localData1)
	hash2 := sha256.Sum256(localData2)
	sha256Key1 := hex.EncodeToString(hash1[:])
	sha256Key2 := hex.EncodeToString(hash2[:])

	// Add blobs to storage
	storage.addObject("test/blobs/sha256:"+sha256Key1, localData1)
	storage.addObject("test/blobs/sha256:"+sha256Key2, localData2)
	storage.addObject("test/blobs/etag:d41d8cd98f00b204", tieredData1)
	storage.addObject("test/blobs/etag:098f6bcd4621d373", tieredData2)

	// Create a manifest with mixed blob types
	manifest := &CASManifest{
		ID:        "mixed-test",
		CreatedAt: time.Now(),
		NodeID:    "test-node",
		Blobs: []CASBlobRef{
			{Key: "sha256:" + sha256Key1, Size: int64(len(localData1)), OriginalName: "local1.sst", Type: "sst", Source: "local"},
			{Key: "sha256:" + sha256Key2, Size: int64(len(localData2)), OriginalName: "local2.sst", Type: "sst", Source: "local"},
			{Key: "etag:d41d8cd98f00b204", Size: int64(len(tieredData1)), OriginalName: "tiered1.sst", Type: "sst", Source: "tiered"},
			{Key: "etag:098f6bcd4621d373", Size: int64(len(tieredData2)), OriginalName: "tiered2.sst", Type: "sst", Source: "tiered"},
		},
		TotalSize: int64(len(localData1) + len(localData2) + len(tieredData1) + len(tieredData2)),
	}

	// Save manifest
	require.NoError(t, SaveCASManifest(context.Background(), storage, "test/", manifest))

	// Update catalog
	catalog := &CASCatalog{
		BlobRefCounts: make(map[string]int),
	}
	catalog.AddSnapshot(*manifest)
	require.NoError(t, SaveCASCatalog(context.Background(), storage, "test/", catalog))

	// Restore
	restoreDir := filepath.Join(tmpDir, "restored")
	err := RestoreCASSnapshot(context.Background(), restoreDir, CASRestoreOptions{
		Storage:         storage,
		Prefix:          "test/",
		SnapshotID:      "mixed-test",
		VerifyChecksums: true, // Only verifies SHA256 blobs
	})
	require.NoError(t, err)

	// Verify all files were restored
	entries, err := os.ReadDir(restoreDir)
	require.NoError(t, err)

	restoredFiles := make(map[string]bool)
	for _, e := range entries {
		restoredFiles[e.Name()] = true
		t.Logf("Restored: %s", e.Name())
	}

	require.True(t, restoredFiles["local1.sst"], "local1.sst should be restored")
	require.True(t, restoredFiles["local2.sst"], "local2.sst should be restored")
	require.True(t, restoredFiles["tiered1.sst"], "tiered1.sst should be restored")
	require.True(t, restoredFiles["tiered2.sst"], "tiered2.sst should be restored")

	t.Log("=== Tiered Storage Restore Test PASSED ===")
}

// mockCheckpointAdapter implements DBAdapter for testing
type mockCheckpointAdapter struct {
	checkpointDir string
	path          string
}

func (a *mockCheckpointAdapter) Checkpoint(destDir string) error {
	// Copy all files from checkpointDir to destDir
	entries, err := os.ReadDir(a.checkpointDir)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(destDir, 0755); err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		srcPath := filepath.Join(a.checkpointDir, entry.Name())
		dstPath := filepath.Join(destDir, entry.Name())

		data, err := os.ReadFile(srcPath)
		if err != nil {
			return err
		}
		if err := os.WriteFile(dstPath, data, 0644); err != nil {
			return err
		}
	}
	return nil
}

func (a *mockCheckpointAdapter) Path() string {
	return a.path
}
