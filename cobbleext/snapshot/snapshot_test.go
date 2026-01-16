// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package snapshot

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/objstorage/remote"
)

// mockStorage implements remote.Storage for testing.
type mockStorage struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		objects: make(map[string][]byte),
	}
}

func (m *mockStorage) CreateObject(objName string) (io.WriteCloser, error) {
	return &mockWriter{storage: m, name: objName}, nil
}

func (m *mockStorage) ReadObject(ctx context.Context, objName string) (remote.ObjectReader, int64, error) {
	m.mu.RLock()
	data, ok := m.objects[objName]
	m.mu.RUnlock()
	if !ok {
		return nil, 0, &notExistError{name: objName}
	}
	// Return a copy of the data
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	return &mockReader{data: dataCopy}, int64(len(data)), nil
}

func (m *mockStorage) Size(objName string) (int64, error) {
	m.mu.RLock()
	data, ok := m.objects[objName]
	m.mu.RUnlock()
	if !ok {
		return 0, &notExistError{name: objName}
	}
	return int64(len(data)), nil
}

func (m *mockStorage) List(prefix, delimiter string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []string
	for name := range m.objects {
		if len(prefix) == 0 || len(name) >= len(prefix) && name[:len(prefix)] == prefix {
			result = append(result, name)
		}
	}
	return result, nil
}

func (m *mockStorage) Delete(objName string) error {
	m.mu.Lock()
	delete(m.objects, objName)
	m.mu.Unlock()
	return nil
}

func (m *mockStorage) Close() error {
	return nil
}

func (m *mockStorage) IsNotExistError(err error) bool {
	_, ok := err.(*notExistError)
	return ok
}

func (m *mockStorage) put(name string, data []byte) {
	m.mu.Lock()
	m.objects[name] = data
	m.mu.Unlock()
}

type notExistError struct {
	name string
}

func (e *notExistError) Error() string {
	return "object not found: " + e.name
}

type mockWriter struct {
	storage *mockStorage
	name    string
	data    []byte
}

func (w *mockWriter) Write(p []byte) (int, error) {
	w.data = append(w.data, p...)
	return len(p), nil
}

func (w *mockWriter) Close() error {
	w.storage.put(w.name, w.data)
	return nil
}

// mockReader implements remote.ObjectReader for testing.
type mockReader struct {
	data []byte
}

func (r *mockReader) ReadAt(ctx context.Context, p []byte, offset int64) error {
	copy(p, r.data[offset:])
	return nil
}

func (r *mockReader) Close() error {
	return nil
}

// mockDB implements DBAdapter for testing.
type mockDB struct {
	checkpointDir string
	files         map[string][]byte
}

func newMockDB(dir string) *mockDB {
	return &mockDB{
		checkpointDir: dir,
		files: map[string][]byte{
			"MANIFEST-000001": []byte("manifest data"),
			"000001.sst":      []byte("sst data 1"),
			"000002.sst":      []byte("sst data 2"),
			"OPTIONS-000001":  []byte("options data"),
		},
	}
}

func (m *mockDB) Checkpoint(destDir string) error {
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return err
	}
	for name, data := range m.files {
		if err := os.WriteFile(filepath.Join(destDir, name), data, 0644); err != nil {
			return err
		}
	}
	return nil
}

func (m *mockDB) Path() string {
	return m.checkpointDir
}

func TestCreateSnapshot(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newMockStorage()
	db := newMockDB(tmpDir)

	ctx := context.Background()
	opts := SnapshotOptions{
		Storage:    storage,
		Prefix:     "test-snapshots/",
		SnapshotID: "test-snap-001",
	}

	result, err := CreateSnapshot(ctx, db, opts)
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}

	if result.SnapshotID != "test-snap-001" {
		t.Errorf("expected snapshot ID test-snap-001, got %s", result.SnapshotID)
	}

	if result.FileCount != 4 {
		t.Errorf("expected 4 files, got %d", result.FileCount)
	}

	if result.Incremental {
		t.Error("expected non-incremental snapshot")
	}

	t.Logf("Snapshot created: ID=%s, Files=%d, Size=%d, Duration=%v",
		result.SnapshotID, result.FileCount, result.TotalSize, result.Duration)

	// Verify catalog was updated
	catalog, err := LoadCatalog(ctx, storage, "test-snapshots/")
	if err != nil {
		t.Fatalf("LoadCatalog failed: %v", err)
	}

	if len(catalog.Snapshots) != 1 {
		t.Fatalf("expected 1 snapshot in catalog, got %d", len(catalog.Snapshots))
	}

	if catalog.Snapshots[0].ID != "test-snap-001" {
		t.Errorf("expected snapshot ID test-snap-001, got %s", catalog.Snapshots[0].ID)
	}

	// Verify manifest exists
	manifest, err := LoadManifest(ctx, storage, "test-snapshots/", "test-snap-001")
	if err != nil {
		t.Fatalf("LoadManifest failed: %v", err)
	}

	if len(manifest.Files) != 4 {
		t.Errorf("expected 4 files in manifest, got %d", len(manifest.Files))
	}
}

func TestIncrementalSnapshot(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newMockStorage()
	db := newMockDB(tmpDir)

	ctx := context.Background()

	// Create first (full) snapshot
	opts1 := SnapshotOptions{
		Storage:    storage,
		Prefix:     "test-snapshots/",
		SnapshotID: "snap-001",
	}

	result1, err := CreateSnapshot(ctx, db, opts1)
	if err != nil {
		t.Fatalf("First snapshot failed: %v", err)
	}

	t.Logf("First snapshot: ID=%s, Files=%d", result1.SnapshotID, result1.FileCount)

	// Modify files for second snapshot
	db.files["000003.sst"] = []byte("new sst data")

	// Create second (incremental) snapshot
	opts2 := SnapshotOptions{
		Storage:     storage,
		Prefix:      "test-snapshots/",
		SnapshotID:  "snap-002",
		Incremental: true,
	}

	result2, err := CreateSnapshot(ctx, db, opts2)
	if err != nil {
		t.Fatalf("Second snapshot failed: %v", err)
	}

	if !result2.Incremental {
		t.Error("expected incremental snapshot")
	}

	if result2.ParentSnapshotID != "snap-001" {
		t.Errorf("expected parent snap-001, got %s", result2.ParentSnapshotID)
	}

	// Should have uploaded only the new file
	if result2.UploadedCount != 1 {
		t.Errorf("expected 1 uploaded file, got %d", result2.UploadedCount)
	}

	t.Logf("Second snapshot: ID=%s, Files=%d, Uploaded=%d, Incremental=%v",
		result2.SnapshotID, result2.FileCount, result2.UploadedCount, result2.Incremental)

	// Verify manifest
	manifest, err := LoadManifest(ctx, storage, "test-snapshots/", "snap-002")
	if err != nil {
		t.Fatalf("LoadManifest failed: %v", err)
	}

	if len(manifest.NewFiles) != 1 {
		t.Errorf("expected 1 new file, got %d", len(manifest.NewFiles))
	}

	if len(manifest.InheritedFiles) != 4 {
		t.Errorf("expected 4 inherited files, got %d", len(manifest.InheritedFiles))
	}
}

func TestRestoreSnapshot(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newMockStorage()
	db := newMockDB(tmpDir)

	ctx := context.Background()

	// Create snapshot
	opts := SnapshotOptions{
		Storage:    storage,
		Prefix:     "test-snapshots/",
		SnapshotID: "snap-restore-test",
	}

	_, err := CreateSnapshot(ctx, db, opts)
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}

	// Restore to a new directory
	restoreDir := filepath.Join(tmpDir, "restored")
	restoreOpts := RestoreOptions{
		Storage:         storage,
		Prefix:          "test-snapshots/",
		SnapshotID:      "snap-restore-test",
		VerifyChecksums: true,
	}

	if err := RestoreSnapshot(ctx, restoreDir, restoreOpts); err != nil {
		t.Fatalf("RestoreSnapshot failed: %v", err)
	}

	// Verify files were restored
	for name, expectedData := range db.files {
		data, err := os.ReadFile(filepath.Join(restoreDir, name))
		if err != nil {
			t.Errorf("failed to read restored file %s: %v", name, err)
			continue
		}
		if string(data) != string(expectedData) {
			t.Errorf("file %s content mismatch", name)
		}
	}

	t.Log("Snapshot restored successfully")
}

func TestDeleteSnapshot(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newMockStorage()
	db := newMockDB(tmpDir)

	ctx := context.Background()

	// Create snapshot
	opts := SnapshotOptions{
		Storage:    storage,
		Prefix:     "test-snapshots/",
		SnapshotID: "snap-delete-test",
	}

	_, err := CreateSnapshot(ctx, db, opts)
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}

	// Delete snapshot
	if err := DeleteSnapshot(ctx, storage, "test-snapshots/", "snap-delete-test"); err != nil {
		t.Fatalf("DeleteSnapshot failed: %v", err)
	}

	// Verify catalog is empty
	catalog, err := LoadCatalog(ctx, storage, "test-snapshots/")
	if err != nil {
		t.Fatalf("LoadCatalog failed: %v", err)
	}

	if len(catalog.Snapshots) != 0 {
		t.Errorf("expected 0 snapshots, got %d", len(catalog.Snapshots))
	}

	t.Log("Snapshot deleted successfully")
}

func TestGarbageCollect(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newMockStorage()
	db := newMockDB(tmpDir)

	ctx := context.Background()

	// Create multiple snapshots
	for i := 1; i <= 5; i++ {
		opts := SnapshotOptions{
			Storage:    storage,
			Prefix:     "test-snapshots/",
			SnapshotID: fmt.Sprintf("snap-%03d", i),
		}
		if _, err := CreateSnapshot(ctx, db, opts); err != nil {
			t.Fatalf("CreateSnapshot %d failed: %v", i, err)
		}
	}

	// Garbage collect, keeping only 2
	gcOpts := GCOptions{
		Storage:  storage,
		Prefix:   "test-snapshots/",
		KeepLast: 2,
	}

	deleted, err := GarbageCollect(ctx, gcOpts)
	if err != nil {
		t.Fatalf("GarbageCollect failed: %v", err)
	}

	if len(deleted) != 3 {
		t.Errorf("expected 3 deleted, got %d", len(deleted))
	}

	// Verify catalog has only 2 snapshots
	catalog, err := LoadCatalog(ctx, storage, "test-snapshots/")
	if err != nil {
		t.Fatalf("LoadCatalog failed: %v", err)
	}

	if len(catalog.Snapshots) != 2 {
		t.Errorf("expected 2 snapshots, got %d", len(catalog.Snapshots))
	}

	t.Logf("GC deleted %d snapshots, %d remaining", len(deleted), len(catalog.Snapshots))
}

func TestSnapshotChain(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newMockStorage()
	db := newMockDB(tmpDir)

	ctx := context.Background()

	// Create base snapshot
	opts1 := SnapshotOptions{
		Storage:    storage,
		Prefix:     "test-snapshots/",
		SnapshotID: "base-001",
	}
	if _, err := CreateSnapshot(ctx, db, opts1); err != nil {
		t.Fatalf("Base snapshot failed: %v", err)
	}

	// Add a file
	db.files["000003.sst"] = []byte("new data 1")

	// Create first incremental
	opts2 := SnapshotOptions{
		Storage:     storage,
		Prefix:      "test-snapshots/",
		SnapshotID:  "incr-001",
		Incremental: true,
	}
	if _, err := CreateSnapshot(ctx, db, opts2); err != nil {
		t.Fatalf("First incremental failed: %v", err)
	}

	// Add another file
	db.files["000004.sst"] = []byte("new data 2")

	// Create second incremental
	opts3 := SnapshotOptions{
		Storage:     storage,
		Prefix:      "test-snapshots/",
		SnapshotID:  "incr-002",
		Incremental: true,
	}
	if _, err := CreateSnapshot(ctx, db, opts3); err != nil {
		t.Fatalf("Second incremental failed: %v", err)
	}

	// Load catalog and verify chain
	catalog, err := LoadCatalog(ctx, storage, "test-snapshots/")
	if err != nil {
		t.Fatalf("LoadCatalog failed: %v", err)
	}

	chain, err := catalog.GetSnapshotChain("incr-002")
	if err != nil {
		t.Fatalf("GetSnapshotChain failed: %v", err)
	}

	if len(chain) != 3 {
		t.Fatalf("expected chain length 3, got %d", len(chain))
	}

	// Chain should be [base-001, incr-001, incr-002]
	if chain[0].ID != "base-001" {
		t.Errorf("expected first in chain to be base-001, got %s", chain[0].ID)
	}
	if chain[1].ID != "incr-001" {
		t.Errorf("expected second in chain to be incr-001, got %s", chain[1].ID)
	}
	if chain[2].ID != "incr-002" {
		t.Errorf("expected third in chain to be incr-002, got %s", chain[2].ID)
	}

	t.Logf("Snapshot chain: %v", []string{chain[0].ID, chain[1].ID, chain[2].ID})

	// Restore from the incremental chain
	restoreDir := filepath.Join(tmpDir, "restored-chain")
	restoreOpts := RestoreOptions{
		Storage:         storage,
		Prefix:          "test-snapshots/",
		SnapshotID:      "incr-002",
		VerifyChecksums: true,
	}

	if err := RestoreSnapshot(ctx, restoreDir, restoreOpts); err != nil {
		t.Fatalf("RestoreSnapshot from chain failed: %v", err)
	}

	// Verify all files are present
	expectedFiles := []string{"MANIFEST-000001", "000001.sst", "000002.sst", "000003.sst", "000004.sst", "OPTIONS-000001"}
	for _, name := range expectedFiles {
		if _, err := os.Stat(filepath.Join(restoreDir, name)); err != nil {
			t.Errorf("expected file %s to exist: %v", name, err)
		}
	}

	t.Log("Incremental chain restore successful")
}
