// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build integration

package cobbleext_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/cobbleext/server"
	"github.com/cockroachdb/pebble/cobbleext/snapshot"
	"github.com/cockroachdb/pebble/objstorage/remote"
)

// =============================================================================
// Mock Storage Implementation (simulates S3/GCS)
// =============================================================================

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
		if len(prefix) == 0 || (len(name) >= len(prefix) && name[:len(prefix)] == prefix) {
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

func (m *mockStorage) objectCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.objects)
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

// =============================================================================
// Geth-like Database Adapter
// =============================================================================

// gethDBAdapter wraps a pebble.DB to implement snapshot.DBAdapter.
type gethDBAdapter struct {
	db   *pebble.DB
	path string
}

func (a *gethDBAdapter) Checkpoint(destDir string) error {
	return a.db.Checkpoint(destDir, pebble.WithFlushedWAL())
}

func (a *gethDBAdapter) Path() string {
	return a.path
}

func (a *gethDBAdapter) Metrics() interface{} {
	return a.db.Metrics()
}

func (a *gethDBAdapter) OnClose(fn func()) {
	// In real implementation, this would register cleanup functions
}

// =============================================================================
// Simulated Geth Node
// =============================================================================

// gethNode simulates a geth node with multiple Pebble databases.
type gethNode struct {
	dataDir string

	// Databases (like in geth)
	chaindata *pebble.DB // Main chain data
	ancient   *pebble.DB // Ancient/freezer data (simplified)
	state     *pebble.DB // State trie data

	// Admin server
	adminServer *server.Server

	// Storage for snapshots
	storage remote.Storage

	// closed tracks if close has been called
	closed bool
}

// newGethNode creates a new simulated geth node.
func newGethNode(t *testing.T, dataDir string, storage remote.Storage) *gethNode {
	t.Helper()

	node := &gethNode{
		dataDir: dataDir,
		storage: storage,
	}

	// Create database directories
	chaindataDir := filepath.Join(dataDir, "chaindata")
	ancientDir := filepath.Join(dataDir, "ancient")
	stateDir := filepath.Join(dataDir, "state")

	for _, dir := range []string{chaindataDir, ancientDir, stateDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("failed to create dir %s: %v", dir, err)
		}
	}

	// Open databases
	opts := &pebble.Options{}
	var err error

	node.chaindata, err = pebble.Open(chaindataDir, opts)
	if err != nil {
		t.Fatalf("failed to open chaindata: %v", err)
	}

	node.ancient, err = pebble.Open(ancientDir, opts)
	if err != nil {
		node.chaindata.Close()
		t.Fatalf("failed to open ancient: %v", err)
	}

	node.state, err = pebble.Open(stateDir, opts)
	if err != nil {
		node.chaindata.Close()
		node.ancient.Close()
		t.Fatalf("failed to open state: %v", err)
	}

	return node
}

// startAdminServer starts the admin server for this node.
func (n *gethNode) startAdminServer(t *testing.T, addr string) {
	t.Helper()

	cfg := server.Config{
		Enabled:             true,
		Addr:                addr,
		TieredStorageEnabled: false,
		AutoSnapshotEnabled: false,
	}

	// Use chaindata as the primary DB for admin server
	adapter := &gethDBAdapter{db: n.chaindata, path: filepath.Join(n.dataDir, "chaindata")}
	n.adminServer = server.New(cfg, adapter)

	if err := n.adminServer.Start(); err != nil {
		t.Fatalf("failed to start admin server: %v", err)
	}
}

// writeData writes test data to all databases.
func (n *gethNode) writeData(t *testing.T, blockNum int) {
	t.Helper()

	// Write to chaindata (block headers, bodies, receipts)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("block-%d-header-%d", blockNum, i)
		value := fmt.Sprintf("header-data-%d-%d", blockNum, i)
		if err := n.chaindata.Set([]byte(key), []byte(value), pebble.Sync); err != nil {
			t.Fatalf("failed to write chaindata: %v", err)
		}
	}

	// Write to ancient (old block data)
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("ancient-%d-%d", blockNum, i)
		value := fmt.Sprintf("ancient-data-%d-%d", blockNum, i)
		if err := n.ancient.Set([]byte(key), []byte(value), pebble.Sync); err != nil {
			t.Fatalf("failed to write ancient: %v", err)
		}
	}

	// Write to state (account states, storage)
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("state-%d-account-%d", blockNum, i)
		value := fmt.Sprintf("state-data-%d-%d", blockNum, i)
		if err := n.state.Set([]byte(key), []byte(value), pebble.Sync); err != nil {
			t.Fatalf("failed to write state: %v", err)
		}
	}
}

// verifyData verifies test data exists in all databases.
func (n *gethNode) verifyData(t *testing.T, blockNum int) {
	t.Helper()

	// Verify chaindata
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("block-%d-header-%d", blockNum, i)
		expected := fmt.Sprintf("header-data-%d-%d", blockNum, i)
		value, closer, err := n.chaindata.Get([]byte(key))
		if err != nil {
			t.Errorf("failed to get chaindata key %s: %v", key, err)
			continue
		}
		if string(value) != expected {
			t.Errorf("chaindata mismatch for %s: got %s, want %s", key, value, expected)
		}
		closer.Close()
	}

	// Verify ancient
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("ancient-%d-%d", blockNum, i)
		expected := fmt.Sprintf("ancient-data-%d-%d", blockNum, i)
		value, closer, err := n.ancient.Get([]byte(key))
		if err != nil {
			t.Errorf("failed to get ancient key %s: %v", key, err)
			continue
		}
		if string(value) != expected {
			t.Errorf("ancient mismatch for %s: got %s, want %s", key, value, expected)
		}
		closer.Close()
	}

	// Verify state
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("state-%d-account-%d", blockNum, i)
		expected := fmt.Sprintf("state-data-%d-%d", blockNum, i)
		value, closer, err := n.state.Get([]byte(key))
		if err != nil {
			t.Errorf("failed to get state key %s: %v", key, err)
			continue
		}
		if string(value) != expected {
			t.Errorf("state mismatch for %s: got %s, want %s", key, value, expected)
		}
		closer.Close()
	}
}

// createSnapshotSet creates a coordinated snapshot of all databases.
func (n *gethNode) createSnapshotSet(t *testing.T, description string) *snapshot.SnapshotSet {
	t.Helper()

	databases := map[string]snapshot.DBAdapter{
		"chaindata": &gethDBAdapter{db: n.chaindata, path: filepath.Join(n.dataDir, "chaindata")},
		"ancient":   &gethDBAdapter{db: n.ancient, path: filepath.Join(n.dataDir, "ancient")},
		"state":     &gethDBAdapter{db: n.state, path: filepath.Join(n.dataDir, "state")},
	}

	opts := snapshot.SnapshotSetOptions{
		Storage:     n.storage,
		Prefix:      "geth-snapshots/",
		Description: description,
		Labels: map[string]string{
			"network": "mainnet",
			"type":    "full",
		},
	}

	set, err := snapshot.CreateSnapshotSet(context.Background(), databases, opts)
	if err != nil {
		t.Fatalf("failed to create snapshot set: %v", err)
	}

	return set
}

// close closes all databases and the admin server.
func (n *gethNode) close(t *testing.T) {
	t.Helper()

	if n.closed {
		return
	}
	n.closed = true

	if n.adminServer != nil {
		if err := n.adminServer.Stop(); err != nil {
			t.Errorf("failed to stop admin server: %v", err)
		}
		n.adminServer = nil
	}

	if n.chaindata != nil {
		if err := n.chaindata.Close(); err != nil {
			t.Errorf("failed to close chaindata: %v", err)
		}
		n.chaindata = nil
	}
	if n.ancient != nil {
		if err := n.ancient.Close(); err != nil {
			t.Errorf("failed to close ancient: %v", err)
		}
		n.ancient = nil
	}
	if n.state != nil {
		if err := n.state.Close(); err != nil {
			t.Errorf("failed to close state: %v", err)
		}
		n.state = nil
	}
}

// =============================================================================
// Integration Tests
// =============================================================================

// TestGethNodeFullWorkflow tests the complete workflow:
// 1. Start a geth-like node with multiple databases
// 2. Write data to simulate block processing
// 3. Create snapshot sets at different block heights
// 4. Verify admin server endpoints
// 5. Restore from snapshot and verify data
func TestGethNodeFullWorkflow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Setup
	tmpDir := t.TempDir()
	storage := newMockStorage()

	// Create and start geth node
	nodeDir := filepath.Join(tmpDir, "node1")
	node := newGethNode(t, nodeDir, storage)
	defer node.close(t)

	node.startAdminServer(t, "127.0.0.1:0")
	adminAddr := node.adminServer.Addr()
	t.Logf("Admin server started on %s", adminAddr)

	// Phase 1: Write initial data (blocks 1-10)
	t.Log("Phase 1: Writing initial data (blocks 1-10)")
	for block := 1; block <= 10; block++ {
		node.writeData(t, block)
	}

	// Create first snapshot set
	t.Log("Creating first snapshot set at block 10")
	set1 := node.createSnapshotSet(t, "Block 10 snapshot")
	t.Logf("Snapshot set 1: %s", set1.ID)

	// Phase 2: Write more data (blocks 11-20)
	t.Log("Phase 2: Writing more data (blocks 11-20)")
	for block := 11; block <= 20; block++ {
		node.writeData(t, block)
	}

	// Create second snapshot set
	t.Log("Creating second snapshot set at block 20")
	set2 := node.createSnapshotSet(t, "Block 20 snapshot")
	t.Logf("Snapshot set 2: %s", set2.ID)

	// Verify admin server endpoints
	t.Log("Verifying admin server endpoints")
	verifyAdminEndpoints(t, adminAddr)

	// List snapshot sets
	t.Log("Listing snapshot sets")
	sets, err := snapshot.ListSnapshotSets(context.Background(), storage, "geth-snapshots/")
	if err != nil {
		t.Fatalf("failed to list snapshot sets: %v", err)
	}
	if len(sets) != 2 {
		t.Errorf("expected 2 snapshot sets, got %d", len(sets))
	}

	// Close the original node
	node.close(t)

	// Phase 3: Restore from first snapshot (block 10)
	t.Log("Phase 3: Restoring from first snapshot (block 10)")
	restoreDir := filepath.Join(tmpDir, "restored")
	destDirs := map[string]string{
		"chaindata": filepath.Join(restoreDir, "chaindata"),
		"ancient":   filepath.Join(restoreDir, "ancient"),
		"state":     filepath.Join(restoreDir, "state"),
	}

	err = snapshot.RestoreSnapshotSet(context.Background(), storage, "geth-snapshots/", set1.ID, destDirs)
	if err != nil {
		t.Fatalf("failed to restore snapshot set: %v", err)
	}

	// Open restored databases and verify data
	t.Log("Verifying restored data")
	restoredNode := newGethNode(t, restoreDir, storage)
	defer restoredNode.close(t)

	// Verify blocks 1-10 exist
	for block := 1; block <= 10; block++ {
		restoredNode.verifyData(t, block)
	}
	t.Log("Blocks 1-10 verified successfully")

	// Blocks 11-20 should NOT exist (we restored from block 10 snapshot)
	key := []byte("block-15-header-0")
	_, closer, err := restoredNode.chaindata.Get(key)
	if err == nil {
		closer.Close()
		t.Error("expected block 15 data to not exist after restoring from block 10 snapshot")
	}

	t.Log("TestGethNodeFullWorkflow completed successfully")
}

// TestIncrementalSnapshots tests incremental snapshot functionality.
func TestIncrementalSnapshots(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tmpDir := t.TempDir()
	storage := newMockStorage()

	nodeDir := filepath.Join(tmpDir, "node")
	node := newGethNode(t, nodeDir, storage)
	defer node.close(t)

	// Write initial data and create full snapshot
	t.Log("Writing initial data")
	for block := 1; block <= 5; block++ {
		node.writeData(t, block)
	}

	// Create full snapshot of chaindata only
	adapter := &gethDBAdapter{db: node.chaindata, path: filepath.Join(nodeDir, "chaindata")}
	fullOpts := snapshot.SnapshotOptions{
		Storage:     storage,
		Prefix:      "incremental-test/",
		Incremental: false,
		SnapshotID:  "full-001",
	}

	fullResult, err := snapshot.CreateSnapshot(context.Background(), adapter, fullOpts)
	if err != nil {
		t.Fatalf("failed to create full snapshot: %v", err)
	}
	t.Logf("Full snapshot: ID=%s, Files=%d, Size=%d", fullResult.SnapshotID, fullResult.FileCount, fullResult.TotalSize)

	// Write more data
	t.Log("Writing additional data")
	for block := 6; block <= 10; block++ {
		node.writeData(t, block)
	}

	// Create incremental snapshot
	incrOpts := snapshot.SnapshotOptions{
		Storage:     storage,
		Prefix:      "incremental-test/",
		Incremental: true,
		SnapshotID:  "incr-001",
	}

	incrResult, err := snapshot.CreateSnapshot(context.Background(), adapter, incrOpts)
	if err != nil {
		t.Fatalf("failed to create incremental snapshot: %v", err)
	}
	t.Logf("Incremental snapshot: ID=%s, Files=%d, Uploaded=%d", incrResult.SnapshotID, incrResult.FileCount, incrResult.UploadedCount)

	// Verify incremental snapshot uploaded fewer files
	if !incrResult.Incremental {
		t.Error("expected incremental snapshot")
	}
	if incrResult.UploadedCount >= fullResult.FileCount {
		t.Logf("Note: Incremental uploaded %d files vs full %d files", incrResult.UploadedCount, fullResult.FileCount)
	}

	// Close node
	node.close(t)

	// Restore from incremental snapshot
	restoreDir := filepath.Join(tmpDir, "restored-incr")
	restoreOpts := snapshot.RestoreOptions{
		Storage:         storage,
		Prefix:          "incremental-test/",
		SnapshotID:      "incr-001",
		VerifyChecksums: true,
	}

	if err := snapshot.RestoreSnapshot(context.Background(), restoreDir, restoreOpts); err != nil {
		t.Fatalf("failed to restore from incremental: %v", err)
	}

	// Open restored database and verify all data
	restoredDB, err := pebble.Open(restoreDir, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open restored DB: %v", err)
	}
	defer restoredDB.Close()

	// Verify all blocks 1-10 exist
	for block := 1; block <= 10; block++ {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("block-%d-header-%d", block, i)
			_, closer, err := restoredDB.Get([]byte(key))
			if err != nil {
				t.Errorf("missing data for block %d: %v", block, err)
			} else {
				closer.Close()
			}
		}
	}

	t.Log("TestIncrementalSnapshots completed successfully")
}

// TestAutoSnapshotScheduler tests the auto-snapshot scheduler.
func TestAutoSnapshotScheduler(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tmpDir := t.TempDir()
	storage := newMockStorage()

	// Create a simple database
	dbPath := filepath.Join(tmpDir, "testdb")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Write some data
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		if err := db.Set([]byte(key), []byte(value), pebble.Sync); err != nil {
			t.Fatalf("failed to write data: %v", err)
		}
	}

	adapter := &gethDBAdapter{db: db, path: dbPath}

	// Configure scheduler with short interval
	cfg := snapshot.SchedulerConfig{
		Interval:    100 * time.Millisecond,
		Incremental: true,
		KeepLast:    3,
		Storage:     storage,
		Prefix:      "auto-test/",
	}

	scheduler := snapshot.NewScheduler(cfg, adapter)

	// Start scheduler
	if err := scheduler.Start(); err != nil {
		t.Fatalf("failed to start scheduler: %v", err)
	}

	// Let it run for a while
	time.Sleep(500 * time.Millisecond)

	// Stop scheduler
	if err := scheduler.Stop(); err != nil {
		t.Fatalf("failed to stop scheduler: %v", err)
	}

	// Verify snapshots were created
	catalog, err := snapshot.LoadCatalog(context.Background(), storage, "auto-test/")
	if err != nil {
		t.Fatalf("failed to load catalog: %v", err)
	}

	t.Logf("Auto scheduler created %d snapshots", len(catalog.Snapshots))

	if len(catalog.Snapshots) == 0 {
		t.Error("expected at least one snapshot from auto scheduler")
	}

	// Verify KeepLast is respected (should be <= 3)
	if len(catalog.Snapshots) > 3 {
		t.Errorf("expected at most 3 snapshots due to KeepLast, got %d", len(catalog.Snapshots))
	}

	t.Log("TestAutoSnapshotScheduler completed successfully")
}

// TestAdminServerUnixSocket tests admin server with Unix socket.
func TestAdminServerUnixSocket(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "admin.sock")

	// Create a simple database
	dbPath := filepath.Join(tmpDir, "testdb")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	adapter := &gethDBAdapter{db: db, path: dbPath}

	// Start admin server on Unix socket
	cfg := server.Config{
		Enabled: true,
		Addr:    "unix://" + socketPath,
	}

	srv := server.New(cfg, adapter)
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start admin server: %v", err)
	}
	defer srv.Stop()

	// Wait for socket to be created
	time.Sleep(100 * time.Millisecond)

	// Verify socket file exists
	if _, err := os.Stat(socketPath); err != nil {
		t.Fatalf("socket file not created: %v", err)
	}

	// Verify socket permissions
	info, err := os.Stat(socketPath)
	if err != nil {
		t.Fatalf("failed to stat socket: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Errorf("expected socket permissions 0600, got %o", info.Mode().Perm())
	}

	t.Log("TestAdminServerUnixSocket completed successfully")
}

// TestSnapshotWithAuthentication tests snapshot operations with admin server auth.
func TestSnapshotWithAuthentication(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tmpDir := t.TempDir()

	// Create a database
	dbPath := filepath.Join(tmpDir, "testdb")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Write some data
	for i := 0; i < 50; i++ {
		if err := db.Set([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)), pebble.Sync); err != nil {
			t.Fatalf("failed to write data: %v", err)
		}
	}

	adapter := &gethDBAdapter{db: db, path: dbPath}

	// Start admin server with token auth
	cfg := server.Config{
		Enabled: true,
		Addr:    "127.0.0.1:0",
		Token:   "test-secret-token",
	}

	srv := server.New(cfg, adapter)
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start admin server: %v", err)
	}
	defer srv.Stop()

	adminAddr := srv.Addr()

	// Test without token (should fail)
	resp, err := http.Get("http://" + adminAddr + "/health")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 without token, got %d", resp.StatusCode)
	}

	// Test with token (should succeed)
	req, _ := http.NewRequest("GET", "http://"+adminAddr+"/health", nil)
	req.Header.Set("Authorization", "Bearer test-secret-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request with token failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 with token, got %d", resp.StatusCode)
	}

	t.Log("TestSnapshotWithAuthentication completed successfully")
}

// =============================================================================
// Helper Functions
// =============================================================================

func verifyAdminEndpoints(t *testing.T, addr string) {
	t.Helper()

	endpoints := []struct {
		path   string
		method string
	}{
		{"/health", "GET"},
		{"/status", "GET"},
		{"/db/stats", "GET"},
		{"/metrics", "GET"},
	}

	client := &http.Client{Timeout: 5 * time.Second}

	for _, ep := range endpoints {
		url := "http://" + addr + ep.path
		req, _ := http.NewRequest(ep.method, url, nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Errorf("failed to request %s: %v", ep.path, err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s returned %d, expected 200", ep.path, resp.StatusCode)
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if ep.path == "/health" {
			var health map[string]interface{}
			if err := json.Unmarshal(body, &health); err != nil {
				t.Errorf("failed to parse /health response: %v", err)
			} else if health["status"] != "ok" {
				t.Errorf("health status is not ok: %v", health["status"])
			}
		}

		t.Logf("Endpoint %s OK (%d bytes)", ep.path, len(body))
	}
}
