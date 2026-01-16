// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package server

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// mockDB implements DBAdapter for testing.
type mockDB struct {
	path string
}

func (m *mockDB) Metrics() interface{} {
	return map[string]interface{}{
		"test_metric": 42,
	}
}

func (m *mockDB) Checkpoint(destDir string) error {
	return nil
}

func (m *mockDB) Path() string {
	return m.path
}

func (m *mockDB) OnClose(fn func()) {}

func TestServerStartStop(t *testing.T) {
	cfg := Config{
		Enabled: true,
		Addr:    "127.0.0.1:0", // Use port 0 to get a random available port
	}
	db := &mockDB{path: "/tmp/test-db"}

	srv := New(cfg, db)

	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	addr := srv.Addr()
	if addr == "" {
		t.Fatal("expected non-empty address")
	}
	t.Logf("Server started on %s", addr)

	// Test health endpoint
	resp, err := http.Get("http://" + addr + "/health")
	if err != nil {
		t.Fatalf("failed to get /health: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	var health map[string]interface{}
	if err := json.Unmarshal(body, &health); err != nil {
		t.Fatalf("failed to parse health response: %v", err)
	}
	if health["status"] != "ok" {
		t.Errorf("expected status ok, got %v", health["status"])
	}

	// Test status endpoint
	resp, err = http.Get("http://" + addr + "/status")
	if err != nil {
		t.Fatalf("failed to get /status: %v", err)
	}
	defer resp.Body.Close()

	body, _ = io.ReadAll(resp.Body)
	var status map[string]interface{}
	if err := json.Unmarshal(body, &status); err != nil {
		t.Fatalf("failed to parse status response: %v", err)
	}
	if status["db_path"] != "/tmp/test-db" {
		t.Errorf("expected db_path /tmp/test-db, got %v", status["db_path"])
	}

	// Test db/stats endpoint
	resp, err = http.Get("http://" + addr + "/db/stats")
	if err != nil {
		t.Fatalf("failed to get /db/stats: %v", err)
	}
	defer resp.Body.Close()

	body, _ = io.ReadAll(resp.Body)
	var stats map[string]interface{}
	if err := json.Unmarshal(body, &stats); err != nil {
		t.Fatalf("failed to parse stats response: %v", err)
	}
	if stats["test_metric"] != float64(42) {
		t.Errorf("expected test_metric 42, got %v", stats["test_metric"])
	}

	// Test metrics endpoint
	resp, err = http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("failed to get /metrics: %v", err)
	}
	defer resp.Body.Close()

	body, _ = io.ReadAll(resp.Body)
	metricsStr := string(body)
	if len(metricsStr) == 0 {
		t.Error("expected non-empty metrics")
	}
	t.Logf("Metrics:\n%s", metricsStr)

	// Stop server
	if err := srv.Stop(); err != nil {
		t.Fatalf("failed to stop server: %v", err)
	}
}

func TestServerWithAuth(t *testing.T) {
	cfg := Config{
		Enabled: true,
		Addr:    "127.0.0.1:0",
		Token:   "secret-token",
	}
	db := &mockDB{path: "/tmp/test-db"}

	srv := New(cfg, db)
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer srv.Stop()

	addr := srv.Addr()

	// Request without token should fail
	resp, err := http.Get("http://" + addr + "/health")
	if err != nil {
		t.Fatalf("failed to get /health: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected status 401 without token, got %d", resp.StatusCode)
	}

	// Request with token should succeed
	req, _ := http.NewRequest("GET", "http://"+addr+"/health", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to get /health with token: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200 with token, got %d", resp.StatusCode)
	}
}

func TestServerUnixSocket(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")

	cfg := Config{
		Enabled: true,
		Addr:    "unix://" + socketPath,
	}
	db := &mockDB{path: "/tmp/test-db"}

	srv := New(cfg, db)
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer srv.Stop()

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	// Verify socket file exists
	if _, err := os.Stat(socketPath); err != nil {
		t.Fatalf("socket file not created: %v", err)
	}

	// Verify socket permissions (should be 0600)
	info, err := os.Stat(socketPath)
	if err != nil {
		t.Fatalf("failed to stat socket: %v", err)
	}
	perm := info.Mode().Perm()
	if perm != 0600 {
		t.Errorf("expected socket permissions 0600, got %o", perm)
	}

	t.Logf("Unix socket created at %s with permissions %o", socketPath, perm)
}
