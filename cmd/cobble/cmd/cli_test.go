// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build (s3 || cloud) && integration

package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/cobbleext/testutil"
)

// CLITestHelper provides utilities for testing the cobble CLI.
type CLITestHelper struct {
	T          testing.TB
	MinIO      *testutil.MinIOContainer
	Bucket     string
	DataDir    string
	ConfigFile string
	CobbleBin  string
	Databases  map[string]*pebble.DB
	cleanupFns []func()
}

// NewCLITestHelper creates a new test helper with MinIO backend.
func NewCLITestHelper(t testing.TB) *CLITestHelper {
	t.Helper()

	minio := testutil.StartMinIO(t)

	bucket := fmt.Sprintf("cli-test-%d", time.Now().UnixNano())
	if err := minio.CreateBucket(context.Background(), bucket); err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("failed to create data dir: %v", err)
	}

	configFile := filepath.Join(tmpDir, "cobble.yaml")

	helper := &CLITestHelper{
		T:          t,
		MinIO:      minio,
		Bucket:     bucket,
		DataDir:    dataDir,
		ConfigFile: configFile,
		Databases:  make(map[string]*pebble.DB),
	}

	// Build cobble binary
	helper.buildCobble()

	// Write config file
	helper.writeConfig()

	return helper
}

// buildCobble builds the cobble binary for testing.
func (h *CLITestHelper) buildCobble() {
	h.T.Helper()

	// Build to an exec-capable temp directory under the repo root; some CI mounts /tmp with noexec.
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		h.T.Fatalf("failed to determine caller path")
	}
	moduleRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))

	baseTmp := filepath.Join(moduleRoot, "testdata", "tmp")
	if err := os.MkdirAll(baseTmp, 0755); err != nil {
		h.T.Fatalf("failed to create temp base dir: %v", err)
	}

	tmpDir, err := os.MkdirTemp(baseTmp, "cobble-bin-")
	if err != nil {
		h.T.Fatalf("failed to create temp dir: %v", err)
	}
	h.cleanupFns = append(h.cleanupFns, func() { os.RemoveAll(tmpDir) })

	binPath := filepath.Join(tmpDir, "cobble")

	// Build from module root so relative paths resolve consistently.
	cmd := exec.Command("go", "build", "-tags", "s3 cloud", "-o", binPath, "./cmd/cobble")
	cmd.Dir = moduleRoot
	if output, err := cmd.CombinedOutput(); err != nil {
		h.T.Logf("build output: %s", string(output))
		h.T.Fatalf("failed to build cobble: %v", err)
	}

	h.CobbleBin = binPath
}

// writeConfig writes the cobble config file.
func (h *CLITestHelper) writeConfig() {
	h.T.Helper()

	config := fmt.Sprintf(`
tiered_storage:
  enabled: true
  s3:
    bucket: %s
    endpoint: %s
    access_key_id: %s
    secret_access_key: %s
    force_path_style: true

snapshot:
  prefix: snapshots/
`, h.Bucket, h.MinIO.Endpoint, h.MinIO.AccessKey, h.MinIO.SecretKey)

	if err := os.WriteFile(h.ConfigFile, []byte(config), 0644); err != nil {
		h.T.Fatalf("failed to write config: %v", err)
	}
}

// CreateDatabase creates a Pebble database with test data.
func (h *CLITestHelper) CreateDatabase(name string, keyCount int) *pebble.DB {
	h.T.Helper()

	dbPath := filepath.Join(h.DataDir, name)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		h.T.Fatalf("failed to create db dir: %v", err)
	}

	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		h.T.Fatalf("failed to open database %s: %v", name, err)
	}

	// Write test data
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s-key-%05d", name, i)
		value := fmt.Sprintf("%s-value-%05d", name, i)
		if err := db.Set([]byte(key), []byte(value), pebble.Sync); err != nil {
			db.Close()
			h.T.Fatalf("failed to write data: %v", err)
		}
	}

	if err := db.Flush(); err != nil {
		db.Close()
		h.T.Fatalf("failed to flush database: %v", err)
	}

	h.Databases[name] = db
	return db
}

// CloseDatabase closes a specific database.
func (h *CLITestHelper) CloseDatabase(name string) {
	h.T.Helper()

	if db, ok := h.Databases[name]; ok {
		if err := db.Close(); err != nil {
			h.T.Errorf("failed to close database %s: %v", name, err)
		}
		delete(h.Databases, name)
	}
}

// CloseAllDatabases closes all open databases.
func (h *CLITestHelper) CloseAllDatabases() {
	h.T.Helper()

	for name := range h.Databases {
		h.CloseDatabase(name)
	}
}

// RunCLI executes a cobble CLI command and returns the result.
func (h *CLITestHelper) RunCLI(args ...string) *CLIResult {
	h.T.Helper()

	// Prepend config file
	fullArgs := append([]string{"--config", h.ConfigFile}, args...)

	cmd := exec.Command(h.CobbleBin, fullArgs...)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("COBBLE_CONFIG=%s", h.ConfigFile),
		fmt.Sprintf("COBBLE_S3_BUCKET=%s", h.Bucket),
		fmt.Sprintf("COBBLE_S3_ENDPOINT=%s", h.MinIO.Endpoint),
		fmt.Sprintf("COBBLE_S3_ACCESS_KEY_ID=%s", h.MinIO.AccessKey),
		fmt.Sprintf("COBBLE_S3_SECRET_ACCESS_KEY=%s", h.MinIO.SecretKey),
		"COBBLE_S3_FORCE_PATH_STYLE=true",
	)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()

	return &CLIResult{
		Args:     fullArgs,
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		Err:      err,
		ExitCode: cmd.ProcessState.ExitCode(),
	}
}

// VerifyDatabaseData verifies that a database contains expected test data.
func (h *CLITestHelper) VerifyDatabaseData(dbPath string, name string, expectedKeys int) error {
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	for i := 0; i < expectedKeys; i++ {
		key := fmt.Sprintf("%s-key-%05d", name, i)
		expectedValue := fmt.Sprintf("%s-value-%05d", name, i)

		value, closer, err := db.Get([]byte(key))
		if err != nil {
			return fmt.Errorf("key %s not found: %w", key, err)
		}
		if string(value) != expectedValue {
			closer.Close()
			return fmt.Errorf("value mismatch for %s: got %s, want %s", key, value, expectedValue)
		}
		closer.Close()
	}

	return nil
}

// Cleanup cleans up test resources.
func (h *CLITestHelper) Cleanup() {
	h.CloseAllDatabases()
	for _, fn := range h.cleanupFns {
		fn()
	}
}

// CLIResult holds the result of a CLI command execution.
type CLIResult struct {
	Args     []string
	Stdout   string
	Stderr   string
	Err      error
	ExitCode int
}

// Success returns true if the command succeeded.
func (r *CLIResult) Success() bool {
	return r.ExitCode == 0
}

// Output returns stdout if successful, stderr if failed.
func (r *CLIResult) Output() string {
	if r.Success() {
		return r.Stdout
	}
	return r.Stderr
}

// MustSucceed fails the test if the command failed.
func (r *CLIResult) MustSucceed(t testing.TB) {
	t.Helper()
	if !r.Success() {
		t.Fatalf("command failed: %v\nstdout: %s\nstderr: %s", r.Err, r.Stdout, r.Stderr)
	}
}

// MustFail fails the test if the command succeeded.
func (r *CLIResult) MustFail(t testing.TB) {
	t.Helper()
	if r.Success() {
		t.Fatalf("expected command to fail, but it succeeded\nstdout: %s", r.Stdout)
	}
}

// Contains checks if stdout contains the given substring.
func (r *CLIResult) Contains(s string) bool {
	return strings.Contains(r.Stdout, s) || strings.Contains(r.Stderr, s)
}

// JSONOutput parses stdout as JSON into the given value.
func (r *CLIResult) JSONOutput(v interface{}) error {
	return json.Unmarshal([]byte(r.Stdout), v)
}

// CreateGethLikeDataDir creates a directory structure simulating geth's multiple databases.
func (h *CLITestHelper) CreateGethLikeDataDir() {
	h.T.Helper()

	// Create typical geth databases
	h.CreateDatabase("chaindata", 100)
	h.CreateDatabase("statedata", 200)
	h.CreateDatabase("ancient", 50)
}

// VerifyGethLikeRestore verifies that a restored geth-like data directory is complete.
func (h *CLITestHelper) VerifyGethLikeRestore(restoreDir string) error {
	databases := []struct {
		name     string
		keyCount int
	}{
		{"chaindata", 100},
		{"statedata", 200},
		{"ancient", 50},
	}

	for _, db := range databases {
		dbPath := filepath.Join(restoreDir, db.name)
		if err := h.VerifyDatabaseData(dbPath, db.name, db.keyCount); err != nil {
			return fmt.Errorf("database %s verification failed: %w", db.name, err)
		}
	}

	return nil
}
