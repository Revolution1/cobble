// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build (s3 || cloud) && integration

package integrationtest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/cobbleext"
	"github.com/cockroachdb/pebble/cobbleext/testutil"
)

// TestPebbleWithS3Integration tests full Pebble workflow with S3 backend.
func TestPebbleWithS3Integration(t *testing.T) {
	// Start MinIO
	minio := testutil.StartMinIO(t)

	ctx := context.Background()
	bucket := "pebble-integration-test"

	// Create bucket
	if err := minio.CreateBucket(ctx, bucket); err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	// Setup environment
	cleanup := minio.SetupTestEnvironment(bucket)
	defer cleanup()

	// Create temp directory for Pebble
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "testdb")

	t.Run("OpenAndClose", func(t *testing.T) {
		db, err := pebble.Open(dbPath, &pebble.Options{})
		if err != nil {
			t.Fatalf("failed to open database: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("failed to close database: %v", err)
		}
	})

	t.Run("BasicOperations", func(t *testing.T) {
		dbPath := filepath.Join(tmpDir, "testdb-basic")
		db, err := pebble.Open(dbPath, &pebble.Options{})
		if err != nil {
			t.Fatalf("failed to open database: %v", err)
		}
		defer db.Close()

		// Write some data
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key-%05d", i))
			value := []byte(fmt.Sprintf("value-%05d", i))
			if err := db.Set(key, value, pebble.Sync); err != nil {
				t.Fatalf("Set failed: %v", err)
			}
		}

		// Read back
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key-%05d", i))
			expected := []byte(fmt.Sprintf("value-%05d", i))
			value, closer, err := db.Get(key)
			if err != nil {
				t.Fatalf("Get(%s) failed: %v", key, err)
			}
			if string(value) != string(expected) {
				t.Errorf("Get(%s) = %s, want %s", key, value, expected)
			}
			closer.Close()
		}
	})

	t.Run("BatchWrite", func(t *testing.T) {
		dbPath := filepath.Join(tmpDir, "testdb-batch")
		db, err := pebble.Open(dbPath, &pebble.Options{})
		if err != nil {
			t.Fatalf("failed to open database: %v", err)
		}
		defer db.Close()

		// Batch write
		batch := db.NewBatch()
		for i := 0; i < 1000; i++ {
			key := []byte(fmt.Sprintf("batch-key-%05d", i))
			value := []byte(fmt.Sprintf("batch-value-%05d", i))
			if err := batch.Set(key, value, nil); err != nil {
				t.Fatalf("batch Set failed: %v", err)
			}
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			t.Fatalf("batch Commit failed: %v", err)
		}

		// Verify
		count := 0
		iter, err := db.NewIter(&pebble.IterOptions{
			LowerBound: []byte("batch-key-"),
			UpperBound: []byte("batch-key-~"),
		})
		if err != nil {
			t.Fatalf("NewIter failed: %v", err)
		}
		for iter.First(); iter.Valid(); iter.Next() {
			count++
		}
		iter.Close()

		if count != 1000 {
			t.Errorf("iterator count = %d, want 1000", count)
		}
	})

	t.Run("CompactionTrigger", func(t *testing.T) {
		dbPath := filepath.Join(tmpDir, "testdb-compaction")
		db, err := pebble.Open(dbPath, &pebble.Options{
			// Smaller memtable to trigger more flushes
			MemTableSize: 1024 * 1024, // 1MB
		})
		if err != nil {
			t.Fatalf("failed to open database: %v", err)
		}
		defer db.Close()

		// Write enough data to trigger compaction
		valueSize := 1024 // 1KB values
		value := make([]byte, valueSize)
		for i := range value {
			value[i] = byte(i % 256)
		}

		// Write 10MB of data to trigger compaction
		numKeys := 10 * 1024
		for i := 0; i < numKeys; i++ {
			key := []byte(fmt.Sprintf("compact-key-%08d", i))
			if err := db.Set(key, value, nil); err != nil {
				t.Fatalf("Set failed at key %d: %v", i, err)
			}
		}

		// Flush to ensure data is persisted
		if err := db.Flush(); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}

		// Verify data
		for i := 0; i < numKeys; i += 100 {
			key := []byte(fmt.Sprintf("compact-key-%08d", i))
			_, closer, err := db.Get(key)
			if err != nil {
				t.Fatalf("Get(%s) failed: %v", key, err)
			}
			closer.Close()
		}

		// Get metrics to verify storage is working
		metrics := db.Metrics()
		t.Logf("Metrics: levels=%d, total-size=%d", len(metrics.Levels), metrics.DiskSpaceUsage())
	})

	t.Run("ReopenDatabase", func(t *testing.T) {
		dbPath := filepath.Join(tmpDir, "testdb-reopen")

		// Open and write
		db, err := pebble.Open(dbPath, &pebble.Options{})
		if err != nil {
			t.Fatalf("failed to open database: %v", err)
		}

		testKey := []byte("reopen-test-key")
		testValue := []byte("reopen-test-value")
		if err := db.Set(testKey, testValue, pebble.Sync); err != nil {
			t.Fatalf("Set failed: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		// Reopen and read
		db, err = pebble.Open(dbPath, &pebble.Options{})
		if err != nil {
			t.Fatalf("failed to reopen database: %v", err)
		}
		defer db.Close()

		value, closer, err := db.Get(testKey)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		defer closer.Close()

		if string(value) != string(testValue) {
			t.Errorf("Get = %s, want %s", value, testValue)
		}
	})
}

// TestConfigLoading tests that configuration is properly loaded from environment.
func TestConfigLoading(t *testing.T) {
	minio := testutil.StartMinIO(t)

	ctx := context.Background()
	bucket := "config-test"

	if err := minio.CreateBucket(ctx, bucket); err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	// Clear any existing config
	os.Unsetenv("COBBLE_CONFIG")
	os.Unsetenv("COBBLE_S3_BUCKET")

	t.Run("LoadFromEnvironment", func(t *testing.T) {
		os.Setenv("COBBLE_S3_BUCKET", bucket)
		os.Setenv("COBBLE_S3_ENDPOINT", minio.Endpoint)
		os.Setenv("COBBLE_S3_ACCESS_KEY_ID", minio.AccessKey)
		os.Setenv("COBBLE_S3_SECRET_ACCESS_KEY", minio.SecretKey)
		os.Setenv("COBBLE_S3_FORCE_PATH_STYLE", "true")
		os.Setenv("COBBLE_TIERING_STRATEGY", "lower")
		os.Setenv("COBBLE_CACHE_SIZE", "100MB")
		defer func() {
			os.Unsetenv("COBBLE_S3_BUCKET")
			os.Unsetenv("COBBLE_S3_ENDPOINT")
			os.Unsetenv("COBBLE_S3_ACCESS_KEY_ID")
			os.Unsetenv("COBBLE_S3_SECRET_ACCESS_KEY")
			os.Unsetenv("COBBLE_S3_FORCE_PATH_STYLE")
			os.Unsetenv("COBBLE_TIERING_STRATEGY")
			os.Unsetenv("COBBLE_CACHE_SIZE")
		}()

		cfg, err := cobbleext.Load()
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if !cfg.TieredStorage.Enabled {
			t.Error("expected tiered storage to be enabled")
		}
		if cfg.TieredStorage.S3 == nil {
			t.Fatal("expected S3 config to be set")
		}
		if cfg.TieredStorage.S3.Bucket != bucket {
			t.Errorf("bucket = %q, want %q", cfg.TieredStorage.S3.Bucket, bucket)
		}
		if cfg.TieredStorage.Strategy != cobbleext.TieringStrategyLower {
			t.Errorf("strategy = %q, want %q", cfg.TieredStorage.Strategy, cobbleext.TieringStrategyLower)
		}
		if cfg.TieredStorage.Cache.Size != "100MB" {
			t.Errorf("cache size = %q, want %q", cfg.TieredStorage.Cache.Size, "100MB")
		}
	})

	t.Run("LoadFromFile", func(t *testing.T) {
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "config.yaml")

		configContent := fmt.Sprintf(`
tiered_storage:
  enabled: true
  strategy: all
  s3:
    bucket: %s
    endpoint: %s
    access_key_id: %s
    secret_access_key: %s
    force_path_style: true
  cache:
    size: 200MB
`, bucket, minio.Endpoint, minio.AccessKey, minio.SecretKey)

		if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
			t.Fatalf("failed to write config file: %v", err)
		}

		os.Setenv("COBBLE_CONFIG", configPath)
		defer os.Unsetenv("COBBLE_CONFIG")

		cfg, err := cobbleext.Load()
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if cfg.TieredStorage.Strategy != cobbleext.TieringStrategyAll {
			t.Errorf("strategy = %q, want %q", cfg.TieredStorage.Strategy, cobbleext.TieringStrategyAll)
		}
		if cfg.TieredStorage.Cache.Size != "200MB" {
			t.Errorf("cache size = %q, want %q", cfg.TieredStorage.Cache.Size, "200MB")
		}
	})
}
