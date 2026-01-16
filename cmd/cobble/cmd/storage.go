// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cmd

import (
	"fmt"
	"os"

	"github.com/cockroachdb/pebble/cobbleext"
	"github.com/cockroachdb/pebble/cobbleext/s3"
	"github.com/cockroachdb/pebble/objstorage/remote"
)

// StorageConfig holds configuration for standalone storage access.
type StorageConfig struct {
	// S3 configuration
	S3Bucket          string
	S3Region          string
	S3Endpoint        string
	S3AccessKeyID     string
	S3SecretAccessKey string
	S3ForcePathStyle  bool
	S3Prefix          string
}

// getStorageFromConfig creates a storage backend from configuration.
func getStorageFromConfig() (remote.Storage, string, error) {
	cfg, err := loadStorageConfig()
	if err != nil {
		return nil, "", err
	}

	if cfg.S3Bucket == "" {
		return nil, "", fmt.Errorf("S3 bucket not configured. Set COBBLE_S3_BUCKET or use config file")
	}

	storage, err := s3.NewStorage(s3.Config{
		Bucket:          cfg.S3Bucket,
		Region:          cfg.S3Region,
		Endpoint:        cfg.S3Endpoint,
		AccessKeyID:     cfg.S3AccessKeyID,
		SecretAccessKey: cfg.S3SecretAccessKey,
		ForcePathStyle:  cfg.S3ForcePathStyle,
		Prefix:          cfg.S3Prefix,
	})
	if err != nil {
		return nil, "", fmt.Errorf("failed to create S3 storage: %w", err)
	}

	return storage, cfg.S3Prefix, nil
}

// loadStorageConfig loads storage configuration from flags, config file, or environment.
func loadStorageConfig() (*StorageConfig, error) {
	cfg := &StorageConfig{}

	// Try to load from config file first
	if fileCfg, err := loadConfig(); err == nil && fileCfg != nil {
		if fileCfg.TieredStorage.S3 != nil {
			cfg.S3Bucket = fileCfg.TieredStorage.S3.Bucket
			cfg.S3Region = fileCfg.TieredStorage.S3.Region
			cfg.S3Endpoint = fileCfg.TieredStorage.S3.Endpoint
			cfg.S3AccessKeyID = fileCfg.TieredStorage.S3.AccessKeyID
			cfg.S3SecretAccessKey = fileCfg.TieredStorage.S3.SecretAccessKey
			cfg.S3ForcePathStyle = fileCfg.TieredStorage.S3.ForcePathStyle
		}
		cfg.S3Prefix = fileCfg.Snapshot.Prefix
	}

	// Override with environment variables
	if v := os.Getenv(cobbleext.EnvS3Bucket); v != "" {
		cfg.S3Bucket = v
	}
	if v := os.Getenv(cobbleext.EnvS3Region); v != "" {
		cfg.S3Region = v
	}
	if v := os.Getenv(cobbleext.EnvS3Endpoint); v != "" {
		cfg.S3Endpoint = v
	}
	if v := os.Getenv(cobbleext.EnvS3AccessKeyID); v != "" {
		cfg.S3AccessKeyID = v
	}
	if v := os.Getenv(cobbleext.EnvS3SecretAccessKey); v != "" {
		cfg.S3SecretAccessKey = v
	}
	if os.Getenv(cobbleext.EnvS3ForcePathStyle) == "true" {
		cfg.S3ForcePathStyle = true
	}
	if v := os.Getenv(cobbleext.EnvSnapshotPrefix); v != "" {
		cfg.S3Prefix = v
	}

	// Override with command-line flags
	if storagePrefix != "" {
		cfg.S3Prefix = storagePrefix
	}
	if storageBucket != "" {
		cfg.S3Bucket = storageBucket
	}
	if storageEndpoint != "" {
		cfg.S3Endpoint = storageEndpoint
	}

	return cfg, nil
}

// Storage-related flags
var (
	storagePrefix   string
	storageBucket   string
	storageEndpoint string
)

func init() {
	// Add persistent flags for storage configuration
	rootCmd.PersistentFlags().StringVar(&storagePrefix, "prefix", "", "Storage prefix for snapshots")
	rootCmd.PersistentFlags().StringVar(&storageBucket, "bucket", "", "S3 bucket name")
	rootCmd.PersistentFlags().StringVar(&storageEndpoint, "endpoint", "", "S3 endpoint URL")
}
