// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cobbleext

import (
	"fmt"

	"github.com/cockroachdb/pebble/objstorage/remote"
)

// OptionsAdapter is an interface that abstracts pebble.Options to avoid import cycles.
// The pebble package provides a concrete implementation.
type OptionsAdapter interface {
	// SetRemoteStorage configures remote storage.
	SetRemoteStorage(factory remote.StorageFactory, locator remote.Locator, strategy remote.CreateOnSharedStrategy)
	// SetSecondaryCacheSize sets the secondary cache size.
	SetSecondaryCacheSize(size int64)
	// Logf logs a message if a logger is configured.
	Logf(format string, args ...interface{})
}

// ApplyConfig loads external configuration and applies it to the options adapter.
// This is the main entry point for external configuration.
func ApplyConfig(opts OptionsAdapter) error {
	cfg, err := Load()
	if err != nil {
		return err
	}

	if !cfg.TieredStorage.Enabled {
		return nil
	}

	return applyTieredStorage(opts, cfg)
}

func applyTieredStorage(opts OptionsAdapter, cfg *Config) error {
	// Create storage factory based on configuration
	factory, locator, err := createStorageFactory(cfg)
	if err != nil {
		return fmt.Errorf("cobbleext: failed to create storage factory: %w", err)
	}

	// Determine strategy
	var strategy remote.CreateOnSharedStrategy
	switch cfg.TieredStorage.Strategy {
	case TieringStrategyLower:
		strategy = remote.CreateOnSharedLower
	case TieringStrategyAll:
		strategy = remote.CreateOnSharedAll
	default:
		strategy = remote.CreateOnSharedNone
	}

	// Apply remote storage settings
	opts.SetRemoteStorage(factory, locator, strategy)

	// Apply cache settings
	cacheSize, err := ParseSize(cfg.TieredStorage.Cache.Size)
	if err != nil {
		return fmt.Errorf("cobbleext: invalid cache size: %w", err)
	}
	opts.SetSecondaryCacheSize(cacheSize)

	// Log configuration
	backend := "unknown"
	if cfg.TieredStorage.S3 != nil {
		backend = fmt.Sprintf("s3://%s/%s", cfg.TieredStorage.S3.Bucket, cfg.TieredStorage.S3.Prefix)
	} else if cfg.TieredStorage.GCS != nil {
		backend = fmt.Sprintf("gs://%s/%s", cfg.TieredStorage.GCS.Bucket, cfg.TieredStorage.GCS.Prefix)
	}
	opts.Logf("cobbleext: tiered storage enabled, backend=%s, strategy=%s, cache=%s",
		backend, cfg.TieredStorage.Strategy, cfg.TieredStorage.Cache.Size)

	return nil
}

// createStorageFactory creates a remote.StorageFactory based on the configuration.
// This function is implemented differently depending on build tags.
func createStorageFactory(cfg *Config) (remote.StorageFactory, remote.Locator, error) {
	if cfg.TieredStorage.S3 != nil {
		return createS3Factory(cfg.TieredStorage.S3)
	}
	if cfg.TieredStorage.GCS != nil {
		return createGCSFactory(cfg.TieredStorage.GCS)
	}
	return nil, "", fmt.Errorf("no storage backend configured")
}
