// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cobbleext

import (
	"os"
	"strings"
)

// Environment variable names for Cobble configuration.
const (
	// EnvConfigFile specifies the path to a YAML/JSON config file.
	EnvConfigFile = "COBBLE_CONFIG"

	// EnvTieredStorage enables tiered storage ("true", "1", "yes", "on").
	EnvTieredStorage = "COBBLE_TIERED_STORAGE"

	// EnvTieringStrategy sets the tiering strategy ("none", "lower", "all").
	EnvTieringStrategy = "COBBLE_TIERING_STRATEGY"

	// S3 configuration
	EnvS3Bucket          = "COBBLE_S3_BUCKET"
	EnvS3Prefix          = "COBBLE_S3_PREFIX"
	EnvS3Region          = "COBBLE_S3_REGION"
	EnvS3Endpoint        = "COBBLE_S3_ENDPOINT"
	EnvS3AccessKeyID     = "COBBLE_S3_ACCESS_KEY_ID"
	EnvS3SecretAccessKey = "COBBLE_S3_SECRET_ACCESS_KEY"
	EnvS3ForcePathStyle  = "COBBLE_S3_FORCE_PATH_STYLE"

	// GCS configuration
	EnvGCSBucket          = "COBBLE_GCS_BUCKET"
	EnvGCSPrefix          = "COBBLE_GCS_PREFIX"
	EnvGCSCredentialsFile = "COBBLE_GCS_CREDENTIALS_FILE"

	// Cache configuration
	EnvCacheSize             = "COBBLE_CACHE_SIZE"
	EnvCacheBlockSize        = "COBBLE_CACHE_BLOCK_SIZE"
	EnvCacheShardingBlockSize = "COBBLE_CACHE_SHARDING_BLOCK_SIZE"
)

// LoadFromEnv loads configuration from environment variables.
// Environment variables take precedence over config file values.
func LoadFromEnv(cfg *Config) {
	// Tiered storage enabled
	if v := os.Getenv(EnvTieredStorage); v != "" {
		cfg.TieredStorage.Enabled = parseBool(v)
	}

	// Tiering strategy
	if v := os.Getenv(EnvTieringStrategy); v != "" {
		cfg.TieredStorage.Strategy = TieringStrategy(strings.ToLower(v))
	}

	// S3 configuration
	loadS3FromEnv(cfg)

	// GCS configuration
	loadGCSFromEnv(cfg)

	// Cache configuration
	loadCacheFromEnv(cfg)
}

func loadS3FromEnv(cfg *Config) {
	bucket := os.Getenv(EnvS3Bucket)
	if bucket == "" {
		return
	}

	// Initialize S3 config if not exists
	if cfg.TieredStorage.S3 == nil {
		cfg.TieredStorage.S3 = &S3Config{}
	}

	cfg.TieredStorage.S3.Bucket = bucket

	if v := os.Getenv(EnvS3Prefix); v != "" {
		cfg.TieredStorage.S3.Prefix = v
	}
	if v := os.Getenv(EnvS3Region); v != "" {
		cfg.TieredStorage.S3.Region = v
	}
	if v := os.Getenv(EnvS3Endpoint); v != "" {
		cfg.TieredStorage.S3.Endpoint = v
	}
	if v := os.Getenv(EnvS3AccessKeyID); v != "" {
		cfg.TieredStorage.S3.AccessKeyID = v
	}
	if v := os.Getenv(EnvS3SecretAccessKey); v != "" {
		cfg.TieredStorage.S3.SecretAccessKey = v
	}
	if v := os.Getenv(EnvS3ForcePathStyle); v != "" {
		cfg.TieredStorage.S3.ForcePathStyle = parseBool(v)
	}

	// Auto-enable tiered storage if S3 bucket is set
	if !cfg.TieredStorage.Enabled {
		cfg.TieredStorage.Enabled = true
	}
	// Default to lower strategy if not set
	if cfg.TieredStorage.Strategy == "" || cfg.TieredStorage.Strategy == TieringStrategyNone {
		cfg.TieredStorage.Strategy = TieringStrategyLower
	}
}

func loadGCSFromEnv(cfg *Config) {
	bucket := os.Getenv(EnvGCSBucket)
	if bucket == "" {
		return
	}

	// Initialize GCS config if not exists
	if cfg.TieredStorage.GCS == nil {
		cfg.TieredStorage.GCS = &GCSConfig{}
	}

	cfg.TieredStorage.GCS.Bucket = bucket

	if v := os.Getenv(EnvGCSPrefix); v != "" {
		cfg.TieredStorage.GCS.Prefix = v
	}
	if v := os.Getenv(EnvGCSCredentialsFile); v != "" {
		cfg.TieredStorage.GCS.CredentialsFile = v
	}

	// Auto-enable tiered storage if GCS bucket is set
	if !cfg.TieredStorage.Enabled {
		cfg.TieredStorage.Enabled = true
	}
	// Default to lower strategy if not set
	if cfg.TieredStorage.Strategy == "" || cfg.TieredStorage.Strategy == TieringStrategyNone {
		cfg.TieredStorage.Strategy = TieringStrategyLower
	}
}

func loadCacheFromEnv(cfg *Config) {
	if v := os.Getenv(EnvCacheSize); v != "" {
		cfg.TieredStorage.Cache.Size = v
	}
	if v := os.Getenv(EnvCacheBlockSize); v != "" {
		cfg.TieredStorage.Cache.BlockSize = v
	}
	if v := os.Getenv(EnvCacheShardingBlockSize); v != "" {
		cfg.TieredStorage.Cache.ShardingBlockSize = v
	}
}

// parseBool parses a boolean string value.
func parseBool(s string) bool {
	s = strings.ToLower(strings.TrimSpace(s))
	return s == "true" || s == "1" || s == "yes" || s == "on" || s == "enabled"
}
