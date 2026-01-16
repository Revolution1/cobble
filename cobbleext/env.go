// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cobbleext

import (
	"os"
	"strconv"
	"strings"
	"time"
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
	EnvCacheSize              = "COBBLE_CACHE_SIZE"
	EnvCacheBlockSize         = "COBBLE_CACHE_BLOCK_SIZE"
	EnvCacheShardingBlockSize = "COBBLE_CACHE_SHARDING_BLOCK_SIZE"

	// Admin server configuration
	EnvAdminEnabled = "COBBLE_ADMIN_ENABLED"
	EnvAdminAddr    = "COBBLE_ADMIN_ADDR"
	EnvAdminToken   = "COBBLE_ADMIN_TOKEN"

	// Auto snapshot configuration
	EnvAutoSnapshotEnabled     = "COBBLE_AUTO_SNAPSHOT"
	EnvAutoSnapshotInterval    = "COBBLE_AUTO_SNAPSHOT_INTERVAL"
	EnvAutoSnapshotIncremental = "COBBLE_AUTO_SNAPSHOT_INCREMENTAL"
	EnvAutoSnapshotKeepLast    = "COBBLE_AUTO_SNAPSHOT_KEEP_LAST"

	// Snapshot configuration
	EnvSnapshotBucket          = "COBBLE_SNAPSHOT_BUCKET"
	EnvSnapshotPrefix          = "COBBLE_SNAPSHOT_PREFIX"
	EnvSnapshotUseTieredConfig = "COBBLE_SNAPSHOT_USE_TIERED_CONFIG"
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

	// Admin server configuration
	loadAdminFromEnv(cfg)

	// Auto snapshot configuration
	loadAutoSnapshotFromEnv(cfg)

	// Snapshot configuration
	loadSnapshotFromEnv(cfg)
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

func loadAdminFromEnv(cfg *Config) {
	if v := os.Getenv(EnvAdminEnabled); v != "" {
		cfg.Admin.Enabled = parseBool(v)
	}
	if v := os.Getenv(EnvAdminAddr); v != "" {
		cfg.Admin.Addr = v
	}
	if v := os.Getenv(EnvAdminToken); v != "" {
		cfg.Admin.Token = v
	}
}

func loadAutoSnapshotFromEnv(cfg *Config) {
	if v := os.Getenv(EnvAutoSnapshotEnabled); v != "" {
		cfg.AutoSnapshot.Enabled = parseBool(v)
	}
	if v := os.Getenv(EnvAutoSnapshotInterval); v != "" {
		cfg.AutoSnapshot.Interval = parseDuration(v)
	}
	if v := os.Getenv(EnvAutoSnapshotIncremental); v != "" {
		cfg.AutoSnapshot.Incremental = parseBool(v)
	}
	if v := os.Getenv(EnvAutoSnapshotKeepLast); v != "" {
		cfg.AutoSnapshot.KeepLast = parseInt(v)
	}
}

func loadSnapshotFromEnv(cfg *Config) {
	if v := os.Getenv(EnvSnapshotBucket); v != "" {
		cfg.Snapshot.Bucket = v
	}
	if v := os.Getenv(EnvSnapshotPrefix); v != "" {
		cfg.Snapshot.Prefix = v
	}
	if v := os.Getenv(EnvSnapshotUseTieredConfig); v != "" {
		cfg.Snapshot.UseTieredConfig = parseBool(v)
	}
}

// parseBool parses a boolean string value.
func parseBool(s string) bool {
	s = strings.ToLower(strings.TrimSpace(s))
	return s == "true" || s == "1" || s == "yes" || s == "on" || s == "enabled"
}

// parseDuration parses a duration string (e.g., "6h", "30m", "1d").
func parseDuration(s string) time.Duration {
	s = strings.TrimSpace(s)
	// Handle day suffix which time.ParseDuration doesn't support
	if strings.HasSuffix(s, "d") {
		s = strings.TrimSuffix(s, "d")
		if days, err := strconv.Atoi(s); err == nil {
			return time.Duration(days) * 24 * time.Hour
		}
	}
	if d, err := time.ParseDuration(s); err == nil {
		return d
	}
	return 0
}

// parseInt parses an integer string.
func parseInt(s string) int {
	if v, err := strconv.Atoi(strings.TrimSpace(s)); err == nil {
		return v
	}
	return 0
}
