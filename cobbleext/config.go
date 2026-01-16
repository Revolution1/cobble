// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cobbleext

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the external configuration for Cobble.
type Config struct {
	TieredStorage TieredStorageConfig `yaml:"tiered_storage" json:"tiered_storage"`
}

// TieredStorageConfig configures tiered storage.
type TieredStorageConfig struct {
	// Enabled enables tiered storage support.
	Enabled bool `yaml:"enabled" json:"enabled"`

	// Strategy determines which levels use remote storage.
	// Valid values: "none", "lower" (L5-L6 only), "all"
	Strategy TieringStrategy `yaml:"strategy" json:"strategy"`

	// S3 configures Amazon S3 or S3-compatible storage.
	S3 *S3Config `yaml:"s3,omitempty" json:"s3,omitempty"`

	// GCS configures Google Cloud Storage.
	GCS *GCSConfig `yaml:"gcs,omitempty" json:"gcs,omitempty"`

	// Cache configures local caching for remote objects.
	Cache CacheConfig `yaml:"cache" json:"cache"`
}

// TieringStrategy determines which levels use remote storage.
type TieringStrategy string

const (
	// TieringStrategyNone disables remote storage.
	TieringStrategyNone TieringStrategy = "none"

	// TieringStrategyLower uses remote storage for L5-L6 only (recommended).
	// This is the safest option as it only affects cold data.
	TieringStrategyLower TieringStrategy = "lower"

	// TieringStrategyAll uses remote storage for all levels.
	// Warning: This significantly increases latency for all operations.
	TieringStrategyAll TieringStrategy = "all"
)

// S3Config configures Amazon S3 or S3-compatible storage.
type S3Config struct {
	// Bucket is the S3 bucket name (required).
	Bucket string `yaml:"bucket" json:"bucket"`

	// Prefix is the key prefix for all objects (optional).
	// Example: "ethereum/mainnet/"
	Prefix string `yaml:"prefix" json:"prefix"`

	// Region is the AWS region (required for AWS S3).
	Region string `yaml:"region" json:"region"`

	// Endpoint is a custom S3 endpoint for S3-compatible storage (optional).
	// Example: "http://localhost:9000" for MinIO
	Endpoint string `yaml:"endpoint,omitempty" json:"endpoint,omitempty"`

	// AccessKeyID is the AWS access key ID (optional, prefer IAM roles).
	AccessKeyID string `yaml:"access_key_id,omitempty" json:"access_key_id,omitempty"`

	// SecretAccessKey is the AWS secret access key (optional, prefer IAM roles).
	SecretAccessKey string `yaml:"secret_access_key,omitempty" json:"secret_access_key,omitempty"`

	// ForcePathStyle forces path-style addressing (required for MinIO).
	ForcePathStyle bool `yaml:"force_path_style,omitempty" json:"force_path_style,omitempty"`

	// MaxRetries is the maximum number of retries for failed requests.
	MaxRetries int `yaml:"max_retries,omitempty" json:"max_retries,omitempty"`

	// ConnectTimeout is the connection timeout.
	ConnectTimeout time.Duration `yaml:"connect_timeout,omitempty" json:"connect_timeout,omitempty"`

	// ReadTimeout is the read timeout for individual requests.
	ReadTimeout time.Duration `yaml:"read_timeout,omitempty" json:"read_timeout,omitempty"`

	// WriteTimeout is the write timeout for individual requests.
	WriteTimeout time.Duration `yaml:"write_timeout,omitempty" json:"write_timeout,omitempty"`
}

// GCSConfig configures Google Cloud Storage.
type GCSConfig struct {
	// Bucket is the GCS bucket name (required).
	Bucket string `yaml:"bucket" json:"bucket"`

	// Prefix is the key prefix for all objects (optional).
	Prefix string `yaml:"prefix" json:"prefix"`

	// CredentialsFile is the path to a service account JSON file (optional).
	// If not specified, uses Application Default Credentials.
	CredentialsFile string `yaml:"credentials_file,omitempty" json:"credentials_file,omitempty"`

	// CredentialsJSON is the raw service account JSON (optional).
	// Takes precedence over CredentialsFile.
	CredentialsJSON string `yaml:"credentials_json,omitempty" json:"credentials_json,omitempty"`
}

// CacheConfig configures local caching for remote objects.
type CacheConfig struct {
	// Size is the cache size (e.g., "10GB", "500MB").
	// Default: "1GB"
	Size string `yaml:"size" json:"size"`

	// BlockSize is the block size for cache entries (e.g., "32KB").
	// Default: "32KB"
	BlockSize string `yaml:"block_size" json:"block_size"`

	// ShardingBlockSize is the sharding block size (e.g., "1MB").
	// Default: "1MB"
	ShardingBlockSize string `yaml:"sharding_block_size" json:"sharding_block_size"`
}

// DefaultConfigPaths are searched in order for config files.
var DefaultConfigPaths = []string{
	"./cobble.yaml",
	"./cobble.yml",
	"./.cobble.yaml",
	"./.cobble.yml",
}

func init() {
	// Add user home directory paths
	if home, err := os.UserHomeDir(); err == nil {
		DefaultConfigPaths = append(DefaultConfigPaths,
			filepath.Join(home, ".cobble", "config.yaml"),
			filepath.Join(home, ".cobble", "config.yml"),
		)
	}
	// Add system paths
	DefaultConfigPaths = append(DefaultConfigPaths,
		"/etc/cobble/config.yaml",
		"/etc/cobble/config.yml",
	)
}

// Load loads configuration from file and environment variables.
// Priority order (highest to lowest):
//  1. Environment variables
//  2. Config file specified by COBBLE_CONFIG
//  3. Config file found in DefaultConfigPaths
//  4. Default values
func Load() (*Config, error) {
	cfg := &Config{}

	// 1. Try to load from config file
	configPath := os.Getenv(EnvConfigFile)
	if configPath == "" {
		// Search default paths
		for _, path := range DefaultConfigPaths {
			if _, err := os.Stat(path); err == nil {
				configPath = path
				break
			}
		}
	}

	if configPath != "" {
		if err := loadFromFile(cfg, configPath); err != nil {
			return nil, fmt.Errorf("cobbleext: failed to load config from %s: %w", configPath, err)
		}
	}

	// 2. Override with environment variables
	LoadFromEnv(cfg)

	// 3. Apply defaults
	applyDefaults(cfg)

	// 4. Validate
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("cobbleext: invalid config: %w", err)
	}

	return cfg, nil
}

func loadFromFile(cfg *Config, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".yaml", ".yml":
		return yaml.Unmarshal(data, cfg)
	default:
		// Default to YAML
		return yaml.Unmarshal(data, cfg)
	}
}

func applyDefaults(cfg *Config) {
	if cfg.TieredStorage.Strategy == "" {
		cfg.TieredStorage.Strategy = TieringStrategyNone
	}
	if cfg.TieredStorage.Cache.Size == "" {
		cfg.TieredStorage.Cache.Size = "1GB"
	}
	if cfg.TieredStorage.Cache.BlockSize == "" {
		cfg.TieredStorage.Cache.BlockSize = "32KB"
	}
	if cfg.TieredStorage.Cache.ShardingBlockSize == "" {
		cfg.TieredStorage.Cache.ShardingBlockSize = "1MB"
	}

	// S3 defaults
	if cfg.TieredStorage.S3 != nil {
		if cfg.TieredStorage.S3.MaxRetries == 0 {
			cfg.TieredStorage.S3.MaxRetries = 3
		}
		if cfg.TieredStorage.S3.ConnectTimeout == 0 {
			cfg.TieredStorage.S3.ConnectTimeout = 10 * time.Second
		}
		if cfg.TieredStorage.S3.ReadTimeout == 0 {
			cfg.TieredStorage.S3.ReadTimeout = 30 * time.Second
		}
		if cfg.TieredStorage.S3.WriteTimeout == 0 {
			cfg.TieredStorage.S3.WriteTimeout = 60 * time.Second
		}
	}
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if !c.TieredStorage.Enabled {
		return nil
	}

	// Validate strategy
	switch c.TieredStorage.Strategy {
	case TieringStrategyNone, TieringStrategyLower, TieringStrategyAll:
		// Valid
	default:
		return fmt.Errorf("invalid tiering strategy: %s", c.TieredStorage.Strategy)
	}

	// Must have exactly one backend configured
	hasS3 := c.TieredStorage.S3 != nil && c.TieredStorage.S3.Bucket != ""
	hasGCS := c.TieredStorage.GCS != nil && c.TieredStorage.GCS.Bucket != ""

	if !hasS3 && !hasGCS {
		return fmt.Errorf("tiered storage enabled but no backend configured (set s3.bucket or gcs.bucket)")
	}
	if hasS3 && hasGCS {
		return fmt.Errorf("only one storage backend can be configured (s3 or gcs, not both)")
	}

	// Validate S3 config
	if hasS3 {
		if c.TieredStorage.S3.Region == "" && c.TieredStorage.S3.Endpoint == "" {
			return fmt.Errorf("s3.region is required when not using a custom endpoint")
		}
	}

	// Validate cache size
	if _, err := ParseSize(c.TieredStorage.Cache.Size); err != nil {
		return fmt.Errorf("invalid cache.size: %w", err)
	}
	if _, err := ParseSize(c.TieredStorage.Cache.BlockSize); err != nil {
		return fmt.Errorf("invalid cache.block_size: %w", err)
	}
	if _, err := ParseSize(c.TieredStorage.Cache.ShardingBlockSize); err != nil {
		return fmt.Errorf("invalid cache.sharding_block_size: %w", err)
	}

	return nil
}

// ParseSize parses a human-readable size string (e.g., "10GB", "500MB", "32KB").
func ParseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" {
		return 0, fmt.Errorf("empty size string")
	}

	// Match number and optional unit
	re := regexp.MustCompile(`^(\d+(?:\.\d+)?)\s*([KMGT]?B?)?$`)
	matches := re.FindStringSubmatch(s)
	if matches == nil {
		return 0, fmt.Errorf("invalid size format: %s", s)
	}

	value, err := strconv.ParseFloat(matches[1], 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number: %s", matches[1])
	}

	unit := matches[2]
	var multiplier float64 = 1
	switch unit {
	case "", "B":
		multiplier = 1
	case "K", "KB":
		multiplier = 1024
	case "M", "MB":
		multiplier = 1024 * 1024
	case "G", "GB":
		multiplier = 1024 * 1024 * 1024
	case "T", "TB":
		multiplier = 1024 * 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unknown unit: %s", unit)
	}

	return int64(value * multiplier), nil
}
