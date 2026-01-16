// Copyright 2024 The Cobble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cobbleext

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseSize(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
		wantErr  bool
	}{
		{"100", 100, false},
		{"100B", 100, false},
		{"1KB", 1024, false},
		{"1K", 1024, false},
		{"1MB", 1024 * 1024, false},
		{"1M", 1024 * 1024, false},
		{"1GB", 1024 * 1024 * 1024, false},
		{"1G", 1024 * 1024 * 1024, false},
		{"1TB", 1024 * 1024 * 1024 * 1024, false},
		{"10GB", 10 * 1024 * 1024 * 1024, false},
		{"500MB", 500 * 1024 * 1024, false},
		{"32KB", 32 * 1024, false},
		{"1.5GB", int64(1.5 * 1024 * 1024 * 1024), false},
		{"  10GB  ", 10 * 1024 * 1024 * 1024, false}, // whitespace
		{"", 0, true},                                // empty
		{"abc", 0, true},                             // invalid
		{"-1GB", 0, true},                            // negative
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseSize(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSize(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.expected {
				t.Errorf("ParseSize(%q) = %d, want %d", tt.input, got, tt.expected)
			}
		})
	}
}

func TestLoadFromEnv(t *testing.T) {
	// Clear all COBBLE_ env vars first
	for _, env := range os.Environ() {
		if len(env) > 7 && env[:7] == "COBBLE_" {
			key := env[:len(env)-len(env[len(env)-len(env)+1:])]
			for i, c := range env {
				if c == '=' {
					key = env[:i]
					break
				}
			}
			os.Unsetenv(key)
		}
	}

	tests := []struct {
		name     string
		envVars  map[string]string
		validate func(*testing.T, *Config)
	}{
		{
			name: "S3 bucket enables tiered storage",
			envVars: map[string]string{
				EnvS3Bucket: "my-bucket",
				EnvS3Region: "us-east-1",
			},
			validate: func(t *testing.T, cfg *Config) {
				if !cfg.TieredStorage.Enabled {
					t.Error("expected tiered storage to be enabled")
				}
				if cfg.TieredStorage.S3 == nil {
					t.Fatal("expected S3 config to be set")
				}
				if cfg.TieredStorage.S3.Bucket != "my-bucket" {
					t.Errorf("bucket = %q, want %q", cfg.TieredStorage.S3.Bucket, "my-bucket")
				}
				if cfg.TieredStorage.S3.Region != "us-east-1" {
					t.Errorf("region = %q, want %q", cfg.TieredStorage.S3.Region, "us-east-1")
				}
				if cfg.TieredStorage.Strategy != TieringStrategyLower {
					t.Errorf("strategy = %q, want %q", cfg.TieredStorage.Strategy, TieringStrategyLower)
				}
			},
		},
		{
			name: "explicit tiered storage config",
			envVars: map[string]string{
				EnvTieredStorage:   "true",
				EnvTieringStrategy: "all",
				EnvS3Bucket:        "test-bucket",
				EnvS3Region:        "eu-west-1",
				EnvS3Prefix:        "data/",
				EnvCacheSize:       "50GB",
			},
			validate: func(t *testing.T, cfg *Config) {
				if !cfg.TieredStorage.Enabled {
					t.Error("expected tiered storage to be enabled")
				}
				if cfg.TieredStorage.Strategy != TieringStrategyAll {
					t.Errorf("strategy = %q, want %q", cfg.TieredStorage.Strategy, TieringStrategyAll)
				}
				if cfg.TieredStorage.S3.Prefix != "data/" {
					t.Errorf("prefix = %q, want %q", cfg.TieredStorage.S3.Prefix, "data/")
				}
				if cfg.TieredStorage.Cache.Size != "50GB" {
					t.Errorf("cache size = %q, want %q", cfg.TieredStorage.Cache.Size, "50GB")
				}
			},
		},
		{
			name: "GCS bucket",
			envVars: map[string]string{
				EnvGCSBucket: "gcs-bucket",
				EnvGCSPrefix: "ethereum/",
			},
			validate: func(t *testing.T, cfg *Config) {
				if !cfg.TieredStorage.Enabled {
					t.Error("expected tiered storage to be enabled")
				}
				if cfg.TieredStorage.GCS == nil {
					t.Fatal("expected GCS config to be set")
				}
				if cfg.TieredStorage.GCS.Bucket != "gcs-bucket" {
					t.Errorf("bucket = %q, want %q", cfg.TieredStorage.GCS.Bucket, "gcs-bucket")
				}
				if cfg.TieredStorage.GCS.Prefix != "ethereum/" {
					t.Errorf("prefix = %q, want %q", cfg.TieredStorage.GCS.Prefix, "ethereum/")
				}
			},
		},
		{
			name: "S3 force path style",
			envVars: map[string]string{
				EnvS3Bucket:         "minio-bucket",
				EnvS3Endpoint:       "http://localhost:9000",
				EnvS3ForcePathStyle: "true",
			},
			validate: func(t *testing.T, cfg *Config) {
				if cfg.TieredStorage.S3 == nil {
					t.Fatal("expected S3 config to be set")
				}
				if cfg.TieredStorage.S3.Endpoint != "http://localhost:9000" {
					t.Errorf("endpoint = %q, want %q", cfg.TieredStorage.S3.Endpoint, "http://localhost:9000")
				}
				if !cfg.TieredStorage.S3.ForcePathStyle {
					t.Error("expected force_path_style to be true")
				}
			},
		},
		{
			name:    "no env vars",
			envVars: map[string]string{},
			validate: func(t *testing.T, cfg *Config) {
				if cfg.TieredStorage.Enabled {
					t.Error("expected tiered storage to be disabled")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear env vars
			os.Unsetenv(EnvTieredStorage)
			os.Unsetenv(EnvTieringStrategy)
			os.Unsetenv(EnvS3Bucket)
			os.Unsetenv(EnvS3Prefix)
			os.Unsetenv(EnvS3Region)
			os.Unsetenv(EnvS3Endpoint)
			os.Unsetenv(EnvS3ForcePathStyle)
			os.Unsetenv(EnvGCSBucket)
			os.Unsetenv(EnvGCSPrefix)
			os.Unsetenv(EnvCacheSize)

			// Set test env vars
			for k, v := range tt.envVars {
				os.Setenv(k, v)
			}

			cfg := &Config{}
			LoadFromEnv(cfg)
			applyDefaults(cfg)

			tt.validate(t, cfg)
		})
	}
}

func TestLoadFromFile(t *testing.T) {
	tmpDir := t.TempDir()

	yamlContent := `
tiered_storage:
  enabled: true
  strategy: lower
  s3:
    bucket: yaml-bucket
    prefix: yaml-prefix/
    region: ap-northeast-1
  cache:
    size: 20GB
    block_size: 64KB
`

	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := &Config{}
	if err := loadFromFile(cfg, configPath); err != nil {
		t.Fatalf("loadFromFile failed: %v", err)
	}

	if !cfg.TieredStorage.Enabled {
		t.Error("expected tiered storage to be enabled")
	}
	if cfg.TieredStorage.Strategy != TieringStrategyLower {
		t.Errorf("strategy = %q, want %q", cfg.TieredStorage.Strategy, TieringStrategyLower)
	}
	if cfg.TieredStorage.S3 == nil {
		t.Fatal("expected S3 config to be set")
	}
	if cfg.TieredStorage.S3.Bucket != "yaml-bucket" {
		t.Errorf("bucket = %q, want %q", cfg.TieredStorage.S3.Bucket, "yaml-bucket")
	}
	if cfg.TieredStorage.S3.Prefix != "yaml-prefix/" {
		t.Errorf("prefix = %q, want %q", cfg.TieredStorage.S3.Prefix, "yaml-prefix/")
	}
	if cfg.TieredStorage.Cache.Size != "20GB" {
		t.Errorf("cache size = %q, want %q", cfg.TieredStorage.Cache.Size, "20GB")
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "valid disabled config",
			cfg: Config{
				TieredStorage: TieredStorageConfig{
					Enabled: false,
				},
			},
			wantErr: false,
		},
		{
			name: "valid S3 config",
			cfg: Config{
				TieredStorage: TieredStorageConfig{
					Enabled:  true,
					Strategy: TieringStrategyLower,
					S3: &S3Config{
						Bucket: "test-bucket",
						Region: "us-east-1",
					},
					Cache: CacheConfig{
						Size:              "1GB",
						BlockSize:         "32KB",
						ShardingBlockSize: "1MB",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid S3 config with endpoint",
			cfg: Config{
				TieredStorage: TieredStorageConfig{
					Enabled:  true,
					Strategy: TieringStrategyLower,
					S3: &S3Config{
						Bucket:   "test-bucket",
						Endpoint: "http://localhost:9000",
					},
					Cache: CacheConfig{
						Size:              "1GB",
						BlockSize:         "32KB",
						ShardingBlockSize: "1MB",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "enabled but no backend",
			cfg: Config{
				TieredStorage: TieredStorageConfig{
					Enabled:  true,
					Strategy: TieringStrategyLower,
					Cache: CacheConfig{
						Size:              "1GB",
						BlockSize:         "32KB",
						ShardingBlockSize: "1MB",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "S3 without region or endpoint",
			cfg: Config{
				TieredStorage: TieredStorageConfig{
					Enabled:  true,
					Strategy: TieringStrategyLower,
					S3: &S3Config{
						Bucket: "test-bucket",
					},
					Cache: CacheConfig{
						Size:              "1GB",
						BlockSize:         "32KB",
						ShardingBlockSize: "1MB",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid cache size",
			cfg: Config{
				TieredStorage: TieredStorageConfig{
					Enabled:  true,
					Strategy: TieringStrategyLower,
					S3: &S3Config{
						Bucket: "test-bucket",
						Region: "us-east-1",
					},
					Cache: CacheConfig{
						Size:              "invalid",
						BlockSize:         "32KB",
						ShardingBlockSize: "1MB",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid strategy",
			cfg: Config{
				TieredStorage: TieredStorageConfig{
					Enabled:  true,
					Strategy: "invalid",
					S3: &S3Config{
						Bucket: "test-bucket",
						Region: "us-east-1",
					},
					Cache: CacheConfig{
						Size:              "1GB",
						BlockSize:         "32KB",
						ShardingBlockSize: "1MB",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "both S3 and GCS configured",
			cfg: Config{
				TieredStorage: TieredStorageConfig{
					Enabled:  true,
					Strategy: TieringStrategyLower,
					S3: &S3Config{
						Bucket: "s3-bucket",
						Region: "us-east-1",
					},
					GCS: &GCSConfig{
						Bucket: "gcs-bucket",
					},
					Cache: CacheConfig{
						Size:              "1GB",
						BlockSize:         "32KB",
						ShardingBlockSize: "1MB",
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEnvOverridesFile(t *testing.T) {
	tmpDir := t.TempDir()

	yamlContent := `
tiered_storage:
  enabled: true
  strategy: lower
  s3:
    bucket: file-bucket
    region: us-west-1
  cache:
    size: 10GB
`

	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
		t.Fatal(err)
	}

	// Set env vars that should override file
	os.Setenv(EnvConfigFile, configPath)
	os.Setenv(EnvS3Bucket, "env-bucket")
	os.Setenv(EnvCacheSize, "50GB")
	defer func() {
		os.Unsetenv(EnvConfigFile)
		os.Unsetenv(EnvS3Bucket)
		os.Unsetenv(EnvCacheSize)
	}()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Bucket should be overridden by env
	if cfg.TieredStorage.S3.Bucket != "env-bucket" {
		t.Errorf("bucket = %q, want %q (env should override file)", cfg.TieredStorage.S3.Bucket, "env-bucket")
	}

	// Region should come from file
	if cfg.TieredStorage.S3.Region != "us-west-1" {
		t.Errorf("region = %q, want %q (should come from file)", cfg.TieredStorage.S3.Region, "us-west-1")
	}

	// Cache size should be overridden by env
	if cfg.TieredStorage.Cache.Size != "50GB" {
		t.Errorf("cache size = %q, want %q (env should override file)", cfg.TieredStorage.Cache.Size, "50GB")
	}
}
