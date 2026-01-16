# Import Path Strategy and Code Skeleton

## 1. Import Path Replacement

### 1.1 The Problem

Pebble uses `github.com/cockroachdb/pebble` as its import path throughout the codebase (2067 occurrences in 518 files). We have two options:

### 1.2 Option A: Keep Original Import Path (Recommended)

**Strategy**: Don't change import paths in the codebase. Use `go.mod` replace directive in consumers.

```go
// go.mod in go-ethereum fork
module github.com/ethereum/go-ethereum

require (
    github.com/cockroachdb/pebble v1.1.0
)

replace github.com/cockroachdb/pebble => github.com/Revolution1/cobble v0.0.0-20240101000000-abcdef123456
```

**Pros**:

- Zero changes to import paths in cobble
- Easier upstream sync (no import path conflicts)
- Standard Go practice for forks

**Cons**:

- Consumers must add replace directive
- Can't publish to pkg.go.dev under a different path

### 1.3 Option B: Change Import Path to cobble

**Strategy**: Replace all imports with `github.com/Revolution1/cobble`.

```bash
# One-time migration
find . -name "*.go" -exec sed -i 's|github.com/cockroachdb/pebble|github.com/Revolution1/cobble|g' {} \;
```

**Pros**:

- Can publish as independent package
- No replace directive needed in consumers

**Cons**:

- 518 files to modify
- Every upstream sync will have import path conflicts
- Maintenance burden

### 1.4 Recommendation

**Use Option A**. The replace directive is standard Go practice and keeps upstream sync clean.

## 2. Module Path Configuration

### 2.1 go.mod Changes

```go
// go.mod
module github.com/cockroachdb/pebble  // Keep original module path

go 1.22

require (
    // Existing dependencies...

    // NEW: Cloud storage dependencies (optional via build tags)
    github.com/aws/aws-sdk-go-v2 v1.24.0
    github.com/aws/aws-sdk-go-v2/config v1.26.0
    github.com/aws/aws-sdk-go-v2/service/s3 v1.47.0
    cloud.google.com/go/storage v1.36.0

    // NEW: Config parsing
    gopkg.in/yaml.v3 v3.0.1
)
```

### 2.2 Build Tags for Optional Dependencies

To avoid pulling cloud SDK dependencies for users who don't need them:

```go
// cobbleext/s3/storage.go
//go:build s3 || cloud || all

// cobbleext/gcs/storage.go
//go:build gcs || cloud || all

// cobbleext/noop.go (default, no cloud support)
//go:build !s3 && !gcs && !cloud && !all
```

## 3. Code Skeleton

### 3.1 Directory Structure

```
cobble/
├── cobbleext/                    # NEW: All extension code
│   ├── config.go                 # Config types and loading
│   ├── config_test.go
│   ├── env.go                    # Environment variable parsing
│   ├── env_test.go
│   ├── apply.go                  # Apply config to Options
│   ├── apply_test.go
│   ├── noop.go                   # No-op implementation (default)
│   ├── s3/                       # S3 backend (build tag: s3)
│   │   ├── storage.go
│   │   ├── storage_test.go
│   │   ├── reader.go
│   │   └── writer.go
│   ├── gcs/                      # GCS backend (build tag: gcs)
│   │   ├── storage.go
│   │   ├── storage_test.go
│   │   ├── reader.go
│   │   └── writer.go
│   └── testutil/
│       ├── minio.go              # MinIO test container
│       └── gcs_emulator.go       # GCS emulator
├── open.go                       # MODIFIED: Add config hook
└── ... (other files unchanged)
```

### 3.2 Core Types

```go
// cobbleext/config.go
package cobbleext

import (
    "time"
)

// Config represents the external configuration for Cobble.
type Config struct {
    TieredStorage TieredStorageConfig `yaml:"tiered_storage" json:"tiered_storage"`
}

// TieredStorageConfig configures tiered storage.
type TieredStorageConfig struct {
    Enabled  bool              `yaml:"enabled" json:"enabled"`
    Strategy TieringStrategy   `yaml:"strategy" json:"strategy"`
    S3       *S3Config         `yaml:"s3,omitempty" json:"s3,omitempty"`
    GCS      *GCSConfig        `yaml:"gcs,omitempty" json:"gcs,omitempty"`
    Cache    CacheConfig       `yaml:"cache" json:"cache"`
}

// TieringStrategy determines which levels use remote storage.
type TieringStrategy string

const (
    TieringStrategyNone  TieringStrategy = "none"  // No remote storage
    TieringStrategyLower TieringStrategy = "lower" // L5-L6 only (recommended)
    TieringStrategyAll   TieringStrategy = "all"   // All levels
)

// S3Config configures S3 storage.
type S3Config struct {
    Bucket          string `yaml:"bucket" json:"bucket"`
    Prefix          string `yaml:"prefix" json:"prefix"`
    Region          string `yaml:"region" json:"region"`
    Endpoint        string `yaml:"endpoint,omitempty" json:"endpoint,omitempty"`
    AccessKeyID     string `yaml:"access_key_id,omitempty" json:"access_key_id,omitempty"`
    SecretAccessKey string `yaml:"secret_access_key,omitempty" json:"secret_access_key,omitempty"`

    // Advanced options
    MaxRetries      int           `yaml:"max_retries" json:"max_retries"`
    ConnectTimeout  time.Duration `yaml:"connect_timeout" json:"connect_timeout"`
    ReadTimeout     time.Duration `yaml:"read_timeout" json:"read_timeout"`
    WriteTimeout    time.Duration `yaml:"write_timeout" json:"write_timeout"`
}

// GCSConfig configures Google Cloud Storage.
type GCSConfig struct {
    Bucket          string `yaml:"bucket" json:"bucket"`
    Prefix          string `yaml:"prefix" json:"prefix"`
    CredentialsFile string `yaml:"credentials_file,omitempty" json:"credentials_file,omitempty"`
    CredentialsJSON string `yaml:"credentials_json,omitempty" json:"credentials_json,omitempty"`
}

// CacheConfig configures local caching for remote objects.
type CacheConfig struct {
    Size              string `yaml:"size" json:"size"`                               // e.g., "10GB"
    BlockSize         string `yaml:"block_size" json:"block_size"`                   // e.g., "32KB"
    ShardingBlockSize string `yaml:"sharding_block_size" json:"sharding_block_size"` // e.g., "1MB"
}
```

### 3.3 Environment Variable Handling

```go
// cobbleext/env.go
package cobbleext

import (
    "os"
    "strconv"
    "strings"
)

const (
    EnvConfigFile       = "COBBLE_CONFIG"
    EnvTieredStorage    = "COBBLE_TIERED_STORAGE"
    EnvTieringStrategy  = "COBBLE_TIERING_STRATEGY"
    EnvS3Bucket         = "COBBLE_S3_BUCKET"
    EnvS3Prefix         = "COBBLE_S3_PREFIX"
    EnvS3Region         = "COBBLE_S3_REGION"
    EnvS3Endpoint       = "COBBLE_S3_ENDPOINT"
    EnvS3AccessKeyID    = "COBBLE_S3_ACCESS_KEY_ID"
    EnvS3SecretAccessKey = "COBBLE_S3_SECRET_ACCESS_KEY"
    EnvGCSBucket        = "COBBLE_GCS_BUCKET"
    EnvGCSPrefix        = "COBBLE_GCS_PREFIX"
    EnvGCSCredentials   = "COBBLE_GCS_CREDENTIALS"
    EnvCacheSize        = "COBBLE_CACHE_SIZE"
)

// LoadFromEnv loads configuration from environment variables.
// Environment variables take precedence over config file values.
func LoadFromEnv(cfg *Config) {
    if v := os.Getenv(EnvTieredStorage); v != "" {
        cfg.TieredStorage.Enabled = parseBool(v)
    }

    if v := os.Getenv(EnvTieringStrategy); v != "" {
        cfg.TieredStorage.Strategy = TieringStrategy(strings.ToLower(v))
    }

    // S3 configuration
    if bucket := os.Getenv(EnvS3Bucket); bucket != "" {
        if cfg.TieredStorage.S3 == nil {
            cfg.TieredStorage.S3 = &S3Config{}
        }
        cfg.TieredStorage.S3.Bucket = bucket
    }
    if v := os.Getenv(EnvS3Prefix); v != "" && cfg.TieredStorage.S3 != nil {
        cfg.TieredStorage.S3.Prefix = v
    }
    if v := os.Getenv(EnvS3Region); v != "" && cfg.TieredStorage.S3 != nil {
        cfg.TieredStorage.S3.Region = v
    }
    if v := os.Getenv(EnvS3Endpoint); v != "" && cfg.TieredStorage.S3 != nil {
        cfg.TieredStorage.S3.Endpoint = v
    }
    if v := os.Getenv(EnvS3AccessKeyID); v != "" && cfg.TieredStorage.S3 != nil {
        cfg.TieredStorage.S3.AccessKeyID = v
    }
    if v := os.Getenv(EnvS3SecretAccessKey); v != "" && cfg.TieredStorage.S3 != nil {
        cfg.TieredStorage.S3.SecretAccessKey = v
    }

    // GCS configuration
    if bucket := os.Getenv(EnvGCSBucket); bucket != "" {
        if cfg.TieredStorage.GCS == nil {
            cfg.TieredStorage.GCS = &GCSConfig{}
        }
        cfg.TieredStorage.GCS.Bucket = bucket
    }
    if v := os.Getenv(EnvGCSPrefix); v != "" && cfg.TieredStorage.GCS != nil {
        cfg.TieredStorage.GCS.Prefix = v
    }
    if v := os.Getenv(EnvGCSCredentials); v != "" && cfg.TieredStorage.GCS != nil {
        cfg.TieredStorage.GCS.CredentialsFile = v
    }

    // Cache configuration
    if v := os.Getenv(EnvCacheSize); v != "" {
        cfg.TieredStorage.Cache.Size = v
    }
}

func parseBool(s string) bool {
    s = strings.ToLower(s)
    return s == "true" || s == "1" || s == "yes" || s == "on"
}

func parseInt64(s string) int64 {
    v, _ := strconv.ParseInt(s, 10, 64)
    return v
}
```

### 3.4 Config Loading

```go
// cobbleext/config.go (continued)

import (
    "os"
    "path/filepath"

    "gopkg.in/yaml.v3"
)

// DefaultConfigPaths are searched in order for config files.
var DefaultConfigPaths = []string{
    "./cobble.yaml",
    "./cobble.yml",
    os.ExpandEnv("$HOME/.cobble/config.yaml"),
    "/etc/cobble/config.yaml",
}

// Load loads configuration from file and environment.
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
            return nil, err
        }
    }

    // 2. Override with environment variables
    LoadFromEnv(cfg)

    // 3. Apply defaults
    applyDefaults(cfg)

    return cfg, nil
}

func loadFromFile(cfg *Config, path string) error {
    data, err := os.ReadFile(path)
    if err != nil {
        return err
    }

    ext := filepath.Ext(path)
    switch ext {
    case ".yaml", ".yml":
        return yaml.Unmarshal(data, cfg)
    default:
        return yaml.Unmarshal(data, cfg) // Default to YAML
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
}
```

### 3.5 Apply Config to Options

```go
// cobbleext/apply.go
package cobbleext

import (
    "fmt"

    "github.com/cockroachdb/pebble"
    "github.com/cockroachdb/pebble/objstorage/remote"
)

// ApplyExternalConfig loads external configuration and applies it to Options.
// This is called from pebble.Open() before EnsureDefaults().
func ApplyExternalConfig(opts *pebble.Options) error {
    cfg, err := Load()
    if err != nil {
        return fmt.Errorf("cobbleext: failed to load config: %w", err)
    }

    if !cfg.TieredStorage.Enabled {
        return nil // No tiered storage configured
    }

    // Create storage factory based on configuration
    factory, locator, err := createStorageFactory(cfg)
    if err != nil {
        return fmt.Errorf("cobbleext: failed to create storage factory: %w", err)
    }

    // Apply to options
    opts.Experimental.RemoteStorage = factory
    opts.Experimental.CreateOnSharedLocator = locator

    switch cfg.TieredStorage.Strategy {
    case TieringStrategyLower:
        opts.Experimental.CreateOnShared = remote.CreateOnSharedLower
    case TieringStrategyAll:
        opts.Experimental.CreateOnShared = remote.CreateOnSharedAll
    default:
        opts.Experimental.CreateOnShared = remote.CreateOnSharedNone
    }

    // Apply cache settings
    cacheSize, err := parseSize(cfg.TieredStorage.Cache.Size)
    if err != nil {
        return fmt.Errorf("cobbleext: invalid cache size: %w", err)
    }
    opts.Experimental.SecondaryCacheSizeBytes = cacheSize

    return nil
}

func createStorageFactory(cfg *Config) (remote.StorageFactory, remote.Locator, error) {
    if cfg.TieredStorage.S3 != nil {
        return createS3Factory(cfg.TieredStorage.S3)
    }
    if cfg.TieredStorage.GCS != nil {
        return createGCSFactory(cfg.TieredStorage.GCS)
    }
    return nil, "", fmt.Errorf("no storage backend configured")
}

// parseSize parses human-readable size strings like "10GB", "500MB".
func parseSize(s string) (int64, error) {
    // Implementation...
    return 0, nil
}
```

### 3.6 No-op Implementation (Default Build)

```go
// cobbleext/noop.go
//go:build !s3 && !gcs && !cloud && !all

package cobbleext

import (
    "github.com/cockroachdb/pebble"
)

// ApplyExternalConfig is a no-op when cloud storage is not compiled in.
func ApplyExternalConfig(opts *pebble.Options) error {
    // Check if config requests cloud storage
    cfg, err := Load()
    if err != nil {
        return nil // Ignore config errors in no-op mode
    }

    if cfg.TieredStorage.Enabled {
        // Log warning that cloud storage is requested but not compiled in
        if opts.Logger != nil {
            opts.Logger.Infof("cobbleext: tiered storage requested but not compiled in (build with -tags s3 or -tags gcs)")
        }
    }

    return nil
}

func createS3Factory(cfg *S3Config) (remote.StorageFactory, remote.Locator, error) {
    return nil, "", fmt.Errorf("S3 support not compiled in (build with -tags s3)")
}

func createGCSFactory(cfg *GCSConfig) (remote.StorageFactory, remote.Locator, error) {
    return nil, "", fmt.Errorf("GCS support not compiled in (build with -tags gcs)")
}
```

### 3.7 S3 Storage Implementation

```go
// cobbleext/s3/storage.go
//go:build s3 || cloud || all

package s3

import (
    "context"
    "fmt"
    "io"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/credentials"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/cockroachdb/pebble/objstorage/remote"
)

// Storage implements remote.Storage for Amazon S3.
type Storage struct {
    client *s3.Client
    bucket string
    prefix string
}

var _ remote.Storage = (*Storage)(nil)

// Config holds S3 storage configuration.
type Config struct {
    Bucket          string
    Prefix          string
    Region          string
    Endpoint        string
    AccessKeyID     string
    SecretAccessKey string
}

// NewStorage creates a new S3 storage backend.
func NewStorage(cfg Config) (*Storage, error) {
    ctx := context.Background()

    // Build AWS config
    var opts []func(*config.LoadOptions) error

    if cfg.Region != "" {
        opts = append(opts, config.WithRegion(cfg.Region))
    }

    if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
        opts = append(opts, config.WithCredentialsProvider(
            credentials.NewStaticCredentialsProvider(
                cfg.AccessKeyID,
                cfg.SecretAccessKey,
                "",
            ),
        ))
    }

    awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
    if err != nil {
        return nil, fmt.Errorf("failed to load AWS config: %w", err)
    }

    // Create S3 client
    clientOpts := []func(*s3.Options){}
    if cfg.Endpoint != "" {
        clientOpts = append(clientOpts, func(o *s3.Options) {
            o.BaseEndpoint = aws.String(cfg.Endpoint)
            o.UsePathStyle = true // Required for MinIO
        })
    }

    client := s3.NewFromConfig(awsCfg, clientOpts...)

    return &Storage{
        client: client,
        bucket: cfg.Bucket,
        prefix: cfg.Prefix,
    }, nil
}

// Close implements remote.Storage.
func (s *Storage) Close() error {
    return nil // S3 client doesn't need explicit close
}

// ReadObject implements remote.Storage.
func (s *Storage) ReadObject(ctx context.Context, objName string) (remote.ObjectReader, int64, error) {
    key := s.prefix + objName

    // Get object metadata first
    headResp, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
        Bucket: aws.String(s.bucket),
        Key:    aws.String(key),
    })
    if err != nil {
        return nil, 0, err
    }

    size := *headResp.ContentLength

    return &objectReader{
        client: s.client,
        bucket: s.bucket,
        key:    key,
        size:   size,
    }, size, nil
}

// CreateObject implements remote.Storage.
func (s *Storage) CreateObject(objName string) (io.WriteCloser, error) {
    key := s.prefix + objName
    return newObjectWriter(s.client, s.bucket, key), nil
}

// List implements remote.Storage.
func (s *Storage) List(prefix, delimiter string) ([]string, error) {
    ctx := context.Background()
    fullPrefix := s.prefix + prefix

    input := &s3.ListObjectsV2Input{
        Bucket: aws.String(s.bucket),
        Prefix: aws.String(fullPrefix),
    }
    if delimiter != "" {
        input.Delimiter = aws.String(delimiter)
    }

    var results []string
    paginator := s3.NewListObjectsV2Paginator(s.client, input)

    for paginator.HasMorePages() {
        page, err := paginator.NextPage(ctx)
        if err != nil {
            return nil, err
        }

        for _, obj := range page.Contents {
            // Remove the prefix from the key
            name := (*obj.Key)[len(s.prefix):]
            if prefix != "" {
                name = name[len(prefix):]
            }
            results = append(results, name)
        }

        // Handle common prefixes (directories) when using delimiter
        for _, cp := range page.CommonPrefixes {
            name := (*cp.Prefix)[len(s.prefix):]
            if prefix != "" {
                name = name[len(prefix):]
            }
            results = append(results, name)
        }
    }

    return results, nil
}

// Delete implements remote.Storage.
func (s *Storage) Delete(objName string) error {
    ctx := context.Background()
    key := s.prefix + objName

    _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
        Bucket: aws.String(s.bucket),
        Key:    aws.String(key),
    })
    return err
}

// Size implements remote.Storage.
func (s *Storage) Size(objName string) (int64, error) {
    ctx := context.Background()
    key := s.prefix + objName

    resp, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
        Bucket: aws.String(s.bucket),
        Key:    aws.String(key),
    })
    if err != nil {
        return 0, err
    }

    return *resp.ContentLength, nil
}

// IsNotExistError implements remote.Storage.
func (s *Storage) IsNotExistError(err error) bool {
    // Check for S3 not found errors
    var nsk *types.NoSuchKey
    var nsb *types.NotFound
    return errors.As(err, &nsk) || errors.As(err, &nsb)
}
```

### 3.8 Hook in open.go

```go
// open.go - minimal change

import (
    // ... existing imports
    "github.com/cockroachdb/pebble/cobbleext"
)

func Open(dirname string, opts *Options) (db *DB, err error) {
    opts = opts.Clone()

    // NEW: Apply external configuration (environment variables, config file)
    // This allows enabling tiered storage without modifying calling code.
    if err := cobbleext.ApplyExternalConfig(opts); err != nil {
        return nil, err
    }

    opts.EnsureDefaults()
    // ... rest unchanged
}
```

## 4. Geth Integration Example

### 4.1 go.mod in go-ethereum fork

```go
module github.com/ethereum/go-ethereum

go 1.22

require (
    github.com/cockroachdb/pebble v1.1.0
    // ... other dependencies
)

// Use cobble instead of pebble
replace github.com/cockroachdb/pebble => github.com/Revolution1/cobble v0.1.0
```

### 4.2 Deployment Example

```bash
# Build geth with S3 support
cd go-ethereum
go build -tags s3 -o geth ./cmd/geth

# Run geth with S3 tiered storage
export COBBLE_TIERED_STORAGE=true
export COBBLE_TIERING_STRATEGY=lower
export COBBLE_S3_BUCKET=my-ethereum-data
export COBBLE_S3_REGION=us-east-1
export COBBLE_CACHE_SIZE=50GB

./geth --datadir /data/ethereum --state.scheme path
```

### 4.3 Docker Example

```dockerfile
FROM golang:1.22 AS builder

WORKDIR /app
COPY go-ethereum /app

# Build with S3 support
RUN go build -tags s3 -o geth ./cmd/geth

FROM ubuntu:22.04

COPY --from=builder /app/geth /usr/local/bin/

# Configuration via environment
ENV COBBLE_TIERED_STORAGE=true
ENV COBBLE_TIERING_STRATEGY=lower
ENV COBBLE_S3_BUCKET=my-bucket
ENV COBBLE_S3_REGION=us-east-1
ENV COBBLE_CACHE_SIZE=50GB

ENTRYPOINT ["geth"]
```

## 5. Testing Commands

```bash
# Run all tests without cloud support
go test ./...

# Run tests with S3 support (requires MinIO or real S3)
go test -tags s3 ./...

# Run tests with both S3 and GCS
go test -tags "s3 gcs" ./...

# Run only cobbleext tests
go test -tags "s3 gcs" ./cobbleext/...

# Run integration tests with MinIO
docker run -d -p 9000:9000 minio/minio server /data
COBBLE_S3_ENDPOINT=http://localhost:9000 \
COBBLE_S3_ACCESS_KEY_ID=minioadmin \
COBBLE_S3_SECRET_ACCESS_KEY=minioadmin \
go test -tags s3 ./cobbleext/s3/...
```
