# Cobble Tiered Storage Design Document

This document outlines the design for adding S3/GCS tiered storage support to Cobble (Pebble fork) while maintaining zero intrusion to geth and easy upstream sync.

## 1. Goals

1. **Zero intrusion to geth**: Enable tiered storage via environment variables and config files only
2. **Easy upstream sync**: Minimize changes to core Pebble files
3. **Production ready**: Support S3 and GCS with proper caching and error handling

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         Geth / op-geth                           │
│                    (unchanged, uses go mod replace)              │
├─────────────────────────────────────────────────────────────────┤
│                           Cobble                                 │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    Modified Files (minimal)                  ││
│  │  - open.go: Add env config loading hook                     ││
│  │  - options.go: Add env config loading method                ││
│  └─────────────────────────────────────────────────────────────┘│
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    New Files (isolated)                      ││
│  │  cobbleext/                                                  ││
│  │  ├── config.go          # Config file parsing               ││
│  │  ├── env.go             # Environment variable handling     ││
│  │  ├── s3/                                                    ││
│  │  │   └── storage.go     # S3 remote.Storage impl           ││
│  │  └── gcs/                                                   ││
│  │      └── storage.go     # GCS remote.Storage impl          ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

## 3. Environment Variable Configuration

### 3.1 Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `COBBLE_CONFIG` | Path to YAML/JSON config file | `/etc/cobble/config.yaml` |
| `COBBLE_TIERED_STORAGE` | Enable tiered storage | `true` |
| `COBBLE_S3_BUCKET` | S3 bucket name | `my-pebble-data` |
| `COBBLE_S3_PREFIX` | S3 key prefix | `ethereum/mainnet/` |
| `COBBLE_S3_REGION` | AWS region | `us-east-1` |
| `COBBLE_S3_ENDPOINT` | Custom S3 endpoint (for MinIO) | `http://localhost:9000` |
| `COBBLE_GCS_BUCKET` | GCS bucket name | `my-pebble-data` |
| `COBBLE_GCS_PREFIX` | GCS key prefix | `ethereum/mainnet/` |
| `COBBLE_CACHE_SIZE` | Local cache size for remote objects | `10GB` |
| `COBBLE_TIERING_STRATEGY` | Which levels use remote storage | `lower` (L5-L6) or `all` |

### 3.2 Config File Format (YAML)

```yaml
# /etc/cobble/config.yaml
tiered_storage:
  enabled: true
  strategy: lower  # none, lower, all

  # S3 configuration
  s3:
    bucket: my-pebble-data
    prefix: ethereum/mainnet/
    region: us-east-1
    # Optional: custom endpoint for S3-compatible storage
    endpoint: ""
    # Optional: credentials (prefer IAM roles or env vars)
    access_key_id: ""
    secret_access_key: ""

  # OR GCS configuration
  gcs:
    bucket: my-pebble-data
    prefix: ethereum/mainnet/
    # Optional: path to service account JSON
    credentials_file: ""

  # Local cache for remote objects
  cache:
    size: 10GB
    block_size: 32KB
    sharding_block_size: 1MB
```

### 3.3 Priority Order

1. Environment variables (highest priority)
2. Config file specified by `COBBLE_CONFIG`
3. Default config file at `~/.cobble/config.yaml` or `/etc/cobble/config.yaml`
4. Code defaults (no tiered storage)

## 4. Code Organization

### 4.1 New Package: `cobbleext/`

All new code lives in `cobbleext/` directory to minimize merge conflicts:

```
cobbleext/
├── config.go           # Config loading and parsing
├── env.go              # Environment variable handling
├── tiered.go           # Tiered storage setup logic
├── s3/
│   ├── storage.go      # remote.Storage implementation for S3
│   ├── storage_test.go
│   └── reader.go       # ObjectReader implementation
├── gcs/
│   ├── storage.go      # remote.Storage implementation for GCS
│   ├── storage_test.go
│   └── reader.go       # ObjectReader implementation
└── testutil/
    └── minio.go        # MinIO test helpers
```

### 4.2 Minimal Changes to Core Files

**open.go** (add ~5 lines):

```go
func Open(dirname string, opts *Options) (db *DB, err error) {
    opts = opts.Clone()

    // NEW: Apply environment/config file settings
    if err := cobbleext.ApplyExternalConfig(opts); err != nil {
        return nil, err
    }

    opts.EnsureDefaults()
    // ... rest unchanged
}
```

**options.go** (add import only):

```go
import (
    // ... existing imports
    "github.com/cockroachdb/pebble/cobbleext"  // NEW
)
```

### 4.3 Build Tags for Optional Dependencies

Use build tags to make S3/GCS dependencies optional:

```go
// cobbleext/s3/storage.go
//go:build s3

package s3

import (
    "github.com/aws/aws-sdk-go-v2/service/s3"
    // ...
)
```

Build commands:

```bash
# Build with S3 support only
go build -tags s3

# Build with GCS support only
go build -tags gcs

# Build with both
go build -tags "s3 gcs"

# Build without any cloud support (default, for compatibility)
go build
```

## 5. Upstream Sync Strategy

### 5.1 Branch Strategy

```
main (cobble)
├── upstream/main          # Mirror of cockroachdb/pebble main
└── feature branches       # Development branches
```

### 5.2 Sync Workflow

```bash
# 1. Fetch upstream changes
git fetch upstream

# 2. Create a sync branch
git checkout -b sync/upstream-$(date +%Y%m%d) main

# 3. Merge upstream
git merge upstream/main

# 4. Resolve conflicts (should be minimal due to isolated changes)
# Conflicts will mainly be in:
#   - open.go (our hook addition)
#   - go.mod (our new dependencies)

# 5. Run tests
go test -tags "s3 gcs" ./...

# 6. Create PR to main
```

### 5.3 Files Modified in Core (Conflict Risk Assessment)

| File | Change | Conflict Risk | Resolution |
|------|--------|---------------|------------|
| `open.go` | Add 5-line hook after Clone() | **Low** | Simple 3-way merge |
| `go.mod` | Add cloud SDK dependencies | **Medium** | Manual review |
| `go.sum` | Auto-generated | **Low** | Regenerate |

### 5.4 Automated Sync Check (CI)

```yaml
# .github/workflows/upstream-sync.yml
name: Check Upstream Sync

on:
  schedule:
    - cron: '0 0 * * 1'  # Weekly on Monday

jobs:
  check-sync:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Fetch upstream
        run: |
          git remote add upstream https://github.com/cockroachdb/pebble.git || true
          git fetch upstream

      - name: Check for new commits
        run: |
          BEHIND=$(git rev-list --count main..upstream/main)
          echo "Behind upstream by $BEHIND commits"
          if [ "$BEHIND" -gt 0 ]; then
            echo "::warning::Upstream has $BEHIND new commits"
          fi

      - name: Test merge
        run: |
          git checkout -b test-merge
          git merge upstream/main --no-commit || {
            echo "::error::Merge conflicts detected"
            git diff --name-only --diff-filter=U
            exit 1
          }
```

## 6. Implementation Phases

### Phase 1: Infrastructure (Week 1-2)

- [ ] Create `cobbleext/` package structure
- [ ] Implement config file parsing
- [ ] Implement environment variable handling
- [ ] Add hook in `open.go`
- [ ] Unit tests for config loading

### Phase 2: S3 Backend (Week 3-4)

- [ ] Implement `remote.Storage` for S3
- [ ] Implement `ObjectReader` for S3
- [ ] Add multipart upload support for large files
- [ ] Integration tests with MinIO
- [ ] Integration tests with real S3 (optional)

### Phase 3: GCS Backend (Week 5)

- [ ] Implement `remote.Storage` for GCS
- [ ] Implement `ObjectReader` for GCS
- [ ] Integration tests with GCS emulator
- [ ] Integration tests with real GCS (optional)

### Phase 4: Geth Integration (Week 6)

- [ ] Create go-ethereum fork
- [ ] Add `go.mod` replace directive
- [ ] Test with Sepolia testnet
- [ ] Performance benchmarks
- [ ] Document deployment instructions

### Phase 5: Production Hardening (Week 7-8)

- [ ] Add metrics/monitoring
- [ ] Add retry logic with exponential backoff
- [ ] Add circuit breaker for S3/GCS failures
- [ ] Write operational runbook
- [ ] Load testing

## 7. Testing Strategy

### 7.1 Unit Tests

- Config parsing
- Environment variable handling
- S3/GCS client mocking

### 7.2 Integration Tests

```go
// Use MinIO for S3 testing
func TestS3Storage(t *testing.T) {
    container := testutil.StartMinIO(t)
    defer container.Stop()

    storage := s3.NewStorage(s3.Config{
        Bucket:   "test-bucket",
        Endpoint: container.Endpoint(),
    })

    // Test operations...
}
```

### 7.3 End-to-End Tests

- Full Pebble operations with S3 backend
- Geth sync with tiered storage
- Failover scenarios (S3 unavailable)

## 8. Monitoring and Observability

### 8.1 Metrics to Export

```go
var (
    remoteReadLatency = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name: "cobble_remote_read_latency_seconds",
            Help: "Latency of remote storage read operations",
        },
        []string{"backend", "operation"},
    )

    remoteReadBytes = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "cobble_remote_read_bytes_total",
            Help: "Total bytes read from remote storage",
        },
        []string{"backend"},
    )

    cacheHitRate = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "cobble_cache_hit_rate",
            Help: "Cache hit rate for remote objects",
        },
        []string{"backend"},
    )
)
```

### 8.2 Logging

```go
// Use structured logging
logger.Info("remote storage initialized",
    "backend", "s3",
    "bucket", config.S3.Bucket,
    "cache_size", config.Cache.Size,
)
```

## 9. Security Considerations

1. **Credentials**: Never log or expose credentials
2. **IAM Roles**: Prefer IAM roles over static credentials
3. **Encryption**: Use TLS for all remote communications
4. **Access Control**: Use bucket policies to restrict access

## 10. FAQ

**Q: Why not implement vfs.FS instead of remote.Storage?**

A: `remote.Storage` is specifically designed for object storage with high latency. It has:

- Context support for cancellation
- Separate read/write paths optimized for object storage
- Built-in integration with SharedCache

**Q: What happens if S3 is unavailable?**

A:

- Reads: Fall back to cache, return error if data not cached
- Writes: WAL is always local, so writes continue. Compaction to S3 will retry with backoff.

**Q: Can I migrate existing data to S3?**

A: Yes, we will provide a migration tool that:

1. Copies L5-L6 SST files to S3
2. Updates the object catalog
3. Deletes local copies after verification
