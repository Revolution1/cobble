# Cobble Design Documents

This directory contains design documents for Cobble (Pebble fork with S3/GCS tiered storage support).

## Documents

1. **[tiered-storage-design.md](tiered-storage-design.md)**
   - Overall architecture
   - Environment variable configuration
   - Implementation phases
   - Testing strategy

2. **[import-path-and-code-skeleton.md](import-path-and-code-skeleton.md)**
   - Import path strategy (keep original vs change)
   - Code skeleton with all interfaces
   - S3/GCS implementation examples
   - Build tag organization

3. **[integration-analysis.md](integration-analysis.md)** (this document)
   - Analysis of go-ethereum, Nitro, op-geth integration
   - Key findings and recommendations
   - Final implementation plan

---

## Executive Summary

### Project Goal

Enable S3/GCS tiered storage for Pebble-based databases (used by go-ethereum, Nitro, op-geth) with:

- **Zero intrusion** to consumer codebases
- **Easy upstream sync** with cockroachdb/pebble
- **Environment-based configuration** for production flexibility

### Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Extension point | `remote.Storage` interface | Already designed for remote storage, includes caching |
| Import path | Keep `github.com/cockroachdb/pebble` | Easier upstream sync, standard Go practice |
| Config loading | Hook in `Open()` after `Clone()` | Non-intrusive, respects user options |
| New code location | `cobbleext/` package | Isolated from core, minimal merge conflicts |
| Cloud dependencies | Build tags (`-tags s3`) | Optional, no impact on users who don't need it |

### Implementation Status

✅ **Completed** - The cobbleext package is now implemented!

### Architecture (Implemented)

```
┌─────────────────────────────────────────────────────────────────┐
│                   go-ethereum / op-geth / Nitro                  │
│                                                                  │
│   go.mod:                                                       │
│   replace github.com/cockroachdb/pebble => github.com/...cobble│
├─────────────────────────────────────────────────────────────────┤
│                         Cobble                                   │
│                                                                  │
│   open.go:                                                      │
│   + if ExternalConfigHook != nil { ExternalConfigHook(opts) }   │
│                                                                  │
│   external_config.go:                                           │
│   + ExternalConfigHook = defaultExternalConfigHook              │
│   + optionsAdapter (implements cobbleext.OptionsAdapter)        │
│                                                                  │
│   cobbleext/ (new package, no import cycle):                    │
│   ├── config.go         - Config types & YAML loading          │
│   ├── env.go            - Environment variable handling         │
│   ├── apply.go          - OptionsAdapter interface & logic     │
│   ├── storage_s3.go     - S3 factory (build tag: s3)           │
│   ├── storage_gcs.go    - GCS factory (build tag: gcs)         │
│   ├── storage_s3_stub.go  - Stub when S3 not compiled          │
│   ├── storage_gcs_stub.go - Stub when GCS not compiled         │
│   ├── s3/storage.go     - S3 remote.Storage impl               │
│   └── gcs/storage.go    - GCS remote.Storage impl              │
├─────────────────────────────────────────────────────────────────┤
│                    Environment Variables                         │
│                                                                  │
│   COBBLE_S3_BUCKET=my-bucket       (auto-enables tiering)       │
│   COBBLE_S3_REGION=us-east-1                                    │
│   COBBLE_CACHE_SIZE=50GB                                        │
│   COBBLE_TIERING_STRATEGY=lower    (default: L5-L6 only)       │
└─────────────────────────────────────────────────────────────────┘
```

### Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Upstream merge conflicts | Low | Medium | Only 1 line change in `open.go` |
| S3 latency affecting performance | Medium | High | Use `CreateOnSharedLower` (L5-L6 only), proper caching |
| Breaking geth compatibility | Low | High | Extensive testing, path scheme requirement |
| Cloud SDK vulnerabilities | Low | Medium | Regular dependency updates, build tags for isolation |

### Quick Start (After Implementation)

```bash
# 1. In go-ethereum fork, add replace directive
echo 'replace github.com/cockroachdb/pebble => github.com/Revolution1/cobble v0.1.0' >> go.mod

# 2. Build with S3 support
go build -tags s3 -o geth ./cmd/geth

# 3. Run with tiered storage
export COBBLE_TIERED_STORAGE=true
export COBBLE_S3_BUCKET=ethereum-data
export COBBLE_S3_REGION=us-east-1
export COBBLE_CACHE_SIZE=50GB
./geth --state.scheme path --datadir /data
```

---

## Detailed Analysis: Consumer Codebases

### go-ethereum

**Pebble Version**: v1.1.5
**Key File**: `ethdb/pebble/pebble.go`

**Configuration Pattern**:

```go
// New creates a new Pebble database
func New(file string, cache int, handles int, namespace string, readonly bool) (*Database, error) {
    opts := &pebble.Options{
        Cache:        pebble.NewCache(cacheSize),
        MemTableSize: memTableSize,
        // ... many options
    }
    db, err := pebble.Open(file, opts)
}
```

**Integration Approach**: The `opts` object is created locally, so our hook in `pebble.Open()` will work. Environment variables will be applied before `EnsureDefaults()`.

### Nitro (Arbitrum)

**Pebble Version**: v1.1.5
**Key File**: `cmd/conf/database.go`

**Notable Features**:

- Custom `PebbleConfig` struct for CLI configuration
- Database conversion tools (LevelDB ↔ Pebble)
- Uses local go-ethereum fork

**Integration Approach**: Same as go-ethereum. Nitro's additional config options don't conflict with our environment-based approach.

### op-geth (Optimism)

**Pebble Version**: v1.1.0 (older)
**Key File**: Same as go-ethereum (it's a fork)

**Considerations**:

- Slightly older Pebble version
- May need to ensure compatibility with v1.1.0 API

---

## Upstream Sync Procedure

### Initial Setup

```bash
# Already done in this repo
git remote add upstream https://github.com/cockroachdb/pebble.git
```

### Weekly Sync Check (Automated)

```yaml
# .github/workflows/upstream-check.yml
name: Upstream Sync Check
on:
  schedule:
    - cron: '0 0 * * 1'  # Monday 00:00 UTC
jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - run: |
          git fetch upstream
          BEHIND=$(git rev-list --count main..upstream/main)
          if [ "$BEHIND" -gt 50 ]; then
            echo "::warning::$BEHIND commits behind upstream, consider syncing"
          fi
```

### Manual Sync Process

```bash
# 1. Fetch upstream
git fetch upstream

# 2. Create sync branch
git checkout -b sync/$(date +%Y%m%d) main

# 3. Merge upstream
git merge upstream/main

# 4. Resolve conflicts (typically only in open.go and go.mod)
# open.go: Re-add the cobbleext.ApplyExternalConfig() line
# go.mod: Keep our additions, update pebble dependencies

# 5. Test
go test -tags "s3 gcs" ./...

# 6. Create PR
gh pr create --title "Sync upstream $(date +%Y-%m-%d)"
```

---

## Implementation Priority

### Phase 1: Core Infrastructure (Must Have)

1. `cobbleext/config.go` - Config types and YAML loading
2. `cobbleext/env.go` - Environment variable handling
3. `cobbleext/apply.go` - Apply config to Options
4. `open.go` modification - Single line hook
5. Unit tests for config loading

### Phase 2: S3 Backend (Must Have)

1. `cobbleext/s3/storage.go` - remote.Storage implementation
2. `cobbleext/s3/reader.go` - ObjectReader with range reads
3. `cobbleext/s3/writer.go` - Multipart upload support
4. Integration tests with MinIO

### Phase 3: GCS Backend (Nice to Have)

1. `cobbleext/gcs/storage.go` - remote.Storage implementation
2. Integration tests with GCS emulator

### Phase 4: Production Readiness

1. Metrics and logging
2. Retry logic with exponential backoff
3. Circuit breaker for cloud failures
4. Migration tooling for existing data
5. Documentation and runbook

---

## File Change Summary

### Modified Files (from upstream)

| File | Lines Changed | Conflict Risk |
|------|---------------|---------------|
| `open.go` | +5 | Low |
| `go.mod` | +10-15 | Medium (deps) |
| `go.sum` | auto | Low |

### New Files

| File | Purpose |
|------|---------|
| `cobbleext/config.go` | Config types and loading |
| `cobbleext/env.go` | Environment variables |
| `cobbleext/apply.go` | Apply config to Options |
| `cobbleext/noop.go` | Default no-op (no cloud deps) |
| `cobbleext/s3/*.go` | S3 implementation |
| `cobbleext/gcs/*.go` | GCS implementation |
| `cobbleext/testutil/*.go` | Test helpers |
| `docs/design/*.md` | Design documents |

---

## Next Steps

1. **Review this design** - Get feedback on approach
2. **Create cobbleext package skeleton** - Empty files with interfaces
3. **Implement config loading** - Start with env vars only
4. **Add open.go hook** - Single line change
5. **Implement S3 backend** - Core functionality first
6. **Test with go-ethereum** - Integration validation
7. **Add GCS backend** - If needed
8. **Production hardening** - Metrics, retries, etc.

---

## Questions to Resolve

1. **Cache location**: Should the SharedCache be stored in the same directory as the DB, or configurable separately? (Recommendation: Same directory by default, configurable via env)

2. **Failure mode**: What happens when S3 is unavailable during compaction? (Recommendation: Retry with backoff, eventually fail the compaction and log error)

3. **Migration tooling**: Should we provide tools to migrate existing local data to S3? (Recommendation: Yes, as a separate CLI tool)

4. **Freezer integration**: Should ancient data (Freezer) also go to S3? (Recommendation: Future work, different access pattern)
