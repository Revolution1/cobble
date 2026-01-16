# Cobble Operations Guide

This guide covers common operational scenarios for managing Pebble database snapshots with Cobble.

## Table of Contents

1. [Overview](#overview)
2. [Initial Setup](#initial-setup)
3. [Daily Operations](#daily-operations)
4. [Disaster Recovery](#disaster-recovery)
5. [Multi-Database Scenarios (Geth)](#multi-database-scenarios-geth)
6. [Tiered Storage Optimization](#tiered-storage-optimization)
7. [Maintenance Tasks](#maintenance-tasks)
8. [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)

---

## Overview

Cobble provides Content-Addressable Storage (CAS) based snapshots for Pebble databases. Key features:

- **Deduplication**: Identical files are stored only once across all snapshots
- **Incremental snapshots**: Only new/changed files are uploaded
- **Server-side copy**: Files already in S3 (tiered storage) are copied without re-upload
- **Multi-database support**: Manage checkpoints for multiple databases (e.g., Geth's chaindata, state, ancient)

### Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Pebble DB     │────▶│   Cobble CLI    │────▶│   S3 Storage    │
│  (local disk)   │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     │  blobs/         │
                                                │  snapshots/     │
┌─────────────────┐                             │  catalog.json   │
│  Tiered Storage │─────────────────────────────│  checkpoints/   │
│  (existing S3)  │  server-side copy           └─────────────────┘
└─────────────────┘
```

---

## Initial Setup

### 1. Configure S3 Credentials

Set environment variables or use a config file:

```bash
# Environment variables
export COBBLE_S3_BUCKET="my-backup-bucket"
export COBBLE_S3_ENDPOINT="https://s3.amazonaws.com"  # or MinIO endpoint
export COBBLE_S3_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
export COBBLE_S3_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
export COBBLE_S3_REGION="us-east-1"

# For MinIO or S3-compatible storage
export COBBLE_S3_FORCE_PATH_STYLE="true"
```

Or create a config file (`~/.cobble/config.yaml`):

```yaml
s3:
  bucket: my-backup-bucket
  endpoint: https://s3.amazonaws.com
  region: us-east-1
  # access_key_id and secret_access_key can also be set here
  # but environment variables are recommended for security
```

### 2. Initialize Storage Prefix

Choose a prefix to organize your snapshots:

```bash
# For a single application
cobble checkpoint create --prefix "myapp/" --data-dir /path/to/db

# For multiple environments
cobble checkpoint create --prefix "prod/myapp/" --data-dir /path/to/db
cobble checkpoint create --prefix "staging/myapp/" --data-dir /path/to/db
```

### 3. Verify Setup

```bash
# Create a test checkpoint
cobble checkpoint create --prefix "test/" --data-dir /path/to/db --description "Initial test"

# List checkpoints
cobble checkpoint list --prefix "test/"

# Verify catalog
cobble catalog verify --prefix "test/"
```

---

## Daily Operations

### Creating Scheduled Snapshots

#### Using cron (Linux)

```bash
# /etc/cron.d/cobble-backup
# Create daily snapshot at 2 AM
0 2 * * * root /usr/local/bin/cobble checkpoint create \
    --prefix "prod/myapp/" \
    --data-dir /var/lib/myapp/db \
    --description "Daily backup $(date +%Y-%m-%d)" \
    >> /var/log/cobble-backup.log 2>&1
```

#### Using systemd timer

```ini
# /etc/systemd/system/cobble-backup.service
[Unit]
Description=Cobble Database Backup

[Service]
Type=oneshot
ExecStart=/usr/local/bin/cobble checkpoint create \
    --prefix "prod/myapp/" \
    --data-dir /var/lib/myapp/db \
    --description "Scheduled backup"
Environment=COBBLE_S3_BUCKET=my-backup-bucket
# ... other environment variables
```

```ini
# /etc/systemd/system/cobble-backup.timer
[Unit]
Description=Run Cobble backup daily

[Timer]
OnCalendar=*-*-* 02:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

```bash
systemctl enable --now cobble-backup.timer
```

### Listing and Inspecting Snapshots

```bash
# List all checkpoints
cobble checkpoint list --prefix "prod/myapp/"

# Output:
# 20260116T150923.519621252Z  (2026-01-16T15:09:23Z)  dbs: 1
#   desc: Daily backup 2026-01-16
#   - mydb (13.3 KB, 7 blobs)

# Get detailed info
cobble checkpoint info 20260116T150923.519621252Z --prefix "prod/myapp/"

# List as JSON (for scripting)
cobble checkpoint list --prefix "prod/myapp/" --json | jq '.[0]'
```

### Verifying Snapshot Integrity

```bash
# Verify catalog structure
cobble catalog verify --prefix "prod/myapp/"

# Check blob integrity (downloads and verifies checksums)
cobble blob verify --prefix "prod/myapp/" --verbose
```

---

## Disaster Recovery

### Scenario 1: Single Database Restore

**Situation**: Database corrupted, need to restore from latest snapshot.

```bash
# 1. Stop the application
systemctl stop myapp

# 2. List available checkpoints
cobble checkpoint list --prefix "prod/myapp/"

# 3. Restore to a new directory
cobble checkpoint restore 20260116T150923.519621252Z \
    --prefix "prod/myapp/" \
    --dest /var/lib/myapp/db-restored

# 4. Verify the restored database
# (application-specific verification)

# 5. Replace the old database
mv /var/lib/myapp/db /var/lib/myapp/db-corrupted
mv /var/lib/myapp/db-restored/mydb /var/lib/myapp/db

# 6. Restart the application
systemctl start myapp

# 7. Clean up after verification
rm -rf /var/lib/myapp/db-corrupted
```

### Scenario 2: Point-in-Time Recovery

**Situation**: Need to restore to a specific point in time.

```bash
# 1. List checkpoints with timestamps
cobble checkpoint list --prefix "prod/myapp/" --json | \
    jq '.[] | {id: .id, created: .created_at, desc: .description}'

# 2. Find the checkpoint closest to your target time
# Example: restore to state before 2026-01-15 14:00

# 3. Restore the identified checkpoint
cobble checkpoint restore 20260115T120000.000000000Z \
    --prefix "prod/myapp/" \
    --dest /var/lib/myapp/db-pit-restore
```

### Scenario 3: Cross-Region Recovery

**Situation**: Primary region unavailable, restore from backup region.

```bash
# Assuming you've replicated snapshots to another bucket/region
export COBBLE_S3_BUCKET="backup-region-bucket"
export COBBLE_S3_ENDPOINT="https://s3.eu-west-1.amazonaws.com"
export COBBLE_S3_REGION="eu-west-1"

# Restore from backup region
cobble checkpoint restore 20260116T150923.519621252Z \
    --prefix "prod/myapp/" \
    --dest /var/lib/myapp/db-dr-restore
```

---

## Multi-Database Scenarios (Geth)

Geth uses multiple Pebble databases: `chaindata`, `state`, and `ancient`. Cobble supports managing these together.

### Creating Checkpoints for All Databases

#### Method 1: Using the Admin API (Recommended for running Geth)

If your application exposes an admin API:

```bash
# 1. Trigger checkpoint creation via admin API
curl -X POST http://localhost:9999/checkpoint \
    -H "Content-Type: application/json" \
    -d '{"dest_dir": "/tmp/geth-checkpoint-v1"}'

# 2. Upload each database checkpoint
for db in chaindata state ancient; do
    cobble checkpoint create \
        --prefix "geth-prod/" \
        --data-dir "/tmp/geth-checkpoint-v1/$db" \
        --description "Geth $db checkpoint $(date +%Y-%m-%d)"
done

# 3. Clean up local checkpoint
rm -rf /tmp/geth-checkpoint-v1
```

#### Method 2: Direct Checkpoint (Requires stopping Geth)

```bash
# 1. Stop Geth
systemctl stop geth

# 2. Create checkpoints for each database
for db in chaindata state ancient; do
    cobble checkpoint create \
        --prefix "geth-prod/" \
        --data-dir "/var/lib/geth/$db" \
        --description "Geth $db checkpoint $(date +%Y-%m-%d)"
done

# 3. Restart Geth
systemctl start geth
```

### Restoring All Geth Databases

```bash
# 1. Stop Geth
systemctl stop geth

# 2. Get checkpoint IDs for each database
CHECKPOINTS=$(cobble checkpoint list --prefix "geth-prod/" --json)

# 3. Restore each database
# Note: Each checkpoint contains a database name, restored under --dest
mkdir -p /var/lib/geth-restored

for checkpoint_id in $(echo "$CHECKPOINTS" | jq -r '.[].id' | head -3); do
    cobble checkpoint restore "$checkpoint_id" \
        --prefix "geth-prod/" \
        --dest /var/lib/geth-restored
done

# 4. Verify restored databases
ls -la /var/lib/geth-restored/
# Should show: chaindata/ state/ ancient/

# 5. Replace and restart
mv /var/lib/geth /var/lib/geth-old
mv /var/lib/geth-restored /var/lib/geth
systemctl start geth
```

### Atomic Multi-Database Snapshots

For consistency, create all database checkpoints at the same time:

```bash
#!/bin/bash
# geth-snapshot.sh - Create atomic snapshot of all Geth databases

set -e

TIMESTAMP=$(date +%Y%m%dT%H%M%S)
CHECKPOINT_DIR="/tmp/geth-checkpoint-$TIMESTAMP"
PREFIX="geth-prod/"

# Stop writes (if using admin API, trigger pause)
curl -X POST http://localhost:9999/admin/pause || true

# Create local checkpoints atomically
mkdir -p "$CHECKPOINT_DIR"
curl -X POST http://localhost:9999/checkpoint \
    -H "Content-Type: application/json" \
    -d "{\"dest_dir\": \"$CHECKPOINT_DIR\"}"

# Resume writes
curl -X POST http://localhost:9999/admin/resume || true

# Upload to S3
for db in chaindata state ancient; do
    cobble checkpoint create \
        --prefix "$PREFIX" \
        --data-dir "$CHECKPOINT_DIR/$db" \
        --description "Geth atomic snapshot $TIMESTAMP - $db"
done

# Cleanup
rm -rf "$CHECKPOINT_DIR"

echo "Snapshot $TIMESTAMP completed"
```

---

## Tiered Storage Optimization

When using Pebble's tiered storage (SST files already in S3), Cobble automatically uses server-side copy to avoid re-uploading data.

### How It Works

1. Cobble reads `REMOTE-OBJ-CATALOG` from the database directory
2. For files already in S3, it uses `CopyObject` (server-side copy)
3. Server-side copy is near-instant and doesn't transfer data through the client

### Verifying Optimization

```bash
# Check blob sources in a snapshot
cobble checkpoint info 20260116T150923.519621252Z --prefix "geth-prod/" --json | \
    jq '.databases[].blobs[] | {name: .original_name, source: .source, key_type: .key[:6]}'

# Expected output for tiered storage files:
# {"name": "000001.sst", "source": "tiered", "key_type": "etag:"}
# {"name": "000002.sst", "source": "tiered", "key_type": "etag:"}

# Local files will show:
# {"name": "MANIFEST-000001", "source": "local", "key_type": "sha256"}
```

### Efficiency Report

```bash
# Show blob statistics
cobble blob list --prefix "geth-prod/"

# Count blob types
cobble blob list --prefix "geth-prod/" --json | \
    jq 'group_by(.key | split(":")[0]) | map({type: .[0].key | split(":")[0], count: length})'
```

---

## Maintenance Tasks

### Garbage Collection

Remove old snapshots and unreferenced blobs:

```bash
# Dry run - see what would be deleted
cobble checkpoint gc --prefix "geth-prod/" --keep-last 7 --dry-run

# Actually delete (keeps last 7 checkpoints per database)
cobble checkpoint gc --prefix "geth-prod/" --keep-last 7

# Delete specific old checkpoint
cobble checkpoint delete 20260110T120000.000000000Z --prefix "geth-prod/" --force
```

### Finding Orphaned Blobs

```bash
# List blobs not referenced by any snapshot
cobble blob orphans --prefix "geth-prod/"

# Clean up orphaned blobs (after verifying)
cobble blob orphans --prefix "geth-prod/" --delete
```

### Storage Analysis

```bash
# Show catalog summary
cobble catalog show --prefix "geth-prod/"

# Output:
# Catalog (updated: 2026-01-16T22:49:53+08:00)
#   Snapshots: 6
#   Unique Blobs: 17
#
# Snapshots:
#   ID                                    CREATED           SIZE     BLOBS
#   20260116T150924.721208151Z-ancient    2026-01-16 15:09  14.6 KB  8
#   ...

# Calculate total storage used
cobble blob list --prefix "geth-prod/" --json | \
    jq '[.[].size] | add' | numfmt --to=iec-i
```

### Compacting Snapshot Chains

If you have a long chain of incremental snapshots, compact them:

```bash
# View snapshot chain
cobble snapshot tree 20260116T150924Z --prefix "geth-prod/"

# Compact (creates standalone snapshot without parent dependency)
cobble snapshot compact 20260116T150924Z \
    --prefix "geth-prod/" \
    --output-id "20260116-compacted"
```

---

## Monitoring and Troubleshooting

### Health Checks

```bash
#!/bin/bash
# cobble-health-check.sh

PREFIX="geth-prod/"

# Check catalog is accessible
if ! cobble catalog show --prefix "$PREFIX" > /dev/null 2>&1; then
    echo "ERROR: Cannot access catalog"
    exit 1
fi

# Check latest checkpoint age
LATEST=$(cobble checkpoint list --prefix "$PREFIX" --json | jq -r '.[0].created_at')
LATEST_EPOCH=$(date -d "$LATEST" +%s)
NOW_EPOCH=$(date +%s)
AGE_HOURS=$(( (NOW_EPOCH - LATEST_EPOCH) / 3600 ))

if [ "$AGE_HOURS" -gt 25 ]; then
    echo "WARNING: Latest checkpoint is $AGE_HOURS hours old"
    exit 1
fi

# Verify catalog integrity
if ! cobble catalog verify --prefix "$PREFIX" > /dev/null 2>&1; then
    echo "ERROR: Catalog verification failed"
    exit 1
fi

echo "OK: Latest checkpoint is $AGE_HOURS hours old"
```

### Common Issues

#### Issue: "catalog.json not found"

```bash
# First time setup - create initial checkpoint
cobble checkpoint create --prefix "myapp/" --data-dir /path/to/db

# Or initialize empty catalog
cobble catalog init --prefix "myapp/"
```

#### Issue: "blob not found during restore"

```bash
# Verify blob integrity
cobble blob verify --prefix "myapp/" --verbose

# Check if blob exists
cobble blob info sha256:abc123... --prefix "myapp/"

# If blob is missing, you may need to restore from a different checkpoint
```

#### Issue: "S3 access denied"

```bash
# Verify credentials
aws s3 ls s3://$COBBLE_S3_BUCKET/$PREFIX --endpoint-url $COBBLE_S3_ENDPOINT

# Check bucket policy allows:
# - s3:GetObject
# - s3:PutObject
# - s3:DeleteObject
# - s3:ListBucket
# - s3:CopyObject (for tiered storage optimization)
```

#### Issue: Slow snapshot creation

```bash
# Check if tiered storage optimization is working
cobble checkpoint create --prefix "myapp/" --data-dir /path/to/db --verbose 2>&1 | \
    grep -E "(server-side copy|uploaded)"

# If you see "uploaded" for large SST files, tiered storage may not be configured
# Check for REMOTE-OBJ-CATALOG in the database directory
ls -la /path/to/db/REMOTE-OBJ-CATALOG
```

### Logging

Enable verbose logging:

```bash
# Verbose output
cobble checkpoint create --prefix "myapp/" --data-dir /path/to/db --verbose

# Debug level (very verbose)
COBBLE_LOG_LEVEL=debug cobble checkpoint create --prefix "myapp/" --data-dir /path/to/db
```

### Metrics (Prometheus)

If using the admin server:

```bash
# Scrape metrics
curl http://localhost:6060/metrics

# Example metrics:
# pebble_cobble_admin_up 1
# pebble_cobble_tiered_storage_enabled 1
# pebble_cobble_snapshot_total 42
# pebble_cobble_snapshot_last_duration_seconds 12.5
```

---

## Quick Reference

### Essential Commands

| Task | Command |
|------|---------|
| Create checkpoint | `cobble checkpoint create --prefix X --data-dir Y` |
| List checkpoints | `cobble checkpoint list --prefix X` |
| Restore checkpoint | `cobble checkpoint restore ID --prefix X --dest Y` |
| Delete checkpoint | `cobble checkpoint delete ID --prefix X --force` |
| Garbage collection | `cobble checkpoint gc --prefix X --keep-last N` |
| Verify catalog | `cobble catalog verify --prefix X` |
| Show catalog | `cobble catalog show --prefix X` |
| List blobs | `cobble blob list --prefix X` |
| Find orphans | `cobble blob orphans --prefix X` |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `COBBLE_S3_BUCKET` | S3 bucket name |
| `COBBLE_S3_ENDPOINT` | S3 endpoint URL |
| `COBBLE_S3_ACCESS_KEY_ID` | AWS access key |
| `COBBLE_S3_SECRET_ACCESS_KEY` | AWS secret key |
| `COBBLE_S3_REGION` | AWS region |
| `COBBLE_S3_FORCE_PATH_STYLE` | Use path-style URLs (for MinIO) |
| `COBBLE_LOG_LEVEL` | Log level (debug, info, warn, error) |

### Storage Layout

```
s3://bucket/prefix/
├── catalog.json           # Snapshot metadata and blob reference counts
├── blobs/
│   ├── sha256:abc123...   # Content-addressed blobs (local files)
│   └── etag:def456...     # ETag-keyed blobs (tiered storage files)
├── snapshots/
│   ├── 20260116T150923Z/
│   │   └── manifest.json  # Snapshot manifest
│   └── 20260116T150924Z/
│       └── manifest.json
└── checkpoints/
    └── 20260116T150923.519621252Z.json  # Checkpoint metadata
```
