# Cobble CLI Integration Test Cases

This document defines integration test cases for the `cobble` CLI tool, focusing on the checkpoint (multi-database snapshot) feature and related commands.

## Test Environment Setup

### Prerequisites
- Docker (for MinIO container)
- Go 1.21+
- Build tags: `s3`, `cloud`, `integration`

### Test Infrastructure
```go
// TestHelper manages CLI test environment
type TestHelper struct {
    MinIO       *testutil.MinIOContainer
    Bucket      string
    DataDir     string    // Simulated geth data directory
    ConfigFile  string    // Cobble config file path
    AdminServer *server.Server
    Databases   map[string]*pebble.DB  // chaindata, statedata, etc.
}
```

---

## 1. Checkpoint Create Tests

### Test Case 1.1: Create Checkpoint with Auto-Discovery

**Purpose**: Verify `cobble checkpoint create` can auto-discover Pebble databases from a data directory.

**Setup**:
- Create directory structure simulating geth:
  ```
  /tmp/geth-data/
  ├── chaindata/  (pebble DB)
  ├── statedata/  (pebble DB)
  └── ancient/    (pebble DB)
  ```
- Write test data to each database

**Command**:
```bash
cobble checkpoint create --data-dir /tmp/geth-data --description "Block 12345678"
```

**Expected**:
1. CLI discovers all 3 Pebble databases
2. Creates a snapshot for each database atomically
3. Stores checkpoint metadata with all database snapshot IDs
4. Returns checkpoint ID
5. All databases or none are snapshotted (atomicity)

**Verification**:
```go
func TestCheckpointCreateAutoDiscovery(t *testing.T) {
    // Setup: create geth-like data directory with multiple DBs
    // Execute: run checkpoint create command
    // Verify:
    //   - Checkpoint exists in catalog
    //   - All 3 databases have corresponding snapshots
    //   - Each snapshot is independently restorable
}
```

---

### Test Case 1.2: Create Checkpoint via Admin API

**Purpose**: Verify `cobble checkpoint create` can get database list from running admin server.

**Setup**:
- Start admin server with multiple registered databases
- Configure CLI with admin server address

**Command**:
```bash
cobble checkpoint create --admin http://localhost:6060 --description "Block 12345678"
```

**Expected**:
1. CLI connects to admin server
2. Gets list of databases via `/databases` endpoint
3. Creates coordinated checkpoint for all databases
4. Handles connection errors gracefully

**Verification**:
```go
func TestCheckpointCreateViaAdminAPI(t *testing.T) {
    // Setup: start admin server with multiple DBs
    // Execute: run checkpoint create with --admin flag
    // Verify:
    //   - All databases from admin API are included
    //   - Checkpoint metadata includes geth version, block info (if available)
}
```

---

### Test Case 1.3: Create Checkpoint with Metadata

**Purpose**: Verify checkpoint can store block metadata.

**Command**:
```bash
cobble checkpoint create \
  --data-dir /tmp/geth-data \
  --block-number 12345678 \
  --block-hash 0xabc123... \
  --state-root 0xdef456... \
  --description "Mainnet block 12345678"
```

**Expected**:
1. Checkpoint includes all metadata fields
2. Metadata is searchable via `checkpoint list`

---

### Test Case 1.4: Create Checkpoint - Partial Failure Handling

**Purpose**: Verify atomicity when one database fails to snapshot.

**Setup**:
- Create 3 databases
- Simulate failure on database 2 (e.g., permission denied, disk full)

**Expected**:
1. All previous snapshots are cleaned up
2. No partial checkpoint is created
3. Error message clearly indicates which database failed

**Verification**:
```go
func TestCheckpointCreatePartialFailure(t *testing.T) {
    // Setup: create 3 DBs, make one fail during snapshot
    // Execute: run checkpoint create
    // Verify:
    //   - Command returns error
    //   - No checkpoint in catalog
    //   - No orphaned blobs
}
```

---

### Test Case 1.5: Create Checkpoint - Empty Data Directory

**Purpose**: Handle edge case where no Pebble databases are found.

**Command**:
```bash
cobble checkpoint create --data-dir /tmp/empty-dir
```

**Expected**:
1. Clear error message: "No Pebble databases found in /tmp/empty-dir"
2. Non-zero exit code

---

## 2. Checkpoint List Tests

### Test Case 2.1: List All Checkpoints

**Setup**:
- Create 5 checkpoints at different "block heights"

**Command**:
```bash
cobble checkpoint list
```

**Expected Output**:
```
ID                                    CREATED              DATABASES  BLOCK    DESCRIPTION
20240115T120000.000000000Z            2024-01-15 12:00     3          12345    Mainnet block 12345
20240115T110000.000000000Z            2024-01-15 11:00     3          12344    Mainnet block 12344
...
```

---

### Test Case 2.2: List with JSON Output

**Command**:
```bash
cobble checkpoint list --json
```

**Expected**:
- Valid JSON array
- Each entry contains: id, created_at, databases, metadata

---

### Test Case 2.3: List with Filter

**Command**:
```bash
cobble checkpoint list --after "2024-01-15T00:00:00Z" --limit 10
```

**Expected**:
- Only checkpoints after specified time
- Maximum 10 results

---

## 3. Checkpoint Info Tests

### Test Case 3.1: Show Checkpoint Details

**Command**:
```bash
cobble checkpoint info <checkpoint-id>
```

**Expected Output**:
```
Checkpoint: 20240115T120000.000000000Z
  Created:      2024-01-15 12:00:00 UTC
  Description:  Mainnet block 12345678

  Metadata:
    Block Number: 12345678
    Block Hash:   0xabc123...
    State Root:   0xdef456...

  Databases:
    NAME        SNAPSHOT ID                           SIZE      BLOBS
    chaindata   20240115T120000.000000000Z-chaindata  1.2 GB    1234
    statedata   20240115T120000.000000000Z-statedata  500 MB    567
    ancient     20240115T120000.000000000Z-ancient    2.5 GB    890

  Total Size: 4.2 GB
```

---

### Test Case 3.2: Show Checkpoint with JSON

**Command**:
```bash
cobble checkpoint info <checkpoint-id> --json
```

**Expected**:
- Complete checkpoint metadata as JSON
- Includes per-database snapshot details

---

### Test Case 3.3: Checkpoint Not Found

**Command**:
```bash
cobble checkpoint info nonexistent-id
```

**Expected**:
- Error: "Checkpoint 'nonexistent-id' not found"
- Non-zero exit code

---

## 4. Checkpoint Restore Tests

### Test Case 4.1: Full Restore

**Purpose**: Restore all databases from a checkpoint to a new directory.

**Setup**:
- Create checkpoint with 3 databases
- Close all databases

**Command**:
```bash
cobble checkpoint restore <checkpoint-id> --dest /tmp/restored-geth
```

**Expected**:
1. Creates directory structure:
   ```
   /tmp/restored-geth/
   ├── chaindata/
   ├── statedata/
   └── ancient/
   ```
2. All databases are fully functional
3. Data integrity verified via checksums

**Verification**:
```go
func TestCheckpointRestoreFull(t *testing.T) {
    // Setup: create checkpoint
    // Execute: restore to new directory
    // Verify:
    //   - All databases open successfully
    //   - All test data is present
    //   - Checksums match
}
```

---

### Test Case 4.2: Restore with Progress

**Command**:
```bash
cobble checkpoint restore <checkpoint-id> --dest /tmp/restored --progress
```

**Expected Output**:
```
Restoring checkpoint 20240115T120000.000000000Z to /tmp/restored

  chaindata:  [████████████████████] 1234/1234 blobs (1.2 GB)
  statedata:  [████████░░░░░░░░░░░░]  300/567 blobs (250 MB)
  ancient:    [░░░░░░░░░░░░░░░░░░░░]    0/890 blobs (0 B)

Elapsed: 2m 30s | ETA: 5m 00s | Speed: 8.5 MB/s
```

---

### Test Case 4.3: Restore to Existing Directory

**Command**:
```bash
cobble checkpoint restore <checkpoint-id> --dest /existing-dir
```

**Expected**:
- Error: "Destination directory /existing-dir already exists. Use --force to overwrite"

**With Force**:
```bash
cobble checkpoint restore <checkpoint-id> --dest /existing-dir --force
```

**Expected**:
- Existing data is removed
- Fresh restore completes

---

### Test Case 4.4: Restore - Atomicity Test

**Purpose**: If restore of one database fails, all are rolled back.

**Setup**:
- Create checkpoint with 3 databases
- Simulate disk full after restoring 2 databases

**Expected**:
1. Partial restore is cleaned up
2. Destination directory is removed or in original state
3. Clear error message

**Verification**:
```go
func TestCheckpointRestoreAtomicity(t *testing.T) {
    // Setup: create checkpoint, simulate failure during restore
    // Execute: run restore
    // Verify:
    //   - Destination is empty or doesn't exist
    //   - No partial data left behind
}
```

---

### Test Case 4.5: Restore with Verify

**Command**:
```bash
cobble checkpoint restore <checkpoint-id> --dest /tmp/restored --verify
```

**Expected**:
1. After restore, verify all blob checksums
2. Open each database and verify integrity
3. Report verification results

---

### Test Case 4.6: Restore Specific Databases Only

**Command**:
```bash
cobble checkpoint restore <checkpoint-id> --dest /tmp/restored --databases chaindata,statedata
```

**Expected**:
1. Only restores specified databases
2. Warns about missing databases
3. Directory structure matches requested databases

---

## 5. Checkpoint Delete Tests

### Test Case 5.1: Delete Single Checkpoint

**Command**:
```bash
cobble checkpoint delete <checkpoint-id>
```

**Expected**:
- Confirmation prompt: "Delete checkpoint 20240115T120000.000000000Z? (y/N)"
- After confirmation: removes checkpoint from catalog

---

### Test Case 5.2: Delete with Blob Cleanup

**Command**:
```bash
cobble checkpoint delete <checkpoint-id> --delete-blobs
```

**Expected**:
1. Deletes checkpoint metadata
2. Deletes all associated snapshots
3. Runs GC to clean up orphaned blobs
4. Reports: "Deleted checkpoint and 1234 orphaned blobs"

---

### Test Case 5.3: Force Delete (No Confirmation)

**Command**:
```bash
cobble checkpoint delete <checkpoint-id> --force
```

**Expected**:
- No confirmation prompt
- Immediate deletion

---

## 6. Checkpoint GC Tests

### Test Case 6.1: Garbage Collect Old Checkpoints

**Setup**:
- Create 10 checkpoints over time

**Command**:
```bash
cobble checkpoint gc --keep-last 3
```

**Expected**:
1. Keeps 3 most recent checkpoints
2. Deletes 7 older checkpoints
3. Cleans up orphaned blobs
4. Reports summary

---

### Test Case 6.2: GC Dry Run

**Command**:
```bash
cobble checkpoint gc --keep-last 3 --dry-run
```

**Expected Output**:
```
DRY RUN - no changes will be made

Would delete 7 checkpoints:
  - 20240101T120000.000000000Z (13 days old)
  - 20240102T120000.000000000Z (12 days old)
  ...

Would clean up 5678 orphaned blobs (2.5 GB)
```

---

### Test Case 6.3: GC Keep by Age

**Command**:
```bash
cobble checkpoint gc --keep-age 7d
```

**Expected**:
- Keeps all checkpoints from last 7 days
- Deletes older checkpoints

---

## 7. Snapshot Commands (Existing + New)

### Test Case 7.1: Snapshot List

**Command**:
```bash
cobble snapshot list
```

**Expected**:
- Lists all individual database snapshots
- Shows which checkpoint each belongs to

---

### Test Case 7.2: Snapshot Tree

**Command**:
```bash
cobble snapshot tree
```

**Expected Output**:
```
Snapshot Tree (8 snapshots across 3 databases):

chaindata:
  20240115T120000Z (main)
    └── 20240115T130000Z (incremental)
        └── 20240115T140000Z (incremental)

statedata:
  20240115T120000Z (main)
    └── 20240115T130000Z (incremental)

ancient:
  20240115T120000Z (main)
```

---

### Test Case 7.3: Snapshot Compact

**Command**:
```bash
cobble snapshot compact <snapshot-id>
```

**Expected**:
1. Creates fully standalone snapshot
2. Removes dependency on parent
3. Updates catalog

---

### Test Case 7.4: Snapshot GC

**Command**:
```bash
cobble snapshot gc --keep-last 5 --dry-run
```

**Expected**:
- Shows what would be cleaned up
- Respects --keep-last flag
- Reports blob cleanup statistics

---

## 8. Catalog Commands

### Test Case 8.1: Catalog Show

**Command**:
```bash
cobble catalog show
```

**Expected**:
- Shows all snapshots with metadata
- Shows blob reference counts

---

### Test Case 8.2: Catalog Verify

**Command**:
```bash
cobble catalog verify
```

**Expected**:
1. Verifies all manifests exist
2. Verifies all referenced blobs exist
3. Verifies reference counts are accurate
4. Reports any inconsistencies

---

### Test Case 8.3: Catalog Export

**Command**:
```bash
cobble catalog export catalog-backup.json
```

**Expected**:
- Exports full catalog to JSON file
- Can be used for backup/analysis

---

## 9. Blob Commands

### Test Case 9.1: Blob List

**Command**:
```bash
cobble blob list --limit 20
```

**Expected**:
- Lists blobs sorted by reference count
- Shows hash and ref count

---

### Test Case 9.2: Blob Verify

**Command**:
```bash
cobble blob verify --verbose
```

**Expected**:
1. Downloads each blob
2. Verifies SHA256 hash
3. Reports corrupted/missing blobs

---

### Test Case 9.3: Blob Orphans

**Command**:
```bash
cobble blob orphans
```

**Expected**:
- Lists blobs not referenced by any snapshot
- Suggests running `cobble snapshot gc`

---

## 10. Maintenance Commands

### Test Case 10.1: Maintenance Run

**Command**:
```bash
cobble maintenance run --keep-last 5 --compact-threshold 10
```

**Expected**:
1. Finds and compacts long dependency chains
2. Runs garbage collection
3. Reports summary

---

### Test Case 10.2: Maintenance Status

**Command**:
```bash
cobble maintenance status
```

**Expected Output**:
```
Storage Status
==============
  Prefix:          snapshots/
  Last Updated:    2024-01-15T12:00:00Z

Checkpoints
-----------
  Total:           5
  Last 24 hours:   2
  Last week:       3
  Older:           0

Snapshots
---------
  Total:           15
  Unique Blobs:    5678
  Total Size:      12.5 GB
  Dedup Ratio:     3.2x

Dependency Chains
-----------------
  Max Chain:       8
  Avg Chain:       3.5
```

---

## 11. Error Handling Tests

### Test Case 11.1: Storage Connection Error

**Setup**: Invalid S3 endpoint

**Command**:
```bash
cobble checkpoint list
```

**Expected**:
- Clear error: "Failed to connect to storage: connection refused"
- Non-zero exit code

---

### Test Case 11.2: Permission Denied

**Setup**: Invalid S3 credentials

**Command**:
```bash
cobble checkpoint create --data-dir /tmp/data
```

**Expected**:
- Clear error: "Access denied to S3 bucket"
- Non-zero exit code

---

### Test Case 11.3: Concurrent Access

**Purpose**: Test distributed locking works correctly.

**Setup**:
- Two concurrent `checkpoint create` operations

**Expected**:
1. One acquires lock and proceeds
2. Other waits or fails gracefully
3. No data corruption

---

## 12. Performance Tests

### Test Case 12.1: Large Database Checkpoint

**Setup**:
- Database with 10GB of data
- 10,000+ SST files

**Command**:
```bash
time cobble checkpoint create --data-dir /tmp/large-data
```

**Expected**:
- Completes within reasonable time (< 5 minutes for 10GB)
- Memory usage stays bounded
- Progress indicator updates smoothly

---

### Test Case 12.2: Large Database Restore

**Setup**:
- Checkpoint of 10GB database

**Command**:
```bash
time cobble checkpoint restore <id> --dest /tmp/restored
```

**Expected**:
- Completes within reasonable time
- Parallel blob downloads
- Progress accurate

---

## Test Implementation Structure

```
cmd/cobble/cmd/
├── cli_test.go              # Test helper utilities
├── checkpoint_test.go       # Checkpoint command tests
├── snapshot_cli_test.go     # Snapshot command tests
├── catalog_test.go          # Catalog command tests
├── blob_test.go             # Blob command tests
├── maintenance_test.go      # Maintenance command tests
└── testdata/                # Test fixtures
    ├── config.yaml
    └── expected_outputs/
```

## Build Tags and Running Tests

```bash
# Run all CLI integration tests
go test -tags "s3 cloud integration" ./cmd/cobble/cmd/... -v

# Run specific test
go test -tags "s3 cloud integration" ./cmd/cobble/cmd/... -run TestCheckpointCreate -v

# Run with short timeout (skip slow tests)
go test -tags "s3 cloud integration" ./cmd/cobble/cmd/... -short
```
