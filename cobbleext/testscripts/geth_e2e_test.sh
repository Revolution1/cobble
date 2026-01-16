#!/bin/bash
# Geth E2E Test Script
# This script tests the cobble CLI with MinIO, simulating a Geth multi-database scenario.
#
# Prerequisites:
# - Docker installed and running
# - Go installed
#
# Usage:
#   ./geth_e2e_test.sh
#
# The script will:
# 1. Start a MinIO container
# 2. Build the cobble CLI
# 3. Create multiple Pebble databases (simulating Geth's chaindata, state, ancient)
# 4. Create snapshots using the CLI
# 5. List and verify snapshots
# 6. Restore from snapshots
# 7. Verify restored data
# 8. Clean up

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
MINIO_CONTAINER_NAME="cobble-test-minio"
MINIO_PORT=9100
MINIO_CONSOLE_PORT=9101
MINIO_ROOT_USER="minioadmin"
MINIO_ROOT_PASSWORD="minioadmin"
BUCKET_NAME="cobble-test"
PREFIX="geth-test/"

# Paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COBBLE_BIN="/tmp/cobble-e2e-test"
TEST_DIR="/tmp/cobble-geth-e2e-test-$$"

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

cleanup() {
    log_info "Cleaning up..."

    # Stop MinIO container
    if docker ps -q -f name="$MINIO_CONTAINER_NAME" | grep -q .; then
        docker stop "$MINIO_CONTAINER_NAME" >/dev/null 2>&1 || true
    fi
    if docker ps -aq -f name="$MINIO_CONTAINER_NAME" | grep -q .; then
        docker rm "$MINIO_CONTAINER_NAME" >/dev/null 2>&1 || true
    fi

    # Remove test directory
    if [ -d "$TEST_DIR" ]; then
        rm -rf "$TEST_DIR"
    fi

    log_success "Cleanup complete"
}

# Set up trap for cleanup on exit
trap cleanup EXIT

# Start MinIO
start_minio() {
    log_info "Starting MinIO container..."

    # Check if container already exists
    if docker ps -aq -f name="$MINIO_CONTAINER_NAME" | grep -q .; then
        docker rm -f "$MINIO_CONTAINER_NAME" >/dev/null 2>&1
    fi

    docker run -d \
        --name "$MINIO_CONTAINER_NAME" \
        -p "$MINIO_PORT:9000" \
        -p "$MINIO_CONSOLE_PORT:9001" \
        -e "MINIO_ROOT_USER=$MINIO_ROOT_USER" \
        -e "MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD" \
        minio/minio server /data --console-address ":9001"

    # Wait for MinIO to be ready
    log_info "Waiting for MinIO to be ready..."
    for i in {1..30}; do
        if curl -s "http://localhost:$MINIO_PORT/minio/health/live" | grep -q "OK" 2>/dev/null; then
            break
        fi
        sleep 1
    done

    # Create bucket using mc or curl
    log_info "Creating bucket: $BUCKET_NAME"
    docker exec "$MINIO_CONTAINER_NAME" mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1
    docker exec "$MINIO_CONTAINER_NAME" mc mb "local/$BUCKET_NAME" >/dev/null 2>&1 || true

    log_success "MinIO started at http://localhost:$MINIO_PORT"
}

# Build cobble CLI
build_cobble() {
    log_info "Building cobble CLI..."
    cd "$PROJECT_ROOT"
    go build -tags "s3 cloud" -o "$COBBLE_BIN" ./cmd/cobble
    log_success "Cobble CLI built: $COBBLE_BIN"
}

# Create test databases using a Go program
create_test_databases() {
    log_info "Creating test databases..."

    mkdir -p "$TEST_DIR"

    # Write a Go program to create test databases
    cat > "$TEST_DIR/create_dbs.go" << 'GOEOF'
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func init() {
	// Disable automatic tiered storage configuration from environment
	pebble.ExternalConfigHook = nil
	pebble.PostOpenHook = nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: create_dbs <base_dir>")
		os.Exit(1)
	}
	baseDir := os.Args[1]

	databases := []struct {
		name   string
		keys   int
		prefix string
	}{
		{"chaindata", 1000, "block-"},
		{"state", 2000, "account-"},
		{"ancient", 500, "ancient-"},
	}

	for _, db := range databases {
		dbPath := filepath.Join(baseDir, db.name)
		fmt.Printf("Creating %s with %d keys...\n", db.name, db.keys)

		// Use plain pebble options without tiered storage
		opts := &pebble.Options{
			FS: vfs.Default,
		}
		pdb, err := pebble.Open(dbPath, opts)
		if err != nil {
			fmt.Printf("Failed to open %s: %v\n", db.name, err)
			os.Exit(1)
		}

		for i := 0; i < db.keys; i++ {
			key := fmt.Sprintf("%s%08d", db.prefix, i)
			value := fmt.Sprintf("value-%s-%08d-data", db.name, i)
			if err := pdb.Set([]byte(key), []byte(value), pebble.Sync); err != nil {
				fmt.Printf("Failed to write to %s: %v\n", db.name, err)
				os.Exit(1)
			}
		}

		if err := pdb.Flush(); err != nil {
			fmt.Printf("Failed to flush %s: %v\n", db.name, err)
			os.Exit(1)
		}

		if err := pdb.Close(); err != nil {
			fmt.Printf("Failed to close %s: %v\n", db.name, err)
			os.Exit(1)
		}

		fmt.Printf("  Created %s successfully\n", db.name)
	}

	fmt.Println("All databases created successfully")
}
GOEOF

    # Run the program (no special tags needed for basic pebble operations)
    cd "$PROJECT_ROOT"
    go run -tags invariants "$TEST_DIR/create_dbs.go" "$TEST_DIR/databases"

    log_success "Test databases created"
}

# Export environment variables for cobble CLI
export_env() {
    export COBBLE_S3_BUCKET="$BUCKET_NAME"
    export COBBLE_S3_ENDPOINT="http://localhost:$MINIO_PORT"
    export COBBLE_S3_ACCESS_KEY_ID="$MINIO_ROOT_USER"
    export COBBLE_S3_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD"
    export COBBLE_S3_FORCE_PATH_STYLE="true"
    export COBBLE_S3_REGION="us-east-1"
}

# Create snapshots using CLI
create_snapshots() {
    log_info "Creating checkpoints (CAS snapshots)..."

    for db in chaindata state ancient; do
        log_info "  Creating checkpoint for $db..."
        "$COBBLE_BIN" checkpoint create \
            --prefix "$PREFIX" \
            --data-dir "$TEST_DIR/databases/$db" \
            --description "Test checkpoint for $db"
        log_success "  Checkpoint created for $db"
    done

    log_success "All checkpoints created"
}

# List checkpoints
list_checkpoints() {
    log_info "Listing checkpoints..."
    "$COBBLE_BIN" checkpoint list --prefix "$PREFIX"
}

# Show catalog
show_catalog() {
    log_info "Showing catalog..."
    "$COBBLE_BIN" catalog show --prefix "$PREFIX"
}

# Verify catalog integrity
verify_catalog() {
    log_info "Verifying catalog integrity..."
    "$COBBLE_BIN" catalog verify --prefix "$PREFIX"
}

# Restore checkpoints
restore_checkpoints() {
    log_info "Restoring checkpoints..."

    mkdir -p "$TEST_DIR/restored"

    # Get the list of checkpoints and restore each one
    local checkpoints
    checkpoints=$("$COBBLE_BIN" checkpoint list --prefix "$PREFIX" --json 2>/dev/null | jq -r '.[].id' 2>/dev/null || echo "")

    if [ -z "$checkpoints" ]; then
        log_error "No checkpoints found to restore"
        return 1
    fi

    local idx=0
    for checkpoint_id in $checkpoints; do
        # Determine dest directory based on index
        local dest_name
        case $idx in
            0) dest_name="chaindata" ;;
            1) dest_name="state" ;;
            2) dest_name="ancient" ;;
            *) dest_name="db-$idx" ;;
        esac

        log_info "  Restoring $checkpoint_id to $dest_name..."
        "$COBBLE_BIN" checkpoint restore "$checkpoint_id" \
            --prefix "$PREFIX" \
            --dest "$TEST_DIR/restored/$dest_name"
        log_success "  Restored $checkpoint_id"

        idx=$((idx + 1))
    done

    log_success "All checkpoints restored"
}

# Verify restored data
verify_restored_data() {
    log_info "Verifying restored data..."

    # Write a Go program to verify data
    cat > "$TEST_DIR/verify_dbs.go" << 'GOEOF'
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"
)

func init() {
	// Disable automatic tiered storage configuration from environment
	pebble.ExternalConfigHook = nil
	pebble.PostOpenHook = nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: verify_dbs <base_dir>")
		os.Exit(1)
	}
	baseDir := os.Args[1]

	databases := []struct {
		name   string
		keys   int
		prefix string
	}{
		{"chaindata", 1000, "block-"},
		{"state", 2000, "account-"},
		{"ancient", 500, "ancient-"},
	}

	allPassed := true
	for _, db := range databases {
		dbPath := filepath.Join(baseDir, db.name)
		fmt.Printf("Verifying %s...\n", db.name)

		pdb, err := pebble.Open(dbPath, &pebble.Options{ReadOnly: true})
		if err != nil {
			fmt.Printf("  FAIL: Failed to open %s: %v\n", db.name, err)
			allPassed = false
			continue
		}

		verified := 0
		for i := 0; i < db.keys; i++ {
			key := fmt.Sprintf("%s%08d", db.prefix, i)
			expectedValue := fmt.Sprintf("value-%s-%08d-data", db.name, i)

			value, closer, err := pdb.Get([]byte(key))
			if err != nil {
				fmt.Printf("  FAIL: Key %s not found: %v\n", key, err)
				allPassed = false
				break
			}

			if string(value) != expectedValue {
				fmt.Printf("  FAIL: Key %s value mismatch\n", key)
				closer.Close()
				allPassed = false
				break
			}
			closer.Close()
			verified++
		}

		pdb.Close()

		if verified == db.keys {
			fmt.Printf("  OK: Verified %d/%d keys\n", verified, db.keys)
		}
	}

	if !allPassed {
		os.Exit(1)
	}
	fmt.Println("All verifications passed!")
}
GOEOF

    cd "$PROJECT_ROOT"
    go run -tags invariants "$TEST_DIR/verify_dbs.go" "$TEST_DIR/restored"

    log_success "All restored data verified"
}

# Create incremental checkpoint
create_incremental_checkpoint() {
    log_info "Testing incremental checkpoints..."

    # Add more data to chaindata
    cat > "$TEST_DIR/add_data.go" << 'GOEOF'
package main

import (
	"fmt"
	"os"

	"github.com/cockroachdb/pebble"
)

func init() {
	// Disable automatic tiered storage configuration from environment
	pebble.ExternalConfigHook = nil
	pebble.PostOpenHook = nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: add_data <db_path>")
		os.Exit(1)
	}
	dbPath := os.Args[1]

	pdb, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		fmt.Printf("Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer pdb.Close()

	// Add 100 more keys
	for i := 1000; i < 1100; i++ {
		key := fmt.Sprintf("block-%08d", i)
		value := fmt.Sprintf("value-chaindata-%08d-new-data", i)
		if err := pdb.Set([]byte(key), []byte(value), pebble.Sync); err != nil {
			fmt.Printf("Failed to write: %v\n", err)
			os.Exit(1)
		}
	}

	if err := pdb.Flush(); err != nil {
		fmt.Printf("Failed to flush: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Added 100 new keys to chaindata")
}
GOEOF

    cd "$PROJECT_ROOT"
    go run -tags invariants "$TEST_DIR/add_data.go" "$TEST_DIR/databases/chaindata"

    # Create v2 checkpoint
    log_info "Creating v2 checkpoint for chaindata..."
    "$COBBLE_BIN" checkpoint create \
        --prefix "$PREFIX" \
        --data-dir "$TEST_DIR/databases/chaindata" \
        --description "Test checkpoint v2 for chaindata"

    log_success "Incremental checkpoint created"
}

# Show blob deduplication stats
show_dedup_stats() {
    log_info "Showing blob statistics..."
    "$COBBLE_BIN" blob stats --prefix "$PREFIX"
}

# Run garbage collection
run_gc() {
    log_info "Running garbage collection (dry run)..."
    "$COBBLE_BIN" checkpoint gc --prefix "$PREFIX" --keep-last 2 --dry-run
}

# Main test flow
main() {
    echo ""
    echo "=========================================="
    echo "  Cobble Geth E2E Test"
    echo "=========================================="
    echo ""

    start_minio
    build_cobble
    export_env
    create_test_databases

    echo ""
    echo "--- Phase 1: Create Initial Checkpoints ---"
    create_snapshots

    echo ""
    echo "--- Phase 2: List and Verify ---"
    list_checkpoints
    show_catalog
    verify_catalog

    echo ""
    echo "--- Phase 3: Restore and Verify ---"
    restore_checkpoints
    verify_restored_data

    echo ""
    echo "--- Phase 4: Incremental Checkpoint ---"
    create_incremental_checkpoint
    list_checkpoints
    show_dedup_stats

    echo ""
    echo "--- Phase 5: Garbage Collection ---"
    run_gc

    echo ""
    echo "=========================================="
    echo -e "  ${GREEN}ALL TESTS PASSED!${NC}"
    echo "=========================================="
    echo ""
}

main "$@"
