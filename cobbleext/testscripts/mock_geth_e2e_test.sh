#!/bin/bash
# Mock Geth E2E Test Script
# This script tests the cobble CLI with MinIO using mock_geth for multi-database scenarios.
#
# Prerequisites:
# - Docker installed and running
# - Go installed
#
# Usage:
#   ./mock_geth_e2e_test.sh
#
# The script will:
# 1. Start a MinIO container
# 2. Build the cobble CLI and mock_geth
# 3. Use mock_geth to create multiple Pebble databases (chaindata, state, ancient)
# 4. Use mock_geth's admin API to create checkpoints
# 5. Upload checkpoints to S3 using cobble CLI
# 6. List and verify snapshots
# 7. Restore from snapshots
# 8. Verify restored data
# 9. Test incremental snapshots (verify deduplication)
# 10. Clean up

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
MINIO_CONTAINER_NAME="cobble-mock-geth-test-minio"
MINIO_PORT=9200
MINIO_CONSOLE_PORT=9201
MINIO_ROOT_USER="minioadmin"
MINIO_ROOT_PASSWORD="minioadmin"
BUCKET_NAME="cobble-geth-test"
PREFIX="geth-multidb/"
MOCK_GETH_PORT=9999

# Paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COBBLE_BIN="/tmp/cobble-mock-geth-e2e"
MOCK_GETH_BIN="/tmp/mock_geth-e2e"
TEST_DIR="/tmp/cobble-mock-geth-e2e-test-$$"

# Server PID
MOCK_GETH_PID=""

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

    # Stop mock_geth server
    if [ -n "$MOCK_GETH_PID" ]; then
        kill "$MOCK_GETH_PID" 2>/dev/null || true
        wait "$MOCK_GETH_PID" 2>/dev/null || true
    fi

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
        if curl -s "http://localhost:$MINIO_PORT/minio/health/live" 2>/dev/null | grep -q "OK"; then
            break
        fi
        sleep 1
    done

    # Create bucket using mc
    log_info "Creating bucket: $BUCKET_NAME"
    docker exec "$MINIO_CONTAINER_NAME" mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1
    docker exec "$MINIO_CONTAINER_NAME" mc mb "local/$BUCKET_NAME" >/dev/null 2>&1 || true

    log_success "MinIO started at http://localhost:$MINIO_PORT"
}

# Build binaries
build_binaries() {
    log_info "Building cobble CLI..."
    cd "$PROJECT_ROOT"
    go build -tags "s3 cloud" -o "$COBBLE_BIN" ./cmd/cobble
    log_success "Cobble CLI built: $COBBLE_BIN"

    log_info "Building mock_geth..."
    go build -tags "s3 cloud" -o "$MOCK_GETH_BIN" ./cobbleext/testscripts/mock_geth.go
    log_success "mock_geth built: $MOCK_GETH_BIN"
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

# Initialize and populate databases using mock_geth
setup_databases() {
    log_info "Initializing databases with mock_geth..."
    mkdir -p "$TEST_DIR"

    "$MOCK_GETH_BIN" init "$TEST_DIR/data"
    log_success "Databases initialized"

    log_info "Writing initial data (1000 records each)..."
    "$MOCK_GETH_BIN" write "$TEST_DIR/data" 1000
    log_success "Initial data written"
}

# Start mock_geth server
start_mock_geth_server() {
    log_info "Starting mock_geth admin server..."
    "$MOCK_GETH_BIN" serve "$TEST_DIR/data" "127.0.0.1:$MOCK_GETH_PORT" &
    MOCK_GETH_PID=$!

    # Wait for server to be ready
    for i in {1..10}; do
        if curl -s "http://127.0.0.1:$MOCK_GETH_PORT/health" 2>/dev/null | grep -q "ok"; then
            break
        fi
        sleep 1
    done

    log_success "mock_geth server running at http://127.0.0.1:$MOCK_GETH_PORT (PID: $MOCK_GETH_PID)"
}

# Create checkpoints via admin API
create_checkpoints_via_api() {
    log_info "Creating checkpoints via mock_geth admin API..."

    CHECKPOINT_DIR="$TEST_DIR/checkpoints/v1"
    mkdir -p "$CHECKPOINT_DIR"

    response=$(curl -s -X POST "http://127.0.0.1:$MOCK_GETH_PORT/checkpoint" \
        -H "Content-Type: application/json" \
        -d "{\"dest_dir\": \"$CHECKPOINT_DIR\"}")

    echo "$response" | jq .

    # Verify checkpoints were created
    for db in chaindata state ancient; do
        if [ ! -d "$CHECKPOINT_DIR/$db" ]; then
            log_error "Checkpoint for $db not created!"
            exit 1
        fi
    done

    log_success "Checkpoints created at $CHECKPOINT_DIR"
}

# Upload checkpoints to S3 using cobble CLI
upload_checkpoints() {
    log_info "Uploading checkpoints to S3 using cobble CLI..."

    for db in chaindata state ancient; do
        log_info "  Uploading $db checkpoint..."
        "$COBBLE_BIN" checkpoint create \
            --prefix "$PREFIX" \
            --data-dir "$TEST_DIR/checkpoints/v1/$db" \
            --description "Initial checkpoint for $db"
        log_success "  Uploaded $db"
    done

    log_success "All checkpoints uploaded to S3"
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

    # Remove any existing restored directory to start fresh
    rm -rf "$TEST_DIR/restored"
    mkdir -p "$TEST_DIR/restored"

    # Get the list of checkpoints with their database info
    local checkpoints_json
    checkpoints_json=$("$COBBLE_BIN" checkpoint list --prefix "$PREFIX" --json 2>/dev/null)

    if [ -z "$checkpoints_json" ] || [ "$checkpoints_json" = "null" ]; then
        log_error "No checkpoints found to restore"
        return 1
    fi

    # Each checkpoint contains databases, restore preserves the db name structure
    # So we just restore to the base restored directory
    # Note: Don't use --force as it removes the entire dest directory,
    # which would delete previously restored databases
    local checkpoint_ids
    checkpoint_ids=$(echo "$checkpoints_json" | jq -r '.[].id')

    for checkpoint_id in $checkpoint_ids; do
        # Get the database names in this checkpoint
        local db_names
        db_names=$(echo "$checkpoints_json" | jq -r ".[] | select(.id == \"$checkpoint_id\") | .databases[].name")

        log_info "  Restoring $checkpoint_id (contains: $db_names)..."
        "$COBBLE_BIN" checkpoint restore "$checkpoint_id" \
            --prefix "$PREFIX" \
            --dest "$TEST_DIR/restored"
        log_success "  Restored $checkpoint_id"
    done

    # Show restored structure
    log_info "Restored directory structure:"
    ls -la "$TEST_DIR/restored/"

    log_success "All checkpoints restored"
}

# Verify restored data using mock_geth
verify_restored_data() {
    log_info "Verifying restored data with mock_geth..."
    "$MOCK_GETH_BIN" verify "$TEST_DIR/restored"
    log_success "Restored data verification passed"
}

# Add more data and create incremental checkpoint
create_incremental_checkpoint() {
    log_info "Adding more data for incremental checkpoint test..."

    # Stop the current server to add data
    if [ -n "$MOCK_GETH_PID" ]; then
        kill "$MOCK_GETH_PID" 2>/dev/null || true
        wait "$MOCK_GETH_PID" 2>/dev/null || true
        MOCK_GETH_PID=""
    fi

    # Add more data
    "$MOCK_GETH_BIN" write "$TEST_DIR/data" 100
    log_success "Added 100 more records to each database"

    # Restart server
    start_mock_geth_server

    # Create new checkpoints
    log_info "Creating v2 checkpoints..."
    CHECKPOINT_DIR="$TEST_DIR/checkpoints/v2"
    mkdir -p "$CHECKPOINT_DIR"

    curl -s -X POST "http://127.0.0.1:$MOCK_GETH_PORT/checkpoint" \
        -H "Content-Type: application/json" \
        -d "{\"dest_dir\": \"$CHECKPOINT_DIR\"}" | jq .

    # Upload v2 checkpoints
    log_info "Uploading v2 checkpoints..."
    for db in chaindata state ancient; do
        log_info "  Uploading $db v2 checkpoint..."
        "$COBBLE_BIN" checkpoint create \
            --prefix "$PREFIX" \
            --data-dir "$CHECKPOINT_DIR/$db" \
            --description "Incremental checkpoint v2 for $db"
        log_success "  Uploaded $db v2"
    done

    log_success "Incremental checkpoints created and uploaded"
}

# Show blob deduplication stats
show_blob_stats() {
    log_info "Showing blob statistics (should show deduplication)..."
    "$COBBLE_BIN" blob list --prefix "$PREFIX"
    echo ""
    log_info "Catalog summary:"
    "$COBBLE_BIN" catalog show --prefix "$PREFIX"
}

# Run garbage collection (dry run)
run_gc() {
    log_info "Running garbage collection (dry run)..."
    "$COBBLE_BIN" checkpoint gc --prefix "$PREFIX" --keep-last 3 --dry-run
}

# Main test flow
main() {
    echo ""
    echo "==========================================="
    echo "  Mock Geth Multi-DB E2E Test"
    echo "==========================================="
    echo ""

    start_minio
    build_binaries
    export_env
    setup_databases
    start_mock_geth_server

    echo ""
    echo "--- Phase 1: Create Checkpoints via Admin API ---"
    create_checkpoints_via_api

    echo ""
    echo "--- Phase 2: Upload to S3 ---"
    upload_checkpoints
    list_checkpoints

    echo ""
    echo "--- Phase 3: Catalog Operations ---"
    show_catalog
    verify_catalog

    echo ""
    echo "--- Phase 4: Restore and Verify ---"
    restore_checkpoints
    verify_restored_data

    echo ""
    echo "--- Phase 5: Incremental Checkpoint (Deduplication Test) ---"
    create_incremental_checkpoint
    list_checkpoints
    show_blob_stats

    echo ""
    echo "--- Phase 6: Garbage Collection ---"
    run_gc

    echo ""
    echo "==========================================="
    echo -e "  ${GREEN}ALL TESTS PASSED!${NC}"
    echo "==========================================="
    echo ""
}

main "$@"
