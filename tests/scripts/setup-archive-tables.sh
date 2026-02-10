#!/bin/bash
#
# Setup Archive Database Tables
#
# Loads schemas from dump_master.js output into the archive database.
# Does not affect data in the source database.
#
# Usage: ./setup-archive-tables.sh [options]
# Options:
#   -h, --help          Show this help message
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTS_DIR="$(dirname "$SCRIPT_DIR")"

# Source .env file if it exists
if [ -f "$TESTS_DIR/.env" ]; then
    set -a
    source "$TESTS_DIR/.env"
    set +a
else
    echo "ERROR: .env file not found at $TESTS_DIR/.env"
    echo "Please copy dot.env to .env and configure it:"
    echo "  cp $TESTS_DIR/dot.env $TESTS_DIR/.env"
    echo "  nano $TESTS_DIR/.env"
    exit 1
fi

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    echo "Setup Archive Database Tables"
    echo ""
    echo "Loads schemas from dump_master.js output into the archive database."
    echo ""
    echo "Usage: $0"
    echo ""
    echo "Environment Variables (from .env or export):"
    echo "  TEST_ARCHIVE_HOST, TEST_ARCHIVE_PORT, TEST_ARCHIVE_USER, TEST_ARCHIVE_PASSWORD, TEST_ARCHIVE_DB"
    echo "  TEST_SOURCE_HOST, TEST_SOURCE_PORT, TEST_SOURCE_USER, TEST_SOURCE_PASSWORD, TEST_SOURCE_DB"
    echo "  DUMP_DIR"
    exit 0
fi

# Check if mysqlsh is available
if ! command -v mysqlsh &> /dev/null; then
    log_error "mysqlsh is not installed or not in PATH"
    exit 1
fi

log_info "============================================================"
log_info "Setting up archive tables"
log_info "============================================================"

# Step 1: Dump schemas from source
log_info "Step 1: Dumping schemas from source database..."

if ! mysqlsh --uri "$TEST_SOURCE_USER:$TEST_SOURCE_PASSWORD@$TEST_SOURCE_HOST:$TEST_SOURCE_PORT" --js -f "$SCRIPT_DIR/dump_master.js"; then
    log_error "Schema dump failed"
    exit 1
fi

# Step 2: Load schemas into archive
log_info ""
log_info "Step 2: Loading schemas into archive database..."

if ! mysqlsh --uri "$TEST_ARCHIVE_USER:$TEST_ARCHIVE_PASSWORD@$TEST_ARCHIVE_HOST:$TEST_ARCHIVE_PORT" --js -f "$SCRIPT_DIR/create_archive.js"; then
    log_error "Failed to load schemas into archive database"
    exit 1
fi

log_info ""
log_info "============================================================"
log_info "Archive tables setup complete!"
log_info "============================================================"
