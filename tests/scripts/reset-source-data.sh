#!/bin/bash
#
# Reset Source Database Data
#
# Drops and recreates the source database from Sakila SQL files.
#
# Usage: ./reset-source-data.sh [options]
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
    echo "Reset Source Database Data"
    echo ""
    echo "Drops and recreates the source database from Sakila SQL files."
    echo ""
    echo "Usage: $0"
    echo ""
    echo "Environment Variables (from .env or export):"
    echo "  TEST_SOURCE_HOST, TEST_SOURCE_PORT, TEST_SOURCE_USER, TEST_SOURCE_PASSWORD, TEST_SOURCE_DB"
    exit 0
fi

# Check if mysqlsh is available
if ! command -v mysqlsh &> /dev/null; then
    log_error "mysqlsh is not installed or not in PATH"
    exit 1
fi

log_info "============================================================"
log_info "Resetting source database"
log_info "============================================================"

# Run the reset script
if ! mysqlsh --uri "$TEST_SOURCE_USER:$TEST_SOURCE_PASSWORD@$TEST_SOURCE_HOST:$TEST_SOURCE_PORT" --js -f "$SCRIPT_DIR/reset_source.js"; then
    log_error "Failed to reset source database"
    exit 1
fi

log_info ""
log_info "============================================================"
log_info "Source database reset complete!"
log_info "============================================================"
