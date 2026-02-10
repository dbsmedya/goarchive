#!/bin/bash
#
# Setup Master Database Tables
#
# Loads Sakila sample database into the source (master) database using mysqlsh JS script.
# Downloads Sakila DB first if not present.
#
# Usage: ./setup-master-tables.sh [options]
# Options:
#   -h, --help          Show this help message
#   --force             Force re-download of Sakila DB even if exists
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
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

FORCE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            echo "Setup Master Database Tables (Sakila)"
            echo ""
            echo "Loads Sakila sample database into the source database."
            echo ""
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  -h, --help          Show this help message"
            echo "  --force             Force re-download of Sakila DB"
            echo ""
            echo "Environment Variables (from .env or export):"
            echo "  TEST_SOURCE_HOST, TEST_SOURCE_PORT, TEST_SOURCE_USER, TEST_SOURCE_PASSWORD, TEST_SOURCE_DB"
            echo "  SAKILA_DIR"
            exit 0
            ;;
        --force)
            FORCE=true
            shift
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Check if mysqlsh is available
if ! command -v mysqlsh &> /dev/null; then
    log_error "mysqlsh is not installed or not in PATH"
    exit 1
fi

log_info "============================================================"
log_info "Setting up master tables (Sakila)"
log_info "============================================================"

# Step 1: Download Sakila DB if not exists
SAKILA_DIR="${SAKILA_DIR:-$TESTS_DIR/sakila-db}"

if [ ! -d "$SAKILA_DIR" ] || [ "$FORCE" = true ]; then
    log_info "Step 1: Downloading Sakila database..."
    if [ "$FORCE" = true ]; then
        "$SCRIPT_DIR/get_sakila_db.sh" --force
    else
        "$SCRIPT_DIR/get_sakila_db.sh"
    fi
else
    log_info "Step 1: Sakila database already exists, skipping download"
fi

# Step 2: Load Sakila into source database using JS script
log_info ""
log_info "Step 2: Loading Sakila into source database..."

if ! mysqlsh --uri "$TEST_SOURCE_USER:$TEST_SOURCE_PASSWORD@$TEST_SOURCE_HOST:$TEST_SOURCE_PORT" --js -f "$SCRIPT_DIR/load_sakila.js"; then
    log_error "Failed to load Sakila database"
    exit 1
fi

log_info ""
log_info "============================================================"
log_info "Master tables setup complete!"
log_info "============================================================"
