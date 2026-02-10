#!/bin/bash
#
# Download and Extract Sakila Database
#
# This script downloads the MySQL Sakila sample database and extracts it
# in the tests folder for use in integration testing.
#
# Usage: ./get_sakila_db.sh [options]
# Options:
#   -h, --help          Show this help message
#   --force             Force re-download even if sakila-db exists
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTS_DIR="$(dirname "$SCRIPT_DIR")"
SAKILA_URL="https://downloads.mysql.com/docs/sakila-db.tar.gz"
SAKILA_TAR="$TESTS_DIR/sakila-db.tar.gz"
SAKILA_DIR="$TESTS_DIR/sakila-db"

FORCE=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            echo "Download and Extract Sakila Database"
            echo ""
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  -h, --help          Show this help message"
            echo "  --force             Force re-download even if sakila-db exists"
            echo ""
            echo "This script downloads the MySQL Sakila sample database from:"
            echo "  $SAKILA_URL"
            echo ""
            echo "The database will be extracted to:"
            echo "  $SAKILA_DIR/"
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

# Check if sakila-db already exists
if [ -d "$SAKILA_DIR" ] && [ "$FORCE" = false ]; then
    log_info "Sakila database already exists at $SAKILA_DIR"
    log_info "Use --force to re-download"
    exit 0
fi

# Check for curl or wget
if command -v curl &> /dev/null; then
    DOWNLOADER="curl -fsSL -o"
elif command -v wget &> /dev/null; then
    DOWNLOADER="wget -q -O"
else
    log_error "Neither curl nor wget is installed. Please install one of them."
    exit 1
fi

# Remove existing directory if force mode
if [ "$FORCE" = true ] && [ -d "$SAKILA_DIR" ]; then
    log_warn "Removing existing sakila-db directory..."
    rm -rf "$SAKILA_DIR"
fi

# Download the tar.gz file
log_info "Downloading Sakila database from $SAKILA_URL..."
if ! $DOWNLOADER "$SAKILA_TAR" "$SAKILA_URL"; then
    log_error "Failed to download Sakila database"
    exit 1
fi

# Extract the tar.gz file
log_info "Extracting Sakila database..."
if ! tar -xzf "$SAKILA_TAR" -C "$TESTS_DIR"; then
    log_error "Failed to extract Sakila database"
    rm -f "$SAKILA_TAR"
    exit 1
fi

# Remove the tar.gz file
log_info "Cleaning up..."
rm -f "$SAKILA_TAR"

# Verify extraction
if [ -d "$SAKILA_DIR" ] && [ -f "$SAKILA_DIR/sakila-schema.sql" ] && [ -f "$SAKILA_DIR/sakila-data.sql" ]; then
    log_info "Sakila database successfully downloaded and extracted to:"
    log_info "  $SAKILA_DIR"
    log_info ""
    log_info "Files available:"
    ls -1 "$SAKILA_DIR"/*.sql | while read -r file; do
        log_info "  - $(basename "$file")"
    done
else
    log_error "Extraction completed but expected files not found"
    exit 1
fi
