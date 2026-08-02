#!/bin/bash
#
# Setup Archive Database Tables
#
# Loads schemas from dump_master.js output into the archive database.
# Does not affect data in the source database.
#
# Idempotent: re-running it is safe and converges on the same result. It first
# clears DUMP_DIR and DROPS the destination schema, because both mysqlsh steps
# refuse to overwrite anything a previous run left behind.
#
# DESTRUCTIVE to the destination schema (TEST_DEST_DB, default sakila_archive)
# by design -- that is what makes the end state deterministic. The source is
# never touched.
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

# Default SAKILA_DIR to repo-relative so mysqlsh scripts find the SQL files.
if [ -z "${SAKILA_DIR:-}" ]; then
    export SAKILA_DIR="$TESTS_DIR/sakila-db"
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
    echo "Idempotent: clears DUMP_DIR and DROPS the destination schema first,"
    echo "so it can be re-run. The source database is never modified."
    echo ""
    echo "Usage: $0"
    echo ""
    echo "Environment Variables (from .env or export):"
    echo "  TEST_DEST_HOST, TEST_DEST_PORT, TEST_DEST_USER, TEST_DEST_PASSWORD, TEST_DEST_DB"
    echo "  TEST_SOURCE_HOST, TEST_SOURCE_PORT, TEST_SOURCE_USER, TEST_SOURCE_PASSWORD, TEST_SOURCE_DB"
    echo "  DUMP_DIR"
    exit 0
fi

# Check if mysqlsh is available
if ! command -v mysqlsh &> /dev/null; then
    log_error "mysqlsh is not installed or not in PATH"
    exit 1
fi

# Defaults must match the JS scripts, which resolve the same variables.
DUMP_DIR="${DUMP_DIR:-/tmp/db1_schema_dump}"
DEST_DB="${TEST_DEST_DB:-sakila_archive}"
DEST_HOST="${TEST_DEST_HOST:-127.0.0.1}"
DEST_PORT="${TEST_DEST_PORT:-3307}"
DEST_USER="${TEST_DEST_USER:-root}"
DEST_PASS="${TEST_DEST_PASSWORD:-${MYSQL_ROOT_PASSWORD:-}}"
SRC_PASS="${TEST_SOURCE_PASSWORD:-${MYSQL_ROOT_PASSWORD:-}}"

# Without a password mysqlsh falls back to ~/.my.cnf and may connect as someone
# else, so refuse rather than run against an identity nobody chose. Every
# mysqlsh call below passes --no-defaults for the same reason.
if [[ -z "$DEST_PASS" || -z "$SRC_PASS" ]]; then
    log_error "No password: set MYSQL_ROOT_PASSWORD (or TEST_SOURCE_PASSWORD / TEST_DEST_PASSWORD) in $TESTS_DIR/.env"
    exit 1
fi

log_info "============================================================"
log_info "Setting up archive tables"
log_info "============================================================"
log_info "Destination: ${DEST_USER}@${DEST_HOST}:${DEST_PORT} schema \`${DEST_DB}\`"
log_info "Dump dir:    ${DUMP_DIR}"

# Step 0: make the run repeatable.
#
# Both later steps refuse to overwrite what a previous run left behind --
# dumpSchemas aborts if DUMP_DIR exists and is non-empty, and loadDump aborts
# with "Duplicate objects found in destination database" if the schema is
# already there. Clearing both is what makes this script re-runnable; without
# it, it worked exactly once.
log_info ""
log_info "Step 0: Clearing previous dump and destination schema..."
rm -rf "${DUMP_DIR:?}"
log_info "  removed ${DUMP_DIR}"

if ! mysqlsh --no-defaults --quiet-start=2 \
    --uri "$DEST_USER:$DEST_PASS@$DEST_HOST:$DEST_PORT" \
    --sql -e "DROP DATABASE IF EXISTS \`${DEST_DB}\`;"; then
    log_error "Could not drop destination schema \`${DEST_DB}\`"
    exit 1
fi
log_info "  dropped schema \`${DEST_DB}\` (recreated by the load)"

# Step 1: Dump schemas from source
log_info ""
log_info "Step 1: Dumping schemas from source database..."

if ! mysqlsh --no-defaults --quiet-start=2 \
    --uri "$TEST_SOURCE_USER:$SRC_PASS@$TEST_SOURCE_HOST:$TEST_SOURCE_PORT" \
    --js -f "$SCRIPT_DIR/dump_master.js"; then
    log_error "Schema dump failed"
    exit 1
fi

# Step 2: Load schemas into archive
log_info ""
log_info "Step 2: Loading schemas into archive database..."

if ! mysqlsh --no-defaults --quiet-start=2 \
    --uri "$DEST_USER:$DEST_PASS@$DEST_HOST:$DEST_PORT" \
    --js -f "$SCRIPT_DIR/create_archive.js"; then
    log_error "Failed to load schemas into archive database"
    exit 1
fi

log_info ""
log_info "============================================================"
log_info "Archive tables setup complete!"
log_info "============================================================"
