#!/bin/bash
#
# Check Test Database Servers
#
# Verifies connectivity to all test database servers and displays their status.
#
# Usage: ./check-servers.sh [options]
# Options:
#   -h, --help          Show this help message
#

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

# Default configuration
SOURCE_HOST="${TEST_SOURCE_HOST:-127.0.0.1}"
SOURCE_PORT="${TEST_SOURCE_PORT:-3305}"
SOURCE_USER="${TEST_SOURCE_USER:-root}"
SOURCE_PASS="${TEST_SOURCE_PASSWORD:-${MYSQL_ROOT_PASSWORD:-}}"
SOURCE_DB="${TEST_SOURCE_DB:-db1}"

ARCHIVE_HOST="${TEST_ARCHIVE_HOST:-127.0.0.1}"
ARCHIVE_PORT="${TEST_ARCHIVE_PORT:-3307}"
ARCHIVE_USER="${TEST_ARCHIVE_USER:-root}"
ARCHIVE_PASS="${TEST_ARCHIVE_PASSWORD:-${MYSQL_ROOT_PASSWORD:-}}"
ARCHIVE_DB="${TEST_ARCHIVE_DB:-sakila_archive}"

REPLICA_HOST="${TEST_REPLICA_HOST:-127.0.0.1}"
REPLICA_PORT="${TEST_REPLICA_PORT:-3308}"
REPLICA_USER="${TEST_REPLICA_USER:-root}"
REPLICA_PASS="${TEST_REPLICA_PASSWORD:-${MYSQL_ROOT_PASSWORD:-}}"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_ok() { echo -e "${GREEN}✓${NC} $1"; }
log_fail() { echo -e "${RED}✗${NC} $1"; }
log_info() { echo -e "${BLUE}ℹ${NC} $1"; }
log_warn() { echo -e "${YELLOW}⚠${NC} $1"; }

# Parse arguments
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    echo "Check Test Database Servers"
    echo ""
    echo "Verifies connectivity to all test database servers."
    echo ""
    echo "Usage: $0"
    echo ""
    echo "Environment Variables (from .env):"
    echo "  TEST_SOURCE_HOST, TEST_SOURCE_PORT, TEST_SOURCE_USER, TEST_SOURCE_PASSWORD"
    echo "  TEST_ARCHIVE_HOST, TEST_ARCHIVE_PORT, TEST_ARCHIVE_USER, TEST_ARCHIVE_PASSWORD"
    echo "  TEST_REPLICA_HOST, TEST_REPLICA_PORT, TEST_REPLICA_USER, TEST_REPLICA_PASSWORD"
    exit 0
fi

# Check if mysqlsh is available
if ! command -v mysqlsh &> /dev/null; then
    log_fail "mysqlsh is not installed or not in PATH"
    exit 1
fi

echo "======================================"
echo "GoArchive Test Environment Check"
echo "======================================"
echo ""

# Check Source Server
echo "Source Database ($SOURCE_DB)"
echo "  Host: $SOURCE_HOST:$SOURCE_PORT"
if mysqlsh --host="$SOURCE_HOST" --port="$SOURCE_PORT" --user="$SOURCE_USER" \
           --password="$SOURCE_PASS" --sql -e "SELECT 1" &> /dev/null; then
    log_ok "Connected"
    
    version=$(mysqlsh --host="$SOURCE_HOST" --port="$SOURCE_PORT" --user="$SOURCE_USER" \
                      --password="$SOURCE_PASS" --sql -e "SELECT VERSION();" 2>/dev/null | tail -1)
    log_info "Version: $version"
    
    if mysqlsh --host="$SOURCE_HOST" --port="$SOURCE_PORT" --user="$SOURCE_USER" \
               --password="$SOURCE_PASS" --sql -e "USE $SOURCE_DB;" &> /dev/null; then
        log_ok "Database '$SOURCE_DB' exists"
    else
        log_warn "Database '$SOURCE_DB' does not exist"
        log_info "Run: ./setup-master-tables.sh"
    fi
else
    log_fail "Cannot connect"
    log_info "Check if MySQL server is running on port $SOURCE_PORT"
fi
echo ""

# Check Archive Server
echo "Archive Database ($ARCHIVE_DB)"
echo "  Host: $ARCHIVE_HOST:$ARCHIVE_PORT"
if mysqlsh --host="$ARCHIVE_HOST" --port="$ARCHIVE_PORT" --user="$ARCHIVE_USER" \
           --password="$ARCHIVE_PASS" --sql -e "SELECT 1" &> /dev/null; then
    log_ok "Connected"
    
    version=$(mysqlsh --host="$ARCHIVE_HOST" --port="$ARCHIVE_PORT" --user="$ARCHIVE_USER" \
                      --password="$ARCHIVE_PASS" --sql -e "SELECT VERSION();" 2>/dev/null | tail -1)
    log_info "Version: $version"
    
    if mysqlsh --host="$ARCHIVE_HOST" --port="$ARCHIVE_PORT" --user="$ARCHIVE_USER" \
               --password="$ARCHIVE_PASS" --sql -e "USE $ARCHIVE_DB;" &> /dev/null; then
        log_ok "Database '$ARCHIVE_DB' exists"
        
        tables=$(mysqlsh --host="$ARCHIVE_HOST" --port="$ARCHIVE_PORT" --user="$ARCHIVE_USER" \
                         --password="$ARCHIVE_PASS" --sql \
                         -e "SHOW TABLES FROM $ARCHIVE_DB;" 2>/dev/null | tail -n +2 | tr '\n' ', ')
        if [ -n "$tables" ]; then
            log_info "Tables: ${tables%, }"
        else
            log_warn "No tables found"
            log_info "Run: ./setup-archive-tables.sh"
        fi
    else
        log_warn "Database '$ARCHIVE_DB' does not exist"
        log_info "Run: ./setup-archive-tables.sh"
    fi
else
    log_fail "Cannot connect"
    log_info "Check if MySQL server is running on port $ARCHIVE_PORT"
fi
echo ""

# Check Replica Server (optional)
echo "Replica Database (optional)"
echo "  Host: $REPLICA_HOST:$REPLICA_PORT"
if mysqlsh --host="$REPLICA_HOST" --port="$REPLICA_PORT" --user="$REPLICA_USER" \
           --password="$REPLICA_PASS" --sql -e "SELECT 1" &> /dev/null; then
    log_ok "Connected"
    
    version=$(mysqlsh --host="$REPLICA_HOST" --port="$REPLICA_PORT" --user="$REPLICA_USER" \
                      --password="$REPLICA_PASS" --sql -e "SELECT VERSION();" 2>/dev/null | tail -1)
    log_info "Version: $version"
else
    log_warn "Cannot connect (optional for most tests)"
fi
echo ""

echo "======================================"
echo "Check complete!"
echo "======================================"
