#!/bin/bash
#
# Run GoArchive Tests
#
# Sets up the test environment and runs all or specific tests.
#
# Usage: ./run-tests.sh [options] [test-args]
# Options:
#   -h, --help          Show this help message
#   --setup             Setup/reset test environment (docker + databases)
#   --sakila            Run Sakila integration tests (1-5)
#   -t, --test NUM      Run only specific Sakila test (1-5, requires --sakila)
#   --unit-only         Run only Go unit tests
#   --integration-only  Run only Go integration tests
#   -v, --verbose       Verbose output
#   --skip-docker       Skip docker compose operations (use existing DBs)
#
# Any additional arguments are passed to 'go test'
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTS_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$TESTS_DIR")"

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
BLUE='\033[0;34m'
NC='\033[0m'

SETUP=false
SAKILA=false
SPECIFIC_TEST=""
UNIT_ONLY=false
INTEGRATION_ONLY=false
VERBOSE=""
SKIP_DOCKER=false
GO_TEST_ARGS=""

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step() { echo -e "${BLUE}[STEP]${NC} $1"; }
log_header() { echo -e "${BLUE}$1${NC}"; }

log_verbose() {
    if [[ -n "$VERBOSE" ]]; then
        echo -e "${BLUE}[VERBOSE]${NC} $1"
    fi
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            echo "Run GoArchive Tests"
            echo ""
            echo "Sets up the test environment and runs all or specific tests."
            echo ""
            echo "Usage: $0 [options] [test-args]"
            echo ""
            echo "Options:"
            echo "  -h, --help          Show this help message"
            echo "  --setup             Setup/reset test environment (docker + databases)"
            echo "  --sakila            Run Sakila integration tests (1-5)"
            echo "  -t, --test NUM      Run only specific Sakila test (1-5, requires --sakila)"
            echo "  --unit-only         Run only Go unit tests"
            echo "  --integration-only  Run only Go integration tests"
            echo "  -v, --verbose       Verbose output"
            echo "  --skip-docker       Skip docker compose operations (use existing DBs)"
            echo ""
            echo "Examples:"
            echo "  $0 --setup                    # Full setup: docker + databases"
            echo "  $0 --setup --sakila           # Setup and run all Sakila tests"
            echo "  $0 --sakila -t 1              # Run only Sakila test 1"
            echo "  $0 --integration-only         # Run Go integration tests only"
            echo "  $0 --unit-only                # Run Go unit tests only"
            exit 0
            ;;
        --setup)
            SETUP=true
            shift
            ;;
        --sakila)
            SAKILA=true
            shift
            ;;
        -t|--test)
            SPECIFIC_TEST="$2"
            shift 2
            ;;
        --unit-only)
            UNIT_ONLY=true
            shift
            ;;
        --integration-only)
            INTEGRATION_ONLY=true
            shift
            ;;
        -v|--verbose)
            VERBOSE="-v"
            shift
            ;;
        --skip-docker)
            SKIP_DOCKER=true
            shift
            ;;
        *)
            GO_TEST_ARGS="$GO_TEST_ARGS $1"
            shift
            ;;
    esac
done

# Database configuration
MYSQL_PASS="${MYSQL_ROOT_PASSWORD:-}"
SOURCE_HOST="${TEST_SOURCE_HOST:-127.0.0.1}"
SOURCE_PORT="${TEST_SOURCE_PORT:-3305}"
SOURCE_USER="${TEST_SOURCE_USER:-root}"
SOURCE_DB="${TEST_SOURCE_DB:-db1}"
ARCHIVE_HOST="${TEST_ARCHIVE_HOST:-127.0.0.1}"
ARCHIVE_PORT="${TEST_ARCHIVE_PORT:-3307}"
ARCHIVE_USER="${TEST_ARCHIVE_USER:-root}"
ARCHIVE_DB="${TEST_ARCHIVE_DB:-sakila_archive}"

# Setup test environment
setup_environment() {
    log_step "Setting up test environment..."
    
    if [ "$SKIP_DOCKER" = false ]; then
        log_info "Stopping existing docker containers..."
        cd "$TESTS_DIR"
        docker compose down 2>/dev/null || true
        
        log_info "Cleaning up database data..."
        rm -rf "$TESTS_DIR/docker_files/db_data"
        
        log_info "Starting docker containers..."
        docker compose up -d
        
        log_info "Waiting for databases to be ready..."
        sleep 10
        
        # Wait for source database
        local retries=0
        while [ $retries -lt 30 ]; do
            if mysqlsh --host="$SOURCE_HOST" --port="$SOURCE_PORT" --user="$SOURCE_USER" --password="$MYSQL_PASS" --sql -e "SELECT 1" &>/dev/null; then
                log_info "Source database is ready"
                break
            fi
            retries=$((retries + 1))
            log_info "Waiting for source database... ($retries/30)"
            sleep 2
        done
        
        if [ $retries -eq 30 ]; then
            log_error "Source database failed to start"
            exit 1
        fi
    fi
    
    # Check servers
    log_info "Checking database connections..."
    "$SCRIPT_DIR/check-servers.sh"
    
    # Load Sakila into source
    log_info "Loading Sakila database into source..."
    "$SCRIPT_DIR/get_sakila_db.sh"
    
    # Create source database and load Sakila using SQL mode
    local source_uri="$SOURCE_USER:$MYSQL_PASS@$SOURCE_HOST:$SOURCE_PORT"
    
    log_info "Creating database '$SOURCE_DB'..."
    if ! mysqlsh --uri "$source_uri" --sql -e "CREATE DATABASE IF NOT EXISTS \`$SOURCE_DB\`;"; then
        log_error "Failed to create source database"
        exit 1
    fi
    
    log_info "Loading Sakila schema..."
    if ! mysqlsh --uri "$source_uri" --sql < "$TESTS_DIR/sakila-db/sakila-schema.sql"; then
        log_error "Failed to load Sakila schema"
        exit 1
    fi
    
    log_info "Loading Sakila data..."
    if ! mysqlsh --uri "$source_uri/sakila" --sql < "$TESTS_DIR/sakila-db/sakila-data.sql"; then
        log_error "Failed to load Sakila data"
        exit 1
    fi
    
    log_info "Sakila database loaded successfully"
    
    # Dump and load schemas
    log_info "Dumping schemas from source..."
    # Clean up old dump directory if it exists
    rm -rf /tmp/db1_schema_dump
    if ! mysqlsh --uri "$SOURCE_USER:$MYSQL_PASS@$SOURCE_HOST:$SOURCE_PORT" --js -f "$SCRIPT_DIR/dump_master.js"; then
        log_error "Schema dump failed"
        exit 1
    fi
    
    log_info "Loading schemas into archive..."
    # Enable local_infile for util.loadDump to work
    local archive_uri="$ARCHIVE_USER:$MYSQL_PASS@$ARCHIVE_HOST:$ARCHIVE_PORT"
    mysqlsh --uri "$archive_uri" --sql -e "SET GLOBAL local_infile = 1;" 2>/dev/null || true
    # Drop existing archive database to avoid conflicts
    mysqlsh --uri "$archive_uri" --sql -e "DROP DATABASE IF EXISTS \`$ARCHIVE_DB\`;" 2>/dev/null || true
    if ! mysqlsh --uri "$archive_uri" --js -f "$SCRIPT_DIR/create_archive.js"; then
        log_error "Schema load failed"
        exit 1
    fi
    
    log_info "Environment setup complete!"
}

# Get row count from a table
get_row_count() {
    local host=$1
    local port=$2
    local user=$3
    local db=$4
    local table=$5
    
    local count
    count=$(mysqlsh --host="$host" --port="$port" --user="$user" --password="$MYSQL_PASS" --sql \
        -e "SELECT COUNT(*) FROM \`$db\`.\`$table\`;" 2>/dev/null | tail -1)
    echo "${count:-0}"
}

# Reset source database
reset_source_database() {
    log_info "Resetting source database..."
    
    # Drop and recreate database using JS script
    if ! mysqlsh --uri "$SOURCE_USER:$MYSQL_PASS@$SOURCE_HOST:$SOURCE_PORT" --js -f "$SCRIPT_DIR/reset_source.js"; then
        log_error "Failed to reset source database"
        return 1
    fi
    
    # Reload Sakila schema and data using SQL mode
    local source_uri="$SOURCE_USER:$MYSQL_PASS@$SOURCE_HOST:$SOURCE_PORT"
    log_info "Reloading Sakila schema..."
    if ! mysqlsh --uri "$source_uri" --sql < "$TESTS_DIR/sakila-db/sakila-schema.sql"; then
        log_error "Failed to reload Sakila schema"
        return 1
    fi
    
    log_info "Reloading Sakila data..."
    if ! mysqlsh --uri "$source_uri/sakila" --sql < "$TESTS_DIR/sakila-db/sakila-data.sql"; then
        log_error "Failed to reload Sakila data"
        return 1
    fi
    
    log_info "Source database reset complete"
}

# Run archive job using goarchive binary
run_archive_job() {
    local config_file="$1"
    local full_config_path="$TESTS_DIR/configs/$config_file"
    
    if [[ ! -f "$full_config_path" ]]; then
        log_error "Config file not found: $full_config_path"
        return 1
    fi
    
    log_info "Running archive job with config: $config_file"
    
    # Build goarchive if needed
    if [[ ! -f "$PROJECT_ROOT/bin/goarchive" ]]; then
        log_info "Building goarchive binary..."
        cd "$PROJECT_ROOT"
        go build -o bin/goarchive ./cmd/goarchive
    fi
    
    cd "$PROJECT_ROOT"
    if ! ./bin/goarchive -config "$full_config_path" 2>&1; then
        log_error "Archive job failed"
        return 1
    fi
    
    return 0
}

# Run specific Sakila test
run_sakila_test() {
    local test_num=$1
    local test_name=""
    local test_desc=""
    local config_file=""
    local tables=""
    local start_time end_time duration
    local test_result="PASS"
    local test_error=""
    
    case $test_num in
        1)
            test_name="Test01_OneToOne"
            test_desc="1-1 Relationship (film → film_text)"
            config_file="test01_one_to_one.yaml"
            tables="film film_text"
            ;;
        2)
            test_name="Test02_OneToMany"
            test_desc="1-N Relationship (language → film)"
            config_file="test02_one_to_many.yaml"
            tables="language film"
            ;;
        3)
            test_name="Test03_OneToManyMultiple"
            test_desc="1-N Multiple Children (film → inventory + film_actor + film_category)"
            config_file="test03_one_to_many_multiple.yaml"
            tables="film inventory film_actor film_category"
            ;;
        4)
            test_name="Test04_OneToManyTwoNested"
            test_desc="1-N Two Nested (country → city → address)"
            config_file="test04_one_to_many_two_nested.yaml"
            tables="country city address"
            ;;
        5)
            test_name="Test05_OneToManyThreeNestedWithOneToOne"
            test_desc="1-N Three Nested with 1-1"
            config_file="test05_one_to_many_three_nested.yaml"
            tables="country city address customer film film_text"
            ;;
        *)
            log_error "Invalid test number: $test_num"
            return 1
            ;;
    esac
    
    log_header ""
    log_header "========================================"
    log_header "Running Test $test_num: $test_desc"
    log_header "========================================"
    
    start_time=$(date +%s)
    
    # Create log file
    mkdir -p "$TESTS_DIR/results"
    local log_file="$TESTS_DIR/results/test_${test_num}.log"
    echo "Running $test_name" > "$log_file"
    echo "Description: $test_desc" >> "$log_file"
    echo "Started: $(date)" >> "$log_file"
    echo "" >> "$log_file"
    
    # Step 1: Reset source database
    log_info "[STEP 1] Resetting source database..."
    reset_source_database >> "$log_file" 2>&1 || {
        log_error "Failed to reset source database"
        test_result="FAIL"
        test_error="Source database reset failed"
        end_time=$(date +%s)
        duration=$((end_time - start_time))
        echo "" >> "$log_file"
        echo "Result: $test_result" >> "$log_file"
        echo "Duration: ${duration}s" >> "$log_file"
        return 1
    }
    
    # Step 2: Count before archiving
    log_info "[STEP 2] Counting rows before archiving..."
    for table in $tables; do
        local count
        count=$(get_row_count "$SOURCE_HOST" "$SOURCE_PORT" "$SOURCE_USER" "$SOURCE_DB" "$table")
        log_info "  $table: Source=$count"
        echo "  $table (before): Source=$count" >> "$log_file"
    done
    
    # Step 3: Run archive job
    log_info "[STEP 3] Running archive job..."
    if ! run_archive_job "$config_file" >> "$log_file" 2>&1; then
        log_error "Archive job failed"
        test_result="FAIL"
        test_error="Archive job failed"
        end_time=$(date +%s)
        duration=$((end_time - start_time))
        echo "" >> "$log_file"
        echo "Result: $test_result" >> "$log_file"
        echo "Duration: ${duration}s" >> "$log_file"
        return 1
    fi
    
    # Step 4: Count after archiving
    log_info "[STEP 4] Counting rows after archiving..."
    local verify_passed=true
    for table in $tables; do
        local count
        count=$(get_row_count "$SOURCE_HOST" "$SOURCE_PORT" "$SOURCE_USER" "$SOURCE_DB" "$table")
        log_info "  $table: Source=$count"
        echo "  $table (after): Source=$count" >> "$log_file"
    done
    
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    
    if [[ "$verify_passed" == true ]]; then
        log_info "Test $test_num completed successfully (Duration: ${duration}s)"
        echo "" >> "$log_file"
        echo "Result: PASS" >> "$log_file"
        echo "Duration: ${duration}s" >> "$log_file"
        return 0
    else
        log_error "Test $test_num failed verification"
        echo "" >> "$log_file"
        echo "Result: FAIL" >> "$log_file"
        echo "Duration: ${duration}s" >> "$log_file"
        return 1
    fi
}

# Generate Sakila test report
generate_sakila_report() {
    log_header ""
    log_header "========================================"
    log_header "Generating Final Report"
    log_header "========================================"
    
    local summary_file="$TESTS_DIR/results/test_summary.txt"
    
    {
        echo "================================================================================"
        echo "SAKILA INTEGRATION TEST SUMMARY"
        echo "================================================================================"
        echo "Generated: $(date -Iseconds)"
        echo ""
        echo "See individual test logs in: $TESTS_DIR/results/"
        echo "================================================================================"
    } > "$summary_file"
    
    cat "$summary_file"
}

# Run Sakila tests
run_sakila_tests() {
    log_header "========================================"
    log_header "Sakila Integration Test Suite"
    log_header "========================================"
    
    # Check prerequisites
    if ! command -v mysqlsh &> /dev/null; then
        log_error "mysqlsh is not installed or not in PATH"
        exit 1
    fi
    
    # Create results directory
    mkdir -p "$TESTS_DIR/results"
    
    local passed=0
    local failed=0
    
    if [[ -n "$SPECIFIC_TEST" ]]; then
        if run_sakila_test "$SPECIFIC_TEST"; then
            ((passed++))
        else
            ((failed++))
        fi
    else
        for i in 1 2 3 4 5; do
            if run_sakila_test "$i"; then
                ((passed++))
            else
                ((failed++))
            fi
            echo ""
        done
    fi
    
    generate_sakila_report
    
    log_header ""
    log_header "========================================"
    log_header "Test Execution Complete"
    log_header "========================================"
    log_info "Passed: $passed"
    if [[ $failed -gt 0 ]]; then
        log_error "Failed: $failed"
        exit 1
    else
        log_info "Failed: $failed"
    fi
}

# Run Go unit tests
run_unit_tests() {
    log_step "Running Go unit tests..."
    
    cd "$PROJECT_ROOT"
    
    local go_test_opts=""
    if [[ -n "$VERBOSE" ]]; then
        go_test_opts="-v"
    fi
    
    if [ -z "$GO_TEST_ARGS" ]; then
        GO_TEST_ARGS="./..."
    fi
    
    go test $go_test_opts -run '^Test[^(Integration|Orchestrator_FailFast|Orchestrator_Full|Execute_|Real)].*' $GO_TEST_ARGS 2>&1 || true
}

# Run Go integration tests
run_integration_tests() {
    log_step "Running Go integration tests..."
    
    cd "$PROJECT_ROOT"
    
    local go_test_opts=""
    if [[ -n "$VERBOSE" ]]; then
        go_test_opts="-v"
    fi
    
    if [ -z "$GO_TEST_ARGS" ]; then
        GO_TEST_ARGS="./internal/archiver"
    fi
    
    go test $go_test_opts -run 'Integration|_Integration|Real' $GO_TEST_ARGS 2>&1
}

# Main execution
main() {
    # Setup environment if requested
    if [ "$SETUP" = true ]; then
        setup_environment
    fi
    
    # Run Sakila tests if requested
    if [ "$SAKILA" = true ]; then
        run_sakila_tests
        exit 0
    fi
    
    # Run Go integration tests
    if [ "$INTEGRATION_ONLY" = true ]; then
        run_integration_tests
        exit 0
    fi
    
    # Run Go unit tests
    if [ "$UNIT_ONLY" = true ]; then
        run_unit_tests
        exit 0
    fi
    
    # Default: run all Go tests
    if [ "$SETUP" = false ]; then
        log_step "Running all Go tests..."
        cd "$PROJECT_ROOT"
        local go_test_opts=""
        if [[ -n "$VERBOSE" ]]; then
            go_test_opts="-v"
        fi
        if [ -z "$GO_TEST_ARGS" ]; then
            GO_TEST_ARGS="./..."
        fi
        go test $go_test_opts $GO_TEST_ARGS 2>&1
    fi
}

# Run main
main
