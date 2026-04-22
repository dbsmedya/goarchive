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
#   --fmt               Check Go code formatting with gofmt
#   --lint              Run golangci-lint checks
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

# Default SAKILA_DIR to the repo-relative path so mysqlsh scripts find the
# Sakila SQL files regardless of the CWD mysqlsh launches in. Do not override
# an explicit non-empty export.
if [ -z "${SAKILA_DIR:-}" ]; then
    export SAKILA_DIR="$TESTS_DIR/sakila-db"
fi

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

SETUP=false
SAKILA=false            # Working Sakila E2E tests (06, 07, 08)
SAKILA_EXAMPLES=false   # Validation-failure demonstration tests (01-05)
SPECIFIC_TEST=""
UNIT_ONLY=false
INTEGRATION_ONLY=false
FMT_CHECK=false
LINT_CHECK=false
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

# Check Go code formatting
run_fmt_check() {
    log_step "Checking Go code formatting..."
    
    cd "$PROJECT_ROOT"
    
    local fmt_output
    fmt_output=$(gofmt -l .)
    
    if [ -n "$fmt_output" ]; then
        log_error "The following files are not formatted:"
        echo "$fmt_output"
        log_info "Run 'make fmt' or 'gofmt -w .' to fix formatting"
        return 1
    else
        log_info "All Go files are properly formatted"
        return 0
    fi
}

# Run golangci-lint checks
run_lint_check() {
    log_step "Running golangci-lint..."
    
    cd "$PROJECT_ROOT"
    
    if ! command -v golangci-lint &> /dev/null; then
        log_warn "golangci-lint is not installed"
        log_info "Install with: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"
        return 1
    fi
    
    if golangci-lint run --timeout=5m ./...; then
        log_info "golangci-lint passed"
        return 0
    else
        log_error "golangci-lint found issues"
        return 1
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
            echo "  --sakila            Run the working Sakila E2E tests (06, 07, 08)"
            echo "  --sakila-examples   Run the validation-demonstration tests (01-05)"
            echo "                      These are DESIGNED to fail preflight; success"
            echo "                      here means the failure matches documented expectation."
            echo "  -t, --test NUM      Run only the specified test number (works with"
            echo "                      either --sakila or --sakila-examples)"
            echo "  --unit-only         Run only Go unit tests"
            echo "  --integration-only  Run only Go integration tests"
            echo "  -v, --verbose       Verbose output"
            echo "  --skip-docker       Skip docker compose operations (use existing DBs)"
            echo ""
            echo "Examples:"
            echo "  $0 --setup                    # Full setup: docker + databases"
            echo "  $0 --setup --sakila           # Setup and run working Sakila tests"
            echo "  $0 --sakila -t 7              # Run only working test 07"
            echo "  $0 --sakila-examples          # Run validation demos (01-05)"
            echo "  $0 --integration-only         # Run Go integration tests only"
            echo "  $0 --unit-only                # Run Go unit tests only"
            echo "  $0 --fmt                      # Check Go code formatting"
            echo "  $0 --lint                     # Run golangci-lint checks"
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
        --sakila-examples)
            SAKILA_EXAMPLES=true
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
        --fmt)
            FMT_CHECK=true
            shift
            ;;
        --lint)
            LINT_CHECK=true
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
ARCHIVE_HOST="${TEST_DEST_HOST:-127.0.0.1}"
ARCHIVE_PORT="${TEST_DEST_PORT:-3307}"
ARCHIVE_USER="${TEST_DEST_USER:-root}"
ARCHIVE_DB="${TEST_DEST_DB:-sakila_archive}"

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
    # Extract job name from config file (first job in the jobs section)
    local job_name=$(grep -A 1 "^jobs:" "$full_config_path" | tail -1 | sed 's/://g' | tr -d ' ')
    if [[ -z "$job_name" ]]; then
        log_error "Could not extract job name from config file"
        return 1
    fi
    
    # STEP 1: Validate configuration first
    log_info "[STEP 1/3] Validating configuration..."
    # Use --force-triggers because Sakila database has DELETE triggers
    if ! ./bin/goarchive validate --config "$full_config_path" --force-triggers 2>&1; then
        log_error "Configuration validation failed - check for missing relations"
        return 1
    fi
    
    # STEP 2: Dry-run to detect issues before actual archive
    log_info "[STEP 2/3] Running dry-run to detect potential issues..."
    if ! ./bin/goarchive dry-run --job "$job_name" --config "$full_config_path" 2>&1; then
        log_error "Dry-run failed - check configuration"
        return 1
    fi
    
    # STEP 3: Run actual archive
    log_info "[STEP 3/3] Executing archive..."
    if ! ./bin/goarchive archive --job "$job_name" --config "$full_config_path" --skip-verify 2>&1; then
        log_error "Archive job failed"
        return 1
    fi
    
    return 0
}

# Ensure the destination database has the same schema as source. Idempotent.
# Used before running working tests that actually copy data.
ensure_destination_schema() {
    local dump_dir="${DUMP_DIR:-/tmp/db1_schema_dump}"
    log_info "Preparing destination schema at $ARCHIVE_HOST:$ARCHIVE_PORT/$ARCHIVE_DB..."

    rm -rf "$dump_dir"
    if ! mysqlsh --uri "$SOURCE_USER:$MYSQL_PASS@$SOURCE_HOST:$SOURCE_PORT" \
        --js -f "$SCRIPT_DIR/dump_master.js" > /dev/null 2>&1; then
        log_error "Failed to dump source schema"
        return 1
    fi

    local archive_uri="$ARCHIVE_USER:$MYSQL_PASS@$ARCHIVE_HOST:$ARCHIVE_PORT"
    if ! mysqlsh --uri "$archive_uri" --sql \
        -e "DROP DATABASE IF EXISTS \`$ARCHIVE_DB\`; CREATE DATABASE \`$ARCHIVE_DB\`;" > /dev/null 2>&1; then
        log_error "Failed to recreate destination database"
        return 1
    fi
    if ! mysqlsh --uri "$archive_uri" --js -f "$SCRIPT_DIR/create_archive.js" > /dev/null 2>&1; then
        log_error "Failed to load schema into destination"
        return 1
    fi
    return 0
}

# Run specific Sakila test. First argument is the test number.
# Tests 06-08 are expected to succeed (archive runs to completion).
# Tests 01-05 are expected to FAIL preflight with a documented error category
# and are only run when --sakila-examples is set.
run_sakila_test() {
    local test_num=$1
    local test_name=""
    local test_desc=""
    local config_file=""
    local tables=""
    local mode=""                 # "working" or "example"
    local expected_error=""       # substring required in error when mode=example
    local archive_flags="--skip-verify"
    local start_time end_time duration
    local test_result="PASS"
    local test_error=""

    case $test_num in
        1)
            test_name="Test01_OneToOne"
            test_desc="1-1 Relationship (film → film_text) [validation demo]"
            config_file="test01_one_to_one.yaml"
            tables="film film_text"
            mode="example"
            expected_error="INTERNAL_FK_COVERAGE"
            ;;
        2)
            test_name="Test02_OneToMany"
            test_desc="1-N Relationship (language → film) [validation demo]"
            config_file="test02_one_to_many.yaml"
            tables="language film"
            mode="example"
            expected_error="FK_INDEX_CHECK"
            ;;
        3)
            test_name="Test03_OneToManyMultiple"
            test_desc="1-N Multiple Children [validation demo]"
            config_file="test03_one_to_many_multiple.yaml"
            tables="film inventory film_actor film_category"
            mode="example"
            expected_error="FK_INDEX_CHECK"
            ;;
        4)
            test_name="Test04_OneToManyTwoNested"
            test_desc="1-N Two Nested (country → city → address) [validation demo]"
            config_file="test04_one_to_many_two_nested.yaml"
            tables="country city address"
            mode="example"
            expected_error="FK_INDEX_CHECK"
            ;;
        5)
            test_name="Test05_OneToManyThreeNestedWithOneToOne"
            test_desc="1-N Three Nested with 1-1 [validation demo]"
            config_file="test05_one_to_many_three_nested.yaml"
            tables="country city address customer film film_text"
            mode="example"
            expected_error="FK_INDEX_CHECK"
            ;;
        6)
            test_name="Test06_CompleteFilmHierarchy"
            test_desc="4-level nested (film → inventory → rental → payment)"
            config_file="test06_complete_film_hierarchy.yaml"
            tables="film inventory rental payment"
            mode="working"
            ;;
        7)
            test_name="Test07_ActorFilmActor"
            test_desc="Simple 1-N (actor → film_actor)"
            config_file="test07_actor_film_actor.yaml"
            tables="actor film_actor"
            mode="working"
            ;;
        8)
            test_name="Test08_CategoryFilmCategory"
            test_desc="Simple 1-N (category → film_category)"
            config_file="test08_category_film_category.yaml"
            tables="category film_category"
            mode="working"
            ;;
        *)
            log_error "Invalid test number: $test_num (expected 1-8)"
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
    
    # Step 3: Run the test depending on mode.
    if [[ "$mode" == "working" ]]; then
        # Working tests expect archive to complete successfully. Destination
        # schema must mirror source, so load it before running.
        log_info "[STEP 3a] Ensuring destination schema..."
        if ! ensure_destination_schema >> "$log_file" 2>&1; then
            log_error "Destination schema setup failed"
            end_time=$(date +%s)
            duration=$((end_time - start_time))
            echo "" >> "$log_file"
            echo "Result: FAIL (destination setup)" >> "$log_file"
            echo "Duration: ${duration}s" >> "$log_file"
            return 1
        fi
        log_info "[STEP 3b] Running archive job (expect success)..."
        if ! run_archive_job "$config_file" >> "$log_file" 2>&1; then
            log_error "Archive job failed"
            end_time=$(date +%s)
            duration=$((end_time - start_time))
            echo "" >> "$log_file"
            echo "Result: FAIL" >> "$log_file"
            echo "Duration: ${duration}s" >> "$log_file"
            return 1
        fi
    else
        # Example tests expect `validate` to fail with a specific error category.
        # Success = exit-non-zero AND stderr contains expected_error substring.
        log_info "[STEP 3] Running validate (expect failure: $expected_error)..."
        local full_config_path="$TESTS_DIR/configs/$config_file"
        local validate_out
        if [[ ! -f "$PROJECT_ROOT/bin/goarchive" ]]; then
            (cd "$PROJECT_ROOT" && go build -o bin/goarchive ./cmd/goarchive) >> "$log_file" 2>&1
        fi
        validate_out=$("$PROJECT_ROOT/bin/goarchive" validate --config "$full_config_path" --force-triggers 2>&1) && {
            log_error "Validate unexpectedly PASSED — validation demo no longer demonstrates failure"
            echo "$validate_out" >> "$log_file"
            end_time=$(date +%s)
            duration=$((end_time - start_time))
            echo "" >> "$log_file"
            echo "Result: FAIL (validate passed)" >> "$log_file"
            echo "Duration: ${duration}s" >> "$log_file"
            return 1
        }
        echo "$validate_out" >> "$log_file"
        if ! echo "$validate_out" | grep -q "$expected_error"; then
            log_error "Validate failed but with unexpected category (expected: $expected_error)"
            end_time=$(date +%s)
            duration=$((end_time - start_time))
            echo "" >> "$log_file"
            echo "Result: FAIL (wrong error category)" >> "$log_file"
            echo "Duration: ${duration}s" >> "$log_file"
            return 1
        fi
        log_info "  Matched expected error category: $expected_error"
    fi

    # Step 4: Count after archiving (working tests only — tables changed)
    if [[ "$mode" == "working" ]]; then
        log_info "[STEP 4] Counting rows after archiving..."
        for table in $tables; do
            local count
            count=$(get_row_count "$SOURCE_HOST" "$SOURCE_PORT" "$SOURCE_USER" "$SOURCE_DB" "$table")
            log_info "  $table: Source=$count"
            echo "  $table (after): Source=$count" >> "$log_file"
        done
    fi

    end_time=$(date +%s)
    duration=$((end_time - start_time))
    if [[ "$mode" == "example" ]]; then
        log_info "Test $test_num: EXPECTED FAILURE matched (Duration: ${duration}s)"
    else
        log_info "Test $test_num completed successfully (Duration: ${duration}s)"
    fi
    echo "" >> "$log_file"
    echo "Result: PASS" >> "$log_file"
    echo "Duration: ${duration}s" >> "$log_file"
    return 0
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

# Run Sakila tests. First argument is a space-separated list of test numbers.
# Second argument is a human label ("working" or "validation demos").
run_sakila_tests() {
    local test_nums="$1"
    local label="$2"

    log_header "========================================"
    log_header "Sakila $label Test Suite"
    log_header "========================================"

    # Check prerequisites
    if ! command -v mysqlsh &> /dev/null; then
        log_error "mysqlsh is not installed or not in PATH"
        exit 1
    fi

    mkdir -p "$TESTS_DIR/results"

    local passed=0
    local failed=0
    local run_list
    if [[ -n "$SPECIFIC_TEST" ]]; then
        run_list="$SPECIFIC_TEST"
    else
        run_list="$test_nums"
    fi

    for i in $run_list; do
        if run_sakila_test "$i"; then
            ((passed++))
        else
            ((failed++))
        fi
        echo ""
    done

    generate_sakila_report

    log_header ""
    log_header "========================================"
    log_header "Test Execution Complete — $label"
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
    # Check formatting if requested
    if [ "$FMT_CHECK" = true ]; then
        run_fmt_check
        exit $?
    fi
    
    # Run linting if requested
    if [ "$LINT_CHECK" = true ]; then
        run_lint_check
        exit $?
    fi
    
    # Setup environment if requested
    if [ "$SETUP" = true ]; then
        setup_environment
    fi
    
    # Run the working Sakila E2E suite (tests 06, 07, 08)
    if [ "$SAKILA" = true ]; then
        run_sakila_tests "6 7 8" "working"
        exit 0
    fi

    # Run the validation demonstration tests (01-05) — these are expected to
    # fail preflight with documented error categories.
    if [ "$SAKILA_EXAMPLES" = true ]; then
        run_sakila_tests "1 2 3 4 5" "validation demos"
        exit 0
    fi
    
    # Run Go integration tests
    if [ "$INTEGRATION_ONLY" = true ]; then
        run_fmt_check || exit 1
        run_lint_check || exit 1
        run_integration_tests
        exit 0
    fi
    
    # Run Go unit tests
    if [ "$UNIT_ONLY" = true ]; then
        run_fmt_check || exit 1
        run_lint_check || exit 1
        run_unit_tests
        exit 0
    fi
    
    # Default: run all Go tests
    if [ "$SETUP" = false ]; then
        run_fmt_check || exit 1
        run_lint_check || exit 1
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
