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
#   --sakila            Run the working Sakila E2E tests (03, 04, 05)
#   -t, --test NUM      Run only specific Sakila test (1-5)
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

# mysqlsh is used by the setup path long before the E2E path checks for it, so
# check here -- otherwise a machine without MySQL Shell fails ~450 lines later
# with a bare "command not found" inside a connection retry loop.
if ! command -v mysqlsh &> /dev/null; then
    echo "ERROR: mysqlsh (MySQL Shell) is not installed or not in PATH."
    echo ""
    echo "  macOS:  brew install mysql-shell"
    echo "  Other:  https://dev.mysql.com/downloads/shell/"
    echo ""
    echo "The test harness drives every database operation through mysqlsh, so"
    echo "nothing below this point can work without it."
    exit 1
fi

# Source .env. On a fresh clone it does not exist -- tests/.env is gitignored --
# so create it from the tracked template rather than making that a manual step.
# tests/dot.env ships with working local defaults; nothing has to be edited.
if [ ! -f "$TESTS_DIR/.env" ]; then
    if [ ! -f "$TESTS_DIR/dot.env" ]; then
        echo "ERROR: neither $TESTS_DIR/.env nor the template $TESTS_DIR/dot.env exists."
        echo "The template is tracked in git; a clone should have it."
        exit 1
    fi
    echo "NOTE: $TESTS_DIR/.env did not exist. Creating it from dot.env."
    echo "      These are local test-container credentials. Edit the file if"
    echo "      your setup differs; the defaults work with 'make test-up'."
    cp "$TESTS_DIR/dot.env" "$TESTS_DIR/.env"
fi

set -a
source "$TESTS_DIR/.env"
set +a

# Default SAKILA_DIR to the repo-relative path so mysqlsh scripts find the
# Sakila SQL files regardless of the CWD mysqlsh launches in. Do not override
# an explicit non-empty export.
if [ -z "${SAKILA_DIR:-}" ]; then
    export SAKILA_DIR="$TESTS_DIR/sakila-db"
fi

# Render configs/*.yaml from their tracked templates -- the same gap .env closes
# above, for the same reason. A rendered config carries the operator's real
# password, so configs/*.yaml is gitignored (tests/.gitignore:5) and only the
# .yaml.template is committed. Nothing rendered them until now: a fresh clone had
# the templates, no configs, and `make e2e` died on a missing file. The four
# configs that existed had been substituted by hand, once, and were invisible to
# everyone else.
#
# Rendered unconditionally on every run. The template is the source of truth, and
# render-if-missing would let a template edit sit un-propagated behind a stale
# local .yaml -- silent divergence, which is worse than a lost local tweak. Edit
# the template, never the .yaml.
#
# Substitution is bash parameter expansion, NOT sed: a password containing '&' or
# the sed delimiter corrupts the replacement silently. ${var//pat/rep} is bash 3.2
# safe -- macOS `make` resolves to /bin/bash 3.2.57, which has no `declare -A` and
# is the floor this suite targets.
render_test_configs() {
    local template rendered line rendered_count=0

    # An empty password renders a syntactically valid config that fails much
    # later as an unexplained MySQL authentication error. Refuse up front.
    if [ -z "${MYSQL_ROOT_PASSWORD:-}" ]; then
        echo "ERROR: MYSQL_ROOT_PASSWORD is empty or unset after sourcing $TESTS_DIR/.env." >&2
        echo "       Test configs cannot be rendered without it." >&2
        return 1
    fi

    for template in "$TESTS_DIR"/configs/*.yaml.template; do
        # With no matches the glob stays literal, so test for a real file.
        [ -e "$template" ] || continue
        rendered="${template%.template}"

        if ! {
            while IFS= read -r line || [ -n "$line" ]; do
                printf '%s\n' "${line//\$\{MYSQL_ROOT_PASSWORD\}/$MYSQL_ROOT_PASSWORD}"
            done < "$template"
        } > "$rendered"; then
            echo "ERROR: failed to render $rendered from $template" >&2
            return 1
        fi

        # Fail closed on a placeholder this function does not know how to fill,
        # rather than handing goarchive a config with a literal '${...}' in it.
        if grep -q '\${' "$rendered"; then
            echo "ERROR: $rendered still contains an unsubstituted \${...} placeholder." >&2
            echo "       render_test_configs only substitutes \${MYSQL_ROOT_PASSWORD}." >&2
            return 1
        fi

        rendered_count=$((rendered_count + 1))
    done

    if [ "$rendered_count" -eq 0 ]; then
        echo "ERROR: no config templates found in $TESTS_DIR/configs/." >&2
        echo "       The .yaml.template files are tracked in git; a clone should have them." >&2
        return 1
    fi
}

render_test_configs || exit 1

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

SETUP=false
SAKILA=false            # Working Sakila E2E tests (03 payment, 04 rental->payment, 05 payment+sha256)
SAKILA_EXAMPLES=false   # Validation-failure demonstration tests (01 composite-PK, 02 FK-index)
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
            echo "  --sakila            Run the working Sakila E2E tests (03 payment, 04 rental->payment, 05 payment+sha256)"
            echo "  --sakila-examples   Run the validation-demonstration tests (01-02)"
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
            echo "  $0 --sakila-examples -t 1     # Run only the composite-PK demo (test 01)"
            echo "  $0 --sakila-examples          # Run validation demos (01-02)"
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

# The goarchive binary under test.
#
# Overridable so this same suite can be run against a DIFFERENT build -- capturing
# a behavioural baseline from an older release, for instance. When the caller sets
# it explicitly, a missing binary is an ERROR rather than a cue to build: compiling
# the current tree over the top would silently exercise a build nobody asked for,
# and every test would pass while proving nothing about the requested one.
if [[ -n "${GOARCHIVE_BIN:-}" ]]; then
    GOARCHIVE_BIN_EXPLICIT=true
else
    GOARCHIVE_BIN="$PROJECT_ROOT/bin/goarchive"
    GOARCHIVE_BIN_EXPLICIT=false
fi

# Build the binary under test, but only when we own it.
ensure_goarchive_bin() {
    if [[ -f "$GOARCHIVE_BIN" ]]; then
        return 0
    fi
    if [[ "$GOARCHIVE_BIN_EXPLICIT" == "true" ]]; then
        log_error "GOARCHIVE_BIN was set explicitly, but no binary exists at:"
        log_error "  $GOARCHIVE_BIN"
        log_error "Refusing to build the current tree in its place -- that would test a"
        log_error "different build than the one requested, and pass."
        return 1
    fi
    log_info "Building goarchive binary at $GOARCHIVE_BIN..."
    (cd "$PROJECT_ROOT" && go build -o "$GOARCHIVE_BIN" ./cmd/goarchive)
}

# Setup test environment
setup_environment() {
    log_step "Setting up test environment..."
    
    if [ "$SKIP_DOCKER" = false ]; then
        log_info "Stopping existing docker containers and clearing data volumes..."
        cd "$TESTS_DIR"
        # `-v` removes the db1_data/db2_data/db3_data named volumes. Without it,
        # --setup does NOT give you a clean database.
        #
        # This previously read `docker compose down` followed by
        # `rm -rf "$TESTS_DIR/docker_files/db_data"` — but the directory has
        # always been `dbdata`, not `db_data`. That rm removed a path that never
        # existed, so --setup printed "Cleaning up database data..." and cleaned
        # up nothing. It is why orphaned state survived a --setup run and
        # resurfaced as "legacy GoArchive tracking tables detected".
        docker compose down -v 2>/dev/null || true

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

# Get row count from a table.
#
# Routed through mysql-query.sh so that A FAILED QUERY CAN NEVER BE MISTAKEN FOR A
# ROW COUNT OF ZERO. The previous form called mysqlsh directly, discarded stderr
# with 2>/dev/null, ignored the exit status, and defaulted an empty result to "0"
# via ${count:-0}. An unreachable server, a wrong password, a dropped table and a
# genuinely empty table were therefore indistinguishable -- all four reported "0".
#
# That is not a theoretical hole. Every post-condition assertion below is built on
# this function, and the purge assertion is "the destination stayed empty": a
# destination query that fails returns 0 and the assertion PASSES, without a
# connection ever having been made. selftest_get_row_count_fails_loud proves it
# does not, and runs on every invocation of the suite.
#
# Contract:
#   stdout / exit 0   the row count
#   stderr / exit 1   the query failed -- callers MUST check
get_row_count() {
    local host=$1
    local port=$2
    local user=$3
    local db=$4
    local table=$5

    # `local out` is declared on its own line. Written as `local out=$(...)` the
    # exit status would be `local`'s (always 0) rather than the query's, which
    # silently restores exactly the defect this function was rewritten to remove.
    local out
    if ! out=$(MYSQL_QUERY_HOST="$host" MYSQL_QUERY_USER="$user" \
        "$SCRIPT_DIR/mysql-query.sh" "$port" "SELECT COUNT(*) FROM \`$db\`.\`$table\`;"); then
        echo "get_row_count: could not count ${db}.${table} on ${host}:${port}" >&2
        return 1
    fi

    # mysql-query.sh emits the column header, then the value.
    echo "$out" | tail -1
}

# Prove that get_row_count reports a failure instead of returning "0".
#
# This is deliberately NOT a mutation proof that has to be applied and reverted.
# The failure mode is reachable through an ARGUMENT, so it can be asserted on
# every run and keeps holding: port 59999 on loopback is refused immediately
# (ECONNREFUSED, measured ~0.3s -- no DNS, no timeout, no server state touched,
# nothing else in the run affected).
#
# A reverted mutation leaves a sentence in a plan file. This leaves a test.
selftest_get_row_count_fails_loud() {
    local out rc
    out=$(get_row_count 127.0.0.1 59999 "$SOURCE_USER" "$SOURCE_DB" actor 2>/dev/null)
    rc=$?
    if [[ $rc -eq 0 ]]; then
        log_error "HARNESS SELF-CHECK FAILED: get_row_count exited 0 against an unreachable port."
        log_error "  It returned: '${out}'"
        log_error "  A failed query is being reported as a row count, which means every"
        log_error "  post-condition assertion in this suite is vacuous. Refusing to run."
        return 1
    fi
    log_verbose "self-check: get_row_count fails loud on an unreachable port (exit $rc)"
    return 0
}

# Assert the end state a command is supposed to produce.
#
# Each command has a DIFFERENT correct outcome, and reusing one assertion for
# another passes vacuously -- a purge checked only for "the source shrank" proves
# nothing, because the destination is empty whether or not purge did its job.
#
#   command     source after        destination after
#   archive     rows moved out      rows arrived
#   purge       rows deleted        nothing copied
#   copy-only   untouched           rows arrived
#
# Conservation (after_src + after_dst == before) is necessary but NOT sufficient:
# a run that copied nothing and deleted nothing satisfies it exactly. So archive
# and copy-only additionally require that the destination actually gained rows,
# and purge requires that the source actually lost some.
#
# The destination is assumed to start empty; ensure_destination_schema drops and
# recreates the destination database before every working test.
#
# Usage: assert_postcondition <command> <table> <before> <after_src> <after_dst>
assert_postcondition() {
    local command=$1
    local table=$2
    local before=$3
    local after_src=$4
    local after_dst=$5

    case "$command" in
        archive)
            if [[ "$after_dst" -le 0 ]]; then
                log_error "  $table: archive copied NOTHING (destination=$after_dst)"
                return 1
            fi
            if [[ $((after_src + after_dst)) -ne "$before" ]]; then
                log_error "  $table: rows not conserved -- before=$before, after source=$after_src + destination=$after_dst = $((after_src + after_dst))"
                return 1
            fi
            log_info "  $table: archive OK (before=$before, source=$after_src, destination=$after_dst)"
            ;;
        purge)
            if [[ "$after_src" -ge "$before" ]]; then
                log_error "  $table: purge deleted NOTHING (before=$before, source=$after_src)"
                return 1
            fi
            if [[ "$after_dst" -ne 0 ]]; then
                log_error "  $table: purge must not copy, but destination holds $after_dst row(s)"
                return 1
            fi
            log_info "  $table: purge OK (before=$before, source=$after_src, destination empty)"
            ;;
        copy-only)
            if [[ "$after_dst" -le 0 ]]; then
                log_error "  $table: copy-only copied NOTHING (destination=$after_dst)"
                return 1
            fi
            if [[ "$after_src" -ne "$before" ]]; then
                log_error "  $table: copy-only must not delete, but source went $before -> $after_src"
                return 1
            fi
            log_info "  $table: copy-only OK (source unchanged at $after_src, destination=$after_dst)"
            ;;
        *)
            # Fail closed: an unrecognised command must never assert nothing.
            log_error "  $table: no post-condition defined for command '$command'"
            return 1
            ;;
    esac
    return 0
}

# Assert the run took at least as long as its configured throttling requires.
#
# The floor is deterministic -- pure arithmetic over the config, with no
# dependence on the machine:
#
#   floor = ceil(rows / batch_size) * sleep_seconds
#         + SUM over batches of (delete chunks per table - 1) * delete_sleep_seconds
#
# sleep_seconds fires after EVERY batch including the last (orchestrator.go:457,
# no guard). delete_sleep_seconds skips each table's final chunk
# (delete.go:185, `batchNum < totalBatches-1`), so a batch of 100 rows at
# batch_delete_size=20 sleeps 4 times, not 5.
#
# Why this is safe to assert, when timing assertions usually are not: it is a
# ONE-SIDED bound. A slow container, a loaded host, a cold filesystem or a
# reseeded estate can only push the measured duration UP, and can therefore
# never fail it. It fails only when the sleeps did not actually happen --
# sleep_seconds silently zeroed, a config that stopped being read, a batch loop
# that ran once where it should have run twenty. Wall-clock alone cannot tell
# those apart from a fast machine; the floor can.
#
# Compared against goarchive's OWN reported duration, not the shell-measured
# test duration. The latter includes the source reset, validate and dry-run,
# which together dwarf the floor and would mask a lost throttle entirely.
assert_min_duration() {
    local log_file=$1
    local floor=$2

    # Fail closed: a working test with no floor configured asserts nothing.
    if [[ -z "$floor" ]]; then
        log_error "  no min_duration configured for this test"
        return 1
    fi

    # The last "duration":<seconds> in the log is the command's own total.
    # archive, purge and copy-only all emit it as a structured field
    # (cmd/goarchive/cmd/{archive,purge,copyonly}.go). The phase-level logs spell
    # duration inside a message string instead, so they cannot collide with this.
    local actual
    actual=$(grep -o '"duration":[0-9.]*' "$log_file" | tail -1 | cut -d: -f2)
    if [[ -z "$actual" ]]; then
        log_error "  could not read the run's own duration from $log_file"
        log_error "  the command logs it as a \"duration\" field; if that changed,"
        log_error "  this assertion has stopped matching and is no longer checking anything."
        return 1
    fi

    # bash 3.2 has no floating-point arithmetic, so awk does the comparison.
    if ! awk -v a="$actual" -v f="$floor" 'BEGIN { exit !(a >= f) }'; then
        log_error "  run finished in ${actual}s, below its ${floor}s pacing floor"
        log_error "  the configured sleep_seconds / delete_sleep_seconds cannot all have"
        log_error "  been applied -- this is a throttling regression, not a fast machine."
        return 1
    fi

    log_info "  pacing OK (${actual}s >= ${floor}s floor)"
    return 0
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

# Run a goarchive job. Second argument is the command under test:
# archive | purge | copy-only. Defaults to archive.
run_archive_job() {
    local config_file="$1"
    local command="${2:-archive}"
    local full_config_path="$TESTS_DIR/configs/$config_file"

    # --force-triggers is NOT accepted by every command. archive and purge delete
    # from the source and so must be told to proceed past Sakila's DELETE
    # triggers; copy-only never deletes and does not define the flag at all, so
    # passing it unconditionally would abort with "unknown flag" and read as a
    # product failure. Verified against cmd/goarchive/cmd/{archive,purge,copyonly}.go.
    local command_flags=""
    case "$command" in
        archive|purge)
            command_flags="--force-triggers"
            ;;
        copy-only)
            command_flags=""
            ;;
        *)
            log_error "run_archive_job: unsupported command '$command' (archive|purge|copy-only)"
            return 1
            ;;
    esac

    if [[ ! -f "$full_config_path" ]]; then
        log_error "Config file not found: $full_config_path"
        return 1
    fi

    log_info "Running $command job with config: $config_file"

    if ! ensure_goarchive_bin; then
        return 1
    fi

    cd "$PROJECT_ROOT"
    # Extract job name from config file (first job in the jobs section)
    local job_name=$(grep -A 1 "^jobs:" "$full_config_path" | tail -1 | sed 's/://g' | tr -d ' ')
    if [[ -z "$job_name" ]]; then
        log_error "Could not extract job name from config file"
        return 1
    fi
    
    # STEP 1: Validate configuration first. Kept for every command -- it is cheap,
    # and the only place the preflight profile is exercised end to end.
    log_info "[STEP 1/3] Validating configuration..."
    # Use --force-triggers because Sakila database has DELETE triggers
    if ! "$GOARCHIVE_BIN" validate --config "$full_config_path" --force-triggers 2>&1; then
        log_error "Configuration validation failed - check for missing relations"
        return 1
    fi

    # STEP 2: Dry-run to detect issues before the real run
    log_info "[STEP 2/3] Running dry-run to detect potential issues..."
    if ! "$GOARCHIVE_BIN" dry-run --job "$job_name" --config "$full_config_path" 2>&1; then
        log_error "Dry-run failed - check configuration"
        return 1
    fi

    # STEP 3: Run the command under test
    log_info "[STEP 3/3] Executing $command..."
    if ! "$GOARCHIVE_BIN" "$command" --job "$job_name" --config "$full_config_path" $command_flags 2>&1; then
        log_error "$command job failed"
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

# Run specific Sakila test. First argument is the test number. There are four:
#   01  Composite-PK rejection    -> expects COMPOSITE_PK_CHECK   [validation demo]
#   02  Uncovered FK coverage     -> expects FK_COVERAGE_CHECK     [validation demo]
#   03  Payment batch             -> working archive (count verification, inherited)
#   04  rental -> payment         -> working archive (count verification, inherited)
#   05  Payment + sha256          -> working archive (sha256 verification, explicit)
# Tests 01-02 are validation demos (mode=example) and only run when --sakila-examples
# is set; tests 03-05 are working archives (mode=working) and run to completion.
#
# 03 and 05 are deliberately the same archive with different verification methods,
# so a failure in 05 alone isolates to the method.
run_sakila_test() {
    local test_num=$1
    local test_name=""
    local test_desc=""
    local config_file=""
    local tables=""
    local mode=""                 # "working" or "example"
    local expected_error=""       # substring required in error when mode=example
    local command="archive"       # archive | purge | copy-only (mode=working)
    local verify_method=""        # verification method the run must report, or
                                  # "none" to assert no verification ran at all
    local min_duration=""         # pacing floor in seconds, derived from the
                                  # config's batch/sleep settings; see
                                  # assert_min_duration for the arithmetic
    local start_time end_time duration
    local test_result="PASS"
    local test_error=""

    case $test_num in
        1)
            test_name="Test01_CompositePKRejected"
            test_desc="Composite-PK rejection: config includes film_actor/film_category (composite PKs) [validation demo]"
            config_file="test01_one_to_one.yaml"
            tables="film film_text film_actor film_category"
            mode="example"
            expected_error="COMPOSITE_PK_CHECK"
            ;;
        2)
            test_name="Test02_UncoveredFKCoverage"
            test_desc="FK-coverage rejection: archiving 'film' leaves out-of-graph tables (inventory/film_actor/film_category) referencing it [validation demo]"
            config_file="test02_one_to_many.yaml"
            tables="language film"
            mode="example"
            expected_error="FK_COVERAGE_CHECK"
            ;;
        3)
            test_name="Test03_PaymentBatch"
            test_desc="Working archive: high-volume payment (single-column PK, multi-batch)"
            config_file="test03_payment_batch.yaml"
            tables="payment"
            mode="working"
            command="archive"
            # No verification block in the config, so the documented default
            # applies: method=count, skip_verification=false.
            verify_method="count"
            # 1999 rows / batch_size 100 = 20 batches.
            #   batch sleep:  20 x 0.2                        = 4.0s
            #   delete sleep: 20 batches x 4 chunk-gaps x 0.2 = 16.0s
            # (100 rows / batch_delete_size 20 = 5 chunks, and the last chunk of
            # each table is not followed by a sleep, so 4 gaps.)
            min_duration="20.0"
            ;;
        4)
            test_name="Test04_RentalPayment"
            test_desc="Working archive: rental -> payment (2-level tree, non-diamond GDPR-shaped subgraph)"
            config_file="test04_rental_payment.yaml"
            tables="rental payment"
            mode="working"
            command="archive"
            verify_method="count"
            # This config declares no processing block at all, so it inherits the
            # global defaults: batch_size 1000, sleep_seconds 1, and
            # delete_sleep_seconds 0. 200 rentals / 1000 = a single batch.
            #   batch sleep:  1 x 1.0 = 1.0s
            #   delete sleep: disabled by default
            min_duration="1.0"
            ;;
        5)
            test_name="Test05_PaymentVerifySHA256"
            test_desc="Working archive: payment with sha256 verification (also the only INSERT IGNORE copy path in the suite)"
            config_file="test05_payment_verify_sha256.yaml"
            tables="payment"
            mode="working"
            command="archive"
            # The config declares verification.method: sha256 explicitly rather
            # than inheriting the default, and asserting on "sha256" here is the
            # entire subject of this test. If this ever reads "count", the test
            # has silently become a third copy of test 03.
            verify_method="sha256"
            # Same slice and same pacing as test 03, so the same floor.
            min_duration="20.0"
            ;;
        *)
            log_error "Invalid test number: $test_num (expected 1-5)"
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
    
    # Step 2: Count before the run. These counts are kept -- Step 4 compares
    # against them. A parallel indexed array, not an associative one: macOS ships
    # bash 3.2, which has no `declare -A`, and this suite is documented as running
    # on an operator workstation.
    log_info "[STEP 2] Counting rows before the run..."
    local before_counts=()
    for table in $tables; do
        local count
        if ! count=$(get_row_count "$SOURCE_HOST" "$SOURCE_PORT" "$SOURCE_USER" "$SOURCE_DB" "$table"); then
            log_error "Could not count $table before the run"
            end_time=$(date +%s)
            duration=$((end_time - start_time))
            echo "" >> "$log_file"
            echo "Result: FAIL (pre-count failed for $table)" >> "$log_file"
            echo "Duration: ${duration}s" >> "$log_file"
            return 1
        fi
        before_counts+=("$count")
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
        log_info "[STEP 3b] Running $command job (expect success)..."
        if ! run_archive_job "$config_file" "$command" >> "$log_file" 2>&1; then
            log_error "$command job failed"
            end_time=$(date +%s)
            duration=$((end_time - start_time))
            echo "" >> "$log_file"
            echo "Result: FAIL" >> "$log_file"
            echo "Duration: ${duration}s" >> "$log_file"
            return 1
        fi

        # Step 3c: assert the verify stage actually ran, naming the method.
        #
        # Nothing asserted this at any layer before. Verification is what makes
        # delete-after-copy safe, and a config change or a silently skipped stage
        # would leave every test passing. The anchor is goarchive's own log line
        # at internal/verifier/verifier.go -- "Starting verification (method=%s)".
        log_info "[STEP 3c] Asserting the verify stage ran (expect: $verify_method)..."
        if [[ "$verify_method" == "none" ]]; then
            if grep -q "Starting verification (method=" "$log_file"; then
                log_error "Verification ran, but this test expects none"
                end_time=$(date +%s)
                duration=$((end_time - start_time))
                echo "" >> "$log_file"
                echo "Result: FAIL (unexpected verification)" >> "$log_file"
                echo "Duration: ${duration}s" >> "$log_file"
                return 1
            fi
            log_info "  confirmed: no verification stage, as expected"
        elif ! grep -q "Starting verification (method=${verify_method})" "$log_file"; then
            log_error "No verification with method=${verify_method} in the run output"
            log_error "  Either the stage did not run, or the log line changed and this"
            log_error "  assertion has stopped matching. Both are findings."
            end_time=$(date +%s)
            duration=$((end_time - start_time))
            echo "" >> "$log_file"
            echo "Result: FAIL (verification method=${verify_method} not observed)" >> "$log_file"
            echo "Duration: ${duration}s" >> "$log_file"
            return 1
        else
            log_info "  confirmed: verification ran with method=$verify_method"
        fi

        # Step 3d: the run must not have been FASTER than its own configured
        # throttling permits. See assert_min_duration for why this is safe.
        log_info "[STEP 3d] Asserting the run respected its pacing floor (${min_duration}s)..."
        if ! assert_min_duration "$log_file" "$min_duration"; then
            end_time=$(date +%s)
            duration=$((end_time - start_time))
            echo "" >> "$log_file"
            echo "Result: FAIL (ran below its pacing floor)" >> "$log_file"
            echo "Duration: ${duration}s" >> "$log_file"
            return 1
        fi
    else
        # Example tests expect `validate` to fail with a specific error category.
        # Success = exit-non-zero AND stderr contains expected_error substring.
        log_info "[STEP 3] Running validate (expect failure: $expected_error)..."
        local full_config_path="$TESTS_DIR/configs/$config_file"
        local validate_out
        if ! ensure_goarchive_bin >> "$log_file" 2>&1; then
            log_error "goarchive binary unavailable"
            end_time=$(date +%s)
            duration=$((end_time - start_time))
            echo "" >> "$log_file"
            echo "Result: FAIL (binary unavailable)" >> "$log_file"
            echo "Duration: ${duration}s" >> "$log_file"
            return 1
        fi
        validate_out=$("$GOARCHIVE_BIN" validate --config "$full_config_path" --force-triggers 2>&1) && {
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

    # Step 4: Assert the post-condition (working tests only — tables changed).
    #
    # This used to count source rows and only LOG them, comparing nothing and
    # never touching the destination at all, so a working test passed if and only
    # if the binary exited 0 -- a run that copied nothing and deleted nothing
    # passed. The counts are now compared, and the destination is counted too.
    if [[ "$mode" == "working" ]]; then
        log_info "[STEP 4] Asserting post-conditions for '$command'..."
        local idx=0
        for table in $tables; do
            local after_src after_dst
            if ! after_src=$(get_row_count "$SOURCE_HOST" "$SOURCE_PORT" "$SOURCE_USER" "$SOURCE_DB" "$table"); then
                log_error "Could not count $table on the source after the run"
                end_time=$(date +%s)
                duration=$((end_time - start_time))
                echo "" >> "$log_file"
                echo "Result: FAIL (post-count failed for source $table)" >> "$log_file"
                echo "Duration: ${duration}s" >> "$log_file"
                return 1
            fi
            if ! after_dst=$(get_row_count "$ARCHIVE_HOST" "$ARCHIVE_PORT" "$ARCHIVE_USER" "$ARCHIVE_DB" "$table"); then
                log_error "Could not count $table on the destination after the run"
                end_time=$(date +%s)
                duration=$((end_time - start_time))
                echo "" >> "$log_file"
                echo "Result: FAIL (post-count failed for destination $table)" >> "$log_file"
                echo "Duration: ${duration}s" >> "$log_file"
                return 1
            fi
            echo "  $table (after): Source=$after_src Destination=$after_dst" >> "$log_file"
            if ! assert_postcondition "$command" "$table" "${before_counts[$idx]}" "$after_src" "$after_dst"; then
                end_time=$(date +%s)
                duration=$((end_time - start_time))
                echo "" >> "$log_file"
                echo "Result: FAIL (post-condition for $table)" >> "$log_file"
                echo "Duration: ${duration}s" >> "$log_file"
                return 1
            fi
            idx=$((idx + 1))
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

    # Prove the harness can tell a failed query from an empty one BEFORE trusting
    # any assertion built on it. Costs one refused connection (~0.3s).
    if ! selftest_get_row_count_fails_loud; then
        exit 1
    fi

    # Resolve the binary once, up front. Left to the per-test path, a wrong
    # GOARCHIVE_BIN fails every test with a generic "job failed" on the console
    # and names the path only in tests/results/test_N.log -- true, but buried.
    if ! ensure_goarchive_bin; then
        exit 1
    fi
    log_info "Binary under test: $GOARCHIVE_BIN"

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

# Run a Go test layer and REFUSE TO REPORT SUCCESS FOR A RUN THAT DID NOTHING.
#
# -v is not a preference here. Without it `go test` prints no "--- PASS" lines,
# so a suite where every test was filtered out, skipped, or never built looks
# byte-for-byte like a suite where everything passed: no "--- FAIL" either way,
# `ok <pkg> <time>`, exit 0. Counting PASS is the only way to tell those apart,
# and counting PASS requires -v. So -v is always on internally; the full log is
# printed only when asked for (--verbose) or when something went wrong.
#
# Floor: PASS must be at least MIN_PASS, which defaults to 1 -- "something ran".
# The bound is INCLUSIVE, so MIN_PASS=1000 means 1000 is acceptable, not 1001.
# Set it to pin a stricter floor; a real number catches a suite that quietly
# lost half its tests, which "at least 1" cannot.
#
# Usage: run_go_layer <label> <command...>
run_go_layer() {
    local label="$1"; shift
    local min_pass="${MIN_PASS:-1}"
    local log rc pass fail skip

    log="$(mktemp)"
    if [[ -n "$VERBOSE" ]]; then
        "$@" 2>&1 | tee "$log"
        rc=${PIPESTATUS[0]}
    else
        "$@" >"$log" 2>&1
        rc=$?
    fi

    pass=$(grep -c -- '--- PASS' "$log")
    fail=$(grep -c -- '--- FAIL' "$log")
    skip=$(grep -c -- '--- SKIP' "$log")

    # Show the evidence whenever the result is not a clean pass.
    if [[ -z "$VERBOSE" ]] && { [[ $rc -ne 0 ]] || [[ $fail -gt 0 ]] || [[ $pass -lt $min_pass ]]; }; then
        cat "$log"
    fi

    log_info "$label: PASS=$pass FAIL=$fail SKIP=$skip (go test exit $rc)"

    if [[ $pass -lt $min_pass ]]; then
        log_error "$label: only $pass tests passed (minimum is $min_pass)."
        log_error "A run that executed nothing is not a green run. Common causes:"
        log_error "  - a -run pattern that matched no test (go test prints 'ok' and exits 0)"
        log_error "  - the build tag missing, so no real-DB test was compiled in"
        log_error "  - every test skipped"
        rm -f "$log"
        return 1
    fi

    if [[ $fail -gt 0 || $rc -ne 0 ]]; then
        log_error "$label: $fail failing test(s), go test exit $rc"
        grep -- '--- FAIL' "$log" | head -30 >&2
        rm -f "$log"
        return 1
    fi

    if [[ $skip -gt 0 ]]; then
        log_warn "$label: $skip test(s) skipped — confirm each is data-dependent, not environmental"
        grep -- '--- SKIP' "$log" | head -10
    fi

    rm -f "$log"
    return 0
}

# Run Go unit tests
run_unit_tests() {
    log_step "Running Go unit tests..."

    cd "$PROJECT_ROOT"

    if [ -z "$GO_TEST_ARGS" ]; then
        GO_TEST_ARGS="./..."
    fi

    run_go_layer "unit" \
        go test -v -run '^Test[^(Integration|Orchestrator_FailFast|Orchestrator_Full|Execute_|Real)].*' $GO_TEST_ARGS
}

# Run Go integration tests
run_integration_tests() {
    log_step "Running Go integration tests..."

    cd "$PROJECT_ROOT"

    # These tests DELETE from source Sakila (measured: rental 16044 -> 5868), so
    # the estate is no longer the one `make e2e-setup` produced. Dropping the
    # marker forces the next E2E run back through the full rebuild.
    #
    # NOTE: the E2E suite is NOT broken by the drain -- every test reloads Sakila
    # in its STEP 1. This is about starting from a known estate, not about
    # protecting the archive from empty tables.
    rm -f "$TESTS_DIR/.e2e-ready"

    if [ -z "$GO_TEST_ARGS" ]; then
        GO_TEST_ARGS="./internal/archiver/..."
    fi

    # Real-DB tests live behind the `integration` build tag and several are not
    # named *Integration*/*Real* (e.g. TestExecute_*, TestOrchestrator_FullWorkflow),
    # so the build tag — not a -run name filter — is what selects them.
    # INTEGRATION_FORCE=true sets IntegrationConfig.Force, which makes every
    # SetupIntegrationTest DROP and recreate its databases and re-apply the
    # fixtures, so each test starts from a known schema. It does NOT gate which
    # tests run -- the build tag does that. Reseed first (`--setup`) so the
    # destination starts empty; see tests/README.md.
    INTEGRATION_FORCE=true run_go_layer "integration" \
        go test -v -tags=integration -count=1 $GO_TEST_ARGS
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
    
    # Run the working Sakila E2E suite. Test 03 (payment, single-column PK),
    # test 04 (rental -> payment, 2-level tree) and test 05 (payment again, but
    # verified by sha256) perform real archives end-to-end.
    if [ "$SAKILA" = true ]; then
        run_sakila_tests "3 4 5" "working"
        exit 0
    fi

    # Run the validation demonstration tests — expected to fail preflight with
    # documented error categories: 01 = COMPOSITE_PK_CHECK, 02 = FK_COVERAGE_CHECK.
    if [ "$SAKILA_EXAMPLES" = true ]; then
        run_sakila_tests "1 2" "validation demos"
        exit 0
    fi
    
    # Run Go integration tests
    if [ "$INTEGRATION_ONLY" = true ]; then
        run_fmt_check || exit 1
        run_lint_check || exit 1
        run_integration_tests
        exit $?
    fi
    
    # Run Go unit tests
    if [ "$UNIT_ONLY" = true ]; then
        run_fmt_check || exit 1
        run_lint_check || exit 1
        run_unit_tests
        exit $?
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
