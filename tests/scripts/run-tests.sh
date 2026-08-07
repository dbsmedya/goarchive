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
#   --sakila            Run the working Sakila E2E tests (03, 04, 05, 06)
#   -t, --test NUM      Run only specific Sakila test (1-6)
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


# The harness itself lives in tests/e2e/. This file is the entry point: it owns
# the environment, the flags and the dispatch, and nothing else. Sourced in
# dependency order.
#
# Sourcing only DEFINES functions, so the estate variables further down this file
# do not need to exist yet -- they are read when a function is called.
for _lib in log query assert estate runner golayer registry engine; do
    if [ ! -f "$TESTS_DIR/e2e/lib/$_lib.sh" ]; then
        echo "ERROR: missing harness library $TESTS_DIR/e2e/lib/$_lib.sh" >&2
        exit 1
    fi
    . "$TESTS_DIR/e2e/lib/$_lib.sh"
done
unset _lib

render_test_configs || exit 1

SETUP=false
SAKILA=false            # Working Sakila E2E tests (03 payment, 04 rental->payment, 05 payment+sha256, 06 payment purge)
SAKILA_EXAMPLES=false   # Validation-failure demonstration tests (01 composite-PK, 02 FK-index)
SPECIFIC_TEST=""
UNIT_ONLY=false
INTEGRATION_ONLY=false
FMT_CHECK=false
LINT_CHECK=false
VERBOSE=""
SKIP_DOCKER=false
GO_TEST_ARGS=""

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
            echo "  --sakila            Run the working Sakila E2E tests (03 payment, 04 rental->payment, 05 payment+sha256, 06 payment purge)"
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
    # verified by sha256) perform real archives end-to-end; test 06 purges half
    # the payment table, deleting without copying.
    if [ "$SAKILA" = true ]; then
        run_e2e_suite "3 4 5 6 7" "working"
        exit 0
    fi

    # Run the validation demonstration tests — expected to fail preflight with
    # documented error categories: 01 = COMPOSITE_PK_CHECK, 02 = FK_COVERAGE_CHECK.
    if [ "$SAKILA_EXAMPLES" = true ]; then
        run_e2e_suite "1 2" "validation demos"
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
