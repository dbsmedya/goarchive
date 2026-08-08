# Go test layers and static checks. NOT part of the E2E suite -- kept here only
# because run-tests.sh is the single entry point for both.
#
# Extracted verbatim from run-tests.sh by rc-phase-023. Behaviour unchanged.
#
# Depends on the caller having defined: PROJECT_ROOT, TESTS_DIR, VERBOSE,
# GO_TEST_ARGS. Requires lib/log.sh.

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
