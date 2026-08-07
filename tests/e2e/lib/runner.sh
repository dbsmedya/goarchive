# E2E harness — resolving and invoking the binary under test.
#
# Extracted verbatim from run-tests.sh by rc-phase-023. Behaviour unchanged.
#
# Depends on the caller having defined: GOARCHIVE_BIN, GOARCHIVE_BIN_EXPLICIT,
# PROJECT_ROOT, TESTS_DIR. Requires lib/log.sh.

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
