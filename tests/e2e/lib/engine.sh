# E2E harness — the execution engine: one test, and the suite loop around it.
#
# A test is a declaration file at tests/e2e/<category>/NN-<slug>.test.sh. This
# file owns everything a test does NOT declare: resetting the estate, running the
# command, and the five assertion steps.
#
# THE PER-TEST VARIABLE CONTRACT
#
#   test_name       identifier used for the log file name and the report
#   test_desc       one line, printed when the test starts
#   config_file     name of a rendered config in tests/configs/
#   tables          space-separated tables to count, source order
#   mode            "working" (runs to completion) or "example" (must fail preflight)
#   expected_error  mode=example only: substring required in the failure
#   command         mode=working only: archive | purge | copy-only
#   verify_method   the method the run must report, or "none" to assert none ran
#   min_duration    pacing floor in seconds
#   expected_rows   exact rows moved, ONE PER ENTRY in $tables, same order
#   orphan_checks   child:fk:parent:parent_pk pairs, required once $tables has >1
#
# The last four fail closed: a working test that omits one asserts less than it
# claims to, so the engine refuses to run it. That property depends entirely on
# each test starting from empty defaults -- see load_test_vars.
#
# The catalogue of which tests exist and why lives in tests/e2e/README.md, not
# here. A list in both places is a list that disagrees with itself.
#
# Depends on the caller having defined: TESTS_DIR, SPECIFIC_TEST, GOARCHIVE_BIN,
# and the estate variables. Requires lib/log.sh, lib/query.sh, lib/assert.sh,
# lib/estate.sh, lib/runner.sh, lib/registry.sh.

# Source a test file's declarations into the CALLER's scope.
#
# Declares nothing local on purpose. Bash is dynamically scoped, so the sourced
# assignments land on the locals of whichever function called this one -- which is
# how run_e2e_test hands each test a clean slate without this function needing to
# know the variable names.
#
# The only local here is deliberately underscore-prefixed so it cannot collide
# with a per-test variable name.
load_test_vars() {
    local _test_file="$1"
    if [[ ! -f "$_test_file" ]]; then
        log_error "load_test_vars: no such test file: $_test_file"
        return 1
    fi
    # shellcheck disable=SC1090
    . "$_test_file"
}

run_e2e_test() {
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
    local expected_rows=""        # exact rows the run must move, ONE PER ENTRY
                                  # in $tables and in the same order; see
                                  # assert_postcondition for why a direction
                                  # check alone is not enough
    local orphan_checks=""        # referential-integrity pairs to verify in the
                                  # destination, as child:fk:parent:parent_pk,
                                  # space-separated. REQUIRED for any test with
                                  # more than one table; see assert_no_orphans
                                  # for why exact counts do not cover this
    local start_time end_time duration
    local test_result="PASS"
    local test_error=""

    # Resolve the number to exactly one test file. The registry fails closed both
    # on a number with no file and on a number claimed by two files, because both
    # have a silent form: the first would report a clean run of nothing, the
    # second would run one test and ignore the other.
    local test_file test_category
    if ! test_file=$(registry_file_for "$test_num"); then
        return 1
    fi
    test_category=$(registry_category_for_file "$test_file")

    # THE PER-TEST VARIABLES ARE DECLARED local ABOVE AND LOADED HERE, IN THAT
    # ORDER, AND THE ORDER IS LOAD-BEARING.
    #
    # Bash is dynamically scoped, so the assignments in the sourced file land on
    # THIS function's locals rather than on globals. That is the only reason each
    # test starts from the empty defaults above -- and the fail-closed guards
    # below (no expected_rows, no min_duration, no orphan_checks on a multi-table
    # test) work only because the previous test's values are gone.
    #
    # Lift the load out of this function, or drop the `local` declarations, and
    # every one of those guards silently stops firing while the suite goes on
    # reporting a pass. Verified on bash 3.2.57; the suite loop asserts after
    # every test that nothing leaked to global scope.
    if ! load_test_vars "$test_file"; then
        return 1
    fi

    # A file that declares nothing at all would otherwise reach the assertions
    # with every value empty and be reported against a blank name.
    if [[ -z "$test_name" ]]; then
        log_error "Test $test_num: $test_file set no test_name"
        log_error "  a test file must declare at least test_name, mode and config_file"
        return 1
    fi

    # Split expected_rows into a parallel indexed array matching $tables by
    # position. bash 3.2 has no `declare -A`, and this must NOT be done with
    # `set -- $expected_rows` inside STEP 4's loop: that clobbers this
    # function's own positional parameters.
    local expected_arr=()
    local n
    for n in $expected_rows; do
        expected_arr+=("$n")
    done

    # Catch a miscount HERE, before a 60s source reseed, and name the real
    # fault. Left to STEP 4, a short list hands an empty value to the extra
    # table and fails with "no expected row count", which points at the wrong
    # thing.
    if [[ "$mode" == "working" ]]; then
        local table_count=0
        for n in $tables; do
            table_count=$((table_count + 1))
        done
        if [[ ${#expected_arr[@]} -ne $table_count ]]; then
            log_error "Test $test_num: expected_rows has ${#expected_arr[@]} entr(ies) but tables has $table_count"
            log_error "  tables:        $tables"
            log_error "  expected_rows: $expected_rows"
            return 1
        fi

        # Fail closed on referential integrity. A multi-table test copies a
        # parent/child graph, and exact row counts cannot see a child landing
        # without its parent -- so a test that declares more than one table and
        # no orphan_checks is asserting less than it appears to. Single-table
        # tests have no edge to check, which is why the rule keys off the table
        # count rather than being universally required.
        if [[ $table_count -gt 1 && -z "$orphan_checks" ]]; then
            log_error "Test $test_num: $table_count tables but no orphan_checks configured"
            log_error "  tables: $tables"
            log_error "  A multi-table test must declare at least one"
            log_error "  child:fk:parent:parent_pk pair. Exact row counts stay true even"
            log_error "  when every copied child points at a parent that never arrived."
            return 1
        fi

        # Validate the SHAPE here too, before the reseed. Parsed leniently, a
        # malformed spec would silently check some other pair of tables -- and
        # pass.
        local spec
        for spec in $orphan_checks; do
            local colons
            colons=$(echo "$spec" | tr -cd ':' | wc -c | tr -d ' ')
            if [[ "$colons" -ne 3 ]]; then
                log_error "Test $test_num: malformed orphan_checks entry '$spec'"
                log_error "  want child:fk:parent:parent_pk (three colons), got $colons"
                return 1
            fi
        done
    fi
    
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
            if ! assert_postcondition "$command" "$table" "${before_counts[$idx]}" "$after_src" "$after_dst" "${expected_arr[$idx]}"; then
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

    # Step 5: Assert referential integrity in the destination.
    #
    # A separate axis from Step 4, not a refinement of it. Step 4 asks "did the
    # right NUMBER of rows move"; this asks "are the rows that moved internally
    # consistent". Both can hold while the other fails: 200 payments referencing
    # 200 absent rentals satisfies every count in Step 4.
    if [[ "$mode" == "working" && -n "$orphan_checks" ]]; then
        log_info "[STEP 5] Asserting referential integrity in the destination..."
        local check
        for check in $orphan_checks; do
            # Pure parameter expansion -- no IFS juggling and no subshell. The
            # three-colon shape was already validated up front.
            local c_tbl c_fk p_tbl p_pk rest
            c_tbl="${check%%:*}"
            rest="${check#*:}"
            c_fk="${rest%%:*}"
            rest="${rest#*:}"
            p_tbl="${rest%%:*}"
            p_pk="${rest##*:}"

            if ! assert_no_orphans "$c_tbl" "$c_fk" "$p_tbl" "$p_pk"; then
                end_time=$(date +%s)
                duration=$((end_time - start_time))
                echo "" >> "$log_file"
                echo "Result: FAIL (orphaned $c_tbl rows in the destination)" >> "$log_file"
                echo "Duration: ${duration}s" >> "$log_file"
                return 1
            fi
            echo "  $c_tbl -> $p_tbl: no orphans in the destination" >> "$log_file"
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

# Run a set of E2E tests. First argument is a space-separated list of test
# numbers. Second argument is a human label ("working" or "validation demos").
#
# The number list is EXPLICIT and ordered by the caller. It is not derived from
# the filesystem: globbing tests/e2e/*/ would order by category name, giving
# archive(03,04,05), copy-only(07), purge(06) -- silently reordering the suite.
run_e2e_suite() {
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

    # Refuse to start on an ambiguous registry. registry_file_for already fails
    # closed per number, but only for the numbers this run was asked for; a
    # duplicate on an untouched number would sit undetected until some later run
    # happened to request it.
    if ! registry_validate; then
        exit 1
    fi

    # Prove the harness can tell a failed query from an empty one BEFORE trusting
    # any assertion built on it. Costs one refused connection (~0.3s).
    if ! selftest_get_row_count_fails_loud; then
        exit 1
    fi
    if ! selftest_count_orphans_fails_loud; then
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
        if run_e2e_test "$i"; then
            ((passed++))
        else
            ((failed++))
        fi

        # Isolation guard, and it asserts the REAL mechanism rather than a copy of
        # it: this function deliberately does not declare the per-test variables,
        # so if run_e2e_test ever stops declaring them local -- or the load is
        # lifted out of it -- the sourced assignments land HERE, in global scope,
        # and are visible now.
        #
        # That breakage has no other symptom. Every test would inherit the
        # previous one's values, every fail-closed guard would stop firing, and
        # the suite would keep reporting a pass.
        if [[ -n "${expected_rows:-}${min_duration:-}${orphan_checks:-}${verify_method:-}" ]]; then
            log_error "HARNESS SELF-CHECK FAILED: test $i leaked its variables out of run_e2e_test."
            log_error "  expected_rows='${expected_rows:-}' min_duration='${min_duration:-}'"
            log_error "  orphan_checks='${orphan_checks:-}' verify_method='${verify_method:-}'"
            log_error "  Per-test values must be local to run_e2e_test. Leaked, they carry into"
            log_error "  the next test and every fail-closed guard in the engine stops firing."
            exit 1
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
