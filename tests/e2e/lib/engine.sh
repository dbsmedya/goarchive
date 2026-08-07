# E2E harness — the execution engine: one test, and the suite loop around it.
#
# Extracted verbatim from run-tests.sh by rc-phase-023. Behaviour unchanged; the
# per-test case switch is restructured into tests/e2e/<category>/*.test.sh by a
# later step of the same phase.
#
# Depends on the caller having defined: TESTS_DIR, SPECIFIC_TEST, GOARCHIVE_BIN,
# and the estate variables. Requires lib/log.sh, lib/query.sh, lib/assert.sh,
# lib/estate.sh, lib/runner.sh.

# Run specific Sakila test. First argument is the test number. There are seven:
#   01  Composite-PK rejection    -> expects COMPOSITE_PK_CHECK   [validation demo]
#   02  Uncovered FK coverage     -> expects FK_COVERAGE_CHECK     [validation demo]
#   03  Payment batch             -> working archive (count verification, inherited)
#   04  rental -> payment         -> working archive (count verification, inherited)
#   05  Payment + sha256          -> working archive (sha256 verification, explicit)
#   06  Payment purge             -> working purge (no verification stage at all)
#   07  rental -> payment copy    -> working copy-only (source must be UNTOUCHED)
# Tests 01-02 are validation demos (mode=example) and only run when --sakila-examples
# is set; tests 03-07 are working runs (mode=working) and run to completion.
#
# 03 and 05 are deliberately the same archive with different verification methods,
# so a failure in 05 alone isolates to the method. 06 and 07 are the only
# non-archive commands in the suite. 04 and 07 share a graph and a slice and
# differ in the command, so a failure in 07 alone isolates to copy-only -- with
# one deliberate exception, the pacing: 04 runs the slice in a single batch,
# while 07 splits it into 20 so the sleep is exercised more than once.
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
            # payment_id <= 2000 selects 1999 rows, not 2000 -- payment_id is
            # not contiguous. Measured, not computed.
            expected_rows="1999"
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
            # One number per table, in the SAME ORDER as $tables above.
            # rental_id <= 200 selects 200 rentals, which pull in 200 payments.
            expected_rows="200 200"
            # The counts above cannot see a payment copied without its rental:
            # this config disables FK checks on the copy path (it must -- payment
            # references the uncopied customer and staff), so the destination
            # accepts an orphan silently.
            orphan_checks="payment:rental_id:rental:rental_id"
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
            expected_rows="1999"
            ;;
        6)
            test_name="Test06_PaymentPurge"
            test_desc="Working purge: delete half the payment table without copying anything"
            config_file="test06_payment_purge.yaml"
            tables="payment"
            mode="working"
            command="purge"
            # Purge is batchDeleteOnly: the verify block sits inside the
            # batchFull||batchCopyVerify gate, so dataVerifier is never called.
            # "none" asserts the verification line is ABSENT -- so a purge that
            # silently gained a verify stage, or an archive misconfigured as a
            # purge, fails instead of passing quietly.
            verify_method="none"
            # 8022 rows / batch_size 500 = 17 batches (16 full, one 22-row tail).
            #   batch sleep:  17 x 0.2                        = 3.4s
            #   delete sleep: 16 batches x 4 chunk-gaps x 0.2 = 12.8s
            # (500 / batch_delete_size 100 = 5 chunks -> 4 gaps. The 22-row tail
            # batch is a single chunk and contributes no gap.)
            min_duration="16.2"
            # HALF the table, exactly: payment_id <= 8024 selects 8022 of
            # payment's 16044 rows. The boundary is 8024 rather than 8022
            # because two ids below it are missing; it was queried against the
            # fixture, not computed. Over-deletion is purge's unrecoverable
            # failure, and this number is the only thing that catches it.
            expected_rows="8022"
            ;;
        7)
            test_name="Test07_RentalPaymentCopyOnly"
            test_desc="Working copy-only: copy rental -> payment, leaving the source untouched"
            config_file="test07_rental_payment_copyonly.yaml"
            tables="rental payment"
            mode="working"
            command="copy-only"
            # copy-only is batchCopyVerify, so the verify stage DOES run -- the
            # copy+verify block is gated on batchFull||batchCopyVerify
            # (batch_pipeline.go:122). Method is inherited, so "count".
            verify_method="count"
            # Same 200-rental slice as test 04, but NOT test 04's pacing. That
            # config inherits batch_size 1000, making 200 rentals a single batch,
            # and one batch exercises one sleep call -- no per-batch accumulation
            # bug is reachable. This config sets batch_size 10:
            #   200 rentals / 10 = 20 batches
            #   batch sleep: 20 x 0.5 = 10.0s
            # Batch term ONLY. copy-only never runs DeletePhase (gated on
            # batchFull||batchDeleteOnly, batch_pipeline.go:146), so
            # delete_sleep_seconds is inert for this command and the config does
            # not set it. Adding a delete term here would build a gate that no
            # correct run can pass.
            min_duration="10.0"
            # Same slice as test 04, so the same counts -- but for copy-only the
            # assertion reads differently: the source must be UNCHANGED and the
            # destination must hold exactly these. See assert_postcondition.
            expected_rows="200 200"
            orphan_checks="payment:rental_id:rental:rental_id"
            ;;
        *)
            log_error "Invalid test number: $test_num (expected 1-7)"
            return 1
            ;;
    esac

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
