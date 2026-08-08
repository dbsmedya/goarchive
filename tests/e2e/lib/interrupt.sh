# E2E harness — interrupting a live run, and asserting what it left behind.
#
# Added by rc-phase-011, which is the first phase whose subject is a PATH rather
# than an end state.
#
# WHY THIS EXISTS. After a successful resume the database looks exactly like it
# does after an uninterrupted run: all rows moved, no duplicates, source drained.
# So the obvious assertions cannot tell "resumed correctly" from "never actually
# interrupted", and the test has a silent-pass region on BOTH sides of its target
# window -- an interrupt too early replays the whole job from scratch and passes, an
# interrupt after the job finished does nothing and passes. Everything here exists
# to make the interrupt land where it was asked to and to fail LOUDLY when it does
# not. An inconclusive run is an ERROR, never a pass.
#
# TWO MECHANISMS, NOT ONE. A single SIGTERM is cooperative: it closes a stop channel
# the loops observe at batch boundaries, so the in-flight batch always runs to a
# terminal state (graceful.go:12-15) and NO non-terminal rows are left. Resume then
# happens through the checkpoint. Only a real crash leaves 'pending'/'copied' rows
# for recover() to replay. They are different code paths, which is why there are two
# tests and two `interrupt` kinds.
#
#   graceful  wait for "Processing batch" batch=N  ->  SIGTERM
#             leaves: checkpoint set, ZERO non-terminal rows, job_status=Idle(0)
#
#   crash     wait for the Nth "Starting delete phase"  ->  SIGKILL
#             leaves: checkpoint NULL, N*batch_size rows at 'copied',
#                     job_status=Running(1) because cleanup() never ran
#
# WHY MARKER-THEN-SIGNAL IS DETERMINISTIC, with no sentinel_file and no sleep.
# "Processing batch" is logged at orchestrator.go:429, AFTER the loop's two
# stopRequested checks, so seeing it proves batch N has passed both. A signal
# arriving any time during batch N closes the stop channel; batch N completes,
# interruptibleSleep returns early, and the next loop-top check breaks. Exactly N
# batches. MEASURED at N=1 (100 rows) and N=3 (300 rows) -- N=3 is the one that
# matters, because it rules out a mechanism that always stops after batch 1 and
# would have made N=1 a vacuous pass.
#
# The deadline is ~1s (until batch N+1 passes the stop check) and the measured
# marker-to-signal latency is 0.004-0.008s. That margin is why this polls the LOG
# and not the database: every get_row_count spawns mysqlsh, which costs ~0.3s
# against that same 1s deadline.
#
# Depends on the caller having defined: TESTS_DIR, PROJECT_ROOT, GOARCHIVE_BIN,
# ARCHIVE_* and SOURCE_* estate variables. Requires lib/log.sh, lib/query.sh,
# lib/runner.sh, lib/tracking.sh.

# How long to wait for a run to reach its interrupt marker, in 0.05s ticks.
# 120s. A run reaches batch 1 in ~100ms, so this only fires when something is
# genuinely wrong -- and then it must fail rather than hang the suite.
INTERRUPT_MARKER_TIMEOUT_TICKS=2400

# Wait until <pattern> appears at least <min_count> times in <log_file>.
#
# Returns 0 when it does; 1 on timeout OR if the process exits first. Those two are
# reported differently on purpose: "the run finished before we could interrupt it"
# is the silent-pass region this phase is built to catch, and it must never be
# confused with a slow machine.
_interrupt_wait_for_pattern() {
    local log_file="$1"
    local pattern="$2"
    local min_count="$3"
    local pid="$4"
    local label="$5"

    local ticks=0 count
    while [[ $ticks -lt $INTERRUPT_MARKER_TIMEOUT_TICKS ]]; do
        # `grep -c` exits 1 when there is no match, but it still PRINTS "0" -- so
        # `|| echo 0` would append a second zero and make $count the two-line string
        # "0\n0", which turns the comparison below into a syntax error on every tick
        # until the marker appears. (It then "worked" only because an erroring [[ is
        # false. Measured, and fixed.) Missing file: grep prints nothing, so the
        # empty-check covers it.
        count=$(grep -c "$pattern" "$log_file" 2>/dev/null)
        [[ -z "$count" ]] && count=0
        if [[ "$count" -ge "$min_count" ]]; then
            log_info "  reached $label after $(awk -v t=$ticks 'BEGIN{printf "%.2f", t*0.05}')s"
            return 0
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            log_error "  the run EXITED before reaching $label"
            log_error "    This is the silent-pass region: a run that finished on its own was"
            log_error "    never interrupted, so a resume was never exercised. It is an ERROR,"
            log_error "    not a pass. Widen the slice or lower the interrupt point."
            return 1
        fi
        sleep 0.05
        ticks=$((ticks + 1))
    done

    log_error "  TIMED OUT after $((INTERRUPT_MARKER_TIMEOUT_TICKS / 20))s waiting for $label"
    log_error "    pattern: $pattern"
    log_error "    Either the run is stuck, or goarchive's log wording changed and this"
    log_error "    marker has stopped matching. Both are findings."
    kill -9 "$pid" 2>/dev/null
    return 1
}

# Run the command under test and interrupt it at a known point.
#
# Usage: run_interrupted_job <config_file> <command> <kind> <after_batches> <log_file>
#        kind = graceful | crash
#
# Deliberately does NOT run validate or dry-run first, unlike run_archive_job: this
# is the throwaway first half of a two-run test, and the second run exercises the
# preflight path anyway. It also invokes the binary DIRECTLY rather than through
# run_archive_job, because backgrounding a shell function makes $! the subshell's
# pid -- signalling that would orphan the binary and hang the test.
run_interrupted_job() {
    local config_file="$1"
    local command="$2"
    local kind="$3"
    local after_batches="$4"
    local log_file="$5"
    local full_config_path="$TESTS_DIR/configs/$config_file"

    local command_flags=""
    case "$command" in
        archive|purge) command_flags="--force-triggers" ;;
        copy-only)     command_flags="" ;;
        *)
            log_error "run_interrupted_job: unsupported command '$command'"
            return 1
            ;;
    esac

    if [[ ! -f "$full_config_path" ]]; then
        log_error "Config file not found: $full_config_path"
        return 1
    fi
    if ! ensure_goarchive_bin; then
        return 1
    fi

    local job_name
    if ! job_name=$(extract_job_name "$full_config_path"); then
        return 1
    fi

    # Pick the marker and the signal. Each kind targets a DIFFERENT interval:
    #   graceful  anywhere inside batch N -- the batch then completes cleanly
    #   crash     after MarkBatchCopied committed (batch_pipeline.go:141) and before
    #             CompleteBatch runs, i.e. inside the delete phase. The only part of
    #             that interval with any duration is the inter-chunk throttle, which
    #             is why a crash test needs a wide delete_sleep_seconds.
    local pattern label signal min_count
    case "$kind" in
        graceful)
            # Field order in the structured log is fixed, so anchoring on the message
            # keeps this from matching some other "batch" field. The trailing comma
            # is load-bearing: without it "batch":1 also matches "batch":10.
            pattern="\"msg\":\"Processing batch\".*\"batch\":${after_batches},"
            label="the start of batch $after_batches"
            signal="TERM"
            min_count=1
            ;;
        crash)
            pattern="Starting delete phase"
            label="delete phase #$after_batches"
            signal="KILL"
            min_count="$after_batches"
            ;;
        *)
            log_error "run_interrupted_job: unknown interrupt kind '$kind' (graceful|crash)"
            return 1
            ;;
    esac

    log_info "  run 1: $command '$job_name', interrupt=$kind at batch $after_batches"
    cd "$PROJECT_ROOT" || return 1

    {
        echo "================ RUN 1 (interrupted: $kind) ================"
    } >> "$log_file"

    # Background the BINARY, not a function -- see the header.
    "$GOARCHIVE_BIN" "$command" --job "$job_name" --config "$full_config_path" \
        $command_flags >> "$log_file" 2>&1 &
    local pid=$!

    if ! _interrupt_wait_for_pattern "$log_file" "$pattern" "$min_count" "$pid" "$label"; then
        return 1
    fi

    kill -"$signal" "$pid" 2>/dev/null
    log_info "  sent SIG$signal to $pid"

    # `wait` yields the job's own status; capture it directly, never after a pipe.
    wait "$pid" 2>/dev/null
    local rc=$?
    log_info "  run 1 exited $rc"

    # A graceful stop is a SUCCESSFUL run -- it breaks out of the batch loop and
    # finalizes normally -- so a non-zero exit means the signal tore something.
    # A crash is SIGKILL, so 137 is the expected exit and 0 would mean the process
    # beat us to the finish line.
    case "$kind" in
        graceful)
            if [[ $rc -ne 0 ]]; then
                log_error "  a graceful stop must exit 0, got $rc"
                log_error "    SIGTERM is cooperative: the loop breaks at a batch boundary and the"
                log_error "    run finalizes normally. A non-zero exit means it did not."
                return 1
            fi
            ;;
        crash)
            if [[ $rc -eq 0 ]]; then
                log_error "  the run exited 0 despite SIGKILL -- it had already completed"
                log_error "    Nothing was interrupted, so nothing will be replayed."
                return 1
            fi
            ;;
    esac
    return 0
}

# Assert what the interrupted run left behind, BEFORE the second run starts.
#
# Usage: assert_interrupt_preconditions <kind> <job_name> <root_table> <root_pk> <expect_dest>
#
# Every check here is deterministic. The one quantity that is NOT -- how many of the
# interrupted batch's source rows were already deleted -- is deliberately not
# asserted: the delete throttle sleeps BETWEEN chunks, so a crash can land with
# anywhere from 0 to batch_size rows gone, and none of the assertions below depend
# on which.
assert_interrupt_preconditions() {
    local kind="$1"
    local job_name="$2"
    local root_table="$3"
    local root_pk="$4"
    local expect_dest="$5"

    local job_id dest ckpt status nonterm copied
    if ! job_id=$(tracking_job_id "$job_name"); then
        log_error "  no archiver_job row for '$job_name' after run 1"
        log_error "    If the destination database was recreated between the runs, the"
        log_error "    tracking tables went with it and there is nothing left to resume."
        return 1
    fi
    log_info "  job id=$job_id"

    # The destination row count at the interrupt point. EXACT, not a window: this is
    # what pins the interrupt to the batch it was asked for. A late interrupt would
    # leave more rows here and invalidate run 2's pacing floor, so it must fail
    # HERE, loudly, rather than surface later as an unexplained floor failure.
    if ! dest=$(get_row_count "$ARCHIVE_HOST" "$ARCHIVE_PORT" "$ARCHIVE_USER" "$ARCHIVE_DB" "$root_table"); then
        log_error "  could not count $root_table in the destination after run 1"
        return 1
    fi
    if [[ "$dest" -ne "$expect_dest" ]]; then
        log_error "  run 1 left $dest row(s) in the destination, expected exactly $expect_dest"
        log_error "    The interrupt did not land on the batch this test declares."
        log_error "    0 rows  -> it landed before any batch completed: nothing to resume,"
        log_error "               and the re-run would silently do the whole job and pass."
        log_error "    more    -> it landed later than declared, so run 2 has fewer batches"
        log_error "               left than min_duration was computed for."
        log_error "    Fix the interrupt point or the declaration. Do NOT lower the floor."
        return 1
    fi
    log_info "  destination holds exactly $dest row(s) at the interrupt point"

    if ! nonterm=$(tracking_nonterminal_count "$job_id"); then return 1; fi
    if ! ckpt=$(tracking_checkpoint "$job_name"); then return 1; fi
    if ! status=$(tracking_job_status "$job_name"); then return 1; fi

    case "$kind" in
        graceful)
            # THE defining property of a cooperative stop, and the reason 08 is not
            # 09: the in-flight batch ran to a terminal state, so recovery has
            # nothing to replay and resume must come from the checkpoint alone.
            if [[ "$nonterm" -ne 0 ]]; then
                log_error "  $nonterm non-terminal row(s) after a GRACEFUL stop, expected 0"
                log_error "    graceful.go: the first signal is observed at batch boundaries, so"
                log_error "    the in-flight batch always reaches a terminal state. Non-terminal"
                log_error "    rows here mean the run was torn mid-batch, which is test 09's"
                log_error "    subject, not this one."
                return 1
            fi
            log_info "  no non-terminal rows, as a graceful stop requires"

            # The checkpoint is 08's ENTIRE resume mechanism, so assert its value and
            # not merely its presence. A checkpoint BELOW what actually landed is
            # invisible to every end-state assertion: run 2 would re-fetch rows that
            # are already deleted from source, process the true remainder, and finish
            # with exactly the right totals.
            if [[ "$ckpt" == "$TRACKING_NULL" ]]; then
                log_error "  the checkpoint is NULL after a graceful stop"
                log_error "    Nothing was committed, so run 2 would restart from the beginning."
                return 1
            fi
            local max_dest
            if ! max_dest=$(_tracking_scalar "MAX($root_pk) in the destination" \
                "SELECT IFNULL(MAX(\`$root_pk\`), '$TRACKING_NULL') FROM \`$ARCHIVE_DB\`.\`$root_table\`;"); then
                return 1
            fi
            if [[ "$ckpt" != "$max_dest" ]]; then
                log_error "  checkpoint is $ckpt but the highest $root_pk copied is $max_dest"
                log_error "    These must agree: the checkpoint is where the forward scan resumes."
                log_error "    A LOWER checkpoint re-opens already-completed roots to the scan and"
                log_error "    is invisible to every end-state count, because the rows it re-reads"
                log_error "    have already been deleted from source."
                return 1
            fi
            log_info "  checkpoint=$ckpt matches the highest $root_pk copied"

            if [[ "$status" -ne 0 ]]; then
                log_error "  job_status=$status after a graceful stop, expected 0 (Idle)"
                log_error "    A cooperative stop finalizes normally, and finalJobStatus maps a"
                log_error "    nil error to Idle. There is no 'completed' status."
                return 1
            fi
            log_info "  job_status=0 (Idle) — the run finalized cleanly"
            ;;

        crash)
            # The crash signature. All three of these are things a cooperative stop
            # CANNOT produce, so together they prove the process really died.
            if ! copied=$(tracking_status_count "$job_id" 1); then return 1; fi
            if [[ "$copied" -ne "$expect_dest" ]]; then
                log_error "  $copied row(s) at 'copied', expected exactly $expect_dest"
                log_error "    The kill missed its window. 'copied' means MarkBatchCopied committed"
                log_error "    and CompleteBatch did not — that interval is the delete phase, and"
                log_error "    the only part of it with any duration is the inter-chunk throttle."
                log_error "    A wider delete_sleep_seconds widens the window."
                return 1
            fi
            log_info "  exactly $copied row(s) left at 'copied' — recovery has work to do"

            if [[ "$ckpt" != "$TRACKING_NULL" ]]; then
                log_error "  checkpoint is $ckpt, expected NULL after a crash mid-batch"
                log_error "    CompleteBatch commits the checkpoint in the same transaction as the"
                log_error "    completion, so a checkpoint here means the batch finished and this"
                log_error "    is a graceful stop, not a crash."
                return 1
            fi
            log_info "  checkpoint is NULL — CompleteBatch never ran"

            if [[ "$status" -ne 1 ]]; then
                log_error "  job_status=$status after SIGKILL, expected 1 (Running)"
                log_error "    A killed process never reaches cleanup(), so the row is left"
                log_error "    claiming to be running. Any other value means the process exited"
                log_error "    through its normal path and this was not a crash."
                return 1
            fi
            log_info "  job_status=1 (Running) — stale, because cleanup() never ran"
            ;;
    esac

    return 0
}

# Assert the tracking tables are fully settled after the resumed run.
#
# Usage: assert_tracking_settled <job_name> <expect_completed>
#
# The real "terminal state" assertion. archiver_job.job_status cannot carry it --
# there is no 'completed' status, and a finished job returns to Idle(0), the same
# value a job that never ran has. The log table can: every root PK must be at
# 'completed' and none may remain replayable.
assert_tracking_settled() {
    local job_name="$1"
    local expect_completed="$2"

    local job_id nonterm completed
    if ! job_id=$(tracking_job_id "$job_name"); then return 1; fi
    if ! nonterm=$(tracking_nonterminal_count "$job_id"); then return 1; fi
    if ! completed=$(tracking_status_count "$job_id" 2); then return 1; fi

    if [[ "$nonterm" -ne 0 ]]; then
        log_error "  $nonterm root PK(s) still non-terminal after the resumed run"
        log_error "    The resume did not finish what it replayed. The row counts can still"
        log_error "    be exactly right — a replayed batch that copied and deleted but never"
        log_error "    reached CompleteBatch leaves the data correct and the log unsettled."
        return 1
    fi
    if [[ "$completed" -ne "$expect_completed" ]]; then
        log_error "  $completed root PK(s) at 'completed', expected $expect_completed"
        log_error "    The log table must account for every root PK in the slice."
        return 1
    fi
    log_info "  tracking settled: $completed completed, 0 non-terminal"
    return 0
}
