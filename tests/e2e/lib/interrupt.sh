# E2E harness — interrupting a live run, and asserting what it left behind.
#
# Added by rc-phase-011, which is the first phase whose subject is a PATH rather
# than an end state. Rewritten for the 3.0 engine in M7b.
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
# TWO MECHANISMS, NOT ONE. A single SIGTERM is cooperative: the run finishes the
# batch in flight and stops at the next batch boundary (runJob,
# cmd/goarchive/cmd/runjob.go; (*run).loop, internal/engine/run.go), so every logged
# root is terminal and recovery has nothing to replay. Only a real crash leaves
# 'copied' roots for startup recovery to replay (spec §6.4). They are different code
# paths, which is why there are two tests and two `interrupt` kinds.
#
#   graceful  wait for the engine's "Batch N:" line  ->  SIGTERM
#             leaves: N batches completed, ZERO non-terminal roots, job_status=Idle(0)
#
#   crash     wait until the log holds N batches' roots, the last batch's at
#             'copied'  ->  SIGKILL
#             leaves: that batch's roots at 'copied' (its copy committed, its deletes
#                     not finished), job_status=Running(1) because the final status
#                     was never written
#
# WHY THE GRACEFUL MARKER IS DETERMINISTIC. "Batch N: <n> roots of <table>" is logged
# at the end of batch N ((*run).batch), and the loop then sleeps sleep_seconds
# before its stop check. A signal arriving in that sleep cuts it short and the loop
# stops before fetching batch N+1: exactly N batches. The marker-to-signal latency is
# milliseconds against the configured sleep, which is why this polls the LOG and not
# the database.
#
# WHY THE CRASH MARKER POLLS THE DATABASE. The 3.0 engine logs nothing between a
# batch's destination commit -- which writes its roots as 'copied' -- and its deletes.
# The only observable is the log table itself. The window is the delete phase, whose
# only duration is the throttle between delete statements, so a crash test sets a
# wide delete_sleep_seconds; a poll costs one mysqlsh call per read (~0.3s), well
# inside that window.
#
# Depends on the caller having defined: TESTS_DIR, PROJECT_ROOT, GOARCHIVE_BIN,
# ARCHIVE_* and SOURCE_* estate variables. Requires lib/log.sh, lib/query.sh,
# lib/runner.sh, lib/tracking.sh.

# How long to wait for a run to reach its interrupt point, in ticks.
# 2400 ticks of 0.05s = 120s. A run reaches batch 1 in well under a second, so this
# only fires when something is genuinely wrong -- and then it must fail rather than
# hang the suite.
INTERRUPT_MARKER_TIMEOUT_TICKS=2400
INTERRUPT_POLL_SECONDS=0.05

# The log line that marks the end of batch N for an interrupt kind, as a grep
# pattern. The trailing ": " is load-bearing: without it "Batch 1" also matches
# "Batch 10".
interrupt_marker_pattern() {
    local kind="$1"
    local n="$2"
    case "$kind" in
        graceful) printf '%s\n' "\"msg\":\"Batch ${n}: " ;;
        *)
            log_error "interrupt_marker_pattern: no log marker for kind '$kind'"
            return 1
            ;;
    esac
}

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
        # until the marker appears. Missing file: grep prints nothing, so the
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
        sleep "$INTERRUPT_POLL_SECONDS"
        ticks=$((ticks + 1))
    done

    log_error "  TIMED OUT waiting for $label"
    log_error "    pattern: $pattern"
    log_error "    Either the run is stuck, or goarchive's log wording changed and this"
    log_error "    marker has stopped matching. Both are findings."
    kill -9 "$pid" 2>/dev/null
    return 1
}

# Wait until job <job>'s log holds exactly <want> roots, some of them at 'copied':
# the last batch's copy has committed and its deletes have not finished.
#
# Usage: _interrupt_wait_for_copied <job_name> <want> <pid>
#
# A read that fails -- the job row or its log table not created yet -- counts as
# "not there yet". The process exiting first is the silent-pass region and fails.
_interrupt_wait_for_copied() {
    local job="$1"
    local want="$2"
    local pid="$3"

    # One mysqlsh call per tick once the job's id is known: the window is one batch's delete
    # phase (a few seconds), and three calls a tick missed it on a slower runner (release
    # verify, beta.1 PR 4: Percona gates, test 09).
    local ticks=0 id= counts copied completed
    while [[ $ticks -lt $INTERRUPT_MARKER_TIMEOUT_TICKS ]]; do
        [[ -n $id ]] || id=$(tracking_job_id "$job" 2>/dev/null) || id=
        if [[ -n $id ]] && counts=$(tracking_copied_completed "$id" 2>/dev/null); then
            read -r copied completed <<<"$counts"
            if [[ "$copied" -gt 0 && $((copied + completed)) -eq "$want" ]]; then
                log_info "  the log holds $want root(s), $copied at 'copied'"
                return 0
            fi
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            log_error "  the run EXITED before its log held $want root(s) with some at 'copied'"
            log_error "    This is the silent-pass region: a run that finished on its own was"
            log_error "    never interrupted, so a replay was never exercised. It is an ERROR,"
            log_error "    not a pass. Widen the delete throttle or lower the interrupt point."
            return 1
        fi
        sleep "$INTERRUPT_POLL_SECONDS"
        ticks=$((ticks + 1))
    done
    log_error "  TIMED OUT waiting for job '$job' to log $want root(s) with some at 'copied'"
    kill -9 "$pid" 2>/dev/null
    return 1
}

# Run the command under test and interrupt it at a known point.
#
# Usage: run_interrupted_job <config_file> <command> <kind> <after_batches> <expect_dest> <log_file>
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
    local expect_dest="$5"
    local log_file="$6"
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
    case "$kind" in
        graceful|crash) ;;
        *)
            log_error "run_interrupted_job: unknown interrupt kind '$kind' (graceful|crash)"
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

    log_info "  run 1: $command '$job_name', interrupt=$kind at batch $after_batches"
    cd "$PROJECT_ROOT" || return 1

    {
        echo "================ RUN 1 (interrupted: $kind) ================"
    } >> "$log_file"

    # Background the BINARY, not a function -- see the header.
    "$GOARCHIVE_BIN" "$command" --job "$job_name" --config "$full_config_path" \
        $command_flags >> "$log_file" 2>&1 &
    local pid=$!

    local signal
    case "$kind" in
        graceful)
            local pattern
            pattern=$(interrupt_marker_pattern graceful "$after_batches") || return 1
            if ! _interrupt_wait_for_pattern "$log_file" "$pattern" 1 "$pid" "the end of batch $after_batches"; then
                return 1
            fi
            signal="TERM"
            ;;
        crash)
            if ! _interrupt_wait_for_copied "$job_name" "$expect_dest" "$pid"; then
                return 1
            fi
            signal="KILL"
            ;;
    esac

    kill -"$signal" "$pid" 2>/dev/null
    log_info "  sent SIG$signal to $pid"

    # `wait` yields the job's own status; capture it directly, never after a pipe.
    wait "$pid" 2>/dev/null
    local rc=$?
    log_info "  run 1 exited $rc"

    # A graceful stop is a SUCCESSFUL run -- it stops at a batch boundary and
    # reports normally -- so a non-zero exit means the signal tore something.
    # A crash is SIGKILL, so 137 is the expected exit and 0 would mean the process
    # beat us to the finish line.
    case "$kind" in
        graceful)
            if [[ $rc -ne 0 ]]; then
                log_error "  a graceful stop must exit 0, got $rc"
                log_error "    SIGTERM is cooperative: the loop stops at a batch boundary and the"
                log_error "    run reports normally. A non-zero exit means it did not."
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
# Usage: assert_interrupt_preconditions <kind> <job_name> <root_table> <expect_dest> <run1_log>
#
# On success it sets INTERRUPT_PENDING: the roots run 1 left at 'copied', which run 2's
# recovery must replay (0 after a graceful stop). After a crash in batch N, batches
# 1..N-1 are 'completed' and only batch N is 'copied', so this is not expect_dest.
#
# Every check here is deterministic. The one quantity that is NOT -- how many of the
# interrupted batch's source rows were already deleted -- is deliberately not
# asserted: the delete throttle sleeps BETWEEN statements, so a crash can land with
# anywhere from 0 to batch_size rows gone, and none of the assertions below depend
# on which.
assert_interrupt_preconditions() {
    local kind="$1"
    local job_name="$2"
    local root_table="$3"
    local expect_dest="$4"
    local run1_log="$5"

    local job_id dest status nonterm copied completed deleting
    INTERRUPT_PENDING=""
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
    if ! status=$(tracking_job_status "$job_name"); then return 1; fi

    case "$kind" in
        graceful)
            # THE defining property of a cooperative stop, and the reason 08 is not
            # 09: the batch in flight ran to a terminal state, so recovery has
            # nothing to replay and run 2 simply continues the where range.
            if [[ "$nonterm" -ne 0 ]]; then
                log_error "  $nonterm non-terminal root(s) after a GRACEFUL stop, expected 0"
                log_error "    The first signal is honoured at batch boundaries, so the batch in"
                log_error "    flight always reaches 'completed'. Non-terminal roots here mean the"
                log_error "    run was torn mid-batch, which is test 09's subject, not this one."
                return 1
            fi
            log_info "  no non-terminal roots, as a graceful stop requires"

            # 3.0 keeps no cursor (R10): the log's completed roots are the record of
            # what run 1 finished, and they must be exactly what landed.
            if ! completed=$(tracking_status_count "$job_id" 2); then return 1; fi
            if [[ "$completed" -ne "$expect_dest" ]]; then
                log_error "  $completed root(s) at 'completed' after a graceful stop, expected $expect_dest"
                return 1
            fi
            log_info "  exactly $completed root(s) at 'completed'"

            if ! grep -q "stopped before the end of the where range" "$run1_log"; then
                log_error "  run 1's report does not say it stopped before the end of the where range"
                log_error "    A graceful stop reports it (printReport). Without the line, run 1"
                log_error "    either finished the range or the report's wording changed."
                return 1
            fi
            log_info "  run 1 reported its stop before the end of the where range"

            if [[ "$status" -ne 0 ]]; then
                log_error "  job_status=$status after a graceful stop, expected 0 (Idle)"
                log_error "    A cooperative stop ends the run normally, which writes Idle."
                log_error "    There is no 'completed' status."
                return 1
            fi
            log_info "  job_status=0 (Idle) — the run ended cleanly"
            INTERRUPT_PENDING=0
            ;;

        crash)
            # The crash signature. All of these are things a cooperative stop
            # CANNOT produce, so together they prove the process really died.
            #
            # The destination holds every root run 1 copied: the earlier batches'
            # at 'completed' and the killed batch's at 'copied'. Only the latter are
            # recovery's work.
            if ! copied=$(tracking_status_count "$job_id" 1); then return 1; fi
            if ! completed=$(tracking_status_count "$job_id" 2); then return 1; fi
            if [[ "$copied" -eq 0 ]]; then
                log_error "  no root at 'copied' ($completed completed), expected the killed batch's"
                log_error "    The kill missed its window. 'copied' means the batch's destination"
                log_error "    commit happened and its completion did not — that interval is the"
                log_error "    delete phase, whose only duration is the throttle between delete"
                log_error "    statements. A wider delete_sleep_seconds widens the window."
                return 1
            fi
            if [[ $((copied + completed)) -ne "$expect_dest" ]]; then
                log_error "  the log holds $copied copied + $completed completed root(s), expected $expect_dest"
                log_error "    Every root in the destination must be logged as copied or completed."
                return 1
            fi
            log_info "  exactly $copied root(s) left at 'copied' ($completed completed) — recovery has work to do"

            if ! deleting=$(tracking_status_count "$job_id" 4); then return 1; fi
            if [[ "$deleting" -ne 0 ]]; then
                log_error "  $deleting root(s) at 'deleting' after an archive crash; archive never writes it"
                return 1
            fi

            if [[ "$status" -ne 1 ]]; then
                log_error "  job_status=$status after SIGKILL, expected 1 (Running)"
                log_error "    A killed process never writes its final status, so the row is left"
                log_error "    claiming to be running. Any other value means the process exited"
                log_error "    through its normal path and this was not a crash."
                return 1
            fi
            log_info "  job_status=1 (Running) — stale, because the final status was never written"
            INTERRUPT_PENDING=$copied
            ;;
        *)
            log_error "assert_interrupt_preconditions: unknown kind '$kind'"
            return 1
            ;;
    esac

    return 0
}

# Assert the resumed run copied only what it still had left to copy, by the path
# its interrupt kind requires.
#
# Usage: assert_resume_copy_accounting <kind> <assert_log> <root_table> <expect_total> <expect_at_interrupt> <pending>
#
# expect_at_interrupt is every root run 1 copied; pending is the part of it left at
# 'copied' (assert_interrupt_preconditions's INTERRUPT_PENDING), which recovery replays.
#
# After a crash, recovery must promote the 'copied' roots by DELETE-ONLY replay
# (spec §6.4): their copy already committed and was verified, so it is not redone.
# Recovery logs "Recovery: replaying <n> copied roots of <table>", and the report
# counts them under "recovered:". Run 2 then copies only the rest of the slice.
#
# After a graceful stop there is nothing to recover, and run 2 copies the rest.
#
# The copy counter is what this adds to the end-state checks. A crash whose kill
# landed before any delete leaves source and destination agreeing, so a re-copy of
# the batch would verify cleanly, the destination total would not change, and the
# extra work would only make the one-sided pacing floor easier. The report's
# per-table copy count is then the only number that moves.
assert_resume_copy_accounting() {
    local kind="$1"
    local assert_log="$2"
    local root="$3"
    local expect_total="$4"
    local expect_at_interrupt="$5"
    local pending="$6"

    case "$pending" in
        ''|*[!0-9]*)
            log_error "assert_resume_copy_accounting: pending='$pending' is not a count of copied roots"
            return 1
            ;;
    esac

    local want=$((expect_total - expect_at_interrupt))
    local line got
    line=$(grep -E '^  copied:  ' "$assert_log" | tail -1)
    got=$(printf '%s\n' "$line" | tr ' ' '\n' | sed -n "s/^${root}=\([0-9][0-9]*\)$/\1/p")
    if [[ -z "$got" ]]; then
        log_error "  could not read $root's copied rows from the resumed run's report"
        log_error "    The report prints \"copied:  <table>=<n> ...\"; if that changed, this"
        log_error "    assertion has stopped matching and is no longer checking anything."
        return 1
    fi
    if [[ "$got" -ne "$want" ]]; then
        log_error "  run 2 copied $got $root row(s), expected exactly $want"
        log_error "    ($expect_total in the slice, $expect_at_interrupt already copied before the interrupt.)"
        if [[ "$got" -eq "$expect_total" ]]; then
            log_error "    It copied the WHOLE slice, so the roots run 1 had already copied were"
            log_error "    re-copied instead of promoted by delete-only replay."
        fi
        return 1
    fi
    log_info "  run 2 copied exactly $got — the $expect_at_interrupt already-copied root(s) were not re-copied"

    case "$kind" in
        crash)
            if ! grep -q "Recovery: replaying ${pending} copied roots of ${root}\"" "$assert_log" ||
                ! grep -q "^  recovered: ${pending} logged roots finished by replay$" "$assert_log"; then
                log_error "  run 2 did not replay exactly the $pending copied root(s) of $root"
                log_error "    Observed:"
                grep -E 'Recovery: replaying|recovered: ' "$assert_log" | head -2 | sed 's/^/      /'
                return 1
            fi
            log_info "  recovery replayed exactly $pending copied root(s) (delete-only)"
            ;;
        graceful)
            if grep -qE 'Recovery: replaying|^  recovered: ' "$assert_log"; then
                log_error "  run 2 recovered roots after a GRACEFUL stop"
                log_error "    A cooperative stop leaves every root terminal, so there is nothing to"
                log_error "    replay. Recovery here means this test has quietly become the crash test."
                return 1
            fi
            log_info "  run 2 recovered nothing, as a graceful stop requires"
            ;;
        *)
            log_error "assert_resume_copy_accounting: unknown kind '$kind'"
            return 1
            ;;
    esac
    return 0
}

# Assert the tracking tables are fully settled after a run.
#
# Usage: assert_tracking_settled <job_name> <expect_completed>
#
# The real "terminal state" assertion. archiver_job.job_status cannot carry it --
# there is no 'completed' status, and a finished job returns to Idle(0), the same
# value a job that never ran has. The log table can: every root of the slice must be
# at 'completed' and none may remain replayable. Every job type logs every root it
# processes (spec §6.3), so this holds for archive, purge and copy-only.
assert_tracking_settled() {
    local job_name="$1"
    local expect_completed="$2"

    local job_id nonterm completed
    if ! job_id=$(tracking_job_id "$job_name"); then return 1; fi
    if ! nonterm=$(tracking_nonterminal_count "$job_id"); then return 1; fi
    if ! completed=$(tracking_status_count "$job_id" 2); then return 1; fi

    if [[ "$nonterm" -ne 0 ]]; then
        log_error "  $nonterm root(s) still non-terminal after the run"
        log_error "    The run did not finish what it logged. The row counts can still be"
        log_error "    exactly right — a batch that copied and deleted but never reached its"
        log_error "    completion leaves the data correct and the log unsettled."
        return 1
    fi
    if [[ "$completed" -ne "$expect_completed" ]]; then
        log_error "  $completed root(s) at 'completed', expected $expect_completed"
        log_error "    The log table must account for every root in the slice."
        return 1
    fi
    log_info "  tracking settled: $completed completed, 0 non-terminal"
    return 0
}
