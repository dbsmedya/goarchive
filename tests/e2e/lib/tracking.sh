# E2E harness — reading goarchive's own tracking tables.
#
# Added by rc-phase-011.
#
# The tracking tables are how a resumed run knows what it already did, so a resume
# test cannot be asserted without reading them. Every other assertion in this suite
# looks at user data; these look at goarchive's bookkeeping.
#
#   <job_schema>.archiver_job              one row per job: status + checkpoint
#   <job_schema>.archiver_job_log_<id>     one row per root PK: log_status
#
# <job_schema> is destination.job_schema, which defaults to the destination
# database -- and no test config sets it, so everything here reads $ARCHIVE_DB.
# The log table is named by archiver_job.id (an autoincrement), NOT by the job
# name, so the id must always be resolved first.
#
# log_status: 0=pending 1=copied 2=completed 3=failed(legacy, never written now).
# 0 and 1 are NON-TERMINAL -- they are what status-aware recovery replays.
#
# job_status: 0=Idle 1=Running 2=Paused 3=Failed. THERE IS NO "COMPLETED".
# A finished job returns to Idle(0), the same value a job that never ran carries,
# so "job_status is terminal" asserts almost nothing on its own. What it IS good
# for is a crash signature: a SIGKILLed run never reaches cleanup(), so it leaves
# the row at Running(1).
#
# CONTRACT, deliberately identical to get_row_count's (lib/query.sh):
#   stdout / exit 0   the value
#   stderr / exit 1   the query failed, or returned no row -- callers MUST check
#
# A failed query must NEVER be reported as a value. "0 non-terminal rows" is the
# assertion a resume test passes on, so a tracking query that silently read 0 on a
# broken connection would report a clean resume without ever having connected.
# selftest_tracking_fails_loud proves it does not, and runs on every suite.
#
# Depends on the caller having defined: SCRIPT_DIR, ARCHIVE_HOST, ARCHIVE_PORT,
# ARCHIVE_USER, ARCHIVE_DB. Requires lib/log.sh.

# The sentinel a SQL NULL is mapped to. A bare NULL renders as the four-character
# string "NULL", which is indistinguishable from a column holding that text; the
# sentinel cannot be confused with any real checkpoint value.
TRACKING_NULL='__NULL__'

# Run one scalar query against the destination and emit exactly one value.
#
# mysql-query.sh emits a header line then the rows, and an EMPTY result set emits
# NOTHING AT ALL (measured) -- so an absent row yields an empty value here rather
# than silently handing back the column header.
_tracking_scalar() {
    local what="$1"
    local sql="$2"

    # `local out` on its own line: written as `local out=$(...)` the exit status
    # would be `local`'s (always 0) rather than the query's. Same reason as
    # get_row_count -- and the same defect if it is ever collapsed.
    local out
    if ! out=$(MYSQL_QUERY_HOST="$ARCHIVE_HOST" MYSQL_QUERY_USER="$ARCHIVE_USER" \
        "$SCRIPT_DIR/mysql-query.sh" "$ARCHIVE_PORT" "$sql"); then
        echo "tracking: could not read $what from ${ARCHIVE_DB} on ${ARCHIVE_HOST}:${ARCHIVE_PORT}" >&2
        return 1
    fi

    if [[ -z "$out" ]]; then
        echo "tracking: $what returned no row (the job or its log table does not exist)" >&2
        return 1
    fi

    printf '%s\n' "$(printf '%s\n' "$out" | tail -1)"
}

# The archiver_job.id for a job name. Every log-table query needs it.
tracking_job_id() {
    local job="$1"
    _tracking_scalar "archiver_job.id for job '$job'" \
        "SELECT id FROM \`$ARCHIVE_DB\`.archiver_job WHERE job_name = '$job';"
}

# archiver_job.job_status (0=Idle 1=Running 2=Paused 3=Failed).
tracking_job_status() {
    local job="$1"
    _tracking_scalar "job_status for job '$job'" \
        "SELECT job_status FROM \`$ARCHIVE_DB\`.archiver_job WHERE job_name = '$job';"
}

# archiver_job.last_processed_root_pk_id, or $TRACKING_NULL when it is SQL NULL.
#
# The column is a nullable VARCHAR(255) (resume.go:157), so "non-zero" is the wrong
# test for "a checkpoint exists" -- compare against $TRACKING_NULL instead.
tracking_checkpoint() {
    local job="$1"
    _tracking_scalar "checkpoint for job '$job'" \
        "SELECT IFNULL(last_processed_root_pk_id, '$TRACKING_NULL') FROM \`$ARCHIVE_DB\`.archiver_job WHERE job_name = '$job';"
}

# Rows in archiver_job_log_<id> at one specific log_status.
tracking_status_count() {
    local job_id="$1"
    local status="$2"
    _tracking_scalar "log_status=$status count in archiver_job_log_${job_id}" \
        "SELECT COUNT(*) FROM \`$ARCHIVE_DB\`.archiver_job_log_${job_id} WHERE log_status = $status;"
}

# Rows in archiver_job_log_<id> that recovery would replay: pending or copied.
#
# This is the single most load-bearing reading in the resume pair, and it means
# opposite things in each test: test 08 requires it to be ZERO after a graceful
# stop (the whole point -- a cooperative stop leaves nothing non-terminal), test 09
# requires it to be NON-zero after a crash.
tracking_nonterminal_count() {
    local job_id="$1"
    _tracking_scalar "non-terminal count in archiver_job_log_${job_id}" \
        "SELECT COUNT(*) FROM \`$ARCHIVE_DB\`.archiver_job_log_${job_id} WHERE log_status IN (0, 1);"
}

# A one-line breakdown for the test log. Diagnostic only -- never an assertion.
tracking_dump() {
    local job="$1"
    local job_id="$2"
    local st ckpt pending copied completed
    st=$(tracking_job_status "$job") || return 1
    ckpt=$(tracking_checkpoint "$job") || return 1
    pending=$(tracking_status_count "$job_id" 0) || return 1
    copied=$(tracking_status_count "$job_id" 1) || return 1
    completed=$(tracking_status_count "$job_id" 2) || return 1
    echo "  tracking: job_status=$st checkpoint=$ckpt log(pending=$pending copied=$copied completed=$completed)"
}

# Prove the tracking reads report a failure instead of returning a value.
#
# Same reachable-argument trick as selftest_get_row_count_fails_loud: port 59999 on
# loopback is refused immediately (~0.3s, no DNS, no server state touched). Not a
# mutation that has to be applied and reverted -- this runs on every suite and
# keeps holding.
#
# It matters more here than anywhere else in the harness. Every other assertion
# fails when a count is wrong; these assertions PASS when a count is zero, and a
# broken query returning "0" would report "no non-terminal rows left" for a resume
# that never happened.
selftest_tracking_fails_loud() {
    local saved_port="$ARCHIVE_PORT"
    local out rc

    ARCHIVE_PORT=59999
    out=$(tracking_nonterminal_count 1 2>/dev/null)
    rc=$?
    ARCHIVE_PORT="$saved_port"

    if [[ $rc -eq 0 ]]; then
        log_error "HARNESS SELF-CHECK FAILED: tracking_nonterminal_count exited 0 against an unreachable port."
        log_error "  It returned: '${out}'"
        log_error "  A failed query is being reported as a tracking count. Every resume"
        log_error "  assertion in this suite would then pass without connecting. Refusing to run."
        return 1
    fi
    log_verbose "self-check: tracking reads fail loud on an unreachable port (exit $rc)"
    return 0
}
