# Test13's live replication witness. The subshell scopes traps and private
# helpers so an interrupted test cannot poison the next engine invocation.
# No operator data is used: this runs only on the seeded E2E payment fixture.
run_replication_hold_job() (
    local config_file="$1" log_file="$2"
    local full_config_path="$TESTS_DIR/configs/$config_file"
    local process_out="${log_file%.log}.replication.stdout"
    local process_err="${log_file%.log}.replication.stderr"
    local pid="" restore_applier=false output_copied=false
    local replica_host="${TEST_REPLICA_HOST:-127.0.0.1}"
    local replica_port="${TEST_REPLICA_PORT:-3308}"

    replica_query() {
        MYSQL_QUERY_HOST="$replica_host" MYSQL_QUERY_USER=root \
            "$SCRIPT_DIR/mysql-query.sh" "$replica_port" "$1"
    }
    source_query() {
        MYSQL_QUERY_HOST="$SOURCE_HOST" MYSQL_QUERY_USER="$SOURCE_USER" \
            "$SCRIPT_DIR/mysql-query.sh" "$SOURCE_PORT" "$1"
    }
    # Reject a missing value instead of turning a failed/empty read into zero.
    scalar_value() {
        awk 'NR == 2 { print; seen=1 } END { if (!seen || NR != 2) exit 1 }'
    }
    replica_field() {
        local status
        status=$(replica_query "SHOW REPLICA STATUS") || return 1
        printf '%s\n' "$status" | awk -F '\t' -v field="$1" '
            NR == 1 { for (i=1;i<=NF;i++) if ($i==field) col=i }
            NR == 2 && col { print $col; seen=1 }
            END { if (!seen || NR != 2) exit 1 }'
    }
    assert_replica_ready() {
        local io sql delay gtid caught
        io=$(replica_field Replica_IO_Running) || return 1
        sql=$(replica_field Replica_SQL_Running) || return 1
        delay=$(replica_field SQL_Delay) || return 1
        if [[ "$io" != Yes || "$sql" != Yes || "$delay" != 0 ]]; then
            log_error "PURGE_REPLICA_NOT_READY: io=$io sql=$sql delay=$delay"
            return 1
        fi
        gtid=$(source_query "SELECT @@GLOBAL.gtid_executed" | scalar_value) || return 1
        # A GTID set contains only UUIDs, transaction ranges and separators.
        if [[ -z "$gtid" || "$gtid" == *[!a-fA-F0-9:,-]* ]]; then
            log_error "PURGE_REPLICA_NOT_READY: missing or invalid source GTID set"
            return 1
        fi
        caught=$(replica_query "SELECT WAIT_FOR_EXECUTED_GTID_SET('$gtid', 30)" | scalar_value) || return 1
        if [[ "$caught" != 0 ]]; then
            log_error "PURGE_REPLICA_NOT_READY: replica did not catch up"
            return 1
        fi
    }
    copy_process_output() {
        if [[ "$output_copied" == false ]]; then
            [[ ! -f "$process_out" ]] || cat "$process_out" >> "$log_file" || return 1
            [[ ! -f "$process_err" ]] || cat "$process_err" >> "$log_file" || return 1
            output_copied=true
        fi
    }
    cleanup_replication_test() {
        local rc=$? deadline
        trap - EXIT INT TERM
        if [[ -n "$pid" ]]; then
            kill -TERM "$pid" 2>/dev/null || true
            deadline=$((SECONDS + 5))
            while kill -0 "$pid" 2>/dev/null && [[ $SECONDS -lt $deadline ]]; do sleep 0.1; done
            kill -KILL "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
        if [[ "$restore_applier" == true ]]; then
            if ! replica_query "START REPLICA SQL_THREAD"; then rc=1; fi
            if ! assert_replica_ready; then rc=1; fi
        fi
        if ! copy_process_output; then rc=1; fi
        exit "$rc"
    }
    trap cleanup_replication_test EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    set -o pipefail
    : > "$process_out" || return 1
    : > "$process_err" || return 1

    # This helper deliberately describes one fixture, not arbitrary user SQL.
    if [[ "$config_file" != test13_purge_replication.yaml ]]; then
        log_error "replication_hold: unsupported fixture $config_file"
        return 1
    fi
    ensure_goarchive_bin || return 1
    local job_name selected before_count held_count held_dest after_count deadline child_rc
    job_name=$(extract_job_name "$full_config_path") || return 1
    "$GOARCHIVE_BIN" validate --config "$full_config_path" --force-triggers >> "$log_file" 2>&1 || return 1
    "$GOARCHIVE_BIN" dry-run --job "$job_name" --config "$full_config_path" >> "$log_file" 2>&1 || return 1
    selected=$(source_query "SELECT COUNT(*) FROM \`$SOURCE_DB\`.payment WHERE payment_id <= 2000" | scalar_value) || return 1
    before_count=$(get_row_count "$SOURCE_HOST" "$SOURCE_PORT" "$SOURCE_USER" "$SOURCE_DB" payment) || return 1
    if [[ "$selected" != 1999 ]]; then
        log_error "PURGE_REPLICATION_FIXTURE: selected=$selected, want 1999"
        return 1
    fi
    assert_replica_ready || return 1
    restore_applier=true
    replica_query "STOP REPLICA SQL_THREAD" || return 1
    if [[ "$(replica_field Replica_SQL_Running)" != No ]]; then
        log_error "PURGE_REPLICATION_FIXTURE: applier did not stop"
        return 1
    fi

    "$GOARCHIVE_BIN" purge --job "$job_name" --config "$full_config_path" --force-triggers > "$process_out" 2> "$process_err" &
    pid=$!
    deadline=$((SECONDS + 60))
    while ! grep -Fq 'replication hold:' "$process_out" "$process_err"; do
        if ! kill -0 "$pid" 2>/dev/null || [[ $SECONDS -ge $deadline ]]; then
            log_error "PURGE_REPLICATION_HOLD_MISSING: process exited or no hold was observed"
            return 1
        fi
        sleep 0.1
    done
    held_count=$(get_row_count "$SOURCE_HOST" "$SOURCE_PORT" "$SOURCE_USER" "$SOURCE_DB" payment) || return 1
    held_dest=$(get_row_count "$ARCHIVE_HOST" "$ARCHIVE_PORT" "$ARCHIVE_USER" "$ARCHIVE_DB" payment) || return 1
    if [[ "$held_dest" != 0 || "$held_count" != "$before_count" ]] || ! kill -0 "$pid" 2>/dev/null; then
        log_error "PURGE_REPLICATION_HOLD_LOST: source changed or process exited while applier stopped"
        return 1
    fi
    echo "replication witness: observed hold; source unchanged; pid=$pid" >> "$log_file"
    replica_query "START REPLICA SQL_THREAD" || return 1
    deadline=$((SECONDS + 60))
    while kill -0 "$pid" 2>/dev/null; do
        if [[ $SECONDS -ge $deadline ]]; then
            log_error "PURGE_REPLICATION_RESUME_TIMEOUT: same process did not finish"
            return 1
        fi
        sleep 0.1
    done
    wait "$pid"
    child_rc=$?
    pid=""
    copy_process_output || return 1
    if [[ "$child_rc" != 0 ]] || ! grep -Fq 'replication gate passed:' "$process_out" "$process_err"; then
        log_error "PURGE_REPLICATION_RESUME_FAILED: exit=$child_rc or recovery log missing"
        return 1
    fi
    after_count=$(source_query "SELECT COUNT(*) FROM \`$SOURCE_DB\`.payment WHERE payment_id <= 2000" | scalar_value) || return 1
    if [[ "$after_count" != 0 ]]; then
        log_error "PURGE_REPLICATION_INCOMPLETE: selected rows remain after resume"
        return 1
    fi
    assert_replica_ready || return 1
    restore_applier=false
    echo "replication witness: same process completed; selected rows=0; replica restored" >> "$log_file"
    return 0
)
