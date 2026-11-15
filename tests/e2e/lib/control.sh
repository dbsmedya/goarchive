# E2E harness — Test20's control socket procedure (TEL-1 C7).
#
# One archive job of the seeded payment slice, run three times through the real binary and driven
# over its runtime control socket with `goarchive control` and, as a script without the binary would,
# with `nc -U` and `socat`:
#
#   run 1  start; dry-run's printed path is the log's and the status's; status, status-json; pause
#          and resume; -s and -g; a second start of the job from a second config file with the same
#          job name and socket_dir exits 4; the doc's nc -U and socat lines; stop -> exit 3, the
#          socket file gone
#   run 2  the rerun, killed with SIGKILL: `control` exits 5 with the stale-socket message and the
#          socket file is left
#   run 3  the next run replaces the stale socket, completes (exit 0), and removes it; `control`
#          against the finished run's path exits 5
#
# Its output goes to the file the engine names, which holds that one run: the standard assertions
# (verification, pacing floor, end state) read it. No operator data is used.
#
# The socket directory is a short private directory made here, because a socket path is limited to
# 103 bytes on macOS, and removed on every path out. Both config files are copies of the rendered
# config with that directory in place of its __SOCKET_DIR__ placeholder, mode 0600 (they hold the
# estate's password).
#
# Needs jq (the harness's own), `nc` (the OpenBSD one) and `socat` on the PATH. A missing tool fails
# the test: a skipped check here would assert less than the test declares.

# Run command <command...> in the background for at most <seconds>, its output in <outfile>.
# BOUNDED_RC is the command's exit status, or 124 when it was killed at the limit.
bounded_command() {
    local secs="$1" out="$2" cpid ticks=0
    shift 2
    "$@" > "$out" 2>&1 &
    cpid=$!
    while kill -0 "$cpid" 2>/dev/null; do
        if [[ $ticks -ge $((secs * 10)) ]]; then
            kill -KILL "$cpid" 2>/dev/null
            wait "$cpid" 2>/dev/null
            BOUNDED_RC=124
            return 0
        fi
        sleep 0.1
        ticks=$((ticks + 1))
    done
    wait "$cpid"
    BOUNDED_RC=$?
}

# run_control_socket_job <config_file> <log_file> <final_log>
# The subshell scopes the traps and the helpers, so an interrupted test cannot poison the next one.
run_control_socket_job() (
    local config_file="$1" log_file="$2" final_log="$3"
    local base="$TESTS_DIR/configs/$config_file"
    local root="" pid="" job sock sock_dir tool run_rc
    local CTL_OUT="" CTL_ERR="" CTL_RC=0 BOUNDED_RC=0

    cleanup_control_test() {
        local rc=$?
        trap - EXIT INT TERM
        if [[ -n "$pid" ]]; then
            kill -KILL "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
        [[ -z "$root" ]] || rm -rf "$root"
        exit "$rc"
    }
    trap cleanup_control_test EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    set -o pipefail

    witness() { echo "control witness: $*" >> "$log_file"; }
    keep_output() { { echo "---- $1 ----"; cat "$2"; } >> "$log_file"; }

    # ctl runs the client against the job's socket; CTL_OUT, CTL_ERR and CTL_RC are its result.
    ctl() {
        CTL_OUT=$("$GOARCHIVE_BIN" control "$sock" "$@" 2> "$root/ctl.err")
        CTL_RC=$?
        CTL_ERR=$(cat "$root/ctl.err")
    }
    # expect <name> <exit> <stdout contains> <stderr contains> <client args...>; an empty string
    # asserts nothing about that stream, except that a failing call prints nothing on stdout.
    expect() {
        local name="$1" want_rc="$2" want_out="$3" want_err="$4"
        shift 4
        ctl "$@"
        if [[ "$CTL_RC" != "$want_rc" ]]; then
            log_error "CONTROL_${name}: exit $CTL_RC, want $want_rc (stdout '$CTL_OUT', stderr '$CTL_ERR')"
            return 1
        fi
        if [[ -n "$want_out" && "$CTL_OUT" != *"$want_out"* ]]; then
            log_error "CONTROL_${name}: stdout '$CTL_OUT' does not contain '$want_out'"
            return 1
        fi
        if [[ -n "$want_err" && "$CTL_ERR" != *"$want_err"* ]]; then
            log_error "CONTROL_${name}: stderr '$CTL_ERR' does not contain '$want_err'"
            return 1
        fi
        if [[ "$want_rc" != 0 && -n "$CTL_OUT" ]]; then
            log_error "CONTROL_${name}: a failing call printed on stdout: '$CTL_OUT'"
            return 1
        fi
        witness "$name: exit $CTL_RC"
    }
    # wait_for <seconds> <what> <command...> polls every 0.2 s until the command succeeds.
    wait_for() {
        local secs="$1" what="$2" deadline
        shift 2
        deadline=$((SECONDS + secs))
        until "$@"; do
            if [[ $SECONDS -ge $deadline ]]; then
                log_error "CONTROL_TIMEOUT: $what did not happen within ${secs}s"
                return 1
            fi
            sleep 0.2
        done
    }
    answers() { "$GOARCHIVE_BIN" control "$sock" status-json > /dev/null 2>&1; }
    state_is() {
        local json
        json=$("$GOARCHIVE_BIN" control "$sock" status-json 2> /dev/null) || return 1
        [[ "$(jq -r '.state' <<< "$json")" == "$1" ]]
    }
    state_is_not() { ! state_is "$1"; }
    start_run() { # start_run <config> <output file>: the run's pid is $pid
        "$GOARCHIVE_BIN" archive --job "$job" --config "$1" --force-triggers > "$2" 2>&1 &
        pid=$!
    }
    wait_exit() { # wait_exit <seconds>: the run's exit status is $run_rc
        local deadline=$((SECONDS + $1))
        while kill -0 "$pid" 2>/dev/null; do
            if [[ $SECONDS -ge $deadline ]]; then
                log_error "CONTROL_TIMEOUT: the run did not end within ${1}s"
                return 1
            fi
            sleep 0.2
        done
        wait "$pid"
        run_rc=$?
        pid=""
    }

    for tool in jq nc socat; do
        if ! command -v "$tool" > /dev/null 2>&1; then
            log_error "CONTROL_PREREQUISITE: $tool is not on the PATH; test 20 needs jq, nc (OpenBSD) and socat"
            return 1
        fi
    done
    ensure_goarchive_bin || return 1
    if ! grep -q '__SOCKET_DIR__' "$base"; then
        log_error "CONTROL_FIXTURE: $config_file has no __SOCKET_DIR__ placeholder"
        return 1
    fi
    job=$(extract_job_name "$base") || return 1
    umask 077
    root=$(mktemp -d /tmp/ga3e2e.XXXXXX) || return 1
    sock_dir="$root/run"
    sock="$sock_dir/$job.sock"
    sed "s|__SOCKET_DIR__|$sock_dir|g" "$base" > "$root/a.yaml" || return 1
    cp "$root/a.yaml" "$root/b.yaml" || return 1

    # ---- the printed path -------------------------------------------------------------------
    "$GOARCHIVE_BIN" validate --config "$root/a.yaml" --force-triggers >> "$log_file" 2>&1 || return 1
    local dry printed
    dry=$("$GOARCHIVE_BIN" dry-run --job "$job" --config "$root/a.yaml" 2>&1) || {
        echo "$dry" >> "$log_file"
        log_error "CONTROL_DRY_RUN: dry-run failed"
        return 1
    }
    echo "$dry" >> "$log_file"
    printed=$(printf '%s\n' "$dry" | sed -n 's/^ *control socket: //p' | head -1)
    if [[ "$printed" != "$sock" ]]; then
        log_error "CONTROL_PRINTED_PATH: dry-run printed '$printed', want $sock"
        return 1
    fi
    witness "dry-run printed the socket path $printed"

    # ---- run 1: driven over the socket, then stopped --------------------------------------
    start_run "$root/a.yaml" "$root/r1.out"
    wait_for 30 "the socket answering" answers || { keep_output "run 1" "$root/r1.out"; return 1; }
    if ! grep -qF "Control socket: $sock" "$root/r1.out"; then
        log_error "CONTROL_LOG_PATH: the run's log has no 'Control socket: $sock' entry"
        keep_output "run 1" "$root/r1.out"
        return 1
    fi
    witness "the run's log carries the same path"

    expect status 0 "$job (archive)" "" status || return 1
    expect status_json 0 '"protocol":1' "" status-json || return 1
    local json
    json=$(cat <<< "$CTL_OUT")
    if [[ "$(jq -r '.socket' <<< "$json")" != "$sock" || "$(jq -r '.job' <<< "$json")" != "$job" || "$(jq -r '.pid' <<< "$json")" != "$pid" ]]; then
        log_error "CONTROL_STATUS_JSON: socket, job or pid is not this run's: $json"
        return 1
    fi

    expect pause 0 "paused" "" pause || return 1
    wait_for 30 "state paused" state_is paused || return 1
    expect resume 0 "resumed" "" resume || return 1
    wait_for 30 "the pause ending" state_is_not paused || return 1

    expect set 0 "batch_delete_size: 100 -> 50" "" -s batch_delete_size=50 || return 1
    expect get 0 "batch_delete_size=50" "" -g batch_delete_size || return 1
    if [[ "$CTL_OUT" != "batch_delete_size=50" ]]; then
        log_error "CONTROL_GET: the getter printed '$CTL_OUT', want exactly batch_delete_size=50"
        return 1
    fi
    expect ceiling 1 "" "Error: ceiling:" -s batch_delete_size=9999 || return 1

    # A second start of the job from a second config file, same job name and socket_dir.
    "$GOARCHIVE_BIN" archive --job "$job" --config "$root/b.yaml" --force-triggers > "$root/r2b.out" 2>&1
    run_rc=$?
    keep_output "second start" "$root/r2b.out"
    if [[ "$run_rc" != 4 ]] || ! grep -qF "the control socket is in use" "$root/r2b.out"; then
        log_error "CONTROL_SECOND_START: exit $run_rc, want 4 with 'the control socket is in use'"
        return 1
    fi
    witness "a second start of the job exited 4"
    answers || { log_error "CONTROL_SECOND_START: the first run no longer answers"; return 1; }

    # The protocol without the binary: the doc's lines, as published.
    bounded_command 20 "$root/nc.out" sh -c 'printf "status-json\n" | nc -U "$1"' sh "$sock"
    if [[ "$BOUNDED_RC" != 0 ]] || [[ "$(head -1 "$root/nc.out")" != OK ]] || ! grep -qF '"protocol":1' "$root/nc.out"; then
        log_error "CONTROL_NC: exit $BOUNDED_RC, reply: $(cat "$root/nc.out")"
        return 1
    fi
    witness "nc -U: status-json answered"
    bounded_command 20 "$root/socat.out" sh -c 'printf "batch_delete_size=100\n" | socat - UNIX-CONNECT:"$1"' sh "$sock"
    if [[ "$BOUNDED_RC" != 0 ]] || [[ "$(head -1 "$root/socat.out")" != OK ]] || ! grep -qF 'batch_delete_size: 50 -> 100' "$root/socat.out"; then
        log_error "CONTROL_SOCAT: exit $BOUNDED_RC, reply: $(cat "$root/socat.out")"
        return 1
    fi
    witness "socat: a setter answered"

    expect stop 0 "stop requested" "" stop || return 1
    wait_exit 60 || { keep_output "run 1" "$root/r1.out"; return 1; }
    keep_output "run 1" "$root/r1.out"
    if [[ "$run_rc" != 3 ]] || ! grep -qF 'Run stopped' "$root/r1.out" || ! grep -qF '"runtime_changes"' "$root/r1.out"; then
        log_error "CONTROL_STOP: exit $run_rc (want 3), or the stopped closing entry with its runtime changes is missing"
        return 1
    fi
    if [[ -e "$sock" ]]; then
        log_error "CONTROL_STOP: the socket file is left after a stopped run"
        return 1
    fi
    witness "stop: exit 3, the closing entry carries the runtime changes, the socket is gone"

    # ---- run 2: the rerun, killed ------------------------------------------------------------
    start_run "$root/a.yaml" "$root/r2.out"
    wait_for 30 "the rerun's socket answering" answers || { keep_output "run 2" "$root/r2.out"; return 1; }
    kill -KILL "$pid"
    wait "$pid" 2> /dev/null
    run_rc=$?
    pid=""
    keep_output "run 2 (killed)" "$root/r2.out"
    if [[ "$run_rc" != 137 ]]; then
        log_error "CONTROL_KILL: the killed run's status is $run_rc, want 137"
        return 1
    fi
    if [[ ! -S "$sock" ]]; then
        log_error "CONTROL_KILL: no socket file is left after SIGKILL"
        return 1
    fi
    expect stale 5 "" "stale" status || return 1
    witness "kill -9: the socket file is left and control says it is stale"

    # ---- run 3: replaces the stale socket and completes ---------------------------------------
    start_run "$root/a.yaml" "$final_log"
    wait_for 30 "the next run's socket answering" answers || { cat "$final_log" >> "$log_file"; return 1; }
    expect replaced 0 "$job (archive)" "" status || return 1
    witness "the next run replaced the stale socket"
    wait_exit 120 || { cat "$final_log" >> "$log_file"; return 1; }
    keep_output "run 3 (final)" "$final_log"
    if [[ "$run_rc" != 0 ]]; then
        log_error "CONTROL_FINAL: the final run exited $run_rc, want 0"
        return 1
    fi
    if [[ -e "$sock" ]]; then
        log_error "CONTROL_FINAL: the socket file is left after a completed run"
        return 1
    fi
    expect finished 5 "" "no control socket at $sock" status || return 1
    witness "the final run completed; control against its path exits 5"
    return 0
)
