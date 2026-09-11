#!/usr/bin/env bash
# The full verification gate. Every invocation owns its evidence directory.
# Keep this script compatible with the system Bash 3.2 on macOS.
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
GATE_ROOT="$PROJECT_ROOT/tests/results/gate"
STAGES=(estate fmt-check vet lint consumer-policy deadcode unit integration characterization e2e e2e-examples)
STATUS=() COMMAND_EXIT=() CAPTURE_EXIT=() LOG_PATH=() HEADLINE=()
cd "$PROJECT_ROOT" || exit 1
mkdir -p "$GATE_ROOT" || { echo "ERROR: gate recording failed: cannot create evidence root" >&2; exit 1; }
RUN_BASE="$(date -u +%Y%m%dT%H%M%SZ)-$$"
RUN_ID="$RUN_BASE"
RUN_DIR="$GATE_ROOT/$RUN_ID"
collision=0
while ! mkdir "$RUN_DIR" 2>/dev/null; do
    # Only an existing path can be a collision; storage errors cannot be
    # repaired by trying an unlimited sequence of different names.
    if [[ ! -e "$RUN_DIR" || "$collision" -ge 100 ]]; then
        echo "ERROR: gate recording failed: cannot allocate $RUN_DIR" >&2
        exit 1
    fi
    collision=$((collision + 1))
    RUN_ID="$RUN_BASE-$collision"
    RUN_DIR="$GATE_ROOT/$RUN_ID"
done
echo "Gate evidence directory: $RUN_DIR"
for ((i=0; i<${#STAGES[@]}; i++)); do
    STATUS[$i]="NOT RUN"
    COMMAND_EXIT[$i]="-"
    CAPTURE_EXIT[$i]="-"
    LOG_PATH[$i]="-"
    HEADLINE[$i]="-"
done
STARTED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
ENDED="-"
SOURCE_SHA=$(git rev-parse HEAD 2>/dev/null || echo unknown)
DIRTY=unknown
if [[ "$SOURCE_SHA" != unknown ]]; then
    if git diff --quiet 2>/dev/null && git diff --cached --quiet 2>/dev/null; then DIRTY=clean; else DIRTY=dirty; fi
fi
CURRENT_STAGE=-1
RUN_COMMAND_EXIT="-"
RUN_RECORDING_EXIT=0
FINALIZED=false

# A single publication boundary for every metadata, summary and completion
# record. Never hide mv's failure; callers retain the exact recording status.
publish_gate_file() { mv "$1" "$2"; }

persist_summary() {
    local tsv="$RUN_DIR/.summary.tsv.$$.$RANDOM" txt="$RUN_DIR/.summary.txt.$$.$RANDOM" j
    {
        printf 'stage\tstatus\tcommand_exit\tcapture_exit\tlog\theadline\n' || return $?
        for ((j=0; j<${#STAGES[@]}; j++)); do
            printf '%s\t%s\t%s\t%s\t%s\t%s\n' "${STAGES[j]}" "${STATUS[j]}" "${COMMAND_EXIT[j]}" "${CAPTURE_EXIT[j]}" "${LOG_PATH[j]}" "${HEADLINE[j]}" || return $?
        done
    } > "$tsv" || return $?
    {
        printf 'GATE SUMMARY (%s)\n' "$RUN_ID" || return $?
        for ((j=0; j<${#STAGES[@]}; j++)); do
            printf '  %-18s %-7s %s\n' "${STAGES[j]}" "${STATUS[j]}" "${HEADLINE[j]}" || return $?
        done
    } > "$txt" || return $?
    publish_gate_file "$tsv" "$RUN_DIR/summary.tsv" || return $?
    publish_gate_file "$txt" "$RUN_DIR/summary.txt"
}

persist_run() {
    local outcome="$1" tmp="$RUN_DIR/.run.tsv.$$.$RANDOM"
    {
        printf 'run_id\tsource_sha\tdirty\tstarted_utc\tended_utc\toutcome\tcommand_exit\trecording_exit\n' || return $?
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$RUN_ID" "$SOURCE_SHA" "$DIRTY" "$STARTED" "$ENDED" "$outcome" "$RUN_COMMAND_EXIT" "$RUN_RECORDING_EXIT" || return $?
    } > "$tmp" || return $?
    publish_gate_file "$tmp" "$RUN_DIR/run.tsv"
}

failure_exit() {
    if [[ "$RUN_COMMAND_EXIT" != - && "$RUN_COMMAND_EXIT" -ne 0 ]]; then
        return "$RUN_COMMAND_EXIT"
    elif [[ "$RUN_RECORDING_EXIT" -ne 0 ]]; then
        return "$RUN_RECORDING_EXIT"
    fi
    return 1
}

recording_failure() {
    RUN_RECORDING_EXIT="$1"
    ENDED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "ERROR: gate recording failed (exit $RUN_RECORDING_EXIT) in $RUN_DIR" >&2
    rm -f "$RUN_DIR/complete" || echo "ERROR: gate recording failed: cannot remove incomplete certification" >&2
    # A failed summary or completion publication can still leave a truthful
    # run verdict. If storage itself is broken, diagnose without promising one.
    persist_run FAIL || echo "ERROR: gate recording failed: cannot publish failure metadata" >&2
    FINALIZED=true
    failure_exit
    exit $?
}

print_summary() {
    local j
    echo
    echo "================================================"
    echo "  GATE SUMMARY"
    echo "================================================"
    for ((j=0; j<${#STAGES[@]}; j++)); do
        printf '  %-18s %-7s %s\n' "${STAGES[j]}" "${STATUS[j]}" "${HEADLINE[j]}"
    done
    echo "================================================"
}

fail_gate() {
    rm -f "$RUN_DIR/complete" || recording_failure $?
    ENDED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    persist_run FAIL || recording_failure $?
    print_summary
    echo "GATE FAILED" >&2
    FINALIZED=true
    failure_exit
    exit $?
}

on_signal() {
    local rc="$1"
    trap '' INT TERM
    RUN_COMMAND_EXIT="$rc"
    if [[ "$CURRENT_STAGE" -ge 0 ]]; then
        STATUS[$CURRENT_STAGE]=FAIL
        COMMAND_EXIT[$CURRENT_STAGE]="$rc"
        HEADLINE[$CURRENT_STAGE]="interrupted (exit $rc)"
    fi
    persist_summary || recording_failure $?
    fail_gate
}

on_exit() {
    local rc="$1"
    trap - EXIT
    [[ "$FINALIZED" == true ]] && return
    # Unexpected shell exits must not leave a successful run verdict. An
    # untrappable kill can still leave RUNNING, which never certifies success.
    [[ "$rc" -eq 0 ]] && rc=1
    RUN_COMMAND_EXIT="$rc"
    if [[ "$CURRENT_STAGE" -ge 0 && "${STATUS[CURRENT_STAGE]}" == RUNNING ]]; then
        STATUS[$CURRENT_STAGE]=FAIL
        COMMAND_EXIT[$CURRENT_STAGE]="$rc"
        HEADLINE[$CURRENT_STAGE]="interrupted (exit $rc)"
    fi
    persist_summary || recording_failure $?
    fail_gate
}
trap 'on_signal 130' INT
trap 'on_signal 143' TERM
trap 'on_exit $?' EXIT

stage_headline() {
    local name="$1" log="$2" p f
    case "$name" in
        estate) grep -m1 -oE 'test estate reachable on .*' "$log" ;;
        lint) grep -m1 -oE '[0-9]+ issues' "$log" ;;
        consumer-policy|deadcode) grep -m1 -oE "$name: .*" "$log" | sed "s/^$name: //" ;;
        integration) grep -m1 -oE 'PASS=[0-9]+ FAIL=[0-9]+ SKIP=[0-9]+' "$log" ;;
        characterization) grep -m1 -oE 'baseline: OK \(.*\)' "$log" | sed 's/^baseline: //' ;;
        e2e|e2e-examples) p=$(grep -oE 'Passed: [0-9]+' "$log" | tail -1); f=$(grep -oE 'Failed: [0-9]+' "$log" | tail -1); [[ -n "$p" || -n "$f" ]] && echo "$p  $f" ;;
    esac
}

run_stage() {
    local name="$1" log rc cap j headline
    local ps=()
    shift
    for ((j=0; j<${#STAGES[@]}; j++)); do [[ "${STAGES[j]}" == "$name" ]] && break; done
    CURRENT_STAGE=$j
    log="$RUN_DIR/$name.log"
    STATUS[$j]=RUNNING
    LOG_PATH[$j]="$log"
    persist_summary || recording_failure $?
    printf '\n=== %s ===\n' "$name"
    "$@" 2>&1 | tee "$log"
    ps=("${PIPESTATUS[@]}")
    rc="${ps[0]}"
    cap="${ps[1]}"
    COMMAND_EXIT[$j]="$rc"
    CAPTURE_EXIT[$j]="$cap"
    RUN_COMMAND_EXIT="$rc"
    RUN_RECORDING_EXIT="$cap"
    headline=$(stage_headline "$name" "$log") || headline=""
    # Keep a TSV field on one physical line even if a future stage changes its
    # headline format. printf treats arbitrary log text as data, never escapes.
    headline=${headline//$'\t'/ }
    headline=${headline//$'\n'/ }
    HEADLINE[$j]="${headline:--}"
    if [[ "$rc" -ne 0 || "$cap" -ne 0 ]]; then
        STATUS[$j]=FAIL
        echo "ERROR: $name failed (command exit $rc, capture exit $cap)" >&2
        persist_summary || recording_failure $?
        if [[ "$rc" -ne 0 ]]; then return "$rc"; fi
        return "$cap"
    fi
    STATUS[$j]=PASS
    CURRENT_STAGE=-1
    persist_summary || recording_failure $?
}

persist_summary || recording_failure $?
persist_run RUNNING || recording_failure $?
if [[ -z "${MYSQL_ROOT_PASSWORD:-}" ]]; then
    echo "ERROR: MYSQL_ROOT_PASSWORD is not set." >&2
    RUN_COMMAND_EXIT=1
    fail_gate
fi
run_stage estate bash tests/scripts/require-containers-up.sh || fail_gate $?
run_stage fmt-check make fmt-check || fail_gate $?
run_stage vet make vet || fail_gate $?
run_stage lint make lint || fail_gate $?
run_stage consumer-policy make consumer-policy || fail_gate $?
run_stage deadcode make deadcode || fail_gate $?
run_stage unit make test-unit || fail_gate $?
run_stage integration bash tests/scripts/run-tests.sh --setup --integration-only || fail_gate $?
run_stage characterization bash tests/scripts/check-characterization-baseline.sh || fail_gate $?
run_stage e2e make e2e || fail_gate $?
run_stage e2e-examples make e2e-examples || fail_gate $?

ENDED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
persist_run PASS || recording_failure $?
completion_tmp="$RUN_DIR/.complete.$$.$RANDOM"
printf 'GATE COMPLETE - every stage above exited 0\nrun_id=%s\nsha=%s\n' "$RUN_ID" "$SOURCE_SHA" > "$completion_tmp" || recording_failure $?
publish_gate_file "$completion_tmp" "$RUN_DIR/complete" || recording_failure $?
FINALIZED=true
print_summary
echo "  GATE COMPLETE - every stage above exited 0"
echo "================================================"
echo "  per-stage logs: $RUN_DIR"
