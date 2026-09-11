#!/usr/bin/env bash
# The full verification gate. Each invocation owns immutable evidence.
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
GATE_ROOT="$PROJECT_ROOT/tests/results/gate"
STAGES=(estate fmt-check vet lint consumer-policy deadcode unit integration characterization e2e e2e-examples)
STATUS=() COMMAND_EXIT=() CAPTURE_EXIT=() LOG_PATH=()
cd "$PROJECT_ROOT" || exit 1
mkdir -p "$GATE_ROOT" || { echo "ERROR: cannot create gate evidence root" >&2; exit 1; }
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"; RUN_DIR="$GATE_ROOT/$RUN_ID"; n=0
while ! mkdir "$RUN_DIR" 2>/dev/null; do n=$((n + 1)); if [[ "$n" -gt 100 ]]; then echo "ERROR: cannot allocate gate evidence directory" >&2; exit 1; fi; RUN_DIR="$GATE_ROOT/$RUN_ID-$n"; done
echo "Gate evidence directory: $RUN_DIR"
for ((i=0; i<${#STAGES[@]}; i++)); do STATUS[$i]="NOT RUN"; COMMAND_EXIT[$i]="-"; CAPTURE_EXIT[$i]="-"; LOG_PATH[$i]="-"; done
publish_gate_file() { mv "$1" "$2"; }
write_atomic() { local d="$1" v="$2" t="$RUN_DIR/.${1##*/}.$$.$RANDOM"; printf '%s' "$v" > "$t" || return 1; publish_gate_file "$t" "$d"; }
persist_summary() {
    local tsv="$RUN_DIR/.summary.tsv.$$.$RANDOM" txt="$RUN_DIR/.summary.txt.$$.$RANDOM" j
    { printf 'stage\tstatus\tcommand_exit\tcapture_exit\tlog\theadline\n'; for ((j=0; j<${#STAGES[@]}; j++)); do printf '%s\t%s\t%s\t%s\t%s\t%s\n' "${STAGES[j]}" "${STATUS[j]}" "${COMMAND_EXIT[j]}" "${CAPTURE_EXIT[j]}" "${LOG_PATH[j]}" "${STATUS[j]}"; done; } > "$tsv" || return 1
    { printf 'GATE SUMMARY (%s)\n' "$RUN_ID"; for ((j=0; j<${#STAGES[@]}; j++)); do printf '  %-18s %s\n' "${STAGES[j]}" "${STATUS[j]}"; done; } > "$txt" || return 1
    publish_gate_file "$tsv" "$RUN_DIR/summary.tsv" || return 1
    publish_gate_file "$txt" "$RUN_DIR/summary.txt"
}
STARTED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
persist_run() { local outcome="$1" rc="$2" sha dirty tmp="$RUN_DIR/.run.tsv.$$.$RANDOM"; sha=$(git rev-parse HEAD 2>/dev/null || echo unknown); if git diff --quiet 2>/dev/null && git diff --cached --quiet 2>/dev/null; then dirty=clean; else dirty=dirty; fi; { printf 'run_id\tsource_sha\tdirty\tstarted_utc\tended_utc\toutcome\tcommand_exit\trecording_exit\n'; printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t0\n' "$RUN_ID" "$sha" "$dirty" "$STARTED" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$outcome" "$rc"; } > "$tmp" || return 1; publish_gate_file "$tmp" "$RUN_DIR/run.tsv"; }
recording_failure() { echo "ERROR: failed to publish gate evidence in $RUN_DIR" >&2; exit 1; }
persist_summary || recording_failure; persist_run RUNNING - || recording_failure
CURRENT_STAGE=-1
on_signal() { if [[ "$CURRENT_STAGE" -ge 0 ]]; then STATUS[$CURRENT_STAGE]=FAIL; COMMAND_EXIT[$CURRENT_STAGE]=143; persist_summary || true; persist_run FAIL 143 || true; fi; exit 143; }
trap on_signal INT TERM
stage_headline() { :; }
run_stage() {
    local name="$1" log rc cap j ps; shift
    for ((j=0; j<${#STAGES[@]}; j++)); do [[ "${STAGES[j]}" == "$name" ]] && break; done
    CURRENT_STAGE=$j; log="$RUN_DIR/$name.log"; STATUS[$j]="RUNNING"; LOG_PATH[$j]="$log"; persist_summary || recording_failure
    printf '\n=== %s ===\n' "$name"; "$@" 2>&1 | tee "$log"; ps=("${PIPESTATUS[@]}"); rc="${ps[0]}"; cap="${ps[1]}"; COMMAND_EXIT[$j]="$rc"; CAPTURE_EXIT[$j]="$cap"
    if [[ "$rc" -ne 0 || "$cap" -ne 0 ]]; then STATUS[$j]="FAIL"; persist_summary || recording_failure; if [[ "$rc" -ne 0 ]]; then return "$rc"; fi; return "$cap"; fi
    STATUS[$j]="PASS"; CURRENT_STAGE=-1; persist_summary || recording_failure
}
print_summary() { local j; echo; echo "================================================"; echo "  GATE SUMMARY"; echo "================================================"; for ((j=0; j<${#STAGES[@]}; j++)); do printf '  %-18s %s\n' "${STAGES[j]}" "${STATUS[j]}"; done; echo "================================================"; }
fail_gate() { local rc="$1"; print_summary; persist_run FAIL "$rc" || recording_failure; echo "GATE FAILED" >&2; exit "$rc"; }
if [[ -z "${MYSQL_ROOT_PASSWORD:-}" ]]; then echo "ERROR: MYSQL_ROOT_PASSWORD is not set." >&2; persist_run FAIL 1 || recording_failure; exit 1; fi
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
persist_run PASS 0 || recording_failure
write_atomic "$RUN_DIR/complete" $'GATE COMPLETE - every stage above exited 0\n'"run_id=$RUN_ID"$'\n'"sha=$(git rev-parse HEAD 2>/dev/null || echo unknown)"$'\n' || recording_failure
print_summary; echo "  GATE COMPLETE - every stage above exited 0"; echo "================================================"; echo "  per-stage logs: $RUN_DIR"
