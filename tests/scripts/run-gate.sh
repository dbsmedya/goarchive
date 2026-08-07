#!/usr/bin/env bash
#
# THE FULL VERIFICATION GATE. `make gate` is a one-line wrapper around this.
#
# Two jobs: run every stage in the only correct order, and end with a summary a
# human can actually read.
#
# WHY THE SUMMARY EXISTS
#
# The gate emits thousands of lines -- mysqlsh progress spinners, per-test
# output, schema dumps. The verdict was reliable (make aborts on the first
# failure) but the EVIDENCE was scattered across the whole run, so a reader had
# to know which greps to write. An independent verifier reported six stages out
# of eight in good faith, simply because the other two had scrolled past.
#
# WHY THIS IS A SCRIPT AND NOT MAKE RECIPE LINES
#
# The obvious way to collect headlines is to `tee` each stage inside the
# Makefile and grep afterwards. That BREAKS FAILURE DETECTION: `cmd | tee`
# returns tee's status, so a failing stage exits 0 and the gate reports green on
# red. make's default shell gives no reliable `pipefail`. Here the shebang is
# bash, so the exit code is taken from PIPESTATUS[0] -- the command's own status,
# never tee's -- and that is checked explicitly for every stage.
#
# Follows the pattern already used by e2e-tests-must-run-after-setup ->
# require-e2e-seed.sh: the Makefile names the target, a script owns the logic.
#
# Requires credentials:  set -a; source tests/.env; set +a

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_DIR="$PROJECT_ROOT/tests/results/gate"

cd "$PROJECT_ROOT" || exit 1
mkdir -p "$LOG_DIR"

# name|status|headline, one per completed stage. A plain indexed array: macOS
# ships bash 3.2, which has no `declare -A`.
SUMMARY=()

# ORDER IS LOAD-BEARING. Do not rearrange:
#   - static and unit first: no database, and they fail in seconds.
#   - integration BEFORE e2e, and with --setup (stale heartbeat state otherwise
#     fabricates failures).
#   - characterization before e2e too: it is behind the integration build tag
#     and wants the estate integration just seeded.
#   - e2e LAST, because it opens with test-reset and destroys the estate the two
#     stages above depend on.

# Pull the one line worth showing out of a stage's log. A stage with nothing
# quotable reports "ok" -- the exit code already carried the verdict.
stage_headline() {
    local name="$1" log="$2" p f

    case "$name" in
        estate)
            grep -m1 -oE 'test estate reachable on .*' "$log" ;;
        lint)
            grep -m1 -oE '[0-9]+ issues' "$log" ;;
        consumer-policy|deadcode)
            # Strip the tool's own "<name>: " prefix -- the column already says it.
            grep -m1 -oE "$name: .*" "$log" | sed "s/^$name: //" ;;
        integration)
            grep -m1 -oE 'PASS=[0-9]+ FAIL=[0-9]+ SKIP=[0-9]+' "$log" ;;
        characterization)
            grep -m1 -oE 'baseline: OK \(.*\)' "$log" | sed 's/^baseline: //' ;;
        e2e|e2e-examples)
            p=$(grep -oE 'Passed: [0-9]+' "$log" | tail -1)
            f=$(grep -oE 'Failed: [0-9]+' "$log" | tail -1)
            [[ -n "$p" || -n "$f" ]] && echo "$p  $f" ;;
        *)
            ;;
    esac
}

run_stage() {
    local name="$1"; shift
    local log="$LOG_DIR/${name}.log"
    local rc headline

    printf '\n=== %s ===\n' "$name"

    # Stream to the terminal AND capture, without the pipe hiding the result.
    # PIPESTATUS[0] is the COMMAND's exit status; tee's is PIPESTATUS[1] and is
    # deliberately ignored.
    #
    # Streams are merged here on purpose, and this is NOT the `2>&1` trap
    # CLAUDE.md warns about. That rule is about deciding pass/fail by grepping
    # merged output, where an error arriving on stderr makes a failure look like
    # an empty result. Pass/fail here comes from the exit code, taken directly
    # below. The log is only re-read afterwards to pull out a display headline,
    # and a stage that fails never reaches that read.
    "$@" 2>&1 | tee "$log"
    rc=${PIPESTATUS[0]}

    if [[ $rc -ne 0 ]]; then
        SUMMARY+=("$name|FAILED|exit $rc — see $log")
        print_summary
        echo ""
        echo "GATE FAILED at stage '$name' (exit $rc)."
        echo "Stages after this one did NOT run."
        echo "Full output: $log"
        exit "$rc"
    fi

    headline=$(stage_headline "$name" "$log")
    [[ -z "$headline" ]] && headline="ok"
    SUMMARY+=("$name|ok|$headline")
    return 0
}

print_summary() {
    local entry name status headline
    echo ""
    echo "================================================"
    echo "  GATE SUMMARY"
    echo "================================================"
    for entry in "${SUMMARY[@]}"; do
        name="${entry%%|*}"
        status="${entry#*|}"; status="${status%%|*}"
        headline="${entry##*|}"
        if [[ "$status" == "ok" ]]; then
            printf '  %-18s %s\n' "$name" "$headline"
        else
            printf '  %-18s ** %s **\n' "$name" "$headline"
        fi
    done
    echo "================================================"
}

if [[ -z "${MYSQL_ROOT_PASSWORD:-}" ]]; then
    echo "ERROR: MYSQL_ROOT_PASSWORD is not set." >&2
    echo "  Run: set -a; source tests/.env; set +a" >&2
    exit 1
fi

run_stage estate           bash tests/scripts/require-containers-up.sh
run_stage fmt-check        make fmt-check
run_stage vet              make vet
run_stage lint             make lint
run_stage consumer-policy  make consumer-policy
run_stage deadcode         make deadcode
run_stage unit             make test-unit
run_stage integration      bash tests/scripts/run-tests.sh --setup --integration-only
run_stage characterization bash tests/scripts/check-characterization-baseline.sh
run_stage e2e              make e2e
run_stage e2e-examples     make e2e-examples

print_summary
echo "  GATE COMPLETE - every stage above exited 0"
echo "================================================"
echo "  per-stage logs: $LOG_DIR"
