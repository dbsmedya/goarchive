#!/usr/bin/env bash
# Waits for a gate started by tests/scripts/gate-start.sh (RVG-3; tests/AGENTS.md → Running the
# gate), up to GATE_WAIT_SECONDS (540), so each wait fits one Bash tool call:
#
#   tests/scripts/gate-wait.sh <evidence-dir>
#
# exit 0  the run has finished (run.tsv outcome PASS or FAIL): prints the outcome and its
#         duration against the gate's 900 s ceiling;
# exit 3  still running, and its gate process is alive: prints the time elapsed; call again;
# exit 4  run.tsv says RUNNING but no gate process is alive (killed untrappably): a hung run;
# exit 2  no such run directory of this checkout, or a bad argument.
# The gate's process is the PID in its run_id (<UTC>-<pid>[-<n>]) whose command is run-gate.sh.
# Keep this script compatible with the system Bash 3.2 on macOS.
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CEILING=900
[[ $# -eq 1 ]] || { echo "usage: gate-wait.sh <evidence-dir>" >&2; exit 2; }
dir=$1
if [[ ! -f $dir/run.tsv || "$(cd "$dir/.." 2>/dev/null && pwd)" != "$PROJECT_ROOT/tests/results/gate" ]]; then
    echo "gate-wait.sh: $dir is not a gate run of this checkout" >&2; exit 2
fi
wait_seconds=${GATE_WAIT_SECONDS:-540}

# epoch prints the seconds since 1970 of a UTC time like 2026-10-17T14:00:00Z (GNU or BSD date).
epoch() { date -u -d "$1" +%s 2>/dev/null || date -u -j -f '%Y-%m-%dT%H:%M:%SZ' "$1" +%s; }

deadline=$(($(date +%s) + wait_seconds))
while :; do
    IFS=$'\t' read -r run_id _ _ started ended outcome _ < <(awk 'NR==2' "$dir/run.tsv")
    case "$outcome" in
        PASS|FAIL)
            duration=$(($(epoch "$ended") - $(epoch "$started")))
            echo "outcome: $outcome"
            if [[ $duration -gt $CEILING ]]; then
                echo "duration: $duration s (ceiling $CEILING s): over the ceiling"
            else
                echo "duration: $duration s (ceiling $CEILING s)"
            fi
            exit 0 ;;
    esac
    pid=$(echo "$run_id" | cut -d- -f2)
    if ! [[ $pid =~ ^[0-9]+$ ]] || ! ps -p "$pid" -o command= 2>/dev/null | grep -q 'run-gate\.sh'; then
        echo "gate-wait.sh: run.tsv says ${outcome:-nothing}, and no gate process $pid is alive: a hung run" >&2
        echo "outcome: ${outcome:-none}, no gate process"
        exit 4
    fi
    if [[ $(date +%s) -ge $deadline ]]; then
        echo "still running: $dir"
        echo "elapsed: $(($(date +%s) - $(epoch "$started"))) s"
        exit 3
    fi
    sleep 1
done
