#!/usr/bin/env bash
# Waits for a gate started by tests/scripts/gate-start.sh (RVG-3; tests/AGENTS.md → Running the
# gate), up to GATE_WAIT_SECONDS (540), so each wait fits one Bash tool call, and never past the
# run's start + 1800 s, where the agent stops waiting:
#
#   tests/scripts/gate-wait.sh <evidence-dir>
#
# exit 0  the run has finished: run.tsv's outcome is PASS or FAIL and its gate process has exited,
#         after its last publication. Prints the outcome and the duration against the gate's
#         900 s ceiling;
# exit 3  still running (or publishing its outcome): prints the time elapsed; call again;
# exit 4  run.tsv says RUNNING but its gate process is gone (killed untrappably): a hung run;
# exit 2  no such run directory of this checkout, or a bad argument.
# The gate's process is the PID in its run_id (<UTC>-<pid>[-<n>]), running run-gate.sh, and
# started no later than the run did (GATE_PROCESS_SLACK, 5 s): a later process on a reused PID
# is another invocation.
# Keep this script compatible with the system Bash 3.2 on macOS.
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CEILING=900 STOP=1800
[[ $# -eq 1 ]] || { echo "usage: gate-wait.sh <evidence-dir>" >&2; exit 2; }
dir=$1
if [[ ! -f $dir/run.tsv || "$(cd "$dir/.." 2>/dev/null && pwd)" != "$PROJECT_ROOT/tests/results/gate" ]]; then
    echo "gate-wait.sh: $dir is not a gate run of this checkout" >&2; exit 2
fi
wait_seconds=${GATE_WAIT_SECONDS:-540} slack=${GATE_PROCESS_SLACK:-5}

# epoch prints the seconds since 1970 of a UTC time like 2026-10-17T14:00:00Z (GNU or BSD date).
epoch() { date -u -d "$1" +%s 2>/dev/null || date -u -j -f '%Y-%m-%dT%H:%M:%SZ' "$1" +%s; }

# seconds converts ps's etime, [[dd-]hh:]mm:ss, to seconds.
seconds() {
    local e=${1// /} d=0 h=0 m s a b c
    if [[ $e == *-* ]]; then d=${e%%-*} e=${e#*-}; fi
    IFS=: read -r a b c <<< "$e"
    if [[ -n $c ]]; then h=$a m=$b s=$c; else m=$a s=$b; fi
    echo $((10#$d * 86400 + 10#$h * 3600 + 10#$m * 60 + 10#$s))
}

# alive reports whether PID $1 is this run's gate: run-gate.sh, started by $2 (epoch) + slack.
alive() {
    local pid=$1 start=$2 etime
    [[ $pid =~ ^[0-9]+$ ]] || return 1
    ps -p "$pid" -o command= 2>/dev/null | grep -q 'run-gate\.sh' || return 1
    etime=$(ps -p "$pid" -o etime= 2>/dev/null) || return 1
    [[ -n ${etime// /} ]] || return 1
    [[ $(($(seconds "$etime") + slack)) -ge $(($(date +%s) - start)) ]]
}

IFS=$'\t' read -r run_id _ _ started _ < <(awk 'NR==2' "$dir/run.tsv")
start=$(epoch "$started") || { echo "gate-wait.sh: run.tsv has no start time" >&2; exit 2; }
pid=$(echo "$run_id" | cut -d- -f2)
deadline=$(($(date +%s) + wait_seconds))
[[ $deadline -gt $((start + STOP)) ]] && deadline=$((start + STOP))
while :; do
    IFS=$'\t' read -r _ _ _ _ ended outcome _ < <(awk 'NR==2' "$dir/run.tsv")
    if alive "$pid" "$start"; then
        :
    else
        case "$outcome" in
            PASS|FAIL)
                duration=$(($(epoch "$ended") - start))
                echo "outcome: $outcome"
                if [[ $duration -gt $CEILING ]]; then
                    echo "duration: $duration s (ceiling $CEILING s): over the ceiling"
                else
                    echo "duration: $duration s (ceiling $CEILING s)"
                fi
                exit 0 ;;
        esac
        echo "gate-wait.sh: run.tsv says ${outcome:-nothing}, and no gate process $pid is alive: a hung run" >&2
        echo "outcome: ${outcome:-none}, no gate process"
        exit 4
    fi
    if [[ $(date +%s) -ge $deadline ]]; then
        echo "still running: $dir"
        echo "elapsed: $(($(date +%s) - start)) s"
        exit 3
    fi
    sleep 1
done
