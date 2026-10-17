#!/usr/bin/env bash
# Starts make gate detached, with tests/.env loaded (RVG-3; tests/AGENTS.md → Running the gate),
# so the gate does not have to fit one Bash tool call. It prints the run's evidence directory, as
# the gate's first line names it, and the file holding the gate's console, then returns while the
# gate runs, once the gate has published the directory's run.tsv, so tests/scripts/gate-wait.sh
# can wait for it. Exit 1, starting nothing, without tests/.env; exit 1, with the console, when
# the gate names no directory or publishes no run.tsv within 30 s, or exits first; exit 2 on any
# argument.
# Keep this script compatible with the system Bash 3.2 on macOS (no setsid there).
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT" || exit 1
[[ $# -eq 0 ]] || { echo "usage: gate-start.sh" >&2; exit 2; }
[[ -f tests/.env ]] || { echo "gate-start.sh: tests/.env is missing; nothing started" >&2; exit 1; }
mkdir -p tests/results/gate || exit 1
console="$PROJECT_ROOT/tests/results/gate/console-$(date -u +%Y%m%dT%H%M%SZ)-$$.txt"
nohup /bin/bash -c 'set -a; . tests/.env; set +a; exec make gate' > "$console" 2>&1 < /dev/null &
pid=$!
disown "$pid" 2>/dev/null || true
line=
for ((i = 0; i < 300; i++)); do
    [[ -n $line ]] || line=$(grep -m1 '^Gate evidence directory: ' "$console" 2>/dev/null)
    if [[ -n $line && -f ${line#Gate evidence directory: }/run.tsv ]]; then
        echo "$line"
        echo "Gate console: $console"
        exit 0
    fi
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.1
done
echo "gate-start.sh: the gate did not start: no evidence directory with a run.tsv; its console: $console" >&2
cat "$console" >&2
exit 1
