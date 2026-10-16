#!/usr/bin/env bash
# The summary of a Matrix verify or Gate verify run (beta.1 PR 4; split in beta.2, RVG-1a+b):
# one Markdown table of every gate and matrix verdict from the run's downloaded artifacts, and of
# the jobs it depends on. Exit 1 unless every job succeeded, every gate is certified for the
# target SHA, every row is OK, and the run holds exactly the rows and gates it wants; exit 2 on
# counts that are missing or not numbers, or that want nothing.
#
#   tests/envmatrix/gh-summary.sh <artifacts-dir> <sha> <rows-wanted> <gates-wanted> [<job>=<result> ...]
#
# Matrix verify wants every rows.tsv row and 0 gates; Gate verify wants 0 rows and 6 gates.
# A gate is certified only on the evidence tests/AGENTS.md requires: exactly one run.tsv, for the
# target SHA, on a clean tree, PASS with command and recording exits 0; the completion marker of
# that run and SHA beside it; and its summary.txt. Each <job>=<result> is a needs.<job>.result;
# anything but success fails the run, whatever its artifacts say.
set -uo pipefail

usage="usage: gh-summary.sh <artifacts-dir> <sha> <rows-wanted> <gates-wanted> [<job>=<result> ...]"
[[ $# -ge 4 && $3 =~ ^[0-9]+$ && $4 =~ ^[0-9]+$ ]] || { echo "$usage" >&2; exit 2; }
(( 10#$3 + 10#$4 > 0 )) || { echo "gh-summary.sh: a summary that wants no rows and no gates verifies nothing" >&2; exit 2; }

dir=$1 sha=$2 want_rows=$((10#$3)) want_gates=$((10#$4)) bad=0
shift 4
title="Matrix verify"
if [[ $want_gates -gt 0 ]]; then title="Gate verify"; fi
echo "## $title: \`${sha:0:12}\`"
echo
echo "| Job | Verdict | Counts | Server |"
echo "|---|---|---|---|"

for need in "$@"; do
    job=${need%%=*} result=${need#*=}
    if [[ $result != success ]]; then
        echo "| job $job | $result | | |"; bad=1
    fi
done

# certify prints why the gate run in run.tsv $1 is not certified, or nothing when it is.
certify() {
    local run=$1 d run_id source dirty outcome cexit rexit
    d=$(dirname "$run")
    IFS=$'\t' read -r run_id source dirty _ _ outcome cexit rexit < <(awk 'NR==2' "$run")
    [[ $source == "$sha" ]] || { echo "run of another SHA"; return; }
    [[ $dirty == clean ]] || { echo "tree $dirty"; return; }
    [[ $outcome == PASS ]] || { echo "outcome $outcome"; return; }
    [[ $cexit == 0 && $rexit == 0 ]] || { echo "exits $cexit/$rexit"; return; }
    [[ -f $d/complete ]] || { echo "no completion marker"; return; }
    if ! grep -qx "run_id=$run_id" "$d/complete" || ! grep -qx "sha=$sha" "$d/complete"; then
        echo "completion marker of another run"; return
    fi
    [[ -f $d/summary.txt ]] || { echo "no summary"; return; }
}

gates=0
for g in "$dir"/gate-*; do
    [[ -d $g ]] || continue
    gates=$((gates + 1))
    name=$(basename "$g")
    version=$(cat "$g/server-version.txt" 2>/dev/null || echo "?")
    runs=()
    while IFS= read -r r; do runs+=("$r"); done < <(find "$g" -name run.tsv)
    if [[ ${#runs[@]} -ne 1 ]]; then
        echo "| $name | NOT CERTIFIED: ${#runs[@]} run records | | $version |"; bad=1; continue
    fi
    why=$(certify "${runs[0]}")
    counts=$(grep -h '^ *integration' "$(dirname "${runs[0]}")/summary.txt" 2>/dev/null | grep -oE 'PASS=[0-9]+ FAIL=[0-9]+ SKIP=[0-9]+')
    if [[ -n $why ]]; then
        echo "| $name | NOT CERTIFIED: $why | $counts | $version |"; bad=1; continue
    fi
    echo "| $name | PASS | $counts | $version |"
done

rows=0
for v in "$dir"/matrix-*/*/verdict.txt; do
    [[ -f $v ]] || continue
    rows=$((rows + 1))
    row=$(basename "$(dirname "$v")")
    verdict=$(grep -oE 'verdict: [A-Z]+' "$v" | cut -d' ' -f2)
    counts=$(grep -oE 'pass: [0-9]+ +fail: [0-9]+ +skip: [0-9]+' "$v")
    image=$(grep -oE 'image: [^ ]+' "$v" | cut -d' ' -f2)
    echo "| matrix $row | $verdict | $counts | $image |"
    [[ $verdict == OK ]] || bad=1
done

echo
echo "Gates: $gates (want $want_gates). Matrix rows: $rows (want $want_rows)."
[[ $gates -eq $want_gates && $rows -eq $want_rows ]] || bad=1
exit "$bad"
