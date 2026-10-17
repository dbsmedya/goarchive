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
# Since RVG-3 a gate is one image's two part artifacts, gate-integration-<n> and gate-e2e-<n>, and
# it counts only when both are certified. A part is certified only on the evidence
# tests/AGENTS.md requires of a gate: exactly one run.tsv, for the target SHA, on a clean tree,
# PASS with command and recording exits 0, naming the artifact's part; the completion marker of
# that run, SHA and part beside it; and its summary.txt. Any other gate-* artifact, and an image
# with one part, fails the run. Each <job>=<result> is a needs.<job>.result; anything but success
# fails the run, whatever its artifacts say.
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

# certify prints why the part run in run.tsv $1 is not certified as part $2, or nothing when it is.
certify() {
    local run=$1 part=$2 d run_id source dirty outcome cexit rexit rpart
    d=$(dirname "$run")
    IFS=$'\t' read -r run_id source dirty _ _ outcome cexit rexit rpart < <(awk 'NR==2' "$run")
    [[ $source == "$sha" ]] || { echo "run of another SHA"; return; }
    [[ $dirty == clean ]] || { echo "tree $dirty"; return; }
    [[ $outcome == PASS ]] || { echo "outcome $outcome"; return; }
    [[ $cexit == 0 && $rexit == 0 ]] || { echo "exits $cexit/$rexit"; return; }
    [[ $rpart == "$part" ]] || { echo "run of part ${rpart:-none}"; return; }
    [[ -f $d/complete ]] || { echo "no completion marker"; return; }
    if ! grep -qx "run_id=$run_id" "$d/complete" || ! grep -qx "sha=$sha" "$d/complete"; then
        echo "completion marker of another run"; return
    fi
    if ! grep -qx "part=$part" "$d/complete" || [[ $(head -1 "$d/complete") != "GATE PART COMPLETE - every stage of part $part exited 0" ]]; then
        echo "completion marker of another part"; return
    fi
    [[ -f $d/summary.txt ]] || { echo "no summary"; return; }
}

# counts prints a part's counts from its summary.txt $1.
counts() {
    local s=$1 part=$2 e x
    if [[ $part == integration ]]; then
        grep -h '^ *integration ' "$s" 2>/dev/null | grep -oE 'PASS=[0-9]+ FAIL=[0-9]+ SKIP=[0-9]+'
        return
    fi
    e=$(grep -h '^ *e2e ' "$s" 2>/dev/null | grep -oE 'Passed: [0-9]+ +Failed: [0-9]+' | tr -cs '0-9' ' ')
    x=$(grep -h '^ *e2e-examples ' "$s" 2>/dev/null | grep -oE 'Passed: [0-9]+ +Failed: [0-9]+' | tr -cs '0-9' ' ')
    set -- $e $x
    [[ $# -eq 4 ]] && echo "E2E $1/$2, examples $3/$4"
}

# ok_integration[n] and ok_e2e[n] mark image n's certified parts; seen[n] an image with any part.
ok_integration=() ok_e2e=() seen=() last=-1
for g in "$dir"/gate-*; do
    [[ -d $g ]] || continue
    name=$(basename "$g")
    version=$(cat "$g/server-version.txt" 2>/dev/null || echo "?")
    if ! [[ $name =~ ^gate-(integration|e2e)-([0-9]+)$ ]]; then
        echo "| $name | NOT CERTIFIED: not a part artifact | | $version |"; bad=1; continue
    fi
    part=${BASH_REMATCH[1]} n=$((10#${BASH_REMATCH[2]}))
    seen[n]=1
    [[ $n -gt $last ]] && last=$n
    runs=()
    while IFS= read -r r; do runs+=("$r"); done < <(find "$g" -name run.tsv)
    if [[ ${#runs[@]} -ne 1 ]]; then
        echo "| gate-$n $part | NOT CERTIFIED: ${#runs[@]} run records | | $version |"; bad=1; continue
    fi
    why=$(certify "${runs[0]}" "$part")
    c=$(counts "$(dirname "${runs[0]}")/summary.txt" "$part")
    if [[ -n $why ]]; then
        echo "| gate-$n $part | NOT CERTIFIED: $why | $c | $version |"; bad=1; continue
    fi
    echo "| gate-$n $part | PASS | $c | $version |"
    if [[ $part == integration ]]; then ok_integration[n]=1; else ok_e2e[n]=1; fi
done

gates=0
for ((n=0; n<=last; n++)); do
    [[ -n ${seen[n]:-} ]] || continue
    for part in integration e2e; do
        if [[ ! -d $dir/gate-$part-$n ]]; then echo "| gate-$n $part | MISSING | | |"; bad=1; fi
    done
    [[ -n ${ok_integration[n]:-} && -n ${ok_e2e[n]:-} ]] && gates=$((gates + 1))
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
