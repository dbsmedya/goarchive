#!/usr/bin/env bash
# The release-verify summary (beta.1 PR 4): one Markdown table of every gate and matrix verdict
# from the run's downloaded artifacts. Exit 1 when any gate or row is not OK, or is missing.
#
#   tests/envmatrix/gh-summary.sh <artifacts-dir> <sha> <rows-wanted>
set -uo pipefail

dir=$1 sha=$2 want_rows=$3 bad=0
echo "## Release verify: \`${sha:0:12}\`"
echo
echo "| Job | Verdict | Counts | Server |"
echo "|---|---|---|---|"

gates=0
for g in "$dir"/gate-*; do
    [[ -d $g ]] || continue
    gates=$((gates + 1))
    run=$(find "$g" -name run.tsv | head -1)
    version=$(cat "$g/server-version.txt" 2>/dev/null || echo "?")
    if [[ -z $run ]]; then
        echo "| $(basename "$g") | NO EVIDENCE | | $version |"; bad=1; continue
    fi
    outcome=$(awk -F'\t' 'NR==2 {print $6}' "$run")
    counts=$(grep -h '^ *integration' "$(dirname "$run")/summary.txt" 2>/dev/null | grep -oE 'PASS=[0-9]+ FAIL=[0-9]+ SKIP=[0-9]+')
    echo "| $(basename "$g") | $outcome | $counts | $version |"
    [[ $outcome == PASS ]] || bad=1
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
echo "Gates: $gates (want 6). Matrix rows: $rows (want $want_rows)."
[[ $gates -eq 6 && $rows -eq $want_rows ]] || bad=1
exit "$bad"
