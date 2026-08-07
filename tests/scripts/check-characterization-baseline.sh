#!/usr/bin/env bash
#
# Run the characterization suite and check it against its recorded baseline.
#
# WHY THIS EXISTS
#
# The baseline used to be a sentence in CLAUDE.md -- "60 / 304 / 364 / 0 / 0" --
# that every verifier re-derived by hand with ad-hoc greps. That is a trap, and
# it fired: counting subtests with `grep -c '^    --- PASS'` returns 206, not
# 304, because these tests nest TWO levels deep. The 98 missing tests are at
# 8-space indent. The result looked exactly like a 98-test regression in a
# change that touched zero .go files.
#
# A number a human has to reproduce with a hand-written grep is not a baseline,
# it is a quiz. This script owns the counting so nobody has to be told the trick.
#
# Requires the test databases (the suite is behind the `integration` build tag).
#
# Usage: check-characterization-baseline.sh [-v]
#   -v   also print the per-depth breakdown

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# The baseline lives in a DATA FILE, not here. Verifiers run this script and
# read its verdict; nobody counts by hand and nobody edits a constant buried in
# a shell script. Changing the numbers is a decision -- see the file's header.
BASELINE_FILE="$PROJECT_ROOT/tests/characterization-baseline.txt"

if [[ ! -f "$BASELINE_FILE" ]]; then
    echo "ERROR: baseline file not found: $BASELINE_FILE" >&2
    echo "  It is tracked in git; a clone should have it." >&2
    exit 1
fi

# Parse KEY=VALUE, ignoring comments and blanks. Not `source` -- this file is
# data, and sourcing it would execute anything that got in there.
EXPECT_TOPLEVEL=""; EXPECT_SUBTESTS=""; EXPECT_PASS=""; EXPECT_FAIL=""; EXPECT_SKIP=""
while IFS='=' read -r key value; do
    case "$key" in
        TOPLEVEL) EXPECT_TOPLEVEL="$value" ;;
        SUBTESTS) EXPECT_SUBTESTS="$value" ;;
        PASS)     EXPECT_PASS="$value" ;;
        FAIL)     EXPECT_FAIL="$value" ;;
        SKIP)     EXPECT_SKIP="$value" ;;
    esac
done < <(grep -E '^[A-Z]+=' "$BASELINE_FILE")

# Fail closed: a baseline file that parsed to nothing would make every
# comparison below vacuous, which is the failure this whole script exists to
# prevent.
for v in TOPLEVEL SUBTESTS PASS FAIL SKIP; do
    eval "val=\${EXPECT_$v}"
    if [[ -z "$val" ]]; then
        echo "ERROR: $BASELINE_FILE does not define $v" >&2
        echo "  Refusing to run: an unparsed baseline asserts nothing." >&2
        exit 1
    fi
done

# Internal consistency of the recorded numbers themselves.
if [[ $((EXPECT_TOPLEVEL + EXPECT_SUBTESTS)) -ne "$EXPECT_PASS" ]]; then
    echo "ERROR: baseline is self-inconsistent: TOPLEVEL($EXPECT_TOPLEVEL) + SUBTESTS($EXPECT_SUBTESTS) != PASS($EXPECT_PASS)" >&2
    exit 1
fi

VERBOSE=0
[[ "${1:-}" == "-v" ]] && VERBOSE=1

log()  { echo "$*"; }
fail() { echo "ERROR: $*" >&2; }

cd "$PROJECT_ROOT" || exit 1

if [[ -z "${MYSQL_ROOT_PASSWORD:-}" ]]; then
    fail "MYSQL_ROOT_PASSWORD is not set."
    fail "  Run: set -a; source tests/.env; set +a"
    exit 1
fi

logfile="$(mktemp -t goarchive-characterization)" || exit 1
trap 'rm -f "$logfile"' EXIT

log "Running the characterization suite (this takes ~10s)..."

# Combined streams ON PURPOSE, and this is the one place that is correct: the
# counting reads `go test -v` output, and a panic reaching stderr must land in
# the same file or a crashed run would count as zero failures.
go test -tags=integration -count=1 -v -run 'TestCharacterization' \
    ./internal/archiver/ > "$logfile" 2>&1
go_status=$?

# Count at ANY depth. `^ *---` is the whole point of this script: the 4-space
# form silently drops the 98 second-level subtests.
toplevel=$(grep -c '^--- PASS' "$logfile")
pass=$(grep -cE '^ *--- PASS' "$logfile")
failed=$(grep -cE '^ *--- FAIL' "$logfile")
skipped=$(grep -cE '^ *--- SKIP' "$logfile")
subtests=$((pass - toplevel))

if [[ $VERBOSE -eq 1 ]]; then
    log ""
    log "PASS lines by nesting depth:"
    grep -oE '^ *--- PASS' "$logfile" | awk '{print length($0)-8}' | sort -n | uniq -c \
        | awk '{printf "  %2d-space indent: %s\n", $2, $1}'
fi

log ""
log "  top-level : $toplevel  (expected $EXPECT_TOPLEVEL)"
log "  subtests  : $subtests  (expected $EXPECT_SUBTESTS)"
log "  PASS      : $pass  (expected $EXPECT_PASS)"
log "  FAIL      : $failed  (expected $EXPECT_FAIL)"
log "  SKIP      : $skipped  (expected $EXPECT_SKIP)"
log ""

status=0

if [[ $go_status -ne 0 ]]; then
    fail "go test exited $go_status"
    grep -E '^ *--- FAIL|^panic|^FAIL' "$logfile" | head -20 >&2
    status=1
fi

if [[ "$toplevel" -ne "$EXPECT_TOPLEVEL" || "$subtests" -ne "$EXPECT_SUBTESTS" \
   || "$pass" -ne "$EXPECT_PASS" || "$failed" -ne "$EXPECT_FAIL" \
   || "$skipped" -ne "$EXPECT_SKIP" ]]; then
    fail "characterization baseline MISMATCH"
    fail "  expected: $EXPECT_TOPLEVEL / $EXPECT_SUBTESTS / $EXPECT_PASS / $EXPECT_FAIL / $EXPECT_SKIP"
    fail "  actual:   $toplevel / $subtests / $pass / $failed / $skipped"
    fail ""
    if [[ "$failed" -eq 0 && "$pass" -gt "$EXPECT_PASS" ]]; then
        fail "  MORE tests pass than the baseline records. If you added"
        fail "  characterization tests deliberately, the baseline needs an"
        fail "  authorized bump -- in this script AND in CLAUDE.md."
    elif [[ "$failed" -eq 0 && "$pass" -lt "$EXPECT_PASS" ]]; then
        fail "  FEWER tests ran, and none failed -- so tests DISAPPEARED rather"
        fail "  than broke. Suspect a build tag, a renamed test, or a -run filter"
        fail "  before suspecting the product."
    fi
    status=1
fi

if [[ $status -eq 0 ]]; then
    log "characterization baseline: OK ($toplevel / $subtests / $pass / $failed / $skipped)"
fi

exit $status
