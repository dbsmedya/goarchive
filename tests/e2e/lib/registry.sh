# E2E harness — the test registry: which numbers exist, and which file owns each.
#
# Added by rc-phase-023.
#
# A test lives at tests/e2e/<category>/NN-<slug>.test.sh. The NUMBER is the test's
# identity and the CATEGORY is only where the file lives:
#
#   - `-t N` targeting, tests/results/test_N.log and tests/configs/testNN_*.yaml
#     all key off the number, so numbers must stay stable across a move between
#     categories.
#   - The category is DERIVED from the parent directory, never declared inside the
#     file. A declared category could disagree with where the file actually is;
#     a derived one cannot.
#
# Ordering is NOT taken from the filesystem. The glob would give archive(03,04,05),
# copy-only(07), purge(06) -- alphabetical by directory, so 03 04 05 07 06, which
# silently reorders the suite. The caller passes an explicit ordered number list;
# this file only answers "which file is number N".
#
# Depends on the caller having defined: TESTS_DIR. Requires lib/log.sh.

# Emit the path of every discovered test file, one per line, in filesystem order.
# Emits nothing and returns 1 when the tree holds no tests at all -- an empty
# suite reported as a pass is the failure mode this guards.
registry_files() {
    local f found=0
    for f in "$TESTS_DIR"/e2e/*/[0-9][0-9]-*.test.sh; do
        # With no matches the glob stays literal, so test for a real file.
        [ -e "$f" ] || continue
        printf '%s\n' "$f"
        found=1
    done
    if [[ $found -eq 0 ]]; then
        log_error "registry: no test files found under $TESTS_DIR/e2e/*/"
        log_error "  expected tests/e2e/<category>/NN-<slug>.test.sh"
        return 1
    fi
    return 0
}

# The two-digit form of a test number, or failure if it is not a number.
#
# `-t 3` and a file called 03-payment-batch.test.sh have to meet somewhere, and
# the flag has always accepted the unpadded form.
#
# Two bash 3.2 traps, both measured, both landing squarely on the tests this
# refactor exists to make room for:
#
#   printf '%02d' abc  -> prints 0 and a diagnostic. So a typo would resolve to
#                         test 00 rather than being rejected. Hence the numeric
#                         check first.
#   printf '%02d' 08   -> "invalid number": a leading zero makes bash read the
#                         argument as OCTAL, and 08 is not octal. `-t 08` and
#                         `-t 09` are the natural way to ask for the resume tests
#                         by the names they will carry, so $((10#$n)) forces
#                         base 10 before printf sees it.
registry_pad() {
    local n="$1"
    case "$n" in
        ''|*[!0-9]*)
            log_error "registry: '$n' is not a test number"
            return 1
            ;;
    esac
    printf '%02d\n' "$((10#$n))"
}

# The category a test file belongs to, derived from its parent directory.
registry_category_for_file() {
    local dir
    dir=$(dirname "$1")
    basename "$dir"
}

# Every test number present in the tree, numerically sorted.
#
# For validation and for reporting -- NOT for deciding run order. See the header.
registry_all_numbers() {
    local f base
    while IFS= read -r f; do
        base=$(basename "$f")
        printf '%s\n' "${base%%-*}"
    done < <(registry_files) | sort -n
}

# Resolve a test number to exactly one file path on stdout.
#
# Fails closed on both ambiguities, because both have a silent form:
#   0 matches  -- a requested test that does not exist must not be reported as a
#                 pass, or `-t 99` would look like a clean run of nothing.
#   >1 matches -- two categories claiming the same number must not resolve
#                 last-wins, which would run one test and silently ignore another.
registry_file_for() {
    local want padded f count=0 hit=""
    want="$1"

    if ! padded=$(registry_pad "$want"); then
        return 1
    fi

    for f in "$TESTS_DIR"/e2e/*/"$padded"-*.test.sh; do
        [ -e "$f" ] || continue
        hit="$f"
        count=$((count + 1))
        if [[ $count -gt 1 ]]; then
            log_error "registry: test number $padded is claimed by more than one file:"
            local dup
            for dup in "$TESTS_DIR"/e2e/*/"$padded"-*.test.sh; do
                [ -e "$dup" ] && log_error "    $dup"
            done
            log_error "  A number identifies exactly one test. Renumber one of them."
            return 1
        fi
    done

    if [[ $count -eq 0 ]]; then
        log_error "registry: no test file for number $padded"
        log_error "  looked for $TESTS_DIR/e2e/*/$padded-*.test.sh"
        log_error "  available: $(registry_all_numbers | tr '\n' ' ')"
        return 1
    fi

    printf '%s\n' "$hit"
    return 0
}

# Refuse to start a suite whose registry is ambiguous.
#
# registry_file_for already fails closed per number, but it is only ever called
# for the numbers a run was asked for. A duplicate on a number this run does not
# touch would sit undetected until some later run happened to request it, so the
# whole tree is checked once up front.
registry_validate() {
    # An empty tree must not validate. registry_all_numbers pipes through sort,
    # so on no input it exits 0 with no output and `uniq -d` finds no duplicates
    # -- a suite with zero tests would pass this check clean.
    if ! registry_files > /dev/null; then
        return 1
    fi

    local dupes
    dupes=$(registry_all_numbers | uniq -d)
    if [[ -n "$dupes" ]]; then
        log_error "registry: duplicate test number(s): $(echo "$dupes" | tr '\n' ' ')"
        local n
        for n in $dupes; do
            local f
            for f in "$TESTS_DIR"/e2e/*/"$n"-*.test.sh; do
                [ -e "$f" ] && log_error "    $n  $f"
            done
        done
        return 1
    fi
    return 0
}
