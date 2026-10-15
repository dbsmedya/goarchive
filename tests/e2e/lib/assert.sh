# E2E harness — assertions.
#
# Extracted verbatim from run-tests.sh by rc-phase-023. Behaviour unchanged.
#
# Depends on the caller having defined: ARCHIVE_HOST, ARCHIVE_PORT, ARCHIVE_USER,
# ARCHIVE_DB. Requires lib/log.sh and lib/query.sh.

# Assert that a copied child/parent pair in the destination has no orphans.
#
# Usage: assert_no_orphans <child> <fk_column> <parent> <parent_pk>
assert_no_orphans() {
    local child=$1
    local fk=$2
    local parent=$3
    local parent_pk=$4

    local orphans
    if ! orphans=$(count_orphans "$ARCHIVE_HOST" "$ARCHIVE_PORT" "$ARCHIVE_USER" \
        "$ARCHIVE_DB" "$child" "$fk" "$parent" "$parent_pk"); then
        log_error "  $child -> $parent: orphan check could not run"
        log_error "    this is a FAILED ASSERTION, not a clean result."
        return 1
    fi
    if [[ -z "$orphans" ]]; then
        log_error "  $child -> $parent: orphan check returned no value"
        return 1
    fi

    if [[ "$orphans" -ne 0 ]]; then
        log_error "  $child: $orphans row(s) reference a $parent that is not in the destination"
        log_error "    the row COUNTS can still be exactly right -- this is the copy landing"
        log_error "    the wrong rows, or landing children whose parents never arrived."
        log_error "    Check the copy ORDER: parents must be copied before their children."
        return 1
    fi

    log_info "  $child -> $parent: no orphans"
    return 0
}

# Explain, on an archive failure, whether rows were conserved.
#
# NOT a gate -- see assert_postcondition's header for why conservation can no
# longer fail on its own. This runs only after an exact-count check has already
# failed, and it separates two failures an operator would chase in completely
# opposite directions:
#
#   conserved     the run was internally consistent -- every row is accounted
#                 for, just not in the quantity this test expected. Suspect a
#                 stale expected_rows or a changed fixture, not the product.
#   not conserved rows went missing or appeared out of nowhere. That is a
#                 product failure, and the count is the least of it.
#
# Archive-only by design. Purge deletes without copying and copy-only copies
# without deleting, so neither has a conservation invariant to report.
explain_archive_conservation() {
    local before=$1
    local after_src=$2
    local after_dst=$3
    local total=$((after_src + after_dst))

    if [[ "$total" -eq "$before" ]]; then
        log_error "    rows WERE conserved ($after_src + $after_dst == $before): the run was internally"
        log_error "    consistent, so suspect a stale expected_rows or a changed fixture."
    else
        log_error "    rows were NOT conserved ($after_src + $after_dst != $before): rows went missing or"
        log_error "    appeared. This is a product failure, not a stale expectation."
    fi
}

# Assert the end state a command is supposed to produce.
#
# Each command has a DIFFERENT correct outcome, and reusing one assertion for
# another passes vacuously -- a purge checked only for "the source shrank" proves
# nothing, because the destination is empty whether or not purge did its job.
#
#   command     source after        destination after
#   archive     rows moved out      rows arrived
#   purge       rows deleted        nothing copied
#   copy-only   untouched           rows arrived
#
# EVERY arm compares an EXACT count, never a direction. Direction alone was
# measured blind: with the previous conservation-only arms, `archive ALL 16044
# rows`, `purge ALL 16044 rows` and an off-by-10x slice ALL PASSED. Conservation
# (after_src + after_dst == before) does not rescue it -- a run that moved the
# whole table satisfies conservation precisely, and so does one that moved
# nothing.
#
# The pacing floor cannot cover this direction either. The floor is ONE-SIDED by
# design: a run that did MORE than configured simply takes longer and passes. So
# the floor catches a run that did too little, and the exact count catches one
# that did too much. Neither substitutes for the other.
#
# Conservation is still computed -- but only on the FAILURE path, where it is
# the one thing that separates a stale <expected> from rows going missing. As a
# gate it is now unfireable: once an archive arm has checked both
# `before - after_src == expected` and `after_dst == expected`, conservation
# follows arithmetically and can never fail on its own.
#
# The destination is assumed to start empty; ensure_destination_schema drops and
# recreates the destination database before every working test.
#
# Usage: assert_postcondition <command> <table> <before> <after_src> <after_dst> <expected>
assert_postcondition() {
    local command=$1
    local table=$2
    local before=$3
    local after_src=$4
    local after_dst=$5
    local expected=$6

    # Fail closed: without an expected count this degrades to a direction check,
    # which is exactly what the function exists to stop doing.
    if [[ -z "$expected" ]]; then
        log_error "  $table: no expected row count configured for this test"
        return 1
    fi

    local removed=$((before - after_src))

    case "$command" in
        archive)
            if [[ "$removed" -ne "$expected" ]]; then
                log_error "  $table: archive removed $removed row(s) from source, expected $expected"
                explain_archive_conservation "$before" "$after_src" "$after_dst"
                return 1
            fi
            if [[ "$after_dst" -ne "$expected" ]]; then
                log_error "  $table: archive put $after_dst row(s) in the destination, expected $expected"
                explain_archive_conservation "$before" "$after_src" "$after_dst"
                return 1
            fi
            log_info "  $table: archive OK (before=$before, source=$after_src, destination=$after_dst, moved exactly $expected)"
            ;;
        purge)
            if [[ "$removed" -ne "$expected" ]]; then
                log_error "  $table: purge deleted $removed row(s), expected exactly $expected"
                # Conservation is not an invariant for purge -- it deletes
                # without copying, so after_src + after_dst is SUPPOSED to be
                # below before. The catastrophic case has its own signature.
                if [[ "$after_src" -eq 0 ]]; then
                    log_error "    the source table is now EMPTY -- the where clause did not bound the delete."
                fi
                return 1
            fi
            if [[ "$after_dst" -ne 0 ]]; then
                log_error "  $table: purge must not copy, but destination holds $after_dst row(s)"
                return 1
            fi
            log_info "  $table: purge OK (before=$before, source=$after_src, deleted exactly $expected, destination empty)"
            ;;
        copy-only)
            if [[ "$after_src" -ne "$before" ]]; then
                log_error "  $table: copy-only must not delete, but source went $before -> $after_src"
                return 1
            fi
            if [[ "$after_dst" -ne "$expected" ]]; then
                log_error "  $table: copy-only put $after_dst row(s) in the destination, expected $expected"
                return 1
            fi
            log_info "  $table: copy-only OK (source unchanged at $after_src, destination=$after_dst, copied exactly $expected)"
            ;;
        *)
            # Fail closed: an unrecognised command must never assert nothing.
            log_error "  $table: no post-condition defined for command '$command'"
            return 1
            ;;
    esac
    return 0
}

# Assert the run took at least as long as its configured throttling requires.
#
# The floor is deterministic -- pure arithmetic over the config, with no
# dependence on the machine:
#
#   floor = (full batches) * sleep_seconds
#         + SUM over batches of max(0, delete statements in the batch - 1) * delete_sleep_seconds
#
# sleep_seconds fires after every FULL batch -- one that fetched batch_size roots --
# and not after a short last batch, which ends the run ((*run).loop,
# internal/engine/run.go). A full last batch is followed by one more fetch that
# finds nothing, so it does sleep. delete_sleep_seconds separates consecutive
# delete statements of a batch, across all its tables ((*run).deleteTree,
# internal/engine/delete.go), so a batch of 100 rows at batch_delete_size=20
# sleeps 4 times, not 5.
#
# Why this is safe to assert, when timing assertions usually are not: it is a
# ONE-SIDED bound. A slow container, a loaded host, a cold filesystem or a
# reseeded estate can only push the measured duration UP, and can therefore
# never fail it. It fails only when the sleeps did not actually happen --
# sleep_seconds silently zeroed, a config that stopped being read, a batch loop
# that ran once where it should have run twenty. Wall-clock alone cannot tell
# those apart from a fast machine; the floor can.
#
# Compared against goarchive's OWN reported elapsed time, not the shell-measured
# test duration. The latter includes the source reset, validate and dry-run,
# which together dwarf the floor and would mask a lost throttle entirely.
assert_min_duration() {
    local log_file=$1
    local floor=$2
    local job=$3

    # Fail closed: a working test with no floor configured asserts nothing.
    if [[ -z "$floor" ]]; then
        log_error "  no min_duration configured for this test"
        return 1
    fi

    # The closing entry's elapsed_seconds is the run's own wall time (logOutcome,
    # cmd/goarchive/cmd/output.go; engine.Report.Elapsed).
    local entry actual
    if ! entry=$(closing_entry "$log_file" "$job" "Run completed"); then
        log_error "  could not read the run's own elapsed time: no single closing entry"
        log_error "  if the closing entry changed, this assertion has stopped matching and is"
        log_error "  no longer checking anything."
        return 1
    fi
    actual=$(jq -r '.elapsed_seconds | select(type == "number" and . >= 0)' <<<"$entry")
    if ! _nonneg_number "$actual"; then
        log_error "  the closing entry's elapsed_seconds is missing or malformed: '$actual'"
        return 1
    fi

    # bash 3.2 has no floating-point arithmetic, so awk does the comparison.
    if ! awk -v a="$actual" -v f="$floor" 'BEGIN { exit !(a >= f) }'; then
        log_error "  run finished in ${actual}s, below its ${floor}s pacing floor"
        log_error "  the configured sleep_seconds / delete_sleep_seconds cannot all have"
        log_error "  been applied -- this is a throttling regression, not a fast machine."
        return 1
    fi

    log_info "  pacing OK (${actual}s >= ${floor}s floor)"
    return 0
}

# Assert the run verified its copies by the declared method, and all of them.
#
# Usage: assert_verified <log_file> <method> <job>     method = sha256 | count | none
#
# The anchor is the run's closing entry (logOutcome, cmd/goarchive/cmd/output.go):
#
#   {"msg":"Run completed","job":"j",...,"copied":{"payment":200},"verified":{"payment":200},
#    "verification":"sha256",...}
#
# verified holds the rows whose verification passed, by table, for the committed batches;
# it is present only when something was compared. So under sha256 or count the two objects
# must be EQUAL -- verified covering fewer rows than were copied means some batch committed
# without verification -- and under none (and for purge, which copies nothing) there must be
# no verified object.
#
# Nothing at any layer asserted that verification ran before 2.x's E2E did, and
# verification is what makes delete-after-copy safe; a config change or a silently
# skipped stage would leave every other assertion passing.
assert_verified() {
    local log_file=$1
    local method=$2
    local job=$3

    local entry verified copied
    entry=$(closing_entry "$log_file" "$job" "Run completed") || return 1
    verified=$(jq -c '.verified // empty' <<<"$entry")
    if [[ "$method" == "none" ]]; then
        if [[ -n "$verified" ]]; then
            log_error "  verification ran, but this test declares verify_method=none: $verified"
            return 1
        fi
        log_info "  confirmed: nothing was verified, as expected"
        return 0
    fi
    if [[ -z "$verified" ]]; then
        log_error "  no verified counts in the run's closing entry (expect method=$method)"
        log_error "    Either nothing was verified, or the closing entry changed and this"
        log_error "    assertion has stopped matching. Both are findings."
        return 1
    fi
    if [[ $(jq -r '.verification // empty' <<<"$entry") != "$method" ]]; then
        log_error "  the run verified by another method than $method: $(jq -r '.verification' <<<"$entry")"
        return 1
    fi
    copied=$(jq -c '.copied // empty' <<<"$entry")
    if ! _count_table "$copied" || ! _count_table "$verified"; then
        log_error "  copied and verified must be tables of nonnegative row counts:"
        log_error "    copied   $copied"
        log_error "    verified $verified"
        return 1
    fi
    if ! _json_equal "$verified" "$copied"; then
        log_error "  verification did not cover every copied row:"
        log_error "    copied   $copied"
        log_error "    verified $verified"
        return 1
    fi
    log_info "  confirmed: every copied row was verified by $method ($copied)"
    return 0
}

# Print the one closing entry of <job> in <log_file> whose msg starts with <msg_prefix>, as
# compact JSON. The closing entry is logOutcome's (cmd/goarchive/cmd/output.go). Fails
# closed: no such entry, or more than one -- an earlier run's entry must never satisfy the
# current run's check, so callers pass a file that holds one run.
closing_entry() {
    local log_file=$1 job=$2 msg_prefix=$3
    local entries n
    # jq reads every JSON line before selecting, so a malformed line anywhere, or an entry
    # that is not an object with a string msg, fails the read with no output: jq 1.8 would
    # otherwise print a match and exit 0 past a per-line error. jq's status is the pipeline's.
    if ! entries=$(grep -E '^\{' "$log_file" | jq -cn --arg job "$job" --arg p "$msg_prefix" \
        '[inputs] as $all
         | if ($all | all(type == "object" and (.msg | type) == "string"))
           then $all[] | select(.job == $job and (.msg | startswith($p)))
           else error("an entry is not an object with a string msg") end' 2>/dev/null); then
        log_error "  $log_file holds a JSON line jq cannot read; the run's evidence is malformed"
        return 1
    fi
    n=$(printf '%s' "$entries" | grep -c '^{')
    if [[ "$n" -ne 1 ]]; then
        log_error "  expected exactly one \"$msg_prefix\" entry for job $job in $log_file, found $n"
        return 1
    fi
    printf '%s\n' "$entries"
}

# Succeed when $1 is a nonnegative decimal number.
_nonneg_number() {
    [[ "$1" =~ ^[0-9]+(\.[0-9]+)?$ ]]
}

# Succeed when the JSON value $1 is a table of counts: an object whose every value is a
# nonnegative integer.
_count_table() {
    jq -e 'type == "object" and all(.[]; type == "number" and . >= 0 and . == floor)' <<<"$1" >/dev/null 2>&1
}

# Succeed when the JSON values $1 and $2 are equal as JSON (key order aside).
_json_equal() {
    [[ $(jq -n --argjson a "$1" --argjson b "$2" '$a == $b' 2>/dev/null) == "true" ]]
}
