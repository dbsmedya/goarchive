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
#   floor = ceil(rows / batch_size) * sleep_seconds
#         + SUM over batches of (delete chunks per table - 1) * delete_sleep_seconds
#
# sleep_seconds fires after EVERY batch including the last (orchestrator.go:457,
# no guard). delete_sleep_seconds skips each table's final chunk
# (delete.go:185, `batchNum < totalBatches-1`), so a batch of 100 rows at
# batch_delete_size=20 sleeps 4 times, not 5.
#
# Why this is safe to assert, when timing assertions usually are not: it is a
# ONE-SIDED bound. A slow container, a loaded host, a cold filesystem or a
# reseeded estate can only push the measured duration UP, and can therefore
# never fail it. It fails only when the sleeps did not actually happen --
# sleep_seconds silently zeroed, a config that stopped being read, a batch loop
# that ran once where it should have run twenty. Wall-clock alone cannot tell
# those apart from a fast machine; the floor can.
#
# Compared against goarchive's OWN reported duration, not the shell-measured
# test duration. The latter includes the source reset, validate and dry-run,
# which together dwarf the floor and would mask a lost throttle entirely.
assert_min_duration() {
    local log_file=$1
    local floor=$2

    # Fail closed: a working test with no floor configured asserts nothing.
    if [[ -z "$floor" ]]; then
        log_error "  no min_duration configured for this test"
        return 1
    fi

    # The last "duration":<seconds> in the log is the command's own total.
    # archive, purge and copy-only all emit it as a structured field
    # (cmd/goarchive/cmd/{archive,purge,copyonly}.go). The phase-level logs spell
    # duration inside a message string instead, so they cannot collide with this.
    local actual
    actual=$(grep -o '"duration":[0-9.]*' "$log_file" | tail -1 | cut -d: -f2)
    if [[ -z "$actual" ]]; then
        log_error "  could not read the run's own duration from $log_file"
        log_error "  the command logs it as a \"duration\" field; if that changed,"
        log_error "  this assertion has stopped matching and is no longer checking anything."
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
