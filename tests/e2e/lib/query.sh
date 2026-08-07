# E2E harness — database reads, and the self-tests that prove they fail loud.
#
# Extracted verbatim from run-tests.sh by rc-phase-023. Behaviour unchanged.
#
# Depends on the caller having defined: SCRIPT_DIR, SOURCE_USER, SOURCE_DB,
# ARCHIVE_USER, ARCHIVE_DB. Requires lib/log.sh.

# Get row count from a table.
#
# Routed through mysql-query.sh so that A FAILED QUERY CAN NEVER BE MISTAKEN FOR A
# ROW COUNT OF ZERO. The previous form called mysqlsh directly, discarded stderr
# with 2>/dev/null, ignored the exit status, and defaulted an empty result to "0"
# via ${count:-0}. An unreachable server, a wrong password, a dropped table and a
# genuinely empty table were therefore indistinguishable -- all four reported "0".
#
# That is not a theoretical hole. Every post-condition assertion below is built on
# this function, and the purge assertion is "the destination stayed empty": a
# destination query that fails returns 0 and the assertion PASSES, without a
# connection ever having been made. selftest_get_row_count_fails_loud proves it
# does not, and runs on every invocation of the suite.
#
# Contract:
#   stdout / exit 0   the row count
#   stderr / exit 1   the query failed -- callers MUST check
get_row_count() {
    local host=$1
    local port=$2
    local user=$3
    local db=$4
    local table=$5

    # `local out` is declared on its own line. Written as `local out=$(...)` the
    # exit status would be `local`'s (always 0) rather than the query's, which
    # silently restores exactly the defect this function was rewritten to remove.
    local out
    if ! out=$(MYSQL_QUERY_HOST="$host" MYSQL_QUERY_USER="$user" \
        "$SCRIPT_DIR/mysql-query.sh" "$port" "SELECT COUNT(*) FROM \`$db\`.\`$table\`;"); then
        echo "get_row_count: could not count ${db}.${table} on ${host}:${port}" >&2
        return 1
    fi

    # mysql-query.sh emits the column header, then the value.
    echo "$out" | tail -1
}

# Prove that get_row_count reports a failure instead of returning "0".
#
# This is deliberately NOT a mutation proof that has to be applied and reverted.
# The failure mode is reachable through an ARGUMENT, so it can be asserted on
# every run and keeps holding: port 59999 on loopback is refused immediately
# (ECONNREFUSED, measured ~0.3s -- no DNS, no timeout, no server state touched,
# nothing else in the run affected).
#
# A reverted mutation leaves a sentence in a plan file. This leaves a test.
selftest_get_row_count_fails_loud() {
    local out rc
    out=$(get_row_count 127.0.0.1 59999 "$SOURCE_USER" "$SOURCE_DB" actor 2>/dev/null)
    rc=$?
    if [[ $rc -eq 0 ]]; then
        log_error "HARNESS SELF-CHECK FAILED: get_row_count exited 0 against an unreachable port."
        log_error "  It returned: '${out}'"
        log_error "  A failed query is being reported as a row count, which means every"
        log_error "  post-condition assertion in this suite is vacuous. Refusing to run."
        return 1
    fi
    log_verbose "self-check: get_row_count fails loud on an unreachable port (exit $rc)"
    return 0
}

# Count rows in the destination's CHILD table that reference a PARENT row which
# is not there.
#
# Row counts cannot cover this, and the reason is structural rather than
# theoretical: every working test that copies must set
# safety.disable_foreign_key_checks: true, because a copied child normally
# references out-of-graph parents the destination does not hold (payment -> the
# uncopied customer and staff). With FK enforcement off, the destination cannot
# reject an orphan on its own. "200 rentals and 200 payments arrived" therefore
# stays true even if all 200 payments point at rentals that never did.
#
# A NULL foreign key is NOT an orphan -- it is a row with no parent reference,
# which is legal. Load-bearing here rather than defensive: sakila's
# payment.rental_id is `INT DEFAULT NULL` with ON DELETE SET NULL
# (tests/sakila-db/sakila-schema.sql:273,280), so omitting the IS NOT NULL guard
# would report legitimately-null rows as orphans.
#
# Contract, deliberately identical to get_row_count's:
#   stdout / exit 0   the orphan count
#   stderr / exit 1   the query failed -- callers MUST check
# A failed query is a FAILURE, never "0 orphans". Returning 0 on error would
# make this assertion indistinguishable from a passing one, which is the exact
# defect get_row_count was rewritten to remove.
count_orphans() {
    local host=$1
    local port=$2
    local user=$3
    local db=$4
    local child=$5
    local fk=$6
    local parent=$7
    local parent_pk=$8

    # `local out` is declared on its own line, for the same reason as in
    # get_row_count: written as `local out=$(...)` the exit status would be
    # `local`'s (always 0) rather than the query's.
    local out
    if ! out=$(MYSQL_QUERY_HOST="$host" MYSQL_QUERY_USER="$user" \
        "$SCRIPT_DIR/mysql-query.sh" "$port" \
        "SELECT COUNT(*) FROM \`$db\`.\`$child\` c \
         LEFT JOIN \`$db\`.\`$parent\` p ON c.\`$fk\` = p.\`$parent_pk\` \
         WHERE c.\`$fk\` IS NOT NULL AND p.\`$parent_pk\` IS NULL;"); then
        echo "count_orphans: could not check ${db}.${child}.${fk} -> ${db}.${parent} on ${host}:${port}" >&2
        return 1
    fi

    # mysql-query.sh emits the column header, then the value.
    echo "$out" | tail -1
}

# Prove that count_orphans reports a failure instead of returning "0".
#
# Same reasoning as selftest_get_row_count_fails_loud, and the same reachable
# argument: an orphan count that silently reads 0 on a broken query reports
# "referential integrity holds" for every test that uses it.
selftest_count_orphans_fails_loud() {
    local out rc
    out=$(count_orphans 127.0.0.1 59999 "$ARCHIVE_USER" "$ARCHIVE_DB" \
        payment rental_id rental rental_id 2>/dev/null)
    rc=$?
    if [[ $rc -eq 0 ]]; then
        log_error "HARNESS SELF-CHECK FAILED: count_orphans exited 0 against an unreachable port."
        log_error "  It returned: '${out}'"
        log_error "  A failed query is being reported as an orphan count, which means every"
        log_error "  referential-integrity assertion in this suite is vacuous. Refusing to run."
        return 1
    fi
    log_verbose "self-check: count_orphans fails loud on an unreachable port (exit $rc)"
    return 0
}
