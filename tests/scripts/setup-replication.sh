#!/bin/bash
#
# Attach the replica (db3) to the source (db1) as a live replication topology,
# IDEMPOTENTLY, so that `run-tests.sh --setup` produces a replicating estate.
#
# Usage:
#   set -a; source tests/.env; set +a
#   tests/scripts/setup-replication.sh
#
# Contract:
#   exit 0      the replica is attached and healthy (freshly, or already was)
#   exit 1      the attach failed, OR an EXISTING config diverges from expected
#   exit 2      the environment is not loaded / a required variable is unsafe
#
# ---------------------------------------------------------------------------
# Two decisions are load-bearing. Both exist because the failure they prevent
# is one that otherwise passes silently.
#
# 1. THIS SCRIPT NEVER AUTO-REPAIRS A DIVERGENT REPLICATION CONFIG.
#
#    If db3 is already replicating from somewhere, the script compares the
#    live config against what this estate requires and, on ANY mismatch, prints
#    the expected/actual diff plus the remediation SQL and exits 1. It does not
#    issue CHANGE REPLICATION SOURCE over the top.
#
#    Re-pointing a replica silently discards whatever the operator was doing --
#    a hand-built lag scenario, a deliberately stopped applier, a different
#    source. "Setup made it work again" is exactly how the estate ends up in a
#    state nobody can explain. Diverged means STOP and tell a human.
#
# 2. THE PROBE CHECKS THE WHOLE CONFIG, NOT JUST "BOTH THREADS RUNNING".
#
#    A replica happily reports Replica_IO_Running=Yes / Replica_SQL_Running=Yes
#    while pointing at the WRONG SOURCE, running as the wrong user, or carrying
#    a leftover SOURCE_DELAY from a lag test. Each of those makes the
#    replication tests measure something other than what they claim to. So the
#    probe pins source host/port, the replication user, both threads, SQL_Delay,
#    and the positioning mode -- all of them, every run.
#
#    For the same reason the script does not return until the replica reports
#    zero lag: handing back an estate that is connected but still replaying
#    means the next test measures catch-up and calls it replication lag.
#
# REPL_PASSWORD containing a single quote is REJECTED rather than escaped. The
# value is injected into SQL as a single-quoted literal; refusing the one
# character that could break out of that literal is the entire quoting rule,
# and it cannot be got subtly wrong the way an escaping scheme can.
#
# Addressing: db3 reaches the source as the compose SERVICE NAME `db1` on the
# IN-CONTAINER port 3306 -- never 127.0.0.1:3305, which is the host mapping and
# is not routable from inside the container.
#
# This script calls tests/scripts/mysql-query.sh exclusively (never mysqlsh
# directly -- see that script's header for why), and never merges stderr into
# stdout.
# ---------------------------------------------------------------------------

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUERY="$SCRIPT_DIR/mysql-query.sh"

SOURCE_PORT="${TEST_SOURCE_PORT:-3305}"
REPLICA_PORT="${TEST_REPLICA_PORT:-3308}"

# Fixed by tests/compose.yml: the address db3 uses to reach db1.
SOURCE_HOST_INTERNAL="db1"
SOURCE_PORT_INTERNAL="3306"

ATTACH_TIMEOUT_SECONDS=30
CATCHUP_TIMEOUT_SECONDS=120
POLL_INTERVAL_SECONDS=2

# --- environment ------------------------------------------------------------

fail_env() {
    echo "setup-replication.sh: $1" >&2
    cat >&2 <<'EOF'

  Load the test environment first:

      set -a; source tests/.env; set +a

  If tests/.env does not exist yet:

      cp tests/dot.env tests/.env        # then edit it

  Reference: tests/README.md
EOF
    exit 2
}

[ -n "${MYSQL_ROOT_PASSWORD:-}" ] || fail_env "MYSQL_ROOT_PASSWORD is not set."
[ -n "${REPL_USER:-}" ] || fail_env "REPL_USER is not set."
[ -n "${REPL_PASSWORD:-}" ] || fail_env "REPL_PASSWORD is not set."

case "$REPL_PASSWORD" in
    *\'*)
        echo "setup-replication.sh: REPL_PASSWORD contains a single quote — refusing to run." >&2
        echo "  The value is injected into SQL as a single-quoted literal. Rejecting the" >&2
        echo "  quote IS the quoting rule here; choose a password without one." >&2
        exit 2
        ;;
esac

REPL_POSITIONING="${REPL_POSITIONING:-}"
case "$REPL_POSITIONING" in
    gtid)     WANT_AUTO_POSITION=1 ;;
    snapshot) WANT_AUTO_POSITION=0 ;;
    "")       fail_env "REPL_POSITIONING is not set (expected 'gtid' or 'snapshot')." ;;
    *)
        echo "setup-replication.sh: REPL_POSITIONING='$REPL_POSITIONING' is not recognised." >&2
        echo "  Expected 'gtid' or 'snapshot'." >&2
        exit 2
        ;;
esac

# --- probe ------------------------------------------------------------------

# read_status prints the live replication config as key=value lines, or nothing
# at all when db3 is not a replica. Empty output is a real answer here ("no
# replication configured"); mysql-query.sh guarantees a FAILURE cannot look the
# same, because it exits non-zero.
read_status() {
    "$QUERY" "$REPLICA_PORT" 'SHOW REPLICA STATUS\G' | awk -F': *' '
        /^[ ]*(Source_Host|Source_Port|Source_User|Replica_IO_Running|Replica_SQL_Running|SQL_Delay|Auto_Position):/ {
            gsub(/^[ ]+/, "", $1)
            print $1 "=" $2
        }'
}

field() {
    printf '%s\n' "$STATUS" | awk -F= -v key="$1" '$1 == key { print $2; exit }'
}

# read_lag prints Seconds_Behind_Source verbatim: "0" when caught up, a positive
# number while applying, or "NULL" when the applier is not running. Anything
# other than "0" means not caught up.
read_lag() {
    "$QUERY" "$REPLICA_PORT" 'SHOW REPLICA STATUS\G' | awk -F': *' '
        /^[ ]*Seconds_Behind_Source:/ { print $2; exit }'
}

# wait_for_catchup blocks until the replica reports zero lag, so that --setup
# hands back an estate that is actually usable rather than one that is merely
# connected. A test that starts while db3 is still replaying Sakila measures
# catch-up lag and calls it replication lag.
#
# Zero is required on TWO consecutive polls. Seconds_Behind_Source is derived
# from the timestamp of the last APPLIED event, so it can read 0 for an instant
# when the applier has drained what it has and the receiver has not yet
# delivered the next batch -- a single zero reading is not proof of catch-up.
wait_for_catchup() {
    local waited=0 stable=0 lag

    while [ "$waited" -lt "$CATCHUP_TIMEOUT_SECONDS" ]; do
        lag="$(read_lag)" || return 1
        if [ "$lag" = "0" ]; then
            stable=$((stable + 1))
            [ "$stable" -ge 2 ] && return 0
        else
            stable=0
        fi
        sleep "$POLL_INTERVAL_SECONDS"
        waited=$((waited + POLL_INTERVAL_SECONDS))
    done

    echo "setup-replication.sh: the replica did not catch up within ${CATCHUP_TIMEOUT_SECONDS}s" >&2
    echo "  (last Seconds_Behind_Source: ${lag:-<absent>}). The estate is attached but behind." >&2
    return 1
}

# expectations, as key=value in the same shape read_status emits
expectations() {
    cat <<EOF
Source_Host=$SOURCE_HOST_INTERNAL
Source_Port=$SOURCE_PORT_INTERNAL
Source_User=$REPL_USER
Replica_IO_Running=Yes
Replica_SQL_Running=Yes
SQL_Delay=0
Auto_Position=$WANT_AUTO_POSITION
EOF
}

# compare_status prints one "  <field>: expected X, actual Y" line per mismatch
# and returns 1 if there was any.
compare_status() {
    local mismatches=0 key want got
    while IFS='=' read -r key want; do
        got="$(field "$key")"
        if [ "$got" != "$want" ]; then
            printf '  %-22s expected %-8s actual %s\n' "$key:" "$want" "${got:-<absent>}" >&2
            mismatches=1
        fi
    done < <(expectations)
    return "$mismatches"
}

STATUS="$(read_status)" || exit 1

if [ -n "$STATUS" ]; then
    if compare_status; then
        wait_for_catchup || exit 1
        echo "setup-replication.sh: db3 is already replicating from ${SOURCE_HOST_INTERNAL}:${SOURCE_PORT_INTERNAL} (${REPL_POSITIONING}) and caught up — nothing to do."
        exit 0
    fi

    cat >&2 <<EOF

setup-replication.sh: db3 already has a replication config, and it DIVERGES
from what this estate requires (mismatches above).

This script does not repair it. Re-pointing a live replica would silently
discard whatever produced the current state — a lag scenario mid-flight, a
deliberately stopped applier, or a different source.

Decide what the current state is, then either fix it deliberately or reset it:

    tests/scripts/mysql-query.sh ${REPLICA_PORT} "STOP REPLICA;"
    tests/scripts/mysql-query.sh ${REPLICA_PORT} "RESET REPLICA ALL;"
    tests/scripts/setup-replication.sh

(The remediation above discards db3's replication config. It does not touch
its data.)
EOF
    exit 1
fi

# --- attach -----------------------------------------------------------------

if [ "$REPL_POSITIONING" = "snapshot" ]; then
    cat >&2 <<EOF
setup-replication.sh: REPL_POSITIONING=snapshot, but db3 has no replication
config and this script implements only the GTID attach.

A snapshot attach must seed db3 from a db1 dump and then supply the matching
binary-log coordinates; performing it with SOURCE_AUTO_POSITION=0 and no seed
would replicate onto divergent data. Seed db3 and attach it by hand, or set
REPL_POSITIONING=gtid (this estate has gtid_mode=ON on both servers).
EOF
    exit 1
fi

echo "setup-replication.sh: attaching db3 -> ${SOURCE_HOST_INTERNAL}:${SOURCE_PORT_INTERNAL} (${REPL_POSITIONING})..."

# Host scope '%' because compose network addresses are dynamic. Narrowing it is
# a later operator call, not a default this script should make.
"$QUERY" "$SOURCE_PORT" \
    "CREATE USER IF NOT EXISTS '${REPL_USER}'@'%' IDENTIFIED BY '${REPL_PASSWORD}';" || exit 1
"$QUERY" "$SOURCE_PORT" \
    "GRANT REPLICATION SLAVE ON *.* TO '${REPL_USER}'@'%';" || exit 1

"$QUERY" "$REPLICA_PORT" "CHANGE REPLICATION SOURCE TO \
SOURCE_HOST='${SOURCE_HOST_INTERNAL}', \
SOURCE_PORT=${SOURCE_PORT_INTERNAL}, \
SOURCE_USER='${REPL_USER}', \
SOURCE_PASSWORD='${REPL_PASSWORD}', \
GET_SOURCE_PUBLIC_KEY=1, \
SOURCE_AUTO_POSITION=1;" || exit 1

"$QUERY" "$REPLICA_PORT" "START REPLICA;" || exit 1

# --- wait for both threads --------------------------------------------------

waited=0
while [ "$waited" -lt "$ATTACH_TIMEOUT_SECONDS" ]; do
    STATUS="$(read_status)" || exit 1
    if [ "$(field Replica_IO_Running)" = "Yes" ] && [ "$(field Replica_SQL_Running)" = "Yes" ]; then
        break
    fi
    sleep "$POLL_INTERVAL_SECONDS"
    waited=$((waited + POLL_INTERVAL_SECONDS))
done

STATUS="$(read_status)" || exit 1

if ! compare_status; then
    cat >&2 <<EOF

setup-replication.sh: the replica did not reach the expected state within
${ATTACH_TIMEOUT_SECONDS}s (mismatches above). Full status:
EOF
    # SHOW REPLICA STATUS does not expose the replication password.
    "$QUERY" "$REPLICA_PORT" 'SHOW REPLICA STATUS\G' >&2
    exit 1
fi

wait_for_catchup || exit 1

echo "setup-replication.sh: db3 is replicating from ${SOURCE_HOST_INTERNAL}:${SOURCE_PORT_INTERNAL} (${REPL_POSITIONING}); both threads running, caught up."
exit 0
