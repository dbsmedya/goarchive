#!/usr/bin/env bash
#
# Refuse to start a gate against a dead test estate.
#
# WHY THIS EXISTS
#
# A killed or interrupted earlier run leaves the containers `Exited (137)`.
# Every subsequent step then fails with "Can't connect to MySQL server", which
# reads exactly like a real product failure and has sent verifiers chasing one.
#
# Note that selftest_get_row_count_fails_loud CANNOT catch this: it deliberately
# probes an always-refused port to prove the helper reports failure, so it
# passes happily while the real estate is down.
#
# Checks the ports the suite actually uses, not `docker compose ps` --
# a container can be Up while MySQL inside it is still starting. The plain estate has three (source,
# destination, replica); with TOPOLOGY=group the group estate's two other members are checked too,
# and an environment that is not the group's is refused, because the plain ports answer while the
# plain estate is up and a check on them would pass against the wrong estate.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=topology.sh
. "$SCRIPT_DIR/topology.sh"

SOURCE_PORT="${TEST_SOURCE_PORT:-3305}"
DEST_PORT="${TEST_DEST_PORT:-3307}"
REPLICA_PORT="${TEST_REPLICA_PORT:-3308}"

if [[ -z "${MYSQL_ROOT_PASSWORD:-}" ]]; then
    echo "ERROR: MYSQL_ROOT_PASSWORD is not set." >&2
    echo "  Run: set -a; source tests/.env; set +a" >&2
    exit 1
fi

specs=("$SOURCE_PORT:source" "$DEST_PORT:destination" "$REPLICA_PORT:replica")
PORTS="${SOURCE_PORT}, ${DEST_PORT}, ${REPLICA_PORT}"
if [[ "$TOPOLOGY" == group ]]; then
    if [[ -z "${TEST_GROUP_MEMBER2_PORT:-}" || -z "${TEST_GROUP_MEMBER3_PORT:-}" ]]; then
        echo "ERROR: TOPOLOGY=group, but the group estate's environment is not loaded." >&2
        echo "  Run: set -a; source tests/.env.group; set +a" >&2
        exit 1
    fi
    specs+=("$TEST_GROUP_MEMBER2_PORT:member 2" "$TEST_GROUP_MEMBER3_PORT:member 3")
    PORTS="${PORTS}, ${TEST_GROUP_MEMBER2_PORT}, ${TEST_GROUP_MEMBER3_PORT}"
fi

down=()
for spec in "${specs[@]}"; do
    port="${spec%%:*}"
    role="${spec#*:}"
    if ! MYSQL_QUERY_HOST=127.0.0.1 MYSQL_QUERY_USER=root \
        "$SCRIPT_DIR/mysql-query.sh" "$port" "SELECT 1;" >/dev/null 2>&1; then
        down+=("$role (port $port)")
    fi
done

if [[ ${#down[@]} -gt 0 ]]; then
    echo "ERROR: the test estate is not reachable." >&2
    echo "" >&2
    for d in "${down[@]}"; do
        echo "  DOWN: $d" >&2
    done
    echo "" >&2
    echo "  Every gate step would now fail with 'Can't connect to MySQL server'," >&2
    echo "  which looks like a product failure but is not." >&2
    echo "" >&2
    echo "  Fix:  make test-up          (start them)" >&2
    echo "        make test-status      (see what docker thinks)" >&2
    echo "        make test-reset       (if a killed run left Exited (137))" >&2
    [[ "$TOPOLOGY" == group ]] && echo "        (all three with TOPOLOGY=group)" >&2
    exit 1
fi

echo "test estate reachable on ${PORTS}"
