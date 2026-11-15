#!/usr/bin/env bash
#
# Start, stop, list or reset the test estate, in the topology TOPOLOGY names (tests/README.md → The
# group estate). `make test-up`, `test-down`, `test-status` and `test-reset` call it, and so does the
# E2E harness's setup.
#
#   TOPOLOGY=plain|group tests/scripts/estate.sh up|down|status|reset
#
# up      start the estate's containers (group: and wait for its members' health checks, which is the
#         wait for the bootstrap)
# down    stop and remove its containers, KEEPING its volumes
# status  list its containers
# reset   remove its containers AND volumes, then up: empty data directories
#
# The plain topology is `docker compose` in tests/ exactly as the Makefile ran it before this script
# existed: no file, project or env-file argument. The group topology runs its own compose file, env
# file and project name, so `down -v` on one estate never reaches the other's containers or volumes.
#
# `up` and `reset` REFUSE while a container of the other topology is running, found by its exact name
# (a port collision is not the check: the two estates' ports are disjoint on purpose). `down` and
# `status` are never refused. A refusal runs nothing.
#
# Exit codes: 0 done; 1 refused or docker failed; 2 unknown topology or action.
# Keep this script compatible with the system Bash 3.2 on macOS.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTS_DIR="$(dirname "$SCRIPT_DIR")"

# shellcheck source=topology.sh
. "$SCRIPT_DIR/topology.sh"

usage() {
    echo "usage: TOPOLOGY=plain|group $0 up|down|status|reset" >&2
    exit 2
}

[ $# -eq 1 ] || usage
ACTION="$1"
case "$ACTION" in
    up | down | status | reset) ;;
    *) usage ;;
esac

# compose runs `docker compose` for this topology, in tests/.
compose() {
    (cd "$TESTS_DIR" && docker compose ${ESTATE_COMPOSE[@]+"${ESTATE_COMPOSE[@]}"} "$@")
}

# refuse_if_other_is_up exits 1, naming the containers, when the other topology has one running.
refuse_if_other_is_up() {
    local running name found=""
    if ! running="$(docker ps --format '{{.Names}}')"; then
        echo "estate: 'docker ps' failed; cannot tell whether the other estate is up" >&2
        exit 1
    fi
    for name in $OTHER_CONTAINERS; do
        if printf '%s\n' "$running" | grep -qx -- "$name"; then
            found="$found $name"
        fi
    done
    if [ -n "$found" ]; then
        echo "estate: refusing to $ACTION the $TOPOLOGY estate: the other estate is up (${found# })." >&2
        echo "  Only one estate runs at a time. Stop the other one first:" >&2
        if [ "$TOPOLOGY" = group ]; then
            echo "      make test-down                     (the plain estate; keeps its volumes)" >&2
        else
            echo "      make test-down TOPOLOGY=group      (the group estate; keeps its volumes)" >&2
        fi
        exit 1
    fi
}

# ensure_env creates the group topology's env file from its tracked example, as run-tests.sh does for
# tests/.env: compose reads it for every action, and the example's values work with `make test-up`.
# The plain topology's .env is read by compose itself and is not created here.
ensure_env() {
    [ "$TOPOLOGY" = group ] || return 0
    [ -f "$TESTS_DIR/$ESTATE_ENV" ] && return 0
    if [ ! -f "$TESTS_DIR/$ESTATE_ENV_EXAMPLE" ]; then
        echo "estate: neither tests/$ESTATE_ENV nor the template tests/$ESTATE_ENV_EXAMPLE exists." >&2
        exit 1
    fi
    echo "NOTE: tests/$ESTATE_ENV did not exist. Creating it from $ESTATE_ENV_EXAMPLE." >&2
    cp "$TESTS_DIR/$ESTATE_ENV_EXAMPLE" "$TESTS_DIR/$ESTATE_ENV" || exit 1
}

UP_ARGS=(up -d)
[ "$TOPOLOGY" = group ] && UP_ARGS=(up -d --wait --wait-timeout 300)

case "$ACTION" in
    up)
        refuse_if_other_is_up
        ensure_env
        compose "${UP_ARGS[@]}"
        ;;
    down)
        ensure_env
        compose down
        ;;
    status)
        ensure_env
        compose ps
        ;;
    reset)
        refuse_if_other_is_up
        ensure_env
        compose down -v || exit $?
        compose "${UP_ARGS[@]}"
        ;;
esac
