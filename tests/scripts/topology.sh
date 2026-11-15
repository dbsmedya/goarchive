# shellcheck shell=bash disable=SC2034
# The facts that differ between the test estate's two topologies. Sourced, never run:
#
#   . "$SCRIPT_DIR/topology.sh"
#
# TOPOLOGY is "plain" (the default: db1 source, db2 destination, db3 replica) or "group" (gr1 the
# primary and source, gr2 and gr3 more members, gr-dest the destination, gr-replica an asynchronous
# replica of gr1; tests/README.md → The group estate). Anything else exits 2 in the sourcing script.
#
# Sets, for the topology:
#   TOPOLOGY            the topology, defaulted and checked
#   ESTATE_ENV          the env file in tests/ that holds its connection settings
#   ESTATE_ENV_EXAMPLE  the tracked example that file is created from
#   ESTATE_CONTAINERS   the names of its containers
#   OTHER_CONTAINERS    the names of the other topology's containers
#   ESTATE_COMPOSE      the arguments `docker compose` takes for it (empty for plain: tests/compose.yml,
#                       the project named after tests/, and the .env that compose reads on its own)
#
# Keep this file compatible with the system Bash 3.2 on macOS.

PLAIN_CONTAINERS="db1 db2 db3"
GROUP_CONTAINERS="gr1 gr2 gr3 gr-dest gr-replica gr-bootstrap"

TOPOLOGY="${TOPOLOGY:-plain}"
case "$TOPOLOGY" in
    plain)
        ESTATE_ENV=".env"
        ESTATE_ENV_EXAMPLE="dot.env"
        ESTATE_CONTAINERS="$PLAIN_CONTAINERS"
        OTHER_CONTAINERS="$GROUP_CONTAINERS"
        ESTATE_COMPOSE=()
        ;;
    group)
        ESTATE_ENV=".env.group"
        ESTATE_ENV_EXAMPLE="dot.env.group"
        ESTATE_CONTAINERS="$GROUP_CONTAINERS"
        OTHER_CONTAINERS="$PLAIN_CONTAINERS"
        ESTATE_COMPOSE=(-f compose.group.yml --env-file "$ESTATE_ENV")
        ;;
    *)
        echo "TOPOLOGY must be plain or group, not '$TOPOLOGY'" >&2
        exit 2
        ;;
esac
