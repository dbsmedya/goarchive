#!/bin/bash
#
# Run one SQL statement against a test database, so that a FAILURE CAN NEVER BE
# MISTAKEN FOR AN EMPTY RESULT.
#
# Usage:
#   tests/scripts/mysql-query.sh <port> "<sql>"
#
#   port    3305 (source) | 3307 (archive) | 3308 (replica)
#   sql     one or more statements, passed to mysqlsh --sql -e
#
# Contract:
#   stdout      query results ONLY (empty means genuinely zero rows)
#   stderr      diagnostics ONLY (empty on success)
#   exit 0      the query ran and its result is on stdout
#   exit 1      the query failed — auth, connectivity, or SQL error
#   exit 2      the environment is not loaded (MYSQL_ROOT_PASSWORD unset)
#
# So a residue check is simply:
#
#   rows=$(tests/scripts/mysql-query.sh 3307 "SELECT job_name FROM ...") || exit 1
#   [ -z "$rows" ] && echo "clean"
#
# ---------------------------------------------------------------------------
# Why this script exists (all measured on mysqlsh 8.4.10, 2026-08-02)
#
# The standing warning was that an auth failure is "indistinguishable from an
# empty result". Measured, that is FALSE — they differ on two independent
# signals:
#
#   case                      exit  stdout        stderr
#   wrong password              1   empty         MySQL Error 1045 Access denied
#   genuinely empty result      0   empty         (empty)
#   rows returned               0   the rows      (empty)
#   SQL error                   1   empty         the error
#   server down                 1   empty         the error
#
# The two become indistinguishable only when a caller merges the streams with
# `2>&1` and ignores the exit code — which is exactly what the documented
# residue checks used to do. This script never merges them.
#
# --no-defaults is NOT optional. Without it mysqlsh reads ~/.my.cnf, and a
# workstation with
#
#     [client]
#     user=root
#     password=...
#     host=127.0.0.1
#
# silently supplies credentials when none were given. Omitting the password
# then SUCCEEDS locally and fails in CI, which is strictly worse than the
# failure this script is guarding against: the check passes for a reason the
# author never intended. MYSQL_PWD is ignored by mysqlsh entirely — do not rely
# on it.
#
# --force is deliberately NOT passed. It masks SQL errors (exit 0 with the
# error still on stderr). It does not mask auth failures, but there is no
# reason to accept the weaker guarantee.
#
# --quiet-start=2 suppresses the "Using a password on the command line
# interface can be insecure" warning, so a non-empty stderr always means
# something actually went wrong.
# ---------------------------------------------------------------------------

set -uo pipefail

PORT="${1:-}"
SQL="${2:-}"

if [[ -z "$PORT" || -z "$SQL" ]]; then
    echo "usage: $0 <port> \"<sql>\"" >&2
    echo "       port: 3305 (source) | 3307 (archive) | 3308 (replica)" >&2
    exit 2
fi

HOST="${MYSQL_QUERY_HOST:-127.0.0.1}"
USER="${MYSQL_QUERY_USER:-root}"

if [[ -z "${MYSQL_ROOT_PASSWORD:-}" ]]; then
    cat >&2 <<EOF
mysql-query.sh: MYSQL_ROOT_PASSWORD is not set — refusing to run.

  Without it mysqlsh may fall back to ~/.my.cnf and connect as somebody else,
  so the result would not mean what you think it means.

  Load the test environment first:

      set -a; source tests/.env; set +a

  If tests/.env does not exist yet:

      cp tests/dot.env tests/.env        # then edit it

  Reference: tests/README.md § Prerequisites
EOF
    exit 2
fi

STDERR_FILE="$(mktemp)"
trap 'rm -f "$STDERR_FILE"' EXIT

# Results to stdout, diagnostics captured separately. Never merge the two.
mysqlsh \
    --no-defaults \
    --quiet-start=2 \
    --host="$HOST" \
    --port="$PORT" \
    --user="$USER" \
    --password="$MYSQL_ROOT_PASSWORD" \
    --sql \
    -e "$SQL" \
    2>"$STDERR_FILE"
rc=$?

# Belt and braces: a non-zero exit is the primary signal, but scan the
# diagnostics too. This is what would catch a future caller that adds --force,
# under which mysqlsh reports a SQL error on stderr and still exits 0.
if [[ $rc -ne 0 ]] || grep -qE '^(ERROR|MySQL Error)[[:space:]:]' "$STDERR_FILE"; then
    {
        echo "mysql-query.sh: query FAILED against ${HOST}:${PORT} (exit ${rc})"
        echo "--- mysqlsh diagnostics ---"
        cat "$STDERR_FILE"
        echo "---------------------------"
        echo "This is a FAILED check. It is not an empty result."
    } >&2
    exit 1
fi

exit 0
