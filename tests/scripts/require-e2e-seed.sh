#!/bin/bash
#
# Refuse to run an E2E suite unless `make e2e-setup` seeded the estate.
#
# CORRECTED 2026-08-02. This guard originally claimed that an integration run
# leaves source Sakila drained, so a later `make e2e` would archive from an
# empty database and report a meaningless pass. That is FALSE, and it was
# measured false: STEP 1 of every E2E test calls reset_source_database(), which
# reloads sakila-schema.sql and sakila-data.sql, and ensure_destination_schema()
# drops and reloads the destination per test. Run directly against an estate
# drained to 5,868 rentals, test 03 still reported `payment (before) = 16044`
# and both tests passed. The E2E suite reseeds itself and is immune.
#
# What the marker actually buys is a deterministic starting point: `make e2e`
# rebuilds the containers from empty volumes before seeding, so a run cannot
# inherit MySQL-level state (variables, users, grants, leftover schemas) that no
# per-test reset touches. That is worth having for a release gate. It is not a
# correctness guard against drained data, and must not be described as one.
#
# Exit 0 = seeded, exit 1 = not seeded (message on stderr).

set -uo pipefail

MARKER="$(cd "$(dirname "$0")/.." && pwd)/.e2e-ready"

if [[ -f "$MARKER" ]]; then
    exit 0
fi

cat >&2 <<'EOF'

E2E REFUSED — the estate is not seeded.

  Run the suites in this order, every time:

      1)  make e2e-setup
      2)  make e2e
      3)  make test-reset

  `make e2e` on its own does not bootstrap anything. Step 1 rebuilds the
  containers from empty volumes, so the run starts from a known MySQL state
  rather than inheriting whatever previous runs left behind.

  You are here because step 1 has not run, or because something invalidated the
  seed since it did. Run step 1.

EOF
exit 1
