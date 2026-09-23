# GoArchive tests — agent automation

[`tests/README.md`](README.md) is the single source of truth for how goarchive's tests are
defined and run. It is written for people. This file only says how an agent carries that out:
which commands to run, in which order, and what to report. It defines no test, layer or
method of its own. Where it names a command or a rule, `tests/README.md` is the authority; if
the two ever disagree, `tests/README.md` wins and this file is corrected.

This file is also the gate instructions file named by `CLAUDE.md`, and the owner of the
measured baselines below.

## Before any database command

1. Check the containers are up: `docker ps` shows ports 3305, 3307 and 3308. If not, run
   `make test-up`. If a connection still fails, stop and ask the operator.
2. In the same shell, load the credentials:

   ```bash
   set -a; source tests/.env; set +a
   ```

3. Query a database only through the wrapper, never `mysqlsh` directly
   (`tests/README.md` → *Querying the test databases*):

   ```bash
   tests/scripts/mysql-query.sh <port> "<sql>"
   ```

| Server | Host | Port | Database |
|--------|------|------|----------|
| Source | 127.0.0.1 | 3305 | sakila |
| Archive | 127.0.0.1 | 3307 | (destination) |
| Replica | 127.0.0.1 | 3308 | (replication-lag tests) |

The schema, the replica topology and the `+03:00` destination are described in
`tests/README.md` → *Prerequisites*.

## Running the gate

1. Run `make gate` from the repository root, as `tests/README.md` → *`make gate` — use this
   rather than assembling the steps* describes. Do not assemble the stages by hand, and do not
   pipe the output through `2>&1`.
2. Read this run's verdict from its evidence directory, as that same section describes. Never
   use a log from an earlier run.
3. Report the candidate SHA, the run's per-stage status, the `PASS=n FAIL=n SKIP=n` counts per
   layer (`tests/README.md` → *Test result counts*), and `OVERALL: GREEN` only if the run
   completed with every stage passing; otherwise `OVERALL: RED`.
4. Change nothing: no fix, skip, relaxed test, re-run to get a pass, baseline edit or commit.

CI runs only the gate's non-database stages (static checks and unit tests). A green CI is not a
green gate; only this run's `OVERALL: GREEN` is.

## Running a single layer

Use these only when a task asks for one layer; the gate runs them all in the right order.

| Layer | Command | `tests/README.md` section |
|-------|---------|---------------------------|
| Unit (no database) | `go test ./... -count=1` | *Unit tests* |
| Integration | `bash tests/scripts/run-tests.sh --setup --integration-only` | *Integration tests* |
| Characterization | `make characterization` | *The characterization baseline is checked, not recited* |
| E2E (Sakila) | `make e2e` | *Sakila E2E tests* |
| Validation demos | `make e2e-examples` (needs a seeded estate) | *Validation demos — preflight MUST fail* |

`make e2e` is the whole E2E procedure (reset, seed, run). Run its individual steps only when a
task says why.

## Measured baselines

Measured by their runners, never calculated from a diff. The coordinator updates this section
after the runner reports; a gate agent never edits it.

- **Integration inventory:** `PASS=1361 FAIL=0 SKIP=17`, measured by the integration runner on
  2026-09-11 for the 2.2.2 release candidate. Sixteen skips need disposable matrix profiles;
  the existing `TestExecute_CheckpointCallbackError` skip remains. Re-measure after adding or
  removing tagged tests.
- **Characterization expectation:** `58 / 287 / 345 / 0 / 0` (top-level / subtests / PASS /
  FAIL / SKIP), held in `tests/characterization-baseline.txt` and checked by
  `make characterization`. It changes only with the operator's prior authorization; change the
  file and this line together. Never count it by hand; the script does the counting.
