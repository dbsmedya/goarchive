# GoArchive tests — agent automation

[`tests/README.md`](README.md) is the single source of truth for how goarchive's tests are
defined and run. It is written for people. This file only says how an agent carries that out:
which commands to run, in which order, and what to report. It defines no test, layer or
method of its own. Where it names a command or a rule, `tests/README.md` is the authority; if
the two ever disagree, `tests/README.md` wins and this file is corrected.

This file is also the gate instructions file named by `CLAUDE.md`, and the owner of the
measured baselines below.

## Before any database command

For single-layer work only. A gate run skips this section: the gate checks the estate in its
own `estate` stage, and a down estate is a RED gate to report, not something to fix.

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

You are given one SHA. Run these steps in order from the repository root. Where a step says
stop, report `OVERALL: BLOCKED` with the reason and do nothing further.

**Run `make gate` exactly once, and run no command other than the ones below.** Never run
`make test-up`, `docker`, another `make` target or a second `make gate`, even when the gate
fails because a server is down. A failed gate is the result: report it as `RED`.

1. **Check the tree.** Run:

   ```bash
   git rev-parse HEAD
   git diff --quiet && git diff --cached --quiet
   ```

   Stop if the first line is not the given SHA, or if the second command exits non-zero
   (uncommitted changes to tracked files). Never check anything out; the tree is prepared for
   you.

2. **Check nothing else is testing.** Run:

   ```bash
   pgrep -fl '[r]un-gate\.sh|[r]un-tests\.sh|[c]heck-characterization-baseline\.sh'
   ```

   Stop if it prints anything, and name what is running. The estate is shared, and two test
   runs on it fail each other.

3. **Load the credentials and run the gate, in one command.** Give the command the Bash tool's
   maximum timeout (600000 ms). Do not add `2>&1`, and run it only once:

   ```bash
   test -f tests/.env && { set -a; . tests/.env; set +a; make gate; }
   ```

   If `tests/.env` is missing, the command exits 1 with no output: stop. Create no file.
   Otherwise, whatever the gate prints and however it exits, go to step 4. What
   `make gate` runs is described in `tests/README.md` → *`make gate` — use this rather than
   assembling the steps*.

4. **Find this run's evidence directory.** It is the path on the first output line,
   `Gate evidence directory: <dir>`. Never use any other directory under
   `tests/results/gate/`.

5. **Read the verdict.** Run:

   ```bash
   cat <dir>/run.tsv <dir>/summary.txt
   test -f <dir>/complete
   ```

   The verdict is `GREEN` only if all of these hold: the step 3 command exited 0; in
   `run.tsv`, `outcome` is `PASS`, `recording_exit` is `0`, `source_sha` is the given SHA and
   `dirty` is `clean`; and `test -f <dir>/complete` exits 0. Anything else is `RED`.

6. **Report**, in this form:

   ```text
   SHA: <given SHA>
   Run: <run_id from run.tsv>
   <each stage line from summary.txt, as printed>
   OVERALL: GREEN | RED | BLOCKED (<reason>)
   ```

   Each `summary.txt` line carries the stage's status and its counts, such as the integration
   stage's `PASS=n FAIL=n SKIP=n` (`tests/README.md` → *Test result counts*).

Change nothing: no fix, skip, relaxed test, re-run, baseline edit, file edit or commit. A red
gate is the finding; report it.

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
