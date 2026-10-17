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

**Start the gate exactly once, and run no command other than the ones below.** Never run
`make test-up`, `docker`, another `make` target, `make gate` itself or a second
`gate-start.sh`, even when the gate fails because a server is down. A failed gate is the result:
report it as `RED`.

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
   pgrep -fl '[r]un-gate\.sh|[r]un-tests\.sh'
   ```

   Stop if it prints anything, and name what is running. The estate is shared, and two test
   runs on it fail each other.

3. **Start the gate, once.** It runs detached, with `tests/.env` loaded, so it does not have
   to fit one tool call. Run only once:

   ```bash
   bash tests/scripts/gate-start.sh
   ```

   If it exits 1 naming `tests/.env`, stop: create no file. If it exits 1 otherwise, stop and
   report its output. On exit 0, its output names the run: `Gate evidence directory: <dir>`
   (and the console file). Never use any other directory under `tests/results/gate/`. What
   `make gate` runs is described in `tests/README.md` → *`make gate` — use this rather than
   assembling the steps*.

4. **Wait for it.** Give each call a 600000 ms timeout:

   ```bash
   bash tests/scripts/gate-wait.sh <dir>
   ```

   - Exit 3: still running. Run the same command again, unless it printed `elapsed:` of 1800 s
     or more: then stop with `BLOCKED (gate still running)`, naming `<dir>`, and start nothing.
   - Exit 4: `run.tsv` says `RUNNING` but no gate process is alive. Stop with
     `BLOCKED (hung run)`.
   - Exit 2: stop with `BLOCKED` and its output.
   - Exit 0: the run has finished; go to step 5 with its `outcome:` and `duration:` lines.

5. **Read the verdict.** Run:

   ```bash
   cat <dir>/run.tsv <dir>/summary.txt
   test -f <dir>/complete
   ```

   The verdict is `GREEN` only if all of these hold: in `run.tsv`, `outcome` is `PASS`,
   `command_exit` and `recording_exit` are `0`, `source_sha` is the given SHA, `dirty` is `clean`
   and `part` is `all`; `test -f <dir>/complete` exits 0; and step 4's duration is at most 900 s.
   A run that is green except for its duration is `RED (over the 900 s ceiling)`. Anything else
   is `RED`.

6. **Report**, in this form:

   ```text
   SHA: <given SHA>
   Run: <run_id from run.tsv>
   Duration: <seconds from step 4> s (ceiling 900 s)
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
| E2E (Sakila) | `make e2e` | *Sakila E2E tests* |
| Validation demos | `make e2e-examples` (needs a seeded estate) | *Validation demos — preflight MUST fail* |

`make e2e` is the whole E2E procedure (reset, seed, run). Run its individual steps only when a
task says why.

The full environment matrix and the gate on every supported image run on GitHub, not here
(`tests/README.md` → *The full matrix runs on GitHub*, which says when each runs). The coordinating
agent, not the gate agent, pushes `verify/<sha7>` on every PR's final SHA and `verify-gate/<sha7>`
on a release PR's, confirms each run started, reads its summary table and deletes the branch.

## Measured baselines

Measured by their runners, never calculated from a diff. The coordinator updates this section
after the runner reports; a gate agent never edits it.

- **Integration inventory:** `PASS=1347 FAIL=0 SKIP=5`, measured by the integration runner
  (`tests/scripts/run-tests.sh --setup --integration-only`) on 2026-10-16 (the beta.2 PRC-1 PR, at
  `51154dc`). The runner counts every test the `integration` tag compiles, unit tests included:
  PRC-1 added `TestEstimateRoots` (28 with its subtests) and
  `TestMySQLProgress/an_estimate,_not_a_count`. It was 1318 before (the beta.2 PKT-1 PR, at
  `0b5b0f9`), and `SKIP=4` before that (the beta.2 TRK-1 PR, at `b53d157`): PKT-1 added
  `TestMySQLArchiveValueLargerThanThePacket`, which runs only under the matrix row
  `max_allowed_packet_dst_4m` and skips on the estate, whose two servers have the same packet.
  Before TRK-1 it was 1306 (the beta.2 LAG-1 PR, at `54e7b3f`): TRK-1 added four
  `internal/tracking` schema-lock tests and `TestSchemaLockedErrorMessage`, and in
  `internal/lock` `TestMySQLWait` (5 rows) and `TestMySQLWaitUnderAStatementTimer`. Re-measure
  after adding or removing tagged tests.
- **Gate duration:** the whole gate must finish within 900 s (15 min), a ceiling the verdict
  checks from `run.tsv` (step 5; RVG-3, beta.3). Until then it had to fit one 600-second Bash call,
  a limit set by the tool, not the gate. The first measured run took
  443 s. Later gates took 450–472 s (466 s at `e4ef55d`, run `20261014T201816Z-82233`, on this
  inventory), and one under host load 515 s, leaving about 85 s; 430 s at `8030861` (run
  `20261015T115700Z-38140`, 1279 inventory); 416–421 s on the 1281 inventory (runs
  `20261015T122959Z-91592` and `20261015T140444Z-16700`, PR #136); 453–482 s on the 1301 inventory
  (runs `20261015T175855Z-14594`, `20261015T183440Z-34044`, `20261015T193313Z-64295`, the LAG-1 PR);
  476 s on the 1306 inventory (run `20261015T201205Z-26156`, LAG-1's final SHA); 488–498 s on the
  1318 inventory (run `20261016T102019Z-33223`, 498 s, TRK-1's final SHA); 445–452 s with the
  validation demos 18 and 19 (runs `20261016T122705Z-2588` and `20261016T123636Z-27187`, TRG-1);
  504–547 s with the packet row's test (run `20261016T152044Z-29845`, 504 s, PKT-1's final SHA;
  `a0c2c4b` 505 s; `3225cab` 547 s under host load); 502–546 s with the verify split (run
  `20261016T162237Z-37397`, 502 s, RVG-1a+b's final SHA; run `20261016T155024Z-12353`, 546 s
  under host load); 518 s on the 1341 inventory (run `20261016T201612Z-86110`, PRC-1 at
  `80e2f41`); 570 s on the 1347 inventory (run `20261016T203025Z-19981`, PRC-1's final SHA, with
  another engine test run on the machine); record each new measurement
  here, since the margin shrinks as tests are added.
- **Release verify on GitHub** (beta.1; split in beta.2 into *Matrix verify* and *Gate verify*):
  the first green run, 38602481533 on 2026-10-14 (beta.1 PR 4, at `ae06474`), took 19 minutes for
  41 jobs. Gates took 15–18 min each, with integration
  `PASS=1209 FAIL=0 SKIP=5` on all six images; that is the local inventory less
  `TestSetupGracefulShutdown_TwoPhase`, which skips where `CI=true`. Matrix rows took 1–3 min each
  against `MX_TIMEOUT` 6m; 33/33 OK. The first split runs, at `770df69` (beta.2 RVG-1a+b): *Matrix
  verify* 38931438357, 36 jobs, 34/34 rows OK, 8 min 18 s; *Gate verify* 38931452843, 8 jobs, 6/6
  gates certified (integration `PASS=1317 FAIL=0 SKIP=6` on each image), 21 min 32 s.
