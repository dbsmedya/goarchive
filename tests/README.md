# GoArchive Test Suite

This directory is the **source of truth** for running GoArchive's unit,
integration, and Sakila end-to-end (E2E) tests.

## Overview

| Test Type | Description | Command |
|-----------|-------------|---------|
| **Everything** | **The full gate, in the only correct order** | **`make gate`** |
| **Unit** | Fast, in-memory; no DB required. Preflight stages consume injected `dbsgomysql` facts — see [Testing Reference](../docs/README_TESTING.md#unit-tests) | `go test ./... -count=1` |
| **Integration** | Real-DB tests behind the `integration` build tag | `make test-integration`, or `./scripts/run-tests.sh --setup --integration-only` |
| **Sakila E2E (working)** | Archive, purge, copy-only, interrupt/resume and control-socket runs that complete (tests 01, 03–09, 13–17 and 20) | `make e2e` (reset + seed + run) |
| **Sakila E2E (demos)** | Configs that intentionally fail preflight (tests 02, 10, 11, 12, 18, 19) | `make e2e-examples` |

### `make gate` — use this rather than assembling the steps

It runs: estate reachability → `fmt-check` `vet` `lint` `consumer-policy` `deadcode` → unit →
integration (`--setup`) → `make e2e` → `make e2e-examples`, and ends with a
summary:

Unit-test ownership rules and typed-fake examples live in
[`docs/README_TESTING.md`](../docs/README_TESTING.md); coverage is measured on demand
([Coverage on demand](#coverage-on-demand)).

Each invocation prints its unique directory beneath `tests/results/gate/` before
checking credentials. Earlier runs remain available and are never reused as current evidence.
The integration stage retains verbose Go output so individual PASS, FAIL and SKIP
results remain available beside the runner summary.

| File in the run directory | What it records |
|---|---|
| `run.tsv` | Run ID, source revision, tracked-tree state, start/end, outcome/exit information and the part (`all` for `make gate`) |
| `summary.tsv` | All ten stages, their status, command/capture exits and current log paths |
| `summary.txt` | The same stage verdicts in a readable form |
| `<stage>.log` | Output from a stage that actually started in this invocation |
| `complete` | Success marker with run ID, revision, part and `GATE COMPLETE - every stage above exited 0` |

A failed gate stops at the failing stage. Later stages are explicitly `NOT RUN`;
they cannot inherit a previous run's PASS or log. A live stage is `RUNNING`; an
interrupted run without a matching completion record is incomplete, never green.

The runner checks both the command and log-capture exit statuses. A failed command
keeps its own exit code; a failed capture or evidence publication also fails the
gate. The `make` wrapper may translate that failure into its own nonzero exit.
Evidence files are published atomically. If storage is unwritable, stderr and the
nonzero process exit report the failure; no successful record is promised.

Read the current run's summary and completion record together with its process
exit. A console line or an older stage log alone does not certify the checkout.

`make gate` runs every stage. *Gate verify* runs each image's gate as two parts on two runners,
`bash tests/scripts/run-gate.sh --part integration` (estate, static checks, unit, integration)
and `--part e2e` (estate, `e2e`, `e2e-examples`). A part's other stages are `NOT RUN`, its
`run.tsv` and `complete` name the part, and `complete` reads `GATE PART COMPLETE - every stage
of part <part> exited 0`: a part is never a gate. Locally only `make gate` certifies a checkout.
`tests/scripts/gate-start.sh` and `gate-wait.sh` start `make gate` detached and wait for it,
for an agent whose tool calls are shorter than the gate (`tests/AGENTS.md`).

**The order is load-bearing, not stylistic.** `make e2e` begins with `test-reset`, which
destroys the estate; run it before integration and that fails for reasons
unrelated to your change. Integration runs after `--setup` reseeds the estate. `make gate`
encodes both so nobody has to remember them.

It fails fast on a dead estate rather than letting every step die with
`Can't connect to MySQL server`, which reads exactly like a product failure. Note the harness's
own `selftest_get_row_count_fails_loud` cannot catch that case — it probes an always-refused
port on purpose, so it passes while the real databases are down.

Credentials first, as always: `set -a; source tests/.env; set +a`. A fresh clone has no
`tests/.env` (copy `tests/dot.env`) and no `tests/sakila-db/`, which setup downloads itself.

**Run the gate and `make e2e` from the main checkout, not a worktree.** `make test-reset`
recreates db1–db3 with their `my.cnf` bind-mounted from the checkout it runs in, so a worktree
that is later removed leaves the containers pointing at files that no longer exist.

> **E2E needs a freshly reseeded estate — the #1 source of false failures.** The E2E
> tests archive Sakila into `sakila_archive`, which must start empty: every copy is a strict
> INSERT, so a prior run's rows fail the next one with `Duplicate entry` (leftover state,
> **not** a regression). They also DELETE from the source. `make e2e` reseeds first. The
> integration tests create and drop their own schemas; a few read the seeded Sakila estate, so
> run `./scripts/run-tests.sh --setup` once before running them with `go test` directly.

### Which checks a change needs

A full gate takes about nine minutes of the dedicated estate.

Every change runs `make check`, and the table below adds what its paths need. Each check runs where
it tests something the change touched, once, on the commit that survives review.

**Reviews come first.** A behaviour change gets its spec and lite plan from the Fable 5.2 agent
before the code. Each change gets Codex's MR verdict before it is merged into `dev/goarchive-3.1`.
A fix changes the commit, so the local checks run after the review: a check run before the review
is spent on a commit that does not ship. A finding from a check is fixed the same way and goes back
to Codex as a re-review of the fix alone.

**The change decides the checks.** A change needs every row that its diff from `dev/goarchive-3.1`
touches, on its final commit; a mixed change needs the union:

| The change changes | Paths | Its local checks, on the final commit |
|---|---|---|
| Documentation only | `docs/`, `*.md`, comments | `make check` (its harness checks the links and the version sites) |
| Static-check configuration | `.golangci.yml` | `make check` |
| Tests only | `*_test.go` | `make check`, and the changed packages' tests under `-tags=integration` on the estate |
| Test tooling | `tests/scripts/`, `tests/harness/`, `tests/envmatrix/`, `.github/workflows/ci.yml`, `matrix-verify.yml`, `gate-verify.yml`, the `Makefile`'s test targets | `make check`; a full local gate (`gate-agent`) when `tests/scripts/run-gate.sh` or a command it runs changed. The matrix and gate runs are the dev→main PR's (*The full matrix runs on GitHub*) |
| The estate and E2E | `tests/compose.yml`, `tests/compose.group.yml`, `tests/dot.env`, `tests/dot.env.group`, `tests/docker_files/`, `tests/e2e/`, `tests/configs/`, `tests/fixtures/`, `tests/sakila-db/` | `make check`, and the changed packages' tests under `-tags=integration` on the estate; a full local gate when `run-gate.sh` or a command it runs changed. The E2E runs are the dev→main PR's *Gate verify* |
| Product code | `internal/`, `cmd/` outside `*_test.go`, `go.mod`, `go.sum` (dependencies and the toolchain line) | `make check`, and the changed packages' tests under `-tags=integration` on the estate. The dev→main PR's *Matrix verify* and *Gate verify* runs cover the full matrix and the gate |
| Build and packaging | the `Makefile`'s build and release targets, `Dockerfile`, `.dockerignore`, `.github/workflows/release.yml`, `docker.yml` | `make check`. The dev→main PR's CI, Docker, *Gate verify* and *Matrix verify* runs, and the release procedure's artifact checks |
| A release | the version bump | `make check`. The dev→main PR that contains the bump runs CI, Docker, *Gate verify* and *Matrix verify* on its head SHA, and the release procedure's artifact checks |
| Any other path | | no exemption: the gate plan classifies it into a row above before the work goes on, and a review that finds a path the plan left out treats it as a finding |

**Baselines.** The integration inventory and gate timings in `tests/AGENTS.md` (*Measured baselines*)
are re-measured once per dev→main PR, when tests were added or removed since the last measurement:
from that PR's *Gate verify* integration counts, or from a local `gate-agent` run on its head SHA
when the operator asks.

**Each plan carries a gate plan.** It names, for each task, the check that exercises the task's
change, and the change's local checks from the table, so the checks are planned with the work.

## Sakila E2E Test Suite

Twenty focused tests: fourteen working runs — six archives, four purges, two
copy-onlys and two interrupt/resume tests — and six preflight-guardrail
demonstrations. Seventeen run on Sakila; three (14–16) run on the **key-matrix
fixture**, tables with the key types Sakila lacks.

**Each test is a declaration file under `tests/e2e/<category>/`, grouped by the
command it exercises**, with the shared harness in `tests/e2e/lib/`.
[`e2e/README.md`](e2e/README.md) documents the file format, the per-test variables
and how to add a test; each category directory has its own `README.md` for what is
peculiar to that command. This file stays the reference for *what the tests prove*
— the catalogue below, the assertion axes, the pacing formula and the estate.

**Configs live in `tests/configs/`, and the tracked file is always the
`.yaml.template`.** `run-tests.sh` renders each one to its `.yaml` on every run,
substituting the `MYSQL_ROOT_PASSWORD` placeholder from `tests/.env`. The
rendered `.yaml` holds a real password, so it is gitignored — and it is
**overwritten every run**, so edit the template, never the generated file.

**The key-matrix fixture** is `tests/fixtures/keymatrix.sql`. `lib/estate.sh`
loads it into the source `sakila` database after Sakila, at setup and at every
source reset, so the destination receives its tables from the same schema dump
as Sakila's. Its `km_*` tables reference only each other. The full cross-product
of key types and job types runs in the integration suite
(`internal/engine`, `TestMySQLKeyMatrix`); the E2E tests carry three shapes
through the real CLI.

### Working configurations — the command runs to completion

| Test | Config | Shape | What it exercises |
|------|--------|-------|-------------------|
| **01** | `test01_film_tree.yaml` | `film` → `film_text`, `film_actor`, `film_category`, `inventory` → `rental` → `payment` | The film tree, three levels deep, with the **composite-key** association tables `film_actor (actor_id, film_id)` and `film_category (film_id, category_id)`. 2.x refused it with `COMPOSITE_PK_CHECK`; 3.0 binds composite keys whole. Not a GDPR diamond: every table has one parent in the job. `film`'s DELETE trigger needs `--force-triggers`. |
| **03** | `test03_payment_batch.yaml` | `payment` (root, single-column key) | High-volume multi-batch copy→verify→delete (`batch_size=100`, `payment_id <= 2000`); `verification: count`, declared |
| **04** | `test04_rental_payment.yaml` | `rental → payment` | 2-level tree archive (`rental_id <= 200`) in one full batch; non-diamond GDPR-shaped subtree; `verification: count` |
| **05** | `test05_payment_verify_sha256.yaml` | `payment`, same slice as 03 | **`verification: sha256`**, declared explicitly — the A/B pair of 03 |
| **06** | `test06_payment_purge.yaml` | `payment`, **half the table** (`payment_id <= 8024` → 8022 of 16044 rows) | Exact-delete **`purge`** control: deletes without copying, and **verifies nothing** |
| **07** | `test07_rental_payment_copyonly.yaml` | `rental → payment`, same slice as 04 | **`copy-only`** — the command that never deletes, so the assertion that carries it is the negative one: **the source must be unchanged** |
| **08** | `test08_payment_graceful_resume.yaml` | `payment`, same config as 05, run **twice** | **Graceful stop → resume.** `SIGTERM` after batch 1; the batch completes, so **zero** non-terminal roots are left, recovery has nothing to replay, and run 2 continues the where range. |
| **09** | `test09_payment_crash_replay.yaml` | `payment`, `payment_id <= 500` → 499 rows, run **twice** | **Crash → replay.** `SIGKILL` inside batch 1's delete phase leaves its 100 roots at `copied`, so run 2's startup recovery replays them delete-only. The only test that starts from a stale `job_status=1`. |
| **13** | `test13_purge_replication.yaml` | `payment`, same selected slice as 03 (1999 selected rows, reasserted before launch) | Stops the real replica applier, observes a purge hold with unchanged rows, restarts it and requires the same process to finish; exact deletes, no copy or verification. Its replica check reads the source's `gtid_executed` on one line: a set with two uuids spans two. |
| **14** | `test14_keymatrix_composite.yaml` | `km_line (order_id, line_no)` → `km_item` → `km_item_note` | Key matrix, **archive**: a composite root, a child on the composite key, a grandchild on a three-column foreign key; composite orphan checks |
| **15** | `test15_keymatrix_binary.yaml` | `km_blob` (`BINARY(16)`) → `km_blob_ref` | Key matrix, **purge**: keys whose bytes are not valid UTF-8 |
| **16** | `test16_keymatrix_latin1.yaml` | `km_name` (`latin1` `VARCHAR`) → `km_name_tag` | Key matrix, **copy-only**: non-ASCII `latin1` keys, verified by sha256 and logged in a `latin1` key column |
| **17** | `test17_purge_replication_stop.yaml` | `payment`, the same selected slice as 13 | Stops the replica applier, observes the purge's hold, sends **one `SIGTERM`**: exit 3 within 10 s with the stop line and the stopped closing entry, the source unchanged. The hold is before the purge logs its roots, so a second purge completes with nothing to recover. |
| **20** | `test20_control_socket.yaml` | `payment`, `payment_id <= 500` → 499 rows, run **three times** | **The runtime control socket, end to end** (TEL-1). The harness (`lib/control.sh`) runs one archive job over a short private `socket_dir`: `dry-run`'s printed path is the log's and `status-json`'s; `goarchive control` reads `status` and `status-json`, pauses (state `paused`), resumes, lowers `batch_delete_size` with `-s`, reads it back with `-g` and is refused above the ceiling (exit 1); a **second start** of the job from a second config file with the same job name and `socket_dir` exits **4**; the doc's `nc -U` and `socat` lines send `status-json` and a setter; `stop` ends the run with exit **3**, its closing entry carries the runtime changes, and the socket file is gone. The rerun is **killed with SIGKILL**: `control` exits **5** with the stale-socket message and the file is left; the next run replaces the stale socket, completes, and removes it; `control` against that path exits 5. The standard assertions (sha256 verification, exact rows, no orphans) read the **final** run's own output. Needs `jq`, `nc` (OpenBSD) and `socat` |

> **03 and 05 are the same archive with different verification methods.** That is
> deliberate: a failure in 05 alone isolates to the method. Keep them in step — if
> you change one's slice or batch sizes, change the other's. 3.0's default method
> is `sha256`, so 03, 04 and 07 declare `count`.

> **Why test 06 purges half the table rather than a token slice.** Purge's
> unrecoverable failure is deleting *more* than the `where` clause selects, and a
> half-table slice makes the gap between a correct run (8022) and a runaway one
> (16044) unmistakable. The boundary is `8024`, not `8022`, because `payment_id`
> is **not contiguous** — two ids below it are missing. Both numbers were queried
> against the fixture, not computed, and both move together if the Sakila data
> file changes. That is what `expected_rows` is there to report.

> **04 and 07 are the same graph and the same slice under different commands**, so
> a failure in 07 alone isolates to `copy-only` — with one deliberate exception.
> **Their pacing differs, and that is not an oversight.** 04 sets `batch_size: 200`,
> so 200 rentals arrive in a *single* full batch, and one batch exercises one sleep
> call: no per-batch accumulation bug is reachable, because there is no second
> batch. 07 sets `batch_size: 10` for 20 batches. If you make them identical, say
> which one you meant to weaken.

> **08 and 09 run the binary twice and prove an interruption/recovery path.** After a
> successful resume the database looks exactly like it does after an uninterrupted
> run, so the obvious assertions cannot distinguish *"resumed correctly"* from
> *"never actually interrupted"* — and both sides of that window pass. What holds
> them shut is an **exact** destination count at the interrupt point, the log's
> statuses at that point, and one log-line assertion (`"Recovery: replaying"`,
> which must be absent for 08 and present for 09). `tests/e2e/resume/README.md`
> explains the mechanism in full; read it before touching either test.
>
> **05 is 08's uninterrupted control** — identical config, so a failure in 08 alone
> isolates to the interrupt. Keep them in step, as with 03 and 05.
>
> **09's `delete_sleep_seconds: 2.0` is the test, not tuning.** The kill has to land
> between a batch's destination commit, which logs its roots `copied`, and its
> completion, and the only part of that interval with any duration is the throttle
> between delete statements; a wide throttle keeps the window open. Its slice is smaller
> (499 rows) because that throttle costs 8 s per batch. How the harness times the kill:
> `e2e/resume/README.md`; how it accounts for the batch the kill lands in: the comments on
> `assert_interrupt_preconditions` in `e2e/lib/interrupt.sh`.

#### The pacing floor — the one timing number that is not machine-dependent

Wall-clock varies with the container, the host and the filesystem. The **sleep**
component does not: it is arithmetic over the config, and no machine can go below
it. Each working test declares that floor as `min_duration`, and the runner fails
the test if the run came in under it.

```
floor = (full batches) × sleep_seconds
      + Σ over batches of max(0, delete statements in the batch, across all tables − 1) × delete_sleep_seconds
```

`sleep_seconds` fires after every **full** batch — one that fetched `batch_size`
roots. A short last batch ends the run without a sleep; a full last batch is
followed by one more fetch that finds nothing, so it sleeps. `delete_sleep_seconds`
separates consecutive delete statements of a batch, so 5 statements mean 4 pauses.
(2.x slept after every batch including a short last one, so five floors are one
`sleep_seconds` lower on 3.0: 03 and 05 from 20.0, 06 from 16.2, 08 from 19.0, 09
from 16.8.)

| Test | rows | `batch_size` | full + last | batch sleep | delete sleep | **floor** |
|---|---|---|---|---|---|---|
| **01** | 50 films | 25 | 2 + 0 | 2 × 0.5 = 1.0 s | 0 *(default)* | **1.0 s** |
| **03** | 1999 | 100 | 19 + 99 | 19 × 0.2 = 3.8 s | 80 × 0.2 = 16.0 s | **19.8 s** |
| **04** | 200 | 200 | 1 + 0 | 1 × 1.0 = 1.0 s | 0 *(default)* | **1.0 s** |
| **05** | 1999 | 100 | 19 + 99 | 19 × 0.2 = 3.8 s | 80 × 0.2 = 16.0 s | **19.8 s** |
| **06** | 8022 | 500 | 16 + 22 | 16 × 0.2 = 3.2 s | 64 × 0.2 = 12.8 s | **16.0 s** |
| **07** | 200 | 10 | 20 + 0 | 20 × 0.5 = 10.0 s | — *(copies only)* | **10.0 s** |
| **08** † | 1899 | 100 | 18 + 99 | 18 × 0.2 = 3.6 s | 76 × 0.2 = 15.2 s | **18.8 s** |
| **09** † | 399 | 100 | 3 + 99 | 3 × 0.2 = 0.6 s | 16 × **2.0** = 32.0 s | **32.6 s** |
| **13** | 1999 | 100 | — | 0 | 0 | **0.0 s** |
| **14** | 180 lines | 50 | 3 + 30 | 3 × 0.2 = 0.6 s | 25 × 0.1 = 2.5 s | **3.1 s** |
| **15** | 120 blobs | 50 | 2 + 20 | 2 × 0.2 = 0.4 s | 5 × 0.1 = 0.5 s | **0.9 s** |
| **16** | 60 names | 20 | 3 + 0 | 3 × 0.3 = 0.9 s | — *(copies only)* | **0.9 s** |
| **17** | 1999 | 100 | — | 0 | 0 | **0.0 s** |

† **08 and 09 declare the floor for their *second* run, not the whole slice.** Run 1
is interrupted and legitimately finishes below any full-slice floor, so the runner
measures the resumed run's own reported time. 08's rows are the 1899 left after
one batch; 09's are the 399 left after one batch of its 499-row slice.

**09's floor deliberately omits its recovery phase**, and that is the one place in
this table where a term is left out on purpose. Recovery runs before the batch loop
and its cost is *variable*: the kill point decides how many of the interrupted
batch's 100 rows are still present to delete, and so how many throttle sleeps it
spends. **Zero is legal**, so folding a recovery term in would build a floor a
correct run can fail — and the tempting fix would be to lower it. That recovery
*happened* is asserted by a log line instead, not by timing.

**Copy-only (07, 16) has no delete term.** It deletes nothing; `delete_sleep_seconds`
and `batch_delete_size` are inert for it, and the configs set neither. Whenever you
add a test, derive the floor from the phases that command actually executes, not
from the shape of the formula.

**13 and 17 sleep nothing.** The length of a hold is the harness's, not the run's.

**20's floor is 0.0 s.** It runs three times (stopped, killed, finished) and the assertions read only
the last, which is what remains after a stop and a kill: its elapsed time has no full-slice floor. The
sleeps (`sleep_seconds: 2`, ten batches of 50) are there to keep the first run alive while the harness
drives it, not to be asserted.

**This is a one-sided bound, which is why it is safe to assert.** A slow or loaded
machine can only push the measured time *up*, so it can never fail.

**It is not a test of sleeping.** The floor encodes the run's *shape* — this job
must process twenty batches with these pauses — so anything that changes that
shape lands below it, **including causes nobody anticipated**, and it catches them
while the command still exits 0 with entirely plausible output:

- a config that silently stopped being read, so the global defaults applied;
- discovery returning fewer roots than the `where` slice implies;
- an early return introduced by a later refactor;
- a resume path that skipped work it should have replayed;
- a batch loop that ran once where it should have run twenty.

That is the point. Each working test asserts on five **independent** axes, and the
floor is the only one that needs no model of what the command means:

| Assertion | Answers | Needs to know |
|---|---|---|
| post-condition | did **exactly** the right rows move, in the right direction | what the command does, and how many rows the slice holds |
| verify stage | did verification run by the declared method, **over every copied row** | the report's `verified (<method>):` line |
| referential integrity | are the rows that moved **internally consistent** | the graph's parent→child edges |
| **pacing floor** | **did the run have the size it was configured to have** | **only the config** |
| tracking tables | does **goarchive's own bookkeeping** agree the work is finished | the `ga_status` contract |

> **The fifth axis asks a question the other four cannot.** All of them read user
> data; this one reads `archiver_job_log_<id>`. A batch that copied and deleted but
> never reached its completion leaves the data perfect and the log still claiming
> roots to replay — so the next run would replay them again. Every job type logs
> every root it processes, so every working test asserts it: the root
> table's expected count is the log's `completed` count, with no root at `copied`
> or `deleting`.

> **Referential integrity is a separate axis, not a refinement of the count.**
> Both can hold while the other fails — 200 payments referencing 200 rentals that
> never arrived satisfies every count. It applies only to multi-table tests, where
> there is an edge to check, and it exists because every test that *copies* must
> set `safety.disable_foreign_key_checks: true`: a copied child normally references
> parents outside the job that the destination does not hold, so the destination
> cannot reject an orphan on its own. A composite edge names its columns with `+`:
> `km_item:order_id+line_no:km_line:order_id+line_no`.

> **The floor and the exact count cover opposite directions, and neither
> substitutes for the other.** The floor is one-sided: a run that did *less* than
> configured finishes early and fails it, but a run that did *more* simply takes
> longer and passes. The exact count is what catches that second direction. Before
> `expected_rows` existed, a purge of the whole table and an archive of the whole
> table both passed every assertion in the suite.

It is compared against **goarchive's own reported time** — the `elapsed_seconds` of the
run's closing log entry (`Run completed`, read with jq) — not the test's elapsed time: the
latter includes the source
reset, `validate` and `dry-run`, which dwarf the floor and would hide a lost
throttle.

### Validation demos — preflight MUST fail

The runner **inverts** pass/fail for demos: a demo "passes" when `validate` fails
with the *expected* error category. An unexpected `validate` **success** is the
regression. (`EXPECTED FAILURE matched` in the log = good.)

| Test | Config | Expected error | Why |
|------|--------|----------------|-----|
| **02** | `test02_one_to_many.yaml` | `FK_COVERAGE_CHECK` | `language → film` pulls `film` into the job, but `film` is also referenced by `inventory`/`film_actor`/`film_category`, which are outside it; deleting archived films would violate those FKs, so every referencing table must be a relation. |
| **10** | `test10_view_not_base_table.yaml` | `TABLE_EXISTENCE_CHECK` | `root_table` is `customer_list`, one of Sakila's seven **views**. Only base tables can be archived — a view has no primary key to delete by. Pins the **non-base-table policy**, and is the one demo that asserts an *improvement*: 1.8.0 reported `PRIMARY_KEY_CHECK` here, telling the operator to fix a primary key a view cannot have. **This test therefore fails on a 1.8 binary by design.** |
| **11** | `test11_internal_fk_undeclared.yaml` | `INTERNAL_FK_COVERAGE` | The `customer → rental → payment` GDPR diamond — see the note below. `payment` has two parents in the job, so one foreign key is never a relation. |
| **12** | `test12_same_database.yaml` | `SRC_DEST_IDENTITY_CHECK` | `source` and `destination` are both `127.0.0.1:3305/sakila`. Every command that opens both connections refuses at connection time, before preflight and before any tracking row is written: an archive into itself would verify the table against itself and delete the only copy (issue #13). The demo runs `validate`; `TestConnectRefusesSameDatabase_Integration` is the integration witness. |
| **18** | `test18_film_tree_recipe.yaml` | `DELETE_TRIGGER_VISIBILITY_CHECK`, naming `film(unconfirmed)` | Test01's `film` tree, run as `ga3e2e_recipe`, an account the engine creates with the documented source recipe less `TRIGGER` (`SELECT, DELETE` on `sakila.*`, `PROCESS`) and drops after `validate`. MySQL lists a table's triggers only to an account holding `TRIGGER`, so this account cannot prove `film` has no DELETE trigger. The refusal arrives although `validate` runs with `--force-triggers`, which does not apply to it. |
| **19** | `test18_film_tree_recipe.yaml` | `DELETE_TRIGGER_CHECK`, naming `film(del_film)` | The same tree and account, with `TRIGGER` added, and `validate` **without** `--force-triggers`: the account sees Sakila's DELETE trigger `del_film` and the job is refused. Every other E2E run passes `--force-triggers`, so this is the one place the refusal is shown on real data. The same account with `--force-triggers` passes (probed), so the trigger alone refuses it. |

Test 01 was the fifth demo in 2.x, refused by `COMPOSITE_PK_CHECK`; 3.0 archives
composite keys, so it is now a working archive.

> **The `customer → rental → payment` (GDPR) shape is unrepresentable, and test 11
> is the gate for that.** `payment` references **both** `customer` and `rental`, and
> `rental` references `customer` — a diamond. A job is a strict tree (each relation
> is nested under one parent), and `INTERNAL_FK_COVERAGE` requires every foreign key
> between two of the job's tables to be a configured relation, so any rooting leaves
> one foreign key uncovered. Tests 01 and 04 are the **working** multi-level shapes:
> `customer` and `staff` stay outside the job as upstream parents.
>
> This is not fixable by rearranging, and both halves of that were measured rather
> than argued: re-nesting `payment` under `rental` only moves which edge is uncovered,
> and dropping either parent leaves `customer` referenced from outside the job so
> `FK_COVERAGE_CHECK` fires instead. Collapsing the diamond to test 04's shape is what
> makes `validate` pass — which is how test 11 is proven able to fail.
>
> **Nesting is what assigns a parent, not `foreign_key`.** A relation listed flat
> becomes a child of the **root**, whatever its `foreign_key` says: writing `payment`
> as a sibling of `rental` with `foreign_key: rental_id` pairs `rental_id` with the
> root's key and is refused by `RELATION_KEY_CHECK` — a different defect from the
> diamond. Nest the child under its parent. (Test 01's 2.x config listed
> `inventory`, `rental` and `payment` flat; under 3.0 that is refused the same way,
> so 3.0's config nests them.)

## Prerequisites

### 1. Environment configuration

```bash
cp tests/dot.env tests/.env      # optional — the runner creates it if absent
```

`tests/.env` is gitignored, so a fresh clone has none. `run-tests.sh` creates it from the
tracked `tests/dot.env` and says so; the template's defaults work against the containers
`make test-up` starts, so nothing has to be edited. You also need **MySQL Shell**
(`brew install mysql-shell`) and **jq** (`brew install jq`; preinstalled on GitHub's Ubuntu
runners) — the runner checks for both up front and stops with install instructions if one is
missing. The E2E assertions read each run's closing JSON log entry with jq, and the harness's
own tests (`tests/harness`) need it too. **Test 20** also needs **`nc`** (the OpenBSD one) and
**`socat`**, because it sends the documented control-socket lines as a script without the binary would:
a Mac has the system `nc` and needs `brew install socat`; Ubuntu needs `apt-get install netcat-openbsd
socat` (`gate-verify.yml` installs both before the E2E part). The harness fails the test, not skips it,
when one is missing.

`run-tests.sh` sources `tests/.env` for you. If you invoke `go test -tags=integration`
**directly**, load it yourself first:

```bash
set -a; source tests/.env; set +a
```

There is no built-in default password. `tests/compose.yml` interpolates `MYSQL_ROOT_PASSWORD`
from `tests/.env` when the containers are **created**, so exporting a different value afterwards
does not change them — it only breaks the connection. To change the password, edit `tests/.env`
and recreate the containers.

Forgetting is safe in the sense that it cannot pass silently: the integration suites
**fail** with a message naming the fix. They do not skip. A skipped suite prints `ok`
and exits 0, which is indistinguishable from a green run unless you pass `-v` and read
the `--- SKIP` lines — so the environment is proven once, up front, before any test
runs.

Default topology:
- **Source** (db1): `127.0.0.1:3305/sakila`
- **Archive** (db2): `127.0.0.1:3307/sakila_archive`
- **Replica** (db3): `127.0.0.1:3308` — a **live replica of db1**, attached by
  `--setup` (see below)

#### Replication topology

`run-tests.sh --setup` finishes by running `tests/scripts/setup-replication.sh`, which
attaches db3 to db1 so the replication-gate tests observe a real replica rather than a
simulated one. Three things about it are worth knowing before you touch the estate:

- **db3 reaches the source as `db1:3306`** — the compose service name and the *in-container*
  port. `127.0.0.1:3305` is the host mapping and is not routable from inside the container.
- **The replica seeds itself by replaying db1's GTID history**, which is why the attach runs
  after Sakila is loaded, and why `REPL_POSITIONING=gtid` requires `Auto_Position=1`.
- **The script does not return until the replica reports zero lag** (bounded at 120s). An
  estate that is connected but still replaying Sakila would make the next test measure
  catch-up and call it replication lag. Zero is required on two consecutive polls, because
  `Seconds_Behind_Source` is derived from the last *applied* event and reads 0 for an instant
  whenever the applier drains ahead of the receiver.
- **The script never repairs a divergent config.** It compares source host/port, the
  replication user, both threads, `SQL_Delay` and `Auto_Position` against what the estate
  requires; on any mismatch it prints the diff plus remediation SQL and exits 1. Re-pointing
  a live replica would silently discard whatever produced the current state — a lag scenario
  mid-flight, a deliberately stopped applier, or a different source. Checking only "both
  threads are running" would accept a replica pointed at the wrong server, or one still
  carrying a `SOURCE_DELAY` left over from a lag test.

To reset the topology deliberately:

```bash
tests/scripts/mysql-query.sh 3308 "STOP REPLICA;"
tests/scripts/mysql-query.sh 3308 "RESET REPLICA ALL;"
tests/scripts/setup-replication.sh          # re-attaches; safe to run twice
```

That discards db3's replication config, not its data.

### The destination runs in `+03:00` — on purpose

`tests/docker_files/my.cnf.d/db2.cnf` sets `default-time-zone = '+03:00'` on the archive
server (3307); the source (3305) and replica (3308) stay in UTC. This is a regression net for
[#16](https://github.com/dbsmedya/goarchive/issues/16): GoArchive pins every session it opens to
`time_zone='+00:00'`, and if that pin is ever lost, every `TIMESTAMP` copied by the suite lands
three hours off while SHA256 verification still matches. Sakila carries 15 `TIMESTAMP` columns,
so the whole E2E matrix crosses the boundary. `TestMySQLTimestampFold` (`internal/engine`)
archives the two instants of Berlin's DST fall-back hour, which a zone-blind copy would collapse
into one, across that boundary, and shows that a pool whose sessions run in Berlin time is
refused before anything is read. The named-zone tables are loaded on every container by the
Percona entrypoint.

### The group estate

A second test estate, for Group Replication. It has the plain estate's three roles, so the suites keep
one vocabulary, and two more members of the group:

| Container | Role | Host port | Server id |
|---|---|---|---|
| `gr1` | the group's primary, and the source | 3410, `TEST_SOURCE_PORT` | 1 |
| `gr2`, `gr3` | the group's other members | 3411 and 3412, `TEST_GROUP_MEMBER2_PORT` and `TEST_GROUP_MEMBER3_PORT` | 2, 3 |
| `gr-dest` | the destination: db2's settings, `+03:00` included | 3413, `TEST_DEST_PORT` | 4 |
| `gr-replica` | an asynchronous replica of `gr1`, attached after seeding as db3 is | 3414, `TEST_REPLICA_PORT` | 5 |

`gr-bootstrap` forms the group, three single-primary members, with the MySQL Shell AdminAPI
(`tests/docker_files/gr-bootstrap.sh`, from dbsgomysql's own bootstrap), and idles. The library's copy in
`tests/envmatrix/group/` stays unchanged: it is the matrix's `group_replication_source` row's.

**One estate at a time.** `TOPOLOGY=plain` (the default) is db1 to db3 and `TOPOLOGY=group` is this one.
`make test-up`, `test-down`, `test-status` and `test-reset`, the E2E harness's setup (`run-tests.sh --setup`)
and the runners (`run-tests.sh`, `check-servers.sh`, `require-containers-up.sh`) take it. `test-up` and
`test-reset` refuse while a container of the other estate is running, found by its name, and `test-down` of
one never touches the other: each has its own compose file, project name (`ga-group`) and volumes.

```bash
make test-down                              # the plain estate: stops it, keeps its volumes
make test-up TOPOLOGY=group                 # creates tests/.env.group from tests/dot.env.group if absent
set -a; source tests/.env.group; set +a     # not tests/.env: the ports differ
TOPOLOGY=group bash tests/scripts/run-tests.sh --setup --skip-docker   # seeds gr1 with Sakila, loads gr-dest, attaches gr-replica to gr1
go test -p 1 -tags=integration ./internal/... ./cmd/... -count=1       # the integration stage, with gr1 as the source, one package at a time
```

(`run-tests.sh --setup` alone, without `--skip-docker`, resets the estate first, as it does for the plain one.)

- **Run the stage one package at a time: `-p 1`.** `go test` runs the packages of a stage at the same time
  (that is its default), and on this estate a test changes what every other test writes to: a primary switch
  makes gr1 read-only for all of them, a lowered `group_replication_transaction_size_limit` lowers the limit
  for every writer, a read-only `gr-dest` refuses every job's tracking tables. A lock cannot say "everyone
  else waits" without starving the others, so the stage is run package by package, and the tests inside a
  package already run one after another (no integration test calls `t.Parallel`:
  `TestIntegrationTestsDoNotRunInParallel`). Run side by side, a test of one package failed registering its
  job because a test of another had made `gr-dest` read-only. **The runners do this themselves for the group
  topology:** `TOPOLOGY=group bash tests/scripts/run-tests.sh --integration-only` and `make test-integration
  TOPOLOGY=group` add `-p 1`, and the plain route's `go test` is exactly what it was
  (`TestTheRunnersGroupRouteRunsOnePackageAtATime`, `TestMakeTestIntegrationRunsTheGroupEstateOnePackageAtATime`). A
  `go test` of your own on this estate needs the flag. Each group test also begins by reading the group's base
  state, and fails, naming what differs, when a killed run left it changed
  ([Servers the suites share](#servers-the-suites-share)).
- **The image** is one per series, `GA_ESTATE_IMAGE` (the plain estate's variable too): `mysql:8.0`,
  `mysql:8.4` (the default), `mysql:9.7`, or `percona/percona-server` of the same series. A data directory
  belongs to its series: change the image with `make test-reset TOPOLOGY=group`. The setup loads the
  archive schema with the host's MySQL Shell (`scripts/dump_master.js`), which must support the server:
  a Shell 8.4.10 stops at that step against a 9.7.4 server ("Unsupported MySQL Server 9.7.4 detected,
  please upgrade the MySQL Shell first"), and `run-tests.sh --setup` exits 1. The gate pins its images
  for the same reason (*The full matrix runs on GitHub*, below).
- **`tests/.env.group`** is `tests/.env` pointed at the group, plus the two other members
  (`TEST_GROUP_MEMBER{2,3}_{HOST,PORT,USER,PASSWORD}`) for tests that need them, and `REPL_SOURCE_HOST=gr1`,
  the name by which `gr-replica` reaches its source. The compose file takes its ports from the same
  variables, so they follow a variable and not a container. It is gitignored; `tests/dot.env.group` is the
  tracked example.
- **`make test-down TOPOLOGY=group` keeps the volumes.** The members are then all stopped, and none can
  bootstrap the group; the next `make test-up TOPOLOGY=group` reboots it
  (`dba.rebootClusterFromCompleteOutage`, about a minute). After any other failure,
  `make test-reset TOPOLOGY=group`.
- **The group is an InnoDB Cluster**: the AdminAPI adds the `mysql_innodb_cluster_metadata` schema and
  `mysql_innodb_cluster_*` accounts to every member, and persists settings. Among them is
  `group_replication_consistency`, which is `BEFORE_ON_PRIMARY_FAILOVER` on 8.4 and 9.7 and `EVENTUAL` on
  8.0 here, each series' own default. No test may assume their absence or a value: a test that needs one
  sets it (a server state) and restores it.
- **A group refuses what it cannot certify.** A table without a primary key can be created but not written
  to (`ERROR 3098`), and a table of another engine than InnoDB is refused the same way. The suites write
  such tables only on a plain destination.
- **The members' memory limits (1 GB for gr2 and gr3) suit the suites, not a transaction of 100 MB**: a
  member was killed by the kernel's OOM handling under one. A measurement that needs a very large
  transaction raises the limits first.
- **No E2E yet.** The E2E configs name the plain estate's ports, so `make e2e` refuses `TOPOLOGY=group`.
  No suite is copied for the group.

### 2. Build the binary

```bash
go build -o bin/goarchive ./cmd/goarchive
```

## Running Tests

### Setup the environment

```bash
# Start Docker containers (db1/db2/db3), load Sakila into source, dump its
# schema, and load that schema into the archive destination.
./scripts/run-tests.sh --setup
```

### Unit tests

```bash
./scripts/run-tests.sh --unit-only          # or: go test ./... -count=1
```

`make test` and `make test-unit` (which adds `-race`) are unit only as well: neither passes the
`integration` tag.

CI runs the unit tests in its **Check** job, and `internal/logger`'s again on Windows (**Logger on
Windows**, `.github/workflows/ci.yml`): a release ships a Windows binary, where a log file's mode
behaves differently. The same Windows step, **Logger and control tests**, runs the control socket's
platform tests too (`internal/control`, `internal/controlpath`, with `-v` so that what the platform
probes saw is in the log): the file lock, the identity of a socket file, a stale socket, the path
limit, the home lookup, and how a connect and a close behave.

#### The control socket's platform tests

These two packages hold the facts the control socket relies on, so some tests need a host that a plain
`go test` does not give. A test that cannot run on its host skips with its reason; those are in
[Expected skips](#expected-skips), with where each of them runs. The tests below do not skip: each is
built for one platform, or takes the host as it is. Each is a unit test and needs no database.

| Test | It needs | Where it runs |
|---|---|---|
| `TestStartLeavesAFullQueueSocket` | Linux: a live listener whose queue is full answers `EAGAIN` at once. On macOS the same listener answers `ECONNREFUSED`, the answer for no listener, which the design accepts (spec §4.2), so there is no test there | Linux: L1, L3 and CI's Ubuntu runner |
| `TestControlFullQueueIsExit9AtOnce` (`cmd/goarchive/cmd`) | a listener whose queue is full, which fails a connect at once: `EAGAIN` on Linux, `WSAENOBUFS` on Windows | Linux (L1 to L3) and the Windows job. There is no test on macOS, where a full queue is refused |
| `TestControlResetBeforeTheReply` | Linux: closing a connection with the request unread resets it | Linux only: macOS ends the stream cleanly, which the client reads as an empty reply |
| `TestHomeIgnoresEnvironment` | the effective user's database entry, whichever it is | everywhere; run as a uid with no entry and `USER` and `HOME` set (L5), it checks the no-default refusal |

The transport tests (`transport_test.go`) pin what each platform does to a connect and a close; on
Windows a refused connection is `WSAECONNREFUSED` (recognised by `isRefused`), a full queue is
`WSAENOBUFS` (undecided: no macOS-like window), and closing with input unread is a reset.

**Linting another OS's files.** `make lint` loads the host's build tags only, so a Linux- or
Windows-tagged file is not linted on a Mac. Install the pinned linter with the repo's toolchain and run
it per target; the host Go may be newer than `go.mod`'s `toolchain` line, which is a minimum, and the
pinned linter cannot read a newer Go's export data when it cross-lints.

```bash
export GOTOOLCHAIN=go1.26.8
GOBIN=/tmp/ga-bin go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.11.4  # the Makefile's pin
GOOS=linux   /tmp/ga-bin/golangci-lint run ./...                  # what CI's Check job runs
GOOS=windows /tmp/ga-bin/golangci-lint run ./...
```

All three targets report 0 issues. A test file that can only run on Unix says so with `//go:build
!windows` and a one-line reason (`internal/database/signal_test.go` and the gate tests in
`tests/harness`, which signal a process group with `syscall.Kill`), so that the tree typechecks for
Windows.

The container runs use the Go release of `go.mod`'s toolchain line, with a writable `GOCACHE` and
`GOPATH` under `/tmp`:

```bash
# L1: as root, cgo as the image has it. L2: the same with -e CGO_ENABLED=0
docker run --rm -v "$PWD":/src -w /src -e GOCACHE=/tmp/gocache -e GOPATH=/tmp/gopath golang:1.26.8 \
  bash -c 'apt-get update -qq && apt-get install -y -qq netcat-openbsd socat &&
           go test ./internal/control/... ./internal/controlpath/ -count=1 -v &&
           go test ./cmd/goarchive/cmd/ -run TestControl -count=1 -v'

# L3: a non-root uid with an entry
docker run --rm -v "$PWD":/src -w /src -e GOCACHE=/tmp/gocache -e GOPATH=/tmp/gopath golang:1.26.8 \
  bash -c 'useradd -m tester && mkdir /tmp/gocache-t /tmp/gopath-t && chown tester /tmp/gocache-t /tmp/gopath-t &&
           su tester -c "cd /src && GOCACHE=/tmp/gocache-t GOPATH=/tmp/gopath-t go test ./internal/control/... ./internal/controlpath/ -count=1 -v && go test ./cmd/goarchive/cmd/ -run TestControl -count=1 -v"'

# L4: a connection between uid 0 and another uid, in both directions (the run of L1 includes it)
docker run --rm -v "$PWD":/src -w /src -e GOCACHE=/tmp/gocache -e GOPATH=/tmp/gopath golang:1.26.8 \
  go test ./internal/control/ -run 'TestPeerUID|TestSameUserOnlyOtherUID' -count=1 -v

# L5: an effective uid with no user-database entry, USER and HOME set. Build the test binaries
# first, then run them as that user, so that the go tool itself touches nothing under that HOME
GOOS=linux GOARCH="$(docker version --format '{{.Server.Arch}}')" CGO_ENABLED=0 \
  go test -c -o /tmp/l5/controlpath.test ./internal/controlpath/
GOOS=linux GOARCH="$(docker version --format '{{.Server.Arch}}')" CGO_ENABLED=0 \
  go test -c -o /tmp/l5/cmd.test ./cmd/goarchive/cmd/
docker run --rm --user 12345:12345 -e USER=ghost -e HOME=/tmp/ghost -v /tmp/l5:/t:ro --entrypoint /t/controlpath.test \
  golang:1.26.8 -test.run 'TestHomeIgnoresEnvironment|TestHomeOfUnknownUID' -test.v
docker run --rm --user 12345:12345 -e USER=ghost -e HOME=/tmp/ghost -v /tmp/l5:/t:ro --entrypoint /t/cmd.test \
  golang:1.26.8 -test.run 'TestNoEntryNoDefault' -test.v
```

On macOS a connection from another uid is never made: `JudgeDir`'s owner rule is tested on a
directory of this user judged against another uid, and the real other-uid connection is the root
container's (L1, L4). CI's Check job runs neither container step, so `TestPeerUIDOtherUser` and
`TestSameUserOnlyOtherUID` skip there.

### Coverage on demand

Coverage is generated from the current checkout instead of maintained as a percentage
snapshot. From the repository root:

```bash
go test -short ./... -count=1 -coverprofile=/tmp/goarchive-short.cover
go tool cover -func=/tmp/goarchive-short.cover
go tool cover -html=/tmp/goarchive-short.cover -o /tmp/goarchive-short.html
```

For focused work, replace `./...` with one package, e.g. `./internal/engine`. Keep generated
profiles and HTML outside the repository. A review may record the measured command and
result, but documentation must not present a percentage as current unless that same change
generated it.

### Integration tests

Real-DB tests behind the `integration` build tag. `--setup` reseeds first so the
destination starts empty (see the Overview note):

```bash
./scripts/run-tests.sh --setup --integration-only
```

A direct `go test` needs `-tags=integration`; without it the integration tests are not
compiled, and `go test` prints `ok` with no test run.

The tests read the estate from `TEST_SOURCE_*`, `TEST_DEST_*` and `TEST_REPLICA_*`
([Environment Variables](#environment-variables)); without them they fail with
`integration environment not loaded: set -a; source tests/.env; set +a`. Most tests build their
fixtures in scratch schemas on the source and destination (`ga3m5_<name>_<pid>`) and drop them
when they end, so those tests do not share state; a killed run can leave one behind. Some read
the seeded Sakila estate instead (for example `internal/key`'s key facts on `film_actor` and
`payment`), so run `--setup` first.

The suites open their pools as goarchive opens its own, through `database.OpenPool`: the same
session pins, per-connection checks and pool checks, with the open-connection cap lifted, as the
suites race more sessions than a run does. A server global a test stands in for (`sql_mode`,
`max_execution_time`, `range_optimizer_max_mem_size`) is a `database.Global`, layered under the
pins as a real global is. A few files open a raw pool on purpose: the `internal/database` pin
tests, the engine's UTC refusal and INSERT diagnostics backstop, the trigger-visibility pool and
the charset probes. `TestIntegrationPoolsUseOpenPool` (`tests/harness`) lists each one with its
reason and fails on a raw open in any other integration test file.

#### Servers the suites share

`go test` runs the packages of the integration stage at the same time, and they share the estate's servers. Four
rules keep a test from meeting another's state:

- **On the plain estate no test changes a global of db1 or db2**, which every package's tests write to. A test
  that needs a read-only server uses db3 (below); a server setting a suite needs on its own throwaway server is
  an [environment matrix](#environment-matrix) row.
- **The replica (db3, and `gr-replica` on the group estate) is one test's at a time, across packages.** The
  helper that opens its pool in each package (`openServer`, `recorded`, `acctOpen`, `serverFor`) takes
  `internal/testestate`'s lock first: one `GET_LOCK` on the replica itself, held on a session until the test's
  cleanups have run, so a test that stops the applier, sets `SOURCE_DELAY`, makes the replica read-only or
  depends on its lag cannot be disturbed, and a killed test process gives the lock back with its connection.
  A subtest uses its test's claim. The one test of `internal/database` that names the replica only connects to
  it as a stand-in member and takes no lock. The lock does not stop another package writing to db1, whose events
  the replica holds back or applies; a test that reads the lag allows for that.
- **A test reads the base state, and does not trust it.** When the replica's lock is taken, the replica's state is read
  through the library and the test fails, naming what differs, if `read_only` or `super_read_only` is on, a thread
  of the default channel is not running or `SOURCE_DELAY` is not 0. Each group test, after it has found the
  source to be a member, does the same for the group: gr1 `PRIMARY` with three members `ONLINE`,
  `group_replication_transaction_size_limit` at 150000000 and `group_replication_consistency` at the series' own
  value on every member. A test that takes a server in a changed state **fails**: it does not skip and it does not go
  on. A killed test process skips its cleanups, so this is how what it left is found. Put the state right by hand with
  the statements the message names, and run again.
- **Nothing is written to the replica by a test.** A statement on a replica is a transaction of its own that its
  source does not have, and replication fails on it later. A schema or an account a test needs there is created on the
  source and reaches the replica by replication; a test that runs preflight against the replica as a destination
  checks afterwards that the replica holds no transaction of its own.

No integration test calls `t.Parallel` (`TestIntegrationTestsDoNotRunInParallel`).

#### Interrupted runs

`TestMySQLKillRecovery` (`internal/engine`) kills a helper process (`TestEngineHelperProcess`)
at each crash point of each job type, then restarts the job: archive and purge must finish the
logged roots by delete-only replay and end equal to an uninterrupted run; copy-only must refuse
the duplicate with `resume: false` and finish with `resume: true`. The crash points are named
seams in the engine, not timers. Binary E2E 08 and 09 send real signals to the CLI; test 13
holds and releases a live replication gate in one invocation.

Three more seams sit between a commit's writes and its `COMMIT` (`delete-written`,
`copy-written`, `log-written`): `TestMySQLGuardedCommit` kills a lock session there and proves
the transaction rolls back. Tests that touch locks follow three rules:

- A test that needs a silent lock holder shortens `sessionWaitTimeout` (`internal/lock`) and
  restores it; a test of the schema lock's wait shortens `initLockWait` (`internal/tracking`)
  the same way. None waits out the 300-second lock-session timeout or the 60-second
  schema-lock wait.
- Lock names come from `lock.TableLockName`, `lock.JobLockName` and `lock.SchemaLockName`, or
  from the SQL formula the docs publish, never a literal: they are hashes.
- Every engine test runs through its `env` (`e.run`, `e.runContext`), which records the run's
  lock names; when the test ends, a guard fails it if any is still held. The helper child of
  `TestMySQLKillRecovery` exits when its parent goes, so a timed-out parent cannot leave its
  locks held.

The environment-matrix row `wait_timeout_10` also proves the lock sessions keep their 300
seconds under a 10-second global (`TestMySQLWaitTimeout`, side A).

#### Runtime control

The control socket's integration tests come in two layers. In `internal/engine` they drive the
settings store directly, at the engine's named points (`TestMySQLControl*`: sizes read once per batch
and per recovery page, the ceilings, a stop at a point, and the tolerance against a replica whose
`SOURCE_DELAY` gives it a real lag: lowered, restored, changed during the inter-delete sleep and in
recovery, and raised one step at a time). In `cmd/goarchive/cmd` (`control_integration_test.go`) they run
the command and talk to its socket, which is the only seam the command has, so each test times the run by
the socket itself: `pause`, then a wait for the state `paused` in `status-json`, or a wait for a step. They
cover the pause and resume, a stop at the boundary, in the final short batch and met by an error, the
closing entry's runtime-changes line after its audit entries, refusals in the log, a copy-only run's
inapplicable settings, a socket in use (exit 4), the cron-style run silent with control enabled, and, with
the applier stopped or the replica delayed, the status during a hold, a stop during a hold and the rerun's
recovery, and the tolerance raised over the socket with its warnings. The replica tests stop the applier or
set `SOURCE_DELAY` on db3 and put both back when they end.

`TestProtocolContractMatchesDoc` (`tests/harness`) compares the protocol contract in
`docs/README_OPERATIONS.md` (the actions, the settings, the reason words and the `status-json` fields) with
the server's own tables, read from its source, so a protocol change that does not update the doc fails
`make check`.

### Environment matrix

The environment matrix runs the integration suites again under **one changed MySQL
environment at a time**: a global variable, a setting fixed at initialization, or another
server version. The rows are in [`envmatrix/rows.tsv`](envmatrix/rows.tsv), and each one states
its expected outcome. Under every row a run must be exact, or refused at startup with a named
error, or fail closed by name before it changes anything. The suites open goarchive's own
pools, so under `max_error_count_0` a session at 0 is raised to 1024, or refused at open with
`MAX_ERROR_COUNT_CHECK` for an account that may not raise it, as the binary refuses it. A row
expected to `pass` has no failing test. A row expected to `refuse:CODE|…` passes
only if every failing test's own output cites one of those codes, a failing parent's own lines
included. A panic, a timeout, a build failure, a test or package that started and never
finished, or a `go test` exit status that disagrees with its output (a killed run) fails any
row. The judge (`envmatrix/judge`) decides each row from `go test -json`.

| Kind | What the runner does |
|---|---|
| `dynamic` | Takes a snapshot of the globals on a long-lived source/destination pair of `MX_IMAGE`, applies the row's `SET GLOBAL` to both servers (to the destination only when the setup starts with `dst:`), runs the suites, then restores the snapshot and compares it |
| `init` | Starts a fresh pair with an extra `my.cnf` (for example `lower_case_table_names = 1`) |
| `version` | Starts a fresh, unchanged pair of another image (Percona and Oracle, 8.0.40 to 9.7) |
| `group` | Starts dbsgomysql's three-member Group Replication cluster (`envmatrix/group/`, copied unchanged from the pinned library) as the source, with a fresh destination, and runs only the unfinished-work guard tests: Group Replication refuses tables without a primary key, which other suites create on purpose |

```bash
make env-matrix                                   # every row
make env-matrix ROWS="sql_mode_empty v84_oracle"  # some rows
```

- **Needs:** Docker, the local images that `rows.tsv` names, and `tests/.env`.
- **Servers:** the throwaway pairs (`ga-mx<i>-src`/`-dst`, ports 3375–3396) are removed with
  their volumes when the run ends; a group row's cluster (compose project `ga-mxg<i>`) takes
  the rest of its pair's block of ten ports, and is removed with its volumes after the row.
- **The estate:** the suites still reach db3 as the replica, so the tests that drive it through
  the live gate are skipped here: `TestMySQLLiveReplicationGate` and `TestMySQLStopDuringHold`,
  which stop db3's applier, and `TestMySQLStopNeverCutsAPass`. Run them on the estate alone.
- **Results:** `tests/results/envmatrix/<tag>/` (`latest` links to the newest run): per row
  `verdict.txt`, `go-test.json` and the variable snapshots, plus `SUMMARY.txt`. The command
  exits 1 if any row is not OK.
- **Time:** three pairs work through the rows in parallel (`MX_PAIRS`). A dynamic row takes
  about 50 s, and a fresh pair adds about 30 s to start.
- **Running alone:** do not run the estate suite or lint beside it.
- **Server variables:** the runner and tests may `SET GLOBAL` on these throwaway servers;
  goarchive itself never does.

#### The full matrix runs on GitHub

Run one row or a few locally while developing. The **full** matrix runs on GitHub through the
*Matrix verify* workflow (`.github/workflows/matrix-verify.yml`), and the gate on every supported
image through the *Gate verify* workflow (`.github/workflows/gate-verify.yml`). Both start by
themselves on a pull request into `main` (the dev→main PR), on its head SHA; each new push to that
PR starts a run and cancels the earlier one. Ad hoc runs start from a push of a branch, on the
operator's request only:

```bash
git push origin <sha>:refs/heads/verify/<sha7>          # Matrix verify
git push origin <sha>:refs/heads/verify-gate/<sha7>     # Gate verify
git push origin --delete verify/<sha7>                  # remove each branch after its run
git push origin --delete verify-gate/<sha7>
```

- **Jobs:** *Matrix verify* runs one job per `rows.tsv` row with one pair (`MX_PAIRS=1`), at most
  14 at once, a target job and a summary: the rows plus two. *Gate verify* runs the gate on an
  estate built from each of `mysql` 8.0.40, 8.4.11 and 9.7.1 and `percona/percona-server`
  8.0.46-37, 8.4.11-11 and 9.7.1-1, pinned so MySQL Shell supports each (`GA_ESTATE_IMAGE` in
  `compose.yml`), as two jobs per image that start together, its integration part and its e2e
  part (`run-gate.sh --part`, above), and a target and a summary job: 14 jobs.
- **Results:** each run page's summary is one table of every verdict (`envmatrix/gh-summary.sh`),
  headed *Matrix verify* or *Gate verify* and the SHA. It is red when any job did not succeed,
  when any row is not OK or is missing, when any gate is missing or lacks the evidence `make gate`
  records for its SHA (above: `complete`, a clean tree, both exits 0), and when a run holds the
  other workflow's jobs. A Gate verify image counts as a gate only when both its parts carry that
  evidence, each naming its own part; an image with one part fails the run. Every job's evidence
  directory is an artifact.
  A gate's integration count is three passes short of a local one on a machine that has `socat`, with
  three more skips: `TestSetupGracefulShutdown_TwoPhase` and `TestStopPhases` skip themselves where
  `CI=true`, which GitHub sets, and the `socat` subtest of `TestShellClientsReturnWhenTheServerCloses`
  skips because the integration job installs no `socat` ([Expected skips](#expected-skips),
  *GitHub Linux*).
- **When, and who:**

  | Run | When | Started by |
  |---|---|---|
  | *Matrix verify* and *Gate verify* (with CI and Docker) | A pull request into `main`, the dev→main PR: when it opens, is reopened or is marked ready, and on each new push to it | GitHub |
  | `verify/<sha7>`, `verify-gate/<sha7>` | An ad hoc run, on the operator's request, for the SHA the operator names | the coordinating agent |

  The repository is public: a push publishes its commits, and deleting the branch does not
  unpublish them. A red run goes to the operator with its summary table and is never re-run to get
  a green one; a red version row caused by a floating image tag is reported as such, and the
  operator decides whether it blocks the PR.
- *Matrix verify* replaces the local full matrix. On the dev→main PR, *Gate verify* replaces the
  local gate. A change to `tests/scripts/run-gate.sh` or a command it runs also needs a local full
  gate on its branch ([Which checks a change needs](#which-checks-a-change-needs)).

### Sakila E2E tests

```bash
# The whole procedure — test-reset, then e2e-setup, then the working tests (01, 03–09, 13–17, 20)
make e2e

# Validation demos (02, 10, 11, 12, 18, 19) — preflight MUST fail. Needs a seeded estate.
make e2e-examples

# The individual steps, if you know why you want one
make test-reset                          # 1. destroy
make e2e-setup                           # 2. rebuild + seed
make e2e-tests-must-run-after-setup      # 3. run; refuses unless step 2 ran

# Target a single test
./scripts/run-tests.sh --sakila -t 4                    # working rental→payment
./scripts/run-tests.sh --sakila-examples -t 2           # FK-coverage demo
```

> ⚠️ **Run E2E sequentially**, not concurrently with integration tests or other
> E2E suites. Each E2E test resets the source by dropping/recreating `sakila`;
> active connections from a concurrent run can block `DROP DATABASE` ("Failed to
> reset source database"). Order: unit → integration → E2E working → E2E demos.

Add `-v` to any command for verbose output.

## Manual Testing Workflow

For interactive debugging, drive the CLI directly. These use working **Test 03**
(`archive-payment-rows`); substitute test 04 (`archive-rental-payments`) the same way.

```bash
./scripts/run-tests.sh --setup        # fresh databases first
CFG=tests/configs/test03_payment_batch.yaml

# 1. List jobs defined in the config
./bin/goarchive list-jobs --config "$CFG"

# 2. Plan: the relation tree, copy order (parents first) and delete order
#    (children first); connects to nothing
./bin/goarchive plan --job archive-payment-rows --config "$CFG"

# 3. Validate: connects and runs the job's preflight.
#    Add --force-triggers when the schema has DELETE triggers (Sakila does).
./bin/goarchive validate --config "$CFG" --force-triggers

# 4. Dry-run: preflight, then MySQL's plans for the first batch; changes nothing.
./bin/goarchive dry-run --job archive-payment-rows --config "$CFG"

# 5. Archive: copy → verify → delete; each batch's roots are logged in the
#    job's archiver_job_log_<id> table.
./bin/goarchive archive --job archive-payment-rows --config "$CFG"

# Confirm the destination received the rows
tests/scripts/mysql-query.sh 3307 \
  "SELECT COUNT(*) FROM sakila_archive.payment WHERE payment_id <= 2000;"
```

### Example: a demo that fails preflight (Test 02)

```bash
$ ./bin/goarchive validate --config tests/configs/test02_one_to_many.yaml --force-triggers
[1/1] Job: archive-language-with-films (archive)
   FAILED: FK_COVERAGE_CHECK: Foreign key constraints not covered by relations (fatal for any ON DELETE rule):
  - film is referenced by: [sakila.film_actor (ON DELETE RESTRICT) sakila.film_category (ON DELETE RESTRICT) sakila.inventory (ON DELETE RESTRICT)]
```

The runner treats this as a **pass** because the failure matches the expected
category (`FK_COVERAGE_CHECK`).

## Preflight Checks

Every check, what it means and how to fix it: [Validation & Preflight](../docs/README_VALIDATION.md).
The demos above pin six of them through the CLI; the rest are pinned by the `internal/preflight`
integration tests on real facts.

## Test Output

Each Sakila test prints a header and a verdict; per-test logs are written to
`results/test_<n>.log`.

- **Working test** → runs `validate → dry-run → <command>`, then **asserts the outcome**,
  and ends with `Result: PASS`.

  Four assertions run after the command, and any one of them fails the test —
  a resume test adds two more, described in `e2e/resume/README.md`: the
  **interrupt preconditions**, checked between the two runs, and the
  **tracking-table settlement** at the end:

  - **Verification must have covered every copied row, by the declared method** — the run
    report's `verified (<method>):` line must list the same tables and rows as its `copied:`
    line, and the console echoes `confirmed: every copied row was verified by <method> (…)`.
    Under `none`, and for purge, there must be no `verified` line.
  - **The post-condition must hold**, per command. Rows are counted on **both** source
    and destination, before and after:

    | command | source | destination |
    |---------|--------|-------------|
    | `archive` | lost **exactly** `expected_rows` | holds **exactly** `expected_rows` |
    | `purge` | lost **exactly** `expected_rows` | must stay **empty** |
    | `copy-only` | must be **unchanged** | holds **exactly** `expected_rows` |

    **Every arm compares an exact count, never a direction.** Direction was measured
    blind: under the previous conservation-only arms, an archive of all 16044 rows, a
    purge of all 16044 rows and an off-by-10× slice *all passed*. Conservation does not
    rescue it — moving the whole table satisfies `after + destination == before`
    precisely, and so does moving nothing.

    Conservation is still computed, but only on the **failure** path, where it separates
    two failures you would chase in opposite directions: *rows were conserved* means the
    run was internally consistent and `expected_rows` is probably stale, while *rows were
    not conserved* means rows went missing and the product is at fault. As a gate it can
    no longer fire at all — once both exact checks hold, conservation follows
    arithmetically — and a check that cannot fail is not protection.
  - **No orphans in the destination**, for every `child:fk:parent:parent_pk` pair the
    test declares in `orphan_checks`. The console echoes `payment -> rental: no orphans`.
    **Required for any test with more than one table**, and it fails closed — a
    multi-table test that declares none is rejected before the source reseed. Single-table
    tests have no edge to check. A NULL foreign key is *not* an orphan.
  - **The run must not have been faster than its pacing floor** — goarchive's own
    reported duration must be at least the test's `min_duration`, and the console
    echoes `pacing OK (21.7s >= 20.0s floor)`. See
    [the pacing floor](#the-pacing-floor--the-one-timing-number-that-is-not-machine-dependent)
    for how the floor is derived and why a slow machine cannot fail it.
- **Demo test** → `validate` fails; the runner prints
  `EXPECTED FAILURE matched` and `Result: PASS` when the category matches, or
  `Result: FAIL (wrong error category)` otherwise.

A summary (`SAKILA INTEGRATION TEST SUMMARY`) is generated at the end with a
`Passed: N / Failed: N` line.

## Querying the test databases

```bash
tests/scripts/mysql-query.sh <port> "<sql>"      # 3305 source · 3307 archive · 3308 replica
```

Results on stdout, diagnostics on stderr. Exit 0 = ran, 1 = query failed, 2 = environment
not loaded. So an empty stdout always means genuinely zero rows:

```bash
rows=$(tests/scripts/mysql-query.sh 3307 "SELECT job_name FROM sakila_archive.archiver_job;") || exit 1
[ -z "$rows" ] && echo "clean"
```

**Prefer the wrapper over raw `mysqlsh`.** It passes `--no-defaults`, without which `mysqlsh`
reads `~/.my.cnf` and can connect as whoever that file names when you forget the password —
succeeding locally and failing in CI. `sakila_archive` is the E2E destination and holds the
E2E jobs' tracking tables; integration tests use scratch schemas named `ga3m5_*`.

## Test result counts

Every Go layer the runner executes reports:

```
[INFO] integration: PASS=<n> FAIL=0 SKIP=<n> (go test exit 0)
```

and **fails when nothing ran**. `go test` prints `ok` and exits 0 for a `-run` pattern that
matched no test, a missing build tag, or a suite where everything skipped — so a gate that
only counts failures passes all three. The runner counts passes instead, which requires `-v`,
so `-v` is always on internally; the full log prints only with `--verbose` or on failure.

Skips are reported by name rather than swallowed. `MIN_PASS=<n>` requires at least n passing
tests, inclusive. It defaults to 1, which only catches a run that did nothing. The measured
baseline is in [`AGENTS.md`](AGENTS.md).

## Expected skips

A test that cannot run on its host **skips with its reason**, and the skip is the intended result
there. This is the one list of them, by the place a run happens. Hold a run's `--- SKIP` lines against
that place's table: a SKIP the table names is expected, and any other is a finding, whether it is a
test no table names or a listed test in a place whose table does not name it. A `SKIP=n` in a
runner's summary counts the `--- SKIP` lines of tests and subtests alike. A table's rows are not that
count: a row can group several tests or subtests, and a row marked *conditional* is a SKIP line only
where its condition holds. Count the lines a place prints, from the rows that apply to it.
In every table, *Where it runs* is where the test runs and does not skip, so a test can be in a place's list
of skips and have another place, or only one, in that column.

- A test in a file built for another OS (`*_linux_test.go`, `*_windows_test.go`, `//go:build
  !windows`) is not compiled elsewhere. It neither runs nor skips, and is not listed.
- A test a runner leaves out with `-skip` is not run and prints no SKIP line. The matrix runner's
  exclusions are named under *GitHub matrix rows*.
- **The guard.** `TestExpectedSkipsAreListed` (`tests/harness`, part of `make check`) fails when a
  test that skips is not named in the first column of a table here, and when a name there is no longer
  a test or no longer skips. A name is one function of one package: when two packages declare it, the
  row gives the package, as a path in backticks after the names it qualifies in the same cell (the
  rows below do); a method is `Receiver.name`. The guard reads tests, not subtests: a row that names
  `Test/sub` names `Test`, and which subtests skip is the row's own account. It does not tell which
  receiver a call through a method name that several receivers of a package share goes to, so such a
  skip is covered by a listed test or method that may reach it. A new `t.Skip` comes with a row in
  the place it skips; a skip that no place should ever meet goes in *No place above*.

### make check on a Mac

`make check` runs `go test ./...` without the `integration` tag, and needs no database. The first
four rows are the SKIP lines of a Mac with `socat` installed (`brew install socat`; `nc` is the
system's). The last two are *conditional* on a tool, and a Mac without `socat` prints one more line.

| Test | Why it skips | Where it runs |
|---|---|---|
| `TestPeerUIDOtherUser` | needs a test process running as uid 0: it starts the server half as uid 12345 and connects as root | a root container (L1, L4) |
| `TestSameUserOnlyOtherUID` | needs uid 0: it starts the control server as root, opens its directory and socket to others (the test's own change to its own temporary files; the directory rules keep others out otherwise) and connects as uid 12345. `TestJudgePeer` is the rule's every case on any host | a root container (L1, L4) |
| `TestControlEndpointChecks/the_directory_belongs_to_another_user` | needs root, to give a directory to another uid. The owner rule itself is also checked, for every user, against the shared judging function | a root container (L1) |
| `TestNoEntryNoDefault` (`cmd/goarchive/cmd`) | needs a uid with no user-database entry, with `USER` and `HOME` set: an enabled job with no `socket_dir` is refused with the no-default message and nothing is created under `$HOME`; the same job with `socket_dir` set starts; a job with control off creates nothing. Every uid of the other places has an entry (a Mac's, a container's root or its non-root user, an Ubuntu runner's), so it skips there | L5 only: the container run of that name under *The control socket's platform tests*, which no workflow makes |
| `TestShellClientsReturnWhenTheServerCloses/socat` | *conditional*: `socat` is not on the `PATH`. A Mac that has it prints no line; a Mac without it does. Of the places below, GitHub's *Check* job, *Gate verify*'s integration job and the matrix rows have none | a Mac with `socat`; L1, which installs it with `apt-get` |
| `TestShellClientsReturnWhenTheServerCloses/nc` | *conditional*: no OpenBSD `nc` on the `PATH`. macOS and the Ubuntu runners ship one, so only L3 (below) prints this line | a Mac, L1, GitHub Linux |

### Integration on the local estate

The gate's integration stage: `go test -tags=integration ./internal/... ./cmd/...` against db1 and db2
(`tests/scripts/run-tests.sh --integration-only`). The tag compiles the unit tests too, so every SKIP
of *make check on a Mac* prints here as well, and with them these. Each of the six below needs a server
setting the estate does not have, and **runs on its row** of the environment matrix (a row sets that
setting on a throwaway pair).

| Test | Why it skips | Where it runs |
|---|---|---|
| `TestMySQLMaxErrorCount` | `@@GLOBAL.max_error_count` is not 0 | row `max_error_count_0` |
| `TestMySQLSQLNotesPin` | `@@GLOBAL.sql_notes` is not 0 | row `sql_notes_0` |
| `TestMySQLWaitTimeout` (its side A subtest) | `@@GLOBAL.wait_timeout` is not below the 30 s keep-alive | row `wait_timeout_10` |
| `TestMySQLInitConnectCharset/text`, `TestMySQLInitConnectCharset/key` | the account's session is already `utf8mb4`; the pair needs a narrowed one | row `init_connect_set_names` |
| `TestMySQLArchiveValueLargerThanThePacket` | the estate's two servers have the same `max_allowed_packet`, so no value exceeds the destination's and fits the source's | row `max_allowed_packet_dst_4m`, where a skip is a failure |

The tests below need a Group Replication member as `TEST_SOURCE`. On this estate (db1 is a plain
server), on every GitHub row and on every *Gate verify* image they skip, one line per test; they run on
the group estate (`make test-up TOPOLOGY=group`, [The group estate](#the-group-estate)). The first
column is each package's helper, which every group-only test of that package calls.

| Test | Why it skips | Where it runs |
|---|---|---|
| `requireMemberSource` (`internal/preflight`) | the source is not a Group Replication member: its plugin is not `ACTIVE`, or it has no group name | the group estate |
| `requireGroupMemberSource` (`internal/engine`) | the same | the group estate |
| `requireGroupMemberSource` (`cmd/goarchive/cmd`) | the same | the group estate |
| `requireGroupEstate` (`internal/testestate`) | the same: the three self-tests of the mode helper and the fence harness | the group estate |

### The L1 to L5 containers

Linux, `golang:1.26.8`, the commands under *The control socket's platform tests* above. They run the
two control packages and `cmd/goarchive/cmd`'s `TestControl` tests, and L5 its own two. A layer not
named below skips nothing: L4 (uid 0 and another uid, in both directions) and L5 (no user-database entry).

| Test | Why it skips | Where it runs |
|---|---|---|
| `TestStartLeavesAnUndecidedSocket`, `TestConnectPermissionError`, `TestControlExitCodes/9_for_a_permission_error_on_connect` | **L1 and L2** (root): a socket with no permission bits refuses a connect only to a user other than root | a Mac, L3 and GitHub Linux |
| `TestPeerUIDOtherUser`, `TestSameUserOnlyOtherUID`, `TestControlEndpointChecks/the_directory_belongs_to_another_user` | **L3** (a non-root uid with an entry): each needs uid 0 | L1, L4 |
| `TestShellClientsReturnWhenTheServerCloses/nc`, `TestShellClientsReturnWhenTheServerCloses/socat` | **L3**: the non-root user cannot install the tools, which L1 installs with `apt-get` | L1 |

### GitHub Windows

The CI job *Logger on Windows* (`.github/workflows/ci.yml`): `./internal/logger`, `./internal/control/...`
and `./internal/controlpath`, then `cmd/goarchive/cmd`'s `TestControl` tests. These skips were read
from the job's log.

| Test | Why it skips | Where it runs |
|---|---|---|
| `TestStartLeavesAnUndecidedSocket`, `TestConnectPermissionError`, `TestControlExitCodes/9_for_a_permission_error_on_connect` | a Windows socket file has no mode bits to deny | a Mac, L3 and GitHub Linux |
| `TestDefaultDirectoryCreation`, `TestDirectoryRules`, `TestControlEndpointChecks/the_directory_has_group_or_other_bits`, `TestControlEndpointChecks/the_owner_rule_is_the_shared_judging_function`, `TestControlEndpointChecks/the_directory_belongs_to_another_user` | directory modes and owners are not enforced on Windows (the last also needs root) | Linux and macOS |
| `TestNotASocket`, `TestControlEndpointChecks/the_directory_is_a_symbolic_link`, `TestControlEndpointChecks/the_socket_path_is_a_symbolic_link_to_a_socket` | creating a symbolic link needs a privilege on Windows | Linux and macOS |
| `TestLockNameRemoved`, `TestEndAfterTakeover`, `TestDirectoryReplaced` | Windows refuses to remove a lock file the run holds open (`being used by another process`) and to rename a directory with open handles (`Access is denied`), so these cases cannot arise there; `TestIdentityCheckRepairsNothing` and `TestShutdownPendingTick` keep their socket halves on Windows | Linux and macOS |
| `TestShellClientsReturnWhenTheServerCloses` | the published lines are for a POSIX shell | a Mac with `socat`, L1 |
| `TestHomeOfUnknownUID` | Windows takes the profile of the process token, not a uid (elsewhere it skips if uid 12345 has an entry, which no place here has) | Linux and macOS |

### GitHub Linux

An Ubuntu runner has a non-root user, `CI=true` and `nc`, and has `socat` only where the workflow
installs it. Read from the workflows as written:

- **The CI job *Check*** (`make check`, `ci.yml`) installs no tool. It skips what *make check on a
  Mac* lists, with the `socat` subtest of `TestShellClientsReturnWhenTheServerCloses` (a Mac without
  `socat` would print it too), and the two rows below.
- ***Gate verify*** (`gate-verify.yml`) runs each image as two jobs that start together
  (`run-gate.sh --part`). The **integration** job (the static checks, the unit stage and the
  integration stage) installs MySQL Shell and no shell-client tool, so the `socat` subtest skips in
  both stages. Its unit stage skips what *Check* skips; its integration stage skips what
  *Integration on the local estate* lists, with the `socat` subtest and the two rows below. The
  **e2e** job runs the shell E2E tests and the validation demos, no Go test, so it prints none of
  these lines; it installs `nc` and `socat` for E2E test 20, which fails, not skips, without them.

| Test | Why it skips | Where it runs |
|---|---|---|
| `TestSetupGracefulShutdown_TwoPhase` | `CI=true`, which GitHub sets: the test needs a real signal, which that environment does not take | a Mac, the local estate |
| `TestStopPhases` | the same: it runs its first part, from code, and skips before the signal | a Mac, the local estate |

### GitHub matrix rows

A row job runs the integration inventory on a throwaway pair, on an Ubuntu runner with `CI=true` that
`matrix-verify.yml` gives no `socat`. It skips what *GitHub Linux* lists for the *Check* job, **the
row tests of** *Integration on the local estate* **on every row but their own**, and the group-only
tests of the same section on every row but a `group` row, which runs only the unfinished-work guard tests. These skip on a row
instead:

| Test | Why it skips | Where it runs |
|---|---|---|
| `TestCheckRangeWarningDetectsFullScanFallback`, `TestCheckRangeWarningFailsClosedWithoutRetainedWarnings` (`internal/key`), `TestMySQLRangeWarning` (`internal/tracking`) | row `max_allowed_packet_64k`: they send their statements straight to MySQL, past preflight, and no statement fits a 64 KiB packet | every other row, and the estate |
| `TestMySQLSourceDeletePermission` | row `partial_revokes_on`: it needs `partial_revokes` OFF; `TestMySQLPartialRevokes` is that row's | every other row, and the estate |

Not skips, so not rows here: `-skip` leaves out `TestMySQLLiveReplicationGate`, `TestMySQLStopDuringHold`
and `TestMySQLStopNeverCutsAPass` (they drive db3, the estate's replica) from every row, and on GitHub
`tests/envmatrix/gh-skip.txt`'s tests; a `group` row runs only the unfinished-work guard tests.

### No place above

A skip that no place above meets: a SKIP of one of these is a finding, a host that is broken or one
the project does not support. A name here that a place above also lists is listed there for its own
skip.

| Test | Why it skips | Where it runs |
|---|---|---|
| `shortTemp`, `enableControl` (`cmd/goarchive/cmd`; helpers of every test that makes a socket directory) | no short directory for a socket: `/tmp` is missing and the system's temporary directory is too long | every host with a `/tmp`, and Windows |
| `TestMaxLen` | no path limit is recorded for the OS: it is not darwin, linux or windows | those three |
| `TestPathLength` | the temporary directory leaves no room for a job name under the path limit | every host with a normal temporary directory |
| `TestMySQLMixedCaseNames` | `lower_case_table_names` is 2, which only a macOS server has: only 0 and 1 run on Linux servers | the estate (0) and row `lower_case_table_names_1` |
| `TestCheckIntervalDuration`, `TestCheckIntervalBound`, `TestWholeNumberGettersRoundTrip`, `TestSettersRefuseWhatTheFileRefuses`, `TestToleranceStepDoesNotOverflow`, `TestGroupBoundStepDoesNotOverflow` | an `int` narrower than 64 bits has no value of the size they need: a 32-bit build | every 64-bit build |
| `TestIdentityDoesNotFollowALink`, `TestJudgeSocket`, `TestJudgeDirKind` | a symbolic link cannot be made: a Windows account without the privilege (GitHub's runner has it, and runs them) | every place above |
| `TestStartControlRefusals` (`cmd/goarchive/cmd`), `TestDeviceNameLoadsWhereNotReserved` (`internal/jobspec`) | Windows, where no job runs them: the Windows job runs the control packages and `TestControl` only | Linux and macOS |
| `TestImageRunTestIgnoresDockerWarnings` (`tests/harness`) | no `bash` on the `PATH` | every host with `bash` |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MYSQL_ROOT_PASSWORD` | (required) | MySQL root password (fallback for `*_PASSWORD` vars) |
| `TEST_SOURCE_HOST` | 127.0.0.1 | Source MySQL host |
| `TEST_SOURCE_PORT` | 3305 | Source MySQL port |
| `TEST_SOURCE_USER` | root | Source MySQL user |
| `TEST_SOURCE_PASSWORD` | (from .env) | Source MySQL password |
| `TEST_SOURCE_DB` | sakila | Source database name |
| `TEST_DEST_HOST` | 127.0.0.1 | Destination MySQL host |
| `TEST_DEST_PORT` | 3307 | Destination MySQL port |
| `TEST_DEST_USER` | root | Destination MySQL user |
| `TEST_DEST_PASSWORD` | (from .env) | Destination MySQL password |
| `TEST_DEST_DB` | sakila_archive | Destination database name |
| `TEST_REPLICA_HOST` | 127.0.0.1 | Replica MySQL host (replication-lag tests) |
| `TEST_REPLICA_PORT` | 3308 | Replica MySQL port |
| `TEST_MATRIX_ROW` | (unset) | The environment-matrix row the suites run under; `run-matrix.sh` sets it for each row. A test tied to one row reads it, so on that row it fails instead of skipping when the row's setup did not apply |
| `REPL_USER` | repl | Replication account created on db1 and used by db3 |
| `REPL_PASSWORD` | (from .env) | Its password. Injected into SQL as a single-quoted literal, so a value containing a single quote is **rejected**, not escaped. |
| `REPL_POSITIONING` | gtid | `gtid` or `snapshot`. The setup script enforces the matching `Auto_Position` (1 or 0), so a snapshot estate never silently accepts a stray GTID config. |
| `SAKILA_DIR` | `tests/sakila-db` | Sakila SQL files location (auto-defaulted by run-tests.sh) |
| `DUMP_DIR` | `/tmp/db1_schema_dump` | Temp dir for destination schema dump |
| `GOARCHIVE_BIN` | `bin/goarchive` | Binary the Sakila E2E suite runs. Set it to test a **different build** — an older release, say — against the same suite. |

**`GOARCHIVE_BIN` behaves differently depending on whether you set it.** Left unset, the
runner rebuilds `bin/goarchive` once at the start of each invocation, even if the file
already exists. Later calls in that invocation reuse the successful build. Set explicitly, a missing binary
is an **error** naming the path — the runner will not build the current tree in its place,
because that would silently test a build you did not ask for and pass. It is resolved once,
up front, and the suite prints `Binary under test: <path>` before the first test.

## Troubleshooting

**"connection refused"** — databases aren't up:
```bash
cd tests && ./scripts/check-servers.sh && docker compose up -d
```

**"table doesn't exist"** — Sakila not loaded; run `./scripts/run-tests.sh --setup`.

**`Duplicate entry` on an E2E copy** — leftover state, not a regression. Reseed:
`make e2e`, or `./scripts/run-tests.sh --setup`.

**`tracking tables in schema "sakila_archive" … this GoArchive requires tracking-schema label
"3.0"`** — the destination holds tracking tables from another release. `make test-reset`
rebuilds the estate (it drops `sakila_archive`).

**Clean slate:**
```bash
cd tests
docker compose down -v      # the -v is what removes the data volumes
./scripts/run-tests.sh --setup
```

**Permission denied on scripts:** `chmod +x scripts/*.sh`.

## File Structure

| File/Directory | Description |
|----------------|-------------|
| `scripts/run-tests.sh` | Entry point: owns the environment, the flags and the dispatch |
| `e2e/README.md` | The E2E test-file format and how to add a test |
| `e2e/lib/` | The E2E harness — engine, registry, assertions, estate helpers, the tracking-table reader and the interrupt/resume arm |
| `e2e/<category>/NN-*.test.sh` | One declaration file per test, grouped by the command under test |
| `harness/` | Database-free tests of the test infrastructure: the E2E library (against a fake `mysql-query.sh`), version and toolchain drift, the consumer policy, every tracked Markdown file's relative links and anchors (`TestDocLinks`), the doc headings that error messages quote (`TestErrorMessagesQuoteDocHeadings`), actionlint on the workflows (`TestWorkflowsPassActionlint`), and the integration suites' pools (`TestIntegrationPoolsUseOpenPool`) |
| `envmatrix/` | The environment matrix: `rows.tsv`, `run-matrix.sh`, the judge, and `group/`, dbsgomysql's Group Replication cluster |
| `scripts/check-servers.sh` | Database connectivity checker |
| `scripts/get_sakila_db.sh` | Downloads the Sakila database |
| `scripts/dump_master.js` | MySQL Shell script for schema dump |
| `scripts/create_archive.js` | MySQL Shell script for loading schema |
| `scripts/reset_source.js` | MySQL Shell script for resetting source |
| `configs/*.yaml.template` | Tracked test configs (local `*.yaml` rendered from these) |
| `results/` | Per-test logs (`test_<n>.log`, plus `test_<n>.run2.log` for a resume test) and summary |
| `sakila-db/` | Sakila database files (downloaded) |
| `docker_files/my.cnf.d/` | Per-server MySQL config, bind-mounted read-only |
| `compose.yml` | Docker Compose configuration (datadirs are named volumes) |

## Adding New Tests

**The E2E test format and the full procedure live in
[`e2e/README.md`](e2e/README.md).** A test is now a declaration file at
`e2e/<category>/NN-<slug>.test.sh` — there is no `case` arm to edit.

The short version:

1. Author `configs/testNN_<name>.yaml.template` (the `.template` is the only
   tracked file; the `.yaml` is rendered every run).
2. Add `e2e/<category>/NN-<slug>.test.sh` declaring `test_name`, `config_file`,
   `tables`, `mode`, and — for a working test — `command`, `verify_method`,
   `min_duration`, `expected_rows`, and `orphan_checks` when `tables` names more
   than one table. **Those last four fail closed**: omit one and the test is
   refused rather than run.
3. Add `NN` to the matching `run_e2e_suite` list in `scripts/run-tests.sh` → `main`: the
   working suite's or the validation demos'.
4. Update the catalogue in this file and the category's `README.md`.
5. Verify: `./scripts/run-tests.sh --sakila -t NN`.

Measure every row count from a real run rather than computing it — Sakila's PK
columns are not contiguous, which is why `payment_id <= 2000` yields 1999 and
`payment_id <= 8024` yields 8022.

### Integration tests that do not leak state

Create the test's own schemas with its package's scratch helper (`internal/engine`: `scratch`),
which drops them when the test ends, and give each job a name of its own. Read MySQL facts
through dbsgomysql, not raw `SHOW` queries.

Destructive CLI tests that intentionally use broken-schema fixtures must pass
`--skip-dangerously-validate-preflight-allow-experimental`, since `archive`, `purge`, and `copy-only` run
preflight at startup.
