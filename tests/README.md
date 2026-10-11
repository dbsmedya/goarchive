# GoArchive Test Suite

This directory is the **source of truth** for running GoArchive's unit,
integration, and Sakila end-to-end (E2E) tests.

## Overview

| Test Type | Description | Command |
|-----------|-------------|---------|
| **Everything** | **The full gate, in the only correct order** | **`make gate`** |
| **Unit** | Fast, in-memory; no DB required. Preflight stages consume injected `dbsgomysql` facts — see [Testing Reference](../docs/README_TESTING.md#unit-tests) | `go test ./... -count=1` |
| **Integration** | Real-DB tests behind the `integration` build tag | `make test-integration`, or `./scripts/run-tests.sh --setup --integration-only` |
| **Sakila E2E (working)** | Archive, purge, copy-only and interrupt/resume runs that complete (tests 01, 03–09 and 13–16) | `make e2e` (reset + seed + run) |
| **Sakila E2E (demos)** | Configs that intentionally fail preflight (tests 02, 10, 11, 12) | `make e2e-examples` |

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
| `run.tsv` | Run ID, source revision, tracked-tree state, start/end and outcome/exit information |
| `summary.tsv` | All ten stages, their status, command/capture exits and current log paths |
| `summary.txt` | The same stage verdicts in a readable form |
| `<stage>.log` | Output from a stage that actually started in this invocation |
| `complete` | Success marker with run ID, revision and `GATE COMPLETE - every stage above exited 0` |

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

## Sakila E2E Test Suite

Sixteen focused tests: twelve working runs — five archives, three purges, two
copy-onlys and two interrupt/resume tests — and four preflight-guardrail
demonstrations. Thirteen run on Sakila; three (14–16) run on the **key-matrix
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
| **13** | `test13_purge_replication.yaml` | `payment`, same selected slice as 03 (1999 selected rows, reasserted before launch) | Stops the real replica applier, observes a purge hold with unchanged rows, restarts it and requires the same process to finish; exact deletes, no copy or verification. |
| **14** | `test14_keymatrix_composite.yaml` | `km_line (order_id, line_no)` → `km_item` → `km_item_note` | Key matrix, **archive**: a composite root, a child on the composite key, a grandchild on a three-column foreign key; composite orphan checks |
| **15** | `test15_keymatrix_binary.yaml` | `km_blob` (`BINARY(16)`) → `km_blob_ref` | Key matrix, **purge**: keys whose bytes are not valid UTF-8 |
| **16** | `test16_keymatrix_latin1.yaml` | `km_name` (`latin1` `VARCHAR`) → `km_name_tag` | Key matrix, **copy-only**: non-ASCII `latin1` keys, verified by sha256 and logged in a `latin1` key column |

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
> **09's `delete_sleep_seconds: 1.0` is the test, not tuning.** The kill has to land
> between batch 1's destination commit, which logs its roots `copied`, and its
> completion, and the only part of that interval with any duration is the throttle
> between delete statements. The engine logs nothing in between, so the harness
> polls the log table for the 100 `copied` roots — one `mysqlsh` call per read — and
> the window must be wide. Its slice is smaller (499 rows) because that throttle
> costs 4 s per batch.

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
| **09** † | 399 | 100 | 3 + 99 | 3 × 0.2 = 0.6 s | 16 × **1.0** = 16.0 s | **16.6 s** |
| **13** | 1999 | 100 | — | 0 | 0 | **0.0 s** |
| **14** | 180 lines | 50 | 3 + 30 | 3 × 0.2 = 0.6 s | 25 × 0.1 = 2.5 s | **3.1 s** |
| **15** | 120 blobs | 50 | 2 + 20 | 2 × 0.2 = 0.4 s | 5 × 0.1 = 0.5 s | **0.9 s** |
| **16** | 60 names | 20 | 3 + 0 | 3 × 0.3 = 0.9 s | — *(copies only)* | **0.9 s** |

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

**13 sleeps nothing.** The length of its hold is the harness's, not the run's.

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

It is compared against **goarchive's own reported time** — the report's
`elapsed: <s>s` line — not the test's elapsed time: the latter includes the source
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
(`brew install mysql-shell`) — the runner checks for it up front and stops with install
instructions if it is missing.

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
`integration environment not loaded: set -a; source tests/.env; set +a`. Each test creates its
own scratch schemas on the source and destination (`ga3m5_<name>_<pid>`) and drops them when it
ends, so tests do not share state; a killed run can leave a scratch schema behind.

#### Interrupted runs

`TestMySQLKillRecovery` (`internal/engine`) kills a helper process (`TestEngineHelperProcess`)
at each crash point of each job type, then restarts the job: archive and purge must finish the
logged roots by delete-only replay and end equal to an uninterrupted run; copy-only must refuse
the duplicate with `resume: false` and finish with `resume: true`. The crash points are named
seams in the engine, not timers. Binary E2E 08 and 09 send real signals to the CLI; test 13
holds and releases a live replication gate in one invocation.

### Environment matrix

The environment matrix runs the integration suites again under **one changed MySQL
environment at a time**: a global variable, a setting fixed at initialization, or another
server version. The rows are in [`envmatrix/rows.tsv`](envmatrix/rows.tsv), and each one states
its expected outcome. Under every row a run must be exact, or refused at startup with a named
error, or fail closed by name before it changes anything (as the INSERT and range checks do on
a session whose `max_error_count` is 0, which the matrix reaches through the suites' own pools:
goarchive's own connections raise such a session to 1024). A row expected to `pass` has no
failing test. A row expected to `refuse:CODE|…` passes
only if every failing test's own output cites one of those codes, a failing parent's own lines
included. A panic, a timeout, a build failure, a test or package that started and never
finished, or a `go test` exit status that disagrees with its output (a killed run) fails any
row. The judge (`envmatrix/judge`) decides each row from `go test -json`.

| Kind | What the runner does |
|---|---|
| `dynamic` | Takes a snapshot of the globals on a long-lived source/destination pair of `MX_IMAGE`, applies the row's `SET GLOBAL` to both servers, runs the suites, then restores the snapshot and compares it |
| `init` | Starts a fresh pair with an extra `my.cnf` (for example `lower_case_table_names = 1`) |
| `version` | Starts a fresh, unchanged pair of another image (Percona and Oracle, 8.0.40 to 9.7) |

```bash
make env-matrix                                   # every row
make env-matrix ROWS="sql_mode_empty v84_oracle"  # some rows
```

- **Needs:** Docker, the local images that `rows.tsv` names, and `tests/.env`.
- **Servers:** the throwaway pairs (`ga-mx<i>-src`/`-dst`, ports 3375–3396) are removed with
  their volumes when the run ends.
- **The estate:** the suites still reach db3 as the replica, so
  `TestMySQLLiveReplicationGate`, which stops db3's applier, is skipped here. Run it on the
  estate alone.
- **Results:** `tests/results/envmatrix/<tag>/` (`latest` links to the newest run): per row
  `verdict.txt`, `go-test.json` and the variable snapshots, plus `SUMMARY.txt`. The command
  exits 1 if any row is not OK.
- **Time:** three pairs work through the rows in parallel (`MX_PAIRS`). A dynamic row takes
  about 50 s, and a fresh pair adds about 30 s to start.
- **Running alone:** do not run the estate suite or lint beside it.
- **Server variables:** the runner and tests may `SET GLOBAL` on these throwaway servers;
  goarchive itself never does.

### Sakila E2E tests

```bash
# The whole procedure — test-reset, then e2e-setup, then the working tests (01, 03–09, 13–16)
make e2e

# Validation demos (02, 10, 11, 12) — preflight MUST fail. Needs a seeded estate.
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
The demos above pin four of them through the CLI; the rest are pinned by the `internal/preflight`
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
3. Add `NN` to the ordered dispatch list in `scripts/run-tests.sh` → `main`
   (`run_e2e_suite "3 4 5 6 7 8 9 NN" "working"`, or `"1 2 10 11 12 NN" "validation demos"`).
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
`--skip-validate-preflight`, since `archive`, `purge`, and `copy-only` run
preflight at startup.
