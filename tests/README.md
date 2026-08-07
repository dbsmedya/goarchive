# GoArchive Test Suite

This directory is the **source of truth** for running GoArchive's unit,
integration, and Sakila end-to-end (E2E) tests.

## Overview

| Test Type | Description | Command |
|-----------|-------------|---------|
| **Everything** | **The full gate, in the only correct order** | **`make gate`** |
| **Unit** | Fast, in-memory (sqlmock); no DB required | `go test ./... -count=1` |
| **Integration** | Real-DB tests behind the `integration` build tag; reseed first | `./scripts/run-tests.sh --setup --integration-only` |
| **Characterization** | Pinned behaviour, checked against a recorded baseline | `make characterization` |
| **Sakila E2E (working)** | Archive, purge and copy-only runs that complete (tests 03–07) | `make e2e` (reset + seed + run) |
| **Sakila E2E (demos)** | Configs that intentionally fail preflight (tests 01–02) | `make e2e-examples` |

### `make gate` — use this rather than assembling the steps

It runs: estate reachability → `fmt-check` `vet` `lint` `consumer-policy` `deadcode` → unit →
integration (`--setup`) → characterization → `make e2e` → `make e2e-examples`, and ends with a
summary:

```
================================================
  GATE SUMMARY
================================================
  estate             test estate reachable on 3305, 3307, 3308
  fmt-check          ok
  ...
  integration        PASS=1026 FAIL=0 SKIP=1
  characterization   OK (60 / 304 / 364 / 0 / 0)
  e2e                Passed: 5  Failed: 0
  e2e-examples       Passed: 2  Failed: 0
================================================
  GATE COMPLETE - every stage above exited 0
```

**Read the summary, not the scrollback.** The run emits thousands of lines — `mysqlsh` progress
spinners, per-test output, schema dumps — and the numbers that matter are scattered through it.
An independent verifier once reported six stages out of eight in good faith, because the other
two had scrolled past.

On failure it stops, prints the summary **so far** with the broken stage marked, names the
per-stage log, and exits with **that stage's own exit code**. Per-stage logs land in
`tests/results/gate/` (gitignored).

> **Why a script (`scripts/run-gate.sh`) rather than Makefile recipe lines.** Collecting each
> stage's output in make would mean `cmd | tee`, and that returns **tee's** exit status — a
> failing stage would exit 0 and the gate would report green on red. make's default shell has no
> reliable `pipefail`. The script takes the status from `PIPESTATUS[0]`, the command's own, and
> checks it explicitly per stage. Same pattern as
> `e2e-tests-must-run-after-setup` → `require-e2e-seed.sh`: the Makefile names the target, a
> script owns the logic.

**The order is load-bearing, not stylistic.** `make e2e` begins with `test-reset`, which
destroys the estate; run it before integration or characterization and those fail for reasons
unrelated to your change. Integration needs `--setup`, or stale heartbeat state fabricates
failures. `make gate` encodes both so nobody has to remember them.

It fails fast on a dead estate rather than letting every step die with
`Can't connect to MySQL server`, which reads exactly like a product failure. Note the harness's
own `selftest_get_row_count_fails_loud` cannot catch that case — it probes an always-refused
port on purpose, so it passes while the real databases are down.

Credentials first, as always: `set -a; source tests/.env; set +a`.

#### The characterization baseline is checked, not recited

`tests/characterization-baseline.txt` holds the recorded counts and
`scripts/check-characterization-baseline.sh` does the comparison.

> **Never count it by hand.** The suite nests **two** levels deep. The obvious
> `grep -c '^    --- PASS'` returns **206** against a baseline of 304 and looks like a 98-test
> regression — the missing 98 are subtests at 8-space indent. That misfire has happened, on a
> change touching zero `.go` files. Run `make characterization` and read its verdict.

Raising the baseline is a decision requiring prior authorization, not a side effect of adding
tests. When authorized, update the file and CLAUDE.md's pointer together.

> **Integration + E2E need a freshly-reseeded destination — the #1 source of
> false failures.** The real-DB tests archive Sakila into `sakila_archive` and
> several rely on it starting empty; a prior run leaves rows behind and aborts
> with `destination already contains a row … Duplicate entry` (leftover state,
> **not** a regression). The `--setup` flag reseeds source + destination first.
> The real-DB tests also DELETE from source, so they are run-once against a fresh
> `--setup`. To run integration tests via `go test` directly:
> `./scripts/run-tests.sh --setup` once, then
> `INTEGRATION_FORCE=true go test -tags=integration ./internal/archiver/... -count=1`.

## Sakila E2E Test Suite

Seven focused tests: five working runs — three archives, one purge and one
copy-only — and two preflight-guardrail demonstrations.

**Configs live in `tests/configs/`, and the tracked file is always the
`.yaml.template`.** `run-tests.sh` renders each one to its `.yaml` on every run,
substituting the `MYSQL_ROOT_PASSWORD` placeholder from `tests/.env`. The
rendered `.yaml` holds a real password, so it is gitignored — and it is
**overwritten every run**, so edit the template, never the generated file.

### Working configurations — the command runs to completion

| Test | Config | Shape | What it exercises |
|------|--------|-------|-------------------|
| **03** | `test03_payment_batch.yaml` | `payment` (root, single-col PK) | High-volume multi-batch copy→verify→delete (`batch_size=100`, `payment_id <= 2000`); verification method inherited (`count`) |
| **04** | `test04_rental_payment.yaml` | `rental → payment` | 2-level tree archive (`rental_id <= 200`); non-diamond GDPR-shaped subgraph |
| **05** | `test05_payment_verify_sha256.yaml` | `payment`, same slice as 03 | **`verification.method: sha256`**, declared explicitly. Also the suite's only `INSERT IGNORE` copy path — `count` forces a plain `INSERT`, `sha256` does not. |
| **06** | `test06_payment_purge.yaml` | `payment`, **half the table** (`payment_id <= 8024` → 8022 of 16044 rows) | The suite's only **`purge`** — the only command that deletes without copying, and the only one with **no verify stage at all**. Also the first E2E exercise of `PreflightProfileSourceOnly`. |
| **07** | `test07_rental_payment_copyonly.yaml` | `rental → payment`, same slice as 04 | The suite's only **`copy-only`** — the only command that never deletes, so the assertion that carries it is the negative one: **the source must be unchanged**. Also the first E2E run of `copyonly_orchestrator.go`'s batch loop. |

> **03 and 05 are the same archive with different verification methods.** That is
> deliberate: a failure in 05 alone isolates to the method. Keep them in step — if
> you change one's slice or batch sizes, change the other's.

> **Why test 06 purges half the table rather than a token slice.** Purge's
> unrecoverable failure is deleting *more* than the `where` clause selects, and a
> half-table slice makes the gap between a correct run (8022) and a runaway one
> (16044) unmistakable. The boundary is `8024`, not `8022`, because `payment_id`
> is **not contiguous** — two ids below it are missing. Both numbers were queried
> against the fixture, not computed, and both move together if the Sakila data
> file changes. That is what `expected_rows` is there to report.

> **04 and 07 are the same graph and the same slice under different commands**, so
> a failure in 07 alone isolates to `copy-only` — with one deliberate exception.
> **Their pacing differs, and that is not an oversight.** 04 declares no
> `processing:` block, so 200 rentals arrive in a *single* batch, and one batch
> exercises one sleep call: no per-batch accumulation bug is reachable, because
> there is no second batch. 07 sets `batch_size: 10` for 20 batches. If you make
> them identical, say which one you meant to weaken.

#### The pacing floor — the one timing number that is not machine-dependent

Wall-clock varies with the container, the host and the filesystem. The **sleep**
component does not: it is arithmetic over the config, and no machine can go below
it. Each working test declares that floor as `min_duration`, and the runner fails
the test if the run came in under it.

```
floor = ceil(rows ÷ batch_size) × sleep_seconds
      + Σ over batches of (delete chunks per table − 1) × delete_sleep_seconds
```

`sleep_seconds` fires after **every** batch including the last; `delete_sleep_seconds`
**skips** each table's final chunk, so 5 chunks means 4 pauses.

| Test | rows | `batch_size` | batches | batch sleep | delete sleep | **floor** |
|---|---|---|---|---|---|---|
| **03** | 1999 | 100 | 20 | 20 × 0.2 = 4.0 s | 80 × 0.2 = 16.0 s | **20.0 s** |
| **04** | 200 | 1000 *(default)* | 1 | 1 × 1.0 = 1.0 s | 0 *(default)* | **1.0 s** |
| **05** | 1999 | 100 | 20 | 20 × 0.2 = 4.0 s | 80 × 0.2 = 16.0 s | **20.0 s** |
| **06** | 8022 | 500 | 17 | 17 × 0.2 = 3.4 s | 64 × 0.2 = 12.8 s | **16.2 s** |
| **07** | 200 | 10 | 20 | 20 × 0.5 = 10.0 s | — *(no delete phase)* | **10.0 s** |

Test 04 declares no `processing:` block at all, so it inherits the global defaults
(`batch_size: 1000`, `sleep_seconds: 1`, `delete_sleep_seconds: 0`).

Test 06's 17th batch is a 22-row tail. It contributes to the **batch** term but
**not** the delete term — 22 rows is a single chunk at `batch_delete_size: 100`,
and a table's final chunk is never followed by a sleep. Only the 16 full batches
produce the 4 gaps each that make 64. Counting the tail as though it had gaps
inflates the floor and produces a gate no correct run can pass.

Its sizing (`500 / 100`) is chosen against this floor rather than copied from 03.
Over a slice four times larger, 03's `100 / 20` would cost **80.4 s** — 64 s of it
pure `sleep` — and prove nothing that `500 / 100` does not, since both give five
delete chunks per batch.

**Test 07 has no delete term at all, and the formula's second line must be dropped
for it.** `copy-only` runs as `batchCopyVerify`, and the delete phase is gated on
`batchFull || batchDeleteOnly` (`internal/archiver/batch_pipeline.go:146`), so
`DeletePhase` never runs. `delete_sleep_seconds` and `batch_delete_size` are inert
for this command — the config sets neither, and including a delete term in its
floor would build a gate no correct run can pass. Whenever you add a test, derive
the floor from the phases that command actually executes, not from the shape of
the formula.

**This is a one-sided bound, which is why it is safe to assert.** A slow or loaded
machine can only push the measured duration *up*, so it can never fail.

**It is not a test of sleeping.** The floor encodes the run's *shape* — this job
must process twenty batches with these pauses — so anything that changes that
shape lands below it, **including causes nobody anticipated**, and it catches them
while the command still exits 0 with entirely plausible output:

- a config that silently stopped being read, so the global defaults applied;
- discovery returning fewer roots than the `where` slice implies;
- an early return introduced by a later refactor;
- a resume path that skipped work it should have replayed;
- a batch loop that ran once where it should have run twenty.

That is the point. Each working test now asserts on four **independent** axes,
and the floor is the only one that needs no model of what the command means:

| Assertion | Answers | Needs to know |
|---|---|---|
| post-condition | did **exactly** the right rows move, in the right direction | what the command does, and how many rows the slice holds |
| verify stage | did verification run, naming its method | the log contract |
| referential integrity | are the rows that moved **internally consistent** | the graph's parent→child edges |
| **pacing floor** | **did the run have the size it was configured to have** | **only the config** |

> **Referential integrity is a separate axis, not a refinement of the count.**
> Both can hold while the other fails — 200 payments referencing 200 rentals that
> never arrived satisfies every count. It applies only to multi-table tests, where
> there is an edge to check, and it exists because every test that *copies* must
> set `safety.disable_foreign_key_checks: true`: a copied child normally references
> out-of-graph parents the destination does not hold, so the destination cannot
> reject an orphan on its own.

> **The floor and the exact count cover opposite directions, and neither
> substitutes for the other.** The floor is one-sided: a run that did *less* than
> configured finishes early and fails it, but a run that did *more* simply takes
> longer and passes. The exact count is what catches that second direction. Before
> `expected_rows` existed, a purge of the whole table and an archive of the whole
> table both passed every assertion in the suite.

It is compared against **goarchive's own reported duration**, not the test's
elapsed time: the latter includes the source reset, `validate` and `dry-run`,
which dwarf the floor and would hide a lost throttle. For scale, measured against
the 20.0 s floor, tests 03 and 05 spend about **0.7 s** doing actual work.

### Validation demos — preflight MUST fail

The runner **inverts** pass/fail for demos: a demo "passes" when `validate` fails
with the *expected* error category. An unexpected `validate` **success** is the
regression. (`EXPECTED FAILURE matched` in the log = good.)

| Test | Config | Expected error | Why |
|------|--------|----------------|-----|
| **01** | `test01_one_to_one.yaml` | `COMPOSITE_PK_CHECK` | Config includes Sakila's composite-PK tables `film_actor` (`actor_id, film_id`) / `film_category` (`film_id, category_id`); GoArchive identifies/deletes by a single PK column, so a multi-column PK is rejected up front. |
| **02** | `test02_one_to_many.yaml` | `FK_COVERAGE_CHECK` | `language → film` pulls `film` into the graph, but `film` is also referenced by out-of-graph `inventory`/`film_actor`/`film_category`; deleting archived films would violate those FKs, so every referencing table must be covered. |

> **Why no `customer → rental → payment` (GDPR) test?** `payment` references
> **both** `customer` and `rental`, and `rental` references `customer` — a diamond.
> GoArchive's graph is a strict tree, and `INTERNAL_FK_COVERAGE` requires every
> in-graph FK edge to be a represented parent→child relation, so any rooting
> leaves one edge uncovered. Test 04 (`rental → payment`) is the closest working
> multi-level shape (`customer`/`staff` stay out-of-graph as upstream parents).
> The earlier tests 04–10 (film hierarchy / actor / category / isolated
> job_schema) were removed: several archived composite-PK association tables by a
> single non-key column (over-delete, now blocked by `COMPOSITE_PK_CHECK`); the
> rest were redundant.

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

Forgetting is safe in the sense that it cannot pass silently: the integration suites
**fail** with a message naming the fix. They do not skip. A skipped suite prints `ok`
and exits 0, which is indistinguishable from a green run unless you pass `-v` and read
the `--- SKIP` lines — so the environment is proven once, up front, before any test
runs.

Default topology:
- **Source** (db1): `127.0.0.1:3305/sakila`
- **Archive** (db2): `127.0.0.1:3307/sakila_archive`
- **Replica** (db3): `127.0.0.1:3308` (optional, replication-lag tests)

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

### Integration tests

Real-DB tests behind the `integration` build tag. `--setup` reseeds first so the
destination starts empty (see the Overview note):

```bash
./scripts/run-tests.sh --setup --integration-only
```

### Sakila E2E tests

```bash
# The whole procedure — test-reset, then e2e-setup, then the tests (03–07)
make e2e

# Validation demos (01–02) — preflight MUST fail. Needs a seeded estate.
make e2e-examples

# The individual steps, if you know why you want one
make test-reset                          # 1. destroy
make e2e-setup                           # 2. rebuild + seed
make e2e-tests-must-run-after-setup      # 3. run; refuses unless step 2 ran

# Target a single test
./scripts/run-tests.sh --sakila -t 4                    # working rental→payment
./scripts/run-tests.sh --sakila-examples -t 1           # composite-PK demo
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

# 2. Plan: shows tables, copy order (parents first), delete order (children
#    first), and estimated row counts
./bin/goarchive plan --job archive-payment-rows --config "$CFG"

# 3. Validate (fails fast on a bad config): connectivity, table existence,
#    single-column/integer PK, FK index + coverage, cycle detection, triggers.
#    Add --force-triggers when the schema has DELETE triggers (Sakila does).
./bin/goarchive validate --config "$CFG" --force-triggers

# 4. Dry-run: discovers affected rows and reports what would be copied/deleted;
#    changes nothing.
./bin/goarchive dry-run --job archive-payment-rows --config "$CFG"

# 5. Archive: copy → verify → delete. Logs progress to archiver_job and the
#    per-job archiver_job_log_<id> table.
#    Do NOT add --skip-verify: it disables the verify stage, so the source rows
#    are deleted whether or not the copy landed.
./bin/goarchive archive --job archive-payment-rows --config "$CFG"

# Confirm the destination received the rows
tests/scripts/mysql-query.sh 3307 \
  "SELECT COUNT(*) FROM sakila_archive.payment WHERE payment_id <= 2000;"
```

### Example: a demo that fails preflight (Test 01)

```bash
$ ./bin/goarchive validate --config tests/configs/test01_one_to_one.yaml --force-triggers
❌ Preflight checks failed: COMPOSITE_PK_CHECK: Composite primary keys are not supported.
   GoArchive identifies and deletes rows by a single primary-key column; a multi-column
   PK would over-match and risk deleting rows outside the archived set.
   (tables: [film_actor(2-column PRIMARY KEY) film_category(2-column PRIMARY KEY)])
```

The runner treats this as a **pass** because the failure matches the expected
category (`COMPOSITE_PK_CHECK`).

## Preflight Checks

`validate` (and the startup preflight of `archive`/`purge`/`copy-only`) runs a
fail-fast battery before any data moves:

| Check | Category tag | Severity | Detects |
|-------|--------------|----------|---------|
| Table existence | `TABLE_EXISTENCE_CHECK` | Error | A graph table missing from the source |
| Storage engine | `STORAGE_ENGINE_CHECK` | Error | Non-InnoDB table (no transactional copy) |
| Single-column PK | `COMPOSITE_PK_CHECK` | Error | A composite (multi-column) PRIMARY KEY |
| Root PK type | `ROOT_PK_TYPE_UNSUPPORTED` | Error | Non-integer root primary key |
| FK index | `FK_INDEX_CHECK` | Error | An FK column without an index (slow deletes) |
| FK coverage | `FK_COVERAGE_CHECK` | Error | A table **outside** the graph with an FK **into** the graph |
| Internal FK coverage | `INTERNAL_FK_COVERAGE` | Error | An FK **between two in-graph tables** not represented as a relation edge |
| DELETE triggers | `DELETE_TRIGGER_CHECK` | Error* | DELETE triggers on source tables (`--force-triggers` to proceed) |
| CASCADE rules | — | Warning | `ON DELETE CASCADE` FKs (may delete more than expected) |

\* fatal unless `--force-triggers` is passed.

### `INTERNAL_FK_COVERAGE` — the relation-completeness check

This is the check that most often blocks a multi-table config. It requires that
**every FK constraint between two tables that are both in the graph** is
represented as a parent→child relation edge. Missing an edge would cause a
delete-phase FK violation, so it is caught at validation time.

```
INTERNAL_FK_COVERAGE: Internal FK relationships not matching configuration:
  - payment.customer_id -> customer.customer_id (constraint: fk_payment_customer) [no graph edge]
```

It is what makes the `customer → rental → payment` diamond unrepresentable (see
the suite note above): `payment` has two in-graph parents, so one edge is always
left uncovered.

### `FK_COVERAGE_CHECK` vs `FK_INDEX_CHECK`

| Check | Purpose | Fails when |
|-------|---------|-----------|
| `FK_COVERAGE_CHECK` | Don't leave dangling references into the archived set | An out-of-graph table has an FK pointing at an in-graph table (its rows would block/orphan the parent delete) |
| `FK_INDEX_CHECK` | Keep deletes efficient | An **in-graph** FK column is not indexed |

> **History:** `FK_COVERAGE_CHECK` was previously **shadowed** by a false-positive
> `FK_INDEX_CHECK` — the index check ran first and flagged out-of-graph
> referencing tables as "unindexed" (it never computes index status for tables
> outside the graph), aborting before coverage was reached. Fixed in the
> `harden/data-safety-p0-p1` branch: `ValidateForeignKeyIndexes` now only checks
> in-graph children, so a genuinely-uncovered config correctly surfaces
> `FK_COVERAGE_CHECK` (test 02 demonstrates this). Because the Sakila schema has
> every FK column indexed, there is no E2E `FK_INDEX_CHECK` demo — that check is
> covered by unit tests only.

## Test Output

Each Sakila test prints a header and a verdict; per-test logs are written to
`results/test_<n>.log`.

- **Working test** → runs `validate → dry-run → <command>`, then **asserts the outcome**,
  and ends with `Result: PASS`.

  Four assertions run after the command, and any one of them fails the test:

  - **The verify stage must have run, naming its method** — the log must carry
    `Starting verification (method=<method>)`, and the console echoes
    `confirmed: verification ran with method=count`.
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
rows=$(tests/scripts/mysql-query.sh 3307 "SELECT job_name FROM goarchive_test.archiver_job;") || exit 1
[ -z "$rows" ] && echo "clean"
```

**Prefer the wrapper over raw `mysqlsh`.** It passes `--no-defaults`, without which `mysqlsh`
reads `~/.my.cnf` and can connect as whoever that file names when you forget the password —
succeeding locally and failing in CI. Integration residue lives in **`goarchive_test`**;
`sakila_archive` is the E2E destination.

## Test result counts

Every Go layer the runner executes reports:

```
[INFO] integration: PASS=1026 FAIL=0 SKIP=1 (go test exit 0)
```

and **fails when nothing ran**. `go test` prints `ok` and exits 0 for a `-run` pattern that
matched no test, a missing build tag, or a suite where everything skipped — so a gate that
only counts failures passes all three. The runner counts passes instead, which requires `-v`,
so `-v` is always on internally; the full log prints only with `--verbose` or on failure.

Skips are reported by name rather than swallowed. `MIN_PASS=<n>` requires at least n passing
tests — inclusive, so `MIN_PASS=1026` accepts exactly 1026. It defaults to 1, which only
catches a run that did nothing.

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
| `SAKILA_DIR` | `tests/sakila-db` | Sakila SQL files location (auto-defaulted by run-tests.sh) |
| `DUMP_DIR` | `/tmp/db1_schema_dump` | Temp dir for destination schema dump |
| `GOARCHIVE_BIN` | `bin/goarchive` | Binary the Sakila E2E suite runs. Set it to test a **different build** — an older release, say — against the same suite. |

**`GOARCHIVE_BIN` behaves differently depending on whether you set it.** Left unset, the
runner builds `bin/goarchive` if it is missing, as before. Set explicitly, a missing binary
is an **error** naming the path — the runner will not build the current tree in its place,
because that would silently test a build you did not ask for and pass. It is resolved once,
up front, and the suite prints `Binary under test: <path>` before the first test.

## Troubleshooting

**"connection refused"** — databases aren't up:
```bash
cd tests && ./scripts/check-servers.sh && docker compose up -d
```

**"table doesn't exist"** — Sakila not loaded; run `./scripts/run-tests.sh --setup`.

**`destination already contains a row … Duplicate entry`** — leftover state, not a
regression. Reseed: `./scripts/run-tests.sh --setup`.

**`legacy GoArchive tracking tables detected`** on every run, even right after
`--setup` — usually leftover state from a killed test process, not a regression.
`orchestrator_integration_test.go` seeds an old-shape `archiver_job` to exercise
legacy detection and drops it in `t.Cleanup`, which does not run when a test
process is killed; because database state lives in a Docker named volume,
plain `make test-down` does not clear it either. Rebuild from scratch:
`make test-reset` (runs `docker compose down -v`, which removes the
`db1_data`/`db2_data`/`db3_data` volumes), then reseed:
`bash tests/scripts/run-tests.sh --setup`.

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
| `scripts/run-tests.sh` | Main test runner (unit / integration / Sakila E2E) |
| `scripts/check-servers.sh` | Database connectivity checker |
| `scripts/get_sakila_db.sh` | Downloads the Sakila database |
| `scripts/dump_master.js` | MySQL Shell script for schema dump |
| `scripts/create_archive.js` | MySQL Shell script for loading schema |
| `scripts/reset_source.js` | MySQL Shell script for resetting source |
| `configs/*.yaml.template` | Tracked test configs (local `*.yaml` rendered from these) |
| `results/` | Per-test logs (`test_<n>.log`) and summary |
| `sakila-db/` | Sakila database files (downloaded) |
| `docker_files/my.cnf.d/` | Per-server MySQL config, bind-mounted read-only |
| `compose.yml` | Docker Compose configuration (datadirs are named volumes) |

## Adding New Tests

1. Create `configs/testNN_description.yaml.template` — the `.template` is the
   **only** file you author; `run-tests.sh` renders the `.yaml` on the next run.
   A test that **copies** into a destination loaded from a DDL-only dump needs
   `safety.disable_foreign_key_checks: true`. A `purge` test does **not** — the
   setting is consumed only on the copy path, so carrying it there is dead config.

   Two constraints that are easy to trip over:

   - **`NN` is the `case` number**, and the same number goes in the dispatch list
     at step 3. Config filename, `case` arm and dispatch entry must agree.
   - **The job key must be the line immediately after `jobs:`**, with no comment
     on it and none between. `run_archive_job` extracts the job name with
     `grep -A 1 "^jobs:" | tail -1`, so anything else on that line ends up inside
     the name.
2. Add a `case` entry to `run_sakila_test()` in `scripts/run-tests.sh`:
   - `mode="working"` → the command runs end-to-end; set `tables="..."`, plus:
     - `command="archive" | "purge" | "copy-only"` (defaults to `archive`).
       Do **not** add `--force-triggers` anywhere — `run_archive_job` applies it per
       command, and `copy-only` does not accept it.
     - `min_duration="<seconds>"` — the pacing floor, computed from the config with
       the formula above. **Required**, and it fails closed: leave it empty and the
       test fails with *"no min_duration configured"*. Recompute it whenever you
       change the slice, `batch_size`, `batch_delete_size` or either sleep.
     - `verify_method="count" | "sha256" | "none"`. **Required**, and it fails closed:
       leave it empty and the test fails, because no run logs `method=`. Use `"none"`
       only for `purge`, which has no verify stage — `"none"` asserts the verification
       line is *absent*, so a stage that vanishes cannot pass unnoticed.
     - `expected_rows="<n> [<n> ...]"` — the exact rows the run must move, **one per
       entry in `tables`, in the same order**. **Required**, and it fails closed with
       *"no expected row count configured"*. A count that disagrees with `tables` is
       caught up front, before the source reseed, naming both lists. Measure each
       number from a real run rather than computing it — Sakila's PK columns are not
       contiguous, which is why `payment_id <= 2000` yields 1999 and
       `payment_id <= 8024` yields 8022.
     - `orphan_checks="child:fk:parent:parent_pk [...]"` — referential-integrity pairs
       checked in the destination, space-separated. **Required as soon as `tables` has
       more than one entry**, and it fails closed with *"N tables but no orphan_checks
       configured"*, before the source reseed. Exact counts cannot cover this: a test
       that copies runs with `disable_foreign_key_checks: true`, so the destination
       accepts a child whose parent never arrived, and the counts stay right. A
       malformed entry is rejected up front rather than silently checking some other
       pair of tables.
   - `mode="example"` → preflight must fail; set `expected_error="CATEGORY"` to
     the exact tag (e.g. `COMPOSITE_PK_CHECK`, `FK_COVERAGE_CHECK`, `INTERNAL_FK_COVERAGE`).
     `command` and `verify_method` are unused here.
3. Wire the number into the dispatch lists in `main()`:
   - Working → `run_sakila_tests "3 4 5 6 7 NN" "working"`.
   - Demos → `run_sakila_tests "1 2 NN" "validation demos"`.
4. Verify: `./scripts/run-tests.sh --sakila -t NN` (or `--sakila-examples -t NN`).
