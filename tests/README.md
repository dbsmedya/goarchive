# GoArchive Test Suite

This directory is the **source of truth** for running GoArchive's unit,
integration, and Sakila end-to-end (E2E) tests.

## Overview

| Test Type | Description | Command |
|-----------|-------------|---------|
| **Unit** | Fast, in-memory (sqlmock); no DB required | `go test ./... -count=1` |
| **Integration** | Real-DB tests behind the `integration` build tag; reseed first | `./scripts/run-tests.sh --setup --integration-only` |
| **Sakila E2E (working)** | Archives that run to completion (tests 03–04) | `make e2e` |
| **Sakila E2E (demos)** | Configs that intentionally fail preflight (tests 01–02) | `make e2e-examples` |

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

Four focused tests: two working archives and two preflight-guardrail
demonstrations. Configs live in `tests/configs/` (`.yaml` is rendered locally
from the tracked `.yaml.template`; only `.template` files are committed).

### Working configurations — archive runs to completion

| Test | Config | Shape | What it exercises |
|------|--------|-------|-------------------|
| **03** | `test03_payment_batch.yaml` | `payment` (root, single-col PK) | High-volume multi-batch copy→verify→delete (`batch_size=100`, `payment_id <= 2000`) |
| **04** | `test04_rental_payment.yaml` | `rental → payment` | 2-level tree archive (`rental_id <= 200`); non-diamond GDPR-shaped subgraph |

### Validation demos — preflight MUST fail

The runner **inverts** pass/fail for demos: a demo "passes" when `validate` fails
with the *expected* error category. An unexpected `validate` **success** is the
regression. (`EXPECTED FAILURE matched` in the log = good.)

| Test | Config | Expected error | Why |
|------|--------|----------------|-----|
| **01** | `test01_one_to_one.yaml` | `COMPOSITE_PK_CHECK` | Config includes Sakila's composite-PK tables `film_actor` (`actor_id, film_id`) / `film_category` (`film_id, category_id`); GoArchive identifies/deletes by a single PK column, so a multi-column PK is rejected up front. |
| **02** | `test02_one_to_many.yaml` | `FK_INDEX_CHECK` | `language → film` where the in-graph FK column is not indexed (unindexed FKs make child-table deletes table-scan). |

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
cp tests/dot.env tests/.env
# Edit tests/.env and verify the settings
```

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
# Working tests (03–04) — archives run to completion
make e2e                                                # short form
./scripts/run-tests.sh --sakila --skip-docker           # explicit

# Full bootstrap (docker + DB seed + working tests)
make e2e-setup                                          # short form
./scripts/run-tests.sh --setup --sakila                 # explicit

# Validation demos (01–02) — preflight MUST fail
make e2e-examples                                       # short form
./scripts/run-tests.sh --sakila-examples --skip-docker  # explicit

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
./bin/goarchive archive --job archive-payment-rows --config "$CFG" --skip-verify

# Verify the destination received the rows
mysqlsh --uri "root:$MYSQL_ROOT_PASSWORD@127.0.0.1:3307/sakila_archive" --sql \
  -e "SELECT COUNT(*) FROM payment WHERE payment_id <= 2000;"
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
| `FK_INDEX_CHECK` | Keep deletes efficient | An FK column is not indexed |

> **Known limitation (not yet fixed):** `FK_COVERAGE_CHECK` is currently
> **shadowed** by `FK_INDEX_CHECK`. The FK-index check runs first and, for an
> out-of-graph table referencing the graph, reports that table's FK column as
> "unindexed" (it never computes the index status for tables outside the graph),
> aborting before coverage is reached. So a genuinely-uncovered config tends to
> surface as `FK_INDEX_CHECK` rather than `FK_COVERAGE_CHECK`. There is therefore
> no dedicated `FK_COVERAGE_CHECK` demo. See `.ayder/002-FK_COVERAGE_CHECK_BUG.md`.

## Test Output

Each Sakila test prints a header and a verdict; per-test logs are written to
`results/test_<n>.log`.

- **Working test** → runs `validate → dry-run → archive` and ends with
  `Result: PASS` (plus `records_copied` / `records_deleted`).
- **Demo test** → `validate` fails; the runner prints
  `EXPECTED FAILURE matched` and `Result: PASS` when the category matches, or
  `Result: FAIL (wrong error category)` otherwise.

A summary (`SAKILA INTEGRATION TEST SUMMARY`) is generated at the end with a
`Passed: N / Failed: N` line.

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

## Troubleshooting

**"connection refused"** — databases aren't up:
```bash
cd tests && ./scripts/check-servers.sh && docker compose up -d
```

**"table doesn't exist"** — Sakila not loaded; run `./scripts/run-tests.sh --setup`.

**`destination already contains a row … Duplicate entry`** — leftover state, not a
regression. Reseed: `./scripts/run-tests.sh --setup`.

**Clean slate:**
```bash
cd tests
docker compose down
rm -rf docker_files/db_data
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
| `docker_files/` | Docker volume data |
| `compose.yml` | Docker Compose configuration |

## Adding New Tests

1. Create `configs/testNN_description.yaml.template` (and render the local
   `.yaml` from it). Destination loaded from a DDL-only dump needs
   `safety.disable_foreign_key_checks: true`.
2. Add a `case` entry to `run_sakila_test()` in `scripts/run-tests.sh`:
   - `mode="working"` → archive runs end-to-end; set `tables="..."`.
   - `mode="example"` → preflight must fail; set `expected_error="CATEGORY"` to
     the exact tag (e.g. `COMPOSITE_PK_CHECK`, `FK_INDEX_CHECK`, `INTERNAL_FK_COVERAGE`).
3. Wire the number into the dispatch lists in `main()`:
   - Working → `run_sakila_tests "3 4 NN" "working"`.
   - Demos → `run_sakila_tests "1 2 NN" "validation demos"`.
4. Verify: `./scripts/run-tests.sh --sakila -t NN` (or `--sakila-examples -t NN`).
