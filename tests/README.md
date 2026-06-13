# GoArchive Test Suite

This directory contains the complete test suite for GoArchive, including unit tests, integration tests, and real database tests using the Sakila sample database.

## Overview

The test suite includes:

| Test Type | Description | Command |
|-----------|-------------|---------|
| **Unit Tests** | Fast in-memory tests | `go test ./... -count=1` |
| **Integration Tests** | Real-DB tests (build tag `integration`); reseed first | `./scripts/run-tests.sh --setup --integration-only` |
| **Sakila E2E (working)** | Archive test 03 runs to completion | `make e2e` |
| **Sakila E2E (demos)** | Tests 01-02 intentionally fail preflight | `make e2e-examples` |

> **Integration tests require a freshly-emptied destination.** They archive
> Sakila into `sakila_archive` and several rely on it starting empty, so a prior
> archive/E2E run makes them abort with `destination already contains a row …
> Duplicate entry` (leftover state, not a regression). The `--setup` flag
> reseeds source + destination first. To run via `go test` directly, reseed once
> with `./scripts/run-tests.sh --setup`, then
> `INTEGRATION_FORCE=true go test -tags=integration ./internal/archiver/... -count=1`.

### ⚠️ Test Configuration Status

| Test IDs | Status | Use Case | Runner |
|----------|--------|----------|--------|
| **Test 03** | ✅ **Working** | Valid configuration; archive runs to completion | `make e2e` / `--sakila` |
| **Test 01-02** | ❌ **Validation demos** | Preflight MUST fail with a documented error category | `make e2e-examples` / `--sakila-examples` |

**For validation demos:** the runner inverts pass/fail semantics. "Passed" means
the expected preflight error category was produced. An unexpected *success* of
`validate` in tests 01-02 is treated as a regression.

**Quick Start:** Run the working E2E suite:
```bash
make test-up     # if containers aren't running yet
make e2e         # runs working test 03
```

### Sakila E2E Test Cases

The Sakila suite is three focused tests: one working archive plus two preflight
guardrail demonstrations.

#### Working Configuration (Use This for Testing)

| Test | Relationship Pattern | Description | Status |
|------|---------------------|-------------|--------|
| **Test 03** | High-volume | Multi-batch payment archive (`batch_size=100`, single-column PK) | ✅ Working |

#### Validation Examples (Demonstrate Error Detection)

| Test | Relationship Pattern | Description | Expected Result |
|------|---------------------|-------------|-----------------|
| **Test 01** | Composite PK | Config includes Sakila's composite-PK tables `film_actor`/`film_category` | ❌ COMPOSITE_PK_CHECK fails |
| **Test 02** | 1-N | Single one-to-many: `language → film` (FK not indexed) | ❌ FK_INDEX_CHECK fails |

> **Note:** Earlier tests 04–10 (film hierarchy / actor / category / isolated
> job_schema) were removed. Several archived composite-PK association tables
> (`film_actor`, `film_category`) by a single non-key column, which over-deletes
> and is now rejected by `COMPOSITE_PK_CHECK`; the rest were redundant with the
> three retained tests.

## Prerequisites

### 1. Environment Configuration

Copy the template environment file:

```bash
cp tests/dot.env tests/.env
# Edit tests/.env and verify the settings
```

Default configuration:
- **Source** (db1): `127.0.0.1:3305/sakila`
- **Archive** (db2): `127.0.0.1:3307/sakila_archive`
- **Replica** (db3): `127.0.0.1:3308` (optional)

### 2. Build the Binary

```bash
go build -o bin/goarchive ./cmd/goarchive
```

## Running Tests

### Quick Start - Full Setup and All Tests

```bash
# Setup environment and run Sakila E2E tests
cd tests
./scripts/run-tests.sh --setup --sakila
```

### Setup Test Environment Only

```bash
# Start Docker containers, load Sakila, dump and load schemas
./scripts/run-tests.sh --setup
```

This will:
1. Start Docker Compose containers (db1, db2, db3)
2. Wait for databases to be ready
3. Load Sakila database into source
4. Dump schema from source
5. Load schema into archive

### Run Unit Tests

```bash
./scripts/run-tests.sh --unit-only
```

### Run Integration Tests

Real-DB tests live behind the `integration` build tag. Reseed first (`--setup`)
so the destination starts empty — see the note under [Overview](#overview):

```bash
./scripts/run-tests.sh --setup --integration-only
```

### Run Sakila E2E Tests

```bash
# Working test (03) — archive runs to completion
make e2e                                                # short form
./scripts/run-tests.sh --sakila --skip-docker           # explicit

# Full bootstrap (docker + database seed + working test)
make e2e-setup                                          # short form
./scripts/run-tests.sh --setup --sakila                 # explicit

# Validation demonstrations (01-02) — preflight MUST fail
make e2e-examples                                       # short form
./scripts/run-tests.sh --sakila-examples --skip-docker  # explicit

# Target a specific test
./scripts/run-tests.sh --sakila -t 3                    # working payment archive
./scripts/run-tests.sh --sakila-examples -t 1           # composite-PK demo
```

> ⚠️ **E2E tests must run sequentially** (not concurrently with integration
> tests or other E2E suites). Each E2E test resets the source database by
> dropping and recreating `sakila`. Active MySQL connections from concurrently
> running integration tests can block `DROP DATABASE`, causing the reset to
> fail with "Failed to reset source database". Run unit and integration tests
> first, then run E2E working tests, then E2E demo tests.

### Verbose Output

Add `-v` to any command for verbose output:

```bash
./scripts/run-tests.sh --unit-only -v
```

## Manual Testing Workflow

For interactive testing and debugging, use the `goarchive` CLI commands in sequence. **Use Test 03 (working configuration) for these examples:**

### 1. List Available Jobs

```bash
./bin/goarchive list-jobs --config tests/configs/test03_payment_batch.yaml
```

This displays all jobs defined in the configuration file.

### 2. Plan a Job

```bash
./bin/goarchive plan --job archive-payment-rows --config tests/configs/test03_payment_batch.yaml
```

This shows the execution plan including:
- Tables involved
- Copy order (dependency order)
- Delete order (reverse dependency order)
- Estimated row counts

### 3. Validate a Job ⭐ IMPORTANT

```bash
./bin/goarchive validate --config tests/configs/test03_payment_batch.yaml
```

This performs pre-flight checks and **fails fast** if configuration is invalid:
- Database connectivity
- Table existence
- Primary key validation
- Foreign key constraint checks
- **FK_COVERAGE_CHECK**: Detects missing relations that would cause delete failures
- Graph cycle detection
- DELETE trigger detection

**Note:** Use `--force-triggers` if the database has DELETE triggers (like Sakila's `del_payment` trigger):
```bash
./bin/goarchive validate --config tests/configs/test03_payment_batch.yaml --force-triggers
```

### 4. Dry-Run a Job ⭐ IMPORTANT

```bash
./bin/goarchive dry-run --job archive-payment-rows --config tests/configs/test03_payment_batch.yaml
```

This simulates the archive operation without making changes:
- Discovers affected rows
- Shows copy and delete operations
- Reports estimated rows to be archived
- No data is actually modified

### 5. Execute Archive

Only proceed to archive after validation passes:

```bash
./bin/goarchive archive --job archive-payment-rows --config tests/configs/test03_payment_batch.yaml --skip-verify
```

This performs the actual archive operation:
- Copies data from source to archive
- Verifies data integrity (if configured)
- Deletes archived rows from source (if delete is enabled)
- Logs progress to `archiver_job` and the per-job `archiver_job_log_<id>` tables

### Complete Manual Test Example (Using Working Test 03)

```bash
# Setup environment
./scripts/run-tests.sh --setup

# Test with Test 03 configuration (working example)
cd tests

# 1. List jobs
../bin/goarchive list-jobs --config configs/test03_payment_batch.yaml

# 2. Plan the job
../bin/goarchive plan --config configs/test03_payment_batch.yaml

# 3. Validate the job (should PASS for Test 03)
../bin/goarchive validate --config configs/test03_payment_batch.yaml

# 4. Dry-run the job
../bin/goarchive dry-run --job archive-payment-rows --config configs/test03_payment_batch.yaml

# 5. Execute the archive
../bin/goarchive archive --job archive-payment-rows --config configs/test03_payment_batch.yaml --skip-verify

# Verify results
mysqlsh --uri 'root:qazokm@127.0.0.1:3307/sakila_archive' --sql -e "SELECT COUNT(*) FROM payment WHERE payment_id <= 2000;"
```

### Example: Composite-PK rejection (Test 01)

**Test 01 (Fails Validation):** its config includes Sakila's composite-PK
association tables `film_actor` (`PRIMARY KEY (actor_id, film_id)`) and
`film_category` (`PRIMARY KEY (film_id, category_id)`).

```bash
$ ./bin/goarchive validate --config tests/configs/test01_one_to_one.yaml --force-triggers
❌ Preflight checks failed: COMPOSITE_PK_CHECK: Composite primary keys are not supported.
   GoArchive identifies and deletes rows by a single primary-key column; a multi-column
   PK would over-match and risk deleting rows outside the archived set.
   (tables: [film_actor(2-column PRIMARY KEY) film_category(2-column PRIMARY KEY)])
```

GoArchive discovers, copies, verifies, and **deletes** rows by a single PK
column (`WHERE pk IN (...)`). A composite primary key makes that filter
over-match — deleting rows that were never part of the archived subgraph — so it
is rejected up front. See the README "Limitations" section. This is why the
former film-hierarchy/association E2E tests were retired: they archived these
tables by a single non-key column and silently over-deleted.

> **Related preflight guardrails** (not currently exercised by a dedicated demo
> config): `FK_COVERAGE_CHECK` flags a referenced table missing from the graph
> entirely, and `INTERNAL_FK_COVERAGE` flags FK edges between in-graph tables
> that aren't represented as parent→child relations.

vs Test 01 - Incorrect: Missing child tables
```yaml
# Test 01 - Incorrect: Only includes film_text, missing inventory/rental/payment
relations:
  - table: film_text    # Only this - missing inventory chain!
    ...
```

**Key Point:** The `ValidateInternalFKCoverage` check ensures all foreign key relationships within the graph are represented as edges, preventing delete-phase failures.

## Test Details

The Sakila E2E suite is three tests. Configs live in `tests/configs/`.

### Test 01 — Composite-PK rejection (validation demo)

**Config:** `test01_one_to_one.yaml` · **Expected:** `COMPOSITE_PK_CHECK` (preflight fails)

The config includes Sakila's composite-PK association tables `film_actor`
(`PRIMARY KEY (actor_id, film_id)`) and `film_category`
(`PRIMARY KEY (film_id, category_id)`). GoArchive identifies and deletes rows by
a single PK column (`WHERE pk IN (...)`), so a multi-column PK would over-match
and is rejected up front.

### Test 02 — Missing FK index (validation demo)

**Config:** `test02_one_to_many.yaml` · **Expected:** `FK_INDEX_CHECK` (preflight fails)

A 1-N relationship (`language → film`) whose foreign-key column is not indexed.
Unindexed FK columns make child-table deletes table-scan, so preflight rejects
the configuration with guidance to add the index.

### Test 03 — Payment archive (working)

**Config:** `test03_payment_batch.yaml` · **Job:** `archive-payment-rows` · **Expected:** PASS

Archives `payment` rows (`payment_id <= 2000`, single-column PK) with
`batch_size=100` to exercise the multi-batch copy→verify→delete pipeline end to
end. This is the suite's real archive run.

> The earlier tests 04–10 (film hierarchy, actor/category associations, isolated
> job_schema) were removed: several archived composite-PK association tables by a
> single non-key column and over-deleted (now blocked by `COMPOSITE_PK_CHECK`),
> and the rest were redundant with the three tests above.

## Preflight Checks

GoArchive performs comprehensive preflight checks before executing any archive operation:

| Check | Description | Severity |
|-------|-------------|----------|
| **Table Existence** | Verifies all tables in graph exist in source database | Error |
| **Storage Engine** | Ensures tables use InnoDB (required for transactions) | Error |
| **FK Index Check** | Validates foreign key columns are indexed | Error |
| **FK_COVERAGE_CHECK** | Detects FK constraints not covered by relations | Error |
| **DELETE Trigger** | Warns about DELETE triggers that will fire | Warning |
| **CASCADE Rules** | Warns about ON DELETE CASCADE rules | Warning |

### FK_COVERAGE_CHECK Details

The `ValidateInternalFKCoverage` check is the most important validation for preventing runtime failures. It ensures that **all foreign key relationships between tables in your graph are represented as edges** (parent-child relations).

**The Algorithm:**

```go
1. Collect all tables in the dependency graph
2. Query information_schema for all FK constraints where:
   - Referenced table (parent) is in the graph
   - Referencing table (child) is also in the graph
3. For each such FK constraint:
   - Verify the child table is configured as a relation of the parent
   - If not, report as uncovered FK
4. Fail if any uncovered FKs are found
```

**What it detects:**

| Scenario | Example | Detected? |
|----------|---------|-----------|
| Missing child table | `film` in graph, `inventory` references `film` but not in config | ✅ Yes |
| Missing nested relation | `inventory` and `rental` in graph, but `rental` not nested under `inventory` | ✅ Yes |
| External reference | `film_actor` references `film`, but `film_actor` not in graph | ❌ No (external FKs are OK) |
| Self-referencing FK | `staff.reports_to` → `staff.staff_id` | ❌ No (handled separately) |

**Example error:**
```
FK_COVERAGE_CHECK: Foreign key constraints not covered by relations:
  - film is referenced by: [film_actor, film_category, inventory]
  - inventory is referenced by: [rental]
```

**Why this matters:**

Without this check, the archive would fail during the delete phase:

```sql
-- During delete phase:
DELETE FROM film WHERE film_id = 1;
-- ERROR 1451: Cannot delete or update a parent row: 
-- a foreign key constraint fails (`sakila`.`inventory`, 
-- CONSTRAINT `fk_inventory_film` FOREIGN KEY (`film_id`) ...)
```

The FK_COVERAGE_CHECK catches this at validation time, **before any data is modified**.

**Comparison with FK Index Check:**

| Check | Purpose | Fails When |
|-------|---------|------------|
| `FK_COVERAGE_CHECK` | Ensure complete relation graph | FK exists between graph tables but no edge configured |
| `FK_INDEX_CHECK` | Ensure query performance | FK column not indexed (required for efficient archive queries) |

## Sakila E2E Test Execution Flow

## Test Output

### Console Output

During test execution, you'll see:

```
========================================
Sakila Integration Test Suite
========================================

========================================
Running Test 1: 1-1 Relationship (film → film_text)
========================================
[INFO] [STEP 1] Resetting source database...
[INFO] [STEP 2] Counting rows before archiving...
  film: Source=1000
  film_text: Source=1000
[INFO] [STEP 3] Running archive job...
[INFO] Test 1 completed successfully (Duration: 2s)
```

### Summary Report

After all tests complete, a summary is generated:

```
================================================================================
SAKILA INTEGRATION TEST SUMMARY
================================================================================
Generated: 2026-02-09T12:00:00+03:00

See individual test logs in: /Users/sinanalyuruk/Vscode/goarchive/tests/results/
================================================================================
```

Individual test logs are saved to `results/test_*.log`.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MYSQL_ROOT_PASSWORD` | (required) | MySQL root password (fallback for *_PASSWORD vars) |
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

### Test Fails with "connection refused"

Ensure databases are running:
```bash
cd tests
./scripts/check-servers.sh
docker compose ps
docker compose up -d
```

### Test Fails with "table doesn't exist"

Check if sakila schema is loaded:
```bash
mysqlsh --uri 'root:qazokm@127.0.0.1:3305' --sql -e "SHOW TABLES FROM sakila;"
```

If missing, run setup:
```bash
./scripts/run-tests.sh --setup
```

### Clean Slate

Reset everything and start fresh:

```bash
cd tests

# Stop containers
docker compose down

# Remove data volumes
rm -rf docker_files/db_data

# Start fresh
./scripts/run-tests.sh --setup
```

### Permission Denied on Scripts

Make scripts executable:
```bash
chmod +x scripts/*.sh
```

## File Structure

| File/Directory | Description |
|----------------|-------------|
| `scripts/run-tests.sh` | Main test runner (unified) |
| `scripts/check-servers.sh` | Database connectivity checker |
| `scripts/get_sakila_db.sh` | Downloads Sakila database |
| `scripts/dump_master.js` | MySQL Shell script for schema dump |
| `scripts/create_archive.js` | MySQL Shell script for loading schema |
| `scripts/reset_source.js` | MySQL Shell script for resetting source |
| `configs/*.yaml` | Test configuration files |
| `results/` | Test output and logs |
| `sakila-db/` | Sakila database files (downloaded) |
| `docker_files/` | Docker volume data |
| `compose.yml` | Docker Compose configuration |

## Adding New Tests

To add a new Sakila test:

1. Create a new config file in `configs/testNN_description.yaml`.
2. Add a case entry to `run-tests.sh`'s `run_sakila_test()` function.
3. Set the fields appropriate to the test's purpose:
   - `mode="working"` → archive runs end-to-end; set `tables="..."` to count.
   - `mode="example"` → preflight must fail; set `expected_error="CATEGORY"`
     to the exact error tag (e.g. `FK_INDEX_CHECK`, `INTERNAL_FK_COVERAGE`).
4. Wire the number into the list passed to `run_sakila_tests`:
   - Working tests → `run_sakila_tests "6 7 8 N" "working"` in `main()`.
   - Demo tests → `run_sakila_tests "1 2 3 4 5 N" "validation demos"`.
5. Verify: `./scripts/run-tests.sh --sakila -t NN` (or `--sakila-examples -t NN`).
