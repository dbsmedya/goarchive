# GoArchive Test Suite

This directory contains the complete test suite for GoArchive, including unit tests, integration tests, and real database tests using the Sakila sample database.

## Overview

The test suite includes:

| Test Type | Description | Command |
|-----------|-------------|---------|
| **Unit Tests** | Fast in-memory tests | `./run-tests.sh --unit-only` |
| **Integration Tests** | Tests with real databases | `./run-tests.sh --integration-only` |
| **Sakila E2E Tests** | 8 test configurations demonstrating validation & working examples | `./run-tests.sh --sakila` |

### ⚠️ Test Configuration Status

| Test IDs | Status | Use Case |
|----------|--------|----------|
| **Test 06, 07, 08** | ✅ **Working** | Valid configurations for E2E testing |
| **Test 01-05** | ❌ **Validation Examples** | Demonstrate preflight error detection |

**Quick Start:** Use Test 07 for your first working test:
```bash
./bin/goarchive validate --config tests/configs/test07_actor_film_actor.yaml
```

### Sakila E2E Test Cases

The Sakila tests verify GoArchive's behavior with different relationship patterns:

#### Working Configurations (Use These for Testing)

| Test | Relationship Pattern | Description | Status |
|------|---------------------|-------------|--------|
| **Test 06** | Complex Nested | 4-level: `film→inventory→rental→payment` | ✅ Working (needs `--force-triggers`) |
| **Test 07** | 1-N Simple | Simple: `actor → film_actor` | ✅ Working |
| **Test 08** | 1-N Simple | Simple: `category → film_category` | ✅ Working |

#### Validation Examples (Demonstrate Error Detection)

| Test | Relationship Pattern | Description | Expected Result |
|------|---------------------|-------------|-----------------|
| **Test 01** | 1-1 | Simple one-to-one: `film → film_text` | ❌ FK_COVERAGE_CHECK fails |
| **Test 02** | 1-N | Single one-to-many: `language → film` | ❌ FK_INDEX_CHECK fails |
| **Test 03** | 1-N Multiple | Multiple children: `film → inventory + film_actor + film_category` | ❌ FK_INDEX_CHECK fails |
| **Test 04** | 1-N Two Nested | Two-level nesting: `country → city → address` | ❌ FK_INDEX_CHECK fails |
| **Test 05** | 1-N Three Nested + 1-1 | Complex: `country→city→address→customer` + `film→film_text` | ❌ FK_INDEX_CHECK fails |

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

```bash
./scripts/run-tests.sh --integration-only
```

### Run Sakila E2E Tests

```bash
# Run all 5 Sakila tests
./scripts/run-tests.sh --sakila

# Run specific test only
./scripts/run-tests.sh --sakila -t 1

# Skip Docker setup (use existing databases)
./scripts/run-tests.sh --sakila --skip-docker
```

### Verbose Output

Add `-v` to any command for verbose output:

```bash
./scripts/run-tests.sh --unit-only -v
```

## Manual Testing Workflow

For interactive testing and debugging, use the `goarchive` CLI commands in sequence. **Use Test 07 (working configuration) for these examples:**

### 1. List Available Jobs

```bash
./bin/goarchive list-jobs --config tests/configs/test07_actor_film_actor.yaml
```

This displays all jobs defined in the configuration file.

### 2. Plan a Job

```bash
./bin/goarchive plan --job archive-actor-with-film-roles --config tests/configs/test07_actor_film_actor.yaml
```

This shows the execution plan including:
- Tables involved
- Copy order (dependency order)
- Delete order (reverse dependency order)
- Estimated row counts

### 3. Validate a Job ⭐ IMPORTANT

```bash
./bin/goarchive validate --config tests/configs/test07_actor_film_actor.yaml
```

This performs pre-flight checks and **fails fast** if configuration is invalid:
- Database connectivity
- Table existence
- Primary key validation
- Foreign key constraint checks
- **FK_COVERAGE_CHECK**: Detects missing relations that would cause delete failures
- Graph cycle detection
- DELETE trigger detection

**Note:** Use `--force-triggers` if the database has DELETE triggers (like Sakila's `del_film` trigger):
```bash
./bin/goarchive validate --config tests/configs/test06_complete_film_hierarchy.yaml --force-triggers
```

### 4. Dry-Run a Job ⭐ IMPORTANT

```bash
./bin/goarchive dry-run --job archive-actor-with-film-roles --config tests/configs/test07_actor_film_actor.yaml
```

This simulates the archive operation without making changes:
- Discovers affected rows
- Shows copy and delete operations
- Reports estimated rows to be archived
- No data is actually modified

### 5. Execute Archive

Only proceed to archive after validation passes:

```bash
./bin/goarchive archive --job archive-actor-with-film-roles --config tests/configs/test07_actor_film_actor.yaml --skip-verify
```

This performs the actual archive operation:
- Copies data from source to archive
- Verifies data integrity (if configured)
- Deletes archived rows from source (if delete is enabled)
- Logs progress to archiver_job and archiver_job_log tables

### Complete Manual Test Example (Using Working Test 07)

```bash
# Setup environment
./scripts/run-tests.sh --setup

# Test with Test 07 configuration (working example)
cd tests

# 1. List jobs
../bin/goarchive list-jobs --config configs/test07_actor_film_actor.yaml

# 2. Plan the job
../bin/goarchive plan --config configs/test07_actor_film_actor.yaml

# 3. Validate the job (should PASS for Test 07)
../bin/goarchive validate --config configs/test07_actor_film_actor.yaml

# 4. Dry-run the job
../bin/goarchive dry-run --job archive-actor-with-film-roles --config configs/test07_actor_film_actor.yaml

# 5. Execute the archive
../bin/goarchive archive --job archive-actor-with-film-roles --config configs/test07_actor_film_actor.yaml --skip-verify

# Verify results
mysqlsh --uri 'root:qazokm@127.0.0.1:3307/sakila_archive' --sql -e "SELECT * FROM film_actor WHERE actor_id <= 5;"
```

### Example: Detecting Missing Relations (Test 01 vs Test 06)

**Test 01 (Fails Validation):** Missing nested relations

```bash
$ ./bin/goarchive validate --config tests/configs/test01_one_to_one.yaml --force-triggers
❌ Preflight checks failed: FK_COVERAGE_CHECK: 
Foreign key constraints not covered by relations:
  - inventory is referenced by: [rental]
  - rental is referenced by: [payment]
```

This happens because Test 01 only includes `film → film_text` but `film` has other tables referencing it (`inventory`), and `inventory` is referenced by `rental`, which is referenced by `payment`. The `ValidateInternalFKCoverage` check detects this chain of uncovered foreign keys.

**Test 06 (Passes Validation):** Complete nested hierarchy

```bash
$ ./bin/goarchive validate --config tests/configs/test06_complete_film_hierarchy.yaml --force-triggers
✅ All preflight checks PASSED
```

The difference is Test 06 properly nests all relations:
```yaml
# Test 06 - Correct: Full hierarchy with nested relations
relations:
  - table: inventory
    primary_key: inventory_id
    foreign_key: film_id
    relations:                      # NESTED under inventory
      - table: rental
        primary_key: rental_id
        foreign_key: inventory_id
        relations:                  # NESTED under rental
          - table: payment
            primary_key: payment_id
            foreign_key: rental_id
```

vs Test 01 - Incorrect: Missing child tables
```yaml
# Test 01 - Incorrect: Only includes film_text, missing inventory/rental/payment
relations:
  - table: film_text    # Only this - missing inventory chain!
    ...
```

**Key Point:** The `ValidateInternalFKCoverage` check ensures all foreign key relationships within the graph are represented as edges, preventing delete-phase failures.

## Test Details

### Working Test Configurations (Use These)

These configurations pass all preflight checks including `ValidateInternalFKCoverage`.

---

### Test 06: Complete Film Hierarchy (Complex)

**Configuration:** `configs/test06_complete_film_hierarchy.yaml`

**Status:** ✅ **Working** (requires `--force-triggers`)

Tests a complete 4-level nested hierarchy with proper FK coverage.

```
Root: film (film_id)
  └── inventory (film_id) [1-N]
        └── rental (inventory_id) [1-N]
              └── payment (rental_id) [1-N]
```

**Why it works:** All foreign key relationships are covered by nested relations. The graph includes:
- `film` → referenced by `inventory`
- `inventory` → referenced by `rental` 
- `rental` → referenced by `payment`

Each referencing table is included as a nested relation, satisfying `ValidateInternalFKCoverage`.

**Usage:**
```bash
./bin/goarchive validate --config tests/configs/test06_complete_film_hierarchy.yaml --force-triggers
./bin/goarchive archive --config tests/configs/test06_complete_film_hierarchy.yaml --force-triggers --skip-verify
```

---

### Test 07: Actor → Film Actor (Simple)

**Configuration:** `configs/test07_actor_film_actor.yaml`

**Status:** ✅ **Working**

Simple 2-level hierarchy with no external FK references or DELETE triggers.

```
Root: actor (actor_id)
  └── film_actor (actor_id) [1-N]
```

**Why it works:** 
- No other tables reference `actor` or `film_actor` in Sakila
- No DELETE triggers on these tables
- Simple parent-child relationship with complete coverage

**Usage:**
```bash
./bin/goarchive validate --config tests/configs/test07_actor_film_actor.yaml
./bin/goarchive archive --config tests/configs/test07_actor_film_actor.yaml --skip-verify
```

---

### Test 08: Category → Film Category (Simple)

**Configuration:** `configs/test08_category_film_category.yaml`

**Status:** ✅ **Working**

Simple 2-level hierarchy similar to Test 07.

```
Root: category (category_id)
  └── film_category (category_id) [1-N]
```

**Why it works:** Same pattern as Test 07 - no external references, no triggers.

**Usage:**
```bash
./bin/goarchive validate --config tests/configs/test08_category_film_category.yaml
./bin/goarchive archive --config tests/configs/test08_category_film_category.yaml --skip-verify
```

---

### Validation Example Configurations (Demonstrate Errors)

These configurations intentionally fail preflight checks and serve as examples of common configuration errors detected by `ValidateInternalFKCoverage` and other checks.

---

### Test 01: One-to-One Relationship (FK_COVERAGE_CHECK Example)

**Configuration:** `configs/test01_one_to_one.yaml`

**Status:** ❌ **Fails Validation** - FK_COVERAGE_CHECK

Attempts to archive a simple 1-1 relationship but misses required nested relations.

```
Root: film (film_id)
  └── film_text (film_id) [1-1]
  └── ❌ MISSING: inventory (film_id) [1-N]
        └── ❌ MISSING: rental (inventory_id) [1-N]
              └── ❌ MISSING: payment (rental_id) [1-N]
```

**Expected Behavior:**
- `ValidateInternalFKCoverage` detects that `film` is referenced by `inventory` (not in graph)
- Also detects `inventory` is referenced by `rental`, and `rental` by `payment`
- Validation fails with: `FK_COVERAGE_CHECK: Foreign key constraints not covered`

**Error Message:**
```
❌ Preflight checks failed: FK_COVERAGE_CHECK: 
Foreign key constraints not covered by relations:
  - inventory is referenced by: [rental]
  - rental is referenced by: [payment]
```

**Lesson:** When archiving a table, you must include all tables that reference it (transitively) in your relations graph.

---

### Test 02: One-to-Many Relationship (FK_INDEX_CHECK Example)

**Configuration:** `configs/test02_one_to_many.yaml`

**Status:** ❌ **Fails Validation** - FK_INDEX_CHECK

Tests archiving `language → film` but fails because `film.language_id` lacks an index.

```
Root: language (language_id)
  └── film (language_id) [1-N]
       ^^^ language_id is NOT INDEXED!
```

**Expected Behavior:**
- `ValidateFKIndex` detects that `film.language_id` foreign key column is not indexed
- Validation fails with: `FK_INDEX_CHECK: foreign key columns must be indexed`

**Error Message:**
```
❌ Preflight checks failed: FK_INDEX_CHECK: 
Table `film` column `language_id` (referenced by `fk_film_language`) 
must have an index for efficient WHERE clause filtering
```

**Lesson:** Foreign key columns used in archive operations should be indexed for performance and validation requirements.

---

### Test 03: One-to-Many Multiple Children (FK_INDEX_CHECK Example)

**Configuration:** `configs/test03_one_to_many_multiple.yaml`

**Status:** ❌ **Fails Validation** - FK_INDEX_CHECK

Similar to Test 02 - attempts to archive multiple children but fails on unindexed FKs.

```
Root: film (film_id)
  ├── inventory (film_id) [1-N]
  ├── film_actor (film_id) [1-N]
  └── film_category (film_id) [1-N]
```

**Expected Behavior:**
- Fails FK_INDEX_CHECK due to unindexed FK columns in child tables

---

### Test 04: One-to-Many Two Nested Levels (FK_INDEX_CHECK Example)

**Configuration:** `configs/test04_one_to_many_two_nested.yaml`

**Status:** ❌ **Fails Validation** - FK_INDEX_CHECK

Tests a two-level nested hierarchy.

```
Root: country (country_id)
  └── city (country_id) [1-N]
        └── address (city_id) [1-N]
```

**Expected Behavior:**
- Fails FK_INDEX_CHECK on FK columns

---

### Test 05: One-to-Many Three Nested with 1-1 (FK_INDEX_CHECK Example)

**Configuration:** `configs/test05_one_to_many_three_nested.yaml`

**Status:** ❌ **Fails Validation** - FK_INDEX_CHECK

Tests the most complex scenario with deep nesting.

```
Job 1 - Geographic Hierarchy:
Root: country (country_id)
  └── city (country_id) [1-N]
        └── address (city_id) [1-N]
              └── customer (address_id) [1-N]

Job 2 - Film Extension:
Root: film (film_id)
  └── film_text (film_id) [1-1]
```

**Expected Behavior:**
- Fails FK_INDEX_CHECK on FK columns

## Test Strategy: Fail-Fast with Validation

GoArchive tests follow a **fail-fast strategy** that detects configuration errors early, before any data is modified.

### The Problem: Incomplete Relation Configurations

Sakila database has complex foreign key relationships. An invalid configuration that doesn't include all necessary relations will fail during the delete phase with errors like:

```
Error 1451 (23000): Cannot delete or update a parent row: 
a foreign key constraint fails (`sakila`.`rental`, 
CONSTRAINT `fk_rental_inventory` FOREIGN KEY ...)
```

### The Solution: Early Validation

The test runner performs **three steps** before executing any archive:

```
1. Reset Source Database
   └── Re-create sakila schema and load data

2. Validate Configuration ⭐ NEW
   ├── Run 'goarchive validate' to check configuration
   ├── Detect missing FK relations (FK_COVERAGE_CHECK)
   ├── Warn about DELETE triggers
   └── Fail fast if configuration is invalid

3. Dry-Run ⭐ NEW
   ├── Run 'goarchive dry-run' to simulate execution
   ├── Show estimated row counts
   └── Detect potential issues without modifying data

4. Execute Archive Job (only if validation passes)
   ├── Load configuration
   ├── Create orchestrator
   ├── Run archive operation
   └── Verify data integrity

5. Count After Archiving
   ├── Count rows in source tables (should decrease)
   ├── Count rows in archive tables (should increase)
   └── Calculate archived row counts

6. Generate Summary
   └── Write results to results/test_summary.txt
```

### FK_COVERAGE_CHECK

The preflight checker includes a new validation that detects uncovered foreign key constraints:

```bash
$ goarchive validate --config test01_one_to_one.yaml
❌ Preflight checks failed: FK_COVERAGE_CHECK: 
Foreign key constraints not covered by relations:
  - inventory is referenced by: [rental]
```

This tells the user exactly which tables need to be added to the configuration.

### Fixing Configuration Issues

When validation fails with FK_COVERAGE_CHECK:

1. **Identify missing tables** from the error message
2. **Add them as NESTED relations** (not siblings) in the config file
3. **Re-run validation** until it passes
4. **Then execute the archive**

#### Example: Fixing Test 01 (Incorrect vs Correct)

**Incorrect Fix (Test 01 structure):**
```yaml
# This STILL fails - tables are siblings, not nested!
relations:
  - table: inventory
    primary_key: inventory_id
    foreign_key: film_id
  - table: rental      # Sibling - wrong!
    primary_key: rental_id
    foreign_key: inventory_id
  - table: payment     # Sibling - wrong!
    primary_key: payment_id
    foreign_key: rental_id
```

**Why it fails:** The `ValidateInternalFKCoverage` check looks for FKs between tables in the graph. With siblings, `rental.inventory_id → inventory.inventory_id` and `payment.rental_id → rental.rental_id` are not represented as edges in the graph.

**Correct Fix (Test 06 structure):**
```yaml
# This PASSES - proper nesting represents FK edges
relations:
  - table: inventory
    primary_key: inventory_id
    foreign_key: film_id
    relations:                      # NESTED under inventory
      - table: rental
        primary_key: rental_id
        foreign_key: inventory_id   # FK to parent (inventory)
        relations:                  # NESTED under rental
          - table: payment
            primary_key: payment_id
            foreign_key: rental_id  # FK to parent (rental)
```

**Why it works:** The nested structure represents the actual foreign key relationships:
- `inventory` references `film` (root) via `film_id`
- `rental` references `inventory` (parent) via `inventory_id`
- `payment` references `rental` (parent) via `rental_id`

The `ValidateInternalFKCoverage` check verifies that every FK between tables in the graph has a matching edge (parent-child relation).

#### Using Test 06 Instead

Rather than fixing Test 01, use **Test 06** which already has the correct structure:
```bash
./bin/goarchive validate --config tests/configs/test06_complete_film_hierarchy.yaml --force-triggers
# ✅ All preflight checks PASSED
```

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
| `MYSQL_ROOT_PASSWORD` | qazokm | MySQL root password |
| `TEST_SOURCE_HOST` | 127.0.0.1 | Source MySQL host |
| `TEST_SOURCE_PORT` | 3305 | Source MySQL port |
| `TEST_SOURCE_USER` | root | Source MySQL user |
| `TEST_SOURCE_PASSWORD` | (from .env) | Source MySQL password |
| `TEST_SOURCE_DB` | sakila | Source database name |
| `TEST_ARCHIVE_HOST` | 127.0.0.1 | Archive MySQL host |
| `TEST_ARCHIVE_PORT` | 3307 | Archive MySQL port |
| `TEST_ARCHIVE_USER` | root | Archive MySQL user |
| `TEST_ARCHIVE_PASSWORD` | (from .env) | Archive MySQL password |
| `TEST_ARCHIVE_DB` | sakila_archive | Archive database name |
| `TEST_DEST_HOST` | 127.0.0.1 | Destination host (alias) |
| `TEST_DEST_PORT` | 3307 | Destination port (alias) |
| `TEST_DEST_DB` | sakila_archive | Destination DB (alias) |

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

1. Create a new config file in `configs/testNN_description.yaml`
2. Add test case to `run-tests.sh` in the `run_sakila_test()` function
3. Define tables to verify in the case statement
4. Run with `./scripts/run-tests.sh --sakila -t NN`
