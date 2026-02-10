# GoArchive Test Suite

This directory contains the complete test suite for GoArchive, including unit tests, integration tests, and real database tests using the Sakila sample database.

## Overview

The test suite includes:

| Test Type | Description | Command |
|-----------|-------------|---------|
| **Unit Tests** | Fast in-memory tests | `./run-tests.sh --unit-only` |
| **Integration Tests** | Tests with real databases | `./run-tests.sh --integration-only` |
| **Sakila E2E Tests** | 5 progressive relationship tests | `./run-tests.sh --sakila` |

### Sakila E2E Test Cases

The Sakila tests verify GoArchive's behavior with different relationship patterns:

| Test | Relationship Pattern | Description |
|------|---------------------|-------------|
| **Test 01** | 1-1 | Simple one-to-one: `film → film_text` |
| **Test 02** | 1-N | Single one-to-many: `language → film` |
| **Test 03** | 1-N Multiple | Multiple children: `film → inventory + film_actor + film_category` |
| **Test 04** | 1-N Two Nested | Two-level nesting: `country → city → address` |
| **Test 05** | 1-N Three Nested + 1-1 | Complex: `country→city→address→customer` + `film→film_text` |

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

For interactive testing and debugging, use the `goarchive` CLI commands in sequence:

### 1. List Available Jobs

```bash
./bin/goarchive list-jobs --config tests/configs/test01_one_to_one.yaml
```

This displays all jobs defined in the configuration file.

### 2. Plan a Job

```bash
./bin/goarchive plan --job Test01_FilmToText --config tests/configs/test01_one_to_one.yaml
```

This shows the execution plan including:
- Tables involved
- Copy order (dependency order)
- Delete order (reverse dependency order)
- Estimated row counts

### 3. Validate a Job ⭐ IMPORTANT

```bash
./bin/goarchive validate --config tests/configs/test01_one_to_one.yaml
```

This performs pre-flight checks and **fails fast** if configuration is invalid:
- Database connectivity
- Table existence
- Primary key validation
- Foreign key constraint checks
- **FK_COVERAGE_CHECK**: Detects missing relations that would cause delete failures
- Graph cycle detection
- DELETE trigger detection

**Note:** Use `--force-triggers` if the database has DELETE triggers (like Sakila):
```bash
./bin/goarchive validate --config tests/configs/test01_one_to_one.yaml --force-triggers
```

### 4. Dry-Run a Job ⭐ IMPORTANT

```bash
./bin/goarchive dry-run --job archive-film-with-text --config tests/configs/test01_one_to_one.yaml
```

This simulates the archive operation without making changes:
- Discovers affected rows
- Shows copy and delete operations
- Reports estimated rows to be archived
- No data is actually modified

### 5. Execute Archive

Only proceed to archive after validation passes:

```bash
./bin/goarchive archive --job archive-film-with-text --config tests/configs/test01_one_to_one.yaml
```

This performs the actual archive operation:
- Copies data from source to archive
- Verifies data integrity (if configured)
- Deletes archived rows from source (if delete is enabled)
- Logs progress to archiver_job and archiver_job_log tables

### Complete Manual Test Example

```bash
# Setup environment
./scripts/run-tests.sh --setup

# Test with Test 01 configuration
cd tests

# 1. List jobs
../bin/goarchive list-jobs --config configs/test01_one_to_one.yaml

# 2. Plan the job
../bin/goarchive plan --config configs/test01_one_to_one.yaml

# 3. Validate the job (catches missing relations early)
../bin/goarchive validate --config configs/test01_one_to_one.yaml --force-triggers

# 4. Dry-run the job
../bin/goarchive dry-run --job archive-film-with-text --config configs/test01_one_to_one.yaml

# 5. Execute the archive
../bin/goarchive archive --job archive-film-with-text --config configs/test01_one_to_one.yaml

# Verify results
mysqlsh --uri 'root:qazokm@127.0.0.1:3307/sakila_archive' --sql -e "SELECT * FROM film LIMIT 5;"
```

### Example: Detecting Missing Relations

If you have an incomplete configuration:

```bash
$ ./bin/goarchive validate --config tests/configs/test01_one_to_one.yaml
❌ Preflight checks failed: FK_COVERAGE_CHECK: 
Foreign key constraints not covered by relations:
  - inventory is referenced by: [rental]
```

**Fix:** Add the missing table to your configuration:
```yaml
relations:
  - table: inventory
    ...
  - table: rental      # Add this
    primary_key: rental_id
    foreign_key: inventory_id
    dependency_type: "1-N"
```

## Test Details

### Test 01: One-to-One Relationship

**Configuration:** `configs/test01_one_to_one.yaml`

Tests archiving a simple 1-1 relationship where both tables share the same primary key.

```
Root: film (film_id)
  └── film_text (film_id) [1-1]
```

**Expected Behavior:**
- Both tables should archive the same number of rows
- Counts in film and film_text should match exactly

### Test 02: One-to-Many Relationship

**Configuration:** `configs/test02_one_to_many.yaml`

Tests archiving a simple parent-child relationship.

```
Root: language (language_id)
  └── film (language_id) [1-N]
```

**Expected Behavior:**
- All films for the selected languages should be archived
- Source counts should decrease after archiving

### Test 03: One-to-Many Multiple Children

**Configuration:** `configs/test03_one_to_many_multiple.yaml`

Tests archiving multiple child relationships from the same parent.

```
Root: film (film_id)
  ├── inventory (film_id) [1-N]
  ├── film_actor (film_id) [1-N]
  └── film_category (film_id) [1-N]
```

**Expected Behavior:**
- All three child tables should have archived rows
- Each child's archived count reflects its relationship to the parent

### Test 04: One-to-Many Two Nested Levels

**Configuration:** `configs/test04_one_to_many_two_nested.yaml`

Tests a two-level nested hierarchy.

```
Root: country (country_id)
  └── city (country_id) [1-N]
        └── address (city_id) [1-N]
```

**Expected Behavior:**
- Cities for selected countries are archived
- Addresses for those cities are archived
- Nested discovery works correctly

### Test 05: One-to-Many Three Nested with 1-1

**Configuration:** `configs/test05_one_to_many_three_nested.yaml`

Tests the most complex scenario with deep nesting and mixed relationship types.

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
- Three-level nested relationships work correctly
- 1-1 relationships work alongside nested hierarchies
- Customer records (3 levels deep) are archived
- Film text records (1-1) are archived

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
2. **Add them to relations** in the config file
3. **Re-run validation** until it passes
4. **Then execute the archive**

Example fix for Test 01:
```yaml
# Before: Missing rental and payment tables
relations:
  - table: inventory
    ...

# After: Complete hierarchy
relations:
  - table: inventory
    ...
  - table: rental      # Added
    primary_key: rental_id
    foreign_key: inventory_id
    dependency_type: "1-N"
  - table: payment     # Added
    primary_key: payment_id
    foreign_key: rental_id
    dependency_type: "1-N"
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

This is the most important check for preventing runtime failures. It queries the database metadata to find all tables that reference tables in your graph but are **not** included in your relations.

**How it works:**
1. Gets all tables in the dependency graph
2. Queries `information_schema.referential_constraints` for FKs referencing those tables
3. Checks if the referencing tables are also in the graph
4. Reports any uncovered FK constraints

**Example error:**
```
FK_COVERAGE_CHECK: Foreign key constraints not covered by relations:
  - film is referenced by: [film_actor, film_category, inventory]
  - inventory is referenced by: [rental]
```

**Why this matters:**
Without this check, the archive would fail during the delete phase with:
```
Error 1451: Cannot delete or update a parent row: a foreign key constraint fails
```

The FK_COVERAGE_CHECK catches this at validation time, **before any data is modified**.

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
