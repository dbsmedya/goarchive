# GoArchive Manual Test Checklist

Use this checklist to track your progress through manual testing of GoArchive.

## ⚠️ Test Status Summary

| Test IDs | Status | Use For |
|----------|--------|---------|
| **Test 06, 07, 08** | ✅ **Working** | E2E testing & validation |
| **Test 01-05** | ❌ **Validation Examples** | Demonstrate preflight error detection only |

> **Note:** Tests 01-05 intentionally fail preflight validation and serve as examples of common configuration errors. Use Tests 06-08 for actual end-to-end testing.
>
> **Quick Start:** Begin with Test 07 (simplest working configuration):
> ```bash
> ./bin/goarchive validate --config tests/configs/test07_actor_film_actor.yaml
> ```

---

## Pre-Test Setup

| Step | Task | Status |
|------|------|--------|
| 1 | Clone repository | [ ] |
| 2 | Install Docker & Docker Compose | [ ] |
| 3 | Install MySQL Shell (`mysqlsh`) | [ ] |
| 4 | Install Go 1.21+ | [ ] |
| 5 | Copy `tests/dot.env` to `tests/.env` | [ ] |
| 6 | Configure `.env` with passwords | [ ] |

---

## Environment Initialization

| Step | Task | Command | Status |
|------|------|---------|--------|
| 1 | Start Docker containers | `cd tests && docker compose up -d` | [ ] |
| 2 | Wait for databases (15s) | `sleep 15` | [ ] |
| 3 | Verify connectivity | `./scripts/check-servers.sh` | [ ] |
| 4 | Download Sakila DB | `./scripts/get_sakila_db.sh` | [ ] |
| 5 | Load Sakila into source | Create DB + load schema + load data | [ ] |
| 6 | Dump schema to archive | `mysqlsh ... dump_master.js` | [ ] |
| 7 | Load schema to archive | `mysqlsh ... create_archive.js` | [ ] |
| 8 | Build GoArchive binary | `make build` or `go build ...` | [ ] |
| 9 | Verify binary | `./bin/goarchive version` | [ ] |

**Quick Setup Alternative:**
```bash
cd tests && ./scripts/run-tests.sh --setup
```

---

## Test 01: One-to-One Relationship (FK_COVERAGE_CHECK Example)

**Config:** `tests/configs/test01_one_to_one.yaml`  
**Status:** ❌ **Fails Validation** - Demonstrates FK_COVERAGE_CHECK error

> **Purpose:** This test demonstrates the `ValidateInternalFKCoverage` check detecting missing nested relations. Use Test 06 for a working film hierarchy.

### Preparation

| Step | Task | Status |
|------|------|--------|
| 1 | Reset source database | [ ] |
| 2 | Verify sakila data loaded | [ ] |
| 3 | Verify archive schema exists | [ ] |

### Execution

| Step | Command | Expected Result | Status |
|------|---------|-----------------|--------|
| 1 | `list-jobs` | Shows `archive-film-with-text` | [ ] |
| 2 | `plan` | Shows film → film_text tree | [ ] |
| 3 | `validate --force-triggers` | ❌ **FK_COVERAGE_CHECK fails** | [ ] |

### Expected Validation Error

```
❌ Preflight checks failed: FK_COVERAGE_CHECK: 
Foreign key constraints not covered by relations:
  - inventory is referenced by: [rental]
  - rental is referenced by: [payment]
```

**Why it fails:** Test 01 only includes `film → film_text` but the graph must include all tables that reference film (transitively): `film → inventory → rental → payment`.

**Fix:** See Test 06 for correct configuration with proper nesting.

---

## Test 02: One-to-Many Relationship (FK_INDEX_CHECK Example)

**Config:** `tests/configs/test02_one_to_many.yaml`  
**Status:** ❌ **Fails Validation** - Demonstrates FK_INDEX_CHECK error

> **Purpose:** This test demonstrates the `ValidateFKIndex` check detecting unindexed foreign key columns.

### Preparation

| Step | Task | Status |
|------|------|--------|
| 1 | Reset source database | [ ] |
| 2 | Verify clean state | [ ] |

### Execution

| Step | Command | Expected Result | Status |
|------|---------|-----------------|--------|
| 1 | `list-jobs` | Shows `archive-language-1` | [ ] |
| 2 | `plan` | Shows language → film tree | [ ] |
| 3 | `validate --force-triggers` | ❌ **FK_INDEX_CHECK fails** | [ ] |

### Expected Validation Error

```
❌ Preflight checks failed: FK_INDEX_CHECK: 
Table `film` column `language_id` (referenced by `fk_film_language`) 
must have an index for efficient WHERE clause filtering
```

**Why it fails:** The `film.language_id` foreign key column lacks an index, which is required for efficient archive queries.

---

## Test 03: One-to-Many Multiple Children (FK_INDEX_CHECK Example)

**Config:** `tests/configs/test03_one_to_many_multiple.yaml`  
**Status:** ❌ **Fails Validation** - Demonstrates FK_INDEX_CHECK error

> **Purpose:** Similar to Test 02, demonstrates FK_INDEX_CHECK on multiple child tables.

### Execution

| Step | Command | Expected Result | Status |
|------|---------|-----------------|--------|
| 1 | `list-jobs` | Shows `archive-film-multiple` | [ ] |
| 2 | `plan` | Shows film with 3 children | [ ] |
| 3 | `validate --force-triggers` | ❌ **FK_INDEX_CHECK fails** | [ ] |

### Expected Validation Error

```
❌ Preflight checks failed: FK_INDEX_CHECK: 
[Multiple unindexed FK columns reported]
```

---

## Test 04: Two Nested Levels (FK_INDEX_CHECK Example)

**Config:** `tests/configs/test04_one_to_many_two_nested.yaml`  
**Status:** ❌ **Fails Validation** - Demonstrates FK_INDEX_CHECK error

> **Purpose:** Demonstrates FK_INDEX_CHECK on nested hierarchy.

### Execution

| Step | Command | Expected Result | Status |
|------|---------|-----------------|--------|
| 1 | `validate --force-triggers` | ❌ **FK_INDEX_CHECK fails** | [ ] |

---

## Test 05: Three Nested Levels + 1-1 (FK_INDEX_CHECK Example)

**Config:** `tests/configs/test05_one_to_many_three_nested.yaml`  
**Status:** ❌ **Fails Validation** - Demonstrates FK_INDEX_CHECK error

> **Purpose:** Demonstrates FK_INDEX_CHECK on complex nested hierarchy.

### Execution

| Step | Command | Expected Result | Status |
|------|---------|-----------------|--------|
| 1 | `validate --force-triggers` | ❌ **FK_INDEX_CHECK fails** | [ ] |

---

## Test 06: Complete Film Hierarchy ✅ WORKING

**Config:** `tests/configs/test06_complete_film_hierarchy.yaml`  
**Status:** ✅ **Working** - Requires `--force-triggers`

> **Purpose:** Complete 4-level nested hierarchy that passes all preflight checks. Demonstrates correct `ValidateInternalFKCoverage` configuration.

### Hierarchy

```
Root: film (film_id)
  └── inventory (film_id) [1-N]
        └── rental (inventory_id) [1-N]
              └── payment (rental_id) [1-N]
```

### Preparation

| Step | Task | Status |
|------|------|--------|
| 1 | Reset source database | [ ] |
| 2 | Verify sakila data loaded | [ ] |
| 3 | Verify archive schema exists | [ ] |

### Execution

| Step | Command | Expected Result | Status |
|------|---------|-----------------|--------|
| 1 | `list-jobs` | Shows `archive-complete-film-hierarchy` | [ ] |
| 2 | `plan` | Shows complete 4-level tree | [ ] |
| 3 | `validate --force-triggers` | ✅ **All preflight checks PASSED** | [ ] |
| 4 | `dry-run` | Shows estimated row counts | [ ] |
| 5 | Count archive (before) | All tables = 0 | [ ] |
| 6 | `archive --force-triggers --skip-verify` | Completes without errors | [ ] |

### Verification

| Step | Check | Expected | Status |
|------|-------|----------|--------|
| 1 | Source film count | Reduced (film_id <= 10 archived) | [ ] |
| 2 | Source inventory count | Reduced | [ ] |
| 3 | Source rental count | Reduced | [ ] |
| 4 | Source payment count | Reduced | [ ] |
| 5 | Archive has all 4 tables | ✅ Yes | [ ] |
| 6 | Archive counts match | ✅ Yes | [ ] |
| 7 | Job log entry | Status = completed | [ ] |

---

## Test 07: Actor → Film Actor ✅ WORKING

**Config:** `tests/configs/test07_actor_film_actor.yaml`  
**Status:** ✅ **Working** - Simplest configuration

> **Purpose:** Simple 2-level hierarchy with no external references or triggers. **Start here for first working test.**

### Hierarchy

```
Root: actor (actor_id)
  └── film_actor (actor_id) [1-N]
```

### Preparation

| Step | Task | Status |
|------|------|--------|
| 1 | Reset source database | [ ] |
| 2 | Verify sakila data loaded | [ ] |
| 3 | Verify archive schema exists | [ ] |

### Execution

| Step | Command | Expected Result | Status |
|------|---------|-----------------|--------|
| 1 | `list-jobs` | Shows `archive-actor-with-film-roles` | [ ] |
| 2 | `plan` | Shows actor → film_actor tree | [ ] |
| 3 | `validate` | ✅ **All preflight checks PASSED** | [ ] |
| 4 | `dry-run` | Shows estimated row counts | [ ] |
| 5 | Count archive (before) | Both tables = 0 | [ ] |
| 6 | `archive --skip-verify` | Completes without errors | [ ] |

### Verification

| Step | Check | Expected | Status |
|------|-------|----------|--------|
| 1 | Source actor count | 195 (5 archived, actor_id <= 5) | [ ] |
| 2 | Source film_actor count | Reduced | [ ] |
| 3 | Archive actor count | 5 | [ ] |
| 4 | Archive film_actor count | Matching rows | [ ] |
| 5 | Job log entry | Status = completed | [ ] |

---

## Test 08: Category → Film Category ✅ WORKING

**Config:** `tests/configs/test08_category_film_category.yaml`  
**Status:** ✅ **Working**

> **Purpose:** Simple 2-level hierarchy similar to Test 07.

### Hierarchy

```
Root: category (category_id)
  └── film_category (category_id) [1-N]
```

### Preparation

| Step | Task | Status |
|------|------|--------|
| 1 | Reset source database | [ ] |
| 2 | Verify sakila data loaded | [ ] |
| 3 | Verify archive schema exists | [ ] |

### Execution

| Step | Command | Expected Result | Status |
|------|---------|-----------------|--------|
| 1 | `list-jobs` | Shows `archive-category-with-films` | [ ] |
| 2 | `plan` | Shows category → film_category tree | [ ] |
| 3 | `validate` | ✅ **All preflight checks PASSED** | [ ] |
| 4 | `dry-run` | Shows estimated row counts | [ ] |
| 5 | `archive --skip-verify` | Completes without errors | [ ] |

### Verification

| Step | Check | Expected | Status |
|------|-------|----------|--------|
| 1 | Archive category count | 5 (category_id <= 5) | [ ] |
| 2 | Archive film_category count | Matching rows | [ ] |
---

## Command Reference

### GoArchive Commands

```bash
# List jobs
./bin/goarchive list-jobs --config <config_file>

# Plan
./bin/goarchive plan --config <config_file> [--job <job_name>]

# Validate
./bin/goarchive validate --config <config_file> [--force-triggers]

# Dry-run
./bin/goarchive dry-run --config <config_file> --job <job_name>

# Archive
./bin/goarchive archive --config <config_file> --job <job_name> [--skip-verify]

# Purge (use with caution!)
./bin/goarchive purge --config <config_file> --job <job_name>
```

### MySQL Shell Commands

```bash
# Connect to source
mysqlsh --uri 'root:PASSWORD@127.0.0.1:3305/sakila' --sql

# Connect to archive
mysqlsh --uri 'root:PASSWORD@127.0.0.1:3307/sakila_archive' --sql

# Count rows
SELECT COUNT(*) FROM table_name;

# Show tables
SHOW TABLES;

# View job log
SELECT * FROM archiver_job_log ORDER BY created_at DESC LIMIT 10;
```

### Docker Commands

```bash
# Start containers
docker compose up -d

# Stop containers
docker compose down

# View logs
docker compose logs -f

# Check status
docker compose ps
```

### Test Runner Commands

```bash
# Full setup
./scripts/run-tests.sh --setup

# Run all Sakila tests
./scripts/run-tests.sh --sakila

# Run specific test
./scripts/run-tests.sh --sakila -t 1

# Check servers
./scripts/check-servers.sh

# Reset source
./scripts/run-tests.sh --setup
```

---

## Issue Log

| Test | Issue | Resolution | Status |
|------|-------|------------|--------|
| | | | |
| | | | |
| | | | |

---

## Sign-off

| Role | Name | Date | Signature |
|------|------|------|-----------|
| Tester | | | |
| Reviewer | | | |

---

## Notes

- Always use `--force-triggers` with Sakila (has DELETE triggers)
- Reset database between tests for clean state
- Use `--skip-verify` for faster testing (skips SHA256 verification)
- Check `tests/results/` for test logs when using automated runner
