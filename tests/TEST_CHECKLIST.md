# GoArchive Manual Test Checklist

Use this checklist to track your progress through manual testing of GoArchive.

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

## Test 01: One-to-One Relationship (film → film_text)

**Config:** `tests/configs/test01_one_to_one.yaml`

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
| 3 | `validate --force-triggers` | ✅ "Preflight checks completed" | [ ] |
| 4 | `dry-run` | Shows estimated row counts | [ ] |
| 5 | Count source rows (film) | ~1000 total, 50 match WHERE | [ ] |
| 6 | Count source rows (film_text) | 50 match WHERE | [ ] |
| 7 | Count archive (before) | Both tables = 0 | [ ] |
| 8 | `archive --skip-verify` | Completes without errors | [ ] |

### Verification

| Step | Check | Expected | Status |
|------|-------|----------|--------|
| 1 | Source film count | 950 (50 archived) | [ ] |
| 2 | Source film_text count | 950 | [ ] |
| 3 | Archive film count | 50 | [ ] |
| 4 | Archive film_text count | 50 | [ ] |
| 5 | Job log entry | Status = completed | [ ] |
| 6 | Data integrity | film_ids match between tables | [ ] |

---

## Test 02: One-to-Many Relationship (language → film)

**Config:** `tests/configs/test02_one_to_many.yaml`

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
| 3 | `validate --force-triggers` | ✅ Passes | [ ] |
| 4 | `dry-run` | Shows affected rows | [ ] |
| 5 | Count source (language) | 6 languages | [ ] |
| 6 | Count source (film) | ~1000 films | [ ] |
| 7 | `archive --skip-verify` | Completes | [ ] |

### Verification

| Step | Check | Expected | Status |
|------|-------|----------|--------|
| 1 | Source language count | 5 (1 archived) | [ ] |
| 2 | Source film count | Reduced (films for lang 1 archived) | [ ] |
| 3 | Archive language count | 1 | [ ] |
| 4 | Archive film count | > 0 | [ ] |

---

## Test 03: One-to-Many Multiple Children (film → inventory + film_actor + film_category)

**Config:** `tests/configs/test03_one_to_many_multiple.yaml`

### Preparation

| Step | Task | Status |
|------|------|--------|
| 1 | Reset source database | [ ] |
| 2 | Verify clean state | [ ] |

### Execution

| Step | Command | Expected Result | Status |
|------|---------|-----------------|--------|
| 1 | `list-jobs` | Shows `archive-film-multiple` | [ ] |
| 2 | `plan` | Shows film with 3 children | [ ] |
| 3 | `validate --force-triggers` | ✅ Passes | [ ] |
| 4 | `dry-run` | Shows all 4 tables | [ ] |
| 5 | Count source before | Record all table counts | [ ] |
| 6 | `archive --skip-verify` | Completes | [ ] |

### Verification

| Step | Check | Expected | Status |
|------|-------|----------|--------|
| 1 | Source film count | Reduced | [ ] |
| 2 | Source inventory count | Reduced | [ ] |
| 3 | Source film_actor count | Reduced | [ ] |
| 4 | Source film_category count | Reduced | [ ] |
| 5 | Archive has all 4 tables | ✅ Yes | [ ] |
| 6 | Archive counts match archived | ✅ Yes | [ ] |

---

## Test 04: Two Nested Levels (country → city → address)

**Config:** `tests/configs/test04_one_to_many_two_nested.yaml`

### Preparation

| Step | Task | Status |
|------|------|--------|
| 1 | Reset source database | [ ] |
| 2 | Verify clean state | [ ] |

### Execution

| Step | Command | Expected Result | Status |
|------|---------|-----------------|--------|
| 1 | `list-jobs` | Shows `archive-country` | [ ] |
| 2 | `plan` | Shows 3-level hierarchy | [ ] |
| 3 | `validate --force-triggers` | ✅ Passes | [ ] |
| 4 | `dry-run` | Shows country → city → address | [ ] |
| 5 | Count source before | Record all counts | [ ] |
| 6 | `archive --skip-verify` | Completes | [ ] |

### Verification

| Step | Check | Expected | Status |
|------|-------|----------|--------|
| 1 | Source country count | Reduced | [ ] |
| 2 | Source city count | Reduced (for archived countries) | [ ] |
| 3 | Source address count | Reduced (for archived cities) | [ ] |
| 4 | Archive has all 3 tables | ✅ Yes | [ ] |
| 5 | City records match countries | ✅ Yes | [ ] |
| 6 | Address records match cities | ✅ Yes | [ ] |

---

## Test 05: Three Nested + 1-1 (country→city→address→customer + film→film_text)

**Config:** `tests/configs/test05_one_to_many_three_nested.yaml`

### Preparation

| Step | Task | Status |
|------|------|--------|
| 1 | Reset source database | [ ] |
| 2 | Verify clean state | [ ] |

### Job 1: Country Hierarchy

| Step | Command | Expected | Status |
|------|---------|----------|--------|
| 1 | `list-jobs` | Shows both jobs | [ ] |
| 2 | `plan` | Shows complex hierarchy | [ ] |
| 3 | `validate --force-triggers` | ✅ Passes | [ ] |
| 4 | `dry-run --job archive-country-hierarchy` | Shows 4-level tree | [ ] |
| 5 | Count source before | Record counts | [ ] |
| 6 | `archive --job archive-country-hierarchy` | Completes | [ ] |
| 7 | Verify source counts | Reduced | [ ] |
| 8 | Verify archive counts | Match archived | [ ] |

### Job 2: Film 1-1

| Step | Command | Expected | Status |
|------|---------|----------|--------|
| 1 | `dry-run --job archive-film-text` | Shows film → film_text | [ ] |
| 2 | Count source before | Record counts | [ ] |
| 3 | `archive --job archive-film-text` | Completes | [ ] |
| 4 | Verify archive film count | Correct | [ ] |
| 5 | Verify archive film_text count | Matches film | [ ] |

### Combined Verification

| Step | Check | Expected | Status |
|------|-------|----------|--------|
| 1 | Archive has country tables | ✅ Yes | [ ] |
| 2 | Archive has film tables | ✅ Yes | [ ] |
| 3 | Job logs for both jobs | ✅ Yes | [ ] |

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
