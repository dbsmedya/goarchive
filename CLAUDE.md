# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GoArchive is a Go CLI tool for safely archiving MySQL relational data across servers. It provides automatic dependency resolution using Kahn's algorithm, crash recovery via checkpoint logging, and zero-lock batch processing.

**Edition**: Community. Recommended for single-operator workstation archival of cold data.
**Version**: `1.4.0-community` (stable for single-operator workstation archival of cold data; see README "Known Limits & Caution").
**Enterprise edition** (metrics, parallelism, large-scale load-testing) is planned as a separate product.

### Versioning (read before bumping the version)

The version string (e.g. `1.4.0-community`, with the `-community` edition suffix)
is duplicated in several places. A bump MUST update **all** of these — a missed
one ships mislabeled binaries:

| Location | What it controls |
|----------|------------------|
| `Makefile` → `RELEASE_VERSION` | Fallback version stamped into binaries when HEAD has no exact-match git tag. **The one most often missed.** |
| `cmd/goarchive/cmd/root.go` → `Version` | Default `Version` constant (overridden by `-ldflags` at build time) |
| `CLAUDE.md` (the **Version** line above) | This document |
| `README.md` (the **Version** line) | User-facing docs |
| `INSTALL.md` (the **Version** line) | User-facing docs |

Do **not** change: `cmd/goarchive/cmd/version_test.go` (uses `1.2.3` as a test
fixture, not the project version), or historical release notes under `.ayder/`.

How the build resolves the version (`Makefile`):
`VERSION := git describe --tags --exact-match || RELEASE_VERSION`. So a properly
**tagged** release commit takes its version from the git tag; an untagged build
falls back to `RELEASE_VERSION`. For an actual release, also create the matching
tag: `make tag V=1.4.0-community` (this creates a `v`-prefixed tag, so a tagged
build reports `v1.4.0-community` while the `RELEASE_VERSION` fallback reports
`1.4.0-community` — keep `RELEASE_VERSION` in sync regardless).

After bumping, verify: `go build -o /tmp/gv ./cmd/goarchive && /tmp/gv --version`
should print the new version, and `make github-release` should stamp every
`bin/goarchive-<version>-*` artifact with it.

## Build Commands

```bash
# Initialize module (first task)
go mod init github.com/dbsmedya/goarchive

# Build
go build -o goarchive ./cmd/goarchive

# Run tests
go test ./...

# Run single test
go test -v -run TestFunctionName ./internal/graph/

# Format code
gofmt -w .

# Lint
golint ./...
```

## Architecture

```
CLI (Cobra) → Config (Viper) → Core Engine → Processing Pipeline → Data Layer
```

### Package Layout

| Directory | Purpose |
|-----------|---------|
| `cmd/` | CLI command implementations (Cobra) |
| `internal/archiver/` | Core archive/purge/copy orchestration, preflight checks, batch processing |
| `internal/config/` | Configuration parsing with Viper, validation |
| `internal/database/` | Database connection management, signal handling |
| `internal/graph/` | Dependency graph, Kahn's algorithm, cycle detection |
| `internal/lock/` | MySQL advisory locking for job concurrency |
| `internal/logger/` | Structured logging (Zap wrapper) |
| `internal/mermaidascii/` | ASCII diagram rendering for plan command |
| `internal/sqlutil/` | SQL identifier quoting and validation |
| `internal/types/` | Shared types (RecordSet, type conversions) |
| `internal/verifier/` | Count and SHA256 data verification |

### Processing Flow

1. **Preflight**: Validate config, check triggers, verify InnoDB
2. **Graph Build**: Parse relations → Kahn's algorithm → copy order (parent-first), delete order (child-first)
3. **Batch Loop**: Fetch root IDs → BFS discovery → copy transaction → verify → delete
4. **Safety**: Advisory locks prevent concurrent jobs; replication lag monitoring pauses processing

### Key Data Structures

- **archiver_job**: Tracks job state and last processed PK (checkpoint); integer `id` PK, `job_name` UNIQUE. Lives in `destination.job_schema` (default = destination database).
- **archiver_job_log_<id>**: Per-job table (named by the job's `id`) holding per-root-PK status as TINYINT (0=pending/1=copied/2=completed/3=failed) for crash recovery. Replaces the former shared `archiver_job_log` table.

## Tech Stack

- Go 1.24+, Cobra (CLI), Viper (config)
- MySQL 8.0+ with InnoDB only
- Zap for structured logging

## Task System

Tasks use hierarchical IDs: `GA-P{phase}-F{feature}-T{task}`

- Task index: `docs/project-plan/tasks/TASK_INDEX.md`
- Current state: `docs/project-plan/tracking/CURRENT_STATE.md`
- Task details: `docs/project-plan/tasks/phase-{n}/GA-P{n}-F{n}-T{n}.md`

## Behavior & Gotchas

Non-obvious current behavior and the rationale behind it. (Chronological "what
changed when" lives in git/GitHub; this section is present-tense current state.)

### Schema compatibility (`DEST_SCHEMA_COMPATIBILITY_CHECK`)

Implemented in `internal/archiver/preflight.go` (`columnIncompatibility`).
Direction-aware, not byte-identical — the destination may be *looser* than the
source but never *stricter*:

- **Allowed (destination looser):** drop secondary indexes (`MUL`/`UNI`),
  `auto_increment`, column defaults (`DEFAULT_GENERATED`, `ON UPDATE`), and relax
  `NOT NULL`. Dropping destination secondary indexes is a supported
  write-performance optimization.
- **Still fatal (destination stricter):** missing/different primary key (needed
  for `INSERT IGNORE` crash-recovery idempotency), destination-only unique
  indexes (INSERT IGNORE would silently skip rows), destination-only `NOT NULL`,
  destination generated columns (copy inserts explicit values), and any
  name/type/count/order difference.
- **Column charset mismatch:** fatal under count verification or when
  verification is skipped (silent transliteration risk), warning-only under a
  sha256 verification that actually runs; collation-only mismatch always warns.
- **Integer display width is normalized away** (`normalizeColumnType`):
  `bigint(20)` and `bigint` compare equal, since the width is cosmetic and MySQL
  8.0.17+ no longer reports it (a schema dumped from an older server would
  otherwise false-fail). `unsigned`/`zerofill` are preserved — they change the
  value range.

### Preflight & permissions

- Write-permission preflight matches the *connected* account (`CURRENT_USER()` +
  active roles) across `USER_`/`SCHEMA_`/`TABLE_PRIVILEGES`, so global grants
  don't false-fail. `SOURCE_DELETE_PERMISSION_CHECK` covers archive/purge.
- `JOB_SCHEMA_PERMISSION_CHECK` requires `CREATE` + `SELECT`/`INSERT`/`UPDATE` on
  the tracking schema (`destination.job_schema`).
- Config identifiers (`root_table`, `primary_key`, relation `table`/`foreign_key`/
  `primary_key`, and `job_schema`) must match `[A-Za-z0-9_]+`; names using `$`,
  dots, or other characters are rejected at config load (`IsValidIdentifier`).
- Legacy old-shape tracking tables are detected at startup and rejected with
  upgrade guidance — there is no auto-migration.
- `archive`/`purge`/`copy-only` run preflight at startup; `--skip-validate-preflight`
  bypasses it (DANGEROUS).

### Processing & verification config

- **Config-only**: there are no `--batch-size`/`--batch-delete-size`/`--sleep`
  CLI flags. Per-job `processing`/`verification` blocks are pointer fields — nil
  inherits the global block, explicit values *including zero* win, and
  `--skip-verify` beats job blocks.
- `where` is required on every job; `"1=1"` is the explicit full-table opt-in.
- `batch_size` is the real copy chunk unit: root and every child table fetch and
  insert `batch_size` rows at a time.
- Crash recovery is status-aware via the per-job log TINYINT status: `pending` →
  full replay, `copied` (copy+verify succeeded, safe to delete) → delete-only, no
  re-verify.
- **Strict-insert jobs refuse to auto-resume `pending` rows.** When strict INSERT
  is forced (`verification.method: count`, `--skip-verify`, or a destination
  secondary unique index) a `pending` row's destination copy may already be
  committed, so re-copying it would abort on duplicate. Resume therefore *refuses*
  with recovery guidance instead of self-blocking; `copied` rows still resume
  delete-only. Applies to archive and copy-only (see `.ayder/003`).
- `delete_sleep_seconds` (default 0) pauses between `batch_delete_size` delete
  chunks to limit binlog/replication lag — independent of `sleep_seconds`, which
  paces whole batches.
- `sentinel_file` (default empty): while the file exists, archive/purge/copy-only
  pause before each batch (re-checked every 1s, context-interruptible).
- `dry-run` runs the non-destructive preflight profile, prints the WHERE clause,
  filters child-table estimates through the relation chain, and validates
  `batch_size` against MySQL's 65,535-placeholder limit and `max_allowed_packet`
  via a rolled-back destination transaction (placeholder check exact; packet
  check approximate for child tables).

### Logging

- Config is loaded from exactly one file (`--config`, default `./archiver.yaml`)
  — no merging or fallback across multiple yaml files.
- Per-job `logging:` block (level/format/output/file_only); unset fields inherit
  the global `logging:` block; CLI `--log-level`/`--log-format` override both
  (`effectiveJobLogging` in `cmd/goarchive/cmd/root.go`). Every entry is tagged
  `job=<name>`.
- File output (`logging.output: <path>`) is plain text (no ANSI); the stdout tee
  stays colored. `logging.file_only: true` suppresses the stdout tee and is
  rejected when output is stdout/stderr.
- No log rotation: files open in append mode — use logrotate `copytruncate` (see
  example config). Logs never contain credentials or DSNs.

## Key Algorithms

- **Kahn's Algorithm**: Topological sort for dependency ordering
- **BFS Traversal**: Discover all child records from root PKs
- **Advisory Locking**: MySQL `GET_LOCK()` prevents duplicate job execution

## Running tests (for agents)

Prereq: test MySQL containers up (`docker ps` shows ports 3305 / 3307 / 3308).
If not, run `make test-up` first. Credentials live in `tests/.env` — source
it before running any integration or E2E command:

```bash
set -a; source tests/.env; set +a
```

Quick layers:

- **Unit** (no DB): `go test ./... -count=1`
- **Integration** (real MySQL, build tag `integration`): `bash tests/scripts/run-tests.sh --setup --integration-only`
- **E2E** (Sakila): `make e2e` (working archives) · `make e2e-examples` (validation-failure demos) · `make e2e-setup` (bootstrap + run)

**Integration + E2E need a freshly-reseeded destination — the #1 source of false
failures.** The real-DB tests archive Sakila into `sakila_archive` and several
rely on it starting empty; a prior run leaves rows behind and aborts with
`destination already contains a row … Duplicate entry` — that is leftover state,
**not** a regression. The `--setup` flag reseeds first. The real-DB tests also
DELETE from source Sakila, so they are run-once against a fresh `--setup`.

> **`tests/README.md` is the source of truth for all integration + E2E testing.**
> Read it before running or adding integration/E2E tests — it owns the full
> command matrix, the Sakila E2E suite (working archives + validation demos and
> their expected error categories), single-test targeting, reseed/run steps, env
> vars, and how to add a test. Do not duplicate that detail here.

Safety-fix notes:
- New orchestrator integration tests should clean `archiver_job` and the
  per-job `archiver_job_log_<id>` table for their job names before/after
  execution so heartbeat and lock state cannot leak across tests. Use
  `testsupport.CleanupArchiverState` (resolves the id and drops the per-job
  table) rather than deleting from a shared log table.
- Destructive CLI tests that intentionally use broken-schema fixtures must pass
  `--skip-validate-preflight`; normal `archive`, `purge`, and `copy-only`
  commands now run preflight at startup.
- Sakila archive/purge E2E invocations need `--force-triggers` because Sakila
  contains DELETE triggers.
- Root primary keys must be integer types (TINYINT through BIGINT, signed or
  unsigned). Preflight rejects non-integer root PKs.
- Every participating table must have a **single-column PRIMARY KEY equal to its
  configured `primary_key`**. Preflight rejects composite PKs (`COMPOSITE_PK_CHECK`),
  no-PK tables, and a `primary_key` that is not the table's actual PRIMARY KEY
  (`PRIMARY_KEY_CHECK`) — all would over-match on delete-by-PK (review `.ayder/003`).
- The job advisory lock is held on a dedicated MySQL connection. Keepalive now
  verifies `IS_USED_LOCK()` against that connection id and aborts if ownership
  is lost; document/assume MySQL `wait_timeout` is higher than expected job
  duration.
- `--force` is a best-effort heartbeat takeover only. It blocks later startups
  after seeding a fresh heartbeat but cannot stop an old process that is stale
  yet still alive and still owns `GET_LOCK()`. Operators must verify the old
  process is dead before forcing.
- Archive deletes are auto-committed in batches. Interruptions can leave source
  temporarily child-gone/parent-present until resume, after copy+verify has
  already succeeded.
- Shared many-to-many membership rows are a documented caveat: discovery/delete
  is per root and can delete a shared child with the first referencing root.
- **DDL-only destination schemas require `safety.disable_foreign_key_checks: true`.**
  When the destination is initialized from a schema dump (e.g. `dump_master.js`
  with `ddlOnly: true`), reference tables such as `language`, `category`, or
  `film` are empty but still have foreign-key constraints. Copying child rows
  that reference those empty tables will hit Error 1452 unless FK checks are
  disabled for the copy phase. This is a normal operator scenario — lookup
  tables are often not part of the archived subgraph — and is safe because
  `copy.go` uses a dedicated connection and always resets
  `FOREIGN_KEY_CHECKS = 1` before returning the connection to the pool.

## Test Environment

Three MySQL 8.4 servers are available for testing. **Ask the user if connection fails.**

| Server | Host | Port | User | Password | Database |
|--------|------|------|------|----------|----------|
| Source | 127.0.0.1 | 3305 | root | (see .env) | sakila |
| Archive | 127.0.0.1 | 3307 | root | (see .env) | (empty) |
| Replica | 127.0.0.1 | 3308 | root | (see .env) | (replication test) |

### Test Database Connection

```bash
# Use mysqlsh for testing (not mysql client)
# Source (has Sakila sample database)
mysqlsh --host=127.0.0.1 --port=3305 --user=root --password=$MYSQL_PASSWORD --sql -e "SHOW DATABASES;"

# Archive (destination for archived data)
mysqlsh --host=127.0.0.1 --port=3307 --user=root --password=$MYSQL_PASSWORD --sql -e "SHOW DATABASES;"

# Replica (for replication lag monitoring tests)
mysqlsh --host=127.0.0.1 --port=3308 --user=root --password=$MYSQL_PASSWORD --sql -e "SHOW REPLICA STATUS\G"
```

### Sakila Schema (Source)

The Sakila database contains sample data with relational tables useful for testing archive operations:
- `customer` → `rental` → `payment` (1-N relationships)
- `film` → `film_actor`, `film_category`, `inventory` (1-N relationships)
- `store` → `staff`, `inventory` (1-N relationships)

### Replica Server (Replication Testing)

The replica server (port 3308) is used for testing replication lag monitoring functionality (GA-P3-F5):
- Tests should configure this as the replica connection in SafetyConfig
- Use `SHOW REPLICA STATUS` to verify replication is running
- Monitor `Seconds_Behind_Master` for lag threshold testing
- Test scenarios: lag exceeding threshold, replica stopped, IO/SQL thread failures
