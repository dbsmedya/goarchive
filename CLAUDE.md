# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GoArchive is a Go CLI tool for safely archiving MySQL relational data across servers. It provides automatic dependency resolution using Kahn's algorithm, crash recovery via checkpoint logging, and zero-lock batch processing.

**Edition**: Community. Recommended for single-operator workstation archival of cold data.
**Version**: `1.1.2-community` (stable for single-operator workstation archival of cold data; see README "Known Limits & Caution").
**Enterprise edition** (metrics, parallelism, large-scale load-testing) is planned as a separate product.

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

- **archiver_job**: Tracks job state and last processed PK (checkpoint)
- **archiver_job_log**: Per-root-PK status (pending/completed/failed) for crash recovery

## Tech Stack

- Go 1.24+, Cobra (CLI), Viper (config)
- MySQL 8.0+ with InnoDB only
- Zap for structured logging

## Task System

Tasks use hierarchical IDs: `GA-P{phase}-F{feature}-T{task}`

- Task index: `docs/project-plan/tasks/TASK_INDEX.md`
- Current state: `docs/project-plan/tracking/CURRENT_STATE.md`
- Task details: `docs/project-plan/tasks/phase-{n}/GA-P{n}-F{n}-T{n}.md`

## Recent Changes

### Validation Hardening (chore/validation, 2026-06-10)
- Schema compatibility now compares column charset: mismatch is fatal under
  count verification (silent transliteration risk) or when verification is
  skipped, warning-only under sha256 that actually runs;
  collation-only mismatch always warns
- Write-permission preflight matches the connected account (CURRENT_USER() +
  active roles) across USER_/SCHEMA_/TABLE_PRIVILEGES — global grants no longer
  false-fail; new SOURCE_DELETE_PERMISSION_CHECK for archive/purge
- `where` is required on every job (`"1=1"` = explicit full-table opt-in)
- dry-run runs the non-destructive preflight profile, prints the WHERE clause,
  and child-table estimates are filtered through the relation chain
- Removed `--batch-size`/`--batch-delete-size`/`--sleep` CLI flags; processing
  is config-only. Per-job processing/verification blocks use pointer fields
  (`ProcessingOverrides`/`VerificationOverrides`): nil inherits, explicit
  values — including zero — win, and `--skip-verify` beats job blocks

### Relaxed Destination Schema Check (chore/validation, 2026-06-10)
- `DEST_SCHEMA_COMPATIBILITY_CHECK` is now direction-aware instead of demanding
  byte-identical column metadata: the destination may drop secondary indexes
  (`MUL`/`UNI`), `auto_increment`, and column defaults (`DEFAULT_GENERATED`,
  `ON UPDATE`), and may relax `NOT NULL` — dropping destination secondary
  indexes is a supported write-performance optimization
- Still rejected (destination stricter than source): missing/different primary
  key (required for `INSERT IGNORE` idempotency during crash recovery),
  destination-only unique indexes (INSERT IGNORE would silently skip rows),
  destination-only `NOT NULL`, destination generated columns (copy inserts explicit values), and any
  name/type/count/order difference (`columnIncompatibility` in
  `internal/archiver/preflight.go`)

### Logging Production Audit (fix/varios, 2026-06-09)
- `RecordDiscovery` now receives the orchestrator logger via `SetLogger` (was silently using a default stdout logger in all three orchestrators)
- `newJobLogger` tags every entry with `job=<name>` (`WithJob`), so all cmd/preflight/orchestrator/phase logs are attributable
- archive/purge/copy-only log a structured completion summary (`Infow`) in addition to the console `fmt` output, so file logs capture run results
- No rotation built in: files open in append mode; example config documents logrotate `copytruncate`
- Audited clean: no credentials/DSNs logged; defer ordering flushes logger last; second-signal handler syncs before exit; zap loggers are goroutine-safe

### Per-Job Logging (fix/varios, 2026-06-09)
- Jobs can carry their own `logging:` block (level/format/output/file_only); unset fields inherit from the global `logging:` block, and CLI `--log-level`/`--log-format` flags override both (`effectiveJobLogging` in `cmd/goarchive/cmd/root.go`)
- archive/purge/copy-only/dry-run build their logger from the job-effective config; orchestrators now expose `SetLogger` and receive the cmd logger (previously they always logged to a default stdout logger)
- Validation checks each job's merged logging config and reports errors as `jobs.<name>.logging.<field>`

### Logging Fixes (fix/varios, 2026-06-09)
- Stdout/stderr zap syncers are now no-op on `Sync()` — kills the spurious `warning: failed to sync logger: sync /dev/stdout: invalid argument` on Linux (fsync on tty/pipe returns EINVAL)
- File output (`logging.output: <path>`) writes plain text (no ANSI color codes); the stdout tee keeps colored output
- New `logging.file_only: true` suppresses the stdout tee; validation rejects it when output is stdout/stderr
- Reminder: config is loaded from exactly one file (`--config`, default `./archiver.yaml`) — there is no merging or fallback between multiple yaml files

### Batched Archive Pipeline (fix/batch_size, 2026-06-08)
- `batch_size` is now the real copy chunk unit: root and every child table are fetched and inserted `batch_size` rows at a time (previously only root fetch was chunked)
- `archiver_job_log` gains a `copied` status as a durable "copy+verify succeeded, safe to delete" marker; crash recovery is now status-aware (`pending` → full replay, `copied` → delete-only, no re-verify)
- `dry-run` validates that `batch_size` fits MySQL's 65,535-placeholder limit and `max_allowed_packet` via a rolled-back destination transaction (placeholder check exact; packet check measured, approximate for child tables)
- `delete_sleep_seconds` (default 0) throttles the delete phase by pausing between `batch_delete_size` delete chunks to limit binlog/replication lag — independent of `sleep_seconds`, which paces whole batches for source/archive load
- `sentinel_file` (default empty) is an operator pause switch honored by archive/purge/copy-only: while the file exists, processing pauses before each batch (re-check every 1s, context-interruptible) and resumes when removed

### CLI Improvements (GA-P4-F8, 2026-02-06)
1. **Removed pterm dependency** - Plan command uses plain text output
2. **Mermaid-ascii integration** - Table relationships shown as ASCII diagrams
3. **Job-specific configs** - Processing and verification settings can be set per-job

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

Then the standard matrix, fastest to slowest:

| Layer | Command | What it covers |
|-------|---------|----------------|
| Unit | `go test ./... -count=1` | Pure-Go, sqlmock, no DB required |
| Integration | `INTEGRATION_FORCE=true go test -tags=integration ./internal/archiver/...` | Real MySQL (3305/3307), smaller fixture |
| E2E (working) | `make e2e` | Sakila tests 06/07/08 — full archive runs |
| E2E (setup + run) | `make e2e-setup` | Same as above but bootstraps docker + DBs from scratch |
| E2E (validation demos) | `make e2e-examples` | Sakila tests 01–05 |

**About the validation demos (`make e2e-examples`, tests 01–05):** these are
designed to FAIL preflight with specific error categories
(`INTERNAL_FK_COVERAGE`, `FK_INDEX_CHECK`, …). The runner inverts the semantics
— "pass" means the failure matched the documented expectation. Do not treat an
`EXPECTED FAILURE matched` line as a regression.

Single-test targeting: `bash tests/scripts/run-tests.sh --sakila -t 7` runs
just working test 7; `--sakila-examples -t 1` runs just demo 1.

Safety-fix notes:
- New orchestrator integration tests should clean `archiver_job` and
  `archiver_job_log` state for their job names before/after execution so
  heartbeat and lock state cannot leak across tests.
- Destructive CLI tests that intentionally use broken-schema fixtures must pass
  `--skip-validate-preflight`; normal `archive`, `purge`, and `copy-only`
  commands now run preflight at startup.
- Sakila archive/purge E2E invocations need `--force-triggers` because Sakila
  contains DELETE triggers.
- Root primary keys must be integer types (TINYINT through BIGINT, signed or
  unsigned). Preflight rejects non-integer root PKs.
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
