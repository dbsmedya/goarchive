# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GoArchive is a Go CLI tool for safely archiving MySQL relational data across servers. It provides automatic dependency resolution using Kahn's algorithm, crash recovery via checkpoint logging, and zero-lock batch processing.

**Edition**: Community. Recommended for single-operator workstation archival of cold data.
**Version**: `0.9.0-community` (beta — use with caution; see README "Known Limits & Caution").
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

### Code Review Refactor (2026-03-27)
- Removed dead code: `ApplyJobOverrides`, `UpdateProcessingConfig`, `PreflightError.Details`
- Added nil guards to all destination preflight methods
- Added max relation nesting depth (10) to config validation
- Fixed float precision in SHA256 verification (`%f` -> `%.17g`)
- Standardized `rows.Close()` error handling

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
