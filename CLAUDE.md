# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GoArchive is a Go CLI tool for safely archiving MySQL relational data across servers. It provides automatic dependency resolution using Kahn's algorithm, crash recovery via checkpoint logging, and zero-lock batch processing.

**Edition**: Community. Recommended for single-operator workstation archival of cold data.
**Version**: `1.9.0-RC-community` — the release-candidate series validating the dbsgomysql integration ahead of 2.0 (see `docs/README_dbsgomysql.md`). The **stable** release is `1.8.0-community` (stable for single-operator workstation archival of cold data; see `docs/README_LIMITATIONS.md`).
**Enterprise edition** (metrics, parallelism, large-scale load-testing) is planned as a separate product.

### Versioning (read before bumping the version)

The version string (e.g. `1.4.0-community`, with the `-community` edition suffix)
is duplicated in several places. A bump MUST update **all** of these — a missed
one ships mislabeled binaries:

Release candidates keep the edition suffix and carry an `RC` marker before it:
`1.9.0-RC-community`. Nothing in the repo parses the version — the workflows
trigger on the `v*` glob and extract it with a prefix strip, and CI injects the
literal `ci-test`. The only semver consumer is `docker/metadata-action`
(`.github/workflows/docker.yml`), and every form above is a valid SemVer
prerelease, which is also what makes `release.yml` mark the GitHub release a
prerelease automatically (`prerelease: contains(VERSION, '-')`).

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

# Dead-code guard (must stay clean — issue #9 purge)
make deadcode
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

## Project history (archival — NOT current state)

The original phase-based plan under `.ayder/project-documentation/project-plan/`
documents the Jan–Feb 2026 build-out using hierarchical task IDs
(`GA-P{phase}-F{feature}-T{task}`). Those IDs still appear in source comments —
that is the only reason to open it.

> **It is frozen and wrong as a status report.**
> `tracking/CURRENT_STATE.md` was last updated 2026-02-06 and claims Phase 4
> integration testing is still pending and the project is "95% complete". Both
> shipped long ago. **Never use it to decide what is or isn't implemented.**

For what is actually current:

| Question | Source of truth |
|----------|-----------------|
| What does the code do today? | **Behavior & Gotchas** below, plus `docs/` |
| What changed, and when? | `git log`, GitHub PRs, `.ayder/00N-version-*.md` release notes |
| What work is in flight? | `.ayder/superpowers_<YYYYMMDD>/{plans,specs,decisions}` |
| How do I run the tests? | `tests/README.md` |

## Documentation Layout (RULE)

There are two distinct documentation trees. Putting a file in the wrong one is a
mistake — `docs/` ships to users, `.ayder/` does not (it is gitignored).

| Tree | Audience | Contents |
|------|----------|----------|
| `README.md`, `INSTALL.md`, `docs/`, `tests/README.md` | **Users / operators** | Published, user-facing documentation. Tracked in git. |
| `.ayder/` | **Development only** | Internal working documents. Gitignored, never shipped. |

The published `docs/` set — keep these current when behavior changes:

| File | Owns |
|------|------|
| `docs/README.md` | Index of the documentation set |
| `docs/README_CONFIGURATION.md` | Every config block, option, default, precedence rule |
| `docs/README_VALIDATION.md` | All 20 preflight checks + the check-to-command matrix |
| `docs/README_PERMISSIONS.md` | Privilege matrix, grant recipes, the invariants preflight enforces |
| `docs/README_LIMITATIONS.md` | Hard constraints, model limits, operational cautions |
| `docs/README_OPERATIONS.md` | Commands/flags, tuning, pausing, crash recovery, resume gates |
| `docs/README_JOBS_SCHEMA.md` | Tracking table DDL, DBA maintenance, safe-truncate rules |
| `docs/README_TESTING.md` | Test layers overview (defers to `tests/README.md`) |

`README.md` keeps only: philosophy, problem statement, pt-archiver comparison,
features, quick start, **Basic Usage**, **Architecture**, and project status.
Detailed reference material belongs in `docs/`, not the README.

> **Do not rename the `## Known Limits & Caution` heading in
> `docs/README_LIMITATIONS.md`.** Two preflight error messages point operators at
> it by name (`internal/archiver/preflight.go`, `ROOT_PK_TYPE_UNSUPPORTED` and
> `COMPOSITE_PK_CHECK`). Renaming the heading breaks that reference.

### RULE: internal development documentation goes under `.ayder/superpowers_<YYYYMMDD>/`

Every Superpowers-style internal development artifact — brainstormed designs,
specs, implementation plans, and architectural decisions — **MUST** be written to:

```
.ayder/superpowers_<YYYYMMDD>/
├── plans/       # implementation plans (writing-plans / executing-plans output)
├── specs/       # designs and specifications (brainstorming output)
└── decisions/   # architectural decision records; why an approach was chosen/rejected
```

- `<YYYYMMDD>` is the date the work started, no separators — e.g.
  `.ayder/superpowers_20260726/`. Existing directories: `superpowers_20260503`,
  `superpowers_20260702`, `superpowers_20260724`.
- Create the dated directory (and only the subdirectories you need) when a new
  body of work begins; keep all artifacts for that effort inside it. Do **not**
  append to a previous date's directory for new work.
- File naming inside: `YYYY-MM-DD-<topic>.md`, with the brainstorming design
  suffixed `-design.md` (e.g. `specs/2026-07-26-readme-docs-split-design.md`,
  `plans/2026-07-26-readme-docs-split.md`).
- This **overrides the Superpowers skills' default paths.** When a skill says to
  write to `docs/superpowers/specs/...` or any other location, write it here
  instead. Never create `docs/superpowers/`.
- `docs/` is reserved exclusively for published user-facing documentation.
  Never place a plan, spec, decision record, review, or session note there.

### RULE: search that corpus with RAG first, grep second

The `.ayder/superpowers_*/` tree is large, heavily cross-referenced, and full of prose
that restates the same decision in different words. **Query it with the
`md-superpowers-search` MCP engine
(`mcp__dbs-vector__search_md_superpowers_search`) before reaching for `grep`.** Fall
back to `grep` when the engine is unavailable, when you already know the exact file, or
when you need a literal identifier or an exhaustive count — grep is still the right tool
for "every call site of `X`".

**Why the order matters.** Grep answers *which files contain this token*; the corpus's
real failure mode is *two documents that agree in tokens and disagree in meaning*. Both
defects found this way were invisible to grep:

- A phase plan whose **Interfaces** section carried a corrected signature while its
  **Step 2** code block still held the version the fix replaced — and Step 2 is what an
  implementing agent copies.
- `INDEX.md`'s trap register asserting in prose that three plans were cleared of a trap,
  while its own rows showed only one of them had been. Grep listed `INDEX.md` as
  containing the token; only reading the passage showed the claim was false.

Both are the same class: **a correction applied to some siblings but summarized as
applied to all.** After correcting anything in a plan, query the corrected concept and
read every returned chunk for the pre-correction wording.

**Two properties of the engine that will mislead you if you assume otherwise:**

- **`similarity` is not a relevance signal.** An off-domain query returned goarchive
  phase plans at 0.83 — above several genuinely relevant hits. The corpus sits in a
  narrow 0.77–0.89 band. **Never** use `min_similarity` as a relevance gate, and never
  conclude "the corpus lacks this" from a low score. Judge by reading the chunk. This is
  a model property, not a tuning gap: the same model was calibrated and found **no safe
  floor**, so do not re-open calibration before the plans freeze at phase 037.
- **`source_filter` takes a full stored path, a trailing fragment (`specs/api.md`,
  `api.md`), or a directory (`specs`)** — not globs, and not a *leading* fragment
  (`phase-030` fails; the full filename works). A filter that matches nothing returns an
  explicit diagnostic saying no search ran. That is **not** evidence about the corpus.

An empty result is a low-confidence signal about one attempt, never proof the corpus is
silent. Re-query with different wording, or fall back to grep, before concluding
anything.

## Behavior & Gotchas

Non-obvious current behavior and the rationale behind it. (Chronological "what
changed when" lives in git/GitHub; this section is present-tense current state.)

### Schema compatibility (`DEST_SCHEMA_COMPATIBILITY_CHECK`)

Implemented in `internal/archiver/preflight_schema_policy.go`
(`evaluateSchemaCompatibility`; destination uniqueness is
`checkDestinationUniqueness`). Direction-aware, not byte-identical — the
destination may be *looser* than the source but never *stricter*:

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
  sha256 verification that actually runs; collation-only mismatch is fatal on a
  column participating in a destination unique index (a looser destination
  collation can collide rows the source's index kept distinct), warning
  otherwise.
- **Integer display width is normalized away** (`normalizeColumnType`):
  `bigint(20)` and `bigint` compare equal, since the width is cosmetic and MySQL
  8.0.17+ no longer reports it (a schema dumped from an older server would
  otherwise false-fail). `unsigned`/`zerofill` are preserved — they change the
  value range.

### Preflight & permissions

- Write-permission preflight matches the *connected* account (`CURRENT_USER()`).
  A privilege must be *provable* for the specific object: one held only through
  an active role, or via a bare global grant while `@@global.partial_revokes` is
  enabled, is reported *unconfirmed* and fails closed — see
  `docs/README_PERMISSIONS.md`. `SOURCE_DELETE_PERMISSION_CHECK` runs for
  `archive`, `purge`, and `validate` (not `dry-run` or `copy-only`).
- `JOB_SCHEMA_PERMISSION_CHECK` requires `CREATE` + `SELECT`/`INSERT`/`UPDATE` on
  the tracking schema (`destination.job_schema`).
- Config identifiers (`root_table`, `primary_key`, relation `table`/`foreign_key`/
  `primary_key`, and `job_schema`) must match `[A-Za-z0-9_]+`; names using `$`,
  dots, or other characters are rejected at config load (`IsValidIdentifier`).
- `primary_key` must match the source column's **exact case**
  (`ValidatePrimaryKeyColumns`, `PK_COLUMN_CHECK`). MySQL's
  `information_schema.COLUMNS.COLUMN_NAME` collates case-insensitively
  (`utf8mb3_tolower_ci`), so the check fetches the real column name and compares
  in Go: a name that matches only case-insensitively (`log_id` vs `LOG_ID`) is
  rejected with a dedicated `PK_COLUMN_CASE_CHECK` (clear "fix the casing"
  message), not the data-loss-flavored `PRIMARY_KEY_CHECK`. A configured column
  that does not exist at all is `PK_COLUMN_CHECK`; a real column that exists but
  is not the table's PRIMARY KEY is `PRIMARY_KEY_CHECK`.
- Legacy old-shape tracking tables are detected at startup and rejected with
  upgrade guidance — there is no auto-migration.
- `FK_COVERAGE_CHECK` inspects **incoming** foreign keys by *referenced* schema,
  so a constraint defined in another schema that references an in-graph table is
  detected and hard-fails for every ON DELETE rule (CASCADE/SET NULL/RESTRICT/NO
  ACTION). Cross-schema children cannot be represented in the graph (identifiers
  forbid `schema.table`), so any such incoming FK is fatal.
- `FK_COVERAGE_VISIBILITY_CHECK` fails closed unless GoArchive can prove it saw
  every incoming cross-schema FK. Completeness is proven by successfully reading
  InnoDB's own foreign-key metadata registry, which requires **effective
  `PROCESS`** (a role-held `PROCESS` counts — the proof is that the read
  succeeded). If that read fails, discovery falls back to `information_schema`,
  which only shows constraints on tables the account is privileged for, and the
  state becomes `unconfirmed`; a state of `unknown` means no completeness proof
  was populated at all — only `complete` passes. It is enforced for the commands
  that delete from source or preview such a delete — `archive`, `purge`,
  `dry-run`, and `validate` — and **skipped for `copy-only`**, which never issues
  a source DELETE (no external cascade can fire). Run those commands as an
  account with `PROCESS`. `archive` and `purge` can bypass this check (and all
  preflight) with `--skip-validate-preflight` (DANGEROUS); `dry-run` and
  `validate` have no skip flag and always enforce it. `copy-only` also accepts
  the flag but is exempt from this check regardless — the exemption is from the
  *visibility* check only: `copy-only` still runs `FK_COVERAGE_CHECK`, so a
  `copy-only` run is still blocked by an uncovered cross-schema incoming FK
  (`--skip-validate-preflight` bypasses it).
- `INVISIBLE_COLUMN_CHECK` hard-fails if any participating (graph) table has an
  `INVISIBLE` column. Rows are copied with `SELECT *`, which MySQL omits invisible
  columns from, so their stored values would be silently dropped from the copy
  **and** the verification hash and then deleted from the source (issue #23).
  Detected via `information_schema.COLUMNS.EXTRA` (catches plain `INVISIBLE` and
  `STORED GENERATED INVISIBLE`). It is a source structural check that runs for
  every command that runs preflight — `archive`, `purge`, `copy-only`, `dry-run`,
  and `validate`. Fix: make the column visible (`ALTER TABLE … ALTER COLUMN …
  SET VISIBLE`) or drop the table from the archive until explicit-column support
  exists.
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
- Crash recovery is status-aware via the per-job log TINYINT status, uniformly
  across archive, copy-only, and purge (all three share one batch pipeline):
  `pending` → full replay; `copied` → delete-only replay (archive/purge) or
  direct promotion to completed (copy-only — copy+verify already succeeded).
  A row marked `failed` by a pre-1.8 release blocks resume with recovery
  guidance; current releases never write `failed` — errors abort the run and
  leave rows recoverable. Checkpoints advance only inside the atomic
  batch-completion transaction (copy-only recovery advances per ascending
  chunk because its source rows persist — but only for chunks whose max PK is
  strictly above the job's startup checkpoint (`checkpointFloor`), so requeuing
  a legacy `failed` row below it will not regress the checkpoint and will not
  advance it either; archive/purge recovery does not advance per chunk).
- **Strict-insert jobs refuse to auto-resume `pending` rows.** When strict INSERT
  is forced (`verification.method: count`, `--skip-verify`, or a destination
  secondary unique index) a `pending` row's destination copy may already be
  committed, so re-copying it would abort on duplicate. Resume therefore *refuses*
  with recovery guidance instead of self-blocking; `copied` rows still resume
  without re-copy (delete-only for archive, promotion to completed for copy-only,
  which has no delete phase). Note that archive under `verification.method: count`
  refuses resume outright — an earlier gate fires on *any* `copied` or `pending`
  row, so the `copied` path is never reached. Applies to archive and copy-only
  via the shared pipeline's resume gates (see `.ayder/003`).
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
| Source | 127.0.0.1 | 3305 | root | `$MYSQL_ROOT_PASSWORD` | sakila |
| Archive | 127.0.0.1 | 3307 | root | `$MYSQL_ROOT_PASSWORD` | (empty) |
| Replica | 127.0.0.1 | 3308 | root | `$MYSQL_ROOT_PASSWORD` | (replication test) |

### Test Database Connection

The root password lives in `tests/.env` as **`MYSQL_ROOT_PASSWORD`** — source it first.
There is no `MYSQL_PASSWORD` variable.

> **Why this matters:** an unset password variable does not make `mysqlsh` fail loudly.
> It connects with no password and returns
> `MySQL Error 1045 (28000): Access denied`. If you are grepping the output for rows —
> as a "did this leave residue?" check does — **an auth failure looks exactly like a
> clean empty result**, so a broken command reports a false PASS. Always source
> `tests/.env` before any `mysqlsh` invocation, and treat `Access denied` as a failed
> check rather than an empty one.

```bash
set -a; source tests/.env; set +a

# Use mysqlsh for testing (not mysql client)
# Source (has Sakila sample database)
mysqlsh --host=127.0.0.1 --port=3305 --user=root --password="$MYSQL_ROOT_PASSWORD" --sql -e "SHOW DATABASES;"

# Archive (destination for archived data)
mysqlsh --host=127.0.0.1 --port=3307 --user=root --password="$MYSQL_ROOT_PASSWORD" --sql -e "SHOW DATABASES;"

# Replica (for replication lag monitoring tests)
mysqlsh --host=127.0.0.1 --port=3308 --user=root --password="$MYSQL_ROOT_PASSWORD" --sql -e "SHOW REPLICA STATUS\G"
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
