# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

**This file is a map, not a copy.** Behavior is documented in `docs/`, testing in
`tests/README.md`. Anything duplicated here rots — record it in its owning file and point at
it from here.

## Project Overview

GoArchive is a Go CLI tool for safely archiving MySQL relational data across servers. It
provides automatic dependency resolution using Kahn's algorithm, crash recovery via checkpoint
logging, and zero-lock batch processing.

**Edition**: Community. Recommended for single-operator workstation archival of cold data.
**Version**: `1.9.0-RC-community` — the release-candidate series validating the dbsgomysql
integration ahead of 2.0 (see `docs/README_dbsgomysql.md`). The **stable** release is
`1.8.0-community` (see `docs/README_LIMITATIONS.md`).
**Enterprise edition** (metrics, parallelism, large-scale load-testing) is planned as a
separate product.

**Preflight validation comes from `github.com/dbsmedya/dbsgomysql`**, not from hand-rolled
probes. GoArchive **must not query `information_schema` directly** — `make consumer-policy`
fails the build if it does. The library owns fact acquisition (schema capture, diffing,
permission and FK-visibility proofs) and verifies those facts against MySQL 8.0, 8.4 and 9.7
on every release; goarchive owns the *policy* applied to them. The dependency is always a
released tag — no `replace`, no pseudo-versions, no committed `go.work`. See
`docs/README_dbsgomysql.md`, and `docs/README_UPGRADING_2_0.md` for what changed for
operators.

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
| `CLAUDE.md` (the **Version** line above, and the RC-format example in this section) | This document |
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

**Releases are automatic and irreversible.** `release.yml` fires on any pushed `v*` tag with
`draft: false` — the tag push *is* the publication, and no tests run on a tag (`ci.yml`
triggers on branches and PRs only). The gate must therefore be the PR containing the bump
commit, and nothing may land after it passes. Agents never create tags or releases; the
operator does.

## Build & guards

```bash
go build -o goarchive ./cmd/goarchive     # build
go test ./... -count=1                    # unit tests (no DB)
gofmt -w .                                # format
make lint                                 # golangci-lint run ./...
make deadcode                             # unreachable-code guard (issue #9) — must stay clean
make consumer-policy                      # fails if goarchive queries information_schema directly
make check                                # fmt-check vet consumer-policy test-ci build
```

## Architecture

```
CLI (Cobra) → Config (Viper) → Core Engine → Processing Pipeline → Data Layer
```

**Stack:** Go 1.24+, Cobra (CLI), Viper (config), Zap (logging), MySQL 8.0+ InnoDB only,
`dbsgomysql` for validation facts.

### Package Layout

| Directory | Purpose |
|-----------|---------|
| `cmd/` | CLI command implementations (Cobra) |
| `internal/archiver/` | Core archive/purge/copy orchestration, preflight policy, batch processing |
| `internal/config/` | Configuration parsing with Viper, validation |
| `internal/database/` | Database connection management, signal handling |
| `internal/graph/` | Dependency graph, Kahn's algorithm, cycle detection |
| `internal/lock/` | MySQL advisory locking for job concurrency |
| `internal/logger/` | Structured logging (Zap wrapper) |
| `internal/mermaidascii/` | ASCII diagram rendering for plan command |
| `internal/types/` | Shared types (RecordSet, type conversions) |
| `internal/verifier/` | Count and SHA256 data verification |

SQL identifier quoting and validation now live in the **library**
(`github.com/dbsmedya/dbsgomysql/pkg/sqlutil`); there is no `internal/sqlutil/`.

### Processing Flow

1. **Preflight**: validate config, check triggers, verify InnoDB
2. **Graph Build**: parse relations → Kahn's algorithm → copy order (parent-first), delete order (child-first)
3. **Batch Loop**: fetch root PKs → BFS discovery → copy transaction → verify → delete
4. **Safety**: `GET_LOCK()` advisory locks prevent concurrent jobs; replication-lag monitoring pauses processing

### Key Data Structures

- **archiver_job**: tracks job state and last processed PK (checkpoint); integer `id` PK,
  `job_name` UNIQUE. Lives in `destination.job_schema` (default = destination database).
- **archiver_job_log_<id>**: per-job table (named by the job's `id`) holding per-root-PK status
  as TINYINT (0=pending/1=copied/2=completed/3=failed) for crash recovery. Replaces the former
  shared `archiver_job_log` table.

## Behavior — `docs/` owns it, this file does not

Do not restate behavior here. When behavior changes, update the owning file below.

| Topic | Owning doc |
|-------|-----------|
| Index of the whole documentation set | `docs/README.md` |
| Schema compatibility (`DEST_SCHEMA_COMPATIBILITY_CHECK`), every preflight check, the check-to-command matrix | `docs/README_VALIDATION.md` |
| Privilege matrix, grant recipes, `PROCESS` requirement, provable-grant rule | `docs/README_PERMISSIONS.md` |
| Every config block, option, default, precedence rule; logging config | `docs/README_CONFIGURATION.md` |
| Commands/flags, tuning, pausing, crash recovery, resume gates | `docs/README_OPERATIONS.md` |
| Hard constraints, model limits, operational cautions | `docs/README_LIMITATIONS.md` |
| Tracking table DDL, DBA maintenance, safe-truncate rules | `docs/README_JOBS_SCHEMA.md` |
| What the dbsgomysql integration changed, and why | `docs/README_dbsgomysql.md` |
| Operator migration notes for 2.0 | `docs/README_UPGRADING_2_0.md` |

### Where behavior lives in code

The one thing `docs/` structurally cannot carry — internal symbols, for navigation.

| Behavior | Symbol |
|----------|--------|
| Schema-compatibility policy | `internal/archiver/preflight_schema_policy.go` → `evaluateSchemaCompatibility` |
| Destination unique-index rule (deviation D3) | same file → `checkDestinationUniqueness` |
| Per-diff-kind disposition, fail-closed `default` | same file → `disposeDiff` |
| Resume checkpoint floor | `internal/archiver/batch_pipeline.go` → `checkpointFloor` (struct field, `:66`) |
| PK column + case validation | `internal/archiver/preflight.go` → `ValidatePrimaryKeyColumns` |
| Per-job logging inheritance | `cmd/goarchive/cmd/root.go` → `effectiveJobLogging` |
| Config identifier rule (`[A-Za-z0-9_]+`) | library → `sqlutil.IsSimpleIdentifier`, called from `internal/config/validation.go` |
| Integer display-width normalization (`bigint(20)` ≡ `bigint`) | library, unexported; goarchive reads `ColumnSpec.NormalizedType` |

> **Do not rename the `## Known Limits & Caution` heading in
> `docs/README_LIMITATIONS.md`.** Two preflight error messages point operators at it by name
> (`internal/archiver/preflight.go`, `ROOT_PK_TYPE_UNSUPPORTED` and `COMPOSITE_PK_CHECK`).
> Renaming the heading breaks that reference.

## Documentation Layout (RULE)

Two distinct trees. Putting a file in the wrong one is a mistake — `docs/` ships to users,
`.ayder/` does not (it is gitignored, 0 tracked files).

| Tree | Audience | Contents |
|------|----------|----------|
| `README.md`, `INSTALL.md`, `docs/`, `tests/README.md` | **Users / operators** | Published documentation. Tracked in git. |
| `.ayder/` | **Development only** | Internal working documents. Gitignored, never shipped. |

`README.md` keeps only: philosophy, problem statement, pt-archiver comparison, features, quick
start, **Basic Usage**, **Architecture**, and project status. Reference material belongs in
`docs/`.

### RULE: internal development documentation goes under `.ayder/superpowers_<YYYYMMDD>/`

Every Superpowers-style internal artifact — brainstormed designs, specs, implementation plans,
architectural decisions — **MUST** be written to:

```
.ayder/superpowers_<YYYYMMDD>/
├── plans/       # implementation plans (writing-plans / executing-plans output)
├── specs/       # designs and specifications (brainstorming output)
├── decisions/   # architectural decision records; why an approach was chosen/rejected
└── pr/          # PR bodies and their gate evidence, one per PR of this effort
```

- `<YYYYMMDD>` is the date the work started, no separators. Existing directories:
  `superpowers_20260503`, `20260702`, `20260724`, `20260726`, `20260727`, `20260801`.
- Create the dated directory when a **new body of work** begins and keep that effort's
  artifacts inside it. Do not append to a previous date's directory for new work — except
  where an existing effort's own INDEX governs the numbering (the `rc-phase-NNN` sequence
  lives in `superpowers_20260801/plans/`).
- File naming: `YYYY-MM-DD-<topic>.md`, brainstorming designs suffixed `-design.md`.
- This **overrides the Superpowers skills' default paths.** When a skill says to write to
  `docs/superpowers/specs/...`, write it here instead. Never create `docs/superpowers/`.
- `docs/` is exclusively published user-facing documentation. Never place a plan, spec,
  decision record, review, or session note there.

### RULE: search the docs with RAG first, grep second

Query `mcp__dbs-vector__search_md_search_gemma` before `grep`. Grep finds *which files contain
a token*; the failure mode here is *two documents that agree in tokens and disagree in
meaning*. Use grep for a literal identifier, an exhaustive count, or a file you can name — but
a string match is not a scope analysis.

- **Indexed and watched:** `docs/`, `tests/`, `.ayder/` — markdown only, re-indexed within
  seconds of a change.
- **Not indexed:** `CLAUDE.md` (already in your context; a lagging copy would contradict it),
  `INSTALL.md`, source code.
- **Never set `min_similarity`.** 0.23 admits pure noise; ≥0.45 cuts correct hits. The safe
  window is 0.06 wide. Cap context with `limit`; judge relevance by reading the chunk.
- **`source_filter`**: full path, trailing fragment (`specs/api.md`), or directory (`specs`) —
  never a leading fragment (`phase-030` fails). A no-match or unmatched filter returns a
  diagnostic, never proof of absence.
- **Split content:** pass a result's `Chunk cursor` to `read_md_search_gemma` (`direction`,
  `count` ≤ 3) — exact text read, not a second search.

Model swaps: `.ayder/dbs-vector/gemma-model-performance.md`.

## Running tests (for agents)

> **`tests/README.md` is the source of truth for all integration and E2E testing** — the full
> command matrix, the Sakila E2E suite and its expected error categories, single-test
> targeting, reseed steps, env vars, and how to add a test. Read it before running or adding
> anything. Do not duplicate that detail here.

Prereq: containers up (`docker ps` shows 3305 / 3307 / 3308), else `make test-up`. Source
credentials before **any** integration, E2E, or `mysqlsh` command:

```bash
set -a; source tests/.env; set +a
```

| Layer | Command |
|-------|---------|
| Unit (no DB) | `go test ./... -count=1` |
| Integration (tag `integration`) | `bash tests/scripts/run-tests.sh --setup --integration-only` |
| E2E (Sakila) | `make e2e` · `make e2e-examples` · `make e2e-setup` |

Three traps that produce **false PASSes**, all of which have cost this project time:

- **`run-tests.sh` always exits 0.** Its exit code is not evidence. Count `--- FAIL` lines
  from the output instead, and never read `go test` output without `-v`.
- **An unset password does not make `mysqlsh` fail loudly.** It connects with no password and
  returns `MySQL Error 1045 (28000): Access denied` — which, to a "did this leave residue?"
  check grepping for rows, is indistinguishable from a clean empty result. Treat
  `Access denied` as a **failed** check, never an empty one.
- **Integration and E2E need a freshly-reseeded destination.** The real-DB tests archive
  Sakila into `sakila_archive` and several rely on it starting empty; leftover rows abort with
  `destination already contains a row … Duplicate entry`, which is residue, **not** a
  regression. `--setup` reseeds. Those tests also DELETE from source Sakila, so they are
  run-once against a fresh `--setup`. If a run is killed mid-flight, `make test-reset`.

The characterization baseline is **`60 / 304 / 364 / 0 / 0`** (top-level / subtests / PASS /
FAIL / SKIP), counted from a `-v` run. It stays unamended unless an increase is authorized in
advance.

## Test Environment

Three MySQL 8.4 servers. **Ask the user if connection fails.** The root password is
`MYSQL_ROOT_PASSWORD` in `tests/.env` — there is no `MYSQL_PASSWORD` variable. Use `mysqlsh`,
not the `mysql` client.

| Server | Host | Port | Database |
|--------|------|------|----------|
| Source | 127.0.0.1 | 3305 | sakila |
| Archive | 127.0.0.1 | 3307 | (destination) |
| Replica | 127.0.0.1 | 3308 | (replication-lag tests) |

```bash
set -a; source tests/.env; set +a
mysqlsh --host=127.0.0.1 --port=3305 --user=root --password="$MYSQL_ROOT_PASSWORD" --sql -e "SHOW DATABASES;"
```

Sakila's schema and the replica setup are documented in `tests/README.md`.

## Source of truth

| Question | Source of truth |
|----------|-----------------|
| What does the code do today? | `docs/` (see the table above) |
| What is already implemented? | `docs/` and `git log` — **not** `.ayder/project-documentation/`, which is frozen at 2026-02-06 and still claims the project is "95% complete" |
| What changed, and when? | `git log`, GitHub PRs, `.ayder/releases/` (one note per version) |
| What work is in flight? | `.ayder/superpowers_<YYYYMMDD>/{plans,specs,decisions}` |
| How do I run the tests? | `tests/README.md` |
| **Where was this decided, and why?** | **RAG first** — `mcp__dbs-vector__search_md_search_gemma` over `docs/`, `tests/`, `.ayder/`. See the rule above before trusting a score or an empty result. |
