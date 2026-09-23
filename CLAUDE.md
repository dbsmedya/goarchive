# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

**This file is a map, not a copy.** Behavior is documented in `docs/`, testing in
`tests/README.md`. Anything duplicated here rots — record it in its owning file and point at
it from here.

## Primary development workflow

**dev-contract** is the primary workflow for non-trivial development, including specs, plans,
reviews, implementation dispatch, gates and PR preparation. Read the globally
installed skill through the agent's skill catalog; its filesystem location depends on the
installation. A workstation may expose `.agents/skills/dev-contract` as an optional local
discovery link, but that ignored link is not shipped with this repository. Do not assume it
exists or maintain a second copy here.

It takes precedence over the older development-workflow, software-architect and Superpowers
workflow instructions. Other skills may support work within this contract. This file and
`tests/README.md` continue to own repository-specific facts and test procedures;
`tests/AGENTS.md` only turns `tests/README.md` into steps for agents.

Read the operator's rulings and approved spec before the plan. Existing approved specs remain
authoritative; converting a plan does not reopen its spec. Workflow adoption does not approve
development of pending plans: each requires the contract's review and dispatch authorization.

## Project Overview

GoArchive is a Go CLI tool for safely archiving MySQL relational data across servers. It
provides automatic dependency resolution using Kahn's algorithm, crash recovery via checkpoint
logging, and zero-lock batch processing.

**Edition**: Community. Recommended for single-operator workstation archival of cold data.
**Enterprise edition** (metrics, parallelism, large-scale load-testing) is planned as a
separate product.

The current version is in `INSTALL.md`; `README.md` also states which release is stable. The
RC series validating the dbsgomysql integration ahead of 2.0 is described in
`docs/README_dbsgomysql.md`.

**Preflight validation comes from `github.com/dbsmedya/dbsgomysql`**, not from hand-rolled
probes. GoArchive **must not query `information_schema` directly** — `make consumer-policy`
fails the build if it does. The library owns fact acquisition (schema capture, diffing,
permission and FK-visibility proofs) and verifies those facts against MySQL 8.0, 8.4 and 9.7
on every release; goarchive owns the *policy* applied to them. The dependency is always a
released tag — no `replace`, no pseudo-versions, no committed `go.work`. See
`docs/README_dbsgomysql.md`, and `docs/README_UPGRADING_2_0.md` for what changed for
operators.

**Library gaps are fixed in the library.** When goarchive would need a workaround because the
library's facts are weaker or missing, the fix is a dbsgomysql release and a pin bump, never
compensating logic here. When the library is stricter than goarchive's pre-2.0 behaviour, adopt
it and document the change: pre-2.0 behaviour is a reference point, not a quality bar. A
library bump is judged by goarchive's baselines alone: an unchanged integration inventory and
characterization expectation is a pass, and library changes the suite did not observe are not
coverage gaps, because the library verifies its own facts. The exception is a library change
that moves goarchive's own operator-visible acceptance; that gets a goarchive test.

### Versioning (read before bumping the version)

The version string carries the `-community` edition suffix; release candidates keep it and
add an `RC` marker before it. It is duplicated in several places, and a bump MUST update
**all** of them — a missed one ships mislabeled binaries:

| Location | What it controls |
|----------|------------------|
| `Makefile` → `RELEASE_VERSION` | Fallback version stamped into binaries when HEAD has no exact-match git tag. **The one most often missed.** |
| `cmd/goarchive/cmd/root.go` → `Version` | Default `Version` constant (overridden by `-ldflags` at build time) |
| `README.md` (the **Version** line) | User-facing docs |
| `README.md` (the **Stable release** line) | User-facing docs. It also names the dbsgomysql version, so it changes when the `go.mod` pin changes, too |
| `INSTALL.md` (the **Version** line) | User-facing docs |

Nothing in the repo parses the version — the workflows trigger on the `v*` glob and extract
it with a prefix strip, and CI injects the literal `ci-test`. The only semver consumer is
`docker/metadata-action` (`.github/workflows/docker.yml`); both forms are valid SemVer
prereleases, which is also what makes `release.yml` mark the GitHub release a prerelease
automatically (`prerelease: contains(VERSION, '-')`).

Do **not** change: `cmd/goarchive/cmd/version_test.go` (uses `1.2.3` as a test
fixture, not the project version), or historical release notes under `.ayder/`.

How the build resolves the version (`Makefile`):
`VERSION := git describe --tags --exact-match || RELEASE_VERSION`. So a properly
**tagged** release commit takes its version from the git tag; an untagged build
falls back to `RELEASE_VERSION`. For an actual release, also create the matching
tag: `make tag V=<version>` (this creates a `v`-prefixed tag, so a tagged build
reports `v<version>` while the `RELEASE_VERSION` fallback reports `<version>` —
keep `RELEASE_VERSION` in sync regardless).

After bumping, verify: `go build -o /tmp/gv ./cmd/goarchive && /tmp/gv --version`
should print the new version, and `make github-release` should stamp every
`bin/goarchive-<version>-*` artifact with it.

**Releases are automatic and irreversible.** `release.yml` fires on any pushed `v*` tag with
`draft: false` — the tag push *is* the publication, and no tests run on a tag (`ci.yml`
triggers on branches and PRs only). The gate must therefore be the PR containing the bump
commit, and nothing may land after it passes. Agents never create tags or releases; the
operator does.

Release notes are written to `.ayder/releases/v<version>.md` (gitignored); the operator pastes
them into the GitHub release body after tagging. Write them for a stranger and keep them
short: what shipped, compatibility, how it was verified. Verify a release by its `release.yml`
run and its assets, not by the release appearing in the list. A PR body closes an issue only
with `Closes #N`; a bare `(#N)` leaves it open.

### Release lines: where work goes

The roadmaps own placement and when `release/2.2` is cut:
`.ayder/roadmaps/roadmap_v2.x-community.md` (the 2.x stability line) and
`.ayder/roadmaps/roadmap_v3.md` (the 3.0 train). Place work on a line before specifying it.

- **2.x is fixes only:** no features, refactors, tracking-schema changes or config breaks.
  Features go to 3.0.
- **Port the test, not the patch.** A fix lands as a `test` commit, then a `fix` commit, never
  squashed. Moving a fix between lines cherry-picks the test and re-implements the fix where
  the code differs. Never merge between lines.
- Numbered design decisions live in `roadmap_v3.md` §2; cite them rather than restating them.

## Build & guards

```bash
go build -o goarchive ./cmd/goarchive     # build
go test ./... -count=1                    # unit tests (no DB)
gofmt -w .                                # format
make lint                                 # golangci-lint v2.11.4, pinned in the Makefile
make deadcode                             # unreachable-code guard (issue #9) — must stay clean
make consumer-policy                      # fails if goarchive queries information_schema directly
make check                                # fmt-check vet lint consumer-policy deadcode test-unit build — what CI runs
```

## Architecture

```
CLI (Cobra) → Config (Viper) → Core Engine → Processing Pipeline → Data Layer
```

**Stack:** Go 1.26+ (builds pinned to go1.26.8 by go.mod's toolchain line), Cobra (CLI), Viper
(config), Zap (logging), MySQL 8.0.40+ InnoDB only, `dbsgomysql` for validation facts.

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
- **goarchive_meta**: one-row tracking-schema marker. The binary recognises exactly one label
  (`trackingSchemaVersion`, `internal/archiver/resume.go`) and refuses every other revision
  and every populated marker-less schema — no inference, no migration. A release that changes
  the tracking tables' layout **or a column's meaning** bumps the label and adds a procedure row
  to `docs/README_JOBS_SCHEMA.md` → *Tracking-schema upgrade procedures*.

## Triage rules

- **Security findings: the mistake test.** GoArchive defends against the operator's
  *mistakes*, not against the operator, who already holds the credentials and a `mysql`
  client. A finding is a weakness only if it widens a forgivable mistake. If only a deliberate
  act reaches it, the privilege matrix (`docs/README_PERMISSIONS.md`) owns it, and the finding
  is closed citing `roadmap_v3.md` decision 14.
- **Rarity is not severity.** A real defect found late, or found by a new model, is fixed
  promptly. The mistake test decides *whether* it is a weakness; rarity never decides that in
  either direction.
- **Design within the operator contracts:** no DDL on participating tables during a run, cold
  data only, and eligibility monotonic in PK order (`roadmap_v3.md` decision 17). The
  during-a-run contract is published in `docs/README_LIMITATIONS.md` → *No DDL and no
  concurrent writes during a run*; the eligibility part is scheduled as 2.x roadmap 2.2.4
  item 7. Contract violations are documented, not detected: add no mid-run drift probe or DML
  tolerance without the operator's direction.

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
| Removing duplicate rows with one job (keep the minimum primary key) | `docs/README_DUPLICATE_CLEANUP.md` |
| What the dbsgomysql integration changed, and why | `docs/README_dbsgomysql.md` |
| Operator migration notes for 2.0 | `docs/README_UPGRADING_2_0.md` |

### Where behavior lives in code

The one thing `docs/` structurally cannot carry — internal symbols, for navigation.

| Behavior | Symbol |
|----------|--------|
| Schema-compatibility policy | `internal/archiver/preflight_schema_policy.go` → `evaluateSchemaCompatibility` |
| Destination unique-index rule (deviation D3) | same file → `checkDestinationUniqueness` |
| Per-diff-kind disposition, fail-closed `default`. It **parses** the library's `SpecDiff.A`/`.B` strings (types, `"true"`/`"false"`), so a change to their format in a dbsgomysql release changes goarchive policy | same file → `disposeDiff`, `goarchiveTypesCompatible` |
| Resume checkpoint floor | `internal/archiver/batch_pipeline.go` → `checkpointFloor` (struct field, `:66`) |
| Checkpoint write — the only writer, inside the batch's completion transaction | `internal/archiver/resume.go` → `(*ResumeManager).CompleteBatch` |
| PK column + case validation | `internal/archiver/preflight.go` → `ValidatePrimaryKeyColumns` |
| Source/destination identity guard (`SRC_DEST_IDENTITY_CHECK`) | `internal/database/identity.go` → `assertDistinctDatabases`, called from `Manager.Connect` |
| AUTO_INCREMENT zero connection initialization and assertion | `internal/database/database.go` → `BuildDSN`, `Manager.connectWithRetry`; `internal/database/auto_zero.go` → `assertAutoIncrementZeroMode` |
| Temporal projection, representation and identity contracts | `internal/types/temporal.go` → `ColumnMetadata`, `TemporalText`, `TemporalIdentitySet`, `TemporalReadContractError` |
| INSERT diagnostic session, collection and classification | `internal/archiver/insert_diagnostics.go` → `readDiagnosticSession`, `collectInsertDiagnostics`, `classifyInsertDiagnostics`, `insertDiagnosticError` |
| Sticky `root_table` on an existing job | `internal/archiver/resume.go` → `GetOrCreateJobWithType` |
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

Every dev-contract internal artifact — designs, specs, implementation plans,
architectural decisions — **MUST** be written to:

```
.ayder/superpowers_<YYYYMMDD>/
├── plans/       # dev-contract implementation plans, one per PR
├── specs/       # designs and specifications
├── decisions/   # architectural decision records; why an approach was chosen/rejected
└── pr/          # PR bodies and their gate evidence, one per PR of this effort
```

- `<YYYYMMDD>` is the date the work started, no separators.
- Create the dated directory when a **new body of work** begins and keep that effort's
  artifacts inside it. Do not append to a previous date's directory for new work.
- **Completed efforts move to `.ayder/archived/superpowers_<YYYYMMDD>/`.** `archived/` is not
  RAG-indexed, so a superseded spec cannot compete with a current one in search results. Read
  an archived artifact by path when a current document cites it. Only in-flight efforts stay
  at `.ayder/superpowers_<YYYYMMDD>/`.
- File naming: `YYYY-MM-DD-<topic>.md`, designs suffixed `-design.md`.
- Keep the existing `superpowers_<YYYYMMDD>` directory convention under dev-contract.
  Reviews live under `.ayder/reviews/` as the contract specifies. Never create
  `docs/superpowers/` for internal artifacts.
- `docs/` is exclusively published user-facing documentation. Never place a plan, spec,
  decision record, review, or session note there.

### RULE: search the docs with RAG first, grep second

Query `mcp__dbs-vector__search_md_goarchive_search` before `grep`. Grep finds *which files
contain a token*; the failure mode here is *two documents that agree in tokens and disagree in
meaning*. Use grep for a literal identifier, an exhaustive count, or a file you can name — but
a string match is not a scope analysis.

Its main use is consistency. Before changing any fact stated in more than one document (a
count, a command, a version, a default, a rule), search for every statement of it.

> The engine name has been renamed several times. If that tool 404s, call
> `mcp__dbs-vector__list_engines` for the current `mcp_tool` / `read_tool` names rather than
> falling back to grep.

- **Indexed and watched:** every markdown file in the repository, re-indexed within seconds of
  a change. That includes `CLAUDE.md`, `README.md` and `INSTALL.md` (measured 2026-09-23).
  `CLAUDE.md` is already in your context, so trust that copy over a search hit from it.
- **Not indexed:** `.ayder/archived/` (excluded by the engine's configuration), source code.
- **Never set `min_similarity`.** 0.23 admits pure noise; ≥0.45 cuts correct hits. The safe
  window is 0.06 wide. Cap context with `limit`; judge relevance by reading the chunk.
- **`source_filter`**: full path, trailing fragment (`specs/api.md`), or directory (`specs`) —
  never a leading fragment (`phase-030` fails). A no-match or unmatched filter returns a
  diagnostic, never proof of absence.
- **Split content:** pass a result's `Chunk cursor` to `read_md_goarchive_search` (`direction`,
  `count` ≤ 3) — exact text read, not a second search.

Model swaps: `.ayder/dbs-vector/gemma-model-performance.md`.

### RULE: Go symbols through the language server, not grep

For callers, references and reachability of a Go symbol, use gopls (the `LSP` tool when the
session offers it) or the code-review graph. Grep counts comments as uses: in this
comment-heavy codebase it once reported about 40 hits for a symbol with zero code references.
Grep stays right for strings, SQL text, non-Go files and exhaustive token counts. gopls cannot
see `../dbsgomysql` (outside this module, and a `go.work` is banned) and silently under-reports
there, so use grep for the library and say which tool proved the claim.

## Running tests

- **`tests/README.md` is the single source of truth** for how goarchive's tests are defined
  and run. It is written for people; read it before adding or changing a test.
- **Agents follow [`tests/AGENTS.md`](tests/AGENTS.md)** to run tests and the gate. It turns
  `tests/README.md` into steps, cites its sections, and defines nothing of its own. It is the
  gate instructions file and the owner of the measured baselines (integration inventory,
  characterization expectation).

## Source of truth

| Question | Source of truth |
|----------|-----------------|
| What does the code do today? | `docs/` (see the table above) |
| What is already implemented? | `docs/` and `git log` — **not** `.ayder/project-documentation/`, which is frozen at 2026-02-06 and still claims the project is "95% complete" |
| What changed, and when? | `git log`, GitHub PRs, `.ayder/releases/` (one note per version) |
| What work is in flight? | `.ayder/superpowers_<YYYYMMDD>/{plans,specs,decisions}` |
| How do I run the tests? | `tests/README.md` (the source of truth); agents follow `tests/AGENTS.md` |
| **Where was this decided, and why?** | **RAG first** — `mcp__dbs-vector__search_md_goarchive_search` over the repository's markdown (not `.ayder/archived/`). See the rule above before trusting a score or an empty result. Decisions of completed efforts live in `.ayder/archived/`; read them by the path a current document cites. |

<!-- code-review-graph MCP tools -->
## MCP Tools: code-review-graph

**IMPORTANT: This project has a knowledge graph. ALWAYS use the
code-review-graph MCP tools BEFORE using Grep/Glob/Read to explore
the codebase.** The graph is faster, cheaper (fewer tokens), and gives
you structural context (callers, dependents, test coverage) that file
scanning cannot.

### When to use graph tools FIRST

- **Exploring code**: `semantic_search_nodes_tool` or `query_graph_tool` instead of Grep
- **Understanding impact**: `get_impact_radius_tool` instead of manually tracing imports
- **Code review**: `detect_changes_tool` + `get_review_context_tool` instead of reading entire files
- **Finding relationships**: `query_graph_tool` with callers_of/callees_of/imports_of/tests_for
- **Architecture questions**: `get_architecture_overview_tool` + `list_communities_tool`

Fall back to Grep/Glob/Read **only** when the graph doesn't cover what you need.

### Key Tools

| Tool | Use when |
| ------ | ---------- |
| `detect_changes_tool` | Reviewing code changes — gives risk-scored analysis |
| `get_review_context_tool` | Need source snippets for review — token-efficient |
| `get_impact_radius_tool` | Understanding blast radius of a change |
| `get_affected_flows_tool` | Finding which execution paths are impacted |
| `query_graph_tool` | Tracing callers, callees, imports, tests, dependencies |
| `semantic_search_nodes_tool` | Finding functions/classes by name or keyword |
| `get_architecture_overview_tool` | Understanding high-level codebase structure |
| `refactor_tool` | Planning renames, finding dead code |

### Workflow

1. The graph auto-updates on file changes (via hooks).
2. Use `detect_changes_tool` for code review.
3. Use `get_affected_flows_tool` to understand impact.
4. Use `query_graph_tool` pattern="tests_for" to check coverage.
