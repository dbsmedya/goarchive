# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

**This file is a map, not a copy.** Behavior is documented in `docs/`, testing in
`tests/README.md`. Anything duplicated here rots — record it in its owning file and point at
it from here.

## Project Overview

GoArchive is a Go CLI tool for safely archiving MySQL relational data across servers. It walks
each job's table tree from its declared relations, recovers an interrupted run by replaying the
job's typed log, and works in key-ordered batches.

**Edition**: Community. Recommended for single-operator workstation archival of cold data.
**Enterprise edition** (metrics, parallelism, large-scale load-testing) is planned as a
separate product.

The current version is the `Makefile`'s `RELEASE_VERSION` (see *Versioning* below); `README.md` also states which release is stable.

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
library bump is judged by goarchive's baselines alone: an unchanged integration inventory is a
pass, and library changes the suite did not observe are not
coverage gaps, because the library verifies its own facts. The exception is a library change
that moves goarchive's own operator-visible acceptance; that gets a goarchive test.

### Versioning (read before bumping the version)

The version string carries the `-community` edition suffix; release candidates keep it and
add an `RC` marker before it. It is repeated at four sites, and a bump MUST update **all** of
them — a missed one ships mislabeled binaries:

| Location | What it controls |
|----------|------------------|
| `Makefile` → `RELEASE_VERSION` | Fallback version stamped into binaries when HEAD has no exact-match git tag. **The one most often missed.** |
| `cmd/goarchive/cmd/root.go` → `Version` | Default `Version` constant (overridden by `-ldflags` at build time) |
| `README.md` (the **Version** line) | User-facing docs |
| `README.md` (the **Stable release** line) | User-facing docs. Must equal `RELEASE_VERSION` when that is a stable version; not compared during a `-RC`/`-alpha`/`-beta` prerelease series, when it keeps naming the previous stable release. |

`TestVersionSitesMatchReleaseVersion` (`tests/harness/version_drift_test.go`) is the guard: it
fails `make check` when a site differs from `RELEASE_VERSION`, or when the release version
appears in any other tracked file or on any other `README.md` line. Cite a release elsewhere by
its bare number; an exception needs an allow-list entry in that test, approved by the operator.

No workflow parses the version — the workflows trigger on the `v*` glob and extract it with a
prefix strip, and CI injects the literal `ci-test`; the drift test is the one reader. The
only semver consumer is `docker/metadata-action` (`.github/workflows/docker.yml`); both forms
are valid SemVer prereleases, which is also what makes `release.yml` mark the GitHub release a
prerelease automatically (`prerelease: contains(VERSION, '-')`).

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

### The 3.0 line (this checkout)

This checkout is branch `v3`, the 3.0 innovation line; `../goarchive` is the 2.x line and is
never modified from here. Until 3.0-alpha (rulings R33, R34 in
`.ayder/superpowers_20261007/decisions/2026-10-07-3.0-core-brainstorm-rulings.md`):

- Work happens on `v3-dev/<milestone>` branches merged locally into `v3`, after the operator's
  confirmed verdict. Nothing is pushed and no GitHub PR is opened.
- `make gate` is not required between merges, and `make deadcode` may be red while new packages
  are not yet wired. Every merge keeps `go build ./...` and `go vet ./...` passing, and every
  task runs its own tests.
- The design is `.ayder/superpowers_20261007/specs/2026-10-07-3.0-core-design.md`, and the plans
  are in `.ayder/superpowers_20261007/plans/`. The approved spec is authoritative.
- The roadmaps named in *Release lines* above live in `../goarchive/.ayder/roadmaps/`. This
  checkout has no copy.
- The test containers (db1/db2/db3) are dedicated to this checkout since 2026-10-10 (R55): the
  same compose project and volumes as `../goarchive`'s, and M7 may run `make test-up`,
  `test-reset` and `e2e` from here. Run nothing else on the estate while they run, and ask the
  operator before a reset destroys a schema that is not the suite's own.
- **From M6 the commands run on the 3.0 packages** (R45 lifted): `archive`, `purge` and
  `copy-only` run `internal/engine`, `dry-run` runs `engine.Explain`, `validate` runs
  `internal/preflight`, and `plan` and `list-jobs` read `internal/jobspec`. `--skip-verify` and
  `--force` are gone (R42, R50). Still never run a v3 build against data that matters before
  3.0-alpha. The E2E suite runs on the 3.0 binary since M7b.
- The 3.0-alpha release PR is the first GitHub PR; it re-measures the baselines and must pass
  the full gate. From then on the rules above this section apply unchanged.

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
CLI (Cobra) → Config (internal/jobspec) → Preflight → Engine → Data Layer
```

**Stack:** Go 1.26+ (builds pinned to go1.26.8 by go.mod's toolchain line), Cobra (CLI), yaml.v3
(config, `internal/jobspec`), Zap (logging), MySQL 8.0.40+ InnoDB only, `dbsgomysql` for validation facts.

### Package Layout

| Directory | Purpose |
|-----------|---------|
| `cmd/` | CLI command implementations (Cobra) |
| `internal/config/` | The shared configuration blocks (source, destination, replication, processing, safety, logging) and their validation; `internal/jobspec` loads the file |
| `internal/database/` | Database connection management, signal handling |
| `internal/engine/` | 3.0 run engine: one run loop for archive, purge and copy-only; startup, recovery, discovery, copy and verification, deletes, INSERT diagnostics |
| `internal/jobspec/` | 3.0 job model: strict config loading, job rules, the table tree and the `references` projection |
| `internal/key/` | 3.0 key model: key definitions from MySQL facts, typed keys, key SQL shapes, statement cap |
| `internal/lock/` | MySQL advisory locking for job concurrency |
| `internal/logger/` | Structured logging (Zap wrapper) |
| `internal/mermaidascii/` | ASCII diagram rendering for plan command |
| `internal/preflight/` | 3.0 preflight: checks by job type over dbsgomysql facts, key definitions, statement caps |
| `internal/replication/` | Replication lag gate over the library's replica status |
| `internal/tracking/` | 3.0 tracking: the `3.0` schema label, the job registry and binding, the typed per-job log |
| `internal/types/` | Temporal column metadata and the temporal read contracts |

SQL identifier quoting and validation now live in the **library**
(`github.com/dbsmedya/dbsgomysql/pkg/sqlutil`); there is no `internal/sqlutil/`.

### Processing Flow

`internal/engine` (spec §7, `.ayder/superpowers_20261007/specs/2026-10-07-3.0-core-design.md`):

1. **Startup**: the command loads the file (`jobspec.Load`) and picks the job
   (`Document.Job`) before any connection, then preflight (`internal/preflight`) → tracking schema → advisory locks → job registry,
   log-key and binding checks (`internal/tracking`) → recovery of logged roots
2. **Batch loop**: fetch root keys after the last batch's → discover children parent-first →
   per job type, a destination transaction (copy → verify → log) and/or source delete chunks,
   each its own transaction with an exact row count
3. **Safety**: `GET_LOCK()` advisory locks, the replication lag gate before every delete chunk,
   the sentinel pause and a graceful stop at batch boundaries

### Key Data Structures

- **archiver_job**: the job registry, with the job binding (source identity, destination
  database, config fingerprint) and `root_key_columns`; integer `id` PK, `job_name` UNIQUE. Lives
  in `destination.job_schema` (default = destination database). There is no persisted cursor.
- **archiver_job_log_<id>**: per-job log whose key columns `k1…kN` mirror the root key's types,
  with `ga_status` TINYINT (1=copied, 2=completed, 4=deleting) for crash recovery.
- **goarchive_meta**: one-row tracking-schema marker. The binary recognises exactly one label
  (`SchemaLabel`, `internal/tracking/tracking.go`) and refuses every other revision, 2.x
  included, and every populated marker-less schema — no inference, no migration. A release that changes
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
| MySQL server behaviours that change how GoArchive binds keys (compatibility issues) | `docs/README_COMPAT.md` |

### Where behavior lives in code

The one thing `docs/` structurally cannot carry — internal symbols, for navigation.

| Behavior | Symbol |
|----------|--------|
| Schema-compatibility policy | `internal/preflight/destination.go` → `judgeSchema` |
| Destination unique-index rule (deviation D3) | same file → `checkDestinationUniqueness` |
| Per-diff-kind disposition, fail-closed `default`. It **parses** the library's `SpecDiff.A`/`.B` strings (types, `"true"`/`"false"`), so a change to their format in a dbsgomysql release changes goarchive policy | same file → `disposeDiff`, `typesCompatible` |
| Destination key identity and the utf8mb3→utf8mb4 twin (R32) | same file → `checkDestinationKeys`, `twinCollation` |
| Key-value binding per type, including the TIME compatibility path (`docs/README_COMPAT.md`, R46) | `internal/key/sql.go` → `Placeholder` |
| The run loop and its in-memory cursor; phases per job type | `internal/engine/run.go` → `(*run).loop`, `(*run).batch` |
| Log status writes, inside the transaction that does the work | `internal/engine/run.go` → `logDeleting`, `complete`; `internal/engine/copy.go` → `(*run).copy`; `internal/tracking/log.go` → `(*Job).Insert`, `Transition` |
| Key-set checks: copy-read identity, verification counts, delete row counts, warning 3170 | `internal/engine/copy.go` → `read`; `verify.go` → `verifyCount`, `verifyHash`; `delete.go` → `deleteOne` |
| Warning 3170 and warnings the session did not keep (`max_error_count`) | `internal/key/chunk.go` → `CheckRangeWarning` |
| Packet bound of each statement cap (`MAX_ALLOWED_PACKET_CHECK`) | `internal/preflight/packet.go` → `checkMaxAllowedPacket`; `internal/key/chunk.go` → `StatementBytes` |
| Startup recovery (delete-only replay) | `internal/engine/recover.go` → `recover`, `replay` |
| Key definitions read from MySQL; documented `primary_key` check (R31) | `internal/preflight/keys.go` → `readKeys`, `checkPrimaryKeyColumns` |
| Relationship pairing, `foreign_key` index, declared-FK pairing | `internal/preflight/relations.go` → `checkRelationKeys`, `checkForeignKeyIndexes`, `checkInternalForeignKeys` |
| Check order and applicability by job type (R5) | `internal/preflight/preflight.go` → `stages`, `ProfileFor` |
| Source/destination identity guard (`SRC_DEST_IDENTITY_CHECK`) | `internal/database/identity.go` → `assertDistinctDatabases`, called from `Manager.Connect` |
| AUTO_INCREMENT zero connection initialization and assertion | `internal/database/database.go` → `BuildDSN`, `Manager.connectWithRetry`; `internal/database/auto_zero.go` → `assertAutoIncrementZeroMode` |
| Session pins on every connection (UTC, `sql_notes`, `sql_mode`, `autocommit`) | `internal/database/database.go` → `BuildDSN` |
| Temporal projection and representation contracts | `internal/types/temporal.go` → `ColumnMetadata`, `TemporalText`, `TemporalReadContractError` |
| INSERT diagnostic session, collection and classification | `internal/engine/diagnostics.go` → `readDiagnosticSession`, `collectInsertDiagnostics`, `classifyInsertDiagnostics`, `insertDiagnosticError` |
| Sticky `root_table` and job type; log-key and binding checks | `internal/tracking/job.go` → `(*Store).Attach` |
| Per-job logging inheritance | `cmd/goarchive/cmd/root.go` → `effectiveJobLogging` |
| Config identifier rule (`[A-Za-z0-9_]+`) | library → `sqlutil.IsSimpleIdentifier`, called from `internal/config/validation.go` |
| Integer display-width normalization (`bigint(20)` ≡ `bigint`) | library, unexported; goarchive reads `ColumnSpec.NormalizedType` |

> **Do not rename the `## Known Limits & Caution` heading in
> `docs/README_LIMITATIONS.md`.** An error message points operators at it by name
> (`internal/database/identity.go`, `SRC_DEST_IDENTITY_CHECK`). Renaming the heading breaks
> that reference.

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

Every internal artifact — designs, specs, implementation plans,
architectural decisions — **MUST** be written to:

```
.ayder/superpowers_<YYYYMMDD>/
├── plans/       # implementation plans, one per PR
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
- Reviews live under `.ayder/reviews/`. Never create
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
  gate instructions file and the owner of the measured baseline (the integration inventory).

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
