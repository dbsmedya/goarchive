# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

**This file is a map, not a copy.** Behavior is documented in `docs/`, testing in
`tests/README.md`. Anything duplicated here rots — record it in its owning file and point at
it from here.

## Project Overview

GoArchive is a Go CLI tool for safely archiving MySQL relational data across servers. It walks
each job's table tree from its declared relations, recovers an interrupted run by replaying the
job's typed log, and works in key-ordered batches.

Recommended for archival of cold data. 3.0 has one edition: there is no Community/Enterprise
split, and the version carries no edition suffix.

The current version is the `Makefile`'s `RELEASE_VERSION` (see *Versioning* below); `README.md` also states which release is stable.

**dbsgomysql** (`github.com/dbsmedya/dbsgomysql`, see `docs/README_dbsgomysql.md`):

- The library owns fact acquisition (schema capture, diffing, permission and FK-visibility
  proofs) and verifies it on MySQL 8.0, 8.4 and 9.7; goarchive owns the *policy* applied to the
  facts. GoArchive **must not query `information_schema` directly**: `make consumer-policy`
  fails the build if it does.
- The dependency is always a released tag: no `replace`, no pseudo-versions, no committed
  `go.work`.
- **Library gaps are fixed in the library** (a dbsgomysql release and a pin bump), never by
  compensating logic here. Where the library is stricter than goarchive's pre-2.0 behaviour,
  adopt it and document the change.
- A library bump is judged by goarchive's baselines alone: an unchanged integration inventory
  passes. Only a change that moves goarchive's operator-visible acceptance needs a goarchive test.

### Versioning (read before bumping the version)

From 3.0 the version is SemVer with no edition suffix (`3.0.0`); a prerelease adds `-alpha.N`,
`-beta.N` or `-RC.N` (2.x versions carry `-community`). A bump MUST update all four sites:

| Location | What it controls |
|----------|------------------|
| `Makefile` → `RELEASE_VERSION` | The version stamped into an untagged build. **The one most often missed.** |
| `cmd/goarchive/cmd/root.go` → `Version` | The default constant, overridden by `-ldflags` at build time |
| `README.md` (the **Version** line) | User-facing docs |
| `README.md` (the **Stable release** line) | Equals `RELEASE_VERSION` for a stable version; during a prerelease series it keeps naming the previous stable release and is not compared |

`TestVersionSitesMatchReleaseVersion` (`tests/harness/version_drift_test.go`) fails `make check`
when a site differs from `RELEASE_VERSION`, or when the release version appears in any other
tracked file. Cite a release elsewhere by its bare number; an exception needs an allow-list
entry in that test, approved by the operator. `cmd/goarchive/cmd/version_test.go`'s `1.2.3` is a
fixture, not the project version.

**Releases are automatic and irreversible:** `release.yml` publishes on any pushed `v*` tag, and
no tests run on a tag. The gate is the PR that contains the bump, and nothing lands after it
passes. **Agents never create tags or releases; the operator does.** The release procedure
(tagging, version resolution, artifact checks, release notes) is `.ayder/RELEASING.md`.

### Release lines

- **2.x is fixes only** (`../goarchive`): no features, refactors, tracking-schema changes or
  config breaks. Features go to 3.0. Placement is decided in the roadmaps,
  `../goarchive/.ayder/roadmaps/roadmap_v2.x-community.md` and `roadmap_v3.md` (numbered design
  decisions in its §2; cite them rather than restating them).
- **Port the test, not the patch.** A fix lands as a `test` commit, then a `fix` commit, never
  squashed. Moving a fix between lines cherry-picks the test and re-implements the fix. Never
  merge between lines.

### The 3.0 line (this checkout)

- This checkout is branch `v3`, the 3.0 line. `../goarchive` is the 2.x line: never modify it
  from here.
- The approved design is `.ayder/superpowers_20261007/specs/2026-10-07-3.0-core-design.md`.
- The test containers db1/db2/db3 are dedicated to this checkout (R55). Run nothing else on
  them while `make test-reset`, `e2e` or the gate runs, and ask the operator before a reset
  drops a schema that is not the suite's own.
- **Branches and PRs.** 3.0 PRs target `v3` on origin; 2.x stays frozen on `main` until `v3` is
  merged onto it by its own PR. The 3.0-alpha PR is the first.
- **Merge order for every PR into `v3`:** work on a branch (a `test` commit, then a `fix`
  commit) → Fable's final review for behaviour changes, when available → `gate-agent` reports
  GREEN on the final SHA (`tests/AGENTS.md`; re-measure the baselines when tests change), and for
  a behaviour change a green *Release verify* run on GitHub (`tests/README.md` → *The full matrix
  runs on GitHub*; started only with the operator's approval) → Codex's MR verdict on that SHA → the
  operator's verdict → push, open the PR, CI → merge. A
  release folds its version bump into its last PR; the operator tags.

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

### Run flow and tracking: where to read

The run's steps are `docs/README_OPERATIONS.md` → *What a run does* (code: `internal/engine`,
`Run` in `engine.go`, `(*run).loop` and `batch` in `run.go`; spec §7). The tracking tables, their
statuses and the startup checks are `docs/README_JOBS_SCHEMA.md` (code: `internal/tracking`).

**Tracking-schema label rule (for changes here):** a release that changes the tracking tables'
layout **or a column's meaning** bumps `SchemaLabel` (`internal/tracking/tracking.go`) and adds a
procedure row to `docs/README_JOBS_SCHEMA.md` → *Tracking-schema upgrade procedures*.

## Triage rules

- **Security findings: the mistake test.** GoArchive defends against the operator's
  *mistakes*, not against the operator, who already holds the credentials and a `mysql`
  client. A finding is a weakness only if it widens a forgivable mistake. If only a deliberate
  act reaches it, the privilege matrix (`docs/README_PERMISSIONS.md`) owns it, and the finding
  is closed citing `roadmap_v3.md` decision 14.
- **Rarity is not severity.** A real defect found late, or found by a new model, is fixed
  promptly. The mistake test decides *whether* it is a weakness; rarity never decides that in
  either direction.
- **Design within the operator contracts:** the run contract is published in
  `docs/README_LIMITATIONS.md` → *No DDL and no concurrent writes during a run (contract)*
  (spec §3, which replaces 2.x's monotonic-eligibility rule). Contract violations are
  documented, not detected: add no mid-run drift probe or DML tolerance without the operator's
  direction.

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
| Moving from 2.x to 3.0: tracking tables, config and flag changes | `docs/README_UPGRADING_3_0.md` |
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
| The driver reads each server's `max_allowed_packet` (large values go as long data); an INSERT value that still does not fit the destination's packet | `internal/database/database.go` → `BuildDSN`; `internal/engine/copy.go` → `packetError`; `internal/engine/errors.go` → `PacketError` |
| Storage engine checks, source and destination (`STORAGE_ENGINE_CHECK`, `DEST_STORAGE_ENGINE_CHECK`) | `internal/preflight/checks.go` → `checkStorageEngine`, `checkDestinationStorageEngine`, `judgeStorageEngine` |
| Trigger checks and their visibility proof: grants and triggers read on one session (`DELETE_TRIGGER_VISIBILITY_CHECK`, `DEST_INSERT_TRIGGER_VISIBILITY_CHECK`) | `internal/preflight/checks.go` → `checkDeleteTriggerVisibility`, `checkDestinationInsertTriggerVisibility`, `checkDeleteTriggers`, `checkDestinationInsertTriggers`; `facts.go` → `mysqlServer.TriggerProof` |
| Startup recovery (delete-only replay; refused before any delete when the destination lacks a rediscovered row, GH #7) | `internal/engine/recover.go` → `recover`, `replay`, `checkHeld`; `internal/engine/errors.go` → `UncopiedError` |
| Lock names (hashed, case-folded) and the lock set: one session, `wait_timeout` 300, keep-alive, discard on release; a waiting lock on its own session, `max_execution_time` 0 | `internal/lock/names.go` → `TableLockName`, `JobLockName`, `SchemaLockName`; `internal/lock/set.go` → `Set`; `internal/lock/wait.go` → `Wait` |
| Startup locking: on a new tracking schema the schema lock, for its creation only; then job lock, source table locks, then the unfinished-work guard (another job on the same source data, sharing a table, marked running or holding `copied`/`deleting` roots) | `internal/engine/startup.go` → `lockJob`, `OverlapError`; `internal/tracking/blockers.go` → `(*Store).Blockers`, `storedTables` |
| Tracking schema creation, one run at a time under the schema lock; a labelled schema takes no lock (TRK-1) | `internal/tracking/schema.go` → `ensureSchema`, `SchemaLockedError` |
| Same source data: `server_uuid`, `gtid_executed`, and the database-name fallback without `gtid_mode = ON` or on a Group Replication member (dbsgomysql facts) | `internal/engine/sourcedata.go` → `sameData`, `readSourceData` |
| Guarded commit (write → check → `COMMIT`) at every commit site | `internal/engine/run.go` → `(*run).commit` |
| Final status written only on a session that holds the job lock; skipped, naming the taker, when another run holds it | `internal/engine/startup.go` → `finalStatus`; `internal/lock/set.go` → `(*Set).Do` |
| Key definitions read from MySQL; documented `primary_key` check (R31) | `internal/preflight/keys.go` → `readKeys`, `checkPrimaryKeyColumns` |
| Relationship pairing, `foreign_key` index, declared-FK pairing; a self-reference's delete rule (`SELF_FK_DELETE_RULE_CHECK`) | `internal/preflight/relations.go` → `checkRelationKeys`, `checkForeignKeyIndexes`, `checkInternalForeignKeys`, `checkSelfForeignKeys` |
| Check order and applicability by job type (R5) | `internal/preflight/preflight.go` → `stages`, `ProfileFor` |
| Source/destination identity guard (`SRC_DEST_IDENTITY_CHECK`) | `internal/database/identity.go` → `assertDistinctDatabases`, called from `Manager.Connect` |
| AUTO_INCREMENT zero connection initialization and assertion | `internal/database/database.go` → `BuildDSN`, `Manager.connectWithRetry`; `internal/database/auto_zero.go` → `assertAutoIncrementZeroMode` |
| Session pins on every connection (UTC, `sql_notes`, `sql_mode`, `autocommit`, `sql_select_limit`) | `internal/database/database.go` → `BuildDSN` |
| Session `max_error_count` raised from 0 on every new source and destination connection (`MAX_ERROR_COUNT_CHECK`) | `internal/database/max_error_count.go` → `ensureMaxErrorCount`, `warningsConnector`, `poolConnector`; `Manager.connect` |
| Temporal projection and representation contracts | `internal/types/temporal.go` → `ColumnMetadata`, `TemporalText`, `TemporalReadContractError` |
| INSERT diagnostic session, collection and classification | `internal/engine/diagnostics.go` → `readDiagnosticSession`, `collectInsertDiagnostics`, `classifyInsertDiagnostics`, `insertDiagnosticError` |
| Sticky `root_table` and job type; log-key and binding checks | `internal/tracking/job.go` → `(*Store).Attach` |
| Per-job logging inheritance | `cmd/goarchive/cmd/root.go` → `effectiveJobLogging` |
| Log files created owner-only (`0600`); a run's closing log entry (completed, stopped, failed) | `internal/logger/logger.go` → `New`; `cmd/goarchive/cmd/output.go` → `logOutcome`, called from `runJob` (`runjob.go`) |
| Config identifier rule (`[A-Za-z0-9_]+`) | library → `sqlutil.IsSimpleIdentifier`, called from `internal/config/validation.go` |
| Integer display-width normalization (`bigint(20)` ≡ `bigint`) | library, unexported; goarchive reads `ColumnSpec.NormalizedType` |

> **Do not rename a doc heading that an error message names.** Operators are pointed at these
> by name: `## Known Limits & Caution` in `docs/README_LIMITATIONS.md`
> (`internal/database/identity.go`, `SRC_DEST_IDENTITY_CHECK`); *Crash recovery* in
> `docs/README_OPERATIONS.md` (`internal/engine/errors.go`, `UncopiedError`); *A value larger than
> the destination's `max_allowed_packet` fails its batch* in `docs/README_LIMITATIONS.md` (same
> file, `PacketError`); *Supported relationship types* in `docs/README_LIMITATIONS.md`
> (`internal/preflight/relations.go`, `SELF_FK_DELETE_RULE_CHECK`). Renaming one breaks that
> reference.

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

### Internal documents go under `.ayder/`

Specs, plans, decision records and PR evidence go under
`.ayder/superpowers_<YYYYMMDD>/{specs,plans,decisions,pr}/`, dated by the day the work started;
reviews under `.ayder/reviews/`. Name files `YYYY-MM-DD-<topic>.md`, designs `-design.md`.
Never put an internal document in `docs/`.

### Search the docs with RAG first, grep second

Query `mcp__dbs-vector__search_md_goarchive_3_search` before `grep` (if it 404s,
`mcp__dbs-vector__list_engines` names the current tool). Grep finds which files contain a token;
the failure here is two documents that agree in tokens and disagree in meaning. Before changing
a fact stated in more than one document (a count, a command, a version, a default, a rule),
search for every statement of it. Never set `min_similarity`, and an empty result is not proof
of absence.

## Running tests

- **`tests/README.md` is the single source of truth** for how goarchive's tests are defined
  and run. It is written for people; read it before adding or changing a test.
- **Agents follow [`tests/AGENTS.md`](tests/AGENTS.md)** to run tests and the gate. It turns
  `tests/README.md` into steps, cites its sections, and defines nothing of its own. It is the
  gate instructions file and the owner of the measured baseline (the integration inventory).
