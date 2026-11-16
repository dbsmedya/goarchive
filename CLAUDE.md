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

From 3.0 the version is SemVer with no edition suffix (`MAJOR.MINOR.PATCH`); a prerelease adds `-alpha.N`,
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
no tests run on a tag. The gate is the dev→main PR that contains the bump as its last change, and
nothing lands after it passes. **Agents never create tags or releases; the operator does.** The release procedure
(tagging, version resolution, artifact checks, release notes) is `.ayder/RELEASING.md`.

### Release lines

- **2.x is fixes only** (branch `release/2.2`, checkout `../goarchive-v2`, which this checkout never
  modifies): no features, refactors, tracking-schema changes or config breaks. Features go to 3.1.
  Placement is decided in the roadmaps,
  `../goarchive/.ayder/roadmaps/roadmap_v2.x-community.md` and `roadmap_v3.md` (numbered design
  decisions in its §2; cite them rather than restating them).
- **Port the test, not the patch.** A fix lands as a `test` commit, then a `fix` commit, never
  squashed. Moving a fix between lines cherry-picks the test and re-implements the fix. Never
  merge between lines.

### The 3.1 line (this checkout)

- This checkout is branch `dev/goarchive-3.1`, the 3.1 line.
- The approved design is `.ayder/superpowers_20261007/specs/2026-10-07-3.0-core-design.md`.
- The test containers db1/db2/db3 are dedicated to this checkout (R55). Run nothing else on
  them while `make test-reset`, `e2e` or the gate runs, and ask the operator before a reset
  drops a schema that is not the suite's own.
- **Development flow.** The order of work. `tests/README.md` → *Which checks a change needs* says
  what each change runs locally; *The full matrix runs on GitHub* says what GitHub runs.
  1. `dev/goarchive-3.1` is the long-lived development branch on origin. Each change gets its own
     branch cut from it, named `3.1/<id>-<topic>`. Change branches stay local.
  2. On the change branch: a `test` commit, then a `fix` or `feat` commit (TDD). A behaviour change
     first gets a spec and a lite plan from the Fable 5.2 agent. Codex's MR verdict comes before
     the merge; a fix after a finding goes back to Codex as a re-review.
  3. The coordinating agent merges the change branch into `dev/goarchive-3.1` with
     `git merge --no-ff` and pushes `dev/goarchive-3.1`. CI runs on that push.
  4. When the line is ready (a release, or when the operator asks), one PR
     `dev/goarchive-3.1` → `main` is opened, only after the operator's verdict. GitHub runs CI,
     Docker, *Gate verify* and *Matrix verify* on its head SHA; those runs are the release gate,
     and a green CI alone is not. The PR is merged with a merge commit, never squashed.
  5. The integration inventory and gate timings (`tests/AGENTS.md` → *Measured baselines*) are
     re-measured once per such PR, when tests were added or removed since the last measurement.
  6. `verify/<sha7>` and `verify-gate/<sha7>` pushes and `workflow_dispatch` runs are ad hoc, for
     the operator's request only.
  7. The version bump is the last change merged into `dev/goarchive-3.1` before the PR, so the PR
     that contains it is the gate. The operator tags `main` after the merge; agents never tag.

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
| `cmd/` | CLI command implementations (Cobra), including `goarchive control`, the client of a run's control socket (`control.go`) |
| `internal/config/` | The shared configuration blocks (source, destination, replication, processing, safety, logging, control) and their validation; `internal/jobspec` loads the file |
| `internal/control/` | The runtime control socket of a run (TEL-1): the listener and its lifecycle, the lock and identity checks, the protocol, the command table, the status, the audit emitter. Imports neither `engine` nor `replication`: `cmd` adapts their types to its read interfaces |
| `internal/controlpath/` | Where a run's control socket lives and whether a thing found there may be used: the path, the job-name and length rules, the judging of a directory and a socket. Imports nothing of goarchive, so the loader, the run and the client share one answer |
| `internal/database/` | Database connection management, the session checks of every new connection (`max_error_count`, the character set, the failover fence G12), signal handling (`Shutdown`: two-phase stop, also requested from code) |
| `internal/engine/` | 3.0 run engine: one run loop for archive, purge and copy-only; startup, recovery, discovery, copy and verification, deletes, INSERT diagnostics |
| `internal/jobspec/` | 3.0 job model: strict config loading, job rules, the table tree and the `references` projection |
| `internal/key/` | 3.0 key model: key definitions from MySQL facts, typed keys, key SQL shapes, statement cap |
| `internal/lock/` | MySQL advisory locking for job concurrency |
| `internal/logger/` | Structured logging (Zap wrapper) |
| `internal/mermaidascii/` | ASCII diagram rendering for plan command |
| `internal/preflight/` | 3.0 preflight: checks by job type over dbsgomysql facts, key definitions, statement caps |
| `internal/replication/` | Replication lag gate over the library's replica status |
| `internal/testestate/` | Test support, not production: what the integration suites of different packages share on one estate: the replica's lock (one test at a time, across packages) and the base-state checks of the replica and of the group; the fence harness (`BuildGroupFenceWith`, `FenceOptions`, `UnlockApplier`, `ElectPrimary`, `SetGlobalConsistency`), whose observing and undoing sessions come from `OpenUnfenced` because goarchive's own pool now fences its sessions. Nothing in the binary imports it |
| `internal/tracking/` | 3.0 tracking: the `3.0` schema label, the job registry and binding, the typed per-job log |
| `internal/tuning/` | A run's runtime settings store: snapshots and revisions, the writer lock, the ceilings and the step rule of the tolerance and of the two group bounds, the bounded audit queue. Every run reads one; only the control server writes it |
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
| Group Replication: running a job on a group, what is supported, the members in the gate and their two bounds, the ProxySQL and MySQL Router examples, following a proxy's threshold during a run, grants | `docs/README_GROUP_REPLICATION.md` |
| Hard constraints, model limits, operational cautions | `docs/README_LIMITATIONS.md` |
| Tracking table DDL, DBA maintenance, safe-truncate rules | `docs/README_JOBS_SCHEMA.md` |
| Removing duplicate rows with one job (keep the minimum primary key) | `docs/README_DUPLICATE_CLEANUP.md` |
| What the dbsgomysql integration changed, and why | `docs/README_dbsgomysql.md` |
| Moving from 2.x to 3.0: tracking tables, config and flag changes | `docs/README_UPGRADING_3_0.md` |
| MySQL server behaviours that change how GoArchive binds keys (compatibility issues) | `docs/README_COMPAT.md` |
| Character sets: the session pin, the supported and refused lists, converting a column | `docs/README_CHARSETS.md` |
| Runtime control: enabling and the path, the protocol contract (version, grammar, actions, settings, reason words, `status-json` fields), when a change applies, the ceilings, the step rule of the tolerance and the group bounds, security | `docs/README_RUNTIME_CONTROL.md` (the `control:` block: `docs/README_CONFIGURATION.md`) |

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
| Log status writes, inside the transaction that does the work | `internal/engine/run.go` → `logDeleting`, `complete`; `internal/engine/copy.go` → `(*run).copy`, `logInsertError` (a root the log already holds: `internal/engine/errors.go` → `RecurringRootError`); `internal/tracking/log.go` → `(*Job).Insert`, `Transition` |
| Key-set checks: copy-read identity, verification counts, delete row counts, warning 3170 | `internal/engine/copy.go` → `read`; `verify.go` → `verifyCount`, `verifyHash`; `delete.go` → `deleteOne` |
| Warning 3170 and warnings the session did not keep (`max_error_count`) | `internal/key/chunk.go` → `CheckRangeWarning` |
| Packet bound of each statement cap (`MAX_ALLOWED_PACKET_CHECK`) | `internal/preflight/packet.go` → `checkMaxAllowedPacket`; `internal/key/chunk.go` → `StatementBytes` |
| The driver reads each server's `max_allowed_packet` (large values go as long data); an INSERT value that still does not fit the destination's packet | `internal/database/database.go` → `BuildDSN`; `internal/engine/copy.go` → `packetError`; `internal/engine/errors.go` → `PacketError` |
| Storage engine checks, source and destination (`STORAGE_ENGINE_CHECK`, `DEST_STORAGE_ENGINE_CHECK`) | `internal/preflight/checks.go` → `checkStorageEngine`, `checkDestinationStorageEngine`, `judgeStorageEngine` |
| Trigger checks and their visibility proof: grants and triggers read on one session (`DELETE_TRIGGER_VISIBILITY_CHECK`, `DEST_INSERT_TRIGGER_VISIBILITY_CHECK`) | `internal/preflight/checks.go` → `checkDeleteTriggerVisibility`, `checkDestinationInsertTriggerVisibility`, `checkDeleteTriggers`, `checkDestinationInsertTriggers`; `facts.go` → `mysqlServer.TriggerProof` |
| Startup recovery (delete-only replay; refused before any delete when the destination lacks a rediscovered row, GH #7) | `internal/engine/recover.go` → `recover`, `replay`, `checkHeld`; `internal/engine/errors.go` → `UncopiedError` |
| Lock names (hashed, case-folded) and the lock set: one session, `wait_timeout` 300, keep-alive, discard on release; a waiting lock on its own session, `max_execution_time` 0 | `internal/lock/names.go` → `TableLockName`, `JobLockName`, `SchemaLockName`; `internal/lock/set.go` → `Set`; `internal/lock/wait.go` → `Wait` |
| Startup locking: on a new tracking schema the schema lock, for its creation only; then job lock, source table locks, then the unfinished-work guard (another job on the same source data, sharing a table, marked running or holding `copied`/`deleting` roots) | `internal/engine/startup.go` → `lockJob`, `OverlapError`; `internal/tracking/blockers.go` → `(*Store).Blockers`, `storedTables` |
| Exit codes; an unfinished-work refusal exits 4 only while every blocker is live (EXC-1) | `cmd/goarchive/cmd/exit.go` → `exitCode`; `internal/engine/startup.go` → `live`, `(*OverlapError).Live` |
| Tracking schema creation, one run at a time under the schema lock; a labelled schema takes no lock (TRK-1) | `internal/tracking/schema.go` → `ensureSchema`, `SchemaLockedError` |
| Same source data: `server_uuid`, `gtid_executed`, and the database-name fallback without `gtid_mode = ON` or on a Group Replication member (dbsgomysql facts) | `internal/engine/sourcedata.go` → `sameData`, `readSourceData` |
| Group Replication facts: preflight's second stage (`GroupFacts`) reads each side's `GroupReplication`, then, for a plugin `ACTIVE` with a name, the source's `GroupMembers`, before any table and under `SkipPolicy` too; both `ReplicationConfig` reads come before it (the `ReadOnly` stage, below). They are returned in `Result.Group` (`SourceConfig`, `DestinationConfig`, the plugin states, the view) with the identity function's answer (recorded, not yet the binding). A refused view read is `SOURCE_GROUP_VIEW_PERMISSION_CHECK`. The report block of `validate`, `dry-run` and the run log | `internal/preflight/group.go` → `readGroupFacts`, `Group`, `Identity`, `(*Result).GroupReport`; `preflight.go` → `stages`; `cmd/goarchive/cmd/output.go` → `printGroupReport`, `printValidation`; `internal/engine/engine.go` → `logGroupReport` |
| A read-only source or destination (`SOURCE_READ_ONLY_CHECK`, `DEST_READ_ONLY_CHECK`): preflight's first stage reads the source's `ReplicationConfig` and refuses an archive or a purge when `read_only` or `super_read_only` is on (copy-only exempt), then reads the destination's and refuses every job type on `super_read_only`; both are judged before any statement a fence holds, outside `SkipPolicy` (no flag skips them). The destination names no cause; the source's refusal ends with its cause from the role of its own row (a SECONDARY names the primary's host and port; a read-only PRIMARY says it was newly elected or made read-only; a plain server, a plugin that is not ACTIVE and a member that is not ONLINE keep the general sentence; a member whose account cannot read the view gets the grant named), read after the refusal's facts by the library's v1.6.0 plugin-state read and the view, which a fence does not hold | `internal/preflight/group.go` → `checkReadOnly`, `readOnlyVariables`, `readOnlyGeneral`, `readOnlySourceCause`, `readOnlyViewGrant`, `memberAddress`; `preflight.go` → `stages` (`ReadOnly`) |
| Copy-only's warning of a read-only primary (spec §4.5): a copy-only run whose source is the PRIMARY of its group in its own view and read-only is not refused and prints one warning before its first table read, through `Config.Banner`, under `SkipPolicy` too: what a newly elected primary may do (hold the run's reads until its backlog is applied, for at most the session's `wait_timeout`) and the stop sequence. A secondary, a writable primary and a plain server get none | `internal/preflight/group.go` → `warnReadOnlyPrimary`; `preflight.go` → `stages` (`ReadOnlyPrimaryWarning`) |
| The failover fence (G12): on every physical source and destination connection, before the pool hands it out, a replacement included, `warningsConnector.Connect` runs `ensureGroupConsistency` after the max_error_count and charset checks: the library's `GroupConsistencyOnDriverConn` reads the plugin's state and the session's own `group_replication_consistency` on the driver connection (two facts kept apart; the global decides nothing); with the plugin `ACTIVE` an `EVENTUAL` session gets goarchive's one `SET` to `BEFORE_ON_PRIMARY_FAILOVER` and the library's read, whole, proves it; the four stronger values are kept; an unfamiliar value, no value, an unreadable state, a refused `SET` and a read-back that differs refuse (`GROUP_CONSISTENCY_CHECK`, the server named, the two ways out). A refused or cancelled connection is closed and not handed out; a server's decision is named and not retried, a failed or cancelled read is wrapped and transient; no refusal is a bare driver sentinel. A server without the plugin costs four small reads and nothing is set | `internal/database/group_consistency.go` → `ensureGroupConsistency`, `GroupConsistencyError`, `groupConsistencyFailure`; `internal/database/max_error_count.go` → `warningsConnector.Connect`; `internal/database/database.go` → `openOn` |
| `replication.servers[].type` (async or group), the two group bounds and the load rules (no channels on a group server, both bounds required with one) | `internal/config/config.go` → `ReplicationConfig`, `ReplicationServerConfig`; `validation.go` → `validateReplication`, `validateGroupBounds` |
| The multi-primary refusal (`GROUP_MULTI_PRIMARY_CHECK`, G10): the library's mode fact (`GroupReplication.SinglePrimaryMode`, `SinglePrimaryModeKnown`) of each side whose plugin is `ACTIVE` with a group name, the source first, for every job type, after the group facts and before any table, **under `SkipPolicy` too** (the one bypass is `--allow-multi-primary`). `ON` passes; `OFF`, no value and any other text are refused (the text named as spelled); a running member's refusal says the group is in multi-primary mode, a source that is not running and a destination say the server is configured for it; no role is consulted. The bypass turns each refusal into a banner that names the case. The report's mode word | `internal/preflight/multiprimary.go` → `checkMultiPrimary`, `modeRefusal`, `modeWord`; `preflight.go` → `stages` (`MultiPrimary`), `Config.AllowMultiPrimary`; `group.go` → `sourceLine`, `destinationLine`; `internal/engine/engine.go` → `Config.AllowMultiPrimary`; `cmd/goarchive/cmd/runjob.go` → `registerAllowMultiPrimaryFlag`, `allowMultiPrimaryFlag` |
| `REPLICATION_TYPE_CHECK` (a `type: group` server must be configured for the source's group; an unreadable one is a warning) and the banner of a member source no group server watches: two stages after `GroupFacts`, for an archive and a purge with replication enabled | `internal/preflight/replication.go` → `checkReplicationType`, `warnUnmonitoredGroup`, `poolGroupRead`; `preflight.go` → `stages` (`ReplicationType`, `ReplicationBanner`), `Config.Replication`, `Config.ReplicaPools` |
| REPLICATION_CONFIGURED's unconditional hint | `internal/replication/problem.go` → `renderProblem`, `groupTypeHint` |
| The gate's group server (RG-1): the probe (the view, then the stats, on the probe's context), the pure judgement in the library's order with coverage and the source rule after it, the three states of what the gate was told of the source, the verdict's type and queues | `internal/replication/group.go` → `groupProbe`, `(*groupProbe).read`, `evaluateGroup`, `uncovered`, `sourceProblems`, `largestQueues`, `groupReadDetail`, `(*Gate).SetSource`; `gate.go` → `New`, `WaitForLag`, `record`, `ServerVerdict` |
| The source handover: preflight's answer, the engine's call when preflight returns, the command's wiring | `internal/preflight/group.go` → `Group.SourceMemberID`; `internal/engine/engine.go` → `Config.TellSource`; `cmd/goarchive/cmd/runjob.go` → `runJob` |
| The status fields of a group server and the two bounds (additions to `status-json`, protocol 1): a group verdict's two queues and the two bounds it was judged by; `replication.group_*_within` are the values in force, read from the same snapshot as `settings.*.value`, and the configured values where the gate is unused; `status` says each queue with its bound | `internal/control/status.go` → `ServerVerdict`, `Info`, `verdictJSON`, `replicationJSON`, `verdictText`, `(*view).groupBounds`, `groupBoundsText`; `cmd/goarchive/cmd/controlrun.go` → `gateSlot.Record`, `groupBounds` |
| The named stops for MySQL error 3781 and error 3797 (a statement a Group Replication fence held past the session's `wait_timeout`: 3781 under `BEFORE_ON_PRIMARY_FAILOVER` or `AFTER`, 3797 under `BEFORE` or `BEFORE_AND_AFTER`, each with its own message): found by number in the error's chain, applied where a failure is printed, before an aggregate drops the chain; the exit code stays 1 | `cmd/goarchive/cmd/output.go` → `heldStatementTimeout`, `failureText`, `logOutcome`; `exit.go` → `finish`; `validate.go` → `printFailure` |
| A commit that Group Replication refused with error 3100 becomes a `GroupCommitError`, and a statement it refused with 3231 (the write set) a `GroupWriteSetError`, when that side's size limit was known at the start: side, transaction, limit, the setting that sizes it and its value. 3231 is classified at the three sites where a transaction ends, on the error it ends with, and the transaction is rolled back, never committed. The membership phrase and the limit come from `preflight.Result` (`SizeLimit`); the engine reads no group fact of its own | `internal/engine/errors.go` → `GroupCommitError`, `GroupWriteSetError`, `Commit`; `run.go` → `(*run).groupCommitError`, `(*run).groupWriteSetError`, `(*run).endedBy`, `inDestinationTx`; `copy.go` → `(*run).copy`; `delete.go` → `deleteOne`; `internal/preflight/group.go` → `Group.SourceSizeLimit`, `Group.DestinationSizeLimit` |
| Guarded commit (write → check → `COMMIT`) at every commit site | `internal/engine/run.go` → `(*run).commit` |
| Final status written only on a session that holds the job lock; skipped, naming the taker, when another run holds it | `internal/engine/startup.go` → `finalStatus`; `internal/lock/set.go` → `(*Set).Do` |
| Key definitions read from MySQL; documented `primary_key` check (R31) | `internal/preflight/keys.go` → `readKeys`, `checkPrimaryKeyColumns` |
| Relationship pairing, `foreign_key` index, declared-FK pairing; a self-reference's delete rule (`SELF_FK_DELETE_RULE_CHECK`) | `internal/preflight/relations.go` → `checkRelationKeys`, `checkForeignKeyIndexes`, `checkInternalForeignKeys`, `checkSelfForeignKeys` |
| Check order and applicability by job type (R5) | `internal/preflight/preflight.go` → `stages`, `ProfileFor` |
| Source/destination identity guard (`SRC_DEST_IDENTITY_CHECK`) | `internal/database/identity.go` → `assertDistinctDatabases`, called from `Manager.Connect` |
| AUTO_INCREMENT zero connection initialization and assertion | `internal/database/database.go` → `BuildDSN`, `OpenPool`; `internal/database/auto_zero.go` → `assertAutoIncrementZeroMode` |
| Session pins on every connection (UTC, `sql_notes`, `sql_mode`, `autocommit`, `sql_select_limit`, the character set `utf8mb4` / `utf8mb4_general_ci`); the one opener, used by `Manager` and the integration suites, and a test's emulated server global | `internal/database/database.go` → `BuildDSN`, `OpenPool`; `internal/database/globals.go` → `Global` |
| Session character set proven on every new source and destination connection (`SESSION_CHARSET_CHECK`) | `internal/database/session_charset.go` → `ensureSessionCharset`; `internal/database/max_error_count.go` → `warningsConnector` |
| Job columns refused outside the character sets that survive the `utf8mb4` session (`COLUMN_CHARSET_CHECK`); the supported list | `internal/preflight/charsets.go` → `checkColumnCharsets`, `supportedCharsets` |
| JSON columns named in a warning under `count` and `none` (the JSON warning, banner-visible through `Config.Banner`) | `internal/preflight/json.go` → `checkJSONVerification`; `internal/preflight/preflight.go` → `Config.Banner` |
| A sha256 mismatch on a table with JSON columns names them | `internal/engine/errors.go` → `VerificationError`; `internal/engine/columns.go` → `columnMetadataFromFacts` |
| Run banners (`--skip-…`, `--force-triggers`, `verification: count` and `none`, FK checks off) | `cmd/goarchive/cmd/output.go` → `banners`, `emitBanners` |
| Session `max_error_count` raised from 0 on every new source and destination connection (`MAX_ERROR_COUNT_CHECK`) | `internal/database/max_error_count.go` → `ensureMaxErrorCount`, `warningsConnector`, `poolConnector`; `internal/database/database.go` → `OpenPool` |
| Temporal projection and representation contracts | `internal/types/temporal.go` → `ColumnMetadata`, `TemporalText`, `TemporalReadContractError` |
| INSERT diagnostic session, collection and classification | `internal/engine/diagnostics.go` → `readDiagnosticSession`, `collectInsertDiagnostics`, `classifyInsertDiagnostics`, `insertDiagnosticError` |
| Sticky `root_table` and job type; log-key and binding checks | `internal/tracking/job.go` → `(*Store).Attach` |
| Per-job logging inheritance | `cmd/goarchive/cmd/root.go` → `effectiveJobLogging` |
| Log files created owner-only (`0600`); a run's closing log entry (completed, stopped, failed) | `internal/logger/logger.go` → `New`; `cmd/goarchive/cmd/output.go` → `logOutcome`, called from `runJob` (`runjob.go`) |
| Runtime settings store: the writer lock, the shared per-value validators, the ceilings (D7), the tolerance's step rule (D15) over the gate's completed rounds, the terminal transition | `internal/tuning/store.go` → `Store.Set`, `candidate`, `raise`, `RoundCompleted`, `Terminal`; `internal/config/validation.go` → `ValidateBatchSize`… `ValidateCheckInterval` |
| The two group bounds as runtime settings (C7s, G6, G15): `group_applier_queue_within` and `group_certifier_queue_within` follow `check_interval` in the store, applicable only where the run's gate lists a `type: group` server (the gate's own words otherwise); one per-value rule shared with the file; a raise above the configured value is the tolerance's step rule (`Store.raise`) with each bound's own configured value as the step (1 for a configured 0); the tolerance, the interval and the two bounds share one gate revision and so one probe round. The gate reads both bounds from the round's snapshot, judges every group server by that pair and records it on each verdict (`ServerVerdict.ApplierQueueWithin`, `CertifierQueueWithin`; nil for an asynchronous server and an unprobed one); a pass cached or reached under an older pair never stands. The raise WARNING covers a bound | `internal/tuning/keys.go` → `GroupApplierQueueWithin`, `GroupCertifierQueueWithin`, `Applicability.Group`, `Key.stepped`; `internal/tuning/store.go` → `candidate`, `(*Store).raise`; `internal/config/validation.go` → `ValidateGroupApplierQueueWithin`, `ValidateGroupCertifierQueueWithin`, `validateGroupBounds`; `internal/replication/gate.go` → `New` (the constant store and the store check), `WaitForLag`, `record`, `ServerVerdict`; `internal/replication/group.go` → `groupProbe`, `groupBounds`; `internal/control/commands.go` → `settingHelp`; `emitter.go` → `warnIfRaised`; `cmd/goarchive/cmd/controlrun.go` → `newRunStore` |
| The gate on the store: tolerance and interval read per probe round, a pass valid only for its gate revision, a replaced-revision pass discarded with LAG-1's hold state kept, the per-server verdict record | `internal/replication/gate.go` → `WaitForLag`, `wait`, `cachedPass`, `Record` |
| Control socket start, end and lifecycle: directory check or creation, lock, live/stale/undecided decision (only `ECONNREFUSED` is stale), bind with auto-unlink off, removal by identity, the fixed end order | `internal/control/server.go` → `Start`, `clearStaleSocket`, `Ending`, `Drain`, `Close`; `startup.go` → `prepareDir`, `classifyStartAttempt`, `removeIfSame`; `identity*.go` → `identityAt`, `sameFile`, `inspect` |
| The identity watch (directory, lock, socket once a minute; reports, repairs nothing) | `internal/control/watch.go` → `checkIdentities` |
| Protocol framing and lexical form; the command table and its reason words; replies | `internal/control/protocol.go` → `readRequest`, `parseRequest`, `reply.wire`; `commands.go` → `commands.execute` |
| Same user only (D11) | `internal/control/peer.go` → `judgePeer`; `platform_*.go` → `peerUID` |
| The audit emitter: the one place the control server writes output, the bounded queue, the cutoff and drain, the raise WARNING through `Banner` | `internal/control/emitter.go` → `emitter`; `internal/tuning/audit.go` → `Audit` |
| The status: the state word's precedence, `status` text, `status-json` | `internal/control/status.go` → `view.text`, `view.jsonLine`, `read.state` |
| The progress tracker every run has, fed at each commit | `internal/engine/progress.go` → `Progress`, `Progress.Status` |
| A stop requested from code (the socket's `stop`), handed to the signal goroutine | `internal/database/signal.go` → `Shutdown.RequestFirst` |
| The control socket in a run: the settings store and tracker every run builds, the server before connect, the adapters, the end order, the closing line, exit 4, the `Control socket:` entry | `cmd/goarchive/cmd/controlrun.go` → `newRunStore`, `startControl`, `controlRun.end`, `runtimeChanges`, `logControlSocket`; `runjob.go` → `runJob`; `exit.go` → `exitCode` |
| Job-name rule for a control socket, with Windows' reserved device names | `internal/controlpath/controlpath.go` → `JobNameError`, `ReservedOnWindows`; `internal/jobspec/validate.go` → `validateControl` |
| The client: `goarchive control`. The command line is judged and spelled before connecting (`controlLine`); the endpoint is looked at without following links (`checkEndpoint`, the judging functions of `controlpath`); a failed connect is classified by the client's own deadline and the server's refusal predicate (`classifyConnect`, `control.IsRefused`, `isQueueFull` per platform); the reply's envelope is judged and nothing in its body (`judgeReply`); every failure is a `*controlError` that `exitCode` maps to 1 or 5 to 9 | `cmd/goarchive/cmd/control.go` → `controlLine`, `controlClient.ask`, `checkEndpoint`, `classifyConnect`, `judgeReply`; `control_other.go`, `control_windows.go` → `isQueueFull`; `exit.go` → `controlError`, `exitCode` |
| The client's help is the server's table | `internal/control/commands.go` → `HelpTable`, `(*commands).help`; the client's request limit is `control.MaxRequest` |
| Config identifier rule (`[A-Za-z0-9_]+`) | library → `sqlutil.IsSimpleIdentifier`, called from `internal/config/validation.go` |
| Integer display-width normalization (`bigint(20)` ≡ `bigint`) | library, unexported; goarchive reads `ColumnSpec.NormalizedType` |

> **Do not rename a doc heading that an error message names.** Operators are pointed at these
> by name: `## Known Limits & Caution` in `docs/README_LIMITATIONS.md`
> (`internal/database/identity.go`, `SRC_DEST_IDENTITY_CHECK`); *Crash recovery* in
> `docs/README_OPERATIONS.md` (`internal/engine/errors.go`, `UncopiedError`); *A value larger than
> the destination's `max_allowed_packet` fails its batch* in `docs/README_LIMITATIONS.md` (same
> file, `PacketError`); *Supported relationship types* in `docs/README_LIMITATIONS.md`
> (`internal/preflight/relations.go`, `SELF_FK_DELETE_RULE_CHECK`); *No DDL and no concurrent
> writes during a run (contract)* in `docs/README_LIMITATIONS.md` and *Maintenance: what is safe
> to delete* in `docs/README_JOBS_SCHEMA.md` (`internal/engine/errors.go`, `RecurringRootError`);
> *JSON values that do not survive a text copy* in `docs/README_COMPAT.md`
> (`internal/engine/errors.go`, `VerificationError`; `internal/preflight/json.go`, the JSON
> warning). Renaming one breaks that reference, and `TestErrorMessagesQuoteDocHeadings`
> (`tests/harness`) fails `make check`.

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
