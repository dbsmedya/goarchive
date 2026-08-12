# Limitations & Constraints

What GoArchive Community edition cannot do, what it refuses to do, and what to
plan around before pointing it at real data.

**Related:** [Configuration](README_CONFIGURATION.md) ·
[Validation & Preflight](README_VALIDATION.md) ·
[Permissions](README_PERMISSIONS.md) · [Operations](README_OPERATIONS.md) ·
[← Back to README](../README.md)

---

> [!WARNING]
> This tool performs data deletion on your source database. It is in use in
> limited production environments, but has not undergone exhaustive large-scale
> testing. **Rigorously test every archive job in a staging environment with
> representative data before running it against production.**

The Community edition is recommended for **single-operator workstation archival
of cold data**. It is not designed for hot, actively-transacting tables.

---

## Contents

- [Known Limits & Caution](#known-limits--caution)
  - [Hard constraints — rejected by preflight](#hard-constraints--rejected-by-preflight)
  - [Privileges must be provable, not merely present (2.0)](#privileges-must-be-provable-not-merely-present-20)
  - [Model limitations](#model-limitations)
  - [Operational cautions](#operational-cautions)
- [Trust model](#trust-model)
- [Environment](#environment)
- [What's included in Community](#whats-included-in-community)

---

## Known Limits & Caution

> This is the section GoArchive's preflight error messages refer to.

### Hard constraints — rejected by preflight

These are not warnings. Preflight fails and the run does not start.

#### Single-column primary keys only; root primary keys must be integer

GoArchive identifies, copies, verifies, and **deletes** rows by a single
primary-key column (`WHERE pk IN (...)`).

- **Composite (multi-column) primary keys are not supported** on any
  participating table (`COMPOSITE_PK_CHECK`). A composite PK would make the
  single-column filter over-match and could delete rows that were never part of
  the archived set.
- Every participating table must have a **single-column `PRIMARY KEY` equal to
  its configured `primary_key`** (`PRIMARY_KEY_CHECK`). Tables with no primary
  key, or where `primary_key` names some other column, are rejected for the same
  over-match reason.
- **Root tables must additionally use an integer primary key** — `TINYINT`
  through `BIGINT`, signed or unsigned (`ROOT_PK_TYPE_UNSUPPORTED`).
  Checkpointing advances a numeric high-water mark, which needs an ordered
  integer key. UUID, `VARCHAR`, `DECIMAL`, `FLOAT`, and datetime root keys are
  rejected.
- **Child tables may use any single-column primary key type.**

If your schema uses composite keys on the tables you want to archive, Community
edition cannot safely archive them.

#### No DDL and no concurrent writes during a run (contract)

`INVISIBLE` columns are fully supported: the copy, both verification reads, and
dry-run payload sampling name every column explicitly, so hidden columns are
copied and hashed like any other.

That correctness rests on two operating contracts:

1. **No DDL on participating tables while a job runs.** Column lists are read
   once at startup. A column renamed or dropped mid-run fails the next batch
   with a MySQL "unknown column" error before that batch reaches deletion;
   previously completed batches remain deleted. A column **added** mid-run is
   *not detected*: it is not copied, and DDL alone can diverge source and
   destination (an add on one side only, or adds with differing defaults) —
   with no error raised.
2. **GoArchive archives cold data.** Concurrent transactions on the archived
   row range are unsupported, including column additions and column-default
   changes taking effect mid-run.

#### InnoDB only

Legacy engines such as MyISAM are strictly unsupported — they lack the
transactional integrity the copy-verify-delete cycle requires
(`STORAGE_ENGINE_CHECK`).

#### Uncovered incoming foreign keys

Any table **outside** the graph holding a foreign key **into** it is fatal, for
every `ON DELETE` rule (`FK_COVERAGE_CHECK`). See
[Model limitations](#model-limitations) for why cross-schema children cannot be
modelled at all.

#### Cross-schema FK coverage requires `PROCESS` on the source account

`FK_COVERAGE_VISIBILITY_CHECK` fails closed unless InnoDB's foreign-key metadata registry
can be read, which requires the `PROCESS` privilege — MySQL hides constraints and entire
schemas from unprivileged accounts, so the `information_schema` fallback cannot prove
complete detection.

Enforced for `archive`, `purge`, `dry-run`, and `validate`; `copy-only` is exempt.
**Upgrade impact:** an account that ran cleanly on 1.8 with a global `SELECT` and no
`PROCESS` will now fail this check, even with no cross-schema foreign keys present. See
[Permissions](README_PERMISSIONS.md).

#### Destination schema must not be stricter than source

The destination may drop secondary indexes, `auto_increment`, and defaults, and
may relax `NOT NULL`. It must not add constraints the source lacks. See the full
matrix in
[Validation](README_VALIDATION.md#schema-compatibility-rules).

#### DELETE triggers require an explicit override

`archive` and `purge` refuse to run against source tables carrying DELETE
triggers until you pass `--force-triggers`, having reviewed what those triggers
do. See [Database triggers](#database-triggers) below.

### Privileges must be provable, not merely present (2.0)

GoArchive 2.0 passes a permission check only when the privilege is *established* for the
object being checked. Two configurations that worked in 1.8 now fail:

- **Privileges granted through a role.** MySQL does not expose a role's grant rows to the
  account holding the role, so GoArchive cannot prove the privilege. Grant DML privileges
  directly to the account GoArchive connects as. (`PROCESS` is exempt — see
  [Permissions](README_PERMISSIONS.md).)
- **A bare global grant under `@@global.partial_revokes`.** A global privilege row proves
  nothing about a particular schema once partial revokes are enabled. Add a direct schema- or
  table-level grant.

---

### Model limitations

Things the dependency model cannot express, which preflight cannot always catch
for you.

#### Supported relationship types

GoArchive supports **1:1** and **1:N** (one-to-many) relationships.

**Unsupported:** many-to-many (N:M) relationships, and self-referential
"adjacency list" hierarchies — a table referencing its own id to build a tree.
The automatic resolver does not handle either.

#### Shared and many-to-many child rows

GoArchive discovers and deletes child rows **per root**, and deletes a discovered
child with the **first referencing root**. A membership or join row shared
between two roots can therefore be deleted earlier than a second root expects.

Model such relationships explicitly and validate on staging before trusting them.

#### Cross-schema children cannot be modelled

Relation identifiers must match `[A-Za-z0-9_]+`, so `schema.table` is not
expressible. A child table in another schema cannot be added to the graph — which
is why an incoming cross-schema foreign key is always fatal rather than fixable
by configuration.

#### Foreign key `ON DELETE CASCADE`

GoArchive manages deletion order itself, via Kahn's algorithm, to prevent
circular looping. A schema relying heavily on database-level `ON DELETE CASCADE`
may hit conflicts or redundant operations. Cascading constraints among graph
tables are reported as a **warning** during preflight.

GoArchive is best suited to schemas where the **application** controls the
deletion flow.

#### Database triggers

GoArchive has no visibility into logic hidden in MySQL triggers.

- A `DELETE` on a source table that fires a trigger modifying other tables
  produces side effects GoArchive is unaware of and does not verify.
- Destination `INSERT` triggers are **fatal** (`DEST_INSERT_TRIGGER_CHECK`) with
  no override — they would mutate archived data behind the verification hash.
- Source `DELETE` triggers are fatal unless overridden with `--force-triggers`.
  `copy-only` skips this check entirely, since it never deletes from source.

Audit your triggers before running a purge.

---

### Operational cautions

Runtime behaviour to plan around. None of these is a bug; all of them can bite an
unprepared operator.

#### Deep or wide graphs can exhaust memory

BFS discovery accumulates **all descendant primary keys per root batch in
memory**. A deeply nested schema (parent → child → grandchild → great-grandchild,
each 1-N with high fanout) can grow that accumulator without bound.

If your root table has ~1M matching rows and each root has many descendants per
level, start with a small `batch_size` — e.g. `100` — and scale up only after
observing actual memory use.

#### The copy-phase transaction spans all tables

**One** destination transaction covers the entire copy phase. It holds row locks
on already-inserted tables while later tables are still streaming.

Avoid running against a shared destination that other workloads read from
concurrently.

#### Sequential by design

One batch at a time, one job at a time per destination. Advisory locks plus
heartbeat-aware same-root checks prevent concurrent runs of the same job name or
root table. There is no parallelism in Community edition.

#### No built-in metrics or telemetry

Progress is observable only through the structured log output and by querying the
per-job `archiver_job_log_<id>` table directly. Look up the `id` from
`archiver_job` by `job_name`.

#### Advisory lock sessions must stay alive

GoArchive holds the job's `GET_LOCK()` on a dedicated connection and **aborts if
ownership is lost**. MySQL `wait_timeout` must be higher than the longest expected
job duration. A very low timeout or a flaky network can correctly fail a job
rather than let it delete without a lock.

#### `--force` is a best-effort takeover, not hard exclusion

`--force` proceeds past advisory lock contention **only** when the previous
holder's heartbeat is stale, then refreshes the heartbeat so later startups are
blocked.

> A stale heartbeat does **not** prove the old process is dead. It may still own
> MySQL's `GET_LOCK()` and still be deleting. **Operators must verify the old
> process is actually dead before forcing.**

It cannot bypass a live heartbeating job, the same-root concurrency check, or
preflight.

#### Partial auto-commit deletes are expected after interruption

Deletes are intentionally committed in batches, to avoid long source locks. If a
run stops between child and parent deletes, the source can temporarily have
children removed while the parent remains.

This is **not data loss** — the rows were copied and verified first — and resume
completes the remaining work. But monitoring that asserts referential integrity
mid-run will see the gap.

#### Verification method controls dirty-destination behaviour

- **`verification.method: count`** — archive uses plain `INSERT` and **aborts on
  any pre-existing destination row with the same key**, before deleting source
  data.
- **`verification.method: sha256`** — archive uses `INSERT IGNORE` and verifies
  destination content by hash. This is the **recommended recovery mode** for
  interrupted jobs with pending PKs.

The method also determines whether an interrupted job can auto-resume at all —
see [Resume semantics](README_OPERATIONS.md#resume-semantics).

#### Schema-stable assumption

GoArchive assumes source and destination schemas do **not change during a batch
loop**. Run migrations either before or after archive jobs, never concurrently.

#### Runtime preflight is automatic — and skippable

`archive`, `purge`, and `copy-only` run preflight at startup, before any
`archiver_job` state is written. `validate` remains useful for inspecting issues
ahead of an operational run.

Use `--skip-validate-preflight` **only** for documented recovery scenarios, after
manually verifying schema safety.

#### Data loss on misconfiguration is possible

A job that passes validation but carries the wrong `where` clause will delete the
wrong rows from source. Validation checks structure, not intent.

**Always run `goarchive dry-run` and review the estimated row counts before
executing `archive`.** Keep valid backups.

---

## Trust model

GoArchive intentionally treats configuration files as **operator-controlled and
trusted** input.

- Job `where` values are **raw SQL fragments** injected into archive selection
  queries.
- Connections use `multiStatements=true` for operational compatibility.
- **Do not expose config editing to untrusted users or untrusted automation
  pipelines.**

Table and column identifiers are the exception — they are validated against
`[A-Za-z0-9_]+` and quoted. The `where` clause is not, by design.

---

## Environment

- **MySQL**: 8.0+ with the **InnoDB** storage engine
- **Go**: 1.21 or later to build from source
- **Network**: access to source, destination, and optionally replica databases

MySQL 5.7 and earlier are not supported.

---

## What's included in Community

For the positive counterpart to this page — the complete list of what Community
edition does provide, and what is deferred to Enterprise — see
[Project Status in the README](../README.md#whats-included-in-community).
