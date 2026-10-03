# Limitations & Constraints

What GoArchive Community edition cannot do, what it refuses to do, and what to
plan around before pointing it at real data.

**Related:** [Configuration](README_CONFIGURATION.md) ·
[Validation & Preflight](README_VALIDATION.md) ·
[Permissions](README_PERMISSIONS.md) · [Operations](README_OPERATIONS.md) ·
[← Back to README](../README.md)

---

> [!WARNING]
> This tool performs data deletion on your source database. **Rigorously test every archive
> job in a staging environment with representative data before running it against
> production.**

The Community edition is recommended for **single-operator workstation archival
of cold data**. It is not designed for hot, actively-transacting tables.

---

## Contents

- [Known Limits & Caution](#known-limits--caution)
  - [Hard constraints](#hard-constraints--rejected-by-preflight)
  - [Model limitations](#model-limitations)
  - [Operational cautions](#operational-cautions)
- [Trust model](#trust-model)
- [Environment](#environment)
- [What's included in Community](#whats-included-in-community)

---

## Known Limits & Caution

> This is the section GoArchive's preflight error messages refer to.

<a id="hard-constraints--rejected-by-preflight"></a>

### Hard constraints

These are not warnings. Structural constraints fail preflight before a run starts;
stored temporal identities are checked when encountered and refused before they
can drive copying, discovery, verification, or deletion.

#### Single-column primary keys only; root primary keys must be integer

GoArchive identifies, copies, verifies, and **deletes** rows by a single
primary-key column (`WHERE pk IN (...)`). Each check below explains its reason and fix in
[Primary key checks](README_VALIDATION.md#primary-key-checks).

- **Composite (multi-column) primary keys are not supported** on any
  participating table (`COMPOSITE_PK_CHECK`).
- Every participating table must have a **single-column `PRIMARY KEY` equal to
  its configured `primary_key`** (`PRIMARY_KEY_CHECK`).
- **Root tables must additionally use an integer primary key** — `TINYINT`
  through `BIGINT`, signed or unsigned (`ROOT_PK_TYPE_UNSUPPORTED`).
- **Child tables may use single-column primary keys**, subject to the
  [temporal identity restriction below](#temporal-values-and-identities).

If your schema uses composite keys on the tables you want to archive, Community
edition cannot safely archive them.

#### Temporal values and identities

GoArchive refuses DATE, DATETIME and TIMESTAMP identities containing an invalid
Gregorian date, zero year/month/day, invalid clock or unsupported text shape.
DATE keys require `YYYY-MM-DD`, year 0001–9999 and a real calendar day. DATETIME and
TIMESTAMP keys additionally require ` HH:MM:SS`, optionally 1–6 fractional digits;
no timezone suffix, whitespace normalization or leap second is accepted. Valid
keys retain their exact SQL spelling. Root keys remain integer-only.

This restriction concerns identities, not ordinary payloads, which are copied as SQL text and
never normalized ([INSERT diagnostics](README_OPERATIONS.md#insert-diagnostics-and-subdivision)).
Copy and enabled verification require each fetched temporal identity set to equal the distinct
requested set, including the empty-result case. Archive and purge run the pre-delete proof
sequence described in the same section.

Missing metadata or unfaithful representations fail with
`TEMPORAL_READ_CONTRACT`. Neither verification nor preflight overrides bypass
these identity rules, regardless of server modes. The existing cold-data/no-DDL
contract still applies; the checks add no locking or concurrent-write protection.

#### No DDL and no concurrent writes during a run (contract)

GoArchive's correctness during a run rests on three operating contracts:

1. **No DDL on participating tables while a job runs.** Column lists are read
   once at startup. A column renamed or dropped mid-run fails the next batch
   with a MySQL "unknown column" error before that batch reaches deletion;
   batches completed earlier remain deleted. A column **added** mid-run is
   *not detected*: it is not copied, and DDL alone can diverge source and
   destination (an add on one side only, or adds with differing defaults) —
   with no error raised. Run migrations before or after archive jobs, never during one.
2. **GoArchive archives cold data.** Concurrent transactions on the archived
   row range are unsupported, including column additions and column-default
   changes taking effect mid-run.
3. **Eligibility only grows, in primary-key order.** A job continues from the last primary key it processed and never
   looks below it, on this run or later ones. So the rows your `where` selects must be cold: once a row matches,
   nothing writes to it, and no row below the checkpoint may start matching later. A predicate such as
   `created_at < NOW() - INTERVAL 1 YEAR` meets this only when `created_at` is set at insert, never changes, and grows
   with the primary key, which rules out backfills, imports with low IDs and rewritten timestamps. A predicate on a
   column that changes, such as `status = 'closed'`, does not: a row that closes after the checkpoint has passed it is
   never archived by that job. To pick such rows up, retire the job and create it again
   ([Retiring a job completely](README_JOBS_SCHEMA.md#retiring-a-job-completely)).

#### Source and destination must be different databases

`SRC_DEST_IDENTITY_CHECK` refuses, at connection time, a source and destination that report
the same `server_uuid` **and** the same schema name
([the check](README_VALIDATION.md#connection-identity-check)). Same server, different schema
is a supported layout. Two conservative refusals are deliberate:

- **Schema names are compared without regard to letter case.** MySQL looks
  schema names up case-insensitively under `lower_case_table_names` 1 and 2
  (its Windows and macOS defaults), where `App` and `app` are one schema.
  GoArchive folds case on every server, so on a case-sensitive server (the Unix
  default, `lower_case_table_names=0`) `App` and `app` are refused as one schema
  even though MySQL keeps them apart. The message prints both spellings. Rename
  one, or use a different destination.
- **Two servers sharing a `server_uuid` are one server to GoArchive.** MySQL
  reads the UUID from `auto.cnf` in the data directory and generates it only
  when that file is absent, so an instance cloned from another's data directory
  carries the same UUID. On the standalone archive server: stop mysqld, remove
  `auto.cnf` from the data directory, start it again, and it writes a fresh
  `server_uuid`. A server that takes part in replication follows MySQL's own
  guidance for changing `server_uuid` instead.

The check assumes each connection reaches one backend for the whole run — the
same assumption verification and the advisory job lock already make. A proxy
that routes one account's statements to different servers is outside it.

#### InnoDB only

Every participating **source** table must be InnoDB (`STORAGE_ENGINE_CHECK`). The destination
engine is not checked and is the operator's choice, but MyISAM, MEMORY and ARCHIVE keep rows
after `ROLLBACK`, voiding the per-batch rollback and `dry-run`'s rolled-back sample, and
BLACKHOLE stores nothing, so verification fails unless it is skipped
([the check](README_VALIDATION.md#storage_engine_check)).

#### Uncovered incoming foreign keys

Any table **outside** the graph holding a foreign key **into** it is fatal, for
every `ON DELETE` rule (`FK_COVERAGE_CHECK`). A table in another schema can never be added to
the graph ([identifier rules](README_CONFIGURATION.md#identifier-rules)), so an incoming
cross-schema foreign key is always fatal rather than fixable by configuration. Proving that no
such key was missed needs `PROCESS` on the source account
([the provable-grant rule](README_PERMISSIONS.md#the-provable-grant-rule)).

#### Destination schema must not be stricter than source

The destination may drop secondary indexes, `auto_increment`, and defaults, and
may relax `NOT NULL`. It must not add constraints the source lacks. See the full
matrix in
[Validation](README_VALIDATION.md#schema-compatibility-rules).

---

### Model limitations

Things the dependency model cannot express, which preflight cannot always catch
for you.

#### Supported relationship types

GoArchive supports **1:1** and **1:N** (one-to-many) relationships.

- **Many-to-many (N:M) cannot be expressed.** A table appears once in the graph, so a join
  table with two in-graph parents can carry only one relation; its second foreign key fails
  `INTERNAL_FK_COVERAGE`. A foreign key spanning several columns between graph tables is
  rejected the same way.
- **Self-referential "adjacency list" hierarchies** — a table referencing its own id to build
  a tree — pass preflight, because no check rejects them, but the model cannot archive the
  hierarchy.

#### Relations must describe ownership

Archive and purge delete the child primary keys discovered for the current root batch; they do
not check whether another root still needs them. If one child is reachable from several roots
through its one configured relation, deduplication within a batch does not postpone its
deletion.

#### A job name is not bound to its source

A job name is bound to its command and root table
([`archiver_job`](README_JOBS_SCHEMA.md#root_table-is-sticky)), but not to the source server
and schema: the same job name pointed at a different source that has a table of the same name
is not detected. Use a new job name whenever the source changes.

#### Foreign key `ON DELETE CASCADE`

GoArchive manages deletion order itself, via Kahn's algorithm, to prevent
circular looping. A schema relying heavily on database-level `ON DELETE CASCADE`
may hit conflicts or redundant operations; preflight warns about cascades among graph tables
([Warnings](README_VALIDATION.md#warnings)).

GoArchive is best suited to schemas where the **application** controls the
deletion flow.

#### Database triggers

GoArchive has no visibility into logic hidden in MySQL triggers. A `DELETE` on a source table
that fires a trigger modifying other tables produces side effects GoArchive does not verify.
Preflight refuses destination `INSERT` triggers and, unless overridden, source `DELETE`
triggers ([Trigger checks](README_VALIDATION.md#trigger-checks)). Audit your triggers before
running a purge.

---

### Operational cautions

Runtime behaviour to plan around. None of these is a bug; all of them can bite an
unprepared operator.

#### Deep or wide graphs can exhaust memory

BFS discovery accumulates **all descendant primary keys per root batch in
memory**. A deeply nested schema (parent → child → grandchild → great-grandchild,
each 1-N with high fanout) can grow that accumulator without bound.

If your root table has ~1M matching rows and each root has many descendants per
level, start with a small `batch_size` and scale up only after observing actual
memory use. Sizing guidance lives in
[Tuning throughput](README_OPERATIONS.md#tuning-throughput).

#### The copy-phase transaction spans all tables

**One** destination transaction covers the entire copy phase. It holds row locks
on already-inserted tables while later tables are still streaming.

Avoid running against a shared destination that other workloads read from
concurrently.

#### Sequential by design

One batch at a time within a run; **there is no parallelism in Community edition.** A second
run of the same job name, or of any job on the same root table, is refused — except that
`copy-only --force` proceeds past a held lock whose heartbeat is stale
([`--force`](README_OPERATIONS.md#--force-only-matters-for-copy-only)). Jobs with different names
and root tables can run at the same time
([Concurrency and locking](README_OPERATIONS.md#concurrency-and-locking)).

#### No built-in metrics or telemetry

Run-level progress is available through the opt-in
[`--progress` display](README_OPERATIONS.md#progress-display). Structured logs and the
[tracking tables](README_JOBS_SCHEMA.md) remain the durable sources for batch activity and
recovery state.

#### Locks and `--force`

The job's advisory lock lives on a dedicated connection; losing it aborts the job, and
`--force` never takes over a held lock for `archive` or `purge`. Requirements, including
`wait_timeout`: [Concurrency and locking](README_OPERATIONS.md#concurrency-and-locking).

#### Partial auto-commit deletes are expected after interruption

Deletes are intentionally committed in batches, to avoid long source locks. If a
run stops between child and parent deletes, the source can temporarily have
children removed while the parent remains.

This is **not data loss** — the rows were copied and verified first — and resume
completes the remaining work. But monitoring that asserts referential integrity
mid-run will see the gap.

#### Verification method controls dirty-destination behaviour

The choice of `verification.method` decides the INSERT strategy, whether a pre-existing
destination row aborts the run, and whether an interrupted job can auto-resume — see
[`verification:`](README_CONFIGURATION.md#verification) and
[Resume semantics](README_OPERATIONS.md#resume-semantics).

#### AUTO_INCREMENT zero values and dry-run allocation

Explicit zero values are preserved on every connection
([`AUTO_INCREMENT_ZERO_MODE_CHECK`](README_VALIDATION.md#error-prefixes-that-are-not-checks)).
Rolling back a dry-run sample does not promise to restore AUTO_INCREMENT counters or eliminate
allocation gaps.

#### Data loss on misconfiguration is possible

Validation checks structure, not intent: a job with the wrong `where` deletes the wrong rows.
Review `goarchive dry-run`'s estimated row counts before running `archive`, and keep valid
backups.

---

## Trust model

GoArchive defends against the operator's **mistakes**, not against the operator. The
configuration file is trusted input: whoever writes it already holds the credentials it names.

- Job `where` values are **free SQL predicates by design**
  ([`where` rules](README_CONFIGURATION.md#where-is-mandatory)). Only table and column
  identifiers are validated ([identifier rules](README_CONFIGURATION.md#identifier-rules)).
- When the person writing the configuration is not the credential holder, the boundary is the
  privileges of the accounts it names: grant only what the
  [privilege matrix](README_PERMISSIONS.md#privilege-matrix) lists.
- A behaviour that only a deliberate operator could exploit is not a weakness.

**Do not expose config editing to untrusted users or untrusted automation pipelines.**

---

## Environment

This section is the single source of truth for supported versions. `README.md` and `INSTALL.md`
link here rather than restating it.

- **MySQL**: Oracle MySQL **8.0.40+** with the **InnoDB** storage engine, and Percona Server for
  MySQL as tested by [dbsgomysql](https://github.com/dbsmedya/dbsgomysql), which supplies every
  preflight fact ([Why GoArchive uses dbsgomysql](README_dbsgomysql.md))
- **Go**: 1.26 or later to build from source — `go.mod` sets `go 1.26.0` and pins
  `toolchain go1.26.8`, and the project's own builds (gate, CI, release binaries, Docker image)
  all use go1.26.8. An older Go with the default `GOTOOLCHAIN=auto` downloads the pinned release
  instead of building with itself
- **Network**: access to the source and destination databases, and to every replica listed
  in `replication.servers` when the replication gate is enabled

MySQL 5.7 and earlier are not supported, and neither is any 8.0 release below **8.0.40**.
Nothing in GoArchive enforces the floor: an older server fails at whichever check
first meets behaviour the library does not model, rather than being refused up front.

---

## What's included in Community

For the positive counterpart to this page — the complete list of what Community
edition does provide, and what is deferred to Enterprise — see
[Project Status in the README](../README.md#whats-included-in-community).
