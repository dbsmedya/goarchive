# Limitations & Constraints

What GoArchive cannot do, what it refuses to do, and what to plan around before pointing it at
real data. What changed from 2.x: [Upgrading to 3.0](README_UPGRADING_3_0.md).

**Related:** [Configuration](README_CONFIGURATION.md) ·
[Validation & Preflight](README_VALIDATION.md) ·
[Permissions](README_PERMISSIONS.md) · [Operations](README_OPERATIONS.md) ·
[← Back to README](../README.md)

---

> [!WARNING]
> This tool performs data deletion on your source database. **Rigorously test every archive
> job in a staging environment with representative data before running it against
> production.**

GoArchive is for **cold data**. It is not designed for hot, actively-transacting tables.

---

## Contents

- [Known Limits & Caution](#known-limits--caution)
  - [Hard constraints](#hard-constraints--rejected-by-preflight)
  - [Model limitations](#model-limitations)
  - [Operational cautions](#operational-cautions)
- [Trust model](#trust-model)
- [Environment](#environment)

---

## Known Limits & Caution

> This is the section GoArchive's preflight error messages refer to.

<a id="hard-constraints--rejected-by-preflight"></a>

### Hard constraints

Preflight refuses these before a run starts; stored temporal identities are refused when
encountered, before they drive any copy, discovery, verification or delete.

#### Primary keys

Every participating table needs a `PRIMARY KEY`, single-column or composite, and its configured
`primary_key` must name exactly those columns in key order
([Primary key checks](README_VALIDATION.md#primary-key-checks)). Refused as keys
(`KEY_TYPE_UNSUPPORTED`): `FLOAT`/`DOUBLE`, `ENUM`/`SET`, `BIT`, and a key part with a prefix
length. The root table's key definition must stay the one the job was created with; a changed
one is refused at startup.

#### Temporal values and identities

`DATE`, `DATETIME` and `TIMESTAMP` key values must be valid: `YYYY-MM-DD` with year 0001–9999
and a real calendar day, plus ` HH:MM:SS` and 0–6 fractional digits for `DATETIME` and
`TIMESTAMP`. Zero dates, invalid clocks, timezone suffixes and leap seconds are refused with
`TEMPORAL_READ_CONTRACT`, whatever the server's modes; no override bypasses it. Ordinary
(non-key) temporal values are copied as SQL text and never normalized
([INSERT diagnostics](README_OPERATIONS.md#insert-diagnostics-and-subdivision)).

#### No DDL and no concurrent writes during a run (contract)

GoArchive's correctness rests on these operating contracts. Violations are documented, not
detected:

1. **No DDL on participating tables while a job runs.** Column lists are read once at startup.
   A renamed or dropped column fails the next batch before it reaches the delete; an
   **added** column is not detected and not copied. Run migrations between runs.
2. **Cold data only.** The rows of a root's tree must not change from the moment GoArchive
   reads them until it has finished that root, including across a crash and the recovery that
   follows. Recovery finishes a logged root whose source rows were deleted from outside
   without being able to tell.
3. **Changed rows are not re-copied.** A source row updated after GoArchive copied it is not
   re-copied, detected or reported.

#### Source and destination must be different databases

`SRC_DEST_IDENTITY_CHECK` refuses a source and destination with the same `server_uuid` **and**
the same schema name ([the check](README_VALIDATION.md#src_dest_identity_check)). Same server,
different schema is supported.

- Schema names are compared case-insensitively on every server, so `App` and `app` are refused
  as one schema even where MySQL keeps them apart.
- Two servers sharing a `server_uuid` (a clone of another's data directory) are one server to
  GoArchive. On a standalone archive server, stop mysqld, remove `auto.cnf`, and start it
  again; a replicating server follows MySQL's guidance for changing `server_uuid`.
- Each connection must reach one backend for the whole run; a proxy that routes one account's
  statements to different servers is unsupported.

#### InnoDB only

Every participating **source** table must be InnoDB (`STORAGE_ENGINE_CHECK`). The destination
engine is not checked, but MyISAM, MEMORY and ARCHIVE keep rows after `ROLLBACK`, voiding the
per-batch rollback, and BLACKHOLE stores nothing, so verification fails unless it is `none`
([the check](README_VALIDATION.md#storage_engine_check)).

#### Uncovered incoming foreign keys

Any table **outside** the job holding a foreign key **into** it is fatal, for every `ON DELETE`
rule (`FK_COVERAGE_CHECK`). A table in another schema cannot join a job
([identifier rules](README_CONFIGURATION.md#identifier-rules)), so an incoming cross-schema
foreign key is always fatal. Proving that none was missed needs `PROCESS` on the source account
([the provable-grant rule](README_PERMISSIONS.md#the-provable-grant-rule)).

#### Destination schema must not be stricter than source

The destination may drop secondary indexes, `auto_increment` and defaults, and may relax
`NOT NULL`; it must not add constraints the source lacks
([the matrix](README_VALIDATION.md#schema-compatibility-rules)).

#### Table names under `lower_case_table_names=1`

On a server that stores names in lower case, write every table name in the job in lower case.
A mixed-case name is refused with `TABLE_EXISTENCE_CHECK`.

---

### Model limitations

Things the relation model cannot express, which preflight cannot always catch.

#### Supported relationship types

**1:1** and **1:N** only.

- **Many-to-many (N:M) cannot be expressed.** A table appears once in a job, so a join table
  with two in-job parents carries only one relation; its second foreign key fails
  `INTERNAL_FK_COVERAGE`.
- **Self-referencing hierarchies** (a table referencing its own key) pass preflight, which skips
  self-referencing constraints, but the model cannot archive the hierarchy.

#### Relations must describe ownership

Archive and purge delete the child keys discovered for the current root batch without checking
whether another root still needs them.

#### Foreign key `ON DELETE CASCADE`

GoArchive deletes children before their parents itself. A cascade among the job's tables is
redundant work that GoArchive does not account for; preflight warns about it
([Warnings](README_VALIDATION.md#warnings)).

#### Database triggers

GoArchive cannot see logic in triggers: a source `DELETE` trigger that changes other tables has
side effects GoArchive does not verify. Preflight refuses destination `INSERT` triggers and,
unless overridden, source `DELETE` triggers
([Trigger checks](README_VALIDATION.md#trigger-checks)).

---

### Operational cautions

Runtime behaviour to plan around.

#### Deep or wide graphs can exhaust memory

Discovery holds every descendant key of a root batch in memory. A deep tree with high fanout at
each level can grow it without bound: start with a small `batch_size` and raise it after
watching memory ([Tuning throughput](README_OPERATIONS.md#tuning-throughput)).

#### A row set larger than `max_allowed_packet` fails its batch

[`MAX_ALLOWED_PACKET_CHECK`](README_VALIDATION.md#max_allowed_packet_check) bounds every
key-addressed statement, but not the INSERTs, whose size is the rows' own. A wide `BLOB` or
`TEXT` row set can fail with error 1153: the batch's destination transaction rolls back and
nothing is deleted. Raise `max_allowed_packet` or lower `batch_size`, then rerun.

#### The copy transaction spans all tables

One destination transaction copies, verifies and logs a batch across every table, holding row
locks on tables already inserted while later ones stream. Avoid a destination that other
workloads read concurrently.

#### Sequential by design

One batch at a time, no parallelism. A second run of the same job, or of any job on the same
root table, is refused; jobs with different names and root tables can run together
([Concurrency and locking](README_OPERATIONS.md#concurrency-and-locking)).

#### The job lock and long replication holds

The job's advisory lock lives on a dedicated connection, and losing it stops the job. Every
session's `wait_timeout` is raised to at least 8 hours, so a replication-lag hold longer than
that fails the next delete chunk (error 4031); the next start's recovery finishes the batch.

#### Partial deletes after an interruption

Delete chunks commit separately, to avoid long source locks. A run stopped between child and
parent deletes leaves children removed while the parent remains, until the next start's
recovery finishes the root. The rows were copied and verified first, but monitoring that
asserts referential integrity mid-run will see the gap.

#### Copy-only reruns

With `resume: false` (the default) a rerun fails on the first root already in the destination.
With `resume: true` it skips the roots the job copied before, but every run rescans the whole
`where` range.

#### No built-in metrics

There is no metrics endpoint. Use the [`--progress` display](README_OPERATIONS.md#progress-display),
the structured logs and the [tracking tables](README_JOBS_SCHEMA.md).

#### Data loss on misconfiguration is possible

Validation checks structure, not intent: a job with the wrong `where` deletes the wrong rows.
Check the `where` against the data and `goarchive dry-run`'s first batch before running
`archive`, and keep valid backups.

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

**Do not expose config editing to untrusted users or untrusted automation pipelines.**

---

## Environment

This section is the single source of truth for supported versions. `README.md` and `INSTALL.md`
link here rather than restating it.

- **MySQL**: Oracle MySQL **8.0.40+** with the **InnoDB** storage engine, and Percona Server for
  MySQL as tested by [dbsgomysql](https://github.com/dbsmedya/dbsgomysql), which supplies every
  preflight fact ([Why GoArchive uses dbsgomysql](README_dbsgomysql.md))
- **Go**: 1.26 or later to build from source. `go.mod` pins `toolchain go1.26.8`, which the
  project's own builds use; an older Go with `GOTOOLCHAIN=auto` downloads it
- **Network**: access to the source and destination databases, and to every replica listed
  in `replication.servers` when the replication gate is enabled

MySQL 5.7 and earlier are not supported, and neither is any 8.0 release below **8.0.40**.
Nothing enforces the floor: an older server fails at whichever check first meets behaviour the
library does not model.
