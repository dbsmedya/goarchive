# Limitations & Constraints

What GoArchive cannot do, and what to plan around before pointing it at real data.

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
  - [Hard constraints](#hard-constraints)
  - [Model limitations](#model-limitations)
  - [Operational cautions](#operational-cautions)
- [Trust model](#trust-model)
- [Environment](#environment)

---

## Known Limits & Caution

> This is the section GoArchive's error messages refer to.

<a id="hard-constraints--rejected-by-preflight"></a>

### Hard constraints

Before a run, GoArchive cannot verify the [run contract](#no-ddl-and-no-concurrent-writes-during-a-run-contract)
or the content of JSON values.

#### Primary keys

Every participating table needs a `PRIMARY KEY`. A key cannot use a `FLOAT`, `DOUBLE`, `ENUM`,
`SET` or `BIT` column, any other type outside the key types in
[Primary key checks](README_VALIDATION.md#primary-key-checks), or a prefix key part. The root
table's key definition cannot change after the job is created.

#### Temporal values and identities

Key values in `DATE`, `DATETIME` and `TIMESTAMP` columns cannot be zero dates, invalid clocks,
timezone suffixes or leap seconds. This holds under every server mode, and no override bypasses
it. Non-key temporal values are not normalized.

#### No DDL and no concurrent writes during a run (contract)

GoArchive's correctness rests on these operating contracts. Violations are not detected, except
where a batch or [recovery](README_OPERATIONS.md#crash-recovery) stops on one:

1. **No DDL on participating tables while a job runs.** An **added** column is not detected and
   not copied. Run migrations between runs.
2. **Cold data only.** The rows of a root's tree must not change from the moment GoArchive
   reads them until it has finished that root, including across a crash and the recovery that
   follows. Recovery finishes a logged root whose source rows were deleted from outside
   without being able to tell.
3. **Changed rows are not re-copied.** A source row updated after GoArchive copied it is not
   re-copied, detected or reported.

#### Source and destination must be different databases

The source and destination cannot be the same database: the same `server_uuid` **and** the same
schema name.

- Schema names are compared case-insensitively on every server, so `App` and `app` are one schema
  even where MySQL keeps them apart.
- Two servers sharing a `server_uuid` (a clone of another's data directory) are one server to
  GoArchive. [Remedy](README_VALIDATION.md#src_dest_identity_check)
- Each connection must reach one backend for the whole run; a proxy that routes one account's
  statements to different servers is unsupported.

#### InnoDB only

Every participating source and destination table must be InnoDB.

#### Uncovered incoming foreign keys

Any table **outside** the job holding a foreign key **into** it is fatal, for every `ON DELETE`
rule. A table in another schema cannot join a job
([identifier rules](README_CONFIGURATION.md#identifier-rules)), so an incoming cross-schema foreign
key is always fatal. For archive and purge, proving that none was missed needs `PROCESS` on the
source account ([the provable-grant rule](README_PERMISSIONS.md#the-provable-grant-rule)).

#### Destination schema must not be stricter than source

The destination must not add constraints the source lacks
([the matrix](README_VALIDATION.md#schema-compatibility-rules)).

#### Table names under `lower_case_table_names=1`

On a server that stores names in lower case, a job's table names must be lower case.

#### Character sets

A job cannot read or write a text column in a character set outside the lists in
[Character sets](README_CHARSETS.md).

#### JSON values

JSON values do not survive the copy: a member's MySQL type is not kept, a DECIMAL member longer
than a double loses digits, and an 8.0 or 8.4 destination can change a double as it parses the
text (MySQL Bug #116160). `count` and `none` do not compare JSON values. What changes, and on
which versions:
[JSON values that do not survive a text copy](README_COMPAT.md#json-values-that-do-not-survive-a-text-copy).

---

### Model limitations

Things the relation model cannot express, which GoArchive cannot always detect.

#### Supported relationship types

**1:1** and **1:N** only.

- **Many-to-many (N:M) cannot be expressed.** A table appears once in a job, so a join table
  with two in-job parents carries only one relation.
- **Self-referencing hierarchies** (a table referencing its own key) cannot be archived.

#### Relations must describe ownership

Archive and purge delete the child keys discovered for the current root batch without checking
whether another root still needs them.

#### Foreign key `ON DELETE CASCADE`

A cascade between two job tables is redundant work that GoArchive does not account for.

#### Database triggers

GoArchive cannot see logic in triggers: a source `DELETE` trigger that changes other tables has
side effects GoArchive does not verify.

---

### Operational cautions

Runtime behaviour to plan around.

#### Deep or wide graphs can exhaust memory

Discovery holds every descendant key of a root batch in memory. A deep tree with high fanout at
each level can grow it without bound: start with a small `batch_size` and raise it after
watching memory ([Tuning throughput](README_OPERATIONS.md#tuning-throughput)).

#### A value larger than the destination's `max_allowed_packet` fails its batch

One value larger than the destination's `max_allowed_packet` cannot be sent, so its batch fails;
raise `max_allowed_packet` above it. An INSERT whose values together exceed the destination's
`max_allowed_packet` can be refused whole, though each value fits: lower `batch_size`.

#### The copy transaction spans all tables

One destination transaction copies, verifies and logs a batch across every table, holding row
locks on tables already inserted while later ones stream. Avoid a destination that other
workloads read concurrently.

#### Sequential by design

One batch at a time, no parallelism. Runs whose job trees share a source table cannot run at the
same time.

A stopped job with unfinished work blocks every other job that shares one of its tables until
its own rerun finishes that work. Its limits:

- **Same-named databases on different servers block each other** when they share one
  `job_schema` and the source cannot tell them apart: `gtid_mode` is not `ON`, the source is a
  Group Replication member, or the shards were provisioned from one another. Give each source
  server its own [`job_schema`](README_CONFIGURATION.md#job_schema-destination-only).
- **Not detected:** a server that received the stopped job's data only before a GTID
  migration, and a server outside a group that holds the group's data.
  [Re-bind the stopped job](README_JOBS_SCHEMA.md#re-binding-a-job-after-a-failover) before
  another job runs on its tables.
- **Not detected:** a stopped job recorded on a replica of this source.
- **One registry:** a stopped job tracked in another `job_schema` does not block.

The locks live on one server. Every connection must reach one backend for the whole run (see
[Source and destination must be different databases](#source-and-destination-must-be-different-databases)):
a read/write-splitting proxy could send the locks to a replica while the deletes go to the
primary. A copy-only job reading a replica and an archive writing its primary are not
serialized; the [run contract](#no-ddl-and-no-concurrent-writes-during-a-run-contract) covers
that case.

#### Partial deletes after an interruption

A run stopped between child and parent deletes leaves children removed while the parent remains,
until the next start's recovery finishes the root. Monitoring that asserts referential integrity
mid-run will see the gap.

#### Copy-only reruns rescan the whole range

There is no checkpoint: a copy-only rerun with `resume: true` reads the whole `where` range
again, which on a very large range costs a full scan per run.

#### Data loss on misconfiguration is possible

Validation cannot judge intent: a job with the wrong `where` deletes the wrong rows. Check the
`where` against the data and `goarchive dry-run`'s first batch before running `archive`, and keep
valid backups.

---

## Trust model

GoArchive does not defend against the operator. The configuration file is trusted input: whoever
writes it already holds the credentials it names.

- Job `where` values are **free SQL predicates by design**, and GoArchive does not validate them.
  Only table and column identifiers are validated
  ([identifier rules](README_CONFIGURATION.md#identifier-rules)).
- When the person writing the configuration is not the credential holder, the boundary is the
  privileges of the accounts it names: grant only what the
  [privilege matrix](README_PERMISSIONS.md#privilege-matrix) lists.
- Once a batch's source rows are deleted, the archived rows exist only on the destination; keeping
  them safe (durability, replication, backups) is not GoArchive's concern.

**Do not expose config editing to untrusted users or untrusted automation pipelines.**

---

## Environment

Servers other than Oracle MySQL and Percona Server (MariaDB included) are not supported, and
neither are tables in a storage engine other than InnoDB.

MySQL 5.7 and earlier are not supported, and neither is any 8.0 release below **8.0.40**. Nothing
enforces the floor: an older server can fail at any point where it meets behaviour the library
does not model.
