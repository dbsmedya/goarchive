# Why GoArchive uses dbsgomysql

GoArchive's preflight validation is built on
[dbsgomysql](https://github.com/dbsmedya/dbsgomysql), a standalone Go library for reading
and comparing MySQL schema, privilege, and metadata facts.

This page explains **why that dependency exists and what it provides**. It is background,
not a task.

## The problem it solves

Every GoArchive preflight check answers a question about MySQL metadata — does this table
exist, is it InnoDB, is this column the primary key, can this account prove it holds
`DELETE` here, does a foreign key point into the archive graph from outside it. Reading
MySQL metadata *correctly* is a specialist problem with sharp edges that have nothing to do
with archiving:

- `information_schema.TABLES.TABLE_NAME` collates case-**sensitively** while
  `COLUMNS.COLUMN_NAME` collates case-**insensitively** — two columns in the same database,
  two different behaviours.
- A privilege held through an active role, or granted globally while
  `@@global.partial_revokes` is enabled, is not provable for a specific object from
  `information_schema` alone.
- InnoDB's own foreign-key registry is complete but normally requires `PROCESS` (MySQL 8.0.40
  serves it without; versions between 8.0.40 and 8.0.46 were not probed:
  [compatibility notes](README_COMPAT.md#foreign-key-metadata-readable-without-process-on-mysql-8040));
  `information_schema` is always readable but only shows constraints on tables the account
  is already privileged for — so a fallback silently answers a weaker question.
- Integer display widths (`bigint(20)`) are cosmetic and not reported by MySQL 8.0.17 and
  later; `tinyint(1)` is the exception — the library reports it as a difference, and
  GoArchive's policy accepts it.

Each of these was a latent false-pass in a data-deleting tool. Extracting them into a
library means one place owns MySQL correctness, and it is versioned, documented, and
tested against several MySQL releases instead of being rediscovered per check.

## What the library provides

| Package | GoArchive uses it for |
|---|---|
| `pkg/validations` | Schema, privilege, and metadata facts (tables, columns, primary keys, triggers and the proof that a trigger list is complete, invisible columns, foreign keys, grants), plus the comparison primitives behind destination schema compatibility. Findings are typed values, not formatted strings — GoArchive decides what is fatal. |
| `pkg/sqlutil` | Identifier quoting and validation. |
| `pkg/replication` | Replica status facts, read by the [replication gate](README_OPERATIONS.md#replication-gating); the source's GTID mode and executed set and its Group Replication state, read by the [unfinished-work guard](README_OPERATIONS.md#concurrency-and-locking); and the [Group Replication facts](README_OPERATIONS.md#group-replication) preflight reads first: each server's plugin state, group name and transaction size limit, the source's `read_only` settings, and a member source's view of its group. |

The library also ships a registry of documented MySQL version quirks and records where
behaviour diverges across the 8.0, 8.4 and 9.7 lines; supported versions are in
[Environment](README_LIMITATIONS.md#environment).

## The division of labour

The split is deliberate and it is the reason the integration is safe:

- **The library reports facts.** It does not know what GoArchive intends to do with a
  table, and it never decides that a schema difference is fatal.
- **GoArchive owns policy.** Whether a destination may be looser than its source, which
  key types can identify rows, whether a difference blocks a copy —
  all of that stays here, in GoArchive, where the consequences are understood (the rules:
  [Schema compatibility](README_VALIDATION.md#schema-compatibility-rules)).
- **GoArchive owns its own session.** Properties of the connection GoArchive itself
  opened — the UTC session it pins, the server identity it refuses to archive into
  itself — are read by GoArchive-owned probes, listed under *Retained
  application-owned MySQL probes* in [README_TESTING.md](README_TESTING.md#retained-application-owned-mysql-probes). They
  inspect what the session reports, not the metadata catalog the library owns.

## The consumer boundary

GoArchive issues **no `information_schema` queries of its own**. That is enforced, not
just intended: `make consumer-policy` walks every non-test Go file in the module and fails
the build on any string literal naming `information_schema`.

Preflight facts are read **once per run** and shared by every check that needs them, so
adding a check does not add a round trip. Grants are read on a dedicated connection,
together with the trigger list they prove when a trigger check needs both, and the
connection is released before any other query runs; this keeps the answer accurate on a
pool and avoids self-deadlocking a single-connection configuration.

## Group Replication facts

Preflight reads them through the library once, in its first two stages, and returns them in its result,
so `validate`, `dry-run` and a run share one read ([what is read, and in which order](README_OPERATIONS.md#group-replication)):

| Fact | Used for |
|---|---|
| `ReplicationConfig` | Each side's `read_only`, `super_read_only` and `server_id`: the first statement sent to that server, judged by [`SOURCE_READ_ONLY_CHECK`](README_VALIDATION.md#source_read_only_check) and [`DEST_READ_ONLY_CHECK`](README_VALIDATION.md#dest_read_only_check) |
| `GroupReplication` | Each side's plugin state, configured group name, `group_replication_transaction_size_limit` and `group_replication_single_primary_mode` (each with whether the server reported it); the mode is judged by [`GROUP_MULTI_PRIMARY_CHECK`](README_VALIDATION.md#group_multi_primary_check) |
| `GroupMembers` | A member source's view: each member's state and role, and which row is the source's own. Needs `SELECT` on `performance_schema.replication_group_members`; read only from a plugin that is `ACTIVE` with a group name |

The transaction size limit came with dbsgomysql v1.5.0, whose upgrade notes apply to any code that builds the library's
`GroupReplication` value: an unkeyed literal no longer compiles, a comparison of the whole value
with one that sets only the status and the name is false wherever the plugin is `ACTIVE` (compare the
fields you mean), and the fact issues a third statement. GoArchive's own literals are keyed and it
compares fields.

The mode came with v1.6.0 (`SinglePrimaryMode` and `SinglePrimaryModeKnown`), which also moved the fact's
statements: the group name, the limit and the mode are read first, and `information_schema.PLUGINS` last and only
when the group name's row is absent. The fact's fields are the same, so the bump needed no change in preflight; its
upgrade notes (a comparison of the whole value with one that leaves the mode empty is false wherever the plugin is
`ACTIVE`, and the mode read joins an allow-list of the library's statements) changed one test, the one that pins each
side's first statements, which now keeps the rule (the configuration read first, the group facts before any table)
and not the library's order. GoArchive's copy of the library's Group Replication test helpers
(`internal/testestate`, for the multi-primary and failover tests) and of its estate files
(`tests/envmatrix/group/`) are from v1.6.0.

## Versioning

GoArchive pins an exact released tag of the library — no `replace` directives, no
pseudo-versions, no committed `go.work`. **Which tag is recorded in `go.mod`, and only
there:**

```bash
grep dbsgomysql go.mod
```

## See also

- [Validation & Preflight](README_VALIDATION.md) — every check
- [Permissions](README_PERMISSIONS.md) — the privilege matrix and grant recipes
- [dbsgomysql on GitHub](https://github.com/dbsmedya/dbsgomysql)
