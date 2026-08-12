# Why GoArchive uses dbsgomysql

GoArchive's preflight validation is built on
[dbsgomysql](https://github.com/dbsmedya/dbsgomysql), a standalone Go library for reading
and comparing MySQL schema, privilege, and metadata facts.

This page explains **why that dependency exists and what it provides**. It is background,
not a task: if you are upgrading, read [Upgrading to 2.0](README_UPGRADING_2_0.md); if you
want to know what each check does, read [Validation & Preflight](README_VALIDATION.md).

## The problem it solves

Every one of GoArchive's 19 preflight checks answers a question about MySQL metadata —
does this table exist, is it InnoDB, is this column the primary key, can this account
prove it holds `DELETE` here, does a foreign key point into the archive graph from outside
it. Through 1.8, GoArchive answered all of those itself, with hand-written
`information_schema` queries scattered across the preflight code.

That turned out to be the wrong place for the work. Reading MySQL metadata *correctly* is
a specialist problem with sharp edges that have nothing to do with archiving:

- `information_schema.TABLES.TABLE_NAME` collates case-**sensitively** while
  `COLUMNS.COLUMN_NAME` collates case-**insensitively** — two columns in the same database,
  two different behaviours.
- A privilege held through an active role, or granted globally while
  `@@global.partial_revokes` is enabled, is not provable for a specific object from
  `information_schema` alone.
- InnoDB's own foreign-key registry is complete but requires `PROCESS`;
  `information_schema` is always readable but only shows constraints on tables the account
  is already privileged for — so a fallback silently answers a weaker question.
- Integer display widths (`bigint(20)`) are cosmetic and no longer reported by MySQL
  8.0.17+, except `tinyint(1)`, which aliases `BOOLEAN` and is not cosmetic at all.

Each of these was a latent false-pass in a data-deleting tool. Extracting them into a
library means one place owns MySQL correctness, and it is versioned, documented, and
tested against several MySQL releases instead of being rediscovered per check.

## What the library provides

| Package | GoArchive uses it for |
|---|---|
| `pkg/validations` | Schema, privilege, and metadata facts (tables, columns, primary keys, triggers, invisible columns, foreign keys, grants), plus the comparison primitives behind destination schema compatibility. Findings are typed values, not formatted strings — GoArchive decides what is fatal. |
| `pkg/sqlutil` | Identifier quoting and validation. This replaced GoArchive's own `internal/sqlutil`, which no longer exists. |

The library also ships a registry of documented MySQL version quirks and records where
behaviour diverges across releases, probed against **MySQL 8.0.46, 8.4.9, and 9.7.1**.

## The division of labour

The split is deliberate and it is the reason the integration is safe:

- **The library reports facts.** It does not know what GoArchive intends to do with a
  table, and it never decides that a schema difference is fatal.
- **GoArchive owns policy.** Whether a destination may be looser than its source, which
  primary-key types are acceptable for a root table, whether a difference blocks a copy —
  all of that stays here, in GoArchive, where the consequences are understood.

A concrete example: the library reports that a source column is `NULL`-able and the
destination is `NOT NULL`. It does not rank that. GoArchive knows the copy inserts
explicit values, so a *looser* destination is safe and a *stricter* one is fatal — and
that judgement lives in GoArchive.

## The consumer boundary

GoArchive issues **no `information_schema` queries of its own**. That is enforced, not
just intended: `make consumer-policy` walks every non-test Go file in the module and fails
the build on any string literal naming `information_schema`.

Preflight facts are read **once per run** and shared by every check that needs them, so
adding a check does not add a round trip. Grants are read on a dedicated connection that
is released before any other query runs, which keeps the answer accurate on a pool and
avoids self-deadlocking a single-connection configuration.

## Versioning

GoArchive pins an exact released tag of the library — no `replace` directives, no
pseudo-versions, no committed `go.work`. **Which tag is recorded in `go.mod`, and only
there:**

```bash
grep dbsgomysql go.mod
```

The 1.9.x release-candidate series validated this integration against successive
**pre-1.0** library releases. GoArchive `2.0.0-community` now pins the library's stable
**v1.0.0** contract and is the current stable release. The RC series exercised that
contract against real archival workloads before its promotion to the production line.

## See also

- [Upgrading to 2.0](README_UPGRADING_2_0.md) — the six behaviours that change, and what to do
- [Validation & Preflight](README_VALIDATION.md) — all 20 checks
- [Permissions](README_PERMISSIONS.md) — the grant recipe and the invariants preflight enforces
- [dbsgomysql on GitHub](https://github.com/dbsmedya/dbsgomysql)
