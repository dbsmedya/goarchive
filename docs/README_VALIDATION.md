# Validation & Preflight Reference

Every check GoArchive runs before it moves a row: what it inspects, why, which job types run
it, and how to fix a failure.

**Related:** [Configuration](README_CONFIGURATION.md) ·
[Permissions](README_PERMISSIONS.md) · [Operations](README_OPERATIONS.md) ·
[Limitations](README_LIMITATIONS.md) · [Testing](README_TESTING.md) ·
[← Back to README](../README.md)

---

## Contents

- [How preflight works](#how-preflight-works)
- [Which checks run for which command](#which-checks-run-for-which-command)
- [Connection checks](#connection-checks)
- [Source structure checks](#source-structure-checks)
- [Primary key checks](#primary-key-checks)
- [Destination checks](#destination-checks)
- [Foreign key checks](#foreign-key-checks)
- [Permission checks](#permission-checks)
- [Trigger checks](#trigger-checks)
- [Server variable checks](#server-variable-checks)
- [Warnings](#warnings)
- [Error prefixes that are not checks](#error-prefixes-that-are-not-checks)
- [Schema compatibility rules](#schema-compatibility-rules)
- [SHA256 verification](#sha256-verification)
- [Bypassing preflight](#bypassing-preflight)

---

## How preflight works

Preflight runs **before any tracking state is written** and before any row is read for copying
or deleting. `archive`, `purge` and `copy-only` run it at startup with their own accounts;
`validate` (every job, or one with `--job`) and `dry-run` run it on demand. A passing `validate`
is not carried forward to a later run.

Preflight checks the job's structure against MySQL. It cannot judge whether a job's `where`
selects the rows the operator meant.

The checks that run follow the job's `job_type`, under `validate` and `dry-run` too: a purge
job is never asked for destination tables. Checks run in a fixed order and **stop at the first
failure**, so fixing one can reveal the next. `validate` reports each job `PASSED` or `FAILED`
and goes on to the next job.

A failure names the check, says what is wrong, and lists the offending tables:

```
PK_COLUMN_CHECK: The configured primary_key must list the PRIMARY KEY's columns, all of them, in
key order (tables: [order_lines(primary_key (id) but the PRIMARY KEY is (order_id, line_no))])
```

Preflight's first statements are not checks, apart from two: it reads the
[Group Replication facts](README_GROUP_REPLICATION.md#what-is-read) of both servers (each side's
configuration, each side's plugin state and, for a source that is a member, its view of the group)
before it reads any table. They are read whatever is skipped, and they are what `validate`, `dry-run`
and a run print as the Group Replication block. The two checks that judge the configurations as they
are read, [`SOURCE_READ_ONLY_CHECK`](#source_read_only_check) and
[`DEST_READ_ONLY_CHECK`](#dest_read_only_check), come first, and no flag skips either.

Preflight computes each key-addressed statement's cap, the most key tuples it carries, and fails
when a cap cannot be computed. `validate` and `dry-run` print the caps; a run logs one
`Statement cap:` line per statement.

---

## Which checks run for which command

In run order. ✅ = enforced, ❌ = not run. `validate` and `dry-run` run the column of the job's
type.

| Check | archive | purge | copy-only |
|-------|:-------:|:-----:|:---------:|
| `SOURCE_READ_ONLY_CHECK` | ✅ | ✅ | ❌ |
| `DEST_READ_ONLY_CHECK` | ✅ | ✅ | ✅ |
| `SOURCE_GROUP_VIEW_PERMISSION_CHECK` | ✅ | ✅ | ✅ |
| `GROUP_MULTI_PRIMARY_CHECK` | ✅ | ✅ | ✅ |
| `REPLICATION_TYPE_CHECK` | ✅ (replication enabled) | ✅ (replication enabled) | ❌ |
| `TABLE_EXISTENCE_CHECK` | ✅ | ✅ | ✅ |
| `PRIMARY_KEY_CHECK` | ✅ | ✅ | ✅ |
| `KEY_TYPE_UNSUPPORTED` | ✅ | ✅ | ✅ |
| `PK_COLUMN_CASE_CHECK` | ✅ | ✅ | ✅ |
| `PK_COLUMN_CHECK` | ✅ | ✅ | ✅ |
| `STORAGE_ENGINE_CHECK` | ✅ | ✅ | ✅ |
| `RELATION_KEY_CHECK` | ✅ | ✅ | ✅ |
| `FK_INDEX_CHECK` | ✅ | ✅ | ✅ |
| `JOB_SCHEMA_PERMISSION_CHECK` | ✅ | ✅ | ✅ |
| `DEST_TABLE_EXISTENCE_CHECK` | ✅ | ❌ | ✅ |
| `DEST_STORAGE_ENGINE_CHECK` | ✅ | ❌ | ✅ |
| `COLUMN_CHARSET_CHECK` | ✅ | ✅ | ✅ |
| `DEST_KEY_IDENTITY_CHECK` | ✅ | ❌ | ✅ |
| `DEST_SCHEMA_COMPATIBILITY_CHECK` | ✅ | ❌ | ✅ |
| `DEST_WRITE_PERMISSION_CHECK` | ✅ | ❌ | ✅ |
| `DEST_INSERT_TRIGGER_VISIBILITY_CHECK` | ✅ | ❌ | ✅ |
| `DEST_INSERT_TRIGGER_CHECK` | ✅ | ❌ | ✅ |
| `FK_COVERAGE_VISIBILITY_CHECK` | ✅ | ✅ | ❌ |
| `FK_COVERAGE_CHECK` | ✅ | ✅ | ✅ |
| `INTERNAL_FK_COVERAGE` | ✅ | ✅ | ✅ |
| `FK_PAIRING_CHECK` | ✅ | ✅ | ✅ |
| `SELF_FK_DELETE_RULE_CHECK` | ✅ | ✅ | ❌ |
| `SOURCE_SELECT_PERMISSION_CHECK` | ✅ | ✅ | ✅ |
| `SOURCE_DELETE_PERMISSION_CHECK` | ✅ | ✅ | ❌ |
| `DELETE_TRIGGER_VISIBILITY_CHECK` | ✅ | ✅ | ❌ |
| `DELETE_TRIGGER_CHECK` | ✅ | ✅ | ❌ |
| `MAX_ALLOWED_PACKET_CHECK` | ✅ | ✅ | ✅ |

- **The two read-only checks and `SOURCE_GROUP_VIEW_PERMISSION_CHECK`** are the checks of preflight's
  first two stages, which read the [Group Replication facts](README_GROUP_REPLICATION.md#what-is-read)
  before any table. The read-only checks come first, on each side's configuration, so a server that is
  read-only is refused before the plugin-state read that a Group Replication fence can hold
  ([a failover's backlog](README_GROUP_REPLICATION.md#a-failovers-backlog)). The view's check refuses only a
  source that is a Group Replication member whose account cannot read the group's view; a source
  without the plugin is never asked, and nothing is refused. A source that is read-only, an archive's
  or a purge's, is refused for that first, even when its account also lacks the view's grant.
- **`GROUP_MULTI_PRIMARY_CHECK`** judges the mode of each server whose plugin is `ACTIVE` with a group name,
  right after those facts, for every job type. No flag but its own, `--allow-multi-primary`, skips it:
  `--skip-dangerously-validate-preflight-allow-experimental` does not.
- **Purge** copies nothing, but its tracking tables live in the destination's `job_schema`, so
  it still connects to the destination and runs `JOB_SCHEMA_PERMISSION_CHECK`.
- **Copy-only** deletes nothing, so no cascade can fire: it skips the foreign-key visibility
  proof, the self-reference delete rule, the delete permission and the DELETE-trigger checks. It
  still runs `FK_COVERAGE_CHECK`, but without `PROCESS` that check sees only the foreign keys the
  account can read, so a cross-schema one can pass unseen.
- [Connection checks](#connection-checks) run before preflight, for every command that connects
  (`archive`, `purge`, `copy-only`, `validate`, `dry-run`).

---

## Connection checks

These run when GoArchive connects, before preflight and before any tracking row is written.
`--skip-dangerously-validate-preflight-allow-experimental` does not reach them.

### `SRC_DEST_IDENTITY_CHECK`

Source and destination report the same `server_uuid` and the same schema name (compared without
regard to letter case). An archive into itself would verify the table against itself and then
delete the only copy. A different schema on the same server is allowed.

```
failed to connect to databases: SRC_DEST_IDENTITY_CHECK: source and destination
are the same database (server_uuid=…; source schema=app at db1:3306, destination
schema=app at db1:3306). GoArchive refuses: it would verify the table against
itself and delete the only copy. …
```

**Fix:** point `destination` at a different server or schema. Schema names that differ only in
letter case count as one schema on every server. For two servers cloned from one data directory,
which share `auto.cnf` and so `server_uuid`: on the standalone archive server, stop mysqld, remove
`auto.cnf` from the data directory, and start it again to get a new `server_uuid`. A replicating
server follows MySQL's guidance for changing `server_uuid`.

### `AUTO_INCREMENT_ZERO_MODE_CHECK`

Every connection — source, destination and replica, including those a pool opens later — adds
`NO_AUTO_VALUE_ON_ZERO` to its inherited session SQL modes, so an explicit zero in an
AUTO_INCREMENT column stays zero while other modes remain intact. The failure means GoArchive
could not prove the session does this. Check whether the server or a proxy honors connection
initialization; a wrapped query cause in the message comes first.

### Session time zone

Every pool, source, destination and replica, proves when it opens that its session runs in UTC
(`time_zone = '+00:00'`, [Session settings](README_CONFIGURATION.md#session-settings)). A server or
proxy that overrides it is refused with `session time zone is not UTC`; the check has no name and
no flag reaches it.

### `MAX_ERROR_COUNT_CHECK`

MySQL keeps at most `max_error_count` of a statement's warnings, and GoArchive reads them after
every INSERT and every key-addressed statement. Every new source and destination connection,
at startup or opened later by its pool, reads its session value and raises a 0 to 1024. The
failure means it could not:

```
failed to connect to databases: failed to connect to destination database: destination: MAX_ERROR_COUNT_CHECK: db2:3306: the session's max_error_count is 0, so MySQL keeps none of a statement's warnings for GoArchive to read, and this account may not raise it (Error 1227 (42000): Access denied; you need (at least one of) the SYSTEM_VARIABLES_ADMIN or SESSION_VARIABLES_ADMIN privilege(s) for this operation). Raise the server's global max_error_count (MySQL's default is 1024), or grant SESSION_VARIABLES_ADMIN to this account so GoArchive raises its own sessions to 1024
```

**Fix:** have a DBA raise the global (`SET PERSIST max_error_count = 1024`), or grant
`SESSION_VARIABLES_ADMIN` to the account named for that server
([Permissions](README_PERMISSIONS.md#privilege-matrix)). Other forms of the message carry the
server's error; diagnose that. A connection that dies during the check is an ordinary
connection failure, retried like one. Replica connections are not checked: they read replica
status only.

### `SESSION_CHARSET_CHECK`

GoArchive reads every value in the session's `character_set_results` and binds every value in its
`character_set_client`, so every connection is pinned to `utf8mb4` with `utf8mb4_general_ci`: the
driver runs `SET NAMES utf8mb4 COLLATE utf8mb4_general_ci` after authentication, so after any
`init_connect`. Every new source and destination connection, at startup or opened later by its
pool, then reads its session and requires `utf8mb4` for client, connection and results, with
`utf8mb4_general_ci`. A narrowed session would read `utf8mb4` text it cannot hold as `?`, and a
key bound back would match a look-alike row. The failure names the values found:

```
failed to connect to databases: failed to connect to source database: source: SESSION_CHARSET_CHECK: db1:3306: the session is client=latin1 connection=latin1 results=latin1 collation=latin1_swedish_ci, not utf8mb4 with utf8mb4_general_ci, so text would read back changed. GoArchive sets it with SET NAMES at connect; an init_connect or a proxy on this server changes it after
```

**Fix:** find what rewrites the session after `SET NAMES`, usually a proxy, and make it keep the
client's character set. A server's `init_connect` runs before GoArchive's `SET NAMES` and needs no
change. The check proves the session at connect, per physical connection; a multiplexing proxy
that changes the session between statements is not covered, as with the other session pins.
Replica connections are pinned but not checked: they read replica status only.

### `GROUP_CONSISTENCY_CHECK`

After a failover the new primary of a Group Replication group is read-only and applies the backlog it held
when it was elected. A session whose `group_replication_consistency` includes the fence
(`BEFORE_ON_PRIMARY_FAILOVER`, `BEFORE`, `AFTER` or `BEFORE_AND_AFTER`) is held there until it has, so it never
reads the old state; an `EVENTUAL` session reads at once, and can read around the backlog
([a failover's backlog](README_GROUP_REPLICATION.md#a-failovers-backlog)). So every new source and
destination connection, at startup or opened later by its pool, a replacement in the middle of a run included,
is made to be fenced before the pool hands it out. It asks the server two things on the connection and keeps
them apart: whether the Group Replication plugin is `ACTIVE`, which says whether the rule applies, and the
session's own `group_replication_consistency`, which says whether the session is fenced. The server's global
value decides nothing.

| The plugin | The session's value | What GoArchive does |
|---|---|---|
| Not installed, or any status but `ACTIVE` | Any, or none | Nothing: the rule does not apply, and the session is left as it was found |
| Its state cannot be read | | Refuses the connection |
| `ACTIVE`, running in a group or not | `EVENTUAL` | Sets the session to `BEFORE_ON_PRIMARY_FAILOVER`, reads it again, and requires it |
| `ACTIVE` | `BEFORE_ON_PRIMARY_FAILOVER`, `BEFORE`, `AFTER`, `BEFORE_AND_AFTER` | Nothing: an operator's stronger choice is never weakened |
| `ACTIVE` | Another value, or none | Refuses the connection |
| `ACTIVE` | Cannot be read, the `SET` is refused, or it does not read back as set | Refuses the connection |

A server without the plugin is not changed. Each new connection to it costs four small reads of the plugin's state
and the session's consistency, none of which reads a table of the job (a new connection to a plain server sends nine
statements in all: the driver's three, the two checks above and these four). A refusal fails the run with the server named, what was found, and the two ways out:

```
GROUP_CONSISTENCY_CHECK: gr2:3306: cannot set the session's group_replication_consistency to BEFORE_ON_PRIMARY_FAILOVER: Error 1227 (42000): Access denied; you need (at least one of) the SYSTEM_VARIABLES_ADMIN or SESSION_VARIABLES_ADMIN privilege(s) for this operation. GoArchive makes every session on a server whose Group Replication plugin is active wait for a newly elected primary's backlog, and it will not run on one it cannot make so. Either give this account the privilege the server asks for that SET (SESSION_VARIABLES_ADMIN or SYSTEM_VARIABLES_ADMIN, where it asks for one), or make a stronger value this account's or the server's default: group_replication_consistency BEFORE_ON_PRIMARY_FAILOVER, BEFORE, AFTER or BEFORE_AND_AFTER
```

**Fix:** one of the two ways out. The `SET` needs no grant beyond the documented ones on the servers measured
([Permissions](README_PERMISSIONS.md#privilege-matrix)). A connection that dies during the check is an ordinary
connection failure, retried like one; a refusal is a property of the server and the account and is not retried.
Replica connections are not checked: they read replica status only.

---

## Source structure checks

### `TABLE_EXISTENCE_CHECK`

Every job table must exist in the source schema **as a base table**. A view or any other object
is refused, named with its type, e.g. `orders(VIEW)`. If the account has no privilege at all on
the source schema, MySQL hides it entirely and this check fails rather than
`SOURCE_SELECT_PERMISSION_CHECK`.

**Fix:** correct the table name, or point `source.database` at the right schema. Archive a
view's underlying table instead. Under `lower_case_table_names=1`, write table names in lower
case: a mixed-case name is not found, and this check refuses it.

### `STORAGE_ENGINE_CHECK`

Every source job table must be **InnoDB**. A base table whose engine MySQL reports as `NULL` is
refused too, as `<table>(<unknown>)`; that usually means a corrupted or unknown-engine table.

**Fix:** `ALTER TABLE <table> ENGINE=InnoDB;`

### `COLUMN_CHARSET_CHECK`

A job column the run reads or writes as text uses a character set that does not survive the
`utf8mb4` session exactly. In such a set two characters can read as one, or a character reads as
`?`: two keys then merge, so a row is never copied and an in-tree `ON DELETE CASCADE` deletes it,
and text is written back changed, while every verification passes. It runs before any row is
read: after `DEST_STORAGE_ENGINE_CHECK`, or after `JOB_SCHEMA_PERMISSION_CHECK` for purge, which
runs no destination check:

```
COLUMN_CHARSET_CHECK: A job column uses a character set that GoArchive cannot read and write back
exactly through its utf8mb4 session, so keys could merge and text change without notice (tables:
[children.code(cp932) destination:notes.body(sjis)])
```

The supported and refused sets, and why: [Character sets](README_CHARSETS.md#supported-character-sets).
The check judges the character set, not the collation.

| Job type | Columns checked |
|---|---|
| `archive`, `copy-only` | every column of every job table on the source, and every destination column of the same name |
| `purge` | the primary-key and `foreign_key` columns of every job table, on the source |

`ENUM` and `SET` columns are checked; numbers, temporal types, binary strings and `JSON` have no
character set and are not. It runs under
`--skip-dangerously-validate-preflight-allow-experimental` too.

**Fix:** convert the column to `utf8mb4` on both servers, or leave the table out of the job. A
strict `ALTER` merges some values without a message, so check first:
[Converting a refused column](README_CHARSETS.md#converting-a-refused-column).

---

## Primary key checks

GoArchive reads every table's primary key from MySQL and builds all SQL from it. The configured
`primary_key` documents the key; it is checked against MySQL and never reaches a statement.

### `PRIMARY_KEY_CHECK`

A job table has no `PRIMARY KEY`, so its rows cannot be addressed.

**Fix:** add a `PRIMARY KEY` to the table.

### `KEY_TYPE_UNSUPPORTED`

A primary-key column cannot identify rows. Allowed: the integer types, `DECIMAL`, `CHAR`,
`VARCHAR`, `BINARY`, `VARBINARY`, `DATE`, `DATETIME`, `TIMESTAMP`, `TIME` and `YEAR`, without a
prefix length. `FLOAT`/`DOUBLE` are approximate, `ENUM`/`SET` sort by their internal index, and
`BIT` is not supported.

**Fix:** choose another root table, or leave the table out of the job.

**Temporal key values** must also be valid. `DATE` is `YYYY-MM-DD`. `DATETIME` and `TIMESTAMP` add
` HH:MM:SS` and an optional fraction: a dot and one to six digits. The year is 0001 to 9999, the day
must exist in its month, and the clock must be valid, so a leap second (`:60`) is refused. A zero
date, an invalid clock, a timezone suffix or a leap second is refused with
`TEMPORAL_READ_CONTRACT`. The check runs in GoArchive on the text the server returns, whatever the
server's SQL modes, and no flag or configuration key skips it. The key text is kept as the server
wrote it. Recovery reads the keys it logged the same way, so a refused key fails the run before the
delete of the batch or recovery page that meets it; batches and pages before it are already deleted.

`PRIMARY_KEY_CHECK` and `KEY_TYPE_UNSUPPORTED` run even under
`--skip-dangerously-validate-preflight-allow-experimental`, because the SQL is built from the keys.

### `PK_COLUMN_CASE_CHECK`

The configured `primary_key` names the key's columns but differs in letter case, e.g. `log_id`
for `LOG_ID`. Reported before `PK_COLUMN_CHECK` because its fix differs.

**Fix:** write each column exactly as the database spells it.

### `PK_COLUMN_CHECK`

The configured `primary_key` is not the table's `PRIMARY KEY`: a column is missing, extra, or out
of key order.

**Fix:** list every key column, in key order. A composite key is a list:
`primary_key: [order_id, line_no]`.

### `RELATION_KEY_CHECK`

Each `foreign_key` column must exist in its table and pair with the parent key column it
references (`references`, by position): two integer types of the same signedness, two `DECIMAL`
types, two character types with the same charset and collation, `BINARY(n)` with `BINARY(n)`,
`VARBINARY` with `VARBINARY`, or the same temporal type with the same fractional precision. A
nullable `foreign_key` column is allowed: a child row with a NULL in it matches no parent.

**Fix:** correct `foreign_key` or `references`, or align the column types.

---

## Destination checks

Skipped for purge jobs, which copy nothing.

### `DEST_TABLE_EXISTENCE_CHECK`

Every job table must exist in the destination schema, under the same name, as a base table.

**Fix:** create the destination tables, usually from a schema-only dump of the source. If the
destination's lookup tables will be empty, see
[`disable_foreign_key_checks`](README_CONFIGURATION.md#disable_foreign_key_checks).

### `DEST_STORAGE_ENGINE_CHECK`

Every destination job table must be **InnoDB**: the copy writes each batch in one destination
transaction, and only InnoDB rolls it back. A table whose engine MySQL reports as `NULL` is
refused as `<table>(<unknown>)`.

**Fix:** `ALTER TABLE <table> ENGINE=InnoDB;` on the destination.

### `DEST_KEY_IDENTITY_CHECK`

The destination `PRIMARY KEY` must have the source's columns, in key order, with the same type,
charset and collation: a different collation changes which keys collide and the order
verification reads rows in. One widening is allowed: a `utf8mb3` key column copied into
`utf8mb4` with the twin collation, such as `utf8mb3_general_ci` to `utf8mb4_general_ci`.

**Fix:** give the destination table the source's primary key.

### `DEST_SCHEMA_COMPATIBILITY_CHECK`

Matches source and destination columns **by name** (ASCII letter case ignored, as MySQL does),
then compares each pair. The destination may be looser than the source, never stricter: the
[Schema compatibility rules](#schema-compatibility-rules) list every difference.

### `DEST_INSERT_TRIGGER_VISIBILITY_CHECK`

The destination account cannot be proven to see a job table's triggers. MySQL lists a table's
triggers only to an account holding `TRIGGER` on it, so without that privilege an empty list does
not mean there are none, and `DEST_INSERT_TRIGGER_CHECK` could not be trusted. The message names
each table with its state, as the [permission checks](#permission-checks) do.

**Fix:** grant `TRIGGER` directly to the account at schema or table scope
([recipe](README_PERMISSIONS.md#grant-recipes)); a role-held `TRIGGER` stays `unconfirmed`.
`--force-triggers` does not apply.

### `DEST_INSERT_TRIGGER_CHECK`

A destination table has an `INSERT` trigger. It would change copied rows before verification
reads them.

**Fix:** drop the destination INSERT triggers. `--force-triggers` does not apply.

---

## Foreign key checks

**Declared foreign keys are a floor, not a ceiling.** A relation needs no foreign key in the
database: relations kept only by an application (an ORM) are declared in the config and archived
the same way. For the foreign keys the database **does** declare, these checks guarantee that no
table outside the job references a job table (`FK_COVERAGE_CHECK`) and that every one between two
job tables is a configured relation, paired the same way (`INTERNAL_FK_COVERAGE`,
`FK_PAIRING_CHECK`). `FK_COVERAGE_VISIBILITY_CHECK` proves GoArchive saw all of them. A
self-referencing foreign key (`category.parent_id → category.id`) is not a relation; archive and
purge refuse one that cascades or sets NULL (`SELF_FK_DELETE_RULE_CHECK`,
[supported relationship types](README_LIMITATIONS.md#supported-relationship-types)).

### `FK_INDEX_CHECK`

Every relation needs an index on its child whose leading columns are exactly the `foreign_key`
columns, in any order: discovery reads the child by `foreign_key`, and without such an index
every batch scans the table. A prefix, functional or `INVISIBLE` index does not count. This
covers relations without a declared foreign key too.

**Fix:** `CREATE INDEX idx_fk ON <table> (<foreign_key columns>)`.

### `FK_COVERAGE_CHECK`

A table **outside** the job holds a foreign key **into** a job table. Deleting the parent either
fails (`RESTRICT`, `NO ACTION`) or changes rows GoArchive never copied (`CASCADE`, `SET NULL`), so
**every `ON DELETE` rule is fatal**. Incoming keys are found by the referenced schema, so a
constraint defined in another schema is detected; such a table can never join the job
([identifier rules](README_CONFIGURATION.md#identifier-rules)).

```
FK_COVERAGE_CHECK: Foreign key constraints not covered by relations
(fatal for any ON DELETE rule):
  - orders is referenced by: [analytics.order_snapshots (ON DELETE CASCADE)]
```

**Fix:** add the referencing table to the job if it is in the same schema and should be archived;
otherwise drop the constraint, or leave the referenced table out of the job.

### `INTERNAL_FK_COVERAGE`

A foreign key between two job tables is not a configured relation with the same `foreign_key`
columns. Each line names the constraint and why:

- `[no relation: … is not configured under …]` — the child is not nested under that parent;
- `[the relation's foreign_key is …]` — the relation uses other columns;
- `[it references …, which is not the PRIMARY KEY of …]` — a foreign key to a parent's unique
  key; `references` lists the parent's primary key, so this is not supported.

A table appears once in a job, so a second foreign key from one child to the same parent over
other columns cannot be configured. Unfixed, the delete fails with error 1451, or `ON DELETE
CASCADE` removes child rows that were never copied.

**Fix:** nest each child under its parent, with `foreign_key` and `references` matching the
constraint, or leave one of the two tables out of the job.

### `FK_PAIRING_CHECK`

The relation's `foreign_key` columns match the constraint, but `references` pairs them with the
parent's key columns differently. A swapped composite pairing would select another parent's
children.

**Fix:** list `references` in the order that pairs with `foreign_key`.

### `SELF_FK_DELETE_RULE_CHECK`

Archive and purge only. A job table, root or child, has a foreign key to itself whose rule is
`ON DELETE CASCADE` or `SET NULL`. Deleting a batch row would make InnoDB delete (`CASCADE`) or
clear the parent of (`SET NULL`) rows of the same table outside the batch, which were never
copied, and the run would report success. Each line names the constraint and its rule:

```
  - comments(parent_id) -> comments(id) (constraint fk_comments_parent, ON DELETE CASCADE)
```

`RESTRICT` and `NO ACTION` pass: a delete they block fails with error 1451 and nothing is lost.
Any other rule is refused.

**Fix:** redefine the constraint with `ON DELETE RESTRICT` (`ALTER TABLE … DROP FOREIGN KEY …,
ADD CONSTRAINT … ON DELETE RESTRICT`), or define a separate copy-only job for the table; a job's
type is fixed once it has run.

### `FK_COVERAGE_VISIBILITY_CHECK`

GoArchive cannot prove it saw **every** foreign key pointing into the job, including those in
schemas the account cannot read. Without that proof an external `ON DELETE CASCADE` or `SET NULL`
could change rows that were never copied.

The proof is a successful read of InnoDB's foreign-key registry, which normally needs `PROCESS`
([the version exception](README_COMPAT.md#foreign-key-metadata-readable-without-process-on-mysql-8040));
the state is then `complete`, the only one that passes. If that read fails, discovery falls back to a
view filtered by the account's privileges and the state is `unconfirmed`; `unknown` means no
proof was populated. The message says which stage failed: a query-stage failure can be
privileges, connectivity or server state, so read the logged MySQL cause first; a read-stage
failure (rows returned but not decoded) is not fixed by grants — check MySQL and dbsgomysql
compatibility.

**Fix**, when the logged cause is a privilege rejection: `GRANT PROCESS ON *.* TO <user>`. A
`PROCESS` held through a role is accepted
([Permissions](README_PERMISSIONS.md#the-provable-grant-rule)).

---

## Permission checks

`SOURCE_SELECT_PERMISSION_CHECK`, `SOURCE_DELETE_PERMISSION_CHECK`,
`DEST_WRITE_PERMISSION_CHECK` and `JOB_SCHEMA_PERMISSION_CHECK` each pass only when the privilege
is provable for the object ([the provable-grant rule](README_PERMISSIONS.md#the-provable-grant-rule)).
Three states fail, and the message names the state:

| State | Meaning | Fix |
|---|---|---|
| absent | no grant exists at any scope | grant the privilege |
| unconfirmed | a grant may exist but cannot be proven ([why](README_PERMISSIONS.md#the-provable-grant-rule)) | grant it **directly** to the account, at schema or table scope |
| unknown | the privilege fact was not populated | report it: an inspection problem, not a configuration one |

Table-scoped checks name `TABLE(state)`, e.g. `orders(unconfirmed)`;
`JOB_SCHEMA_PERMISSION_CHECK` names `PRIVILEGE(state)`, e.g. `CREATE(unconfirmed)`, because it
tests four privileges on one schema. Each fix is a
[grant recipe](README_PERMISSIONS.md#grant-recipes).

### `SOURCE_READ_ONLY_CHECK`

An archive or a purge deletes from the source, and a run's recovery replays deletes there, so a
source with `read_only` or `super_read_only` on is refused before anything is copied or replayed. It
is judged on the source's configuration, preflight's first statement there, and says which of the two
variables is on. Which one stops GoArchive's own account depends on its privileges, so the check
refuses on either. It runs for an archive and a purge; a copy-only run never writes the source and is
exempt (reading a secondary or a replica is a use [the docs describe](README_GROUP_REPLICATION.md)).
Under `--skip-dangerously-validate-preflight-allow-experimental` it still runs, and no other flag skips it.

It applies to any read-only source, a plain replica included. On a Group Replication group a
secondary is read-only, and so is a newly elected primary until it has applied its backlog
([a failover's backlog](README_GROUP_REPLICATION.md#a-failovers-backlog)). The refusal is the same either
way. Its last sentence says why, from the role of the source's own row in its group's view, which is read
after the configuration and is not held by a failover's fence:

| The source | The message ends with |
|---|---|
| A `SECONDARY` | `The source is a SECONDARY of group <name>; the primary is <host:port>.` |
| The `PRIMARY` of its group, and read-only | `The source is the PRIMARY of group <name> and is read-only: it was newly elected and is still applying its backlog, or an operator made it read-only. Run again when it is writable.` GoArchive cannot tell the two apart, and says both |
| Not a member of a group, or a member that is not `ONLINE` | The general sentence: `On a Group Replication group a secondary is read-only, and so is a newly elected primary until it has applied its backlog.` A plain read-only server's view is not read |
| A member whose account cannot read the group's view | The general sentence, and the grant: `GRANT SELECT ON performance_schema.replication_group_members TO <user>`. The refusal is issued either way |

```
SOURCE_READ_ONLY_CHECK: the source is read-only (super_read_only is ON). An archive or a purge deletes
from the source, and its recovery replays deletes there, so it is refused before anything is copied or
replayed. The source is a SECONDARY of group 8a94f357-aab4-11df-86ab-c80aa9429563; the primary is gr1:3306.
```

Point the job at a source that is writable, or wait until the primary is, and run again. The log's
roots stay as they are and the next run recovers them.

A copy-only run whose source is the `PRIMARY` of its group in its own view and is read-only is not refused: a
primary an operator made read-only is a legitimate source for a copy. It prints one warning before its first table
read, under every flag: if the source was newly elected, the server holds the run's reads until it has applied its
backlog, for at most the session's `wait_timeout`; and how a held run is stopped (two signals, or one after a `stop`
over the control socket). A secondary, a writable primary and a plain server get no warning.

### `DEST_READ_ONLY_CHECK`

Every job type writes to the destination: its data, or its tracking tables. A destination with
`super_read_only` on is refused before anything else is asked of it, for an archive, a purge and a
copy-only run, and `validate` and `dry-run` answer for the job as a run would. No account can write
under `super_read_only`, so the check refuses no run that could have written at that moment; it is
also what a Group Replication secondary and a newly elected primary are set to. `read_only` alone
still lets a privileged account write and is not refused: such a run works, or fails at its first
write, as it does today. No flag skips this check, and nothing bypasses it, because the server would
refuse the writes.

```
DEST_READ_ONLY_CHECK: the destination is read-only (read_only and super_read_only are ON). Every job
writes to the destination: its data, or its tracking tables, so it is refused before anything else is
asked of it. On a Group Replication group a secondary is read-only, and so is a newly elected primary
until it has applied its backlog.
```

When both servers are read-only, the source's refusal is the one reported: it is judged first, and the
destination's configuration is not read for a job that is already refused.

### `SOURCE_GROUP_VIEW_PERMISSION_CHECK`

A source that is a Group Replication member (its plugin is `ACTIVE` and a group name is set)
needs `SELECT` on `performance_schema.replication_group_members`: GoArchive reads the group's
view to tell whether the source is a running member. It is not one of the provable-grant checks
above: the server's own refusal (error 1142) is the answer, so it has no states, and it is judged
before any table is read, so nothing has been locked, copied or replayed when it fails. A source
without the plugin is never asked and needs nothing. Runs for every job type.

```
SOURCE_GROUP_VIEW_PERMISSION_CHECK: The source is configured for Group Replication group
8a94f357-…, and its account cannot read the group's view (replication: group_members: Error 1142
(42000): SELECT command denied to user 'ga_src'@'…' for table 'replication_group_members').
GoArchive reads the view to tell whether the source is a running member of the group; grant it
directly to the account (GRANT SELECT ON performance_schema.replication_group_members TO <user>)
```

### `GROUP_MULTI_PRIMARY_CHECK`

GoArchive serializes runs with advisory locks, and an advisory lock is local to one server. In a Group Replication
group in multi-primary mode two runs connected to different members are not serialized, which is the mistake
the locks exist to catch, so a multi-primary group is refused. 3.1 supports single-primary groups.

The check reads one fact of each server whose plugin is `ACTIVE` with a group name, the source and then the
destination: `group_replication_single_primary_mode`, as the server spells it. It judges that value and no member's
role: a group in multi-primary mode with one member `ONLINE` and a peer recovering shows one primary, and the
peer becomes a second writer when it returns. It needs no privilege beyond connecting, so a destination that is a
member needs no new grant. It runs for every job type, after the group facts and before any table is read.

| The server's value | Outcome |
|---|---|
| `ON` | passes: single-primary |
| `OFF` | refused: multi-primary |
| no value returned although the plugin is active | refused, as "the group's mode could not be read" |
| any other text | refused the same way, naming the text as the server spelled it |

The value is the one the server is configured with. On a source that is a running member (its own row in the
group's view is `ONLINE`) that is the group's mode in force, and the refusal says the group is in multi-primary
mode. On a source that is not a running member, and on every destination (whose view is not read), it is the
server's own setting, which can differ from its group's when the server was down during a switch, and the
refusal says that the server is configured for multi-primary mode, and no more. A server that is not running
and is configured single-primary passes, whatever its group has become since.

```
GROUP_MULTI_PRIMARY_CHECK: the source is a member of Group Replication group 8a94f357-…, and the group is in
multi-primary mode (group_replication_single_primary_mode is OFF). GoArchive serializes runs with advisory
locks, and an advisory lock is local to one member, so in multi-primary mode two runs connected to different
members are not serialized. GoArchive 3.1 supports single-primary groups. Run the job against a single-primary
group, or accept this with --allow-multi-primary (command line only).
```

`--allow-multi-primary` is the one bypass: a command-line flag of `archive`, `purge`, `copy-only`, `validate`
and `dry-run`, with no configuration key. It accepts every refusal above, the "could not be read" cases
included, and prints one banner for each side it accepted, naming the case:

```
WARNING: --allow-multi-primary: the source is a member of Group Replication group 8a94f357-…, and the group is
in multi-primary mode (group_replication_single_primary_mode is OFF). Advisory locks are local to one member,
so two runs connected to different members are not serialized; GoArchive 3.1 supports single-primary groups.
```

It says nothing when nothing was refused. The bypass does not make the job safe: multi-primary mode has
restrictions of its own (cascading foreign keys among them) that GoArchive does not test, and the second run, on
another member, is not stopped by the first run's locks. The mode is judged once, at the start: a mode switched
during a run is not detected.

### `REPLICATION_TYPE_CHECK`

A server listed in `replication.servers` with `type: group` must be configured for Group Replication: its
plugin `ACTIVE` with a group name, and the same group as the source's when the source is configured for one. The
check reads each group server's state through the connection opened to it, which needs no privilege. It runs for
an archive and a purge with `replication.enabled: true`: a copy-only job deletes nothing and never asks the gate,
and with replication off there is no gate. It is a policy check, so
`--skip-dangerously-validate-preflight-allow-experimental` skips it. It follows the group facts, whose source group
it compares with.

The refusal names every wrong server, the type written and what was found:

```
REPLICATION_TYPE_CHECK: replication.servers[1] gr9.internal:3306 is listed with type: group, but the Group
Replication plugin is not installed on it. A server listed with type: group must be configured for the source's
group; an asynchronous replica is listed without a type, or with type: async.
```

The other findings are: the plugin is `INACTIVE` or `DISABLED` (or another state) on it, it is `ACTIVE` with no group
name set, or it is configured for another group than the source's (the message names both).

A server whose state cannot be read, because its connection failed after it was opened or the read errored, is not
judged: one `WARNING: REPLICATION_TYPE_CHECK: …` line says so (on stderr like a run banner, and in the log) and the run
goes on, the replication gate deciding about it. A server that is down when the command starts fails the connection
phase, for every monitored server, before preflight.

### `SOURCE_SELECT_PERMISSION_CHECK`

The source account needs `SELECT` on every job table. Runs for every job type.

### `SOURCE_DELETE_PERMISSION_CHECK`

The source account needs `DELETE` on every job table of an archive or purge job. `validate` and
`dry-run` check it too, so a missing grant is caught before a run has copied anything.

### `DEST_WRITE_PERMISSION_CHECK`

The destination account needs `INSERT` on every job table. Verification also needs destination
`SELECT`, which this check does not prove, so grant both
([recipe](README_PERMISSIONS.md#grant-recipes)).

### `JOB_SCHEMA_PERMISSION_CHECK`

The destination account needs `CREATE`, `SELECT`, `INSERT` and `UPDATE` on the tracking schema
(`destination.job_schema`, by default the destination database). `CREATE` is needed at run time:
each job's log table is created the first time the job runs. The message names the grant, and
`CREATE DATABASE` when `CREATE` is missing, since GoArchive never creates the schema
([`job_schema`](README_CONFIGURATION.md#job_schema-destination-only)):

```
JOB_SCHEMA_PERMISSION_CHECK: destination account lacks provable CREATE, UPDATE on tracking
schema "goarchive" (states: CREATE(unconfirmed), UPDATE(unconfirmed)). Grant each missing
privilege directly to the account at schema scope (DBA must: CREATE DATABASE `goarchive`;
GRANT CREATE, UPDATE ON `goarchive`.* TO <user>)
```

---

## Trigger checks

### `DELETE_TRIGGER_VISIBILITY_CHECK`

The source account of an archive or purge job cannot be proven to see a job table's triggers, so
an empty list of `DELETE` triggers proves nothing. The message names each table with its state, as
the [permission checks](#permission-checks) do.

**Fix:** grant `TRIGGER` directly to the account at schema or table scope
([recipe](README_PERMISSIONS.md#grant-recipes)); a role-held `TRIGGER` stays `unconfirmed`.
`--force-triggers` does not apply: it accepts triggers preflight can list, never a list it cannot
trust.

### `DELETE_TRIGGER_CHECK`

A source table of an archive or purge job has a `DELETE` trigger. GoArchive cannot see what it
does, and its side effects are outside the archive and its verification.

**Override:** `--force-triggers` (on `archive`, `purge`, `validate` and `dry-run`), for the triggers
this check lists; the triggers then fire on every delete. Audit them first.

---

## Server variable checks

### `MAX_ALLOWED_PACKET_CHECK`

Every key-addressed statement — the copy read, verification, delete chunks, discovery by
`foreign_key`, and the tracking log's statements — carries at most its statement cap of key
tuples, and at most `batch_size` (the source's by-key statements: the larger of `batch_size` and
`batch_delete_size`). Preflight sizes each statement at that many keys, counting the prepared
text and the bound values as they travel (character keys at four bytes a character, in the
session's `utf8mb4`). The statements after the last key, the root fetch and recovery's log
pages, are sized at any batch size. If one exceeds a server's `max_allowed_packet`, which GoArchive's
driver reads from each server, the run is refused before any row is read; without the check it
would fail mid-run.

The message names the server, the statement, the table and the bytes needed, and the remedies
that fit: raise `max_allowed_packet` to at least that size; lower `batch_size` or
`batch_delete_size` to the largest count that fits; or, for a statement at its cap, lower
`range_optimizer_max_mem_size` (set it, when it is 0) to shrink the cap. INSERTs carry rows, not
keys, and are not bounded here: the driver sends their large values separately, so what can still
fail them is one value larger than the destination's packet, or rarely the whole statement
([Known Limits](README_LIMITATIONS.md#a-value-larger-than-the-destinations-max_allowed_packet-fails-its-batch)).

---

## Warnings

Logged, never fatal:

- **`ON DELETE CASCADE`** between two job tables, for archive and purge jobs: GoArchive deletes
  children first itself, so a cascade is redundant work outside its ordering. Verify it is
  intended.
- **A relation's edge declared by several foreign keys** with the same columns and pairing: one
  advisory naming them; redundant and harmless to the job.
- **Schema advisories:** a collation difference outside the keys and destination unique indexes,
  a charset difference under sha256 verification, column names that differ only in ASCII
  letter case, and a column order that differs from the source's
  ([Schema compatibility rules](#schema-compatibility-rules)).
- **A Group Replication source that no group server watches**, for archive and purge jobs with
  `replication.enabled: true`, under `--skip-dangerously-validate-preflight-allow-experimental` too: one
  `WARNING: replication: the source is a Group Replication member and no server of type group is monitored: the
  group's backlog does not hold this job`, when the source is an `ONLINE` member and no `type: group` server is
  listed. It is a warning and not a refusal, because gating only an asynchronous replica is a legitimate
  choice. It prints on stderr like a run banner.
- **A JSON column under `verification: count` or `none`**, for archive and copy-only jobs, under
  `--skip-dangerously-validate-preflight-allow-experimental` too: one `WARNING:` naming every JSON
  column of the job's source tables, and every destination column that is JSON where its same-named
  source column is not. A JSON value is copied as MySQL's printed text, so a member's type is not
  kept, a long DECIMAL loses digits, and an 8.0 or 8.4 destination can change a double (MySQL Bug
  #116160); neither mode compares values, so the change goes unseen. The run proceeds; to have such
  a change refused, use `verification: sha256`
  ([JSON values that do not survive a text copy](README_COMPAT.md#json-values-that-do-not-survive-a-text-copy)):

  ```
  WARNING: verification: count: JSON columns orders.payload: a JSON value is copied as MySQL's
  printed text, so a member's type is not kept, a DECIMAL can lose digits, and an 8.0 or 8.4
  destination can change a double (MySQL Bug #116160). count does not compare values, so such a
  change goes unseen; an archive then deletes the original. See docs/README_COMPAT.md → JSON
  values that do not survive a text copy.
  ```

Preflight logs them at warning level, under every command, so `logging.level: error` hides them. The
JSON warning and the two replication warnings (the unmonitored group and an unreadable group server) are the
exception: like a run banner, they print on stderr whatever the log level.

---

## Error prefixes that are not checks

These can appear in preflight output. They are not checks and do not point at your schema or
grants: they mean GoArchive and its validation library disagree, and preflight fails closed
rather than guess. Report them with the full message.

| Prefix | Meaning |
|---|---|
| `PREFLIGHT_INSPECTION_INTEGRITY` | An inspection result was internally inconsistent |
| `PREFLIGHT_UNEXPECTED_FACTS` | A check arrived carrying facts of the wrong type |
| `PREFLIGHT_UNKNOWN_FINDING` | The library returned a check GoArchive does not recognise |
| `PREFLIGHT_UNEXPECTED_DIFF` | A schema difference GoArchive's inspection cannot produce |
| `PREFLIGHT_UNKNOWN_DIFF` | A schema difference kind GoArchive does not recognise |

`preflight <stage> inspection failed: <cause>` is different: a database failure (a lost
connection, a query error) while reading facts. Diagnose the cause.

These run-time failures are not preflight checks either, and stay active under
`--skip-dangerously-validate-preflight-allow-experimental`:

| Identifier | Meaning | Explained in |
|---|---|---|
| `TEMPORAL_READ_CONTRACT` | A temporal value, its metadata, or a key identity could not be established | [Temporal key values](#key_type_unsupported) |
| `INSERT_DIAGNOSTIC_REJECTED` | The INSERT raised a forbidden condition; the message carries its code, level and a bounded preview | [INSERT diagnostics](README_OPERATIONS.md#insert-diagnostics-and-subdivision) |
| `INSERT_DIAGNOSTICS_UNPROVEN` | The INSERT's diagnostics could not be read completely | [INSERT diagnostics](README_OPERATIONS.md#insert-diagnostics-and-subdivision) |

---

## Schema compatibility rules

`DEST_SCHEMA_COMPATIBILITY_CHECK` lets the destination be **looser** than the source, never
**stricter**. The copy inserts an explicit value for every column and never relies on
destination defaults or indexes, so a relaxation is harmless, while an extra constraint would
reject rows. Every incompatible table is reported, each with its first fatal difference.

### Allowed — destination may be looser

| Difference | Why it is safe |
|------------|----------------|
| Secondary indexes dropped | The copy does not read them; a supported write-performance optimization |
| `auto_increment`, defaults or `ON UPDATE` dropped | The copy supplies every value |
| `NOT NULL` relaxed to nullable | Strictly more permissive |
| Source column generated, destination plain | The read materialises the value |
| Column `INVISIBLE` on one side only | Every read and INSERT names its columns, so the column is copied and verified |
| Integer display width (`bigint(20)` vs `bigint`, `tinyint(1)` vs `tinyint`, a `zerofill` width) | Cosmetic: the copy and the hash read the value, not its padded text. `unsigned` and `zerofill` themselves must match |
| `year(4)` vs `year` | Cosmetic; normalised by the library |
| Column order differs | **Advisory**, one per table naming each displaced column and its source and destination positions. Every read and INSERT names its columns, so rows copy and verify normally |
| Column name differs only in ASCII letter case | **Advisory.** MySQL resolves column names case-insensitively and the copy names the source's columns, so rows copy and verify normally. Non-ASCII letters are compared exactly |

### Fatal — destination must not be stricter

| Difference | Consequence |
|------------|-------------|
| A column on one side only | Wrong column mapping |
| Column type differs (beyond display width) | Value corruption or insert failure |
| Destination `NOT NULL`, source nullable | NULL rows rejected mid-copy |
| Destination column generated, even when the source's is generated the same way | MySQL rejects an explicit value with error 3105 |
| Destination-only unique index | Rows the source holds as distinct collide, and the copy fails |
| Primary key differs | `DEST_KEY_IDENTITY_CHECK`'s, above |

CHECK constraints and foreign keys are not compared. A destination-only one that rejects a row
fails that batch's copy, which rolls back before anything is deleted (for foreign keys, see
[`disable_foreign_key_checks`](README_CONFIGURATION.md#disable_foreign_key_checks)).

**How destination unique indexes are compared.** By what they make unique, not by name: two
unique indexes are equivalent when they have the same set of parts, each `(column or expression,
prefix length, collation)`.

- **Prefix length counts:** `UNIQUE(email(10))` rejects addresses that `UNIQUE(email)` accepts.
- **Collation counts:** a case-insensitive destination collation collides rows a binary source
  collation keeps apart. A `utf8mb3` column copied into its `utf8mb4` twin compares as the
  source.
- **Ignored:** the index name, ASCII letter case of column names, part order and `DESC`.
- **Functional unique indexes** pass only when the expression matches a source one exactly and no
  column differs in charset or collation, since MySQL does not expose an expression's collation.

**Fix:** drop the destination unique index, or add an equivalent one to the source.

### Charset and collation

| Situation | Result |
|-----------|--------|
| Charset differs on a non-key column, `verification: count` or `none` | **Fatal** |
| Charset differs on a non-key column, `verification: sha256` | Warning |
| Collation differs on a column of a destination unique index | **Fatal** (unless the column is a `utf8mb4` twin) |
| Collation differs elsewhere outside the key | Warning |
| Charset or collation differs on a key column | `DEST_KEY_IDENTITY_CHECK` |
| Either side's character set is not supported | `COLUMN_CHARSET_CHECK`, earlier |

A charset change can transliterate or truncate text silently. Count verification proves which
keys arrived, not their values, so it cannot see that; sha256 can, and fails the batch before
anything is deleted.

---

## SHA256 verification

With `verification: sha256`, each batch is verified per table by a row count and one SHA-256
over that batch's rows, read in key order on the source and on the destination.

- Each row is hashed as `column=value` pairs sorted by column name.
- Text, DECIMAL, BIT and binary values hash as the bytes a `utf8mb4` session receives: the same
  text stored as latin1 and as utf8mb4 hashes equal.
- JSON hashes as MySQL's printed text. Its key order and spacing are MySQL's own, measured
  identical on 8.0.40, 8.4.11 and 9.7.1, but not guaranteed across releases. A member whose
  printed text changes in the copy (a DECIMAL's scale, a long DECIMAL, a double that an 8.0 or 8.4
  destination reads back differently) fails the batch, and the error names the table's JSON columns
  ([JSON values that do not survive a text copy](README_COMPAT.md#json-values-that-do-not-survive-a-text-copy)).
- `NULL` and an empty string hash differently.
- DATE, DATETIME and TIMESTAMP hash as MySQL's text for the value, with the column's fractional
  digits, in a UTC session.
- Column-type differences are the [schema compatibility check](#schema-compatibility-rules)'s
  job, not the hash's.

A mismatch rolls back the batch's copy and stops the run before anything is deleted; batches
already completed stay archived.

---

## Bypassing preflight

`--skip-dangerously-validate-preflight-allow-experimental` is accepted by `archive`, `purge`, `copy-only` and
`dry-run`, on the command line only: no configuration key sets it. A run prints a warning before
it starts:

```
WARNING: --skip-dangerously-validate-preflight-allow-experimental: preflight's policy checks are skipped (storage
engines, permissions, triggers, foreign-key coverage, destination compatibility, packet size).
The keys are still read from MySQL; a run still checks the job's log key and binding. A misspelled
foreign_key column fails as a statement-caps error instead of RELATION_KEY_CHECK.
```

Still enforced on a run: the [connection checks](#connection-checks), the
[Group Replication facts](README_GROUP_REPLICATION.md#what-is-read) and so
`SOURCE_GROUP_VIEW_PERMISSION_CHECK` (the identity and the report depend on them, so no flag skips
their reads), [`SOURCE_READ_ONLY_CHECK`](#source_read_only_check) and
[`DEST_READ_ONLY_CHECK`](#dest_read_only_check) (judged on the configurations those reads
return, and no flag skips them either), [`GROUP_MULTI_PRIMARY_CHECK`](#group_multi_primary_check) (judged on the
mode those reads return; `--allow-multi-primary` is its only bypass), `PRIMARY_KEY_CHECK`,
`KEY_TYPE_UNSUPPORTED`, `COLUMN_CHARSET_CHECK`, the statement caps (computed from the source keys
and, for archive and copy-only, the destination keys, which must be readable), the job's
[startup checks](README_JOBS_SCHEMA.md#startup-checks), and the run-time guards above. The JSON
warning is still printed. Every other check in
[the table](#which-checks-run-for-which-command) is skipped, `TABLE_EXISTENCE_CHECK`,
`PK_COLUMN_CHECK`, `RELATION_KEY_CHECK` and `FK_INDEX_CHECK` included. `dry-run` prints the same
warning, but it never reads the tracking tables, so it does not test the job's registration or
binding: a dry-run that passes does not promise the run will start. `validate` has no skip flag.

Use it only to recover from an incident, after verifying the schema by hand.

It is also the only way to run against an experimental destination engine, which GoArchive does
not test or support. A non-InnoDB destination table keeps its rows when a batch rolls back: a
failed batch leaves orphan rows there, and the rerun stops on their duplicate keys until they are
removed by hand. `MEMORY` loses the archive when the server restarts, after the source rows are
deleted. `BLACKHOLE` stores nothing, so every verification fails, unless it is `none`, which then
discards the rows. With `enforce_gtid_consistency = ON` on the destination, MySQL itself refuses
a non-transactional table's rows inside the copy transaction (error 1785). The old name,
`--skip-validate-preflight`, is refused.
