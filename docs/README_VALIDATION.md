# Validation & Preflight Reference

Every preflight check GoArchive runs, what it inspects, why it exists, which
commands run it, and how to fix a failure.

**Related:** [Configuration](README_CONFIGURATION.md) ·
[Permissions](README_PERMISSIONS.md) · [Operations](README_OPERATIONS.md) ·
[Limitations](README_LIMITATIONS.md) · [Testing](README_TESTING.md) ·
[← Back to README](../README.md)

---

## Contents

- [How preflight works](#how-preflight-works)
- [Which checks run for which command](#which-checks-run-for-which-command)
- [Source structure checks](#source-structure-checks)
- [Primary key checks](#primary-key-checks)
- [Destination checks](#destination-checks)
- [Foreign key checks](#foreign-key-checks)
- [Permission checks](#permission-checks)
- [Trigger checks](#trigger-checks)
- [Warnings](#warnings)
- [Schema compatibility rules](#schema-compatibility-rules)
- [SHA256 verification](#sha256-verification)
- [Dry-run payload validation](#dry-run-payload-validation)
- [Bypassing preflight](#bypassing-preflight)

---

## How preflight works

Preflight runs **before any state is written** and before any row is copied or
deleted. `archive`, `purge`, and `copy-only` run it automatically at startup, with their own
account; `dry-run` and `validate` exist to run it on demand. A prior `validate` result is
not carried forward to a later run.

A failure returns a named check identifier, a message, and usually the offending
tables:

```
preflight checks failed (run 'goarchive validate' for full diagnostics):
COMPOSITE_PK_CHECK: Composite primary keys are not supported. GoArchive
identifies and deletes rows by a single primary-key column; a multi-column PK
would over-match and risk deleting rows outside the archived set.
See README 'Known Limits & Caution' [film_actor(2-column PRIMARY KEY)]
```

Checks run in a fixed order and **abort at the first failure**, so fixing one
error can reveal the next. `goarchive validate` is the fastest way to iterate.

GoArchive selects one of three profiles per command:

| Profile | Used by | Skips |
|---------|---------|-------|
| Full | `archive`, `validate` | nothing |
| Source-only | `purge` | destination checks (nothing is copied) |
| Non-destructive | `copy-only`, `dry-run` | source delete permission, DELETE triggers, CASCADE warning |

`copy-only` additionally skips `FK_COVERAGE_VISIBILITY_CHECK`; `dry-run` runs it.

---

## Which checks run for which command

19 preflight checks, plus the connection-time `SRC_DEST_IDENTITY_CHECK` (20 rows).
✅ = enforced, ❌ = not run.

| Check | `archive` | `purge` | `copy-only` | `dry-run` | `validate` |
|-------|:---------:|:-------:|:-----------:|:---------:|:----------:|
| `SRC_DEST_IDENTITY_CHECK` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `TABLE_EXISTENCE_CHECK` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `PK_COLUMN_CHECK` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `PK_COLUMN_CASE_CHECK` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `COMPOSITE_PK_CHECK` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `PRIMARY_KEY_CHECK` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `ROOT_PK_TYPE_UNSUPPORTED` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `STORAGE_ENGINE_CHECK` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `JOB_SCHEMA_PERMISSION_CHECK` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `DEST_TABLE_EXISTENCE_CHECK` | ✅ | ❌ | ✅ | ✅ | ✅ |
| `DEST_SCHEMA_COMPATIBILITY_CHECK` | ✅ | ❌ | ✅ | ✅ | ✅ |
| `DEST_WRITE_PERMISSION_CHECK` | ✅ | ❌ | ✅ | ✅ | ✅ |
| `DEST_INSERT_TRIGGER_CHECK` | ✅ | ❌ | ✅ | ✅ | ✅ |
| `FK_INDEX_CHECK` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `FK_COVERAGE_VISIBILITY_CHECK` | ✅ | ✅ | **❌** | ✅ | ✅ |
| `FK_COVERAGE_CHECK` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `INTERNAL_FK_COVERAGE` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `SOURCE_SELECT_PERMISSION_CHECK` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `SOURCE_DELETE_PERMISSION_CHECK` | ✅ | ✅ | ❌ | ❌ | ✅ |
| `DELETE_TRIGGER_CHECK` | ✅ | ✅ | ❌ | ❌ | ✅ |

Notes:

- **`purge`** still runs `JOB_SCHEMA_PERMISSION_CHECK`, because it writes tracking state.
- **`copy-only` is exempt from `FK_COVERAGE_VISIBILITY_CHECK`** because it never
  deletes from source, so no external cascade can fire. It still runs
  `FK_COVERAGE_CHECK` — a `copy-only` run is still blocked by an uncovered
  cross-schema foreign key.
- **`SRC_DEST_IDENTITY_CHECK` is not a preflight check** — it runs at connection time
  ([below](#connection-identity-check)) — and is listed because it refuses a run the way a
  check does.

`DEST_*` checks additionally require a configured destination connection, which
every command that runs preflight has when `destination.database` is set.

---

## Connection identity check

### `SRC_DEST_IDENTITY_CHECK`

Source and destination report the same `server_uuid` and the same schema name
(compared without regard to letter case). GoArchive refuses at connection time,
before preflight and before any tracking row is written, and `--skip-validate-preflight`
does not reach it: an archive into itself
would copy nothing, verify the table against itself, and then delete the only
copy.

```
failed to connect to databases: SRC_DEST_IDENTITY_CHECK: source and destination
are the same database (server_uuid=…; source schema=app at db1:3306, destination
schema=app at db1:3306). GoArchive refuses: it would verify the table against
itself and delete the only copy. …
```

**Fix:** point `destination` at a different server or a different schema. If the
two schema names differ only in letter case, GoArchive treats them as one schema
on every server — rename one. If the two servers were cloned from one data
directory they share `auto.cnf`; the remedy is in
[Source and destination must be different databases](README_LIMITATIONS.md#source-and-destination-must-be-different-databases).

---

## Source structure checks

### `TABLE_EXISTENCE_CHECK`

Every table in the graph — root and all relations — must exist in the source
schema, **and must be a `BASE TABLE`**. Views, `SYSTEM VIEW`, and any other
`TABLE_TYPE` are rejected fail-closed — the copy/delete model is defined only for
real tables. The error names the object together with its observed type, e.g.
`orders(VIEW)`, so an operator can tell which object and why from the message
alone.

**Fix:** correct the table name in `relations`, or point `source.database` at the
right schema. Table names are matched as configured. If the object is a view,
archive the underlying table instead.

### `STORAGE_ENGINE_CHECK`

Every participating **source** table must use **InnoDB**. The check covers source tables
only; the destination engine is the operator's choice, and it matters: MyISAM, MEMORY and
ARCHIVE keep rows after `ROLLBACK`, so they void the per-batch rollback and keep `dry-run`'s
sample rows; BLACKHOLE stores nothing, so verification fails unless it is skipped.

**Fix:** `ALTER TABLE <table> ENGINE=InnoDB;`

**Anomalous metadata:** a `BASE TABLE` whose `ENGINE` is reported as `NULL` also
fails here, closed, named as `<table>(<unknown>)`. This is not a condition to
expect in normal operation — MySQL associates NULL-heavy
`information_schema.TABLES` rows with views (already excluded by
`TABLE_EXISTENCE_CHECK`), so a NULL `ENGINE` on a genuine `BASE TABLE` typically
indicates a corrupted or unknown-engine table.

---

## Primary key checks

Four checks guard the same invariant: GoArchive identifies, copies, verifies, and
**deletes** rows by a single primary-key column (`WHERE pk IN (...)`). Anything
that breaks the one-column-uniquely-identifies-one-row assumption risks deleting
rows outside the archived set.

They are reported separately because the fixes differ.

### `PK_COLUMN_CHECK`

The configured `primary_key` column does not exist in the source table at all, or
was never configured (there is no implicit default to `id`).

**Fix:** set `primary_key` to a real column name on that table.

### `PK_COLUMN_CASE_CHECK`

A column exists but its name differs **only in letter case** — e.g. configured
`log_id`, actual `LOG_ID`.

This has its own check because MySQL's `information_schema.COLUMNS.COLUMN_NAME`
collates case-insensitively (`utf8mb3_tolower_ci`), so a plain lookup would treat
the two as equal. GoArchive fetches the real column name and compares it in Go,
so a mere casing typo gets a clear "fix the casing" message instead of the
data-loss-flavoured `PRIMARY_KEY_CHECK`.

**Fix:** set `primary_key` to the exact case used in the schema.

### `COMPOSITE_PK_CHECK`

The table's `PRIMARY KEY` spans **more than one column**.

**Fix:** composite primary keys are not supported in Community edition. Remove
the table from the archive graph. See
[Limitations](README_LIMITATIONS.md#known-limits--caution).

### `PRIMARY_KEY_CHECK`

The table has **no `PRIMARY KEY`**, or the configured `primary_key` is a real
column that is **not** the table's `PRIMARY KEY`.

Either way the column is probably not unique, so deleting by it would over-match.

**Fix:** add a single-column `PRIMARY KEY`, or set `primary_key` to the column
that actually is the primary key.

### `ROOT_PK_TYPE_UNSUPPORTED`

The **root** table's primary key must be an integer type: `TINYINT`, `SMALLINT`,
`MEDIUMINT`, `INT`/`INTEGER`, or `BIGINT`, signed or unsigned. Checkpointing
advances a numeric high-water mark, which requires an ordered integer key.

UUID, `VARCHAR`, `DECIMAL`, `FLOAT`, and datetime root primary keys are rejected.
**Child tables may use single-column PK types**, subject to the
[temporal identity rules](README_LIMITATIONS.md#temporal-values-and-identities).

**Fix:** choose a different root table, or archive from a table with an integer
PK. (A related `ROOT_PK_TYPE_LOOKUP` error means the column's type could not be
read at all — usually a permissions or naming problem.)

---

## Destination checks

Skipped by `purge`, which copies nothing.

### `DEST_TABLE_EXISTENCE_CHECK`

Every participating table must also exist in the destination schema, with the
same name, **and must be a `BASE TABLE`**. Views, `SYSTEM VIEW`, and any other
`TABLE_TYPE` are rejected fail-closed, named with their observed type, e.g.
`orders(VIEW)`.

**Fix:** create the destination tables. A schema-only dump of the source is the
usual approach — see the note on
[`disable_foreign_key_checks`](README_CONFIGURATION.md#disable_foreign_key_checks)
if the destination's lookup tables will be empty. If the object is a view,
create a real table instead.

### `DEST_SCHEMA_COMPATIBILITY_CHECK`

Matches source and destination columns **by name** (ASCII letter case ignored, as MySQL
does), then compares each matched pair's attributes and its ordinal position. See
[Schema compatibility rules](#schema-compatibility-rules) for the full matrix.

### `DEST_INSERT_TRIGGER_CHECK`

Hard-fails if any destination table has an `INSERT` trigger.

A trigger firing during the copy would mutate archived data outside GoArchive's
knowledge, and the verification hash would then disagree with what was actually
stored.

**Fix:** drop or disable the destination INSERT triggers. There is **no** force
flag for this one.

---

## Foreign key checks

**Declared foreign keys are a floor, not a ceiling.** A relation does not need a foreign key in
the database: schemas whose relations live only in the application (ORM-managed) are archived
the same way, and you declare each relation in the config. When the checks that apply to a
command pass, they guarantee two things about the foreign keys the database **does** declare:
no table outside the graph references a graph table (`FK_COVERAGE_CHECK`), and every foreign
key between two different graph tables matches a relation (`INTERNAL_FK_COVERAGE`).
`FK_COVERAGE_VISIBILITY_CHECK` proves GoArchive saw all of them; `copy-only` skips that proof.
Self-referencing foreign keys are outside the guarantee
([`INTERNAL_FK_COVERAGE`](#internal_fk_coverage)). A relation with no declared foreign key is
checked by none of them, so its correctness and the index on its join column are yours: discovery
selects child rows by the join column, so without an index each discovery query scans the child
table.

### `FK_INDEX_CHECK`

Every declared foreign key on an **in-graph child table** must have its supporting index.

This is a defensive assertion: MySQL creates that index with every InnoDB foreign key and
refuses to drop it, so the check does not fire against a real server. It does not cover
relations without a declared foreign key. Out-of-graph children are
`FK_COVERAGE_CHECK`'s concern.

### `FK_COVERAGE_CHECK`

Finds tables **outside** the graph that hold a foreign key **referencing a table
inside** the graph, and fails.

Deleting an in-graph parent while an unmodelled child still references it either
fails outright (`RESTRICT` / `NO ACTION`) or silently mutates rows GoArchive never
copied (`CASCADE` / `SET NULL`). **Every ON DELETE rule is fatal** — the failure
mode differs, but none of them is safe.

The check inspects **incoming** foreign keys by *referenced* schema, so a
constraint defined in another schema that points at an in-graph table is
detected. A cross-schema child cannot be added to the graph
([identifier rules](README_CONFIGURATION.md#identifier-rules)), so such a foreign key is
always fatal.

Errors group by referenced table:

```
FK_COVERAGE_CHECK: Foreign key constraints not covered by relations
(fatal for any ON DELETE rule):
  - orders is referenced by: [analytics.order_snapshots (ON DELETE CASCADE)]
```

**Fix:** add the referencing table to `relations` if it is in the same schema and
should be archived; otherwise drop the constraint, or exclude the referenced
table from the archive.

### `INTERNAL_FK_COVERAGE`

Where `FK_COVERAGE_CHECK` looks outward, this looks **inward**: for foreign keys
where *both* tables are in the graph, the configured relation must match the real
constraint. It reports four distinct problems:

- **`[no graph edge]`** — the tables are related in the database but the config
  declares them as siblings rather than parent and child.
- **`[FK column mismatch]`** — `foreign_key` names a different column than the
  constraint uses.
- **`[reference column mismatch]`** — the parent's configured `primary_key` is
  not the column the constraint references.
- **`[multi-column foreign key]`** — a relation carries one `foreign_key`/`primary_key`
  pair, so a multi-column constraint between two graph tables cannot be modelled.

The first three produce a wrong delete order and MySQL Error 1451 at delete time.
A self-referencing foreign key (`category.parent_id → category.id`) is not a graph edge,
so this check does not examine it and no check rejects it; the model still cannot archive
such a hierarchy ([supported relationship types](README_LIMITATIONS.md#supported-relationship-types)).

**Fix:** nest child tables under their true parent, and make `foreign_key` and
`primary_key` match the real constraint. A multi-column foreign key between graph tables is
not supported: remove one of the two tables from the graph.

> Note the name: this is the one check without a `_CHECK` suffix.

### `FK_COVERAGE_VISIBILITY_CHECK`

Fails when GoArchive cannot prove it saw **every** foreign key pointing into the archive
graph — including ones defined in schemas the account has no privilege on. Without that
proof, an external `ON DELETE CASCADE` or `SET NULL` could delete or mutate rows that were
never copied.

**How completeness is proven.** Foreign-key discovery reads InnoDB's own metadata registry,
which requires the `PROCESS` privilege. A successful read is the proof, and the check
reports `complete`. If that read fails, discovery falls back to `information_schema` — which
only shows constraints on tables the account is privileged for — and the state becomes
`unconfirmed`; a visibility-filtered view cannot prove completeness. A state of `unknown`
means no completeness proof was populated at all. **Only `complete` passes**, and the error
names the state it saw.

For `unconfirmed`, GoArchive also reports which primary-source stage failed. A query-stage
failure can be caused by missing privileges, but also by connectivity, server state, or
another query error; inspect the error-level log before changing grants. A read-stage
failure means rows were returned but could not be scanned or decoded. Changing privileges
does not repair that case—inspect the logged cause and verify MySQL and `dbsgomysql`
compatibility. An absent or unrecognized reason remains a generic fail-closed diagnostic.
The raw MySQL error is logged once and is not copied into the structured preflight error.

**Fix**, when the logged query-stage cause is a privilege rejection: grant `PROCESS`, which
is accepted even when held through a role ([Permissions](README_PERMISSIONS.md#the-provable-grant-rule)).

**Applies to:** `archive`, `purge`, `dry-run`, `validate`. Skipped by `copy-only`.

---

## Permission checks

Four checks verify privileges: `DEST_WRITE_PERMISSION_CHECK`,
`SOURCE_SELECT_PERMISSION_CHECK`, `SOURCE_DELETE_PERMISSION_CHECK` and
`JOB_SCHEMA_PERMISSION_CHECK`.

Each passes only when the privilege is provable for the object
([the provable-grant rule](README_PERMISSIONS.md#the-provable-grant-rule)). Three states
fail:

| State | Meaning | Fix |
|---|---|---|
| absent | no grant exists at any scope | grant the privilege |
| unconfirmed | a grant may exist but cannot be proven — held through a role, or a global grant while `@@global.partial_revokes` is enabled | grant it **directly** to the account, at schema or table scope |
| unknown | the privilege fact was not populated | report it; this indicates an inspection problem, not a configuration one |

The error message names the state, so "absent" and "unconfirmed" are distinguishable
without re-reading the grant tables. **How it names the subject depends on the check's
scope**, and the difference is deliberate:

- **Table-scoped** checks (`DEST_WRITE`, `SOURCE_SELECT`, `SOURCE_DELETE`) report
  `TABLE(state)` — e.g. `orders(unconfirmed)`. The privilege is not repeated because each
  of these checks tests exactly one privilege, already named in its own message.
- **Schema-scoped** `JOB_SCHEMA_PERMISSION_CHECK` reports `PRIVILEGE(state)` — e.g.
  `CREATE(absent)` — because it tests four privileges against one schema.

Privilege introspection itself needs no extra grants — MySQL always exposes the
connected account's own privilege rows. Each check's fix is the matching
[grant recipe](README_PERMISSIONS.md#grant-recipes).

### `DEST_WRITE_PERMISSION_CHECK`

The destination account needs `INSERT` on every participating table. Verified up
front because otherwise the failure lands mid-run, after copy has already
committed rows. The check proves `INSERT` only; verification also needs destination
`SELECT`, which preflight does not check, so grant both
([recipe](README_PERMISSIONS.md#grant-recipes)).

### `SOURCE_SELECT_PERMISSION_CHECK`

Fails when the source account cannot be *proven* to hold `SELECT` on a participating table.
Every GoArchive command reads source rows or estimates from them, so this check runs for
all five commands, before any work starts.

**Note on a related failure:** if the account has *no* privilege at all on the source
schema, MySQL hides the schema from `information_schema` entirely and you will see
`TABLE_EXISTENCE_CHECK` instead. `SOURCE_SELECT_PERMISSION_CHECK` catches the partial
case — the account can see the tables but cannot read them.

### `SOURCE_DELETE_PERMISSION_CHECK`

The source account needs `DELETE` on every participating table. `dry-run` does **not** run
this check, even though it previews a delete; `validate` does, without deleting anything, so
it catches a missing grant before `archive` fails part-way through, after rows have already
been copied.

### `JOB_SCHEMA_PERMISSION_CHECK`

The destination account needs `CREATE`, `SELECT`, `INSERT`, and `UPDATE` on the
tracking schema (`destination.job_schema`, defaulting to the destination
database). `CREATE` is required **at runtime** because per-job log tables are
created on the fly.

Checked at global and schema scope only — there is no per-table fallback, because
the per-job tracking tables do not exist yet at preflight time.

The error names the exact grant needed, and prefixes `CREATE DATABASE` when the
schema itself is missing (GoArchive never creates it;
[`job_schema`](README_CONFIGURATION.md#job_schema-destination-only)):

```
JOB_SCHEMA_PERMISSION_CHECK: destination account lacks provable CREATE, UPDATE
on tracking schema "goarchive" (states: CREATE(absent), UPDATE(absent)).
GoArchive 2.0 requires each privilege to be provable for the object: grant each
missing privilege directly to the account at schema scope (DBA must:
CREATE DATABASE `goarchive`; GRANT CREATE, UPDATE ON `goarchive`.* TO <user>)
```

---

## Trigger checks

### `DELETE_TRIGGER_CHECK`

Fails if any source table has a `DELETE` trigger.

GoArchive has no visibility into trigger logic. A trigger that modifies other
tables when a row is deleted produces side effects outside the archive model and
outside verification.

**Override:** `--force-triggers`, accepted by `archive`, `purge`, and `validate`.
Triggers **will fire** during the delete phase. Audit what they do first.

`copy-only` and `dry-run` never run this check.

---

## Warnings

Not all findings are fatal.

- **`ON DELETE CASCADE` rules** — logged as a warning listing every cascading
  constraint found among graph tables. Cascades can delete related records
  automatically, outside GoArchive's ordering. Verify the behaviour is intended.
  Runs for `archive`, `purge`, and `validate`.
- **Collation mismatch** (outside a destination unique index), **charset mismatch** under a
  running sha256 verification, and **column names differing only in ASCII letter case** —
  see [Schema compatibility rules](#schema-compatibility-rules).
- **`disable_foreign_key_checks: true`** — a loud warning on every validate and
  every copy run.

---

## Error prefixes that are not checks

These prefixes can appear in preflight output. **They are not additional named checks — the
count remains 19 preflight checks** — and they do not indicate a configuration problem.

| Prefix | Meaning | What to do |
|---|---|---|
| `PREFLIGHT_INSPECTION_INTEGRITY` | An inspection result was internally inconsistent — a fact that must be populated was not, or was captured for only one side of a comparison | Report it |
| `PREFLIGHT_UNEXPECTED_FACTS` | A recognised check arrived carrying a payload of the wrong type | Report it |
| `PREFLIGHT_UNKNOWN_FINDING` | A validation check GoArchive does not recognise was returned | Report it |
| `PREFLIGHT_UNEXPECTED_DIFF` | A schema difference was reported that GoArchive's inspection cannot produce | Report it |
| `PREFLIGHT_UNKNOWN_DIFF` | A schema difference kind GoArchive does not recognise | Report it |

The five above signal a **contract failure between GoArchive and its validation library**,
not a fault in your schema or grants. They fail closed deliberately: GoArchive aborts rather
than guess at a result it cannot interpret. Please report them with the full message.

`COMPOSITE_PK_LOOKUP` and `ROOT_PK_TYPE_LOOKUP` are different: they **wrap an underlying
inspection or database failure** — a lost connection, a permissions problem, a query error.
The wrapped cause is included in the message; diagnose that. They do not by themselves
indicate a software defect.

`AUTO_INCREMENT_ZERO_MODE_CHECK` is a **connection-startup failure**, before preflight.
Every new GoArchive connection — source, destination and replication server, including
replacement connections — adds `NO_AUTO_VALUE_ON_ZERO` to its inherited session SQL modes, so
an explicit zero in an AUTO_INCREMENT column stays zero while omitted or NULL values are still
allocated; other modes remain intact. The failure means GoArchive could not prove the session
does this.
The run stops before data processing. Check whether the server or proxy honors connection
initialization, then restart.
If the driver's connection-initialization statement itself is rejected, its error can appear
before this named assertion runs. Otherwise, diagnose any wrapped query cause in the named
error. This is not an additional preflight check and `--skip-validate-preflight` does not
disable it.

Runtime value-preservation errors are also separate from the preflight check count:

| Identifier | Meaning | Where it is explained |
|---|---|---|
| `TEMPORAL_READ_CONTRACT` | A temporal representation, metadata fact, key identity set, or pre-delete proof could not be established | [Temporal identity rules](README_LIMITATIONS.md#temporal-values-and-identities) |
| `INSERT_DIAGNOSTIC_REJECTED` | The INSERT read a forbidden condition; the message carries its code, level and a bounded preview | [INSERT diagnostics](README_OPERATIONS.md#insert-diagnostics-and-subdivision) |
| `INSERT_DIAGNOSTICS_UNPROVEN` | Session facts, diagnostic I/O, completeness or execution context could not be proved | [INSERT diagnostics](README_OPERATIONS.md#insert-diagnostics-and-subdivision) |

These guards are application-owned and remain active with skipped preflight.
`validate` is structural; it is not a scan or certification of every payload.
None of these failures grants permission to delete.

---

## Schema compatibility rules

`DEST_SCHEMA_COMPATIBILITY_CHECK` is **direction-aware**, not byte-identical. The
destination may be *looser* than the source, never *stricter*.

The rationale: the copy inserts explicit values for every column and never relies
on destination defaults or indexes — so relaxations are harmless, while extra
constraints would reject or silently skip rows.

### Allowed — destination may be looser

| Difference | Why it is safe |
|------------|----------------|
| Secondary indexes dropped (`MUL`, `UNI` → none) | Copy does not read them. **A supported write-performance optimization.** |
| `auto_increment` dropped | Copy supplies explicit key values |
| Column defaults dropped (`DEFAULT_GENERATED`, `ON UPDATE`) | Copy supplies explicit values |
| `NOT NULL` relaxed to nullable | Strictly more permissive |
| Source column generated, destination plain | `SELECT` materialises the value; a plain column accepts it |
| Column `INVISIBLE` on one side, visible on the other | Visibility never reaches the copy: every read and every `INSERT` names its columns explicitly rather than using `SELECT *`, so an `INVISIBLE` column is copied, verified, and hashed like any other. Dry-run payload sampling names them too. |
| Integer display width differs (`bigint(20)` vs `bigint`) | Cosmetic. Normalised away — MySQL 8.0.17 and later do not report it, so a schema dumped from an older server would otherwise false-fail. `tinyint(1)` vs `tinyint` is accepted too: the library reports it as a difference, and GoArchive's policy accepts it. `unsigned` and the `zerofill` attribute **are** preserved — they change the value range and rendering — but a `zerofill` column's *display width* (`int(5) zerofill` vs `int(10) zerofill`) is accepted: it changes only how the server pads the value as text; the copy and the hash read the value, not the text. |
| Column name differs only in ASCII letter case (`email` vs `Email`), including the primary-key column | **Advisory, not fatal.** MySQL resolves column names case-insensitively, the copy names *source* columns, and the verifier reads both sides with that same list — so rows copy and verify normally. The warning names both spellings; align them if the archive should be a byte-faithful copy of the source schema. Non-ASCII letters are compared exactly. |
| `year(4)` vs `year` | Cosmetic; normalised away by the library (dbsgomysql 1.1.3+). MySQL 8.4 drops the width at `CREATE` time, so the shape survives only in an in-place-upgraded data dictionary. |

### Fatal — destination must not be stricter

| Difference | Consequence |
|------------|-------------|
| A column present on one side only (names differing beyond ASCII letter case) | Wrong column mapping; the first such column per table is reported |
| Column type mismatch (after width normalisation) | Value corruption or insert failure |
| Column **order** mismatch | Policy: the destination must keep the source's column order. Columns are matched by name, so this is reported on its own rather than as a cascade of type mismatches. |
| Destination `NOT NULL`, source nullable | NULL rows rejected mid-copy |
| Primary key present on one side only | `INSERT IGNORE` crash-recovery idempotency depends on it |
| Primary key column list differs (column, key order, or prefix length) | `INSERT IGNORE` crash-recovery idempotency depends on the destination rejecting a re-inserted row by the same key the source identified it with. ASCII letter case of the key columns is ignored; key direction (`DESC`) is ignored. |
| Destination-only unique index | `INSERT IGNORE` would **silently skip rows** |
| Destination column is generated | MySQL rejects explicit inserts with Error 3105, even under `INSERT IGNORE` |

**How destination unique indexes are compared.** GoArchive compares **uniqueness
predicates**, not index names. Two unique indexes are equivalent when they have the same set
of key parts, where a part is `(column or expression, prefix length, column collation)`:

- **Prefix length is part of the predicate.** `UNIQUE(email)` and `UNIQUE(email(10))` are
  different: the second rejects two addresses sharing a 10-character prefix.
- **Column collation is part of the predicate.** The same `UNIQUE(email)` under a
  case-insensitive destination collation collides rows that a binary source collation keeps
  distinct.
- **The index name is ignored**, so renaming an equivalent index never fails.
- **Column-name ASCII case is ignored**: MySQL resolves column names case-insensitively, so
  `UNIQUE(email)` and `UNIQUE(Email)` enforce the same predicate. Non-ASCII letters are
  compared exactly.
- **Key order and `DESC` are ignored**: `UNIQUE(a,b)` and `UNIQUE(b,a)` enforce the same row
  uniqueness.
- **Functional unique indexes** (`UNIQUE((lower(email)))`) are accepted only when the
  expression text matches a source functional unique exactly **and** the table has no column
  charset or collation difference. MySQL does not expose an expression's result collation,
  so an identical column environment is the only available proof that the expressions
  compare values the same way.

**Fix:** drop the destination unique index.

### Charset and collation

| Situation | Result |
|-----------|--------|
| Charset differs, `verification.method: count` | **Fatal** |
| Charset differs, verification skipped | **Fatal** |
| Charset differs, `verification.method: sha256` and running | Warning |
| Collation differs on a column in a destination unique index | **Fatal**, regardless of verification method |
| Collation differs, column not in a destination unique index | Warning |

Count verification proves that primary keys arrived — not that text survived
intact. A charset mismatch can silently transliterate or truncate values, and
count verification cannot see it. SHA256 can, and fails before any delete.

---

## SHA256 verification

With `verification.method: sha256`, each batch is verified by one SHA-256 and a row count per
participating table, computed over that batch's primary keys on the source and on the
destination.

- Each row is hashed as `column=value` pairs sorted by column name.
- Text, DECIMAL, JSON, BIT and binary values hash as the bytes a utf8mb4 session receives: the
  same text stored as latin1 and as utf8mb4 hashes equal, and JSON key order and spacing do not
  matter (MySQL normalizes JSON).
- `NULL` and an empty string hash differently.
- DATE, DATETIME and TIMESTAMP hash as MySQL's text for the value, with the column's
  fractional digits, in a UTC session.
- Column-type differences (FLOAT vs DOUBLE, DECIMAL scale, fractional precision) are the
  [schema compatibility check](#schema-compatibility-rules)'s job, not the hash's.

A mismatch stops that batch before its delete; batches already completed stay archived.

---

## Dry-run payload validation

`goarchive dry-run` runs the non-destructive preflight profile and adds checks
that only matter for a real copy. It prints the job's WHERE clause and estimates
row counts **filtered through the actual relation chain**, not full-table counts.
`dry-run` embeds the `where` exactly as the run does, so a `where` the run would
reject fails in `dry-run` first, with the same MySQL error.

Samples use raw temporal projections and go through the runtime copy's
[diagnostic collection and subdivision](README_OPERATIONS.md#insert-diagnostics-and-subdivision)
inside one transaction that always ends in `ROLLBACK`. The rollback undoes the sample only on
a transactional (InnoDB) destination; MyISAM, MEMORY and ARCHIVE keep the rows
([`STORAGE_ENGINE_CHECK`](#storage_engine_check)). Complete duplicate-only diagnostics are
accepted solely because of that rollback, and the notice says so: the sample
cannot prove destination equality or readiness for every job row. Sample observations are
reported separately from committed-copy totals. Payload and placeholder estimates are based
on the configured batch, not the smaller executed INSERTs.

### Placeholder check — exact

`batch_size × column_count` must be below MySQL's **65,535** prepared-statement
placeholder limit, per table.

This runs even for empty tables, so a wide table is caught before it holds data.

### `max_allowed_packet` check — measured

The dry-run copies a `batch_size`-sized sample into a destination transaction and
**immediately rolls it back**, which persists nothing on an InnoDB destination. If a table's row width
exceeds the packet limit it fails fast and tells you to lower `batch_size`.

> The packet check is **approximate for child tables**: child rows are sampled
> arbitrarily rather than through full BFS discovery, which would be too expensive
> for a dry-run. The placeholder check is exact for every table.

If you skip dry-run and `batch_size` is too large, the real run fails on the first
copy chunk. Already-processed root PKs stay checkpointed and the interrupted
batch replays automatically after you lower `batch_size`.

---

## Bypassing preflight

`--skip-validate-preflight` is accepted by `archive`, `purge`, and `copy-only`.
**It is dangerous** and prints a full-width banner:

```
================================================================
  WARNING: --skip-validate-preflight is set
  Preflight checks will NOT run before this destructive operation.

  This is unsafe. Continue only if you are recovering from an
  incident and have manually verified schema integrity.
================================================================
```

Running `archive` with `verification.method: count` and skipped preflight prints
a second banner, because that combination is the most dangerous one available:
count verification proves PK presence, not row equality, so with schema
compatibility unverified, archive can DELETE source rows after copying them into
incompatible destination columns.

`dry-run` and `validate` have **no skip flag**. `validate` runs every check; `dry-run` runs
the non-destructive profile above. Both enforce `FK_COVERAGE_VISIBILITY_CHECK`.

Use the flag only for documented recovery scenarios, after manually verifying
schema safety.
