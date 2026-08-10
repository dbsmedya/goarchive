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
- [Dry-run payload validation](#dry-run-payload-validation)
- [Bypassing preflight](#bypassing-preflight)

---

## How preflight works

Preflight runs **before any state is written** and before any row is copied or
deleted. `archive`, `purge`, and `copy-only` run it automatically at startup;
`dry-run` and `validate` exist to run it on demand.

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

---

## Which checks run for which command

20 named checks exist. ✅ = enforced, ❌ = not run.

| Check | `archive` | `purge` | `copy-only` | `dry-run` | `validate` |
|-------|:---------:|:-------:|:-----------:|:---------:|:----------:|
| `TABLE_EXISTENCE_CHECK` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `PK_COLUMN_CHECK` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `PK_COLUMN_CASE_CHECK` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `COMPOSITE_PK_CHECK` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `PRIMARY_KEY_CHECK` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `ROOT_PK_TYPE_UNSUPPORTED` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `STORAGE_ENGINE_CHECK` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `INVISIBLE_COLUMN_CHECK` | ✅ | ✅ | ✅ | ✅ | ✅ |
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

Notes on the two asymmetries:

- **`purge` skips destination checks** because it never copies anything. It still
  runs `JOB_SCHEMA_PERMISSION_CHECK`, because it writes tracking state.
- **`copy-only` is exempt from `FK_COVERAGE_VISIBILITY_CHECK`** because it never
  deletes from source, so no external cascade can fire. It still runs
  `FK_COVERAGE_CHECK` — a `copy-only` run is still blocked by an uncovered
  cross-schema foreign key.

`DEST_*` checks additionally require a configured destination connection, which
every command that runs preflight has when `destination.database` is set.

---

## Source structure checks

### `TABLE_EXISTENCE_CHECK`

Every table in the graph — root and all relations — must exist in the source
schema, **and must be a `BASE TABLE`**. Views, `SYSTEM VIEW`, and any other
`TABLE_TYPE` are rejected fail-closed — the copy/delete model is defined only for
real tables. The error names the object together with its observed type, e.g.
`orders(VIEW)`, so an operator can tell which object and why from the message
alone. (A source view previously passed this check by name only and failed later
at `PRIMARY_KEY_CHECK`, since a view has no primary key; it now fails here with
the accurate reason.)

**Fix:** correct the table name in `relations`, or point `source.database` at the
right schema. Table names are matched as configured. If the object is a view,
archive the underlying table instead.

### `STORAGE_ENGINE_CHECK`

Every participating table must use **InnoDB**.

MyISAM and other non-transactional engines cannot provide the transactional
integrity the copy-verify-delete cycle depends on.

**Fix:** `ALTER TABLE <table> ENGINE=InnoDB;`

**Anomalous metadata:** a `BASE TABLE` whose `ENGINE` is reported as `NULL` also
fails here, closed, named as `<table>(<unknown>)`. This is not a condition to
expect in normal operation — MySQL associates NULL-heavy
`information_schema.TABLES` rows with views (already excluded by
`TABLE_EXISTENCE_CHECK`), so a NULL `ENGINE` on a genuine `BASE TABLE` typically
indicates a corrupted or unknown-engine table. (Previously this aborted preflight
with a raw `database/sql` scan error naming neither a check nor a table.)

### `INVISIBLE_COLUMN_CHECK`

Hard-fails if any participating table has an `INVISIBLE` column.

GoArchive copies rows with `SELECT *`, which MySQL **omits invisible columns
from**. Their stored values would be silently dropped from both the destination
INSERT and the verification hash, and then deleted from the source — silent data
loss. Detected via `information_schema.COLUMNS.EXTRA`, which catches both plain
`INVISIBLE` and `STORED GENERATED INVISIBLE`.

**Fix:** `ALTER TABLE <table> ALTER COLUMN <col> SET VISIBLE;` or remove the
table from the archive until explicit-column support exists.

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

A composite PK cannot be filtered by a single column without over-matching.

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
**Child tables may use any single-column PK type.**

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
`orders(VIEW)`. (A destination view previously passed this check by name only
and failed later at `DEST_SCHEMA_COMPATIBILITY_CHECK` as a structural mismatch;
it now fails here with the accurate reason.)

**Fix:** create the destination tables. A schema-only dump of the source is the
usual approach — see the note on
[`disable_foreign_key_checks`](README_CONFIGURATION.md#disable_foreign_key_checks)
if the destination's lookup tables will be empty. If the object is a view,
create a real table instead.

### `DEST_SCHEMA_COMPATIBILITY_CHECK`

Compares source and destination column-by-column, in ordinal order. See
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

### `FK_INDEX_CHECK`

Every foreign key column on an **in-graph child table** must be indexed.

An unindexed FK column makes each delete a full table scan, which is the single
most common cause of an archive run that never finishes. Out-of-graph children
are deliberately not flagged here — they are `FK_COVERAGE_CHECK`'s problem, which
is the more actionable error.

**Fix:** `CREATE INDEX idx_fk ON <table>(<column>);`

### `FK_COVERAGE_CHECK`

Finds tables **outside** the graph that hold a foreign key **referencing a table
inside** the graph, and fails.

Deleting an in-graph parent while an unmodelled child still references it either
fails outright (`RESTRICT` / `NO ACTION`) or silently mutates rows GoArchive never
copied (`CASCADE` / `SET NULL`). **Every ON DELETE rule is fatal** — the failure
mode differs, but none of them is safe.

The check inspects **incoming** foreign keys by *referenced* schema, so a
constraint defined in another schema that points at an in-graph table is
detected. Because relation identifiers cannot express `schema.table`, a
cross-schema child cannot be added to the graph — such a foreign key is always
fatal.

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
constraint. It reports three distinct problems:

- **`[no graph edge]`** — the tables are related in the database but the config
  declares them as siblings rather than parent and child.
- **`[FK column mismatch]`** — `foreign_key` names a different column than the
  constraint uses.
- **`[reference column mismatch]`** — the parent's configured `primary_key` is
  not the column the constraint references.

Any of these produces a wrong delete order and MySQL Error 1451 at delete time.
Self-referencing foreign keys (`category.parent_id → category.id`) are skipped.

**Fix:** nest child tables under their true parent, and make `foreign_key` and
`primary_key` match the real constraint.

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

**Grant, when the logged query-stage cause is a privilege rejection:**

```sql
GRANT PROCESS ON *.* TO '<user>'@'<host>';
```

`PROCESS` has no narrower scope in MySQL.

**A role-held `PROCESS` is accepted.** This differs from the DML permission checks, and the
reason is the kind of evidence available: here the proof is that the *statement succeeded*,
which a role-held privilege produces identically. For DML privileges the proof would have to
come from grant tables the account cannot read for its own roles.

**Applies to:** `archive`, `purge`, `dry-run`, `validate`.
**Skipped by `copy-only`**, which never issues a source `DELETE`, so no external cascade can
fire. `copy-only` still runs `FK_COVERAGE_CHECK`: an uncovered incoming foreign key is a
graph-modelling error regardless.

---

## Permission checks

Four checks verify privileges: `DEST_WRITE_PERMISSION_CHECK`,
`SOURCE_SELECT_PERMISSION_CHECK`, `SOURCE_DELETE_PERMISSION_CHECK` and
`JOB_SCHEMA_PERMISSION_CHECK`.

**The privilege must be provable for the object.** GoArchive 2.0 passes a permission check
only when the privilege is *established* for the specific schema or table. Three states
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

**Roles.** A privilege held only through an active role is reported *unconfirmed*. MySQL
does not expose a role's grant rows to the account that holds the role, so GoArchive cannot
prove the privilege — and a check that cannot prove what it claims is worse than no check.
Grant DML privileges directly to the account GoArchive connects as. (The FK metadata
visibility check is different: see `FK_COVERAGE_VISIBILITY_CHECK`.)

Privilege introspection itself needs no extra grants — MySQL always exposes the
connected account's own privilege rows.

### `DEST_WRITE_PERMISSION_CHECK`

The destination account needs `INSERT` on every participating table. Verified up
front because otherwise the failure lands mid-run, after copy has already
committed rows. See [Permission checks](#permission-checks) above for the
absent/unconfirmed/unknown states; the offending tables are reported as
`TABLE(state)`.

**Fix:**

```sql
GRANT INSERT ON `<destination_schema>`.* TO '<user>'@'<host>';
```

### `SOURCE_SELECT_PERMISSION_CHECK`

**New in 2.0.** Fails when the source account cannot be *proven* to hold `SELECT` on a
participating table.

Every GoArchive command reads source rows or estimates from them, so this check runs for
all five commands — unlike `SOURCE_DELETE_PERMISSION_CHECK`, which runs only for
`archive`, `purge`, and `validate`.

GoArchive 1.8 never validated source read permission. An account holding `PROCESS` and
`DELETE` but not `SELECT` passed preflight and then failed part-way through the run,
after the tracking row and per-job log table had already been created. 2.0 catches it
before any work starts.

**The privilege must be provable for the object.** A grant held only through an active
role, or a bare global grant while `@@global.partial_revokes` is enabled, is reported as
*unconfirmed* and fails closed. See [Permissions](README_PERMISSIONS.md) for the
recommended grant recipe.

**Fix:**

```sql
GRANT SELECT ON `<source_schema>`.* TO '<user>'@'<host>';
```

**Note on a related failure:** if the account has *no* privilege at all on the source
schema, MySQL hides the schema from `information_schema` entirely and you will see
`TABLE_EXISTENCE_CHECK` instead. `SOURCE_SELECT_PERMISSION_CHECK` catches the partial
case — the account can see the tables but cannot read them.

### `SOURCE_DELETE_PERMISSION_CHECK`

The source account needs `DELETE` on every participating table. Runs for
`archive`, `purge`, and `validate`. `dry-run` does **not** run this check, even
though it previews a delete — "runs the check" and "deletes or previews a
delete" are not the same set. `validate` enforces the privilege without
deleting anything, which is the point: it catches a missing grant before
`archive` fails part-way through, after rows have already been copied. See
[Permission checks](#permission-checks) above for the absent/unconfirmed/unknown
states; the offending tables are reported as `TABLE(state)`.

**Fix:**

```sql
GRANT DELETE ON `<source_schema>`.* TO '<user>'@'<host>';
```

### `JOB_SCHEMA_PERMISSION_CHECK`

The destination account needs `CREATE`, `SELECT`, `INSERT`, and `UPDATE` on the
tracking schema (`destination.job_schema`, defaulting to the destination
database). `CREATE` is required **at runtime** because per-job log tables are
created on the fly. See [Permission checks](#permission-checks) above for the
absent/unconfirmed/unknown states; the offending privileges are reported as
`PRIVILEGE(state)`.

Checked at global and schema scope only — there is no per-table fallback, because
the per-job tracking tables do not exist yet at preflight time.

The error names the exact grant needed, and prefixes `CREATE DATABASE` when the
schema itself is missing:

```
JOB_SCHEMA_PERMISSION_CHECK: destination account lacks provable CREATE, UPDATE
on tracking schema "goarchive" (states: CREATE(absent), UPDATE(absent)).
GoArchive 2.0 requires each privilege to be provable for the object: grant each
missing privilege directly to the account at schema scope (DBA must:
CREATE DATABASE `goarchive`; GRANT CREATE, UPDATE ON `goarchive`.* TO <user>)
```

**Fix:**

```sql
GRANT CREATE, SELECT, INSERT, UPDATE ON `<job_schema>`.* TO '<user>'@'<host>';
```

See [Permissions](README_PERMISSIONS.md) for full grant recipes.

---

## Trigger checks

### `DELETE_TRIGGER_CHECK`

Fails if any source table has a `DELETE` trigger.

GoArchive has no visibility into trigger logic. A trigger that modifies other
tables when a row is deleted produces side effects outside the archive model and
outside verification.

**Override:** `--force-triggers`, accepted by `archive`, `purge`, and `validate`.
Triggers **will fire** during the delete phase. Audit what they do first.

Sakila's `del_film` trigger is why the E2E suite passes this flag.

`copy-only` never runs this check — it does not delete from source.

---

## Warnings

Not all findings are fatal.

- **`ON DELETE CASCADE` rules** — logged as a warning listing every cascading
  constraint found among graph tables. Cascades can delete related records
  automatically, outside GoArchive's ordering. Verify the behaviour is intended.
  Runs for `archive`, `purge`, and `validate`.
- **Collation mismatch** — a warning, except on a column participating in a
  destination unique index, where D3 makes it fatal (see
  [Schema compatibility rules](#schema-compatibility-rules)): a looser
  destination collation can collide rows the source's index kept distinct.
- **Charset mismatch under a running sha256 verification** — a warning rather
  than an error, because the hash comparison fails closed before any delete.
- **`disable_foreign_key_checks: true`** — a loud warning on every validate and
  every copy run.

---

## Error prefixes that are not checks

These prefixes can appear in preflight output. **They are not additional named checks — the
published count remains 20** — and they do not indicate a configuration problem.

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
| Integer display width differs (`bigint(20)` vs `bigint`) | Cosmetic. Normalised away — MySQL 8.0.17+ no longer reports it, so a schema dumped from an older server would otherwise false-fail. `unsigned`/`zerofill` **are** preserved: they change the value range. |

### Fatal — destination must not be stricter

| Difference | Consequence |
|------------|-------------|
| Column name mismatch | Wrong column mapping |
| Column type mismatch (after width normalisation) | Value corruption or insert failure |
| Column **count** mismatch | Reported before per-column comparison |
| Column **order** mismatch | Columns compare by ordinal position |
| Destination `NOT NULL`, source nullable | NULL rows rejected mid-copy |
| Primary key present on one side only | `INSERT IGNORE` crash-recovery idempotency depends on it |
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
- **Key order and `DESC` are ignored**: `UNIQUE(a,b)` and `UNIQUE(b,a)` enforce the same row
  uniqueness.
- **Functional unique indexes** (`UNIQUE((lower(email)))`) are accepted only when the
  expression text matches a source functional unique exactly **and** the table has no column
  charset or collation difference. MySQL does not expose an expression's result collation,
  so an identical column environment is the only available proof that the expressions
  compare values the same way.

**Fix:** drop the destination unique index. Dropping destination secondary indexes is a
supported write-performance optimization — the copy inserts explicit values and relies on no
destination index except the primary key.

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

## Dry-run payload validation

`goarchive dry-run` runs the non-destructive preflight profile and adds checks
that only matter for a real copy. It prints the job's WHERE clause and estimates
row counts **filtered through the actual relation chain**, not full-table counts.

### Placeholder check — exact

`batch_size × column_count` must be below MySQL's **65,535** prepared-statement
placeholder limit, per table.

This runs even for empty tables, so a wide table is caught before it holds data.

### `max_allowed_packet` check — measured

The dry-run copies a `batch_size`-sized sample into a destination transaction and
**immediately rolls it back**. Nothing is persisted. If a table's row width
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

`dry-run` and `validate` have **no skip flag** and always enforce every check
including `FK_COVERAGE_VISIBILITY_CHECK`.

Use the flag only for documented recovery scenarios, after manually verifying
schema safety.
