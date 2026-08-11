# Upgrading to GoArchive 2.0

GoArchive 2.0 re-platforms its preflight validation onto
[dbsgomysql](https://github.com/dbsmedya/dbsgomysql). The **19 check IDs from 1.8 keep their
names and meanings**, **one check ID is added**, and **six behaviours change deliberately** —
**four** of which may require you to do something, and **two** of which only improve how a
failure is diagnosed. Nothing about archiving, copying, deleting, checkpointing, or resuming
changes.

**Read this if** you are upgrading from 1.8 and your preflight passes today. If you are a
new user, you do not need this document — [Permissions](README_PERMISSIONS.md) and
[Validation](README_VALIDATION.md) describe the software as it is.

## The short version

Grant your source account `PROCESS`, make sure every privilege GoArchive needs is granted
**directly** to the connecting account rather than through a role, and check whether your
destination tables carry unique indexes your source tables do not.

```sql
-- Source account
GRANT PROCESS, SELECT ON *.* TO '<user>'@'<host>';
-- Required by archive, purge, and validate preflight; only archive and purge delete rows.
GRANT DELETE ON `<source_schema>`.* TO '<user>'@'<host>';

-- Destination account
GRANT SELECT, INSERT ON `<destination_schema>`.* TO '<user>'@'<host>';
GRANT CREATE, SELECT, INSERT, UPDATE ON `<job_schema>`.* TO '<user>'@'<host>';
```

> The `DELETE` grant is **not** "archive and purge only". `validate` enforces
> `SOURCE_DELETE_PERMISSION_CHECK` without deleting anything, so an account missing the
> grant fails the very `validate` this document tells you to run first. `dry-run` and
> `copy-only` do not run that check.

Then run `goarchive validate` before your first 2.0 archive. It runs every check and
changes nothing.

## Four changes that may require action

### 1. Foreign-key visibility now requires `PROCESS`

**Symptom**

```
FK_COVERAGE_VISIBILITY_CHECK: foreign-key metadata completeness is not established
(state: unconfirmed), so GoArchive cannot verify there are no foreign keys in other
schemas referencing the archive graph ...
```

**What changed.** 1.8 accepted a global `SELECT` privilege as proof that it could see every
foreign key in every schema. 2.0 proves it by reading InnoDB's own foreign-key metadata
registry, which requires `PROCESS`.

**Why.** The 1.8 reasoning was wrong in a way that could lose data. With
`@@global.partial_revokes` enabled, a global `SELECT` grant does **not** guarantee every
schema is readable — a revoked schema stays invisible, including any foreign key defined
in it that points into your archive graph. An external `ON DELETE CASCADE` from such a
table would delete rows GoArchive never copied. 1.8 passed those configurations.

**Diagnosis and fix.** The error now distinguishes a failure to start the primary query
from a failure while reading its returned rows. A query-stage failure is not automatically
a privilege failure: inspect the error-level log for the retained MySQL cause. A read-stage
failure is a decoding/compatibility problem, and changing grants will not repair it.

When the logged query-stage cause is a privilege rejection, grant:

```sql
GRANT PROCESS ON *.* TO '<user>'@'<host>';
```

`PROCESS` has no narrower scope in MySQL. A `PROCESS` held through an active role is
accepted — see [Why roles behave differently here](#why-roles-behave-differently-for-process).
GoArchive logs the primary failure once for diagnosis but does not copy raw driver text into
the structured preflight error.

**Not affected:** `copy-only`, which never deletes from the source and is exempt from this
check (unchanged from 1.8).

### 2. Permission checks require a provable grant

**Symptom**

```
SOURCE_DELETE_PERMISSION_CHECK: Source account lacks provable DELETE privilege on
required tables ... (tables: [orders(unconfirmed)])
```

Note the state in parentheses. `absent` means no grant exists at any scope. `unconfirmed`
means a grant may exist but GoArchive cannot prove it applies to that object. A third state,
`unknown`, means the privilege fact was not populated at all — completeness could not be
established. Only a proven grant passes; all three failing states fail closed.

**The message no longer names the account.** 1.8 printed the resolved grantee
(``Source account `svc`@`%` lacks …``); 2.0 prints only "Source account", because the
privilege fact does not expose the identity it resolved and GoArchive deliberately does
not issue a second `SELECT CURRENT_USER()` to re-acquire something already observed. If
you run several accounts against the same host, identify the connecting one yourself:

```sql
SELECT CURRENT_USER(), CURRENT_ROLE();
```

`CURRENT_USER()` is the account whose grants must prove the object. `CURRENT_ROLE()` is
the most common reason a privilege you believe you granted reports `unconfirmed` — a
privilege reachable only through an active role is not provable and will fail closed.
Roles are not the only cause: partial revokes and wildcard schema grants also produce
`unconfirmed`, which is why the check's own message names no single cause.

**What changed.** 1.8 resolved the account's active roles and matched them against the
privilege tables, and treated a global grant as covering everything. 2.0 passes only when
the privilege is *established* for the specific schema or table.

**Why.** Two problems with the 1.8 approach:

- **Roles.** MySQL does not expose a role's grant rows to the account that holds the role.
  1.8's role resolution appeared to work, but it was only ever exercised as `root` — an
  account privileged enough to read *other* identities' grants. An ordinary account cannot,
  so the check was claiming a proof it did not have. Nested roles were never handled at all.
- **Partial revokes.** A global privilege row proves nothing about a specific schema once
  `@@global.partial_revokes` is enabled. 1.8 short-circuited on the global row and passed.

**Fix.** Grant DML privileges directly to the account GoArchive connects as:

```sql
GRANT DELETE ON `<source_schema>`.* TO '<user>'@'<host>';
GRANT SELECT, INSERT ON `<destination_schema>`.* TO '<user>'@'<host>';
GRANT CREATE, SELECT, INSERT, UPDATE ON `<job_schema>`.* TO '<user>'@'<host>';
```

Table-level grants work too. What does not work is relying on a role, or on a bare global
grant while partial revokes are enabled.

**Affects:** `SOURCE_DELETE_PERMISSION_CHECK`, `DEST_WRITE_PERMISSION_CHECK`,
`JOB_SCHEMA_PERMISSION_CHECK`, and the new `SOURCE_SELECT_PERMISSION_CHECK`.

### Why roles behave differently for `PROCESS`

`PROCESS` held through a role is accepted; `DELETE` held through a role is not. That looks
inconsistent, and the reason is the kind of evidence available.

For `PROCESS`, the proof is that **the metadata read succeeded**. A role-held privilege
produces exactly that outcome, so the proof is just as good.

For DML privileges, GoArchive is not performing the operation at preflight time — it is
asking the privilege tables whether the operation *would* succeed. Those tables do not show
the account its own roles' grants, so the answer is genuinely unknown.

### 3. New check: `SOURCE_SELECT_PERMISSION_CHECK`

**Symptom.** A check ID you have not seen before, on an account that used to pass.

**What changed.** 1.8 never validated that the source account could *read* the tables it
was about to archive. An account with `DELETE` but not `SELECT` passed preflight and then
failed part-way through the run — after the tracking row and the per-job log table had
already been created, leaving state to clean up.

**Fix**

```sql
GRANT SELECT ON `<source_schema>`.* TO '<user>'@'<host>';
```

**Applies to all five commands** — every command reads source rows or estimates from them.

**Related symptom.** If the account has *no* privilege at all on the source schema, MySQL
hides the schema from `information_schema` entirely and you will see
`TABLE_EXISTENCE_CHECK` instead. `SOURCE_SELECT_PERMISSION_CHECK` catches the partial case:
the account can see the tables but cannot read them.

### 4. Destination unique constraints are judged by uniqueness predicate

**Symptom**

```
DEST_SCHEMA_COMPATIBILITY_CHECK: Source and destination schemas are incompatible
(tables: [orders(destination unique index "uq_email" has no equivalent on the source ...)])
```

**What changed.** 1.8 compared a per-column projection of the index picture
(`information_schema.COLUMNS.COLUMN_KEY`). 2.0 compares the uniqueness predicate each
unique index enforces: the set of `(column or expression, prefix length, column collation)`
parts.

**Why.** The 1.8 projection provably missed four shapes, each of which causes
`INSERT IGNORE` to silently skip rows — which are then deleted from the source, after a
verification that never saw them:

| Destination-only unique index | Why the projection missed it |
|---|---|
| `UNIQUE (a, b)` composite | `COLUMN_KEY` for `a` is `MUL`, not `UNI` |
| `UNIQUE (email(10))` vs source `UNIQUE (email)` | both report `UNI`; the prefix is invisible |
| `UNIQUE ((lower(email)))` functional | no column part, so `COLUMN_KEY` stays empty |
| `UNIQUE (email)` under a looser collation | both report `UNI`; 1.8 emitted only a warning |

The last one is the least obvious: a case- or accent-insensitive destination collation
collides rows that a binary source collation keeps distinct.

**Fix.** Drop the destination unique index:

```sql
ALTER TABLE `<destination_schema>`.`<table>` DROP INDEX `<index_name>`;
```

Dropping destination secondary indexes is a supported write-performance optimization —
the copy inserts explicit values for every column and relies on no destination index
except the primary key. If you need the constraint in the archive, add an equivalent one
to the **source** so the two predicates match.

**Equivalence is generous where it safely can be.** Index names, key order and `DESC` are
ignored, so renaming an index or declaring `UNIQUE(b,a)` against `UNIQUE(a,b)` will not
fail. Prefix length and column collation are not ignored, because both change which rows
collide.

## Two additional fail-closed changes

Neither adds a check ID, and neither changes what GoArchive does with your data. Both change
*which* check reports a problem and *what the message says*.

### 5. Views and other non-base objects are rejected earlier, by name and type

**Symptom**

```
TABLE_EXISTENCE_CHECK: Only base tables can be archived. These source objects are not
base tables; remove them from the configuration, or archive the underlying tables
instead (tables: [orders(VIEW)])
```

**What changed.** Only `BASE TABLE` objects are supported, and that is now enforced at the
existence stage on both sides, with the object's observed type in the message.

| | Source view | Destination view |
|---|---|---|
| **1.8** | passed name existence, then failed `PRIMARY_KEY_CHECK` — telling you to fix a primary key a view cannot have | passed name existence, then failed `DEST_SCHEMA_COMPATIBILITY_CHECK` as a structural mismatch |
| **2.0** | fails `TABLE_EXISTENCE_CHECK`, naming the object `orders(VIEW)` | fails `DEST_TABLE_EXISTENCE_CHECK`, naming the object `orders(VIEW)` |

**Execution safety did not change.** A view was always rejected during preflight — it has no
PRIMARY KEY, so the primary-key check caught it. No view ever reached the copy or delete
phase in 1.8, and none does now. What changed is that the rejection now names the real
problem instead of a downstream symptom, and object types nobody has classified fail closed
rather than falling through.

**Fix.** Archive the underlying table instead of the view, or create a real table in the
destination.

### 6. A `BASE TABLE` with NULL engine metadata fails with attribution

**Symptom**

```
STORAGE_ENGINE_CHECK: Only InnoDB tables are supported. Use ALTER TABLE to convert
(tables: [orders(<unknown>)])
```

**What changed.** 1.8 aborted preflight with a raw `database/sql` scan error naming neither
the check nor the table. 2.0 fails closed under `STORAGE_ENGINE_CHECK` and names the object
as `<table>(<unknown>)`.

**This is anomalous metadata, not a routine migration concern.** MySQL associates NULL-heavy
`information_schema.TABLES` rows with views, which are already excluded by the previous
check; a NULL `ENGINE` on a genuine `BASE TABLE` indicates a corrupted or unknown-engine
table. A healthy MySQL 8 does not produce it. If you see it, investigate the table itself —
this is not a state you should expect to encounter while upgrading.

## What did not change

- Every 1.8 check ID keeps its name and its meaning.
- The order checks run in, and the abort-on-first-failure behaviour.
- Which checks each command runs — except the new `SOURCE_SELECT_PERMISSION_CHECK`, which
  runs for all five.
- `--skip-validate-preflight` still bypasses preflight entirely, with the same risks.
- `--force-triggers` still overrides `DELETE_TRIGGER_CHECK` only.
- Archiving, copying, deleting, checkpointing, resume, locking, and replication-lag
  handling. This release changes validation, not execution.

## Before you upgrade

### Tracking tables: upgrade from 1.8, not from 1.2

2.0 uses the tracking-table shape already written by 1.8: `archiver_job` keyed by
an integer `id`, plus one `archiver_job_log_<id>` table per job. Upgrading directly
from a release older than that `id` column is not supported. Upgrade to **1.8 first,
then 2.0**; alternatively, drain in-flight jobs and drop the old tracking tables,
accepting that every checkpoint is discarded.

2.0 also adds `goarchive_meta`, a one-row declaration of the tracking schema's
revision. It is created and stamped automatically on first use. No new privilege is
required: the existing `CREATE`, `SELECT`, `INSERT`, and `UPDATE` grant on
`job_schema` covers it. From this release onward, GoArchive refuses a revision it
does not recognize instead of guessing compatibility.

1. Run `goarchive validate` with your existing configuration, on the 2.0 binary. It runs
   every check and changes nothing.
2. Fix whatever it reports, using the sections above.
3. Run `goarchive dry-run` to confirm the plan is unchanged.
4. Archive as usual.

If `validate` passes, nothing in this document affects you.
