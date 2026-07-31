# Permissions Reference

The MySQL privileges GoArchive requires, why each is needed, and copy-paste grant
recipes.

**Related:** [Configuration](README_CONFIGURATION.md) ·
[Validation & Preflight](README_VALIDATION.md) ·
[Operations](README_OPERATIONS.md) · [Limitations](README_LIMITATIONS.md) ·
[← Back to README](../README.md)

---

## Contents

- [Recommended grant recipe](#recommended-grant-recipe)
- [What preflight actually enforces](#what-preflight-actually-enforces)
- [Privilege matrix](#privilege-matrix)
- [Why roles are treated differently for PROCESS and for DML](#why-roles-are-treated-differently-for-process-and-for-dml)
- [Grant recipes](#grant-recipes)
- [Troubleshooting](#troubleshooting)

---

## Recommended grant recipe

This is what to grant. It satisfies every invariant below on a server without
`@@global.partial_revokes`.

**Source account:**

```sql
GRANT PROCESS, SELECT ON *.* TO '<user>'@'<host>';
GRANT DELETE ON `<source_schema>`.* TO '<user>'@'<host>';   -- archive, purge, and validate only
```

**Destination account:**

```sql
GRANT SELECT, INSERT ON `<destination_schema>`.* TO '<user>'@'<host>';
GRANT CREATE, SELECT, INSERT, UPDATE ON `<job_schema>`.* TO '<user>'@'<host>';
```

`PROCESS` is server-wide by definition — MySQL has no narrower scope for it. `SELECT ON *.*`
is recommended rather than required; a direct grant on the source schema also satisfies the
invariant.

---

## What preflight actually enforces

The recipe is a convenient way to satisfy three invariants. If your environment cannot use
it — partial revokes, tightly scoped accounts, an existing role structure — these are the
contracts to satisfy instead.

### I1 — foreign-key metadata completeness

`FK_COVERAGE_VISIBILITY_CHECK` requires that InnoDB's foreign-key metadata registry can be
read, which requires **effective** `PROCESS`. Effective means the read succeeds; a role-held
`PROCESS` qualifies. Applies to `archive`, `purge`, `dry-run` and `validate`; `copy-only` is
exempt.

### I2 — DML privileges must be provable per object

`SOURCE_DELETE_PERMISSION_CHECK`, `DEST_WRITE_PERMISSION_CHECK` and
`JOB_SCHEMA_PERMISSION_CHECK` pass only when the privilege is *established* for the specific
object. A privilege held through a role, or a global grant while `@@global.partial_revokes`
is enabled, is *unconfirmed* and fails closed.

Under partial revokes, satisfy this with a direct schema- or table-level grant. The global
grant in the recipe is the convenient path, not the only one.

### I3 — source read permission

`SOURCE_SELECT_PERMISSION_CHECK` requires provable `SELECT` on every participating table,
for **all five commands** — every command reads source rows or estimates from them.

---

## Privilege matrix

| Server | Privileges | Used for |
|--------|-----------|----------|
| **Source** | `SELECT`, `DELETE` | Reading rows and deleting archived rows |
| **Source** | `PROCESS` (global) | Foreign-key metadata completeness (I1) — required for `archive`, `purge`, `dry-run`, `validate`; not required for `copy-only` |
| **Source** | `SELECT ON *.*` (global) | Convenient alternative to a per-schema `SELECT` grant (I3); not required if `SELECT` is already granted directly on the source schema |
| **Destination** (data tables) | `SELECT`, `INSERT` | Copying rows into archive tables |
| **Tracking schema** (`job_schema`) | `CREATE`, `SELECT`, `INSERT`, `UPDATE` | Creating and maintaining `archiver_job` and the per-job `archiver_job_log_<id>` tables |
| **Tracking schema** (optional) | `DELETE`, `DROP` | DBA cleanup only. `DROP` is additionally needed for `TRUNCATE`. |
| **Replica** (optional) | `REPLICATION CLIENT` | Lag monitoring via `SHOW REPLICA STATUS` |

Source `DELETE` is required for `archive`, `purge`, and `validate` — `validate`
enforces the privilege without deleting anything, so a `validate`-only run still
needs the grant. It is **not** required for `dry-run` or `copy-only`: `dry-run`
previews a delete without running this check, and `copy-only` never deletes from
source at all.

Source `SELECT` on every participating table is validated by
`SOURCE_SELECT_PERMISSION_CHECK` for all five commands (I3) — see
[Validation & Preflight](README_VALIDATION.md). The privilege must be provable for the
object: a grant held only through an active role, or a bare global grant while
`@@global.partial_revokes` is enabled, is reported as unconfirmed and fails closed.

### Why `CREATE` on the tracking schema

`CREATE` is a **runtime** requirement, not a one-time setup step. GoArchive
creates each job's `archiver_job_log_<id>` table on the fly the first time that
job runs. A grant that omits `CREATE` fails at startup with
`JOB_SCHEMA_PERMISSION_CHECK`.

GoArchive does **not** create schemas. If `job_schema` names a schema that does
not exist, a DBA must `CREATE DATABASE` it first.

The optional `DELETE`/`DROP` grants are for maintenance only — see
[Job Tracking Schema](README_JOBS_SCHEMA.md#maintenance-what-is-safe-to-delete).

---

## Why roles are treated differently for PROCESS and for DML

**A role-held `PROCESS` is accepted; a role-held DML privilege is not.** The reason is the
kind of evidence available for each.

`FK_COVERAGE_VISIBILITY_CHECK` (I1) proves completeness by successfully reading InnoDB's
foreign-key metadata registry, which requires `PROCESS`. A role-held `PROCESS` produces that
same successful read, so the proof holds regardless of whether the privilege reached the
account directly or through a role.

The DML checks (I2 — `SOURCE_DELETE_PERMISSION_CHECK`, `DEST_WRITE_PERMISSION_CHECK`,
`JOB_SCHEMA_PERMISSION_CHECK`; and I3 — `SOURCE_SELECT_PERMISSION_CHECK`) have no equivalent
proof available. MySQL does not expose a role's own grant rows to the account that holds the
role, so GoArchive cannot confirm the privilege exists — only that it might. A privilege held
only through an active role is therefore reported *unconfirmed* and fails closed. Grant DML
privileges directly to the account GoArchive connects as.

---

## Grant recipes

### Source

```sql
GRANT SELECT, DELETE ON production.* TO 'archiver'@'%';

-- Required for foreign-key metadata completeness (I1) on archive/purge/dry-run/validate.
-- See "What preflight actually enforces" above.
GRANT PROCESS ON *.* TO 'archiver'@'%';
```

### Destination — tracking tables in the destination database (default)

When `job_schema` is unset it defaults to `destination.database`, so one combined
grant covers both data and tracking tables:

```sql
GRANT SELECT, INSERT, CREATE, UPDATE ON archive.* TO 'archiver'@'%';
```

### Destination — tracking tables in an isolated schema

With `destination.job_schema: goarchive`, the two are granted separately:

```sql
-- Data tables
GRANT SELECT, INSERT ON archive.* TO 'archiver'@'%';

-- Tracking schema. The DBA must create it first — GoArchive never does.
CREATE DATABASE goarchive;
GRANT CREATE, SELECT, INSERT, UPDATE ON goarchive.* TO 'archiver'@'%';

-- Optional, for DBA cleanup only:
-- GRANT DELETE, DROP ON goarchive.* TO 'archiver'@'%';
```

### Replica (optional)

```sql
GRANT REPLICATION CLIENT ON *.* TO 'archiver'@'%';
```

Only needed when `replica.enabled: true`.

---

## Troubleshooting

**`JOB_SCHEMA_PERMISSION_CHECK: ... lacks CREATE, UPDATE on tracking schema`**

The error names the exact grant, and prefixes `CREATE DATABASE` when the schema
itself is missing:

```
(DBA must: CREATE DATABASE `goarchive`; GRANT CREATE, UPDATE ON `goarchive`.* TO <user>)
```

**`DEST_WRITE_PERMISSION_CHECK` / `SOURCE_DELETE_PERMISSION_CHECK` lists tables
you believe are granted**

Check three things: the grant targets the account `CURRENT_USER()` actually
resolves to (not the one you connected *as*, if proxying or host-pattern matching
is involved); the privilege is granted **directly** to the account rather than
through any role — under I2, a role-held DML grant is always reported
unconfirmed, whether or not the role is nested or activated; and the privilege
is on the right schema — source `DELETE` and destination `INSERT` are separate
grants on separate servers.

**`FK_COVERAGE_VISIBILITY_CHECK` on a schema with no cross-schema foreign keys**

Expected. The check tests whether foreign-key metadata completeness is
provable, not the schema's contents. Grant `PROCESS`, or use `copy-only`, which
is exempt.

**Least-privilege deployments**

Running `validate` or `dry-run` from a separate, more-privileged account is
**diagnostic only** — it tells you whether *that* account satisfies the
invariants, not whether the account that will actually run `archive`, `purge`,
or `copy-only` does. `archive`, `purge`, and `copy-only` each connect with their
own configured account and **re-run preflight themselves at startup**
(`cmd/goarchive/cmd/archive.go`, `purge.go`, `copyonly.go`); a prior `validate`
result is not carried forward and grants the run nothing. The account that
actually runs the command must itself satisfy every privilege in the
[matrix](#privilege-matrix), including `PROCESS` where required.

The only way to run a command without its own account holding these privileges
is `--skip-validate-preflight`, which is **DANGEROUS** and bypasses every
preflight check, not just the permission ones. `dry-run` and `validate` do not
accept that flag at all.
