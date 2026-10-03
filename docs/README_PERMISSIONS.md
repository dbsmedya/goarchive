# Permissions Reference

The MySQL privileges GoArchive requires, why each is needed, and copy-paste grant
recipes.

**Related:** [Configuration](README_CONFIGURATION.md) ·
[Validation & Preflight](README_VALIDATION.md) ·
[Operations](README_OPERATIONS.md) · [Limitations](README_LIMITATIONS.md) ·
[← Back to README](../README.md)

---

## Contents

- [Grant recipes](#grant-recipes)
- [Privilege matrix](#privilege-matrix)
- [The provable-grant rule](#the-provable-grant-rule)
- [Troubleshooting](#troubleshooting)

---

## Grant recipes

These grants satisfy every permission check, with or without `@@global.partial_revokes`,
because each privilege is granted directly at schema scope
([the provable-grant rule](#the-provable-grant-rule)).

### Source

```sql
GRANT SELECT, DELETE ON `<source_schema>`.* TO '<user>'@'<host>';  -- DELETE: archive, purge and validate only
GRANT PROCESS ON *.* TO '<user>'@'<host>';                         -- archive, purge, dry-run and validate
```

`PROCESS` is server-wide by definition — MySQL has no narrower scope for it.

### Destination — tracking tables in the destination database (default)

When `job_schema` is unset it defaults to `destination.database`, so one grant covers both
data and tracking tables:

```sql
GRANT SELECT, INSERT, CREATE, UPDATE ON `<destination_schema>`.* TO '<user>'@'<host>';
```

### Destination — tracking tables in a separate schema

With `destination.job_schema` set, the two are granted separately. A DBA must create that
schema first; GoArchive never does ([`job_schema`](README_CONFIGURATION.md#job_schema-destination-only)).

```sql
GRANT SELECT, INSERT ON `<destination_schema>`.* TO '<user>'@'<host>';
GRANT CREATE, SELECT, INSERT, UPDATE ON `<job_schema>`.* TO '<user>'@'<host>';
```

### Monitored replicas (optional)

```sql
-- Run on EACH server listed in replication.servers, for that entry's user.
GRANT REPLICATION CLIENT ON *.* TO 'monitor'@'%';
```

Only needed when `replication.enabled: true`. A server whose account lacks it is held by the
gate, not skipped ([Replication gating](README_OPERATIONS.md#replication-gating)).

### Tracking-table cleanup (a separate DBA account)

Pruning or truncating tracking tables
([what is safe to delete](README_JOBS_SCHEMA.md#maintenance-what-is-safe-to-delete)) needs
`DELETE`, and `DROP` for `TRUNCATE`. GoArchive itself never needs either: grant them to a DBA
account, not to the account GoArchive runs as.

---

## Privilege matrix

| Server | Privileges | Used for |
|--------|-----------|----------|
| **Source** | `SELECT` | Reading rows and estimates — all five commands |
| **Source** | `DELETE` | Deleting archived rows — `archive`, `purge`, `validate` |
| **Source** | `PROCESS` (global) | Proving foreign-key metadata completeness (`FK_COVERAGE_VISIBILITY_CHECK`) — `archive`, `purge`, `dry-run`, `validate`; not `copy-only` |
| **Destination** (data tables) | `SELECT`, `INSERT` | Copying rows, and reading them back to verify |
| **Tracking schema** (`job_schema`) | `CREATE`, `SELECT`, `INSERT`, `UPDATE` | Creating and maintaining `archiver_job`, the per-job `archiver_job_log_<id>` tables, and the one-row `goarchive_meta` revision marker |
| **Each monitored replica** (optional) | `REPLICATION CLIENT` | Replication gating, on every server listed in `replication.servers`, for the account that entry names |

Source `DELETE` is required for `archive`, `purge`, and `validate` — `validate`
enforces the privilege without deleting anything, so a `validate`-only run still
needs the grant. It is **not** required for `dry-run` or `copy-only`: `dry-run`
previews a delete without running this check, and `copy-only` never deletes from
source at all.

Preflight proves destination `INSERT` only. Verification also reads the copied rows back, so
the destination account needs `SELECT` too: an account without it passes preflight and fails
at verification, after the copy and before any delete.

### Why `CREATE` on the tracking schema

`CREATE` is a **runtime** requirement, not a one-time setup step. GoArchive
creates each job's `archiver_job_log_<id>` table on the fly the first time that
job runs. A grant that omits `CREATE` fails at startup with
`JOB_SCHEMA_PERMISSION_CHECK`.

---

## The provable-grant rule

A permission check passes only when the privilege is **established for the specific object**.
A privilege held only through a role, or a global grant while `@@global.partial_revokes` is
enabled, is *unconfirmed* and fails closed: MySQL does not expose a role's grant rows to the
account that holds the role, so GoArchive cannot prove the privilege exists — only that it
might. Grant `SELECT`, `DELETE`, `INSERT` and the tracking-schema privileges **directly** to
the account GoArchive connects as, at schema or table scope.

This governs `SOURCE_SELECT_PERMISSION_CHECK`, `SOURCE_DELETE_PERMISSION_CHECK`,
`DEST_WRITE_PERMISSION_CHECK` and `JOB_SCHEMA_PERMISSION_CHECK`; how each reports the state it
saw is in [Permission checks](README_VALIDATION.md#permission-checks).

**`PROCESS` is the exception: a role-held `PROCESS` is accepted.** `FK_COVERAGE_VISIBILITY_CHECK`
proves completeness by successfully reading InnoDB's foreign-key metadata registry, which needs
effective `PROCESS`. A role-held `PROCESS` produces that same successful read, so the proof
holds however the privilege reached the account.

---

## Troubleshooting

**`JOB_SCHEMA_PERMISSION_CHECK`**

The message names the exact grant, and the `CREATE DATABASE` when the schema itself is missing
([the check](README_VALIDATION.md#job_schema_permission_check)).

**`DEST_WRITE_PERMISSION_CHECK` / `SOURCE_DELETE_PERMISSION_CHECK` lists tables
you believe are granted**

Check three things: the grant targets the account `CURRENT_USER()` actually
resolves to (not the one you connected *as*, if proxying or host-pattern matching
is involved); the privilege is granted **directly** to the account rather than
through any role ([the provable-grant rule](#the-provable-grant-rule)); and the privilege
is on the right schema — source `DELETE` and destination `INSERT` are separate
grants on separate servers.

**`FK_COVERAGE_VISIBILITY_CHECK` on a schema with no cross-schema foreign keys**

Expected: the check tests whether foreign-key metadata completeness is provable, not the
schema's contents ([the check](README_VALIDATION.md#fk_coverage_visibility_check)).

**Least-privilege deployments**

Running `validate` or `dry-run` from a separate, more-privileged account is
**diagnostic only**: `archive`, `purge`, and `copy-only` re-run preflight with their own
account ([How preflight works](README_VALIDATION.md#how-preflight-works)). The account that
actually runs the command must itself hold every privilege in the
[matrix](#privilege-matrix), including `PROCESS` where required. The only way around that is
`--skip-validate-preflight`, which bypasses every preflight check
([Bypassing preflight](README_VALIDATION.md#bypassing-preflight)).
