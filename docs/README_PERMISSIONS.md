# Permissions Reference

The MySQL privileges GoArchive requires, why each is needed, and copy-paste grant
recipes.

**Related:** [Configuration](README_CONFIGURATION.md) ·
[Validation & Preflight](README_VALIDATION.md) ·
[Operations](README_OPERATIONS.md) · [Limitations](README_LIMITATIONS.md) ·
[← Back to README](../README.md)

---

## Contents

- [Privilege matrix](#privilege-matrix)
- [Grant recipes](#grant-recipes)
- [The global SELECT requirement](#the-global-select-requirement)
- [How privileges are verified](#how-privileges-are-verified)
- [Troubleshooting](#troubleshooting)

---

## Privilege matrix

| Server | Privileges | Used for |
|--------|-----------|----------|
| **Source** | `SELECT`, `DELETE` | Reading rows and deleting archived rows |
| **Source** | `SELECT ON *.*` (global) | Cross-schema foreign key visibility — see [below](#the-global-select-requirement) |
| **Destination** (data tables) | `SELECT`, `INSERT` | Copying rows into archive tables |
| **Tracking schema** (`job_schema`) | `CREATE`, `SELECT`, `INSERT`, `UPDATE` | Creating and maintaining `archiver_job` and the per-job `archiver_job_log_<id>` tables |
| **Tracking schema** (optional) | `DELETE`, `DROP` | DBA cleanup only. `DROP` is additionally needed for `TRUNCATE`. |
| **Replica** (optional) | `REPLICATION CLIENT` | Lag monitoring via `SHOW REPLICA STATUS` |

`copy-only` never deletes from source, so it does not require source `DELETE`.
Every other command does.

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

## Grant recipes

### Source

```sql
GRANT SELECT, DELETE ON production.* TO 'archiver'@'%';

-- Required for cross-schema FK visibility on archive/purge/dry-run/validate.
-- See "The global SELECT requirement" below.
GRANT SELECT ON *.* TO 'archiver'@'%';
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

## The global SELECT requirement

`FK_COVERAGE_VISIBILITY_CHECK` **hard-fails** when the source account lacks
`SELECT ON *.*`. This is the least intuitive requirement GoArchive imposes, so
the reasoning matters.

### Why a schema-scoped grant is not enough

GoArchive must prove that no table **outside** the archive graph holds a foreign
key pointing **into** it. An external `ON DELETE CASCADE` or `SET NULL` would
delete or mutate rows GoArchive never copied — silent data loss during the delete
phase.

MySQL makes that impossible to prove from a least-privilege account:

1. A foreign key constraint appears in `information_schema` only to an account
   with some privilege on the constraint's **child** table.
2. A schema the account has no privilege on is **entirely invisible** — it does
   not even appear in `SCHEMATA`.

So an unprivileged account cannot distinguish "no external foreign keys exist"
from "external foreign keys exist but I cannot see them". GoArchive fails closed
rather than assuming the former.

### Which commands enforce it

| Command | Enforced | Reason |
|---------|:--------:|--------|
| `archive` | ✅ | Deletes from source |
| `purge` | ✅ | Deletes from source |
| `dry-run` | ✅ | Previews a delete |
| `validate` | ✅ | Validates a delete-capable configuration |
| `copy-only` | ❌ | Never deletes from source — no cascade can fire |

`archive` and `purge` can bypass it with `--skip-validate-preflight`, which
bypasses **all** preflight and is dangerous. `dry-run` and `validate` have no skip
flag.

The exemption for `copy-only` is from the **visibility** check only. `copy-only`
still runs `FK_COVERAGE_CHECK`, so a `copy-only` run by a global-privileged
account is still blocked by an uncovered cross-schema incoming foreign key.

### Upgrade impact

> An existing least-privilege source account that ran cleanly on an earlier
> release **will now fail** this check on `archive`, `purge`, `dry-run`, and
> `validate` — even when the schema contains no cross-schema foreign keys at all.
> The check tests the account's privilege, not the schema's contents.

Fix by granting global SELECT, or run preflight as an account that already holds
it.

---

## How privileges are verified

Preflight verifies destination `INSERT`, source `DELETE`, and the tracking-schema
privileges **up front** — these are precisely the failures that would otherwise
surface mid-run, after copy has already committed rows.

### Grantee resolution

The check matches the **connected account**, resolved as:

- `CURRENT_USER()`, converted to `information_schema` GRANTEE format
  (`'user'@'host'`)
- plus every **active role** from `CURRENT_ROLE()`

### Scope resolution

For each privilege, three scopes are consulted in order, and the first match
wins:

1. `information_schema.USER_PRIVILEGES` — global (`ON *.*`)
2. `information_schema.SCHEMA_PRIVILEGES` — schema (`ON db.*`)
3. `information_schema.TABLE_PRIVILEGES` — per table

A global or schema-level grant therefore satisfies the check without any
per-table grant. `GRANT ALL` works too — MySQL expands it into individual
privilege rows.

The tracking-schema check (`JOB_SCHEMA_PERMISSION_CHECK`) uses global and schema
scope **only**, with no per-table fallback: the per-job tracking tables do not
exist yet when preflight runs.

### No extra privileges needed to run the checks

Privilege introspection requires no additional grants. MySQL always exposes the
connected account's own privilege rows in `information_schema`.

### Known limitation: nested roles

Only **directly activated** roles are detected. A privilege held through a role
granted to another role is not visible to the check and will conservatively fail
it. Grant the privilege directly, or activate the role that actually holds it.

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
is involved); the role holding it is activated and not nested; and the privilege
is on the right schema — source `DELETE` and destination `INSERT` are separate
grants on separate servers.

**`FK_COVERAGE_VISIBILITY_CHECK` on a schema with no cross-schema foreign keys**

Expected. The check tests the account's privilege, not the schema. Grant
`SELECT ON *.*`, or use `copy-only`, which is exempt.

**Least-privilege deployments**

Run preflight-enforcing commands (`validate`, `dry-run`) as a global-SELECT
account, and keep the archive account narrower. The preflight and the run do not
have to use the same account, as long as the account performing the run holds the
privileges in the [matrix](#privilege-matrix).
