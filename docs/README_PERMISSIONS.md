# Permissions Reference

The MySQL privileges GoArchive needs, why, and grant recipes.

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

These grants pass every permission check, with or without `@@global.partial_revokes`, because
each privilege is granted directly at schema scope
([the provable-grant rule](#the-provable-grant-rule)).

### Source

```sql
GRANT SELECT, DELETE ON `<source_schema>`.* TO '<user>'@'<host>';  -- DELETE: archive and purge jobs
GRANT PROCESS ON *.* TO '<user>'@'<host>';                         -- archive and purge jobs (see the version note below)
```

`PROCESS` is server-wide by definition; MySQL has no narrower scope for it.

### Destination — tracking tables in the destination database (default)

When `job_schema` is unset it is `destination.database`, so one grant covers the data and the
tracking tables:

```sql
GRANT SELECT, INSERT, CREATE, UPDATE ON `<destination_schema>`.* TO '<user>'@'<host>';
```

### Destination — tracking tables in a separate schema

With `destination.job_schema` set, grant the two separately. A DBA creates that schema first;
GoArchive never does ([`job_schema`](README_CONFIGURATION.md#job_schema-destination-only)).

```sql
GRANT SELECT, INSERT ON `<destination_schema>`.* TO '<user>'@'<host>';
GRANT CREATE, SELECT, INSERT, UPDATE ON `<job_schema>`.* TO '<user>'@'<host>';
```

### Monitored replicas (optional)

```sql
-- On EACH server in replication.servers, for that entry's user.
GRANT REPLICATION CLIENT ON *.* TO 'monitor'@'%';
```

Only with `replication.enabled: true`. A server whose account lacks it holds the job rather than
being skipped ([Replication gating](README_OPERATIONS.md#replication-gating)).

### Tracking-table maintenance (a DBA account)

Pruning, abandoning, retiring and dropping in the tracking schema
([Job Tracking Schema](README_JOBS_SCHEMA.md#maintenance-what-is-safe-to-delete)) need `DELETE`
and `DROP`. GoArchive never does any of it: grant them to a DBA account, not to GoArchive's.

---

## Privilege matrix

By job type; `validate` and `dry-run` need what the job's type needs, since they run its
preflight.

| Server | Privileges | archive | purge | copy-only | Used for |
|--------|-----------|:---:|:---:|:---:|----------|
| **Source** | `SELECT` | ✅ | ✅ | ✅ | Reading rows |
| **Source** | `DELETE` | ✅ | ✅ | — | Deleting archived rows |
| **Source** | `PROCESS` (global) | ✅ | ✅ | — | Proving foreign-key metadata complete (`FK_COVERAGE_VISIBILITY_CHECK`); not needed on MySQL 8.0.40 (below) |
| **Destination** (data) | `INSERT` | ✅ | — | ✅ | Copying rows |
| **Destination** (data) | `SELECT` | ✅ | — | ✅ | Reading copies back under `verification: sha256` or `count` |
| **Tracking schema** (`job_schema`) | `CREATE`, `SELECT`, `INSERT`, `UPDATE` | ✅ | ✅ | ✅ | The registry, the per-job logs, the label |
| **Source** and **destination** (optional, global) | `SESSION_VARIABLES_ADMIN` | ✅ | ✅ | ✅ | Only on a server whose global `max_error_count` is 0 ([`MAX_ERROR_COUNT_CHECK`](README_VALIDATION.md#max_error_count_check)) |
| **Each monitored replica** (optional) | `REPLICATION CLIENT` | ✅ | ✅ | — | Replication gating |

- **Destination `SELECT`.** Preflight proves `INSERT` only, and `validate` and `dry-run` never read
  copies. A run that verifies (`sha256` or `count`) reads them back, so an account without
  `SELECT` fails there, before any delete; under `verification: none` nothing reads them. The
  recipes grant it, for the default `sha256`.
- **`PROCESS` and the MySQL version.** The proof reads InnoDB's foreign-key registry. MySQL
  8.0.46 and later refuse that read without `PROCESS`; MySQL 8.0.40 serves it unfiltered without
  `PROCESS`, so the check passes there without the grant and still sees every foreign key
  ([compatibility notes](README_COMPAT.md)).
- **Locks need no privilege.** `GET_LOCK()`, `IS_USED_LOCK()` and `RELEASE_ALL_LOCKS()` need no
  grant, on the source or the destination, and work on a `super_read_only` server. Reading
  `performance_schema.metadata_locks` (the upgrade check in
  [README_UPGRADING_3_0.md](README_UPGRADING_3_0.md#moving-from-30-alpha0)) needs `SELECT` on
  `performance_schema`: run it as a DBA account, not as GoArchive's.
- **The source facts need no privilege.** The unfinished-work guard reads the source's
  `gtid_mode` and `gtid_executed` and its Group Replication state through dbsgomysql; neither
  needs a grant beyond connecting
  ([Concurrency and locking](README_OPERATIONS.md#concurrency-and-locking)).
- **`SESSION_VARIABLES_ADMIN`** also lets the account change other restricted session variables,
  so raising the global `max_error_count` is usually the better remedy. The recipes above do not
  include it.

### Why `CREATE` on the tracking schema

`CREATE` is needed at run time, not once: each job's `archiver_job_log_<id>` is created the first
time the job runs. A grant without it fails at startup with `JOB_SCHEMA_PERMISSION_CHECK`.

---

## The provable-grant rule

A permission check passes only when the privilege is **established for the specific object**.
A privilege held only through a role, or a global grant while `@@global.partial_revokes` is
enabled, cannot be proven — MySQL does not show the account a role's grant rows — and fails
closed as `unconfirmed`. So does a privilege granted nowhere, to an account that cannot read the
`mysql` schema: only such a read can tell `absent` from `unconfirmed`. Grant `SELECT`, `DELETE`, `INSERT` and the tracking-schema privileges
**directly** to the account GoArchive connects as, at schema or table scope. How each check
reports what it saw: [Permission checks](README_VALIDATION.md#permission-checks).

**`PROCESS` is the exception: a role-held `PROCESS` is accepted.** The visibility proof is a
successful read of the registry, and a role-held `PROCESS` produces the same read.

---

## Troubleshooting

**A check lists tables you believe are granted.** Check that the grant targets the account
`CURRENT_USER()` resolves to (not the one you connected as, with proxying or host patterns); that
it is granted **directly**, not through a role; and that it is on the right schema and server:
source `DELETE` and destination `INSERT` are separate grants on separate servers.

**`FK_COVERAGE_VISIBILITY_CHECK` on a schema with no cross-schema foreign keys.** Expected: the
check proves that discovery was complete, whatever the schema holds
([the check](README_VALIDATION.md#fk_coverage_visibility_check)).

**Least-privilege deployments.** Running `validate` or `dry-run` from a more privileged account is
diagnostic only: `archive`, `purge` and `copy-only` run preflight again with their own accounts,
which must hold every privilege their job type needs. `--skip-validate-preflight` skips the
permission checks, not the privileges: a missing one then fails mid-run
([Bypassing preflight](README_VALIDATION.md#bypassing-preflight)).
