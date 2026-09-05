# Job Tracking Schema — DBA Maintenance Guide

How GoArchive stores job state on the destination, what the tracking tables
contain, and when it is safe to clean them up.

**Related:** [Configuration](README_CONFIGURATION.md) ·
[Operations](README_OPERATIONS.md) · [Permissions](README_PERMISSIONS.md) ·
[Validation & Preflight](README_VALIDATION.md) · [← Back to README](../README.md)

---

## Contents

- [Where tracking state lives](#where-tracking-state-lives)
- [`archiver_job`](#archiver_job)
- [`archiver_job_log_<id>`](#archiver_job_log_id)
- [Lifecycle](#lifecycle)
- [Growth and sizing](#growth-and-sizing)
- [Inspection cookbook](#inspection-cookbook)
- [Maintenance: what is safe to delete](#maintenance-what-is-safe-to-delete)
- [Clearing a stale running job](#clearing-a-stale-running-job)
- [Tracking-schema version marker](#tracking-schema-version-marker)
- [Backup and replication notes](#backup-and-replication-notes)

---

## Where tracking state lives

All tracking state lives on the **destination** server, in the schema named by
`destination.job_schema`. When that is unset it defaults to
`destination.database`.

```yaml
destination:
  database: archive
  job_schema: goarchive     # optional; omit to use `archive`
```

Three structures exist:

| Object | Cardinality | Holds |
|--------|-------------|-------|
| `archiver_job` | one table, one row per job | Checkpoint, status, heartbeat |
| `archiver_job_log_<id>` | **one table per job** | Per-root-PK processing status |
| `goarchive_meta` | one table, one row | The tracking schema's own revision |

The log table is named by the job's integer `id` — **not** by job name. Resolve it
first:

```sql
SELECT id, job_name, root_table, job_type FROM goarchive.archiver_job;
-- job id 42  ->  goarchive.archiver_job_log_42
```

> GoArchive never issues `CREATE DATABASE`. If `job_schema` names a schema that
> does not exist, a DBA must create it. GoArchive **does** create its tracking tables,
> which is why the account needs `CREATE` at runtime — see
> [Permissions](README_PERMISSIONS.md).

---

## `archiver_job`

Created once per schema, idempotently, at startup.

```sql
CREATE TABLE IF NOT EXISTS archiver_job (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    root_table VARCHAR(255) NOT NULL,
    job_type VARCHAR(32) NOT NULL DEFAULT 'archive',
    last_processed_root_pk_id VARCHAR(255) DEFAULT NULL,
    job_status TINYINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    last_heartbeat_at DATETIME NULL,
    UNIQUE KEY uk_job_name (job_name),
    INDEX idx_status (job_status),
    INDEX idx_updated (updated_at)
) ENGINE=InnoDB
```

| Column | Meaning |
|--------|---------|
| `id` | Surrogate key. **Determines the log table name** — `archiver_job_log_<id>`. |
| `job_name` | The job key from `archiver.yaml`. Unique. |
| `root_table` | Root table, used by the same-root concurrency check. |
| `job_type` | `archive`, `purge`, or `copy-only`. |
| `last_processed_root_pk_id` | **The checkpoint.** Highest root PK fully completed. `NULL` = never run. Stored as a string; compared numerically. |
| `job_status` | See below. |
| `created_at` / `updated_at` | Maintained by MySQL. |
| `last_heartbeat_at` | Liveness signal, refreshed every **15 seconds** while running. |

### `job_status` values

| Value | Name | Meaning |
|-------|------|---------|
| `0` | Idle | Not running. Normal resting state. |
| `1` | Running | A process claims this job. |
| `2` | Paused | Reserved; not written by current releases. |
| `3` | Failed | Reserved; not written by current releases. |

A job left at `1` with a stale heartbeat means a crashed run — see
[Clearing a stale running job](#clearing-a-stale-running-job).

### `job_type` is sticky

A job name is bound to the command that created it. Running a different command
against the same job name fails:

```
job "archive_old_orders" exists with type "archive", expected "copy-only"
```

This is deliberate — the resume semantics differ per type. To repurpose a job
name, delete its row and log table (see
[Maintenance](#maintenance-what-is-safe-to-delete)), or just use a new name.

### `root_table` is sticky

A job name is also bound to the root table that created it. Running the same job
name against a different `root_table` fails before any recovery marker is read:

```
job "archive_old_orders" exists for root table "orders", expected "invoices": a job name is bound to the root table that created it, and its recovery markers must never be replayed against another table. Retire the job (docs/README_JOBS_SCHEMA.md, "Retiring a job completely") or use a new job name
```

A `copied` marker written for one table could otherwise authorise a delete-only
replay against another (issue #14). The comparison is exact, including letter
case, like `PK_COLUMN_CASE_CHECK`. To repurpose a job name,
[retire the job](#retiring-a-job-completely) or use a new name.

What is **not** bound on this release line is the source itself: GoArchive does
not yet persist the source server and schema in `archiver_job`, so the same job
name pointed at a different source that has a table of the same name is not
detected. Use a new job name whenever the source changes.

### Heartbeat timing

| Constant | Value |
|----------|-------|
| Heartbeat write interval | 15 seconds |
| Staleness threshold | 60 seconds |
| Consecutive heartbeat failures before abort | 3 |
| Advisory lock keepalive interval | 30 seconds |

A heartbeat older than 60 seconds is considered stale. **Stale does not mean
dead** — the process may still hold `GET_LOCK()` and still be deleting.

---

## `archiver_job_log_<id>`

Created lazily, the first time a job runs, once its integer `id` is known.

```sql
CREATE TABLE IF NOT EXISTS archiver_job_log_42 (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    root_pk_id VARCHAR(255) NOT NULL,
    log_status TINYINT NOT NULL DEFAULT 0,
    error_message TEXT,
    UNIQUE KEY uk_pk (root_pk_id),
    INDEX idx_status (log_status)
) ENGINE=InnoDB
```

| Column | Meaning |
|--------|---------|
| `id` | Surrogate key, no operational meaning. |
| `root_pk_id` | One root table primary key. Unique — inserts use `INSERT IGNORE`, so replay is idempotent. |
| `log_status` | Processing status. See below. |
| `error_message` | **Vestigial.** Never written by current releases; only read when reporting legacy `log_status=3` rows. |

There is no `job_name` column and no timestamps — the table *is* the job scope.

### `log_status` values

| Value | Name | Meaning | On resume |
|-------|------|---------|-----------|
| `0` | pending | Discovered, not yet copied | Full replay |
| `1` | copied | Copied **and verified**; only delete remains | Delete-only (archive/purge) or promote to completed (copy-only) |
| `2` | completed | Fully processed | Skipped |
| `3` | failed | **Legacy only** — written by pre-1.8 releases | **Blocks resume** with recovery guidance |

> **Current releases never write `3`.** An error aborts the run and leaves rows in
> a recoverable status (`0` or `1`). If you see `log_status=3`, those rows came
> from a pre-1.8 release and will block resume until resolved.

Completed rows are **kept as evidence** and never removed automatically. This is
the main reason these tables grow.

### The status transition is transactional

`log_status → 2` and the `archiver_job.last_processed_root_pk_id` advance happen
in **one transaction**. There is no window where the checkpoint has moved past a
row still marked pending. If you are reasoning about a crashed run, you can trust
that invariant.

---

## Lifecycle

1. **First run of any job in the schema** — `archiver_job` and `goarchive_meta` are
   created if absent (`CREATE TABLE IF NOT EXISTS`). A fresh schema — no marker and no
   rows in `archiver_job` — is stamped with the current revision (`2.2`); anything else
   is refused, see [Tracking-schema version marker](#tracking-schema-version-marker).
2. **First run of a specific job** — a row is inserted, `id` is assigned, and
   `archiver_job_log_<id>` is created.
3. **During a run** — `job_status = 1`, heartbeat every 15s, log rows transition
   `0 → 1 → 2` in batches.
4. **Normal completion** — `job_status` returns to `0`; the checkpoint holds the
   highest completed PK; completed log rows remain.
5. **Crash** — `job_status` stays `1` with a stale heartbeat; non-terminal log
   rows remain for the next run to replay.

Nothing is ever cleaned up automatically. That is intentional — the log is the
audit trail for what was deleted from production.

---

## Growth and sizing

`archiver_job` is bounded by the number of jobs — effectively never a problem.

`archiver_job_log_<id>` grows by **one row per root primary key processed**, and
those rows are never deleted. Child rows are not logged, only roots.

Rough sizing: each row is a short `VARCHAR` PK plus a `TINYINT` and an empty
`TEXT`, with two indexes — on the order of **100–150 bytes** per root PK
including index overhead. Ten million archived roots is roughly 1–1.5 GB.

If a job archives millions of roots and you do not need the audit trail
indefinitely, see [Maintenance](#maintenance-what-is-safe-to-delete).

---

## Inspection cookbook

> `last_heartbeat_at` is a UTC wall-clock since 2.2. Always measure its age with
> `UTC_TIMESTAMP()`, never `NOW()` — `NOW()` follows *your* session's zone and misreports the
> age by your UTC offset.

**All jobs and their checkpoints**

```sql
SELECT id, job_name, root_table, job_type, job_status,
       last_processed_root_pk_id AS checkpoint,
       last_heartbeat_at,
       TIMESTAMPDIFF(SECOND, last_heartbeat_at, UTC_TIMESTAMP()) AS heartbeat_age_sec
FROM goarchive.archiver_job
ORDER BY id;
```

**Is anything running right now?**

```sql
SELECT job_name, root_table,
       TIMESTAMPDIFF(SECOND, last_heartbeat_at, UTC_TIMESTAMP()) AS heartbeat_age_sec,
       CASE WHEN last_heartbeat_at IS NULL
                 OR TIMESTAMPDIFF(SECOND, last_heartbeat_at, UTC_TIMESTAMP()) > 60
            THEN 'STALE' ELSE 'live' END AS liveness
FROM goarchive.archiver_job
WHERE job_status = 1;
```

**Status breakdown for one job** (substitute the id)

```sql
SELECT log_status,
       CASE log_status WHEN 0 THEN 'pending' WHEN 1 THEN 'copied'
                       WHEN 2 THEN 'completed' WHEN 3 THEN 'failed (legacy)' END AS name,
       COUNT(*) AS rows
FROM goarchive.archiver_job_log_42
GROUP BY log_status;
```

**Rows that would be replayed on the next run**

```sql
SELECT root_pk_id, log_status
FROM goarchive.archiver_job_log_42
WHERE log_status IN (0, 1)
ORDER BY CAST(root_pk_id AS UNSIGNED);
```

**Legacy failed rows and their reasons**

```sql
SELECT root_pk_id, error_message
FROM goarchive.archiver_job_log_42
WHERE log_status = 3;
```

**Tracking table sizes**

```sql
SELECT table_name,
       table_rows,
       ROUND((data_length + index_length) / 1024 / 1024, 1) AS size_mb
FROM information_schema.tables
WHERE table_schema = 'goarchive'
  AND table_name LIKE 'archiver_job%'
ORDER BY (data_length + index_length) DESC;
```

**Orphaned log tables** — log tables whose job row no longer exists

```sql
SELECT t.table_name
FROM information_schema.tables t
LEFT JOIN goarchive.archiver_job j
       ON t.table_name = CONCAT('archiver_job_log_', j.id)
WHERE t.table_schema = 'goarchive'
  AND t.table_name LIKE 'archiver_job_log_%'
  AND j.id IS NULL;
```

---

## Maintenance: what is safe to delete

> **Rule zero: never touch tracking tables while the job is running.** Confirm
> `job_status = 0`, or `job_status = 1` with a heartbeat older than 60 seconds
> *and* a verified-dead process, before doing anything below.

### ⚠️ Never drop `goarchive_meta` on its own

Dropping it makes the schema look unstamped. The next run would stamp it with the
running binary's revision, which may be false if another release wrote the tracking
tables and would discard the signal needed for a future migration.

If you intentionally clear all tracking state, remove `goarchive_meta`, `archiver_job`,
and every `archiver_job_log_<id>` table together. Never reset only the declaration.

### ⚠️ Never `TRUNCATE archiver_job` on its own

`TRUNCATE` resets the `AUTO_INCREMENT` counter to 1. The next job created then
takes `id = 1` and derives log table `archiver_job_log_1` — which
`CREATE TABLE IF NOT EXISTS` will happily **reuse, along with every stale row
still in it**.

A brand-new job would inherit another job's `pending` and `copied` rows, and the
resume gates would fire against roots it has never seen — or, worse, replay them.

If you truncate `archiver_job`, **drop every `archiver_job_log_*` table in the
same maintenance window**:

```sql
-- Generate the DROP statements, review, then run them.
SELECT CONCAT('DROP TABLE IF EXISTS `goarchive`.`', table_name, '`;')
FROM information_schema.tables
WHERE table_schema = 'goarchive' AND table_name LIKE 'archiver_job_log_%';

TRUNCATE TABLE goarchive.archiver_job;
```

`DELETE FROM archiver_job` does **not** reset `AUTO_INCREMENT` and is the safer
verb if you only need to remove some rows. The counter is persisted across
restarts in MySQL 8.0.

> `TRUNCATE` requires the `DROP` privilege, which GoArchive's own account does
> not need. Run these as a DBA account.

### Safe: pruning completed log rows

Rows at `log_status = 2` are pure audit history. The forward scan is driven by
the checkpoint, not by the log, so removing completed rows below the checkpoint
does not cause reprocessing.

```sql
-- Verify nothing is in flight first — this must return 0.
SELECT COUNT(*) FROM goarchive.archiver_job_log_42 WHERE log_status IN (0, 1, 3);

-- Then prune in chunks to keep the transaction small.
DELETE FROM goarchive.archiver_job_log_42 WHERE log_status = 2 LIMIT 50000;
-- repeat until 0 rows affected
```

**What you lose:** the record of which root PKs were archived. If that trail has
compliance value, export it before pruning.

### Safe: truncating a log table for a fully completed job

If a job has finished, has no non-terminal rows, and you do not need the audit
trail:

```sql
-- Must return 0.
SELECT COUNT(*) FROM goarchive.archiver_job_log_42 WHERE log_status IN (0, 1, 3);

TRUNCATE TABLE goarchive.archiver_job_log_42;
```

The checkpoint in `archiver_job` is untouched, so the next run resumes from where
it left off. `TRUNCATE` on the log table is safe — unlike on `archiver_job`, its
`AUTO_INCREMENT` has no external meaning.

### Unsafe: deleting non-terminal log rows

Removing `log_status IN (0, 1)` rows discards recovery information.

- Rows **below** the checkpoint are never re-fetched by the forward scan, so
  their work is silently lost — for `archive`, that can mean source rows copied
  but never deleted, or discovered but never copied.
- Rows **above** the checkpoint get re-processed from scratch, which for a
  strict-INSERT job aborts on duplicate keys.

Resolve non-terminal rows by **re-running the job**, not by deleting them.

### Retiring a job completely

To remove a job and all its state — for example, a job name you no longer use:

```sql
-- 1. Confirm it is not running.
SELECT job_name, job_status,
       TIMESTAMPDIFF(SECOND, last_heartbeat_at, UTC_TIMESTAMP()) AS heartbeat_age_sec
FROM goarchive.archiver_job WHERE job_name = 'archive_old_orders';

-- 2. Drop its log table (substitute the id).
DROP TABLE IF EXISTS goarchive.archiver_job_log_42;

-- 3. Remove the job row. DELETE, not TRUNCATE — see the warning above.
DELETE FROM goarchive.archiver_job WHERE job_name = 'archive_old_orders';
```

**What this means for a re-created job of the same name:** the checkpoint is gone,
so the next run scans from the beginning of the job's `where` clause.

| Command | Consequence of a lost checkpoint |
|---------|----------------------------------|
| `archive`, `purge` | Mostly harmless — the rows were deleted from source, so the scan no longer matches them. Costs a full re-scan. |
| `copy-only` | **Re-copies everything.** Idempotent under `verification.method: sha256` (`INSERT IGNORE`), but a strict-INSERT job — `count`, `--skip-verify`, or a destination unique index — **aborts on duplicate keys**. |

### Cleaning up orphaned log tables

Log tables are not dropped when a job row is deleted. Find them with the
[orphan query](#inspection-cookbook), confirm the id is genuinely retired, then
drop them. Do this **before** any operation that could reset the `AUTO_INCREMENT`
counter.

### Quick reference

| Operation | Safe? | Condition |
|-----------|:-----:|-----------|
| `DELETE ... WHERE log_status = 2` | ✅ | Job not running |
| `TRUNCATE archiver_job_log_<id>` | ✅ | No rows in status 0/1/3 |
| `DROP TABLE archiver_job_log_<id>` | ✅ | Job retired, or job row also deleted |
| `DELETE FROM archiver_job WHERE job_name = …` | ⚠️ | Loses the checkpoint — see table above |
| `DELETE ... WHERE log_status IN (0,1)` | ❌ | Discards recovery state |
| `TRUNCATE archiver_job` alone | ❌ | Reuses ids and adopts stale log tables |
| `DROP TABLE goarchive_meta` alone | ❌ | Discards the declared schema revision |
| Anything while `job_status = 1` and heartbeat fresh | ❌ | Job is live |

---

## Clearing a stale running job

A crashed run leaves `job_status = 1` with an ageing heartbeat. GoArchive's
same-root concurrency check then blocks new runs and tells you exactly this:

```
cannot run archive on root_table "orders": stale running job(s) detected:
[archive_old_orders (heartbeat 3841s ago)]. This indicates a prior crashed run.
Manually inspect and clear with:
  UPDATE `goarchive`.`archiver_job` SET job_status = 0 WHERE job_name = '<name>';
```

**Before running that `UPDATE`, verify the previous process is actually dead.**
A stale heartbeat does not release `GET_LOCK()` — the advisory lock is released
only when the owning MySQL *session* closes.

```sql
-- Is anything still connected and holding the lock?
SELECT * FROM performance_schema.metadata_locks WHERE object_type = 'USER LEVEL LOCK';
SHOW PROCESSLIST;
```

Once confirmed dead:

```sql
UPDATE goarchive.archiver_job SET job_status = 0 WHERE job_name = 'archive_old_orders';
```

Then simply re-run the job — the non-terminal log rows drive recovery. Do not
clear the log rows.

`--force` automates the heartbeat side of this, but it **cannot** steal a held
advisory lock for `archive` or `purge`, and it is a best-effort takeover, not
proof the old process is gone. See
[Concurrency and locking](README_OPERATIONS.md#concurrency-and-locking).

---

## Tracking-schema version marker

`goarchive_meta` declares which revision of the tracking-table contract a schema holds:

```sql
CREATE TABLE IF NOT EXISTS goarchive_meta (
    id TINYINT NOT NULL PRIMARY KEY,
    schema_version VARCHAR(16) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;
```

It has one authoritative row, always `id = 1`. The value is the **tracking-schema revision**,
not the binary version: a release bumps it when it changes the tables' layout **or the meaning
of a column**, and leaves it alone otherwise. GoArchive 2.2 writes `schema_version = '2.2'`;
2.0 and 2.1 wrote `'2.0'` (2.2 made `last_heartbeat_at` a UTC wall-clock).

**The rules are deliberately simple.** Each GoArchive binary recognises exactly one revision —
its own. At startup it

- **stamps** a fresh schema — no marker, and no rows in `archiver_job`;
- **refuses** anything else: a different revision, or populated tracking tables with no marker
  (written by a release older than 2.0).

There is no inferred revision and no automatic migration. Every refusal is resolved by one of
the manual procedures below. The refusal reads:

```
tracking tables in schema "goarchive" report schema_version "2.0", which this
GoArchive does not recognize (it requires schema_version "2.2").
GoArchive never migrates or infers a tracking-schema revision. Stop every
GoArchive process that uses this schema (any version), then follow the matching
row of "Tracking-schema upgrade procedures" in docs/README_JOBS_SCHEMA.md
```

### Tracking-schema upgrade procedures

Run the matching row **after stopping every GoArchive process that uses the schema, whatever
its version** — a 2.1 job still running would keep writing local-time heartbeats.

| Schema reports | Procedure |
|---|---|
| `schema_version = '2.0'` (written by 2.0 or 2.1) | `UPDATE goarchive.archiver_job SET last_heartbeat_at = NULL;`<br>`UPDATE goarchive.goarchive_meta SET schema_version = '2.2' WHERE id = 1;` |
| no marker, populated tables with an integer `id` column (1.8) | Run the new binary once — it creates `goarchive_meta` and refuses — then:<br>`UPDATE goarchive.archiver_job SET last_heartbeat_at = NULL;`<br>`INSERT INTO goarchive.goarchive_meta (id, schema_version) VALUES (1, '2.2');` |
| no marker, no integer `id` column (older than 1.8) | Unsupported directly. Upgrade to **1.8 first**, then follow the row above — or drain every in-flight job and drop the old `archiver_job_log` / `archiver_job` tables, discarding every checkpoint (see [Retiring a job](#maintenance-what-is-safe-to-delete)). |
| a revision newer than the binary understands | Upgrade the GoArchive binary, or point `job_schema` at a different schema. |

**Why heartbeats are voided rather than converted.** `last_heartbeat_at` is a `DATETIME` and
carries no zone. Values written before 2.2 are in whatever zone the old session used; 2.2 cannot
know it, and a wrong guess would make a dead job look live — or a live one dead — to `--force`
and the same-root check. `NULL` is what both checks already treat as *stale*, so a voided
heartbeat can never masquerade as live. Checkpoints, statuses and per-job log tables are
untouched; resume behaves exactly as before. If you know the zone the old sessions used, you may
`CONVERT_TZ(last_heartbeat_at, '<old zone>', '+00:00')` instead of voiding.

---

## Backup and replication notes

- **Tracking tables are InnoDB and fully transactional.** They are safe to
  include in a normal logical or physical backup of the destination.
- **Back them up with the archive data, not separately.** A backup where the
  archived rows and the tracking state come from different points in time can
  make a restored job's checkpoint disagree with what is actually in the
  destination.
- **Restoring destination data without its tracking tables** leaves jobs with no
  checkpoint — see the re-copy consequences above.
- **Tracking writes are frequent but small**: one heartbeat `UPDATE` every 15
  seconds, plus batched multi-row `INSERT IGNORE`/`UPDATE` statements sized by
  `batch_size`. On a replicated destination this is negligible next to the
  archived row volume itself.
- **Isolating `job_schema`** into its own schema makes tracking state easy to
  exclude from archive-data dumps, and easy to grant separately. See
  [`job_schema`](README_CONFIGURATION.md#job_schema-destination-only).
