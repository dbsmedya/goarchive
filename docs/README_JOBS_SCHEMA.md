# Job Tracking Schema — DBA Maintenance Guide

How GoArchive stores job state on the destination, what the tracking tables hold, and what a
DBA may change in them.

**Related:** [Configuration](README_CONFIGURATION.md) ·
[Operations](README_OPERATIONS.md) · [Permissions](README_PERMISSIONS.md) ·
[Validation & Preflight](README_VALIDATION.md) · [← Back to README](../README.md)

---

## Contents

- [Where tracking state lives](#where-tracking-state-lives)
- [`archiver_job`](#archiver_job)
- [`archiver_job_log_<id>`](#archiver_job_log_id)
- [Startup checks](#startup-checks)
- [Lifecycle](#lifecycle)
- [Inspection cookbook](#inspection-cookbook)
- [Maintenance: what is safe to delete](#maintenance-what-is-safe-to-delete)
- [Abandoning logged roots](#abandoning-logged-roots)
- [Re-binding a job after a failover](#re-binding-a-job-after-a-failover)
- [Retiring a job completely](#retiring-a-job-completely)
- [Clearing a stale running job](#clearing-a-stale-running-job)
- [Tracking-schema version marker](#tracking-schema-version-marker)
- [Backup and replication notes](#backup-and-replication-notes)

---

## Where tracking state lives

All tracking state lives on the **destination** server, in the schema named by
[`destination.job_schema`](README_CONFIGURATION.md#job_schema-destination-only) (default: the
destination database):

| Object | Cardinality | Holds |
|--------|-------------|-------|
| `archiver_job` | one table, one row per job | The registry: type, root table, binding, status, heartbeat |
| `archiver_job_log_<id>` | one table per job | Each logged root key and its status |
| `goarchive_meta` | one table, one row | The tracking schema's label, `3.0` |

GoArchive creates the tables but never the schema, and its account holds no `DELETE` or `DROP`
there ([grants](README_PERMISSIONS.md#grant-recipes)): every deletion below is a DBA's act. The
log table is named by the job's `id`:

```sql
SELECT id, job_name, root_table, job_type FROM goarchive.archiver_job;
-- job id 42  ->  goarchive.archiver_job_log_42
```

---

## `archiver_job`

Created at the first run in the schema.

```sql
CREATE TABLE IF NOT EXISTS archiver_job (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  job_name VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
  root_table VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  job_type VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  job_status TINYINT NOT NULL DEFAULT 0,
  source_server_uuid VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  source_database VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  destination_database VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  destination_server_uuid VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
  config_fingerprint TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  root_key_columns TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  last_heartbeat_at DATETIME NULL,
  UNIQUE KEY uk_job_name (job_name),
  KEY idx_status (job_status)
) ENGINE=InnoDB
```

| Column | Meaning |
|--------|---------|
| `id` | Surrogate key; **names the log table**, `archiver_job_log_<id>` |
| `job_name` | The job's name in the config: printable ASCII, unique **without regard to letter case** |
| `root_table` | The root table; fixed for the job's life |
| `job_type` | `archive`, `purge` or `copy-only`; fixed for the job's life |
| `job_status` | `0` idle, `1` running, `3` failed (the last run ended in an error) |
| `source_server_uuid`, `source_database` | The source the job's log rows came from |
| `destination_database` | The database the copies went to; it can differ from `job_schema` |
| `destination_server_uuid` | Reserved for binding the destination server; not written by this release (`''`) |
| `config_fingerprint` | The relations (parents, tables, column pairs) and the `where` text, as JSON |
| `root_key_columns` | The root key's column names, in key order, as JSON; empty while registration is incomplete |
| `last_heartbeat_at` | A UTC wall clock refreshed every 15 seconds while running ([timing](README_OPERATIONS.md#heartbeat-and-keepalive-timing)); measure its age with `UTC_TIMESTAMP()`, never `NOW()` |

The source and destination columns and `config_fingerprint` are the job's **binding**: the
facts that give its log rows their meaning ([Startup checks](#startup-checks)).

---

## `archiver_job_log_<id>`

Created when the job is registered, typed by the root key: one column `k1…kN` per root key
column, in key order, with the column's own type, charset and collation (a `TIMESTAMP` is stored
as `DATETIME` of the same precision, so no session time zone can shift it). For a root key
`(order_id BIGINT UNSIGNED, line_no INT)`:

```sql
CREATE TABLE IF NOT EXISTS archiver_job_log_42 (
  `k1` bigint unsigned NOT NULL COMMENT 'order_id',
  `k2` int NOT NULL COMMENT 'line_no',
  ga_status TINYINT NOT NULL,
  ga_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`k1`, `k2`),
  KEY idx_ga_status (ga_status)
) ENGINE=InnoDB
```

Each comment names the root column, for people; GoArchive never reads it. The table must stay
InnoDB, with `PRIMARY KEY (k1…kN)` and the two `ga_` columns as created: a log whose shape
changed is refused at startup. Only roots are logged, never child rows.

### `ga_status` values

| Value | Name | Written |
|-------|------|---------|
| `1` | copied | archive, in the destination transaction with the batch's copy and verification; source rows not yet all deleted |
| `2` | completed | archive and purge after their deletes; copy-only with its copy |
| `4` | deleting | purge, in its own transaction, before its deletes |

Each status is written in the transaction that does the work it records. What a rerun does with
them: [Crash recovery](README_OPERATIONS.md#crash-recovery).

---

## Startup checks

Before anything is recovered or run, every run checks its job against the registry. These are
bookkeeping integrity checks: no flag skips them.

1. **The job is fixed.** A different `job_type` or `root_table` under an existing name, or a
   name that differs from a stored one only in letter case, is refused: use a new name, or
   [retire the job](#retiring-a-job-completely).
2. **The root key is fixed.** `root_key_columns` and the log's `k1…kN` must match the root
   table's current primary key: names, order, types, charsets and collations. A changed key is
   refused, since the log cannot record it: retire the job or use a new name.
3. **The binding.** While the job has **pending work** — archive and purge: any `copied` or
   `deleting` root; copy-only: any `completed` root, whatever its `resume` setting — a change to
   the parts that work depends on is refused, naming the parts and the number of roots:

   | Job type | Compared |
   |----------|----------|
   | archive | source server and database, relations |
   | purge | source server and database, relations, `where` |
   | copy-only | source server and database, relations, destination database |

   The refusal lists three ways out: restore the original config or source and run once; or
   [abandon the roots](#abandoning-logged-roots); or, after a source failover,
   [re-bind the job](#re-binding-a-job-after-a-failover). With no pending work, the stored
   binding is overwritten with the current one, so changes between clean runs are free.

**Registration** is two steps, because the log is named by the job's id: the job row is written
with an empty `root_key_columns`, then the log is created, and only then are the key's names
written. A run that finds an empty `root_key_columns` completes the registration. A table that
already exists under the new log's name and holds rows, or has another shape, was left by a
dropped or truncated registry: it is never adopted, and the job stays unregistered until a DBA
drops that table (after confirming no job uses it) or restores `archiver_job`.

---

## Lifecycle

1. **First run in the schema** — `goarchive_meta` and `archiver_job` are created; a fresh schema
   is labelled `3.0` ([version marker](#tracking-schema-version-marker)).
2. **First run of a job** — the job is registered and its log created.
3. **During a run** — `job_status = 1`, the heartbeat refreshes, log rows are written per batch.
4. **The end of a run** — `job_status` returns to `0`, or `3` after an error. A crash or kill
   leaves `1` with a stale heartbeat ([Clearing a stale running job](#clearing-a-stale-running-job)).

Nothing is cleaned up automatically: the log is the record of what left production.

---

## Inspection cookbook

**All jobs**

```sql
SELECT id, job_name, root_table, job_type, job_status,
       TIMESTAMPDIFF(SECOND, last_heartbeat_at, UTC_TIMESTAMP()) AS heartbeat_age_sec
FROM goarchive.archiver_job
ORDER BY id;
```

**Is anything running?** A heartbeat older than 60 seconds is stale.

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
SELECT ga_status,
       CASE ga_status WHEN 1 THEN 'copied' WHEN 2 THEN 'completed' WHEN 4 THEN 'deleting' END AS name,
       COUNT(*) AS roots
FROM goarchive.archiver_job_log_42
GROUP BY ga_status;
```

**Roots the next run will replay** (archive: `1`; purge: `4`)

```sql
SELECT * FROM goarchive.archiver_job_log_42 WHERE ga_status IN (1, 4) ORDER BY k1;
```

**The job's binding**

```sql
SELECT source_server_uuid, source_database, destination_database, config_fingerprint, root_key_columns
FROM goarchive.archiver_job WHERE job_name = 'archive_old_orders';
```

---

## Maintenance: what is safe to delete

> **Rule zero: never touch tracking tables while the job runs.** Confirm `job_status = 0`, or
> `job_status = 1` with a stale heartbeat **and** a verified-dead process
> ([below](#clearing-a-stale-running-job)).

| Operation | Safe? | Condition and consequence |
|-----------|:-----:|---------------------------|
| `DELETE … WHERE ga_status = 2` (archive, purge) | ✅ | Audit history only: there is no checkpoint, and completed roots are gone from the source |
| `DELETE … WHERE ga_status = 2` (copy-only) | ⚠️ | Those roots are no longer skipped by `resume: true`; a rerun re-copies them and fails on the duplicates unless their destination rows are removed too |
| `DELETE … WHERE ga_status IN (1, 4)` | ⚠️ | Abandons recovery: [Abandoning logged roots](#abandoning-logged-roots) |
| `DROP TABLE archiver_job_log_<id>` and `DELETE` its job row | ✅ | [Retiring the job](#retiring-a-job-completely) |
| `TRUNCATE archiver_job` | ❌ | Resets `AUTO_INCREMENT`: new jobs take the ids of old logs, and a job whose old log holds rows stays unregistered until that log is dropped. Drop every log table in the same window, or use `DELETE` |
| `DROP TABLE archiver_job` alone | ❌ | A labelled schema without `archiver_job` is refused: the logs would meet new jobs reusing their ids |
| `DROP TABLE goarchive_meta` alone | ❌ | A populated `archiver_job` without a label is refused until the label row is restored |
| Anything while `job_status = 1` with a fresh heartbeat | ❌ | The job is live |

To clear all tracking state, drop `goarchive_meta`, `archiver_job` and every
`archiver_job_log_<id>` together. Prune large logs in chunks
(`DELETE … WHERE ga_status = 2 LIMIT 50000`, repeated), and export them first if the record has
compliance value. `TRUNCATE` needs `DROP`, which GoArchive's account does not hold: run these as
a DBA.

---

## Abandoning logged roots

When the [binding check](#startup-checks) refuses a run and neither restoring the config nor a
re-bind applies, a DBA can abandon the pending roots by deleting their log rows. The next run
then rebinds the job to the current config. Confirm the job is not running first.

```sql
DELETE FROM goarchive.archiver_job_log_42 WHERE ga_status = 1;   -- archive: copied
DELETE FROM goarchive.archiver_job_log_42 WHERE ga_status = 4;   -- purge: deleting
DELETE FROM goarchive.archiver_job_log_42 WHERE ga_status = 2;   -- copy-only: completed
```

What is left behind, and must be resolved by hand:

| Job type | Consequence |
|----------|-------------|
| archive | The roots' trees are in the destination while some of their source rows may remain. If the new config fetches those roots again, their copy fails on duplicate keys: delete the remaining source rows of those trees, or their destination copies, first |
| purge | Some of the roots' source rows may remain; the new config purges them only if its `where` selects them |
| copy-only | Every root copied before is copied again: into a new destination database that is right; into the same one, remove the old copies first, or use a new job name instead |

---

## Re-binding a job after a failover

After a source failover, the promoted server holds the same data under a new `server_uuid`, and
the binding check refuses a job with pending work. Once you have confirmed it is the same data,
point the job at the new server, then rerun it to finish the logged work:

```sql
-- On the new source:
SELECT @@GLOBAL.server_uuid;
-- On the destination, with that value:
UPDATE goarchive.archiver_job SET source_server_uuid = '<new server_uuid>'
WHERE job_name = 'archive_old_orders';
```

This is a deliberate act: replaying a log against a source that does not hold the same data
deletes or skips the wrong rows.

---

## Retiring a job completely

To remove a job and all its state — a name you no longer use, or one you want to give another
type, root table or root key:

```sql
-- 1. Confirm it is not running.
SELECT job_name, job_status,
       TIMESTAMPDIFF(SECOND, last_heartbeat_at, UTC_TIMESTAMP()) AS heartbeat_age_sec
FROM goarchive.archiver_job WHERE job_name = 'archive_old_orders';

-- 2. Drop its log table (substitute the id).
DROP TABLE IF EXISTS goarchive.archiver_job_log_42;

-- 3. Remove the job row: DELETE, never TRUNCATE.
DELETE FROM goarchive.archiver_job WHERE job_name = 'archive_old_orders';
```

A job created again under the name starts with an empty log. Archive and purge lose nothing but
the record. A copy-only job loses its `resume` skips, so its rerun re-copies every root, and
fails on duplicates in a destination that still holds them.

---

## Clearing a stale running job

A crashed run leaves `job_status = 1` with an ageing heartbeat, and the same-root check refuses
new runs on that root table:

```
engine: the root table "orders" has jobs marked running whose heartbeat stopped:
archive_old_orders (heartbeat 3841s ago). A crashed run leaves its job marked running. When no
process runs them, mark them idle with UPDATE `goarchive`.archiver_job SET job_status = 0
WHERE job_name = '<name>'
```

**Before that `UPDATE`, verify the old process is gone.** A stale heartbeat does not release
the job lock; only the closing of its MySQL session does:

```sql
SELECT * FROM performance_schema.metadata_locks WHERE object_type = 'USER LEVEL LOCK';
SHOW PROCESSLIST;
```

Then mark the job idle and rerun it: its log drives recovery. Do not delete the log rows.

---

## Tracking-schema version marker

```sql
CREATE TABLE IF NOT EXISTS goarchive_meta (
  id TINYINT NOT NULL PRIMARY KEY,
  schema_version VARCHAR(16) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB
```

One row, `id = 1`. The value is the **tracking-schema label**, not the binary version: a release
changes it when it changes the tables' layout **or a column's meaning**. This release writes and
recognises exactly one label, `3.0`. At startup it labels a fresh schema and refuses everything
else; there is no inferred revision and no automatic migration.

### Tracking-schema upgrade procedures

Stop every GoArchive process that uses the schema, whatever its version, then follow the row that
matches the refusal:

| The refusal says the tables… | Procedure |
|---|---|
| report another `schema_version`, or hold a 2.x `archiver_job` | Written by GoArchive 2.x or earlier: [Moving from 2.2](README_UPGRADING_3_0.md#moving-from-22) |
| are labelled 3.0 but have no `archiver_job` table | Restore `archiver_job` from a backup, or drop every tracking table to start afresh |
| hold job rows but no `schema_version` label | After confirming GoArchive 3.0 wrote them: `INSERT INTO goarchive.goarchive_meta (id, schema_version) VALUES (1, '3.0')`; or drop every tracking table |
| report a label newer than this binary | Upgrade GoArchive, or point `job_schema` at another schema |

---

## Backup and replication notes

- Tracking tables are InnoDB and transactional: include them in the destination's normal
  backups, **taken together with the archived data**. A log restored from another point in time
  than the data it describes misleads recovery and copy-only `resume`.
- Restoring the destination data without its tracking tables leaves jobs unregistered: archive
  and purge start afresh; copy-only re-copies everything, and fails on the duplicates.
- Tracking writes are small: a heartbeat `UPDATE` every 15 seconds and one multi-row log
  statement per batch, negligible on a replicated destination next to the archived rows.
- A separate `job_schema` keeps tracking state out of archive-data dumps and is granted on its
  own ([`job_schema`](README_CONFIGURATION.md#job_schema-destination-only)).
