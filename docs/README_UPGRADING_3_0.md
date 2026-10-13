# Upgrading to 3.0

What changed from 2.x, and how to move a job to 3.0.

**Related:** [Configuration](README_CONFIGURATION.md) · [Operations](README_OPERATIONS.md) ·
[Limitations](README_LIMITATIONS.md) · [Tracking tables](README_JOBS_SCHEMA.md) ·
[← Back to README](../README.md)

---

## Contents

- [The short version](#the-short-version)
- [Moving from 2.2](#moving-from-22)
- [Moving from 3.0 alpha.0](#moving-from-30-alpha0)
- [Configuration changes](#configuration-changes)
- [Command-line changes](#command-line-changes)
- [Limits removed or changed](#limits-removed-or-changed)
- [Coming from a release before 2.2](#coming-from-a-release-before-22)

---

## The short version

1. Finish every job with your 2.x binary, then drop the 2.x tracking tables
   ([Moving from 2.2](#moving-from-22)). 3.0 never reads or migrates them.
2. Rewrite the configuration: every job needs `job_type`, `verification` is one value, and
   unknown keys are refused ([Configuration changes](#configuration-changes)).
3. Drop `--force` and `--skip-verify` from your scripts
   ([Command-line changes](#command-line-changes)).
4. Run `goarchive validate`; 3.0's preflight checks keys and relations more strictly than 2.x.

A 2.x configuration fails to load with
`GoArchive 3.0 changed the configuration format; see docs/README_UPGRADING_3_0.md`, and 2.x
tracking tables are refused at startup with a pointer to the next section.

---

## Moving from 2.2

3.0's tracking tables are new: a typed log per job and a job binding, with no checkpoint
([Tracking tables](README_JOBS_SCHEMA.md)). A 2.x schema is refused, whatever its label, and
nothing is migrated.

1. **Finish every 2.x job with the 2.x binary**, so no log row is left `pending` or `copied`.
   Retire a job you cannot finish, as the 2.x documentation describes.
2. **Stop every GoArchive process** that uses the tracking schema.
3. **Drop the 2.x tracking tables** in `job_schema`, as a DBA (GoArchive's account holds no
   `DROP`):

   ```sql
   -- Generate the DROP statements for every log table, review them, run them.
   SELECT CONCAT('DROP TABLE `', table_schema, '`.`', table_name, '`;')
   FROM information_schema.tables
   WHERE table_schema = 'goarchive' AND table_name LIKE 'archiver_job_log%';

   DROP TABLE goarchive.archiver_job;
   DROP TABLE goarchive.goarchive_meta;
   ```

   Export the 2.x logs first if the record of what was archived has compliance value. Or point
   `destination.job_schema` at a new, empty schema instead.
4. **Run 3.0.** The first run labels the schema `3.0` and registers each job afresh. Every run
   starts at the beginning of the job's `where` range: for archive and purge the finished rows
   are gone from the source; a copy-only job would copy everything again, so point it at an empty
   destination, or give it a `where` that starts after what 2.x copied.

---

## Moving from 3.0 alpha.0

The lock names changed: alpha.0 locked `goarchive:job:<name>` and `goarchive:root:<table>` on
the destination; later releases lock a hashed job name on the destination and a hashed name for
every table of the tree on the source (`GET_LOCK()` names, not table locks). The two do not see
each other's locks. **Stop every alpha.0 run before starting a run of a later release.** Mixed,
the same job can run twice, and jobs whose trees overlap run together; the outcome is a loud
failure (a duplicate key, or a row-count refusal), but it is the stop that prevents it. A later
release's run is still refused while an alpha.0 job whose tree shares one of its tables is
marked running in the shared registry. Nothing at startup looks for alpha.0's locks.

To see live alpha.0 runs, as a DBA account on each destination:

```sql
SELECT OBJECT_NAME FROM performance_schema.metadata_locks
WHERE OBJECT_TYPE = 'USER LEVEL LOCK' AND OBJECT_NAME LIKE 'goarchive:%';
```

Names starting `goarchive:job:` or `goarchive:root:` are alpha.0 runs. The tracking tables are
unchanged.

**Stopped jobs now hold their tables.** A job whose log holds `copied` or `deleting` roots, in
any status, refuses the runs of other jobs whose trees share one of its tables, until its own
rerun finishes them ([Concurrency and locking](README_OPERATIONS.md#concurrency-and-locking)).
Under alpha.0 a failed or idle job blocked nobody, and a running one blocked only its own root
table. Before upgrading, finish the unfinished work of every job: list the jobs, then each
log's pending roots, and rerun every job whose count is not 0.

```sql
SELECT id, job_name, job_type, job_status FROM goarchive.archiver_job ORDER BY id;
SELECT COUNT(*) FROM goarchive.archiver_job_log_42 WHERE ga_status IN (1, 4);  -- per id
```

Two jobs that share a table and both still hold such roots refuse each other's reruns; finish
one by [abandoning its roots](README_JOBS_SCHEMA.md#abandoning-logged-roots), then rerun the
other.

**Destination tables must be InnoDB.** `archive` and `copy-only` refuse a destination table on
another engine ([`DEST_STORAGE_ENGINE_CHECK`](README_VALIDATION.md#dest_storage_engine_check)),
and `validate` and `dry-run` report it; convert it with `ALTER TABLE … ENGINE=InnoDB`. A destination created on a server whose
`default_storage_engine` is not InnoDB is the usual cause. **`--skip-validate-preflight` is
renamed** `--skip-dangerously-validate-preflight-allow-experimental`; the old name is refused.

**A destination whose columns are in another order is accepted.** alpha.0 refused it with
`DEST_SCHEMA_COMPATIBILITY_CHECK`; later releases log one warning per table naming the displaced
columns, because every read and INSERT names its columns
([Schema compatibility rules](README_VALIDATION.md#schema-compatibility-rules)).

---

## Configuration changes

The file is read strictly ([how errors are reported](README_CONFIGURATION.md#how-configuration-is-loaded)).

- **Every job needs `job_type`:** `archive`, `purge` or `copy-only`. A job runs only under that
  command.
- **`primary_key`, `foreign_key` and `references` may be lists**, for composite keys.
  `primary_key` must name exactly the table's `PRIMARY KEY` columns, in key order; `references`
  names the parent's key columns, paired with `foreign_key` by position, and may be omitted only
  when the parent key has one column.
- **`verification` is one value:** `sha256` (the default), `count` or `none`, globally or per
  job. The 2.x block (`method`, `skip_verification`) is refused:

  | 2.x | 3.0 |
  |-----|-----|
  | `method: count` | `verification: count` |
  | `method: sha256` | `verification: sha256` (the default) |
  | `skip_verification: true` | `verification: none` |

- **Each job type accepts only the settings it uses.** A purge job with `verification`, or a
  copy-only job with `batch_delete_size` or `delete_sleep_seconds`, is refused. `resume` is
  copy-only's alone.
- **Unknown keys are refused.** Key names are exact and lowercase: 2.x accepted `Root_Table`;
  3.0 refuses it. A helper key such as `x-defaults:` is refused too, so a YAML anchor shared by
  merge keys must live under a known key, for example a real job.
- **Job names keep their case**, so the name on the command line must match the file exactly,
  and two names that differ only in case are refused; names must be printable ASCII
  ([the rule](README_CONFIGURATION.md#jobs)).
- **A table may appear once in a job.**
- **One YAML document per file:** a second document after `---` is refused rather than ignored.
- **The 2.0 lag keys are refused** (they were removed in 2.1):

  | 2.0 | 3.0 |
  |-----|-----|
  | `replica:` (one server) | `replication.servers:` (a list) |
  | `replica.host` / `port` / `user` / `password` | `replication.servers[].host` / `port` / `user` / `password` |
  | `replica.replication_channel: "billing"` | `replication.servers[].channels: ["billing"]` |
  | `replica.replication_channel: ""` | `replication.servers[].channels: [""]`, the default channel only; omitted or `[]` gates on every channel |
  | `safety.lag_threshold` | `replication.seconds_behind_source_within` |
  | `safety.check_interval` | `replication.check_interval` |

Every option: [Configuration](README_CONFIGURATION.md).

---

## Command-line changes

| 2.x | 3.0 |
|-----|-----|
| `--skip-verify` | `--verification=none`; `--verification=<sha256\|count\|none>` overrides the file for one run |
| `--force` | Removed. A copy-only rerun uses `resume: true`; a held job lock is never taken over |
| `dry-run` inserted a sample into the destination and rolled it back | `dry-run` writes nothing: it prints MySQL's plans for the first batch's statements |
| `validate` ran every check on every job | `validate` checks each job under its `job_type` |

---

## Limits removed or changed

| 2.x | 3.0 |
|-----|-----|
| Single-column primary keys only (`COMPOSITE_PK_CHECK`); a foreign key spanning several columns between job tables was refused | Composite primary keys and composite foreign keys are supported at the root and at every child |
| Root keys had to be integers (`ROOT_PK_TYPE_UNSUPPORTED`) | Root keys may be any supported key type: integer, `DECIMAL`, string, binary or temporal. `FLOAT`/`DOUBLE`, `ENUM`/`SET`, `BIT` and prefix key parts are refused (`KEY_TYPE_UNSUPPORTED`) |
| A job continued from a persisted checkpoint, so eligibility had to grow in primary-key order, and a row that became eligible below the checkpoint was never archived | There is no checkpoint: every run starts at the beginning of the `where` range, and a row that becomes eligible later is found by the next run |
| Recovery replayed `pending` roots in full and `copied` roots as deletes, behind four resume gates | A batch's copy, verification and log are one transaction, so only archive's `copied` and purge's `deleting` roots are replayed, as deletes; the gates are gone |
| A job name was bound to its command and root table only | A job is also bound to its root key, and to its source server and schema and its relations (purge: also its `where`; archive and copy-only: also their destination database). A changed root key is refused; a changed binding is refused while the job has logged work that depends on it ([Startup checks](README_JOBS_SCHEMA.md#startup-checks)) |
| `verification.method` chose the INSERT strategy (`INSERT IGNORE` under sha256) | Every copy is a strict INSERT: a row already in the destination fails the batch |
| Community and Enterprise editions; the version carried `-community` | One edition, and the version carries no edition suffix |

---

## Coming from a release before 2.2

3.0 keeps what 2.0–2.2 introduced. If you skip those releases:

- **Grants must be provable** (2.0): each privilege granted directly to the account, and
  `PROCESS` on the source for archive and purge ([Permissions](README_PERMISSIONS.md)).
- **Sessions are UTC** (2.2): `where` runs in a UTC session, so a `DATETIME` column compared with
  `NOW()`, a `TIMESTAMP` column compared with a bare literal, and `CURDATE()` boundaries sit at
  UTC rather than the server's zone ([`where`](README_CONFIGURATION.md#where-is-mandatory)).
  `last_heartbeat_at` is UTC: measure its age with `UTC_TIMESTAMP()`.
- **`TIMESTAMP` values archived before 2.2 may be wrong** when the source and destination
  sessions ran in different zones: each copy took the source's wall-clock text and the
  destination read it in its own zone. A DBA can repair them **once**:

  ```sql
  -- 1. Back the table up.
  -- 2. A non-zero count means a zone name is not recognised and the UPDATE would NULL
  --    the column: stop.
  SET time_zone = '+00:00';
  SELECT COUNT(*) FROM archive.orders
   WHERE <only rows archived before 2.2>
     AND ts IS NOT NULL
     AND CONVERT_TZ(CONVERT_TZ(ts, '+00:00', '<zone the old DESTINATION sessions used>'),
                    '<zone the old SOURCE sessions used>', '+00:00') IS NULL;

  -- 3. Repair, exactly once per column.
  UPDATE archive.orders
     SET ts = CONVERT_TZ(CONVERT_TZ(ts, '+00:00', '<zone the old DESTINATION sessions used>'),
                         '<zone the old SOURCE sessions used>', '+00:00')
   WHERE <only rows archived before 2.2>;
  ```

  Name the zones as the old sessions saw them (`SYSTEM` means the server's OS zone). A second run
  corrupts the values again. Values that fell in a daylight-saving fall-back hour cannot be
  recovered.
