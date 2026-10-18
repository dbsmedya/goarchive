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
- [Moving from 3.0 alpha.1](#moving-from-30-alpha1)
- [Moving from 3.0 alpha.2](#moving-from-30-alpha2)
- [Moving from 3.0 beta.1](#moving-from-30-beta1)
- [Moving from 3.0 beta.2 (unreleased)](#moving-from-30-beta2-unreleased)
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

## Moving from 3.0 alpha.1

The tracking tables are unchanged, and a job stopped under alpha.1 is recovered by its next run.

**Archive recovery checks the destination before it deletes.** A `copied` root's tree is
rediscovered from the source, and each of its rows must be in the destination before any of it
is deleted. A row the destination lacks, typically one written into the tree after the copy,
stops the run with `UncopiedError`, which names the rows and the remedies; nothing is deleted
([Crash recovery](README_OPERATIONS.md#crash-recovery)). Recovery reads the destination even
under `verification: none`, so the archive account needs `SELECT` on the destination tables
([privileges](README_PERMISSIONS.md)); the recipes already grant it.

**An archive job is bound to its destination database** while its log holds `copied` roots:
a changed `destination.database` is refused at startup
([Startup checks](README_JOBS_SCHEMA.md#startup-checks)). Finish the job before changing it.

**Rows and INSERTs larger than `max_allowed_packet` are copied.** GoArchive reads each server's
`max_allowed_packet` when it connects (one `SELECT @@max_allowed_packet`, no privilege needed),
where alpha.1's driver assumed 64 MiB. A destination below 64 MiB dropped the connection over a
large INSERT (`broken pipe`); now only one value larger than the destination's packet, or rarely
a whole INSERT, fails, with an error naming the table and the remedy
([the limit](README_LIMITATIONS.md#a-value-larger-than-the-destinations-max_allowed_packet-fails-its-batch)).
[`MAX_ALLOWED_PACKET_CHECK`](README_VALIDATION.md#max_allowed_packet_check) no longer caps a key
statement at 64 MiB: the server's value is the only limit.

**Logging.** A new log file is created readable by its owner only (`0600`) on Unix; on Windows
it inherits its folder's permissions. An existing file keeps its mode. A run's log ends with one entry saying how it ended (`Run completed`,
`Run stopped …` or `Run failed`) and its counts
([logging](README_CONFIGURATION.md#logging)).

---

## Moving from 3.0 alpha.2

The tracking tables are unchanged. A job stopped under alpha.2 is recovered by its next run once
it passes the two checks below.

**Grant `TRIGGER` directly before upgrading.** MySQL lists a table's triggers only to an account
holding `TRIGGER` on it. An alpha.2 account set up from the old recipes saw an empty list and ran
past triggers it could not see. Two new checks now refuse the job unless `TRIGGER` is granted
directly on every job table, at `<schema>.*` or table scope:

- `DELETE_TRIGGER_VISIBILITY_CHECK`: the source account, for archive and purge.
- `DEST_INSERT_TRIGGER_VISIBILITY_CHECK`: the destination account, for archive and copy-only.

A `TRIGGER` held only through a role, or granted globally under `partial_revokes`, cannot be
proven and is refused; grant it directly as well. The [grant recipes](README_PERMISSIONS.md#grant-recipes)
include it, and [the trigger checks](README_VALIDATION.md#trigger-checks) say what each refusal
names. `--force-triggers` does not cover these checks.

**Archive and purge refuse a self-referencing `CASCADE` or `SET NULL`.** A job table, root or
child, with a foreign key to itself whose rule is `ON DELETE CASCADE` or `SET NULL` is refused
before any row moves ([`SELF_FK_DELETE_RULE_CHECK`](README_VALIDATION.md#self_fk_delete_rule_check)).
Deleting a batch row would have deleted, or cleared the parent of, rows of that table outside the
batch, uncopied. alpha.2 accepted such a job. A job alpha.2 left interrupted on such a table is
refused before recovery deletes anything, until the constraint is redefined with
`ON DELETE RESTRICT`. Rows that InnoDB already deleted or changed under alpha.2 are not in the
destination and cannot be recovered by GoArchive. Copy-only is unaffected.

**Every session pins `sql_select_limit`** to no limit
([session settings](README_CONFIGURATION.md#session-settings)). A server's global
`sql_select_limit` used to cap GoArchive's reads, so a table or a tree wider than it was copied,
verified and deleted only in part. Nothing to do.

---

## Moving from 3.0 beta.1

The tracking tables are unchanged. What changes is what goarchive prints, where, and how it exits
([Output and exit codes](README_OPERATIONS.md#output-and-exit-codes)), and how a replication hold
ends.

**Exit codes.** A graceful stop now exits **3**, not 0; "another run holds the work" (the job,
a source table, or unfinished work on the same data) exits **4**; a usage or configuration error
found before anything connected exits **2**; every other failure still exits 1. A script that
treated any non-zero code as a failure, or a stop as success, needs the table.

**A run prints to stdout only with `--progress`**, or when the log itself is on stdout. With a log
file and no `--progress`, a run is silent unless it fails; the failure prints a summary and the
error on stderr. A script that read the report from stdout passes `--progress`, or reads the log's
closing entry, which carries the same counts.

**A log file's console tee moved to stderr, as text, and only with `--progress`**, showing
warnings and live events; info entries such as `Batch N` stay in the file. A script that parsed
JSON from stdout under a file log reads the file.

**The `WARNING:` banners moved to stderr** and are also recorded in the log; `dry-run` and
`validate` print them too. `version`, `list-jobs` and the help print on stdout.

**The closing block** (with `--progress`) starts `Job <name> (<type>): <status> in <seconds>s`
and lists the counts as a table; the `elapsed:` line is gone.

**`dry-run` prints a new report**: the job and its coverage, the orders with the first batch's rows,
the settings, the caps, then one section per statement. It now counts the `where` range, a read of
the whole range on the source.

**`--progress`'s total is MySQL's estimate, not a count.** beta.1 counted the whole `where` range
before batch 1, a read of the range on the source that could outlast the copy. The total after `~`
is now the optimizer's estimate from one `EXPLAIN` and can be off in either direction: past it the
ETA shows `unknown`, and a run that finishes shows its done count as the total
([Progress display](README_OPERATIONS.md#progress-display)).

**Text logs have no colour codes.** `validate` and `dry-run` no longer log the statement caps they
print; a run logs them.

**A long replication hold no longer fails the run.** A hold that outlasted the source session's
`wait_timeout` (8 hours or more) failed the next delete chunk with error 4031; each delete chunk
now takes its session after the hold, so a hold of any length resumes when the replicas do.

**A first `SIGTERM` or `SIGINT` ends a replication hold**, with exit code 3, instead of waiting
for the replicas; the next run's recovery finishes the held batch. A `docker stop` grace period
no longer has to cover a hold, only a replica check in progress when the hold begins
([Graceful shutdown](README_OPERATIONS.md#graceful-shutdown)).

**Two runs starting together in a new tracking schema both start.** One of them could fail
with MySQL error 1213 (deadlock) while both created the tracking tables; now one creates them
under a schema lock and the other waits for it, up to 60 seconds. Exit code 4 has one more
cause: that lock still held after the wait
([Concurrency and locking](README_OPERATIONS.md#concurrency-and-locking)).

## Moving from 3.0 beta.2 (unreleased)

The tracking tables are unchanged.

**Exit code 4 now means a live run only.** A run refused because a stopped job's unfinished
work, or a job marked running whose heartbeat stopped, holds its tables now exits 1, not 4: that
block waits for the stopped job's rerun, by its schedule or by an operator, and the error names
the remedy. A script that treats 4 as "try later" now sees these as failures
([Output and exit codes](README_OPERATIONS.md#output-and-exit-codes)).

**A job column in an unsupported character set is now refused.** Every session is now pinned to
`utf8mb4`, and preflight refuses a job column whose character set does not survive it exactly,
such as `cp932`, `sjis` or `cp1250`, with `COLUMN_CHARSET_CHECK`: through the `utf8mb4` session two
of its keys could merge and its text change, silently. Convert the column to `utf8mb4`
([Character sets](README_CHARSETS.md): which sets, and how to convert).

**A server whose `init_connect` narrows the session now works.** GoArchive's `SET NAMES` runs after
`init_connect`, and `SESSION_CHARSET_CHECK` refuses a session something else narrows
([the check](README_VALIDATION.md#session_charset_check)).

**The image runs non-root, on a supported Alpine base.** The published image runs as UID 65532
from `/work`, on `alpine:3.24.2` (was 3.19, no longer supported), and the binary moves to
`/usr/local/bin`. A config mounted under `/root/` is no longer readable: mount it at `/work/`,
or anywhere readable and enterable by the user the container runs as. On a Linux host, a config
file readable by you alone (`chmod 600`), which root read before, now needs
`--user "$(id -u):$(id -g)"` or a group-readable mode, and the same `--user` makes a log file
written to a mounted directory yours
([Run the published image](../INSTALL.md#run-the-published-image)).

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
- **A new log file is readable by its owner only** on Unix (`0600`; 2.x created it `0644`); on
  Windows it inherits its folder's permissions. An existing file keeps its mode, so a log reader
  running as another user needs the file created first
  ([logging](README_CONFIGURATION.md#logging)), and a log 2.x created keeps `0644` until you
  `chmod 0600` it.

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
