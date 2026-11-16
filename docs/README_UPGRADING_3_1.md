# Upgrading to 3.1

What changed from 3.0 in the tracking tables, and how to move a registry of jobs to 3.1. Coming from 2.x, or from
a 3.0 prerelease: read [Upgrading to 3.0](README_UPGRADING_3_0.md) first.

**Related:** [Upgrading to 3.0](README_UPGRADING_3_0.md) · [Group Replication](README_GROUP_REPLICATION.md) ·
[Tracking tables](README_JOBS_SCHEMA.md) ·
[Operations](README_OPERATIONS.md) · [Limitations](README_LIMITATIONS.md) ·
[← Back to README](../README.md)

---

## Contents

- [The short version](#the-short-version)
- [Moving from 3.0](#moving-from-30)
  - [What the binary can and cannot verify](#what-the-binary-can-and-cannot-verify)
  - [A row left unconverted](#a-row-left-unconverted)
  - [The upgrade is one way](#the-upgrade-is-one-way)

---

## The short version

1. Stop every GoArchive process that uses the tracking schema, whatever its version.
2. **No source of yours is a Group Replication member:** relabel the schema (step 6 of
   [Moving from 3.0](#moving-from-30)) and nothing else. Nothing else changes for you.
3. **A source of yours is a Group Replication member:** follow every step of
   [Moving from 3.0](#moving-from-30). Most of the work is step 3, knowing each pending job's history.
4. Run 3.1. A schema still labelled `3.0` is refused with a message that names this procedure and
   leaves the tables as they are.

The tracking tables have the same columns as in 3.0. The label changes because a column's meaning
does: `archiver_job.source_server_uuid` now holds the group's name when the job's source was an
`ONLINE` member of a Group Replication group, and the server's uuid otherwise.

---

## Moving from 3.0

**What changed.** A 3.0 run recorded the uuid of the server it read. A 3.1 run on an `ONLINE`
member of a Group Replication group records the **group's name** instead, so a job's pending work
is found again from any member after a failover, with no re-bind
([Group Replication](README_GROUP_REPLICATION.md)). A run on any other server records that
server's uuid, as before. The check that compares the stored value with the run's has no exception:
equal means the same source, and anything else is a different source.

So a row written by 3.0 on a member holds that member's uuid, and 3.1 meets it on a member as a
different source: a job with pending work is refused (`BindingError`), and a job without any has its
row rewritten to the group's name. A 3.1 run never decides that a row on a member's uuid is the
group's, because nothing in the data proves where its work was done. Converting such a row is
therefore a deliberate act of yours, and the steps below treat it as one.

3.1 refuses a schema labelled `3.0`, whatever its contents, and changes nothing in it. A 2.x schema
is refused as before ([Moving from 2.2](README_UPGRADING_3_0.md#moving-from-22)).

The statements below are for the destination's tracking schema, written `<job_schema>` (the
destination's [`job_schema`](README_CONFIGURATION.md#job_schema-destination-only), which is its `database` when unset). Stop every GoArchive process that
uses it first, whatever its version, and take a backup of the schema.

1. **List every binding, with what depends on it.** One statement gives each job; a second, per
   job, counts its pending roots (`copied` for an archive and `deleting` for a purge, which are
   status `1` and `4`; `completed` for a copy-only job, status `2`):

   ```sql
   SELECT id, job_name, job_type, source_server_uuid, source_database
     FROM <job_schema>.archiver_job ORDER BY id;

   SELECT ga_status, COUNT(*) FROM <job_schema>.archiver_job_log_<job id> GROUP BY ga_status;
   ```

2. **Decide which rows need anything.** Only a row that holds pending work, and whose stored value
   is a server's uuid, can need converting. A row without pending work is left alone: its job's
   first run under 3.1 rewrites it. If no source of yours is a Group Replication member, go to
   step 6.

3. **Classify each of those rows.** For each one, answer one question: *was this job's pending
   work done on this group, while that server was a member of it?* The row's uuid being in today's
   view of the group does not answer it: a server that was standalone when the job ran and joined
   the group later, by clone or by distributed recovery, is in the view too, and its job's log
   describes data the group never held. Nor does a uuid's absence from the view answer it: a
   member that has since left the group wrote group data. You know this from the job's history and
   the group's. Each row is one of:
   - **a group job**: its work was done on a member of this group, one that is present now or one
     that has left;
   - **not a group job**: its work was done on a server that was not a member then;
   - **not known**.

4. **Write the mapping down and review it.** One line per row of step 2, and a second pair of eyes
   on it:

   | Job id | Old value | Source database | Class | Group name (a group job) |
   |---|---|---|---|---|
   | | | | | |

   The group's name is `group_replication_group_name`, read on any member.

5. **Convert the group jobs only, one statement per job**, each scoped to its row and to the value
   the mapping recorded:

   ```sql
   UPDATE <job_schema>.archiver_job
      SET source_server_uuid = '<group name>'
    WHERE id = <job id>
      AND source_server_uuid = '<old value>'
      AND source_database = '<source database>';
   -- must report exactly one row changed
   ```

   A statement that changes every row of one member's uuid at once is a shortcut, and is right
   only when the mapping shows that **every** row with that uuid is a group job:

   ```sql
   UPDATE <job_schema>.archiver_job
      SET source_server_uuid = '<group name>'
    WHERE source_server_uuid = '<member uuid>';
   ```

   **A row that is "not a group job" or "not known" is not converted.** It stays as it is, and its
   job is refused by the binding check until you resolve it: by
   [abandoning its logged roots](README_JOBS_SCHEMA.md#abandoning-logged-roots), or by a re-bind
   once the question of step 3 has an answer.

6. **Relabel the schema:**

   ```sql
   UPDATE <job_schema>.goarchive_meta SET schema_version = '3.1' WHERE id = 1;
   ```

   Then run 3.1. A converted group job continues on any member of the group, with no re-bind. A job
   you left unconverted is refused, with nothing replayed, skipped or deleted.

### What the binary can and cannot verify

No fact lets 3.1 prove where an old job's work was done, so it verifies nothing about the
conversion. What it guarantees is that an unconverted row is never accepted silently: with pending
work, its job is refused.

### A row left unconverted

On a member, the [unfinished-work guard](README_OPERATIONS.md#concurrency-and-locking) still sees a
job that is recorded on another member's uuid, because a server configured for a group counts every
server's database of that name in the registry as one. A reader outside the group is as it was in
3.0: a job still recorded on a member's own uuid is not found from a server that does not hold that
uuid in its `gtid_executed`
([Sequential by design](README_LIMITATIONS.md#sequential-by-design)). That is true of a row left
"not known", and of a row for a member that had left the group and was overlooked. The conversion is
complete when step 3 has classified every row of step 2, a departed member's included.

### The upgrade is one way

Going back from 3.1 to 3.0 is not supported, and there is no procedure for it. A 3.0 binary
pointed at a schema labelled `3.1` refuses it as a label it did not write: tables written by a later
release belong to that release, so run that release, and do not drop them.
