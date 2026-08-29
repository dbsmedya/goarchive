# Upgrading to GoArchive 2.2

GoArchive 2.2 pins **every database session to UTC** and makes `last_heartbeat_at` a UTC
wall-clock. Both close [issue #16](https://github.com/dbsmedya/goarchive/issues/16): on two
servers in different time zones, every copied `TIMESTAMP` shifted by the offset while SHA256
verification still passed — and the source row was then deleted.

**Every existing installation has one mandatory step**, UTC server or not: 2.2 refuses to start
against tracking tables written by an earlier release until you run the one-time procedure
below. Beyond that, on a UTC server the data GoArchive copies, verifies and deletes is
unchanged, and no config key, flag, privilege or tracking-table DDL changes anywhere.

**Read this if** you have existing tracking tables (an `archiver_job` written by 2.1 or
earlier — that is every upgrade), or your MySQL servers do not run in UTC.

## The short version

| Situation | What to do |
|---|---|
| Existing tracking tables | 2.2 **refuses to start** until you run a one-time SQL procedure — [below](#existing-tracking-tables-are-refused-until-upgraded) |
| Servers not in UTC | Review `where` clauses that use date literals, `CURDATE()`, or `NOW()` against `DATETIME` columns — [below](#where-is-evaluated-in-utc) |
| Monitoring on `last_heartbeat_at` | Replace `NOW()` with `UTC_TIMESTAMP()` — [below](#heartbeats-are-utc) |
| Rows archived by 2.1 or earlier across a zone boundary | Already shifted; 2.2 prevents, it does not repair — [below](#previously-archived-timestamps) |

## Existing tracking tables are refused until upgraded

2.2 writes `schema_version = '2.2'` into `goarchive_meta`. A schema reporting `'2.0'` (written
by 2.0 or 2.1), or populated tracking tables with no marker (1.8), is refused at startup:

```
tracking tables in schema "goarchive" report schema_version "2.0", which this
GoArchive does not recognize (it requires schema_version "2.2").
GoArchive never migrates or infers a tracking-schema revision. Stop every
GoArchive process that uses this schema (any version), then follow the matching
row of "Tracking-schema upgrade procedures" in docs/README_JOBS_SCHEMA.md
```

Stop every GoArchive process that uses the schema — **any version** — then run the row for your
case in [Tracking-schema upgrade procedures](README_JOBS_SCHEMA.md#tracking-schema-upgrade-procedures).
For 2.0 / 2.1 it is two statements:

```sql
UPDATE goarchive.archiver_job   SET last_heartbeat_at = NULL;
UPDATE goarchive.goarchive_meta SET schema_version = '2.2' WHERE id = 1;
```

The old heartbeats are local wall-clock in a zone 2.2 cannot know. Voiding them is safe — a
`NULL` heartbeat already counts as *stale* — and checkpoints, statuses and log tables are
untouched, so resume behaves exactly as before.

## `where` is evaluated in UTC

Your `where` fragment now runs in a UTC session. `TIMESTAMP` columns compared with `NOW()`
behave as before. A `DATETIME` column compared with `NOW()`, a `TIMESTAMP` column compared with a
bare literal, and `CURDATE()` / `DATE()` boundaries now sit at UTC instead of your server's local
zone — a shift equal to the server's UTC offset. To keep a local boundary, give the literal an
offset (`'2025-01-01 00:00:00+03:00'`, MySQL 8.0.19+) or use `CONVERT_TZ`. See
[`where`](README_CONFIGURATION.md#where-is-mandatory).

## Heartbeats are UTC

`last_heartbeat_at` is written with `UTC_TIMESTAMP()`. Any monitoring that computes its age with
`NOW()` is now wrong by your session's UTC offset on a non-UTC server; use `UTC_TIMESTAMP()`. The
[inspection cookbook](README_JOBS_SCHEMA.md#inspection-cookbook) is updated.

## Previously archived timestamps

If source and destination sessions ran in different zones before 2.2, `TIMESTAMP` values
archived by earlier releases are wrong, and their source rows are gone. 2.2 prevents this; it
cannot undo it. A DBA can repair them **once**, because the error has a known shape: the old
copy took the source session's wall-clock rendering of each value, and the destination session
read that wall-clock back as its own local time. The inverse runs those two steps backwards —
first reconstruct the wall-clock the destination saw, then read it in the source zone. Two
`CONVERT_TZ` calls, in that order; a single call is wrong whenever a named zone changes offset
between the two dates involved:

```sql
-- 1. Back the table up.
-- 2. Dry-run. A non-zero count means a zone name is not recognised (CONVERT_TZ
--    returns NULL for one) and the UPDATE would NULL the column: stop.
--    ts IS NOT NULL keeps legitimately NULL values out of the count.
SET time_zone = '+00:00';
SELECT COUNT(*) FROM archive.orders
 WHERE <only rows archived before 2.2>
   AND ts IS NOT NULL
   AND CONVERT_TZ(CONVERT_TZ(ts, '+00:00', '<zone the old DESTINATION sessions used>'),
                  '<zone the old SOURCE sessions used>', '+00:00') IS NULL;

-- 3. Repair, exactly once.
UPDATE archive.orders
   SET ts = CONVERT_TZ(
            CONVERT_TZ(ts, '+00:00', '<zone the old DESTINATION sessions used>'),
            '<zone the old SOURCE sessions used>',
            '+00:00')
 WHERE <only rows archived before 2.2>;   -- by primary-key range, or the job's own where
```

`SET time_zone = '+00:00'` matters: it makes `ts` read and write as the stored instant, so the
two `CONVERT_TZ` calls are the only conversions. Name the zones as the old *sessions* saw them
(`SYSTEM` is accepted and means the server's OS zone). A second run corrupts the values again,
so scope the `WHERE` to rows archived before the upgrade and run it once per column. Example:
source sessions in `+00:00`, destination sessions in `+03:00` — every archived value is three
hours early and the expression reduces to `ts + INTERVAL 3 HOUR`. Rows whose source rendering
fell inside a daylight-saving fall-back hour were collapsed with their twin an hour apart and
cannot be recovered; the `dst-overlap-source` subtest in `timezone_integration_test.go` shows
exactly this.

## What did not change

- Copy, verification, deletion, checkpointing and resume semantics.
- `DATETIME`, `DATE` and `TIME` columns — stored as written, unaffected by the session zone.
- Configuration keys, CLI flags, required privileges, and every tracking-table definition.
- Preflight: still 19 checks. The session-zone check runs at connection time and cannot be
  bypassed by `--skip-validate-preflight`.
