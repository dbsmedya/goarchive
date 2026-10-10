# MySQL Compatibility Notes

This page records MySQL server behaviours that change how GoArchive builds its statements. Each
entry covers a behaviour that differs from the MySQL documentation, or between MySQL versions.
For each one, it says:

- what was observed, and on which servers;
- why it matters to an archive job;
- the path GoArchive takes;
- what is still open.

The supported versions are listed in [Limitations → Environment](README_LIMITATIONS.md#environment);
this page does not restate them.

GoArchive binds every key value as a prepared-statement parameter, never as an inline literal. A
key value therefore reaches MySQL as a `?`. MySQL types the `?` from its context, and for some
types GoArchive wraps it in a `CAST` to the column's own type. The entries below are cases where
that typing does not behave as documented.

---

## TIME key values bound as prepared parameters

**Affects:** jobs whose primary key or foreign key includes a `TIME` column. Every statement that
addresses rows by key is involved: discovery of child rows, the copy read, verification, the
source delete, and the job's log.

**Observed** (2026-10-10, on Percona Server for MySQL 8.0.46, 8.4.11 and 9.7.1). Each probe read
a key's text back from its own column and bound it as a prepared parameter:

1. **`CAST(? AS TIME(n))` returns a different time for some values on all three versions.**
   `'10:00:00.01'` becomes `01:00:00.00`, and `'03:03:00'` and `'01:09:13.1'` are also
   mis-converted. Out of 400 random values, 42–45 were wrong at `TIME(0)`, 39 at `TIME(1)`, 6 at
   `TIME(2)`, 1 at `TIME(3)` and none at `TIME(4)` to `TIME(6)`.
   - The same happens through server-side `PREPARE … EXECUTE`, so it is not the client driver.
   - It happens under every `sql_mode` tried: the server default, empty, `ALLOW_INVALID_DATES`,
     `TIME_TRUNCATE_FRACTIONAL`, and the strict date modes.
   - The same SQL with a literal (`CAST('10:00:00.01' AS TIME(2))`) is correct.
2. **A bare `?` behaves differently between versions.** In an `IN` list of several keys on a
   `TIME(0)` or `TIME(2)` column, a bare `?` matched no rows at all on 8.0.46 and 8.4.11, and every
   row on 9.7.1. A single bare `?`, a row constructor such as `(a, k) IN ((?, ?))`, and `k > ?`
   were correct on all three.
3. **`CAST(CAST(? AS CHAR) AS TIME(n))` was correct in every one of these shapes on all three
   versions.** It keeps range access on the index (`EXPLAIN` shows `type=range`). It cost at
   most 0.6 ms more per 1,000-key statement than a bare `?`, because the cast runs once per bound
   value, not once per row.

`DATE`, `DATETIME`, `TIMESTAMP` and `YEAR` keys were checked the same way, with 400 random
values per type on each server, and are bound correctly by their casts.

**Why it matters.** A key bound as a different value selects a different row. For a delete, the
row count still matches: one key requested, one row deleted. Purge has no copy read, so with the
first binding it could delete a row outside the job's `where`, together with its children, and
record the root as done. Archive and copy-only would fail safely: their copy read checks that the
rows returned are exactly the keys requested.

**What GoArchive does.** On every version, a `TIME` key value is bound as
`CAST(CAST(? AS CHAR) AS TIME(n))`, where `n` is the column's fractional-seconds precision. The
inner cast keeps the parameter as text, so MySQL converts it the way it converts a literal. The
code is `internal/key` → `Placeholder`.

**Still open.** The cause has not been identified.
- MySQL's release notes, bug tracker and source have not been reviewed.
- Oracle MySQL Community builds have not been tested.
- The 8.0 line was tested at 8.0.46 only, not at the supported floor of 8.0.40.

Until that work is done, GoArchive keeps the binding above on every version, including where a
simpler form happens to work.
