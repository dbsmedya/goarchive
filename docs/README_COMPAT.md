# MySQL Compatibility Notes

This page records MySQL server behaviours that differ from the MySQL documentation, or between
MySQL versions, and change what GoArchive does. For each one, it says:

- what was observed, and on which servers;
- why it matters to an archive job;
- the path GoArchive takes;
- what is still open.

The supported versions are listed in [Limitations → Environment](README_LIMITATIONS.md#environment);
this page does not restate them.

---

## TIME key values bound as prepared parameters

GoArchive binds every key value as a prepared-statement parameter, never as an inline literal, so
a key value reaches MySQL as a `?`. MySQL types the `?` from its context, and for some types
GoArchive wraps it in a `CAST` to the column's own type. For `TIME`, that typing does not behave
as documented.

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
- The mis-conversion was probed on Percona 8.0.46, 8.4.11 and 9.7.1 only. GoArchive's suites,
  with this binding, pass on Oracle MySQL 8.0.40, 8.0.46, 8.4.11 and 9.7.1 too.

Until that work is done, GoArchive keeps the binding above on every version, including where a
simpler form happens to work.

---

## Foreign-key metadata readable without `PROCESS` on MySQL 8.0.40

**Affects:** `FK_COVERAGE_VISIBILITY_CHECK` and `FK_COVERAGE_CHECK`, for archive and purge jobs
([the checks](README_VALIDATION.md#foreign-key-checks)).

**Observed** (2026-10-11). An account holding only `SELECT` on one schema, without `PROCESS`, read
`information_schema.INNODB_FOREIGN` and `INNODB_FOREIGN_COLS`:

| Server | Result |
|--------|--------|
| Oracle MySQL 8.0.40 | Readable and **unfiltered**: every foreign key, including those in schemas the account cannot see |
| Oracle MySQL 8.0.46, Percona 8.0.46 | Refused with error 1227 (`PROCESS` required) |

`INNODB_TABLES` was refused without `PROCESS` on every server.

**Why it matters.** The visibility proof is a successful read of that registry. On 8.0.40 the
read succeeds without `PROCESS`, so the check passes; because the result is unfiltered,
`FK_COVERAGE_CHECK` still sees a foreign key from a schema the account cannot read, and the
guarantee holds.

**What GoArchive does.** Nothing version-specific: it judges the read, not the grant. On 8.0.40
`PROCESS` is not needed; from 8.0.46 it is ([Permissions](README_PERMISSIONS.md#privilege-matrix)).

**Still open.** The versions between 8.0.40 and 8.0.46 were not probed.

---

## Character sets that do not survive a utf8mb4 session

**Affects:** every job column the run reads or writes as text
([`COLUMN_CHARSET_CHECK`](README_VALIDATION.md#column_charset_check)), and every session
([`SESSION_CHARSET_CHECK`](README_VALIDATION.md#session_charset_check)).

**Observed** (2026-10-17 and 2026-10-18, on Oracle MySQL 8.0.40 and 9.7.1 and Percona 8.4.11).
Every valid single character of each of MySQL's 41 character sets was converted to `utf8mb4` and
back:

- **19 sets are exact:** `utf8mb4`, `utf8mb3`, `ascii`, `latin1`, `latin2`, `latin5`, `latin7`,
  `cp850`, `cp852`, `cp866`, `keybcs2`, `koi8r`, `koi8u`, `macce`, `macroman`, `utf16`, `utf16le`,
  `utf32` and `binary`. In `ascii`, bytes 0x80–0xFF cannot be stored (1366); `utf16`, `utf16le`
  and `utf32` hold all 1,112,064 Unicode characters, surrogate pairs included.
- **22 sets lose data:**
  - two characters read as one: sjis writes its backslash 0x5C back as 0x815F (`C:\tmp` arrives
    as `43 3A 81 5F …`); cp932 and eucjpms map their NEC and IBM duplicates (such as 0x8790 and
    0x81E0) to one character; ujis, armscii8, tis620 and big5 have such pairs too;
  - storable codes with no Unicode character read back as `?`: cp1250, cp1251, cp1256, cp1257,
    dec8, geostd8, greek, hebrew, hp8, swe7, big5, cp932, euckr, gb2312, gbk, sjis, eucjpms, ujis
    and gb18030; tis620 and big5 codes become U+FFFD;
  - ucs2's 2,048 lone surrogates read as bytes that are not UTF-8 (`EDA080`), which MySQL refuses to
    bind back (3988), strict or not.

  Every collation of these sets keeps the colliding characters distinct, so a primary key holds
  both.
- **An ill-formed value inside an exact set** (the `EDA080` an `ALTER` from ucs2 writes into
  `utf8mb4`) is read unchanged: a strict destination refuses its INSERT (1366), a non-strict one
  stores it empty with warning 1366. A non-strict load stores an `ascii` byte above 0x7F as empty,
  before any run.
- **`init_connect` does not run for an account with `CONNECTION_ADMIN` (or `SUPER`).** A
  narrowing `init_connect` such as `SET NAMES latin1` reaches only the others. 8.0's
  `--skip-character-set-client-handshake` narrows every account, root included, when the server's
  own character set is not `utf8mb4`.

**Why it matters.** MySQL converts each result column to the session's `character_set_results`
and each bound value from `character_set_client` into the column's set, and GoArchive reads keys
and binds them back. Two keys that read as one were discovered as one: one row was copied, and an
in-tree `ON DELETE CASCADE` deleted the other, while every verification passed.

**What GoArchive does.** It pins every session to `utf8mb4` / `utf8mb4_general_ci` and checks it
on every connection, and refuses any job column outside the 19 sets in preflight.
`TestMySQLCharsetRoundTrip` proves the 19 on every supported server: every character read through
the pinned session is distinct and binds back to its own row, and the same measure sees the loss
in cp1250, sjis, tis620 and ucs2.

**Still open.** Reading and binding keys as bytes (`CAST(… AS BINARY)`), which would lift the
refusal, is not built.

