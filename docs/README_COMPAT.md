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

- **19 sets are exact, and 22 lose data:** characters that read as one, codes that read as `?` or
  U+FFFD, and ucs2's lone surrogates, which read as bytes that are not UTF-8 and cannot be bound
  back (3988). Both lists, set by set: [Character sets](README_CHARSETS.md#supported-character-sets).
  Every collation of the lossy sets keeps the colliding characters distinct, so a primary key
  holds both.
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
`TestMySQLCharsetRoundTrip` proves the 19 on every server a release is gated on (Oracle MySQL
8.0.40, 8.4.11 and 9.7.1, Percona Server 8.0.46, 8.4.11 and 9.7.1): every character read through
the pinned session is distinct and binds back to its own row, and the same measure sees the loss
in cp1250, sjis, tis620 and ucs2.

**Still open.** Reading and binding keys as bytes (`CAST(… AS BINARY)`), which would lift the
refusal, is not built.

---

## JSON values that do not survive a text copy

**Affects:** every JSON column of an archive or copy-only job.

**Observed** (2026-10-18, on Oracle MySQL 8.0.40 and 9.7.1 and Percona 8.4.11-11). A client reads a
JSON column only as MySQL's printed text, over the text and the binary protocol alike, and a bound
string is rebuilt by the destination's JSON parser. A document built by SQL, copied through its
printed text:

| Member as SQL built it | After the copy | What changes |
|---|---|---|
| DATETIME, DATE, TIME | a string with the same text | the type |
| opaque binary (`CAST(x AS BINARY)`, BIT, a BLOB column) | a string `"base64:typeNN:…"` | the type |
| a DECIMAL with no fraction; YEAR on 8.4 and 9.7, which build it as UNSIGNED INTEGER (8.0 builds an INTEGER, which stays one) | an integer | the type |
| DECIMAL `12.50` | DOUBLE `12.5` | the type and the scale |
| a DECIMAL with more significant digits than a double keeps (`1.234567890123456789012345678901`) | a DOUBLE without the digits it cannot keep (`1.2345678901234567`) | **the value** |
| a double, on an 8.0 or 8.4 destination | sometimes one unit in the last place away | **the value** |
| strings, integers, booleans, null, nested objects and arrays | unchanged | nothing |

All three servers behave the same, except for doubles:

| The source's double, copied to | `0.9057697559760601` | `9088544342.689999` | 20,000 random doubles |
|---|---|---|---|
| MySQL 8.0.40 | `0.90576975597606` | `9088544342.69` | 1,816 changed |
| Percona 8.4.11-11 | `0.90576975597606` | `9088544342.69` | 1,816 changed |
| MySQL 9.7.1 | unchanged | unchanged | none changed |

- **On 8.0 and 8.4 the share depends on the values:** from none to 43 % of 20,000 across five
  generators, the same counts on 8.0.40 and 8.4.11-11. The parser can change any number with a
  fraction or an exponent, a DECIMAL's text included. This is MySQL
  [Bug #116160](https://bugs.mysql.com/bug.php?id=116160) (*Precision loss of double value in
  JSON*, Verified for 8.0 and higher, no fix release); [Bug #83954](https://bugs.mysql.com/bug.php?id=83954)
  is the DECIMAL case. The bug report shows a dump and reload losing the same precision: any client
  that copies JSON as text meets it.
- **The change is in the destination's JSON parser, not in the printed text:** SQL
  `CAST(text AS DOUBLE)` rebuilt all 20,000 exactly on 8.0.40 and 8.4.11.
- **9.7.1's exact parse is measured, not documented:** no release note names a fix.

**Why it matters.** A changed type is visible inside MySQL: `JSON_TYPE()`, a comparison of the
member with a date, and generated columns and indexes over a JSON path see a string where the
source held a date. DECIMAL scale and binary bytes are the two an application is likeliest to
notice. A changed value is data loss, and an archive then deletes the original.

**What GoArchive does.**

- **`sha256`** hashes the printed text, so any change to it fails the batch before anything is
  deleted, and the error names the table's JSON columns. A member whose printed text changes, such
  as a DECIMAL with a scale or a double that an 8.0 or 8.4 destination reads back differently,
  stops such a table's archive under sha256. A double that reads back unchanged (`41.00823` in the
  probes, on every server) does not.
- **`count` and `none`** cannot see the change. They are the operator's choice, so the run
  proceeds, after a warning naming the job's JSON columns ([Warnings](README_VALIDATION.md#warnings)).
- **Dates, times and binary members arriving as strings** is accepted under every mode: the text is
  kept.

For a table that sha256 refuses:

- **a changed double:** a 9.7 destination (measured on 9.7.1). Writing the member as JSON text
  does not help: an 8.x parser can change a double again on each copy (Bug #116160's
  `9088544342.689998` → `…689999` → `…69`);
- **a DECIMAL:** the member written as JSON text, not built from a SQL DECIMAL;
- **either:** the table left out of the job.

**Still open.** Rebuilding each member with its type on the destination, with a `CAST` per member
from the source's `JSON_TYPE`, keeps types and avoids the 8.x parser. It is not built.

---

## A transaction's write set is also bounded by `group_replication_transaction_size_limit`

**Affects:** archive, purge and copy-only jobs whose source or destination is a Group Replication member. Every
transaction of a batch is involved: a delete chunk, the copy transaction and its log insert, and a purge's log
write before its deletes and the log write after them.

**What the manual says.** The MySQL 8.4 Reference Manual's entry for
[`group_replication_transaction_size_limit`](https://dev.mysql.com/doc/refman/8.4/en/group-replication-system-variables.html#sysvar_group_replication_transaction_size_limit)
says that it "configures the maximum transaction size in bytes which the replication group accepts". By its
entry, a larger transaction is rolled back and not broadcast to the group, the default is 150000000 bytes, `0`
means unlimited, and a change applies from the next transaction. The entry does not mention the transaction's
write set, error 3231, a per-key figure, or what state a transaction is left in after a statement fails.
Everything below is a measurement, not the manual.

**Observed** (2026-11-15, on Oracle MySQL 8.0.46, 8.4.13 and 9.7.4, three-member single-primary groups; the
limit set to 4096 bytes on every member and restored; Percona was not run; the same result on all three unless
a row says otherwise):

| Case | Result |
|---|---|
| A table with a primary key only: one transaction changes 512 rows, then 513 | 512 are accepted. 513 get `ERROR 3231 (HY000): The size of writeset data for the current transaction exceeds a limit imposed by an external component. If using Group Replication check 'group_replication_transaction_size_limit'.` It comes at the statement that takes the write set over the limit, before any commit: 8 bytes of write set for a row |
| A table with a unique key or a foreign key as well (9.7.4 only) | 256 rows are accepted and 257 refused, for a `DELETE` and for an `INSERT`: 16 bytes for a row |
| After a 3231 inside a transaction | The transaction is still open and only the statement is undone: an earlier `INSERT` of the same transaction is still visible. A `COMMIT` then fails with error 3100 (`Error on observer while running replication hook 'before_commit'`) and rolls the whole transaction back |
| After a 3231 in autocommit | The statement is refused whole and leaves nothing |
| One row of 100 KB (the control) | No error at the statement; 3100 at the commit, and the whole transaction is rolled back |

The 8 and 16 bytes are figures for these table shapes on these servers, not a general formula.

**Why it matters.** The variable is enforced in two places with two errors. The transaction's size is checked at
the commit and gives 3100, whose text does not say why (a member that stops or leaves the group during a commit
gives the same error). The transaction's write set is checked as each statement adds to it and gives 3231,
whose text says why. A transaction of a few large rows meets 3100; one of many small rows meets 3231 first. At
the default limit, 3231 needs about 18.7 million rows of a primary-key-only table in one transaction; at a
limit a DBA lowered to 1 MiB, about 131000.

**What GoArchive does.** When the limit of the side was known at the run's start, a 3100 at a commit and a 3231
at a statement each end the run with an error that names the limit, the setting to lower and what is not lost
([A transaction over the size limit](README_GROUP_REPLICATION.md#a-transaction-over-the-size-limit)). The 3100 message
says that MySQL's error does not name the cause; the 3231 message states it, as MySQL's error does. GoArchive
never commits a transaction after one of its statements failed, so a 3100 that would follow a 3231 is not seen.

**Still open.** Percona and limits other than 4096 were not run; the 16-byte figure is from 9.7.4 only; whether
the member's error log has a line for 3231 was not recorded.

## A newly elected primary is read-only until it has applied its backlog

**Affects:** archive, purge and copy-only jobs whose source or destination is a Group Replication member that
has just become the primary of a single-primary group, by a planned switch or after the old primary failed.

**What the manual says.** The MySQL 8.4 Reference Manual's page on
[transaction consistency guarantees](https://dev.mysql.com/doc/refman/8.4/en/group-replication-consistency-guarantees.html)
says, of `EVENTUAL`, that "in the event of a primary failover, the new primary can accept new read-only and
read/write transactions before the previous primary transactions are all applied. Read-only transactions could
result in outdated values, read/write transactions could result in a rollback due to conflicts." Its page on
[single-primary mode](https://dev.mysql.com/doc/refman/8.4/en/group-replication-single-primary-mode.html) says
that a new primary "is automatically set to read/write". The manual does not describe a window in which the new
primary is read-only. Everything below is a measurement, not the manual.

**Observed** (2026-11-15, on Oracle MySQL 8.0.46, 8.4.13 and 9.7.4, three-member single-primary groups; Percona
was not run; the same result on all three series). The new primary's applier was kept busy by a table lock
held on it, so that its backlog stayed unapplied for as long as the lock was held:

| Case | Result |
|---|---|
| The new primary after `group_replication_set_as_primary` (1 s) and after the old primary was killed (20 to 23 s) | It is `PRIMARY` in `performance_schema.replication_group_members` and has `super_read_only` on, for as long as the backlog is unapplied. A write from a job's account gets `ERROR 1290`. The plugin lifts `super_read_only` itself when the backlog is applied: its log says, at every election, "A new primary with address … was elected. The new primary will execute all previous group transactions before allowing writes." |
| Six trials (a switch and a kill on each series), each polling the new primary in an `EVENTUAL` session 1.9 to 10.1 million times: `super_read_only`, then whether a row of the backlog was there | No poll read `super_read_only` off with the backlog's row still missing: no writable primary with an unapplied backlog was seen |
| The applier's queue counter (`COUNT_TRANSACTIONS_REMOTE_IN_APPLIER_QUEUE`) | Not a measure of the backlog: it read 0 while `super_read_only` was still on, the transactions having left the queue for blocked workers |
| A job whose session is `EVENTUAL` (the 3.0 behaviour on a server whose default or setting is `EVENTUAL`), on that primary | It reads stale data. An archive's recovery did not see roots the log held as `copied` or `deleting` whose `INSERT` was unapplied, logged them as "already gone from the source" and marked them `completed` while the rows were still in the source, which makes the next archive run fail on duplicates in the destination; a child row not yet applied is left out of the copy and the first `DELETE` fails with `1290`, which stops the run loudly |
| The same jobs on a session the server fences (`group_replication_consistency` `BEFORE_ON_PRIMARY_FAILOVER` or stronger) | The run's first table read is held by the server (thread state "Executing hook on transaction begin."); when the backlog was released after 45 s the run went on and finished with every root deleted, `completed`, and the destination holding each root with all its children |
| Which statements a fence holds on that primary | Held: a table read, `information_schema.TABLES` and the plugin-state read of `information_schema.PLUGINS`. Let through: `SELECT @@GLOBAL.…` (the configuration read), the group's view in `performance_schema`, `GET_LOCK`, and `SET SESSION group_replication_consistency` |
| What ends a held statement | Only the session's `wait_timeout`, with `ERROR 3781 (HY000): Timeout exceeded for held statement while new Group Replication primary member is applying backlog.` (measured with a client whose `wait_timeout` was 8 s). `max_execution_time=8000` and `innodb_lock_wait_timeout=8` did not end it in 40 s. GoArchive pins its pool sessions' `wait_timeout` to the larger of the global value and 28800, so a held GoArchive statement waits at least eight hours; eight hours were not waited for. For a session whose `group_replication_consistency` is `BEFORE` or `BEFORE_AND_AFTER` the library's fence table (six servers) records `ERROR 3797 … Error while waiting for group transactions commit on group_replication_consistency= 'BEFORE'` for the same hold, which this repository did not repeat |
| Stopping a held run | The first signal, or the socket's `stop`, is accepted and the statement stays held; a second `stop` answers `already stopping`; the next signal cancels the run (`context canceled`, exit 1). `KILL` of the connection on the server ends it at once (`invalid connection`, exit 1) |
| What a held write does after its client cancelled it or went away (2026-11-16, on 8.4.13 only) | The server's thread stays held ("Executing hook on transaction begin."), keeps the session's advisory locks, and runs the statement when the hold ends. A held `INSERT` outside a transaction that the client's context cancelled, and one whose client had closed its connection, were both applied when the backlog was applied, and the locks were free after |

**Why it matters.** The result differs from the manual's account of `EVENTUAL`, and the difference decides
what a job can safely do on such a primary: a read-only server cannot take a `DELETE`, so an archive's or a
purge's recovery must not conclude that a root is "already gone" from what it reads there.

**What GoArchive does.** An archive or a purge whose source is read-only, and any job whose destination has
`super_read_only` on, is refused at once, before anything is copied, replayed or held, by
[`SOURCE_READ_ONLY_CHECK`](README_VALIDATION.md#source_read_only_check) and
[`DEST_READ_ONLY_CHECK`](README_VALIDATION.md#dest_read_only_check). The refusal is judged on the
configuration read, which a fence lets through, and not on any statement that it holds. A run that the server
holds past the session's `wait_timeout` ends with a named stop and exit code 1
([a failover's backlog](README_GROUP_REPLICATION.md#a-failovers-backlog)). Its own sessions to a server whose plugin
is `ACTIVE` are made fenced when they connect, a replacement in the middle of a run included
([`GROUP_CONSISTENCY_CHECK`](README_VALIDATION.md#group_consistency_check)), so that no read of data or of the
tracking tables is served from before the backlog; and the read-only refusal names its cause from the role of the
source's own row. GoArchive makes no claim the manual does
not make: it does not rely on the window to behave as measured, and a server that behaved as the manual says
(a writable new primary that serves stale reads) is not covered by the measurement above.

**Still open.** Percona was not run, and the held write of the row above was measured on one series. The backlog was held by a lock on the applier and not by a slow apply, so
the plugin's log line is what ties the lift of `super_read_only` to the apply. Nothing documents the window, so
a later server may behave as the manual says. Eight hours were not waited for, and the investigation into the
window itself is deferred.
