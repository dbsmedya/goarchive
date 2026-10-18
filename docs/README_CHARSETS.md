# Character Sets

GoArchive reads every value through its MySQL session and binds every key back the same way, so
a value survives only if it converts exactly between its column's character set and the session's.
Every session is therefore `utf8mb4`, and preflight refuses a job column whose character set does
not convert exactly. This page explains why, how GoArchive and its MySQL driver set the session,
which character sets are supported and refused, and how to convert a refused column.

---

## Contents

- [The problem](#the-problem)
- [GoArchive's approach](#goarchives-approach)
- [Supported character sets](#supported-character-sets)
- [Refused character sets](#refused-character-sets)
- [Converting a refused column](#converting-a-refused-column)

---

## The problem

**How MySQL moves text.** MySQL converts every result column to the session's
`character_set_results`, and every bound value from the session's `character_set_client` into the
column's character set. The driver passes the bytes through unchanged. So every value GoArchive
reads, every key it binds back, and every value it writes passes through the session's character
set, twice.

**Where it loses data.** Most character sets convert to `utf8mb4` and back exactly. Some do not:

- **Two real characters read as one.** cp932 and eucjpms map their duplicate NEC and IBM codes
  (such as 0x8790 and 0x81E0) to one Unicode character; sjis writes its backslash 0x5C back as
  0x815F, so `C:\tmp` arrives as `43 3A 81 5F …`.
- **A code with no Unicode character reads back as `?`** (or as U+FFFD in tis620 and big5).
- **A ucs2 lone surrogate reads back as bytes that are not UTF-8**, which MySQL refuses to bind
  back (error 3988).

Every collation of these character sets keeps the colliding characters apart, so a primary key
can hold both.

**What that does to a run, with no error and no warning:**

- Two keys that read as one are discovered as one, so only one row is copied. An in-tree
  `ON DELETE CASCADE` then deletes the other, never copied.
- A non-key value is written back changed, even on schemas that are identical.
- Count verification, the copy-read identity check and the delete row counts all pass, because
  each checks the keys it was given.

**A narrowed session loses data too.** An `init_connect` such as `SET NAMES latin1`, MySQL 8.0's
`--skip-character-set-client-handshake` on a server whose own character set is not `utf8mb4`, or a
proxy can leave a session in another character set. Then even `utf8mb4` text outside that set
reads back as `?`, and a key bound back can match a look-alike row.

---

## GoArchive's approach

**The session is pinned.** `BuildDSN` sets the driver's character set to `utf8mb4`, collated
`utf8mb4_general_ci`:

- **The handshake:** the driver (go-sql-driver/mysql) sends collation 45, `utf8mb4_general_ci`,
  as GoArchive always did, so comparisons in a job's `where` and in the key SQL do not change.
- **After authentication** the driver runs `SET NAMES utf8mb4 COLLATE utf8mb4_general_ci` on every
  new connection. That is after any `init_connect`, so a narrowing `init_connect` is overridden. A
  refused `SET NAMES` closes the connection.

**The session is checked** ([`SESSION_CHARSET_CHECK`](README_VALIDATION.md#session_charset_check)).

- **What it proves:** every physical source and destination connection, at connect, reports
  `utf8mb4` for client, connection and results, collated `utf8mb4_general_ci`. Anything else is
  refused by name.
- **What it does not cover:** a multiplexing proxy that changes the session between statements,
  as with GoArchive's other session settings.
- **Replica connections** are pinned but not checked: they read only replica status.

**Job columns are checked** ([`COLUMN_CHARSET_CHECK`](README_VALIDATION.md#column_charset_check)).
Before any row is read, preflight refuses a job column the run reads or writes as text whose
character set is not [supported](#supported-character-sets):

| Job type | Columns checked |
|---|---|
| `archive`, `copy-only` | every column of every job table on the source, and every destination column of the same name |
| `purge` | the primary-key and `foreign_key` columns of every job table, on the source |

`ENUM` and `SET` columns are checked. Numbers, temporal types, binary strings and `JSON` have no
character set and are not.

**No bypass.** Both checks are enforced under
`--skip-dangerously-validate-preflight-allow-experimental`. The loss is silent and cannot be found
after the run, so no option turns them off.

`TestMySQLCharsetRoundTrip` proves the supported list on every supported MySQL version: every
character of each set, read through the pinned session, is distinct and binds back to its own
row; and the same measure sees the loss in cp1250, sjis, tis620 and ucs2.

---

## Supported character sets

| Character set | Note |
|---|---|
| `utf8mb4` | The session's own character set |
| `utf8mb3` | |
| `ascii` | Bytes 0x80–0xFF cannot be stored (MySQL error 1366), so none is lost |
| `latin1` | |
| `latin2` | |
| `latin5` | |
| `latin7` | |
| `cp850` | |
| `cp852` | |
| `cp866` | |
| `keybcs2` | |
| `koi8r` | |
| `koi8u` | |
| `macce` | |
| `macroman` | |
| `utf16` | Every Unicode character, surrogate pairs included |
| `utf16le` | Every Unicode character, surrogate pairs included |
| `utf32` | Every Unicode character |
| `binary` | Listed for completeness: a binary string reports no character set |

---

## Refused character sets

Every MySQL character set that is not supported is refused:

| Character set | How it loses data |
|---|---|
| `sjis` | Two characters read as one (0x5C and 0x815F); codes read as `?` |
| `cp932` | Two characters read as one (the NEC and IBM duplicates); codes read as `?` |
| `eucjpms` | Two characters read as one (the NEC and IBM duplicates); codes read as `?` |
| `ujis` | Two characters read as one (a full-width backslash and tilde); codes read as `?` |
| `armscii8` | Six punctuation bytes read as their ASCII look-alikes |
| `tis620` | Codes read as U+FFFD |
| `big5` | Codes read as `?` or U+FFFD |
| `euckr` | Codes read as `?` |
| `gb2312` | Codes read as `?` |
| `gbk` | Codes read as `?` |
| `gb18030` | Codes read as `?` |
| `cp1250` | Unmapped bytes read as `?` |
| `cp1251` | Unmapped bytes read as `?` |
| `cp1256` | Unmapped bytes read as `?` |
| `cp1257` | Unmapped bytes read as `?` |
| `dec8` | Unmapped bytes read as `?` |
| `geostd8` | Unmapped bytes read as `?` |
| `greek` | Unmapped bytes read as `?` |
| `hebrew` | Unmapped bytes read as `?` |
| `hp8` | Unmapped bytes read as `?` |
| `swe7` | Unmapped bytes read as `?` |
| `ucs2` | Lone surrogates read as bytes that are not UTF-8 and cannot be bound back |

The MySQL facts behind both lists, and the servers they were measured on:
[Character sets that do not survive a utf8mb4 session](README_COMPAT.md#character-sets-that-do-not-survive-a-utf8mb4-session).

---

## Converting a refused column

Convert the column to `utf8mb4` on both servers
(`ALTER TABLE <table> MODIFY <column> … CHARACTER SET utf8mb4`), or leave the table out of the
job. The conversion is MySQL's own, under the default strict mode:

| Value | Strict `ALTER … CHARACTER SET utf8mb4` |
|---|---|
| a code with no Unicode character (cp932 0x81AD, cp1250 0x81) | refused, 1366 `Incorrect string value` |
| a primary-key pair that would merge (cp932 0x8790 and 0x81E0) | refused, 1062 `Duplicate entry` |
| a non-key pair that maps to one character (cp932 0x8790 and 0x81E0; sjis 0x5C and 0x815F) | **merged without a message**: both rows hold the same character afterwards |
| a ucs2 lone surrogate (0xD800) | stored as `EDA080`, invalid UTF-8, which a strict destination then refuses (1366) |

So, before converting:

1. **Find non-key values that map to one character:** the cp932 and eucjpms NEC and IBM
   duplicates, and the sjis and ujis full-width backslash. A strict `ALTER` merges them without a
   message.
2. **Remove ucs2's lone surrogates.** A strict `ALTER` stores them as invalid UTF-8.
3. **Convert under strict mode.** MySQL refuses the rest itself: a code with no Unicode character
   (1366), and a key pair that would merge (1062).
