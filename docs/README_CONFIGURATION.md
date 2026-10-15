# Configuration Reference

Every block, option, default and precedence rule in `archiver.yaml`. An annotated example ships
as [`configs/archiver.yaml.example`](../configs/archiver.yaml.example).

**Related:** [Validation & Preflight](README_VALIDATION.md) ·
[Permissions](README_PERMISSIONS.md) · [Operations](README_OPERATIONS.md) ·
[Limitations](README_LIMITATIONS.md) · [Upgrading to 3.0](README_UPGRADING_3_0.md) ·
[← Back to README](../README.md)

---

## Contents

- [How configuration is loaded](#how-configuration-is-loaded)
- [`source:` / `destination:`](#source--destination)
- [Session settings](#session-settings)
- [`replication:`](#replication)
- [`jobs:`](#jobs)
- [`processing:`](#processing)
- [`safety:`](#safety)
- [`verification:`](#verification)
- [`logging:`](#logging)
- [Per-job overrides and precedence](#per-job-overrides-and-precedence)
- [Identifier rules](#identifier-rules)

---

## How configuration is loaded

GoArchive reads **exactly one** YAML file, `--config`/`-c` (default `archiver.yaml` in the
working directory). There is no merging, no directory scan and no fallback chain. Values the
file leaves out take the defaults below.

The file is read strictly. A validation error names the field it concerns; an unknown key also
its line; a YAML parse error reports the line where the parser has one:

- **Unknown keys are refused.** Key names are exact and lowercase.
- **One YAML document per file:** a second document after `---` is refused.
- **Each job type accepts only the settings it uses** ([`jobs:`](#jobs)).

Only `--log-level`, `--log-format` and `--verification` override the file. Batch sizes, sleeps
and the sentinel file are file-only, deliberately: one file holds many jobs, and one CLI value
cannot suit them all. `goarchive validate` loads the file and runs preflight for every job.

---

## `source:` / `destination:`

Connection settings. Both blocks take the same options; `job_schema` is destination-only.

| Option | Description | Default |
|--------|-------------|---------|
| `host` | Database host | **required** |
| `port` | Database port (1–65535) | `3306` |
| `user` | Username | **required** |
| `password` | Password | — |
| `database` | Database name | **required** |
| `tls` | `disable`, `preferred`, `skip-verify` or `required` | `preferred` |
| `max_connections` | Max open connections, not negative; a run needs at least 2 on the source and 3 on the destination | `10` |
| `max_idle_connections` | Max idle connections, not negative | `5` |

Source and destination must be different databases
([`SRC_DEST_IDENTITY_CHECK`](README_VALIDATION.md#src_dest_identity_check)).

### `job_schema` (destination only)

| Option | Description | Default |
|--------|-------------|---------|
| `job_schema` | Schema holding the tracking tables (`archiver_job`, `archiver_job_log_<id>`, `goarchive_meta`) | same as `database` |

```yaml
destination:
  database: archive
  job_schema: goarchive
```

**A DBA must create this schema.** GoArchive never issues `CREATE DATABASE`; it creates the
tracking tables, so its account needs `CREATE` there
([Permissions](README_PERMISSIONS.md#why-create-on-the-tracking-schema)). The tables:
[Job Tracking Schema](README_JOBS_SCHEMA.md).

**One `job_schema` per source server where database names repeat.** A job that stopped with
unfinished work holds its tables against the other jobs in its registry on the same source
data. Where a server cannot tell its data from another's (no `gtid_mode = ON`, a Group
Replication member, or shards provisioned from one another), every source database of the same
name in one registry counts as one, so one server's stopped job holds the same-named tables of
every other server ([Sequential by design](README_LIMITATIONS.md#sequential-by-design)). A
`job_schema` per source server keeps them apart.

---

## Session settings

GoArchive sets these on every connection it opens, at session scope only; it never changes a
server's global. None is configurable.

| Setting | Value | Why |
|---------|-------|-----|
| `time_zone` | `'+00:00'` | MySQL converts `TIMESTAMP` through the session zone on every read and write; UTC on both sides keeps each value the same instant. Startup refuses a server or proxy that does not honour it. `DATETIME`, `DATE` and `TIME` are unaffected |
| `autocommit` | `1` | Tracking writes outside the copy and delete transactions must not stay open under a global `autocommit = 0` |
| `sql_mode` | the inherited mode plus `NO_AUTO_VALUE_ON_ZERO` | An explicit zero in an AUTO_INCREMENT column stays zero ([`AUTO_INCREMENT_ZERO_MODE_CHECK`](README_VALIDATION.md#auto_increment_zero_mode_check)) |
| `sql_notes` | `1` | Notes are kept, so INSERT diagnostics see them |
| `sql_select_limit` | `18446744073709551615` (no limit) | A server's global limit would cap every read without its own `LIMIT`: column lists and discovery would silently see only that many rows, and verification would hash only the columns read; a short copy read or row count fails loudly |
| `wait_timeout` | `GREATEST(@@GLOBAL.wait_timeout, 28800)`; `300` on the two lock sessions | A session idle through a pause or a replication hold is not closed under the run before 8 hours. A lock session is checked every 30 seconds, and its 300 seconds bound how long a lost host keeps its locks ([The lock sessions must stay alive](README_OPERATIONS.md#the-lock-sessions-must-stay-alive)) |
| `max_error_count` | raised from 0 to 1024 (source and destination) | GoArchive reads every statement's warnings ([`MAX_ERROR_COUNT_CHECK`](README_VALIDATION.md#max_error_count_check)) |

---

## `replication:`

Optional. Holds the job while any monitored replica is unhealthy, before every delete chunk, and
continues the same run once they all recover
([Replication gating](README_OPERATIONS.md#replication-gating)). Copy-only never deletes and is
never gated. Absent, monitoring is off.

| Option | Description | Default |
|--------|-------------|---------|
| `enabled` | Turn replication gating on | `false` |
| `seconds_behind_source_within` | Lag tolerance in seconds; `0` demands exact sync. Not negative | `10` |
| `check_interval` | Seconds between re-checks while holding; positive **when enabled** | `5` |
| `cache_ttl` | Seconds a **passing** verdict stays fresh; `0` checks every time. Not negative | `15` |
| `servers` | The replicas to monitor; at least one **when enabled** | — |

The 2.0 `replica:` block and `safety.lag_threshold` / `safety.check_interval` are refused
([Upgrading to 3.0](README_UPGRADING_3_0.md#configuration-changes)).

### `servers[]`

| Option | Description | Default |
|--------|-------------|---------|
| `host` | Replica host, without a newline or carriage return | required |
| `port` | Replica port (1–65535) | `3306` |
| `user` | Account that reads replication status | required |
| `password` | Account password | — |
| `tls` | `disable`, `preferred`, `skip-verify` or `required` | `preferred` |
| `type` | Only `async` is accepted | `async` |
| `channels` | The replication channels to gate on | _(all)_ |

### Channel selection

MySQL's default channel is **named `""`** — an empty name, not an absence:

| `channels` value | Gates on |
|---|---|
| omitted, or `[]` | every channel the server reports |
| `[""]` | the default channel only |
| `["", "billing"]` | the default channel **and** `billing` |

A listed channel missing from the server fails the check rather than being ignored. Logs show
the default channel as `<default>`. Two servers may not share a `host:port`, and one server may
not list a channel twice. Each monitored account needs
[`REPLICATION CLIENT`](README_PERMISSIONS.md#monitored-replicas-optional). Per-server settings
are validated whenever present, even with `enabled: false`, so a disabled block cannot hide a
typo.

---

## `jobs:`

A map of job name → job. At least one job is required. **Job names** are kept as written and
must be printable ASCII without spaces, not starting with `-` (the command line would read it as a
flag), at most 255 characters; two names that differ only in letter case are refused. The name on
the command line must match exactly.

| Option | Description | Required |
|--------|-------------|----------|
| `job_type` | `archive`, `purge` or `copy-only`; the job runs only under that command | yes |
| `root_table` | The table the job starts from | yes |
| `primary_key` | The root's primary key: a column, or a list for a composite key | yes |
| `where` | SQL condition selecting the root rows | yes |
| `relations` | The child tables | no |
| `resume` | Copy-only only: `true` skips the roots this job copied before ([Resume semantics](README_OPERATIONS.md#resume-semantics)) | no (`false`) |
| `processing` | [Per-job processing](#per-job-overrides-and-precedence); a copy-only job may not set `batch_delete_size` or `delete_sleep_seconds` | no |
| `verification` | [Per-job verification](#verification); not on purge jobs | no |
| `logging` | [Per-job logging](#per-job-overrides-and-precedence) | no |

### `where` is mandatory

There is no implicit "archive everything": an empty `where` fails validation. To process a whole
table, say so: `where: "1=1"`.

`where` is a **raw SQL condition**, trusted operator input
([Trust model](README_LIMITATIONS.md#trust-model)). Each database call carries one statement, so
text that adds a second fails with a syntax error. The root table is queried without an alias,
so a subquery refers to the row being tested by the table name (`orders.id`). Every run starts
at the beginning of the `where` range; the rows it selects must stay unchanged while GoArchive
handles them ([the run contract](README_LIMITATIONS.md#no-ddl-and-no-concurrent-writes-during-a-run-contract)).

`where` runs **in a UTC session** ([Session settings](#session-settings)). A `TIMESTAMP` column
compared with `NOW()` is unaffected, but a `DATETIME` column compared with `NOW()`, a `TIMESTAMP`
column compared with a bare literal, and `CURDATE()` / `DATE()` day boundaries sit at UTC rather
than the server's zone. To pin a local boundary, give the literal an offset
(`created_at < '2025-01-01 00:00:00+03:00'`, MySQL 8.0.19+) or use `CONVERT_TZ`.

### `primary_key` documents the key

`primary_key` is required on the root and on every relation. It documents the key: GoArchive
reads every key from MySQL and builds all SQL from that, and preflight refuses a `primary_key`
that does not name exactly the table's `PRIMARY KEY` columns, in key order and spelling
([Primary key checks](README_VALIDATION.md#primary-key-checks)). A composite key is a list:

```yaml
primary_key: [order_id, line_no]
```

### `relations:`

Each relation is one child table; relations nest for grandchildren, at most 10 levels deep. A
table may appear once in a job.

| Option | Description | Required |
|--------|-------------|----------|
| `table` | The child table | yes |
| `primary_key` | The child's primary key: a column or a list | yes |
| `foreign_key` | The child's column, or list of columns, that holds the parent's key | yes |
| `references` | The parent's key columns, paired with `foreign_key` by position; each parent key column exactly once | only when the parent key has more than one column |
| `dependency_type` | `1-1` or `1-N`, shown by `plan` | no |
| `relations` | Nested child relations | no |

```yaml
relations:
  - table: order_lines
    primary_key: [order_id, line_no]
    foreign_key: order_id                  # parent key (id) has one column: references may be omitted
    relations:
      - table: line_notes
        primary_key: id
        foreign_key: [order_id, line_no]
        references: [order_id, line_no]    # pairs with foreign_key by position
```

A relation needs no foreign key in the database: relations an application manages are declared
here like any other. Each child needs an index whose leading columns are its `foreign_key`
columns ([`FK_INDEX_CHECK`](README_VALIDATION.md#fk_index_check)). Where the database does declare
foreign keys, the relations must mirror them, nesting and pairing included
([Foreign key checks](README_VALIDATION.md#foreign-key-checks)).

---

## `processing:`

| Option | Description | Default |
|--------|-------------|---------|
| `batch_size` | Root keys per batch, and the most keys any copy, verification or discovery statement addresses. Positive | `1000` |
| `batch_delete_size` | Keys per `DELETE` chunk. Positive | `500` |
| `sleep_seconds` | Pause between batches; fractions accepted. Not negative | `1` |
| `delete_sleep_seconds` | Pause between consecutive delete chunks of a batch, across tables; fractions accepted. Not negative | `0` |
| `sentinel_file` | While this path exists, the run pauses before its next batch | _(empty)_ |

How to choose values: [Tuning throughput](README_OPERATIONS.md#tuning-throughput). The statement
sizes `batch_size` must fit: [`MAX_ALLOWED_PACKET_CHECK`](README_VALIDATION.md#max_allowed_packet_check).
The sentinel: [Pausing a run](README_OPERATIONS.md#pausing-a-run-sentinel_file).

---

## `safety:`

| Option | Description | Default |
|--------|-------------|---------|
| `disable_foreign_key_checks` | Copy with `FOREIGN_KEY_CHECKS = 0` on the destination | `false` |

### `disable_foreign_key_checks`

Enable it when the destination was created from a schema-only dump and its lookup tables
(outside the job) are empty but still carry foreign keys: copying child rows that reference them
otherwise fails with error 1452. Do not enable it to cover a relation tree whose order you have
not checked. The setting applies to the copy's own destination connection and is reset to `1`
before that connection returns to the pool. Every archive and copy-only run with it prints a
warning.

---

## `verification:`

One value, globally or per job:

| Value | Compares | Notes |
|-------|----------|-------|
| `sha256` (default) | a row count and one SHA-256 over every copied row, per table, on both servers | Detects a changed value ([how rows are hashed](README_VALIDATION.md#sha256-verification)) |
| `count` | each table's row count on both servers against the keys requested | Proves which keys landed, not their values: a transliterated or truncated text goes unseen, so a non-key charset difference is fatal under it ([Charset and collation](README_VALIDATION.md#charset-and-collation)) |
| `none` | nothing | Conversion warnings (1264, 1265, 1292, 1366) are accepted and counted, and an archive then deletes the source rows; a warning is printed before the run |

```yaml
verification: sha256          # global
jobs:
  copy_old_orders:
    verification: count       # this job only
```

Verification runs inside the batch's destination transaction, before anything is logged or
deleted: a mismatch rolls the batch back and stops the run. Purge copies nothing and takes no
`verification`. The 2.x `method` / `skip_verification` block is refused
([Upgrading to 3.0](README_UPGRADING_3_0.md#configuration-changes)).

---

## `logging:`

| Option | Description | Default |
|--------|-------------|---------|
| `level` | `debug`, `info`, `warn`, `error` | `info` |
| `format` | `json` or `text` | `json` |
| `output` | `stdout`, `stderr`, or a file path | `stdout` |
| `file_only` | Keep every log entry off the console when `output` is a file path | `false` |

- A **file path** in `output` logs every entry to the file, in `format`. A run with `--progress`
  also tees the file's warnings, errors and live events to **stderr**, one text line each: a
  replication hold and the replica recovering, the gate passing again, a sentinel pause and its
  end, recovery replaying. `file_only: true` turns that tee off. Without `--progress` nothing is
  teed, so a cron run prints only a failure (README_OPERATIONS.md,
  [Output and exit codes](README_OPERATIONS.md#output-and-exit-codes)).
- `stdout` or `stderr` makes the log that stream, in `format`. Text has no colour codes; JSON is
  one object per line, the container setup.
- `file_only: true` is refused when `output` is empty, `stdout` or `stderr`.
- **A log file GoArchive creates is readable by its owner only** (`0600`), since the log names
  tables, keys and servers. An existing file is appended to and keeps its owner and mode: when a
  log collector or another user must read the log, create the file first with the mode they
  need. On Windows the file takes its folder's permissions.
- Every entry is tagged `job=<name>`. Logs never contain credentials or DSNs.
- A run's last entry says how it ended, with its counts: `Run completed`, `Run stopped …`, or
  `Run failed` with the error ([the report](README_OPERATIONS.md#what-a-run-does)). Under
  `level: error` only `Run failed` is written.
- Preflight's advisories are warnings, so `level: error` hides them.

**No log rotation.** Files are opened in append mode and held for the whole run, so rotate with
logrotate's `copytruncate`, or between runs.

---

## Per-job overrides and precedence

`processing`, `verification` and `logging` may be set per job:

- **`processing`** — each field the job sets wins, even `0`; each it omits keeps the global value.
  The merged result is validated per job.
- **`verification`** — the job's value wins over the global one.
- **`logging`** — each of `level`, `format`, `output` and `file_only` the job sets wins; each it
  omits keeps the global value.

```yaml
processing:
  sleep_seconds: 5
jobs:
  fast_job:
    processing:
      sleep_seconds: 0     # explicit 0 disables the global 5 s sleep
```

| Setting | CLI flag | Per-job | Global |
|---------|----------|---------|--------|
| `logging.level` | `--log-level` | ✅ | ✅ |
| `logging.format` | `--log-format` | ✅ | ✅ |
| `logging.output`, `logging.file_only` | — | ✅ | ✅ |
| `verification` | `--verification` | ✅ | ✅ |
| `processing.*` | — | ✅ | ✅ |
| `safety.*`, `replication.*` | — | — | ✅ |

A CLI flag wins over the job, which wins over the global block.

---

## Identifier rules

These must match `[A-Za-z0-9_]+`, letters, digits and underscores only:

- `root_table` and every `relations[].table`;
- every column in `primary_key`, `foreign_key` and `references`;
- `destination.job_schema`.

A name with `$`, a dot, a space or a hyphen is refused when the file loads. A relation therefore
cannot name a table in another schema
([consequence](README_LIMITATIONS.md#uncovered-incoming-foreign-keys)).
