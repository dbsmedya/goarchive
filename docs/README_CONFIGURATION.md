# Configuration Reference

Complete reference for every block, option, default, and precedence rule in
`archiver.yaml`.

**Related:** [Validation & Preflight](README_VALIDATION.md) ·
[Permissions](README_PERMISSIONS.md) · [Operations](README_OPERATIONS.md) ·
[Limitations](README_LIMITATIONS.md) · [Testing](README_TESTING.md) ·
[← Back to README](../README.md)

---

## Contents

- [How configuration is loaded](#how-configuration-is-loaded)
- [`source:` / `destination:`](#source--destination)
- [`replication:`](#replication)
- [`jobs:`](#jobs)
- [`processing:`](#processing)
- [`safety:`](#safety)
- [`verification:`](#verification)
- [`logging:`](#logging)
- [Per-job overrides and precedence](#per-job-overrides-and-precedence)
- [Identifier rules](#identifier-rules)
- [Complete example](#complete-example)

---

## How configuration is loaded

GoArchive reads **exactly one** YAML file. There is no merging, no directory
scan, and no fallback chain.

```bash
goarchive archive -c /etc/goarchive/orders.yaml --job archive_old_orders
```

- `--config` / `-c` selects the file. Default: `archiver.yaml` in the working
  directory.
- The file must be YAML. Values not present in the file take the defaults listed
  in this document.
- Only three CLI flags override file values: `--log-level`, `--log-format`, and
  `--skip-verify`. Everything else — batch sizes, sleeps, sentinel file — is
  **config-file only**, deliberately: one file holds many jobs, and a single CLI
  value cannot be correct for all of them.

Validate before running:

```bash
goarchive validate -c archiver.yaml
```

---

## `source:` / `destination:`

Connection settings. Both blocks accept the same options; `job_schema` is
destination-only and ignored on `source`.

| Option | Description | Default |
|--------|-------------|---------|
| `host` | Database host | **required** |
| `port` | Database port (1–65535) | `3306` |
| `user` | Username | **required** |
| `password` | Password | — |
| `database` | Database name | **required** |
| `tls` | TLS mode — `disable`, `preferred`, `skip-verify`, or `required` | `preferred` |
| `max_connections` | Max open connections (must not be negative) | `10` |
| `max_idle_connections` | Max idle connections (must not be negative) | `5` |

**Every session runs in UTC.** GoArchive opens each connection — source, destination, every
replication server — with `time_zone = '+00:00'`, and refuses to start if a server or proxy does
not honour it. This is what keeps a `TIMESTAMP` value the same instant on both sides: MySQL
converts `TIMESTAMP` through the *session* zone on every read and write, so two servers in
different zones — or two `SYSTEM` zones that resolve differently — would otherwise shift every
copied value by their offset while verification still matched. It is not configurable.
`DATETIME`, `DATE` and `TIME` are stored as written and are unaffected.

Source and destination must be different databases
([`SRC_DEST_IDENTITY_CHECK`](README_VALIDATION.md#connection-identity-check)). Every connection
also preserves explicit AUTO_INCREMENT zero values
([`AUTO_INCREMENT_ZERO_MODE_CHECK`](README_VALIDATION.md#error-prefixes-that-are-not-checks)),
and session SQL modes and INSERT diagnostics are covered in
[Operations](README_OPERATIONS.md#insert-diagnostics-and-subdivision).

### `job_schema` (destination only)

| Option | Description | Default |
|--------|-------------|---------|
| `job_schema` | Schema holding GoArchive's tracking tables (`archiver_job`, `archiver_job_log_<id>`, `goarchive_meta`) | same as `database` |

Use it to keep tracking tables out of the archive data schema:

```yaml
destination:
  database: archive
  job_schema: goarchive
```

**A DBA must pre-create this schema.** GoArchive never issues `CREATE DATABASE`; it does
create the tracking tables, so its account needs `CREATE` there
([Permissions](README_PERMISSIONS.md#why-create-on-the-tracking-schema)). The tables
themselves are described in [Job Tracking Schema](README_JOBS_SCHEMA.md).

---

## `replication:`

Optional. Holds batch processing while any monitored replica is unhealthy, and
resumes the same run once every one of them recovers. Absent from the
configuration, replication monitoring is off.

> **Applies to `archive` and `purge`.** `copy-only` does not gate on replication
> even when this block is enabled — it never deletes from source. See
> [Replication gating](README_OPERATIONS.md#replication-gating).

> A config carrying the old `replica:` block or `safety.lag_threshold` /
> `safety.check_interval` fails validation with a migration message — see
> [Upgrading to 2.1](README_UPGRADING_2_1.md).

| Option | Description | Default |
|--------|-------------|---------|
| `enabled` | Turn replication gating on | `false` |
| `seconds_behind_source_within` | Lag tolerance in seconds. `0` demands exact sync. Must not be negative. | `10` |
| `check_interval` | Seconds between re-checks while holding. Must be positive **when enabled**. | `5` |
| `cache_ttl` | Seconds a **passing** verdict stays fresh. `0` checks every time. Must not be negative. | `15` |
| `servers` | One or more replicas to monitor. At least one is required **when enabled**. | — |

### `servers[]`

| Option | Description | Default |
|--------|-------------|---------|
| `host` | Replica host. Must not contain a newline or carriage return. | required |
| `port` | Replica port (1–65535) | `3306` |
| `user` | Account used to read replication status | required |
| `password` | Account password | — |
| `tls` | `disable`, `preferred`, `skip-verify`, or `required` | `preferred` |
| `type` | Only `async` is accepted; any other value is rejected | `async` |
| `channels` | Which replication channels to gate on. Omitted or `[]` gates on **every** channel the server reports. | _(all)_ |

### Channel selection

MySQL's default channel is **named `""` — an empty name, not an absence**. That
distinction is the one non-obvious spelling in this block:

| `channels` value | Gates on |
|---|---|
| omitted, or `[]` | every channel the server reports |
| `[""]` | the default (unnamed) channel only |
| `["", "billing"]` | the default channel **and** `billing` |

Every listed channel must exist on that server; a missing one fails the check
rather than being silently ignored. In logs the default channel renders as
`<default>`.

Two servers may not share a `host:port`, and one server may not list the same
channel twice. Each monitored account needs
[`REPLICATION CLIENT`](README_PERMISSIONS.md#monitored-replicas-optional).

Per-server settings are validated **whenever they are present**, even with
`enabled: false`, so a disabled block cannot hide a typo that would only surface
the day someone turns it on.

---

## `jobs:`

A map of job name → job definition. **At least one job is required.**

| Option | Description | Required |
|--------|-------------|----------|
| `root_table` | Table the archive starts from | yes |
| `primary_key` | Primary key column of `root_table` | yes |
| `where` | Raw SQL WHERE clause selecting root rows | yes |
| `relations` | Child tables to include | no |
| `processing` | Per-job [processing overrides](#per-job-overrides-and-precedence) | no |
| `verification` | Per-job [verification overrides](#per-job-overrides-and-precedence) | no |
| `logging` | Per-job [logging overrides](#per-job-overrides-and-precedence) | no |

### `where` is mandatory

There is no implicit "archive everything". An empty or whitespace-only `where`
fails validation. To process a whole table, opt in explicitly:

```yaml
where: "1=1"
```

`where` is a **raw SQL fragment** injected into selection queries. Configuration
is treated as trusted operator input — see
[Trust model](README_LIMITATIONS.md#trust-model). Each database call carries exactly one SQL
statement, so a `where` is always part of a single statement: text that adds a second one fails
with a MySQL syntax error. The root table is queried without an alias, so a subquery in `where`
refers to the row being tested by the table name (`orders.id`).

A recurring job only archives rows that are cold when it first reaches them; see the
[eligibility rule](README_LIMITATIONS.md#no-ddl-and-no-concurrent-writes-during-a-run-contract).

`where` is evaluated **in a UTC session** (see [`source:` / `destination:`](#source--destination)).
A `TIMESTAMP` column compared with `NOW()` is unaffected — both follow the session — but a
`DATETIME` column compared with `NOW()`, a `TIMESTAMP` column compared with a bare literal, and
`CURDATE()` / `DATE()` day boundaries all sit at UTC rather than at your server's local zone.
To pin a boundary to a local time, give the literal an offset —
`created_at < '2025-01-01 00:00:00+03:00'` (MySQL 8.0.19+) — or use `CONVERT_TZ`.

### `primary_key` must match exactly

`primary_key` has no default; it must be stated for the root table and every
relation, as the column's exact name, and must be the table's actual `PRIMARY KEY`. Preflight
enforces this, and an integer root key
([Primary key checks](README_VALIDATION.md#primary-key-checks)).

### `relations:`

Each relation describes one child table. Relations nest to represent
grandchildren.

| Option | Description | Required |
|--------|-------------|----------|
| `table` | Child table name | yes |
| `primary_key` | Child table's primary key column | yes |
| `foreign_key` | Column on the child that references the parent's primary key | yes |
| `dependency_type` | `1-1` or `1-N`. Omitted is accepted. | no |
| `relations` | Nested child relations | no |

A relation does not need a foreign key declared in the database: relations your application
manages (ORM-style) are declared here like any other, and you index each `foreign_key` column
yourself. Where the database does declare foreign keys, the relations must mirror them —
nesting included; what preflight guarantees is in
[Foreign key checks](README_VALIDATION.md#foreign-key-checks). The
[complete example](#complete-example) shows nesting.

**Nesting is capped at 10 levels.** Exceeding it fails validation with
`relation nesting exceeds maximum nesting depth of 10`.

---

## `processing:`

Batch sizing and pacing.

| Option | Description | Default |
|--------|-------------|---------|
| `batch_size` | Root PKs per batch, and the copy chunk size for **every** table. Must be positive. | `1000` |
| `batch_delete_size` | Rows per `DELETE` statement. Must be positive. | `500` |
| `sleep_seconds` | Pause between batches. Must not be negative. Accepts fractions. | `1` |
| `delete_sleep_seconds` | Pause between delete chunks of a batch, including between tables. Must not be negative. Accepts fractions. | `0` |
| `sentinel_file` | Operator pause switch — while this path exists, pause before each batch | _(empty)_ |

How to choose values: [Tuning throughput](README_OPERATIONS.md#tuning-throughput);
`sentinel_file`: [Pausing a run](README_OPERATIONS.md#pausing-a-run-sentinel_file); the limits
`batch_size` must respect: [Dry-run payload validation](README_VALIDATION.md#dry-run-payload-validation).

---

## `safety:`

| Option | Description | Default |
|--------|-------------|---------|
| `disable_foreign_key_checks` | Set `FOREIGN_KEY_CHECKS = 0` on the destination during copy | `false` |

### `disable_foreign_key_checks`

Disabled by default. When enabled, `goarchive validate` and every copy run emit a
loud warning.

It runs on a **dedicated destination connection** and is explicitly reset to `1`
before that connection returns to the pool, so it cannot leak into other pooled
destination operations.

Enable it when the destination was initialized from a DDL-only schema dump and
reference tables (lookup tables outside the archived subgraph) are empty but
still carry foreign-key constraints. Copying child rows that reference those
empty tables otherwise fails with Error 1452. This is a normal operator
scenario. Do not enable it to paper over a graph whose copy order you have not
verified.

---

## `verification:`

| Option | Description | Default |
|--------|-------------|---------|
| `method` | `count` or `sha256` | `count` |
| `skip_verification` | Skip copied-data comparison and accept reported conversion/truncation warnings; SQL errors, unknown/incomplete diagnostics and [temporal identity failures](README_LIMITATIONS.md#temporal-values-and-identities) remain fatal. | `false` |

Recognized conversion warnings are codes `1264`, `1265`, `1292`, and `1366`, at
Warning or Note level, from a successful INSERT. All diagnostics must be readable
and complete. Actual SQL errors remain fatal even with one of those codes.
The existing precedence remains CLI > explicit job value > global value. Skipping
comparison still forces plain INSERT, so duplicates remain fatal. Accepted
conversion totals describe diagnostics in committed copies, not modified rows.

The method is not just a strictness dial — it changes the INSERT strategy and how
a dirty destination is handled:

| | `count` | `sha256` |
|---|---|---|
| Insert statement | plain `INSERT` | `INSERT IGNORE` only when comparison is enabled and destination has no secondary unique index; otherwise plain `INSERT` |
| Pre-existing destination row with the same key | **aborts** before deleting source | tolerated; content verified by hash |
| Detects silent charset transliteration | no | yes |
| Recommended for resuming an interrupted job | no | **yes** |

Because `count` cannot detect transcoded text, a column charset mismatch is treated
differently by method ([Charset and collation](README_VALIDATION.md#charset-and-collation)).
How `sha256` hashes rows: [SHA256 verification](README_VALIDATION.md#sha256-verification).

Verification method also governs whether an interrupted job can auto-resume — see
[Resume semantics](README_OPERATIONS.md#resume-semantics).

---

## `logging:`

| Option | Description | Default |
|--------|-------------|---------|
| `level` | `debug`, `info`, `warn`, `error` | `info` |
| `format` | `json` or `text` | `json` |
| `output` | `stdout`, `stderr`, or a file path | `stdout` |
| `file_only` | Suppress the stdout tee when `output` is a file path | `false` |

Behaviour:

- A **file path** in `output` logs to the file **and** tees to stdout. The file
  is plain text with no ANSI escapes; the stdout tee stays coloured.
- `file_only: true` suppresses the tee. It is **rejected at validation** when
  `output` is empty, `stdout`, or `stderr` — there would be nowhere to log.
- Every entry is tagged `job=<name>`, so runs stay attributable when several jobs
  share an output file.
- Logs never contain credentials or DSNs.

### No log rotation

GoArchive does not rotate logs. Files are opened in **append** mode and the
handle stays open for the whole run, so external rotation must not move the file:
use logrotate with `copytruncate`, or rotate between runs when scheduling by
cron.

```
/var/log/goarchive/*.log {
    daily
    rotate 14
    compress
    missingok
    notifempty
    copytruncate
}
```

---

## Per-job overrides and precedence

`processing`, `verification`, and `logging` may each be overridden per job. The
three blocks do **not** share the same inheritance rule.

### `processing` — pointer semantics

Every field is optional and distinguishes "unset" from "explicitly zero". An
unset field inherits the global value; **an explicitly set field wins even when
it is `0`.**

```yaml
processing:
  batch_size: 1000
  sleep_seconds: 5

jobs:
  fast_job:
    processing:
      sleep_seconds: 0     # explicit 0 — disables the global 5s sleep
      # batch_size unset   — inherits 1000
```

The merged result is validated per job, so a job that overrides `batch_size: 0`
fails validation against that job's name.

### `verification` — empty-string semantics for `method`

`method` inherits when it is **absent or empty**, so there is no way to "unset"
it back to the default from a job block. `skip_verification` uses pointer
semantics like the processing fields, so an explicit `false` is honoured.

### `logging` — empty-string semantics, with one trap

`level`, `format`, and `output` inherit when absent or empty.

> **`file_only` does not inherit.** It is a plain boolean, so **any job that
> defines a `logging:` block at all** takes that block's `file_only` value —
> including the default `false` when the field is omitted. A job with a `logging:`
> block therefore silently re-enables the stdout tee even if the global block set
> `file_only: true`. Restate `file_only: true` in every job block that needs it.

### Full precedence chain

For `level` and `format`: **CLI flag > per-job block > global block.**

```
--log-level debug   →  wins over jobs.<name>.logging.level  →  wins over logging.level
```

`--skip-verify` is stronger still: it forces skipping for the run and **clears
any per-job `skip_verification` value**, so a job's explicit
`skip_verification: false` cannot undo an operator's `--skip-verify`.

| Setting | CLI flag | Per-job | Global |
|---------|----------|---------|--------|
| `logging.level` | `--log-level` | ✅ | ✅ |
| `logging.format` | `--log-format` | ✅ | ✅ |
| `logging.output` | — | ✅ | ✅ |
| `logging.file_only` | — | ✅ (always wins, see trap above) | ✅ |
| `verification.skip_verification` | `--skip-verify` | ✅ | ✅ |
| `verification.method` | — | ✅ | ✅ |
| all `processing.*` | — | ✅ | ✅ |
| all `safety.*` | — | — | ✅ |

---

## Identifier rules

These values must match `[A-Za-z0-9_]+` — letters, digits, and underscores only:

- `jobs.<name>.root_table`
- `jobs.<name>.primary_key`
- `jobs.<name>.relations[].table`
- `jobs.<name>.relations[].foreign_key`
- `jobs.<name>.relations[].primary_key`
- `destination.job_schema`

Names containing `$`, dots, spaces, or hyphens are rejected at config load. This
also means a relation cannot name a table in another schema — `schema.table` is
not expressible ([consequence](README_LIMITATIONS.md#uncovered-incoming-foreign-keys)).

---

## Complete example

A fuller annotated example ships in the repository at
[`configs/archiver.yaml.example`](../configs/archiver.yaml.example).

```yaml
source:
  host: source-db.internal
  port: 3306
  user: archiver
  password: change_me
  database: production
  tls: skip-verify
  max_connections: 10
  max_idle_connections: 5

destination:
  host: archive-db.internal
  port: 3306
  user: archiver
  password: change_me
  database: archive
  tls: skip-verify
  max_connections: 10
  max_idle_connections: 5
  job_schema: goarchive        # DBA must CREATE DATABASE goarchive first

replication:
  enabled: false
  seconds_behind_source_within: 10
  check_interval: 5
  cache_ttl: 15
  servers:
    - host: replica-db.internal
      port: 3306
      user: monitor
      password: change_me
      channels: []            # [] = every channel; [""] = default channel only

jobs:
  archive_old_orders:
    root_table: orders
    primary_key: id
    where: "created_at < DATE_SUB(NOW(), INTERVAL 2 YEAR)"
    relations:
      - table: order_items
        primary_key: id
        foreign_key: order_id
        dependency_type: "1-N"
      - table: shipments
        primary_key: id
        foreign_key: order_id
        dependency_type: "1-1"
        relations:
          - table: shipment_items
            primary_key: id
            foreign_key: shipment_id
            dependency_type: "1-N"
    processing:
      sleep_seconds: 0         # explicit 0 overrides the global 1s
    logging:
      output: /var/log/goarchive/archive_old_orders.log
      file_only: true          # must be restated — it does not inherit

processing:
  batch_size: 1000
  batch_delete_size: 500
  sleep_seconds: 1
  delete_sleep_seconds: 0
  sentinel_file: ""

safety:
  disable_foreign_key_checks: false

verification:
  method: count
  skip_verification: false

logging:
  level: info
  format: json
  output: stdout
  file_only: false
```
