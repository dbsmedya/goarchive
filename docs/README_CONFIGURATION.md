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
- [Tracking tables](#tracking-tables)
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

**A DBA must pre-create this schema.** GoArchive never issues `CREATE DATABASE`.
It does create the per-job log tables at runtime, which is why `CREATE` is a
required grant — see [Permissions](README_PERMISSIONS.md).

`job_schema` must satisfy the [identifier rules](#identifier-rules).

---

## `replication:`

Optional. Holds batch processing while any monitored replica is unhealthy, and
resumes the same run once every one of them recovers. Absent from the
configuration, replication monitoring is off.

> **Applies to `archive` only.** `purge` and `copy-only` do not gate on
> replication even when this block is enabled — see
> [Replication gating](README_OPERATIONS.md#replication-gating) and
> [#19](https://github.com/dbsmedya/goarchive/issues/19).

> **Replaces the 2.0 `replica:` block** and `safety.lag_threshold` /
> `safety.check_interval`. Those keys are now **rejected** — a config carrying
> them fails validation with a migration message. See
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
| `type` | Only `async` is supported in 2.1; any other value is rejected | `async` |
| `channels` | Which replication channels to gate on. Omitted or `[]` gates on **every** channel the server reports. | _(all)_ |

```yaml
replication:
  enabled: true
  seconds_behind_source_within: 10
  check_interval: 5
  cache_ttl: 15
  servers:
    - host: replica1.internal
      user: monitor
      password: change_me
    - host: replica2.internal
      port: 3307
      user: monitor
      password: change_me
      channels: ["", "billing"]
```

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
channel twice. Each monitored account needs `REPLICATION CLIENT` — see
[Permissions](README_PERMISSIONS.md).

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
[Trust model](README_LIMITATIONS.md#trust-model).

### `primary_key` must match exactly

`primary_key` has no default; it must be stated for the root table and every
relation. It must also be the column's **exact name including letter case**, and
must be the table's actual `PRIMARY KEY`. Preflight enforces all three — see
`PK_COLUMN_CHECK`, `PK_COLUMN_CASE_CHECK`, and `PRIMARY_KEY_CHECK` in
[Validation](README_VALIDATION.md).

Root primary keys must additionally be an **integer type**.

### `relations:`

Each relation describes one child table. Relations nest to represent
grandchildren.

| Option | Description | Required |
|--------|-------------|----------|
| `table` | Child table name | yes |
| `primary_key` | Child table's primary key column | yes |
| `foreign_key` | Column on the child pointing at the parent's primary key | yes |
| `dependency_type` | `1-1` or `1-N`. Omitted is accepted. | no |
| `relations` | Nested child relations | no |

```yaml
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
```

**Nesting is capped at 10 levels.** Exceeding it fails validation with
`relation nesting exceeds maximum nesting depth of 10`.

Nesting must mirror the real foreign keys. Declaring a grandchild as a sibling
passes YAML validation but fails preflight with `INTERNAL_FK_COVERAGE`, because
the delete order would be wrong and MySQL would reject it with Error 1451.

---

## `processing:`

Batch sizing and pacing. Config-file only — no CLI overrides.

| Option | Description | Default |
|--------|-------------|---------|
| `batch_size` | Root PKs per batch, and the copy chunk size for **every** table. Must be positive. | `1000` |
| `batch_delete_size` | Rows per `DELETE` statement. Must be positive. | `500` |
| `sleep_seconds` | Pause between batches. Must not be negative. Accepts fractions. | `1` |
| `delete_sleep_seconds` | Pause between delete chunks. Must not be negative. Accepts fractions. | `0` |
| `sentinel_file` | Operator pause switch — while this path exists, pause before each batch | _(empty)_ |

`sleep_seconds` and `delete_sleep_seconds` throttle different pressures — general
server load versus binlog/replication lag. See
[Tuning throughput](README_OPERATIONS.md#tuning-throughput) for how to choose
values, and [Pausing a run](README_OPERATIONS.md#pausing-a-run-sentinel_file)
for `sentinel_file`.

`batch_size` interacts with MySQL's 65,535-placeholder limit and
`max_allowed_packet`. Validate it with `goarchive dry-run` before a real run.

---

## `safety:`

| Option | Description | Default |
|--------|-------------|---------|
| `disable_foreign_key_checks` | Set `FOREIGN_KEY_CHECKS = 0` on the destination during copy | `false` |

> `lag_threshold` and `check_interval` were **removed in 2.1** and now live in
> [`replication:`](#replication) as `seconds_behind_source_within` and
> `check_interval`. A config still carrying them fails validation. See
> [Upgrading to 2.1](README_UPGRADING_2_1.md).

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
| `skip_verification` | Skip verification entirely | `false` |

The method is not just a strictness dial — it changes the INSERT strategy and how
a dirty destination is handled:

| | `count` | `sha256` |
|---|---|---|
| Insert statement | plain `INSERT` | `INSERT IGNORE` |
| Pre-existing destination row with the same key | **aborts** before deleting source | tolerated; content verified by hash |
| Detects silent charset transliteration | no | yes |
| Recommended for resuming an interrupted job | no | **yes** |

Because `count` cannot detect transcoded text, a source/destination **column
charset mismatch is fatal** under `count` or when verification is skipped, and
only a warning under a `sha256` verification that actually runs. See
`DEST_SCHEMA_COMPATIBILITY_CHECK` in [Validation](README_VALIDATION.md).

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
not expressible. Cross-schema children are therefore outside the graph model, and
an incoming foreign key from one is fatal at preflight
(`FK_COVERAGE_CHECK`).

---

## Tracking tables

GoArchive maintains two tracking structures in `job_schema` (the destination
database unless overridden).

### `archiver_job`

One row per configured job. Holds checkpoint and heartbeat state.

- `id` — integer `PRIMARY KEY`
- `job_name` — `UNIQUE KEY`
- `job_status`, `last_processed_root_pk_id` — checkpoint state

### `archiver_job_log_<id>`

One table per job, named by that job's integer `id` — not by job name. Tracks
per-root-PK status as a `TINYINT`:

| Value | Status | Meaning |
|-------|--------|---------|
| `0` | pending | discovered, not yet copied |
| `1` | copied | copied and verified, not yet deleted |
| `2` | completed | fully processed |
| `3` | failed | **legacy only** |

Completed rows are kept as evidence and are never deleted automatically.

**Current releases never write `3`.** An error aborts the run and leaves that
batch's rows in a recoverable status (`0` or `1`) for the next run to replay.
The value survives only for rows written by a pre-1.8 release, which block resume
until an operator resolves them.

To find a job's log table:

```sql
SELECT id, job_name FROM <job_schema>.archiver_job;
-- per-job log table: archiver_job_log_<id>
```

A third table, `goarchive_meta`, records the tracking schema's own revision.
GoArchive stamps it when absent and **refuses to run against a revision it does
not recognize**. There is no auto-migration.

📖 Full DDL, inspection queries, and what is safe to prune or truncate:
[Job Tracking Schema — DBA Maintenance Guide](README_JOBS_SCHEMA.md).

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
