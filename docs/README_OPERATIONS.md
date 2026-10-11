# Operations Reference

Commands, flags, tuning, pausing and crash recovery: running GoArchive day to day.

**Related:** [Configuration](README_CONFIGURATION.md) ·
[Validation & Preflight](README_VALIDATION.md) ·
[Permissions](README_PERMISSIONS.md) · [Limitations](README_LIMITATIONS.md) ·
[Tracking tables](README_JOBS_SCHEMA.md) · [← Back to README](../README.md)

---

## Contents

- [Commands](#commands)
- [Flags](#flags)
- [Recommended operator workflow](#recommended-operator-workflow)
- [What a run does](#what-a-run-does)
- [Tuning throughput](#tuning-throughput)
- [Pausing a run: `sentinel_file`](#pausing-a-run-sentinel_file)
- [Replication gating](#replication-gating)
- [Crash recovery](#crash-recovery)
- [Resume semantics](#resume-semantics)
- [Concurrency and locking](#concurrency-and-locking)

---

## Commands

| Command | Description | Deletes from source? |
|---------|-------------|:--------------------:|
| `archive` | Copy, verify, then delete each batch | **yes** |
| `copy-only` | Copy and verify; never deletes | no |
| `purge` | Delete without copying | **yes** |
| `dry-run` | Preflight, then MySQL's plans for the first batch's statements; writes nothing | no |
| `validate` | Load the file and run preflight for every job, or one with `--job` | no |
| `plan` | The relation tree, copy order and delete order, without connecting | no |
| `list-jobs` | The jobs in the file | no |
| `version` | The version | no |

A job runs only under the command its `job_type` names: `goarchive copy-only --job j` refuses an
archive job before connecting.

---

## Flags

### Persistent flags — every command

| Flag | Default | Description |
|------|---------|-------------|
| `-c`, `--config <path>` | `archiver.yaml` | The configuration file |
| `--log-level <level>` | — | Override the log level (`debug`, `info`, `warn`, `error`) |
| `--log-format <format>` | — | Override the log format (`json`, `text`) |

Processing settings (`batch_size`, sleeps, `sentinel_file`) have no flags
([how configuration is loaded](README_CONFIGURATION.md#how-configuration-is-loaded)).

### Command flags

| Command | Flags |
|---------|-------|
| `archive` | `-j`, `--job` · `--verification` · `--skip-validate-preflight` · `--force-triggers` · `--progress[=<interval>]` |
| `copy-only` | `-j`, `--job` · `--verification` · `--skip-validate-preflight` · `--progress[=<interval>]` |
| `purge` | `-j`, `--job` · `--skip-validate-preflight` · `--force-triggers` · `--progress[=<interval>]` |
| `validate` | `-j`, `--job` · `--verification` · `--force-triggers` |
| `dry-run` | `-j`, `--job` · `--verification` · `--skip-validate-preflight` · `--force-triggers` |
| `plan` | `-j`, `--job` |

| Flag | Meaning |
|------|---------|
| `-j`, `--job <name>` | The job to run; required except for `validate`, which checks every job without it |
| `--verification=<sha256\|count\|none>` | Overrides the job's and the file's `verification` for this run ([`verification`](README_CONFIGURATION.md#verification)) |
| `--force-triggers` | Accept source DELETE triggers; they fire on every delete ([`DELETE_TRIGGER_CHECK`](README_VALIDATION.md#delete_trigger_check)) |
| `--skip-validate-preflight` | **Dangerous.** Skips preflight's policy checks ([Bypassing preflight](README_VALIDATION.md#bypassing-preflight)) |
| `--progress[=<interval>]` | Periodic progress on stdout ([Progress display](#progress-display)) |

Before a run starts, each setting that weakens its checks prints one `WARNING:` line:
`--skip-validate-preflight`, `--force-triggers`, `verification: none`, and
`safety.disable_foreign_key_checks`.

### Progress display

A bare `--progress` reports every 30 seconds; `--progress=10s` or `--progress=1m30s` sets the
interval, and a bare integer is seconds (minimum one second). The value must be attached with
`=`: `--progress 10s` is refused with that hint.

```text
progress: roots 4000/~12000 (33.3%) remaining=8000 copied_rows=45210 deleted_rows=45210 elapsed=7m10s eta=14m20s
```

- `roots`, the percentage, `remaining` and the ETA count root rows. The total is counted once,
  after recovery, so `~` marks an estimate: rows can age into or out of the `where` range, so
  the percentage caps at 100 and `remaining` at zero.
- `copied_rows` and `deleted_rows` count rows of every table: archive shows both, copy-only
  `copied_rows`, purge `deleted_rows`.
- While the [sentinel](#pausing-a-run-sentinel_file) pauses the run, `eta=paused` and
  `[PAUSED: sentinel file "…" present]` are shown.
- One final line is printed on every exit; a run that ends naturally shows `remaining=0`.

---

## Recommended operator workflow

```bash
goarchive validate -c archiver.yaml                         # 1. preflight for every job
goarchive dry-run  -c archiver.yaml -j archive_old_orders   # 2. the first batch's plans
goarchive archive  -c archiver.yaml -j archive_old_orders   # 3. the run
```

`dry-run` runs the job's preflight, reads the first batch of root keys, and prints
`EXPLAIN FORMAT=TRADITIONAL` for the root fetch and for each child table's discovery query,
bound with those keys, followed by notes (for example a fetch that sorts every batch) and the
statement caps. It takes no lock, writes no tracking row and changes no data. Read the plans for
full scans before a long run. `plan` shows the relation tree and both orders at any time
without connecting.

---

## What a run does

1. **Load** the file and the job, before any connection.
2. **Connect**: the [connection checks](README_VALIDATION.md#connection-checks) and the session
   settings.
3. **Preflight** under the job's type ([checks](README_VALIDATION.md#which-checks-run-for-which-command)).
4. **Startup**: create or check the [tracking schema](README_JOBS_SCHEMA.md), take the locks
   ([below](#concurrency-and-locking)), check the job's registration and binding, mark it
   running.
5. **Recovery** of the roots an interrupted run left in the log ([below](#crash-recovery)).
6. **Batches** from the start of the `where` range, each continuing after the previous batch's
   last root key: discover every child row parent-first, then per job type:
   - **archive** — copy, verify and log the roots `copied` in one destination transaction, then
     delete child-first in chunks, then mark the roots `completed`;
   - **copy-only** — copy, verify and log the roots `completed` in one destination transaction;
   - **purge** — log the roots `deleting`, delete child-first in chunks, mark them `completed`.

   Each delete chunk is its own source transaction, committed only when it deleted exactly the
   rows asked for.
7. **End** when a batch comes back short, on a graceful stop, or on an error; the job is marked
   idle or failed and a report is printed:

```text
Job archive_old_orders (archive): 12000 roots in 12 batches
  recovered: 300 logged roots finished by replay
  copied:  order_items=45210 orders=12000
  deleted: order_items=45210 orders=12000
  verified (sha256): order_items=45210 orders=12000
  elapsed: 431.207s
```

A copy-only rerun with `resume: true` adds `skipped: <n> roots this job had already copied`;
under `verification: none`, accepted conversions are listed per table; a graceful stop adds
`stopped before the end of the where range: run the job again to continue`.

---

## Tuning throughput

### `batch_size` — the root batch and every statement's key count

Each batch fetches up to `batch_size` root keys and discovers their whole subtree; copy reads,
verification and discovery then address at most `batch_size` keys per statement, and fewer
where a [statement cap](README_VALIDATION.md#max_allowed_packet_check) is lower. It also drives
memory: discovery holds the batch's whole descendant key set
([why it is unbounded](README_LIMITATIONS.md#deep-or-wide-graphs-can-exhaust-memory)). On deep,
high-fanout schemas start at `100` and raise it while watching memory.

### `batch_delete_size` — keys per delete chunk

How many rows one `DELETE` statement removes, each in its own transaction. Lower it to reduce
binlog volume and replication lag on the source's replicas; it does not affect the copy.

### Two pacing knobs

| Knob | Pauses | Use when |
|------|--------|----------|
| `sleep_seconds` | between batches | general load on the source or destination is the concern |
| `delete_sleep_seconds` | between consecutive delete chunks of a batch, across tables | replication lag from delete binlog volume is the bottleneck |

Pair a small `batch_delete_size` with a non-zero `delete_sleep_seconds` when replication lag,
not source load, limits you. Both accept fractional seconds and can be set per job
([per-job overrides](README_CONFIGURATION.md#per-job-overrides-and-precedence)).

### Memory

Go sets no memory limit from cgroups. `GOMEMLIMIT` is a soft limit on the memory the Go runtime
manages, not a hard RSS cap: under a container or systemd `MemoryMax`, set `GOMEMLIMIT` below
that limit. CPU quotas are honoured by default.

### INSERT diagnostics and subdivision

Every copy is a strict multi-row `INSERT`; there is no `INSERT IGNORE`, so a row already in the
destination fails the batch with a duplicate-key error. After every INSERT, GoArchive reads
`SHOW COUNT(*) WARNINGS` and, when it is positive, the complete `SHOW WARNINGS` on the same
transaction. Any condition it cannot prove harmless fails the batch's copy transaction
(`INSERT_DIAGNOSTIC_REJECTED`, or `INSERT_DIAGNOSTICS_UNPROVEN` when the list cannot be read in
full), before anything is logged or deleted. The one exception: under `verification: none`, the
conversions MySQL reports while still writing a changed value (codes 1264, 1265, 1292, 1366) are
accepted and counted in the run report; the source originals may then be deleted.

One INSERT carries at most as many rows as the session keeps warnings for (`max_error_count`,
1024 by default), and within MySQL's 65,535 placeholders, so every condition can be read back: a
5,000-row chunk of a 13-column table becomes INSERTs of 1024, 1024, 1024, 1024 and 904 rows in
the same transaction. Each copy reads the session's `sql_notes`, `sql_mode` and
`max_error_count` first and refuses facts it cannot prove. DATE, DATETIME and TIMESTAMP values
are read and copied as SQL text, never normalized.

Every key-addressed statement (copy read, verification, discovery, delete chunk) is checked the
same way for warning 3170, the range optimizer's fall-back to a full scan: it fails the batch,
and so does a session that kept fewer warnings than the statement raised, since the 3170 could
be among the rest.

Source and destination SQL modes need not match. To accept legacy temporal values, a DBA can set
the destination's global `sql_mode` (or an `init_connect` policy) for new sessions, then restart
the job; this affects other applications on that server. GoArchive appends
`NO_AUTO_VALUE_ON_ZERO` to the inherited mode and never changes a global.

### `max_error_count` during a run

Every new source and destination connection raises a session `max_error_count` of 0 to 1024, or
fails with [`MAX_ERROR_COUNT_CHECK`](README_VALIDATION.md#max_error_count_check). Pools retire a
connection 10 minutes after it opened, or after 5 minutes idle, so a global lowered to 0 mid-run
reaches the next replacement connection: if the account lacks `SESSION_VARIABLES_ADMIN`, the run
stops there. Only the open transaction rolls back; committed copies and delete chunks stay, and
the next run's [recovery](#crash-recovery) finishes the batch once the setting or grant is fixed.
Connections the run holds throughout (the job lock's, a source session pinned through a lag
wait) keep the value they started with.

---

## Pausing a run: `sentinel_file`

```yaml
processing:
  sentinel_file: /var/run/goarchive/pause.flag
```

Before each batch, and before each page of recovery, GoArchive checks whether that file exists.
**While it is present, the run pauses**, re-checking every second and logging a reminder about
every 30 seconds. Remove the file to resume. Presence is the only signal; contents are ignored.
Empty (the default) disables it.

```bash
touch /var/run/goarchive/pause.flag   # pause, e.g. to relieve a struggling replica
rm /var/run/goarchive/pause.flag      # resume
```

- A paused run holds only its job lock's session, so a second run of the same job is refused as
  already running.
- A first `Ctrl-C` or `SIGTERM` ends the pause and stops the run; a second signal cancels at
  once.

---

## Replication gating

With `replication.enabled: true` ([options](README_CONFIGURATION.md#replication)), GoArchive
checks every configured replica **before every delete chunk**, recovery's included, and before a
purge batch logs its roots, and **holds the job** while any of them is unhealthy. The same run
continues once the whole fleet is healthy again: a hold is a pause, not a failure. Copy-only
never deletes, so it is never gated; throttle it with `sleep_seconds` or pause it with the
[sentinel](#pausing-a-run-sentinel_file).

A replica is unhealthy when it is unreachable, when replication is not configured or not
running, when it is further behind than `seconds_behind_source_within`, or when its status
cannot be read, a privilege error included: an account lacking
[`REPLICATION CLIENT`](README_PERMISSIONS.md#monitored-replicas-optional) holds the job rather
than being skipped. By default **every channel counts**; `channels` narrows that
([channel selection](README_CONFIGURATION.md#channel-selection)).

### What you see while it holds

One `WARN` line per unhealthy server per check, with the reason and the time held so far:

```
replication hold: server replica1.internal:3306 unhealthy (channel <default>: lag=42s tolerance=10s); job held, retrying in 5s
```

Healthy servers stay silent. A recovered server logs one `INFO` line, and the last one to recover
logs another with the total hold. If the reason changes while a server is still down, the held
time keeps accumulating. A hold longer than 8 hours fails the next delete chunk
([the limit](README_LIMITATIONS.md#the-job-lock-and-long-replication-holds)).

### `cache_ttl` and the bounded detection delay

A **passing** verdict is cached for `cache_ttl` seconds, so a fast loop does not re-query healthy
replicas before every chunk; failures are never cached. For up to `cache_ttl` seconds after a
pass, a replica that has just become unhealthy is not noticed. `cache_ttl: 0` checks every time.

---

## Crash recovery

There is no checkpoint. Each job's log records the roots of each batch with a status, written
in the same transaction as the work it describes
([statuses](README_JOBS_SCHEMA.md#ga_status-values)).

Rerun the same command to recover; there is no resume subcommand. Before its first batch, a run
replays its log, page by page:

- **archive** — `copied` roots: their trees are rediscovered from the source by key, deleted
  child-first, and the roots marked `completed`. Nothing is copied again.
- **purge** — `deleting` roots, the same way.
- **copy-only** — nothing: each batch's copy, verification and log are one transaction, so a
  batch either happened or did not.

A root already gone from the source was deleted before the crash, after its children, so only
the roots still present are deleted again. Then the run starts at the beginning of the `where`
range: completed archive and purge roots are gone from the source, so they are not seen again.

### Graceful shutdown

The first `SIGTERM` or `SIGINT` lets the batch or recovery page in flight finish, then stops the
run; the report says so. A second signal cancels at once: the open transaction rolls back, and
the next run's recovery finishes what the log holds. A third terminates the process.

---

## Resume semantics

What a rerun does with work already done:

- **archive and purge** — recovery finishes the logged roots, then the scan starts over; the
  rows already archived or purged are no longer in the source.
- **copy-only, `resume: false`** (the default) — a plain copy. A root already in the destination
  fails its batch on the duplicate key, the batch rolls back, the run stops, and the error
  suggests `resume: true`.
- **copy-only, `resume: true`** — each batch drops the roots this job logged as copied before,
  and copies the rest. Every run rescans the whole `where` range.

Before any of this, startup checks the job against its registration: its type, root table and
root key are fixed, and while it has logged work, the config that work depends on must not
change ([Startup checks](README_JOBS_SCHEMA.md#startup-checks)).

---

## Concurrency and locking

GoArchive runs [sequentially by design](README_LIMITATIONS.md#sequential-by-design), per job name
and per root table, through locks on the destination server:

1. **The root-table lock** (`goarchive:root:<table>`) serializes startups on one root table. A
   startup waits up to 10 seconds for it, then refuses: `timed out waiting for the root-table lock
   of "<table>": another job is starting on it`. It is held until the job is marked running.
2. **The same-root check**: a different job marked running on the same root table refuses the
   run. When that job's heartbeat stopped (a crash leaves a job marked running), the error says
   so and gives the `UPDATE … SET job_status = 0` to mark it idle once no process runs it.
3. **The job lock** (`GET_LOCK()` on the job name) is held for the whole run on a dedicated
   connection; a second run of the same job is refused as already running.

Jobs with different names and different root tables can run at the same time against one
destination. Startup needs at least three destination connections (`max_connections` ≥ 3).

### Heartbeat and keepalive timing

| Constant | Value |
|----------|-------|
| Heartbeat write interval | 15 seconds |
| Staleness threshold | 60 seconds |
| Consecutive heartbeat failures before the run stops | 3 |
| Job-lock ownership check | 30 seconds |

### The lock connection must stay alive

Every 30 seconds the run checks that its job lock is still held by its own connection, and stops
if it is not: GoArchive will not go on deleting without its lock. A lost lock is therefore
noticed within 30 seconds. The lock connection is used every 30 seconds and its `wait_timeout` is
raised ([Session settings](README_CONFIGURATION.md#session-settings)), so neither a long run nor
a long pause lets the server close it.
