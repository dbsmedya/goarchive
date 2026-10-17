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
  - [Output and exit codes](#output-and-exit-codes)
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
| `dry-run` | Preflight, the `where` count, then the job, its orders, settings, caps and the first batch's plans; writes nothing | no |
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
| `archive` | `-j`, `--job` · `--verification` · `--skip-dangerously-validate-preflight-allow-experimental` · `--force-triggers` · `--progress[=<interval>]` |
| `copy-only` | `-j`, `--job` · `--verification` · `--skip-dangerously-validate-preflight-allow-experimental` · `--progress[=<interval>]` |
| `purge` | `-j`, `--job` · `--skip-dangerously-validate-preflight-allow-experimental` · `--force-triggers` · `--progress[=<interval>]` |
| `validate` | `-j`, `--job` · `--verification` · `--force-triggers` |
| `dry-run` | `-j`, `--job` · `--verification` · `--skip-dangerously-validate-preflight-allow-experimental` · `--force-triggers` |
| `plan` | `-j`, `--job` |

| Flag | Meaning |
|------|---------|
| `-j`, `--job <name>` | The job to run; required except for `validate`, which checks every job without it |
| `--verification=<sha256\|count\|none>` | Overrides the job's and the file's `verification` for this run ([`verification`](README_CONFIGURATION.md#verification)) |
| `--force-triggers` | Accept source DELETE triggers that preflight can list; they fire on every delete ([`DELETE_TRIGGER_CHECK`](README_VALIDATION.md#delete_trigger_check)) |
| `--skip-dangerously-validate-preflight-allow-experimental` | **Dangerous.** Skips preflight's policy checks ([Bypassing preflight](README_VALIDATION.md#bypassing-preflight)) |
| `--progress[=<interval>]` | Periodic progress, a start line and a closing block ([Progress display](#progress-display), [Output and exit codes](#output-and-exit-codes)) |

Before a run starts, each setting that weakens its checks prints one `WARNING:` line on stderr,
whatever the log level, and records it in the log:
`--skip-dangerously-validate-preflight-allow-experimental`, `--force-triggers`, `verification: none`, and
`safety.disable_foreign_key_checks`. `dry-run` and `validate` print them too.

### Progress display

A bare `--progress` reports every 30 seconds; `--progress=10s` or `--progress=1m30s` sets the
interval, and a bare integer is seconds (minimum one second). The value must be attached with
`=`: `--progress 10s` is refused with that hint.

```text
Job archive_old_orders (archive): starting. Log: /var/log/goarchive/orders.log
progress: roots  4000/~12000 ( 33.3%)  remaining=8000   copied_rows=21640      deleted_rows=21640      elapsed=42s      eta=1m18s
progress: roots  8000/~12000 ( 66.7%)  remaining=4000   copied_rows=43310      deleted_rows=43310      elapsed=1m12s    eta=36s
progress: roots 12000/~12000 (100.0%)  remaining=0      copied_rows=64920      deleted_rows=64920      elapsed=1m49s    eta=0s
```

The columns have fixed widths, so successive lines line up; a count wider than its column
pushes the rest of its own line. With the log as JSON on the console, each line is instead a
`progress` log entry with the fields `roots_done`, `roots_total`, `percent`, `remaining`,
`copied_rows`, `deleted_rows`, `elapsed_seconds` and `eta`
([Output and exit codes](#output-and-exit-codes)).

- `roots`, the percentage, `remaining` and the ETA count root rows. The total is MySQL's
  optimizer estimate of the `where` range, read once after recovery from an `EXPLAIN`; nothing is
  counted, so `--progress` adds no pass over the source. `~` marks the estimate: it is close when
  an index serves the `where`, and can be far off in either direction when the `where` filters
  each row with a subquery or a function. The percentage caps at 100 and `remaining` at zero; past
  the estimate the ETA shows `unknown`, and a run that finishes shows its done count as the total.
  `dry-run` counts the range exactly ([its report](#recommended-operator-workflow) says what that
  count costs).
- `copied_rows` and `deleted_rows` count rows of every table: archive shows both, copy-only
  `copied_rows`, purge `deleted_rows`.
- While the [sentinel](#pausing-a-run-sentinel_file) pauses the run, `eta=paused` and
  `[PAUSED: sentinel file "…" present]` are shown.
- `elapsed` counts from the run's start, so the last line agrees with the closing block's
  duration; the ETA's rate counts from the first batch, so startup and recovery do not slow it.
- One final line is printed on every exit; a run that ends naturally shows `remaining=0`.

### Output and exit codes

goarchive writes a command's **result** on stdout, **diagnostics** on stderr, and **every log
entry** to the log, as `logging` configures it. Nothing reaches stdout that you did not ask for,
and an error always reaches stderr.

| Stream | Carries |
|---|---|
| stdout | The `validate`, `dry-run`, `plan` and `list-jobs` reports and `version`. For a run, only with `--progress`: the start line, the progress lines and the closing block. The log, when `logging.output` is `stdout` (or empty). |
| stderr | Every error; the `WARNING:` banners; with `--progress` and a log file, the warnings and live events. The log, when `logging.output` is `stderr`. |
| log | Every entry, in its `format`, at its `level`. A log file is the run's record. |

**Three logging setups.**

- **A log file** (`output: <path>`), the setup for cron. Without `--progress` a run is silent: it
  prints nothing when it completes or stops, and on a failure prints a summary and the error on
  stderr:

  ```text
  Job archive_old_orders (archive): failed in 61.240s. Log: /var/log/goarchive/orders.log
  Error: engine: …
  ```

  A failure before the first batch says `failed before the first batch.` With `--progress`, the
  start line, the progress lines and the closing block go to stdout (a failure before the first
  batch prints its summary there instead of the block), and the log's warnings go to stderr as
  text lines, with the live events whatever the level: a replication hold, the replica
  recovering, a stop ending a hold, the gate passing again, a sentinel pause and its end, recovery replaying. `file_only: true` keeps every
  log entry off the console; the banners and errors still reach stderr.
- **The log on the console as text** (`output: stdout` or `stderr`, `format: text`): the log
  stream is plain text, with no colour codes. `--progress` adds its lines and the closing block on
  stdout.
- **The log on the console as JSON** (`output: stdout` or `stderr`, `format: json`), the default
  and the setup for containers: every line of the log stream is one JSON object. `--progress`
  writes `Run starting`, `progress` and the closing entry there, whatever the level, instead of
  printing lines; the closing entry carries the counts. On a failure with `output: stdout`, one
  plain `Error:` line goes to stderr; with `output: stderr` the error is the closing entry itself.
  The one exception: an error found before the log exists (a flag, the config file, an unknown job)
  is a plain `Error:` line. `validate` and `dry-run` print their reports as text on stdout in every
  setup; in a container, log them to `stderr` to keep stdout JSON.

The closing block of a run with `--progress`:

```text
Job archive_old_orders (archive): completed in 109.412s
  roots: 12000 in 12 batches

  table          copied  deleted  verified (sha256)
  orders          12000    12000              12000
  order_details   40920    40920              40920
  shipment        12000    12000              12000
```

The status is `completed`, `stopped before the end of the where range: run the job again to
continue`, or `failed` (the error follows on stderr). Purge shows `deleted`, copy-only `copied`
and `verified`; `recovered`, `skipped` and accepted conversions are listed under `roots`.

**Exit codes.**

| Code | Meaning |
|---|---|
| 0 | Done: the run exhausted the `where` range, or `validate`, `dry-run`, `plan`, `list-jobs` or `version` printed its result |
| 1 | Failed: a check refused, a stopped job's unfinished work holds this run's tables, or the run failed part way. Read stderr and the log |
| 2 | Usage or configuration error, found before anything connected: a flag, the config file, an unknown job |
| 3 | Stopped before the end of the `where` range, without error (a graceful stop): the next run continues |
| 4 | Not started: another live run holds the job, a source table, or work on the same data, or is still creating a new tracking schema. Nothing was done |

```bash
goarchive archive -c archiver.yaml -j archive_old_orders
case $? in
  0|3) ;;                     # done, or stopped: the next run continues
  4)   exit 0 ;;              # another run holds the work: try later
  *)   exit 1 ;;              # failed or misused: read stderr and the log
esac
```

A stop that meets an error exits 1.

**Containers.** Keep the default logging, JSON to stdout, and `docker logs` and log drivers read
the run with no file or mount. A log file on a mounted directory belongs to the user the container
runs as: pass `--user "$(id -u):$(id -g)"` to own it ([INSTALL.md](../INSTALL.md#run-the-published-image)). A foreground `docker run` returns goarchive's code (Docker's own
launch failures are 125–127). For a detached container, the code is the number `docker wait
<name>` prints, or `docker inspect -f '{{.State.ExitCode}}' <name>`; the exit status of
`docker wait` itself only says whether the wait worked. `docker stop` sends SIGTERM, a
[graceful stop](#graceful-shutdown): the batch in flight finishes and the run exits 3, if it gets
there before Docker's deadline (10 seconds by default; then SIGKILL, and the next run recovers
what the log holds). Set `docker stop --time`, or compose's `stop_grace_period`, above one batch's
duration. A stop during a replication hold ends the hold without waiting for the replicas, though it
may wait for one replica check in progress ([Graceful shutdown](#graceful-shutdown)).

---

## Recommended operator workflow

```bash
goarchive validate -c archiver.yaml                         # 1. preflight for every job
goarchive dry-run  -c archiver.yaml -j archive_old_orders   # 2. the first batch's plans
goarchive archive  -c archiver.yaml -j archive_old_orders   # 3. the run
```

`dry-run` runs the job's preflight, counts the rows the `where` range matches, reads the first
batch of root keys, and prints a report:

- **Job**: the root table and key, the `where`, the matching roots and about how many batches.
  The count reads the whole `where` range on the source, so on a large range it takes as long as
  that count takes; the range can change before the run.
- **Copy order** and **delete order**, with each table's rows in the first batch.
- **Settings** the run would use: `processing`, the verification (after `--verification`), the
  foreign-key checks, the replication gate and its servers.
- The **statement caps**.
- **Plans for the first batch**: the root fetch, then each child table's first discovery chunk,
  each statement as sent (the `where` as written, the `LIMIT` and the number of parent tuples in
  place of placeholders; no key value), then MySQL's `EXPLAIN FORMAT=TRADITIONAL` rows, one line
  each: `full table scan`, `full index scan on <key>`, `<type> on <key>`, or the optimizer's
  message, with the estimated rows and filter. A note sits under the statement it concerns, for
  example a fetch that sorts every batch.

It takes no lock, writes no tracking row and changes no data. Read the plans for full scans
before a long run. `plan` shows the relation tree and both orders at any time without
connecting.

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
   idle or failed. With `--progress`, a closing block shows the status, the duration and the
   counts per table ([Output and exit codes](#output-and-exit-codes)); without it, the outcome is
   the exit code, the log's closing entry and, on a failure, stderr.

The run's log ends with one entry saying how the run ended, with the report's counts:
`Run completed`, `Run stopped before the end of the where range: run the job again to continue`,
or, at error level, `Run failed` with the error. A run that fails before its logger exists, on a
configuration error, logs nothing. A copy-only rerun with `resume: true` counts `skipped` roots;
under `verification: none`, accepted conversions are listed per table.

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
The two lock sessions, which the run holds throughout, keep the value they started with; a
delete chunk takes a pool session like any other.

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

- A paused run still holds both lock sets, so for the length of the pause a second run of the
  same job is refused as already running, and so is any run whose tables overlap.
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
logs another with the total hold; with `--progress` and a log file, the hold and both recovery
lines also reach stderr. If the reason changes while a server is still down, the held
time keeps accumulating. A hold keeps no source session open, so it can last as long as the
replicas need. A first `SIGTERM` or `SIGINT` ends it without waiting for them to recover
([Graceful shutdown](#graceful-shutdown) names the one check it may wait for), with one more `WARN` line, which reaches stderr as the
hold lines do:

```
replication hold ended by a stop request; job held 2h14m5s
```

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

- **archive** — `copied` roots: their trees are rediscovered from the source by key and checked
  against the destination; when the destination holds every rediscovered row, they are deleted
  child-first and the roots marked `completed`. Nothing is copied again.
- **purge** — `deleting` roots: their trees are rediscovered and deleted the same way; there is
  no destination to check.
- **copy-only** — nothing: each batch's copy, verification and log are one transaction, so a
  batch either happened or did not.

A root already gone from the source was deleted before the run stopped: by a crash after its
deletes and before it was marked completed, by a run that lost its locks between the same two
steps, or by another run that took the job over after a stall. Recovery deletes only the roots
still present, and warns about the rest. Then the run starts at the beginning of the `where`
range: completed archive and purge roots are gone from the source, so they are not seen again.

An archive page whose rediscovered rows the destination does not all hold is refused before any
delete: recovery stops, names the missing rows by table, and leaves the page's roots `copied`.
Every start checks again, so the refusal holds until the rows are resolved, and meanwhile no
other job sharing the tables runs.

- **A missing child row** was either written into its root's tree after the root was copied,
  against the [run contract](README_LIMITATIONS.md#no-ddl-and-no-concurrent-writes-during-a-run-contract),
  or lost by the destination. Copy it into the destination and rerun: recovery then deletes it
  with its tree. Remove it from the source only once you know it is not wanted, since the source
  may hold its last copy.
- **A missing root** was logged in the transaction that copied it, so the destination lost it
  (a restore, a failover that lost transactions, a retention job on the archive tables). Restore
  it to the destination and rerun, or
  [abandon the logged roots](README_JOBS_SCHEMA.md#abandoning-logged-roots).

A root gone from both the source and the destination exists on neither server; recovery warns,
naming it, and finishes the page.

### Graceful shutdown

The first `SIGTERM` or `SIGINT` lets the batch or recovery page in flight finish, then stops the
run with exit code 3; with `--progress` the closing block says so, and the run's log ends with
`Run stopped …`. During a [replication hold](#replication-gating) the first signal ends the hold
without waiting for the replicas: the run stops there with exit code 3, nothing is rolled back,
and the held batch's roots stay logged for the next run's [recovery](#crash-recovery) (a purge
held before it logs its roots leaves nothing). One wait remains: a signal that lands while the
gate is making a chunk's first check of the replicas waits for that check to return, which for an
unreachable replica can be the connection timeout, longer than a container's grace period. Once
the gate has seen an unhealthy replica, a signal ends the hold at once. The closing counts then read: `roots` and
`batches`, the work completed; `copied` and `deleted`, the rows committed, the stopped batch's
included. A second signal cancels at once: the open transaction rolls back, the log ends with
`Run failed`, the cancellation and the counts so far, and the next run's recovery finishes what
the log holds. A third terminates the process.

---

## Resume semantics

What a rerun does with work already done:

- **archive and purge** — recovery finishes the logged roots, then the scan starts over; the
  rows already archived or purged are no longer in the source. A root key that returns to the
  `where` range after its job finished it fails its batch, naming the root: that is outside the
  run contract, and the error names the log row to delete to process it again.
- **copy-only, `resume: false`** (the default) — a plain copy. A root already in the destination
  fails its batch on the duplicate key (or, when its destination rows are gone, on its completed
  log row), the batch rolls back, the run stops, and the error suggests `resume: true`.
- **copy-only, `resume: true`** — each batch drops the roots this job logged as copied before,
  and copies the rest. Every run rescans the whole `where` range.

Before any of this, startup checks the job against its registration: its type, root table and
root key are fixed, and while it has logged work, the config that work depends on must not
change ([Startup checks](README_JOBS_SCHEMA.md#startup-checks)).

---

## Concurrency and locking

GoArchive runs [sequentially by design](README_LIMITATIONS.md#sequential-by-design): two runs
that would touch the same source table never run together, whatever their job names, root
tables or destinations. A run holds two sets of MySQL user-level locks (`GET_LOCK()`) for its
whole length, each on a dedicated session, and on a new tracking schema a third lock for a
moment before them. They are names in MySQL's lock registry, **not table
locks**: GoArchive never runs `LOCK TABLES`, and a name locks no table or row, so the
application's reads and writes are not blocked by it. It only stops another GoArchive run.

**The schema lock**, on the destination, is held only while a run creates a new tracking schema
(a `job_schema` with no `3.0` label yet). One run at a time creates the tables and the label;
another run starting then waits for it, up to 60 seconds, finds the schema ready and goes on.
If the lock is still held after 60 seconds the run does not start (exit code 4) and names the
connection holding it. A schema already labelled takes no lock. Then:

1. **The job lock**, on the destination, beside the tracking tables. It is taken first (after
   the schema lock, on a new tracking schema), so a second run of the same job is refused as
   `job "<name>" is already running: its advisory lock is held by another session
   (destination connection <id>)`.
2. **A lock name for every table of the job's tree**, root included, on the **source**: the
   one server every run that could conflict shares. A run whose tree shares a table with a
   running job is refused before it does anything:
   `the source table <database>.<table> is in use by another GoArchive run (source connection
   <id>). Jobs whose tables overlap run one at a time; this run did nothing.` This holds for
   every job type and across destinations: the same source tables archived to two
   destinations run one after the other, and so do two copy-only jobs reading the same tables.
3. **The unfinished-work guard**, while both lock sets are held. A live job is already refused
   by the table lock names; the guard protects a job that stopped (a crash, an error, a stop
   during recovery) until its own rerun finishes that work. The run is refused before it does
   anything when another job in this run's registry
   ([limits](README_LIMITATIONS.md#sequential-by-design)):
   - is on **the same source data**,
   - shares a table of its tree with this run's,
   - and is marked running, or holds unfinished roots (`copied` or `deleting`) in its log,
     whatever its status.

   *The same source data* is:
   - this server's;
   - that of a server whose transactions are in this server's `gtid_executed`: a promoted
     replica, a clone, or a server this source replicates from. A server that replicates from
     this source holds this source's data, not the other way round, so a job recorded there is
     another server's;
   - where the server cannot tell, every server's database of the same name in the registry,
     counted as one:
     - without `gtid_mode = ON`;
     - on a Group Replication member, whose group logs its transactions under the group name,
       not a member's uuid;
     - for shards provisioned from one another, which keep each other's uuids in
       `gtid_executed`.

   The refusal lists each job with its state and the tables it shares, and the way out: rerun
   that job so its recovery finishes its work. A job marked running with nothing in its log,
   whose process is gone, is cleared as before
   ([Clearing a stale running job](README_JOBS_SCHEMA.md#clearing-a-stale-running-job)). The
   refusal exits 4 while every job it names is live (marked running, heartbeat within the last
   60 seconds), and 1 otherwise: a stopped job's work waits for that job's rerun, by its
   schedule or by an operator.

Every lock name is a prefix and a hash, so any table or job name fits MySQL's 64-character
limit. MySQL computes the same names, so you can find a holder from SQL:

```sql
-- the job lock of job_name in job_schema (run on the destination)
SELECT IS_USED_LOCK(CONCAT('goarchive:j:', LEFT(SHA2(CONCAT(LOWER('<job_schema>'), CHAR(0), LOWER('<job_name>')), 256), 32)));
-- the lock on database.table (run on the source)
SELECT IS_USED_LOCK(CONCAT('goarchive:t:', LEFT(SHA2(CONCAT(LOWER('<database>'), CHAR(0), LOWER('<table>')), 256), 32)));
-- the schema lock of a new tracking schema (run on the destination)
SELECT IS_USED_LOCK(CONCAT('goarchive:s:', LEFT(SHA2(CONCAT(LOWER('<job_schema>'), CHAR(0), LOWER('')), 256), 32)));
```

The result is the holding connection id (`SHOW PROCESSLIST`), or `NULL` when the lock is free.
A run also logs each lock with the job or table it stands for, at debug level.

A run needs at least two source connections and three destination connections
(`max_connections`).

### Heartbeat and keepalive timing

| Constant | Value |
|----------|-------|
| Heartbeat write interval | 15 seconds |
| Staleness threshold | 60 seconds |
| Consecutive heartbeat failures before the run stops | 3 |
| Lock-ownership check, each lock set | 30 seconds, and before every commit |
| Lock session `wait_timeout` | 300 seconds |
| Schema-lock wait, new tracking schema | 60 seconds |

### The lock sessions must stay alive

Every 30 seconds, and again before every transaction the run commits, the run checks that its
own sessions still hold both lock sets. If either is lost, the run stops and the transaction
in flight rolls back: GoArchive never commits a copy, a delete or a log write without its
locks. The check comes after the transaction's writes and before its `COMMIT`.

Each lock session runs with `wait_timeout = 300`, and the 30-second check keeps it from
idling that long. If goarchive's process dies, the server frees its locks at once. If its
host or network is lost, the server frees them within 5 minutes: a run started before then is
refused at once (the job and table locks never wait; only the schema lock of a new tracking
schema waits, up to 60 seconds), and the next start after that goes through.

A run that lost its job lock may find the job already started again by another run. It then
leaves the job's status to that run and its error names that run's destination connection.
