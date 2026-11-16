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
- [Group Replication](#group-replication)
- [Runtime control](#runtime-control)
  - [Enabling it and where the socket is](#enabling-it-and-where-the-socket-is)
  - [The client: `goarchive control`](#the-client-goarchive-control)
  - [The protocol contract](#the-protocol-contract)
  - [When a change applies](#when-a-change-applies)
  - [Security](#security)
  - [Docker and systemd](#docker-and-systemd)
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
| `plan` | The relation tree, copy order and delete order, with the processing and verification settings, without connecting | no |
| `list-jobs` | The jobs in the file, with their type, keys and relations, without connecting | no |
| `version` | The version, commit, Go version and platform | no |
| `control <socket-path> …` | Send one request to a running job's [control socket](#the-client-goarchive-control): an action, `-s <key>=<value>` or `-g <key>`. Reads no configuration file | no |
| `completion <shell>` | A shell completion script (`bash`, `zsh`, `fish`, `powershell`) on stdout | no |
| `help [command]` | Help for any command, as `--help` | no |

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
| `-h`, `--help` | — | Help for the command |

`control` is the one command that refuses `-c`/`--config`, set before or after it: it finds the run by its
socket path and reads no file ([the client](#the-client-goarchive-control)).

`goarchive -v` or `goarchive --version` prints the version on one line; `goarchive version` adds
the commit, Go version and platform.

An unknown `--log-level` or `--log-format` value is not refused: the level falls back to `info`
and the format to `text`. The same values in the file's `logging` block are refused.

Processing settings (`batch_size`, sleeps, `sentinel_file`) have no flags
([how configuration is loaded](README_CONFIGURATION.md#how-configuration-is-loaded)).

### Command flags

| Command | Flags |
|---------|-------|
| `archive` | `-j`, `--job` · `--verification` · `--skip-dangerously-validate-preflight-allow-experimental` · `--force-triggers` · `--allow-multi-primary` · `--progress[=<interval>]` |
| `copy-only` | `-j`, `--job` · `--verification` · `--skip-dangerously-validate-preflight-allow-experimental` · `--allow-multi-primary` · `--progress[=<interval>]` |
| `purge` | `-j`, `--job` · `--skip-dangerously-validate-preflight-allow-experimental` · `--force-triggers` · `--allow-multi-primary` · `--progress[=<interval>]` |
| `validate` | `-j`, `--job` · `--verification` · `--force-triggers` · `--allow-multi-primary` |
| `dry-run` | `-j`, `--job` · `--verification` · `--skip-dangerously-validate-preflight-allow-experimental` · `--force-triggers` · `--allow-multi-primary` |
| `plan` | `-j`, `--job` |
| `control` | `-s`, `--set` · `-g`, `--get` (one action, or one of these; `-c`/`--config` is refused) |

| Flag | Meaning |
|------|---------|
| `-j`, `--job <name>` | The job to run; required except for `validate`, which checks every job without it |
| `--verification=<sha256\|count\|none>` | Overrides the job's and the file's `verification` for this run ([`verification`](README_CONFIGURATION.md#verification)). Ignored for a purge job under `validate` and `dry-run` |
| `--force-triggers` | Accept source DELETE triggers that preflight can list; they fire on every delete ([`DELETE_TRIGGER_CHECK`](README_VALIDATION.md#delete_trigger_check)). Under `validate` and `dry-run` it has no effect on a copy-only job and prints no warning there |
| `--allow-multi-primary` | Accept a Group Replication group that is in multi-primary mode, or whose mode cannot be read, on the source or the destination: advisory locks are local to one member, so two runs on different members are not serialized ([`GROUP_MULTI_PRIMARY_CHECK`](README_VALIDATION.md#group_multi_primary_check)). A command-line flag only, with no configuration key; the skip flag below does not stand in for it |
| `--skip-dangerously-validate-preflight-allow-experimental` | **Dangerous.** Skips preflight's policy checks ([Bypassing preflight](README_VALIDATION.md#bypassing-preflight)) |
| `--progress[=<interval>]` | Periodic progress, a start line and a closing block ([Progress display](#progress-display), [Output and exit codes](#output-and-exit-codes)) |
| `-s`, `--set <key>=<value>` | `control` only: change one setting of a running job, sent as `<key>=<value>`. Given once per call |
| `-g`, `--get <key>` | `control` only: read one setting of a running job, sent as `<key>=?`. Given once per call |

Before a run starts, each setting that weakens its checks is reported once on stderr, whatever
the log level, and recorded in the log:
`--skip-dangerously-validate-preflight-allow-experimental` on any job; `--force-triggers` on
archive and purge jobs; `verification: count`, `verification: none` and
`safety.disable_foreign_key_checks` on archive and copy-only jobs (purge copies nothing). Under
`count` or `none`, a job with JSON columns gets one more, naming them
([Warnings](README_VALIDATION.md#warnings)). With `logging.output: stderr` the warning is one log
entry on stderr, in the log's format; otherwise it is a plain `WARNING:` line. `dry-run` and
`validate` print them too.

A group that `--allow-multi-primary` accepted gets one banner for each side it accepted, naming the case (a source
that is a running member of a group in multi-primary mode, a server configured for multi-primary mode, or a mode
that could not be read), also under the skip flag; a job whose servers are not in such a group gets none, flag or
not ([`GROUP_MULTI_PRIMARY_CHECK`](README_VALIDATION.md#group_multi_primary_check)).

An archive or purge job whose source is an `ONLINE` Group Replication member, with `replication.enabled: true`
and no server of `type: group` listed, gets one more, also under the skip flag:
`WARNING: replication: the source is a Group Replication member and no server of type group is monitored: the
group's backlog does not hold this job`. It is a banner and not a refusal, because gating only an asynchronous
replica is a legitimate choice. A job with replication off, or a source that is no member, gets none.

A replication lag tolerance raised above its configured value over the
[control socket](#the-tolerances-step-rule-and-the-warning) is reported the same way, each time it is
raised, while the run is going: one `WARNING:` line on stderr and a log entry, whatever the log level.

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
`copied_rows`, `deleted_rows`, `elapsed_seconds` and `eta`, and, while paused, `paused`
([Output and exit codes](#output-and-exit-codes)).

- `roots`, the percentage, `remaining` and the ETA count root rows. The total is MySQL's
  optimizer estimate of the `where` range, read once after recovery from an `EXPLAIN`; nothing is
  counted, so `--progress` adds no pass over the source. `~` marks the estimate: it is close when
  an index serves the `where`, and can be far off in either direction when the `where` filters
  each row with a subquery or a function. The percentage caps at 100 and `remaining` at zero; the
  ETA shows `calculating` until a root is done and `unknown` past the estimate, and a run that
  finishes shows its done count as the total. `dry-run` counts the range exactly
  ([its report](#recommended-operator-workflow) says what that count costs).
- If that `EXPLAIN` fails, or its plan has no row GoArchive can read for the root table, the run
  fails before its first batch (`engine: --progress estimate on <table>: …`); run without
  `--progress` to proceed.
- `copied_rows` and `deleted_rows` count rows of every table: archive shows both, copy-only
  `copied_rows`, purge `deleted_rows`.
- While the run is [paused](#pausing-a-run-sentinel_file), `eta=paused` and `[PAUSED: …]` are shown, naming
  what holds it: `control socket`, `sentinel file "…" present`, or both. A change of the reason during a
  pause is logged.
- `elapsed` counts from the run's start (when the command began the run, before it connected), so the
  last line agrees with the closing block's duration; the ETA's rate counts from the first batch, so startup and recovery do not slow it.
- Once the first batch has started, one final line is printed on every exit; a run that ends
  naturally shows `remaining=0`. A run that stops or fails during startup or recovery prints no
  progress line.

### Output and exit codes

goarchive writes a command's **result** on stdout, **diagnostics** on stderr, and **every log
entry** to the log, as `logging` configures it. Nothing reaches stdout that you did not ask for,
and an error always reaches stderr.

| Stream | Carries |
|---|---|
| stdout | The `validate`, `dry-run`, `plan` and `list-jobs` reports, `version`, `--version`, `completion` and the help. For a run, only with `--progress`: the start line, the progress lines and the closing block. The log, when `logging.output` is `stdout` (or empty). |
| stderr | Every error; the `WARNING:` banners; with `--progress` and a log file, the warnings and errors (as `level` keeps them) and live events. The log, when `logging.output` is `stderr`. |
| log | Every entry, in its `format`, at its `level`. A log file is the run's record. |

**Three logging setups.**

- **A log file** (`output: <path>`), the setup for cron. Without `--progress` a run is silent: it
  prints nothing when it completes or stops, and on a failure prints a summary and the error on
  stderr:

  ```text
  Job archive_old_orders (archive): failed in 61.240s. Log: /var/log/goarchive/orders.log
  Error: engine: …
  ```

  A failure before the run's startup completes (connecting, preflight, locks, registration) says
  `failed before the first batch.`; a failure in recovery shows its duration. With `--progress`, the
  start line, the progress lines and the closing block go to stdout (a failure before startup
  completes prints its summary there instead of the block), and the log's warnings and errors (as
  `level` keeps them) go to stderr as text lines, with the live events whatever the level: a
  replication hold, the replica recovering, a stop ending a hold, the gate passing again, a sentinel
  pause and its end, recovery replaying. `file_only: true` keeps every log entry off the console;
  the banners and errors still reach stderr.
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
  setup; in a container, set `logging.output: stderr` for them, so stdout carries only the
  report and stderr only the log.

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
and `verified`; under `verification: none` there is no `verified` column. `recovered`, `skipped`
and accepted conversions are listed under `roots`.

A run whose [control socket](#runtime-control) accepted changes lists them under `roots` too, and the run's
closing log entry carries the same line as its `runtime_changes` field:

```text
  runtime changes: batch_delete_size 500 -> 100 (2 changes); paused 1 time for 4m10s
```

For each setting that was changed it names the configured value, the final value and the number of
changes (a setting changed and then restored is still listed); a lag tolerance that ends above its
configured value is marked, as in `seconds_behind_source_within 10 -> 40 (3 changes, above the configured
value)`. A run that accepted nothing prints and logs nothing new. A run's console is otherwise unchanged
with control enabled: the socket's path is in the log only (`Control socket: <path>`), and a successful
cron-style run stays silent.

**Exit codes.**

| Code | Meaning |
|---|---|
| 0 | Done: the run exhausted the `where` range, or `validate`, `dry-run`, `plan`, `list-jobs` or `version` printed its result, or `control` printed a run's `OK` reply |
| 1 | Failed: a check refused, a stopped job's unfinished work holds this run's tables, or the run failed part way. Read stderr and the log. For `control`: the run answered `ERR`, refusing the request, and **1 always means that** there |
| 2 | Usage or configuration error, found before anything connected: a flag, the config file, an unknown job; for `control`, a malformed command line ([the client](#the-client-goarchive-control)) |
| 3 | Stopped before the end of the `where` range, without error (a graceful stop): the next run continues |
| 4 | Not started: another live run holds the job, a source table, or work on the same data, is still creating a new tracking schema, or serves the job's control socket. Nothing was done |
| 5 | `control` only. No socket: nothing is at the path, or a socket file is there and the connection is refused because nothing listens on it, as after a run that was killed. The message names the path: the job is not running, control is not enabled for it, or the path is wrong |
| 6 | `control` only. Timeout: the client's own deadline passed. The connection was not made within 5 seconds, or the exchange did not finish within 10 |
| 7 | `control` only. Malformed reply: the first line is neither `OK` nor `ERR <reason> <message>`, an `ERR` line is followed by more, the reply is empty, does not end with a line feed, or is over 1 MiB. Nothing is printed |
| 8 | `control` only. Refused endpoint: the client's own check of the socket's directory or of the socket failed ([the client](#the-client-goarchive-control)); nothing was connected or sent |
| 9 | `control` only. Transport failure: any other failure to connect or to exchange, for example a permission error on connect, or a connection reset before the reply was complete |

```bash
goarchive archive -c archiver.yaml -j archive_old_orders
case $? in
  0|3) ;;                     # done, or stopped: the next run continues
  4)   exit 0 ;;              # another run holds the work: try later
  *)   exit 1 ;;              # failed or misused: read stderr and the log
esac
```

`goarchive control` never returns 3 or 4, which describe a run; 5 to 9 are its own, so that a collector
that polls `status-json` between cron runs can tell "no run answers at this path" (5) from every other way
of failing to get an answer, without reading stderr:

```bash
goarchive control "$SOCK" status-json || [ $? -eq 5 ]   # a run that is not there is not an error
```

A stop that meets an error exits 1. A [`stop` over the control socket](#graceful-shutdown) changes no rule: the
code is decided as for a signal, and `OK` to the request acknowledges it, not an outcome.

**Containers.** Keep the default logging, JSON to stdout, and `docker logs` and log drivers read
the run with no file or mount. On a Linux host, a log file on a mounted directory belongs to the
user the container runs as, and the container reads a mounted config only if that user may: pass
`--user "$(id -u):$(id -g)"` to run as yourself
([INSTALL.md](../INSTALL.md#run-the-published-image)). A foreground `docker run` returns
goarchive's code (Docker's own launch failures are 125–127). For a detached container, the code is the number `docker wait
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
  foreign-key checks, the replication gate and its servers, and the
  [control socket](#enabling-it-and-where-the-socket-is): `control socket: <path>`, with the default
  directory marked as the invoking user's, or `off`. `validate` prints the same line under each job.
- The **statement caps**.
- **Plans for the first batch**: the root fetch, then each child table's first discovery chunk,
  each statement as sent (the `where` as written, the `LIMIT` and the number of parent tuples in
  place of placeholders; no key value), then MySQL's `EXPLAIN FORMAT=TRADITIONAL` rows, one line
  each: `full table scan`, `full index scan on <key>`, `<type> on <key>`, or the optimizer's
  message, with the estimated rows and filter. A note sits under the statement it concerns, for
  example a fetch that sorts every batch, or a `where` subquery that sorts.

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

Each `DELETE` after the first of a batch sleeps `delete_sleep_seconds` first, and only then is the
[replication gate](#replication-gating) asked, so the gate decision comes after the pause, just
before the statement. The decision may be a pass that is still valid in the gate's cache
(`cache_ttl`), which lets the `DELETE` go without a new check. A first stop request does not shorten
the pause. After it, a healthy gate lets the batch finish; if the gate sees an unhealthy replica, the
hold ends at once ([Graceful shutdown](#graceful-shutdown)) and the next run's
[recovery](#crash-recovery) finishes the unfinished batch.

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
Empty (the default) disables it. A path that cannot be checked (for example, permission denied)
counts as absent: the run logs one warning and does not pause.

```bash
touch /var/run/goarchive/pause.flag   # pause, e.g. to relieve a struggling replica
rm /var/run/goarchive/pause.flag      # resume
```

- A paused run still holds both lock sets, so for the length of the pause a second run of the
  same job is refused as already running, and so is any run whose tables overlap.
- A first `Ctrl-C` or `SIGTERM` ends the pause and stops the run; a second signal cancels at
  once.

**A second input: the control socket.** With [runtime control](#runtime-control) enabled, `pause` over the
socket holds the run at the same places, and `resume` ends it; the sentinel file and the socket pause are two
inputs to one wait, and the run waits while either holds. `pause` takes effect at the next boundary and the
batch in flight finishes first. `resume` with the sentinel file still present clears the socket pause and
says so: the run stays paused, and its log says `Pause continues: control socket pause cleared, but sentinel
file … is still present`. A socket pause lives in the process: it ends with the run, and a rerun starts
unpaused, while the sentinel file persists across runs. The pause lines in the log and the `[PAUSED: …]`
reason name the inputs that hold, and `status` shows how long a pause has lasted.

---

## Replication gating

With `replication.enabled: true` ([options](README_CONFIGURATION.md#replication)), GoArchive
checks every configured replica **immediately before every delete chunk**, recovery's included
(after that chunk's `delete_sleep_seconds` pause, so the check is the last step before the
statement), and before a purge batch logs its roots, and **holds the job** while any of them is
unhealthy. The same run
continues once the whole fleet is healthy again: a hold is a pause, not a failure. Copy-only
never deletes, so it is never gated; throttle it with `sleep_seconds` or pause it with the
[sentinel](#pausing-a-run-sentinel_file).

A replica is unhealthy when it is unreachable, when replication is not configured or not
running, when it is further behind than `seconds_behind_source_within`, or when its status
cannot be read, a privilege error included: an account lacking
[`REPLICATION CLIENT`](README_PERMISSIONS.md#monitored-replicas-optional) holds the job rather
than being skipped. By default **every channel counts**; `channels` narrows that
([channel selection](README_CONFIGURATION.md#channel-selection)).

A server listed without a type is an asynchronous replica and needs a replication channel. A Group Replication
member has none of its own that counts, so listed that way it holds the job with `REPLICATION_CONFIGURED`, whose
detail ends with the hint that a Group Replication member is listed with `type: group`. To watch a member, list it
with [`type: group`](README_CONFIGURATION.md#servers) and the two group bounds. `validate`, `dry-run` and a run's
start check the listing ([`REPLICATION_TYPE_CHECK`](README_VALIDATION.md#replication_type_check)).

### Group Replication members

A server listed with `type: group` is judged by what the group says of itself, and not by a lag. Its account makes
two reads on that server, the group's view and the members' counters
([the grants](README_PERMISSIONS.md#monitored-replicas-optional)), and the server is unhealthy, in this order of
reporting, when:

1. **its own row is not `ONLINE`** in its own view, or the view has no row of its own;
2. **any member of its view is not `ONLINE`** (`UNREACHABLE`, `RECOVERING`, `OFFLINE`, `ERROR`);
3. **an applier queue is above `group_applier_queue_within`**, in transactions, on any member of the view;
4. **a certification queue is above `group_certifier_queue_within`**, on any member of the view.

Only when none of these found anything are two more things checked, and either holds the job:

- **Coverage:** every member of the view must have a row in the counters, with its member id and a view id. A member
  without one is `GROUP_STATS_NOT_RUNNING`, because a queue of zero says nothing of it.
- **The source rule:** when the source is an `ONLINE` member of a group, the view of the listed server must contain it.
  A listed member of another group, or one cut off from the source, holds the job with `GROUP_SOURCE_NOT_IN_VIEW`.
  A source that is no member is not judged by this rule. The run tells the gate which it is when its preflight returns,
  and until then a group server holds the job with `GROUP_SOURCE_NOT_TOLD`: no group server is judged without it.

Things to know:

- **All rows of the listed server's view are judged**, not only its own, from the periodic copies it keeps of its peers'
  counters, which can be about a second late. One listed member therefore already covers the whole present view.
- **A member that is stopped** leaves its peers' views, so only its own entry holds the job for it: list every member
  whose absence should hold the job. The group's configured size is not known to the gate.
- **A queue equal to its bound passes**, and a bound of `0` tolerates no queue.
- **There is no lag**: `lag_seconds` is `null` for a group server. Its verdict carries the largest applier queue and
  the largest certification queue of its view at the last probe instead, and `null` for both before the first probe and
  after a failed read. The tolerance of a round does not apply to it.
- **A failed read** is classified as for a replica: `unreachable` for a connection that is gone, `unreadable` for
  anything else, a missing `SELECT` on either table included, in which case the line names the table and the grant.
  A server that is down when the run starts fails the connection phase, as every monitored server does; one that goes
  down during the run holds the job as `unreachable`.
- **The two bounds are configuration**, not settings: over the [control socket](#runtime-control) they are unknown keys,
  and a change of the tolerance or the interval does not move them.

A hold line has the shape of every other, with the finding as its detail:

```
replication hold: server gr2.internal:3306 unhealthy (GROUP_REPLICATION_APPLIER_QUEUE_WITHIN: Group member "…" reports an applier queue of 4210, above the bound 1000); job held, retrying in 5s
```

`status` says the same in the server's line, with the queues in place of the lag and the tolerance:
`gr2.internal:3306 unhealthy (…), group, applier queue 4210, certifier queue 0, checked 3s ago (…)`.

### What you see while it holds

One `WARN` line per unhealthy server per check, with the reason, and from the second check on,
the time held so far:

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

### What can change while it runs

With [runtime control](#runtime-control), `seconds_behind_source_within` and `check_interval` can be
changed while the run goes, and a change applies to every `DELETE` whose gate decision has not yet been
made. A probe round judges every server by the tolerance it loaded when it began, and a pass is valid only
for the settings it was judged under: a round that passes while the tolerance is lowered is discarded and
judged again at once, so a delete is never let through on a verdict made under settings that have since been
replaced. The settings are read once more as the very last step of a pass, after the line that says the job
is resuming has been written and after a cached pass has been looked up, so a change made while those were
under way is not missed. What the log shows depends on when the change is found:

- **A round that passes under settings replaced while it ran** is discarded without a line. A server that
  looked recovered in it is not logged as recovered: that line waits for a round that stands. The call
  judges again at once, and a server that still fails continues its hold from where it began.
- **A failing round whose wait a gate setting cuts short** says so: `replication gate settings changed: the
  fleet is judged again at once, not in 5s`, since the hold line before it had said `retrying in 5s`.
- **A change that arrives while a pass's own entries are being written** finds the "recovered" and "job
  resuming" lines already out. They stay, and one more line says that the pass above does not stand
  (`replication gate settings changed while the pass was being written: …`). The call's hold, and so the
  rules for a stop, go on from where the call began to hold; the per-server hold entries that those
  "recovered" lines ended are gone, so a server that still fails at the new settings starts a new one.

A tolerance raised above its configured value goes
one step at a time ([the step rule](#the-tolerances-step-rule-and-the-warning)). `cache_ttl` is startup
configuration only: over the socket it is an unknown key.

### `cache_ttl` and the bounded detection delay

A **passing** verdict is cached for `cache_ttl` seconds, so a fast loop does not re-query healthy
replicas before every chunk; failures are never cached. For up to `cache_ttl` seconds after a
pass, a replica that has just become unhealthy is not noticed. `cache_ttl: 0` checks every time.

---

## Group Replication

Every preflight, `validate` and `dry-run` included, starts by reading the Group Replication facts of
both servers. This section says what is read, in which order, what `validate`, `dry-run` and a run
print from it, which groups are supported, and the one grant it asks for. A pair of servers without Group
Replication reads the same facts, finds no group and prints nothing.

### What is read

In this order, before preflight reads any table or catalog, and whatever is skipped
(`--skip-dangerously-validate-preflight-allow-experimental` skips checks, never these reads):

1. **Each side's configuration:** `read_only`, `super_read_only` and `server_id`, the source's first and
   then the destination's. Each is the first statement preflight sends to that server, and each is
   judged as it is read: a source that is read-only is refused by
   [`SOURCE_READ_ONLY_CHECK`](README_VALIDATION.md#source_read_only_check) (an archive and a purge),
   and a destination with `super_read_only` on by
   [`DEST_READ_ONLY_CHECK`](README_VALIDATION.md#dest_read_only_check) (every job type). No flag skips
   either, and neither waits for the reads that follow.
2. **Each side's plugin state:** whether the Group Replication plugin is `ACTIVE`, the group name it
   is configured with, its `group_replication_transaction_size_limit` and its
   `group_replication_single_primary_mode`. The mode is judged right away by
   [`GROUP_MULTI_PRIMARY_CHECK`](README_VALIDATION.md#group_multi_primary_check), on every server whose plugin is
   `ACTIVE` with a group name (see [Single-primary groups](#single-primary-groups-are-supported)).
3. **The source's view of its group**, only when its plugin is `ACTIVE` and a group name is set:
   each member with its state and role, and which row is the source's own. A server that is not
   configured for a group is never asked for it.

A read that fails stops preflight with the error, as any failed inspection does. The order matters:
a newly elected primary answers the first read and can hold the second
([a failover's backlog](#a-failovers-backlog)), so the read-only refusals come before it. Only the
third read needs a grant: `SELECT` on `performance_schema.replication_group_members`
([Permissions](README_PERMISSIONS.md#source)). Without it the job is refused with
[`SOURCE_GROUP_VIEW_PERMISSION_CHECK`](README_VALIDATION.md#source_group_view_permission_check),
exit 1, before anything is locked, copied or replayed.

### What you see

`validate`, `dry-run` and a run print one block when the source or the destination is configured
for a group, and nothing otherwise. `validate` prints it under `PASSED`, `dry-run` ahead of the
statement caps, and a run logs it once, as one entry:

```text
Group Replication:
  source:      member of group 8a94f357-…, ONLINE, PRIMARY, mode single-primary; transaction size limit 150000000 bytes
  destination: Group Replication plugin not active
  sizes:       batch_size 1000, batch_delete_size 500 (each transaction must fit the limit of its side)
```

- **`source`** says `member of group …` with the state and role the group reports, when the
  source's own row is `ONLINE`. A source whose plugin is `ACTIVE` with a name but that is not an
  `ONLINE` member reads `configured for group …, not a running member (state OFFLINE)`, or
  `(no row of its own in its view)`. A source without the plugin reads `Group Replication plugin
  not active`.
- **The mode word** is the server's `group_replication_single_primary_mode`: `mode single-primary` or
  `mode multi-primary` on a source whose own row is `ONLINE`, where it is the group's mode; `configured
  single-primary` or `configured multi-primary` on a source that is not a running member and on the
  destination, whose view is not read, where it is the server's own setting; and `mode unknown` where the
  server returned no value or a text that is neither `ON` nor `OFF`. A run is refused for every value but
  `ON` unless `--allow-multi-primary` accepts it.
- **`destination`** has no state or role: its view is not read. A destination configured for a group
  reads `configured for group …`, then its mode.
- **The limit** is the server's `group_replication_transaction_size_limit`, as the server reports it.
  A transaction larger than it fails at its commit (error 3100), so each batch's copy transaction and
  each delete chunk must fit the limit of the server they run on; `sizes` is the job's own
  `batch_size` and `batch_delete_size`. `0` is the server's own "no limit" for this variable, and
  `unknown` means the server did not report it.

### Single-primary groups are supported

3.1 supports Group Replication groups in single-primary mode, as the source or as the destination. A group in
multi-primary mode is refused at the start of every job type, before any table is read, because GoArchive
serializes runs with advisory locks and an advisory lock is local to one member: two runs connected to different
members of a multi-primary group are not serialized. The refusal is
[`GROUP_MULTI_PRIMARY_CHECK`](README_VALIDATION.md#group_multi_primary_check). It judges the server's own
`group_replication_single_primary_mode` and never a count of primaries, so a lone member that is still `ONLINE`
while the others are down, or a peer that is recovering, does not make a multi-primary group look
single-primary. A mode that cannot be read, or that is neither `ON` nor `OFF`, is refused as well, naming the text.

`--allow-multi-primary` accepts the refusal, on the command line only, and prints a banner that names the case
(the banners under [Command flags](#command-flags)). It does not make the job safe: multi-primary mode has restrictions of its own,
cascading foreign keys among them, that GoArchive does not test. The mode is read once, at the start; a mode
switched during a run is not detected, and a peer that returns while a run is going changes nothing the run
reports.

### A failover's backlog

After a failover, the new primary is `PRIMARY` in the group's view but stays read-only
(`super_read_only` on) until it has applied the transactions the old primary had committed. The MySQL
manual does not describe that window; it was measured on Oracle MySQL 8.0.46, 8.4.13 and 9.7.4, after a
planned switch and after the primary was killed ([the measurement](README_COMPAT.md#a-newly-elected-primary-is-read-only-until-it-has-applied-its-backlog)).
A GoArchive job sees it in four ways:

- **An archive or a purge whose source is that server is refused at once**, by
  [`SOURCE_READ_ONLY_CHECK`](README_VALIDATION.md#source_read_only_check), before anything is copied or
  replayed, and the run is not held. A job whose log holds roots as `copied` or `deleting` keeps them
  exactly as they are: recovery has not run, no root is replayed or marked `completed`, and no row is
  deleted. The same refusal comes under `--skip-dangerously-validate-preflight-allow-experimental`.
  Run the job again when the primary is writable, or point it at the new primary once it is, and the
  run recovers what the log holds.
- **Any job whose destination is that server is refused at once**, by
  [`DEST_READ_ONLY_CHECK`](README_VALIDATION.md#dest_read_only_check), whatever its type: it would
  fail at its first write, and a server that fences the session would hold it before that.
- **A copy-only run on that server is not refused**, because it only reads. Whether it waits depends on
  the server's `group_replication_consistency` for GoArchive's session. With
  `BEFORE_ON_PRIMARY_FAILOVER` or a stronger value (the default on 8.4 and 9.7; `EVENTUAL` is the 8.0
  default, and a cluster built by the AdminAPI persists the series' own), the server holds the
  session's statements on a newly elected primary until the backlog is applied, and the run goes on
  by itself when it is. With `EVENTUAL` the run reads at once, and its reads can lag the group until
  the backlog is applied.
- **A run the server is holding shows nothing new.** The log has `Run starting` and, with the control
  socket on, the `Control socket` line. `status` reads `starting, batch 0` with `activity: none` and
  `hold: none`, or its last state if the run was already going. A statement that takes much longer than
  usual after a failover is the server holding it, not GoArchive waiting for a replica: a
  [replication hold](#replication-gating) is a different thing, and says so in the log and in `status`.

**What ends a held statement.** The backlog being applied, or the session's `wait_timeout`, whichever
comes first. GoArchive pins the `wait_timeout` of its pool sessions to the larger of the server's global value and 28800
seconds, so no server setting makes a held run give up in under eight hours (`max_execution_time`
and `innodb_lock_wait_timeout` were measured not to end it). When `wait_timeout` passes first, MySQL ends the
statement with error 3781 (for a session whose `group_replication_consistency` is `BEFORE_ON_PRIMARY_FAILOVER`,
the 8.4 and 9.7 default, or `AFTER`), and the run stops with exit code 1 and this message, in the run's diagnostic, in its
closing log entry (the statement's own error stays in the entry's `cause` field) and, under `validate`,
on the job's `FAILED:` line:

```text
the server held a statement of this run while a newly elected Group Replication primary applied its
backlog, and the session's wait_timeout passed first (MySQL error 3781). The statement did not run. Run
again when the primary is writable; the next run recovers what the log holds.
```

**A session set to `BEFORE` or `BEFORE_AND_AFTER`** is held the same way, and the server ends its statement
with error 3797 instead of 3781 ("Error while waiting for group transactions commit on
`group_replication_consistency= 'BEFORE'`"). GoArchive never sets either value: it is the server's or the
account's. The wait is not only a failover's: under `BEFORE`, a read on a secondary whose applier is behind waits
for it to catch up and ends the same way. A hold can begin at the start of a transaction, before any table is read.
The run stops with exit code 1 and its own message, in the same three places. It does not say which case it was:

```text
the server made a statement of this run wait until this member had applied the group's earlier transactions,
and the wait ended first (MySQL error 3797). The statement did not run. Sessions whose
group_replication_consistency is BEFORE or BEFORE_AND_AFTER wait this way, on a newly elected primary that is
applying its backlog and on a secondary whose applier is behind, for at most the session's wait_timeout.
GoArchive does not set either value: it is the server's or the account's setting. Run again when the member has
caught up; the next run recovers what the log holds.
```

**Stopping a held run.** The first stop (a `SIGINT` or `SIGTERM`, or the socket's `stop`) is accepted and
logged as `Stop requested`, but it takes effect at the next batch boundary, and a held statement never
reaches one: the statement stays held. What ends it:

- with no stop requested before: **two signals**. The first is logged and changes nothing; the second logs
  `Cancelling the run now`, cancels the statement and ends the run with exit code 1;
- after the control socket's `stop`: **one signal**. A second `stop` answers `already stopping` and changes
  nothing, because no socket command cancels a run; the signal is the second phase.

Either way nothing the run had committed is undone, and the next run's [recovery](#crash-recovery) finishes
what the log holds. Killing the run's connection on the server also ends it at once, with exit code 1.

Stopping a held run ends GoArchive, not the server's session: the server's thread stays in the process list
(`Executing hook on transaction begin.`) until the backlog is applied or the session's `wait_timeout` passes. `KILL`
it to end it sooner.

### A transaction over the size limit

`group_replication_transaction_size_limit` (default 150000000 bytes) bounds the transactions of a Group
Replication server, and a batch's transactions can exceed it: a delete chunk on a source, or the copy
transaction and its log writes on a destination, that is a Group Replication server. MySQL enforces it in two
places and reports two errors ([the measurements](README_COMPAT.md#a-transactions-write-set-is-also-bounded-by-group_replication_transaction_size_limit)).
When the run knew the server's limit at its start (the plugin was `ACTIVE`; [the block above](#what-you-see)
prints it), it ends with an error that names the limit and the setting to lower. A server that did not report a
limit (no plugin, or not `ACTIVE`) leaves the error as MySQL sent it.

**Error 3100, at the commit: the transaction's size.** MySQL reports `Error 3100 … replication hook
'before_commit'`, which does not name the cause: a member that stops or leaves the group during a commit gets
the same error. So the message says what the run saw and no more:

```text
engine: batch 7: engine: delete from order_items: the commit failed with MySQL error 3100 (replication
hook 'before_commit') on the source, which was an ONLINE member of Group Replication group 8a94f357-…
when the run started. Group Replication refuses a transaction larger than
group_replication_transaction_size_limit, which was 150000000 bytes on this server when the run started.
MySQL's error does not say whether that was the reason; the member's error log does. If it was, lower
batch_delete_size (now 500) and rerun. Nothing is lost: the roots stay logged as copied, and the rerun's
recovery finishes them.
```

- **Find the reason in the member's error log.** A transaction over the limit is logged there with its size
  and the limit (`Transaction of size … exceeds specified limit …`). Without that line the failure had another
  cause, and lowering a batch size will not help.
- **A limit of 0** is the server's own "no limit" for the variable. The message then says so as history, offers
  no remedy from a batch size, and points at the error log: the limit may have been changed since.

**Error 3231, at a statement: the transaction's write set.** The same variable bounds a transaction's write set,
which grows with the number of rows the transaction changes and with their keys, not with the size of the rows.
MySQL refuses the statement that takes it over the limit with `Error 3231`, whose text names the cause. A
transaction of a few large rows meets 3100; one of many small rows meets 3231 first. The message states the
cause and the remedy, which is unconditional, a limit of 0 included:

```text
engine: batch 7: engine: delete from order_items: the statement failed with MySQL error 3231 on the source,
which was an ONLINE member of Group Replication group 8a94f357-… when the run started. MySQL refused it
because the transaction's write set exceeded a limit. Group Replication bounds a transaction's write set by
group_replication_transaction_size_limit, which was 65536 bytes on this server when the run started. The write
set grows with the number of rows the transaction changes and with their keys, not with the size of the rows.
Lower batch_delete_size (now 20000) and rerun. Nothing is lost: the roots stay logged as copied, and the
rerun's recovery finishes them.
```

- **How many rows.** Measured with the limit at 4096 bytes: on a table with only a primary key, a transaction
  of 512 rows was accepted and 513 refused, which is 8 bytes of write set a row (Oracle MySQL 8.0.46, 8.4.13
  and 9.7.4). On a table with a unique key or a foreign key besides, 256 rows were accepted and 257 refused, 16
  bytes a row (9.7.4 only). These are figures for those table shapes on those servers, not a formula: at the
  default limit the first shape needs about 18.7 million rows in one transaction, and a limit a DBA lowered to
  1 MiB needs about 131000. The message gives no per-row figure.
- **The setting:** a delete chunk is sized by `batch_delete_size`, and the copy transaction and the log
  writes by `batch_size`. A recovery page is read by `batch_size` too, so a `batch_size` that is too large for
  the destination's limit fails the log write after the deletes of a recovery as well.

**What to do, for both.** Lower the setting the message names, and rerun. A run that has not failed yet can
have either lowered over the control socket ([Runtime control](#runtime-control)); after a failure the rerun
starts with the file's values. GoArchive cannot tell you a safe size: what counts is the rows' images (3100) or
their number and keys (3231).

**Nothing is lost.** The engine rolls the failed transaction back as it does after any statement error, and
never commits it:

- a delete chunk: the roots stay logged (`copied`, or `deleting` for a purge) and the rerun's recovery finishes
  them;
- the copy transaction, at a copy `INSERT`, at its log `INSERT` or at its commit: the batch was rolled back, so
  nothing of it was copied, logged or deleted;
- a purge's log write before its deletes: nothing was logged or deleted;
- the log write that completes a batch after its deletes, in a run or in a recovery page: the rows are deleted
  and the roots stay logged for recovery to complete.

**The values are the start's.** The message reads nothing at the failure: the limit and the membership are what
the run saw when it began, and the limit may have changed since. A destination's view is not read, so the
message never says it was a running member.

## Runtime control

A run can listen on a Unix socket. While it works you can look at it, pause it, stop it, and change how
fast it goes: slow it down when the source is loaded or a replica lags, and bring it back up when the
pressure is gone. Nothing listens unless the configuration says so. The protocol is a public contract
(below), so a script can talk to a run with nothing but a tool that speaks to a Unix socket.

A socket changes only what is run-scoped: a change lasts until the run ends, and the next run starts from
the file again ([how configuration is loaded](README_CONFIGURATION.md#how-configuration-is-loaded)).

### Enabling it and where the socket is

```yaml
control:
  enabled: true
  socket_dir: /home/archiver/.goarchive/run   # optional
```

`control` is off by default, and only the file turns it on: there is no flag and no environment
variable. It can be set per job, and the job's value wins
([options](README_CONFIGURATION.md#control), [per-job overrides](README_CONFIGURATION.md#per-job-overrides-and-precedence)).
`archive`, `purge` and `copy-only` bind the socket; `validate`, `dry-run`, `plan` and `list-jobs` never
bind or create anything.

- **The socket** is `<socket_dir>/<job>.sock`, with its lock `<job>.lock` beside it: one socket per job
  name in a directory. Two configuration files that give the same job name the same directory name one
  socket, and the second run exits 4.
- **The default directory** is `<home>/.goarchive/run`, where `<home>` is the home directory that the
  system's user database records for the user the run runs as, never `$HOME`: cron, a service manager and a
  shell can each carry another `$HOME`, and the place must not depend on how the run was started. A user
  with no entry in that database, or no absolute home in it, has no default: the run is refused before it
  connects to anything, saying so, and `control.socket_dir` is the remedy. The published image's user has
  no entry.
- **The job's name** is used as a file name, so with control enabled it must match
  `[A-Za-z0-9_-][A-Za-z0-9_.-]*`; otherwise the file is refused when it is loaded (exit 2). On Windows a
  name that is a reserved device name is refused too ([Windows](README_CONFIGURATION.md#windows)).
- **The path's length** is limited by the platform to 103 bytes on macOS and 107 on Linux and Windows,
  counted over the whole path. What that leaves for the job's name is `limit − len(socket_dir) − 1 − 5`
  bytes (the separator and `.sock`). A path over the limit is refused when the file is loaded if
  `socket_dir` is set, and at the start of the run for the default directory.

**Where you read the path.** It is never printed on a run's console, which stays as it is: a successful
cron-style run is silent with control enabled.

- Before the run: `validate` prints `control socket: <path>` under each job and `dry-run` in its
  *Settings* section. The default directory depends on the user, so the line says that the path is the
  invoking user's. A job with control off prints `off`; a job whose path cannot be formed prints why, and
  `validate` fails it, as a run would refuse to start it.
- During the run: the log holds one entry, `Control socket: <path>`, at every `level`, in the log's own
  stream and never on the console tee.
- Over the socket: `status` and `status-json` carry it as `socket`.

**Starting.** The socket is bound after the run's log exists and before it connects to MySQL, so `status`
answers during preflight and startup. A socket that cannot be bound fails the run there, naming the path
and the reason; nothing was connected or changed. The failures:

- A live run already serves the socket (its lock is held, or the lock was free and something answered on
  the socket): the run exits **4**, "not started", naming the lock file or the socket.
- Every other failure exits 1: the directory rules below, no default directory for this user, a path over
  the limit under the default directory, something at the socket path that is not a socket, a socket the
  run cannot tell is stale.

A socket left by a run that was killed is replaced by the next start. GoArchive removes a socket only
when its lock is held and a connection to it is refused; it never removes anything that is not a socket,
and at the end of a run it removes only its own.

### The client: `goarchive control`

```text
goarchive control <socket-path> <action>
goarchive control <socket-path> -s|--set <key>=<value>
goarchive control <socket-path> -g|--get <key>
```

```bash
goarchive control /home/archiver/.goarchive/run/orders_archive.sock status-json
goarchive control /home/archiver/.goarchive/run/orders_archive.sock -s batch_delete_size=200
goarchive control /home/archiver/.goarchive/run/orders_archive.sock -g batch_size
docker exec <container> goarchive control /work/control/orders_archive.sock status
```

- **The socket path locates the run, and nothing else does.** It is the first argument: the path that
  `validate` and `dry-run` print and that the run's log records. There is no `-c`, no `-j` and no
  directory flag, and the client reads no configuration file. An explicitly set `-c`/`--config`, before
  or after `control`, is a usage error (exit 2), because a flag that is accepted and ignored would be an
  ambiguity.
- **Three forms, one request per call.** An action is the second argument: `status`, `status-json`,
  `pause`, `resume`, `stop` or `help`. A setter is `-s`/`--set` with `<key>=<value>`, a getter is
  `-g`/`--get` with `<key>`, and a call takes exactly one of the three. The protocol has no multi-key
  request, and several requests for one command line could be accepted in part and refused in part, so a
  change is one call: each is accepted or refused on its own, and logged on its own.
- **The client only spells the line.** An action is sent as written, `-s k=v` as `k=v` and `-g k` as
  `k=?` ([the grammar](#the-protocol-contract)).
- **Names are judged by the run, not by the client.** An action or setting the run does not know, or
  does not use, comes back as the run's `ERR` and exit 1. `goarchive control --help` lists the actions
  and settings from the same table the run's `help` is built from, so a client and a run of different
  versions cannot disagree about which names exist.
- **Usage errors (exit 2)** are found before connecting: no socket path; no action and no flag; more
  than one of an action, `-s` and `-g`; a repeated `-s` or `-g`, in the short or the long form; more than
  two arguments; a second argument containing `=` (use `-s <key>=<value>`); a `-s` value without `=`, or
  with an empty key or value; a `-g` key that is empty or contains `=`; a socket path over the platform's
  limit; a request line over 4096 bytes or with a newline.

**It checks where it is about to connect.** The run's same-user rule proves the client to the run;
nothing in it proves the run to the client, and a client that connected to whatever is at a path would take
another account's `OK` for the run's. So before it connects, on Linux and macOS, it looks, without
following links and without changing anything: the socket's directory must be a real directory (not a
symbolic link), owned by the user the client runs as, with no group or other permission bit (the rule the
run itself starts under), and the path must be a socket, not a symbolic link and not another type. If
either fails, the client connects to nothing, sends nothing, creates and repairs nothing, and exits **8**
with a message naming the path and what it found. A path with nothing at it is not that case: it is "no
socket" (exit 5). On Windows the checks that depend on an owner and a mode are not made.

**What it prints.** It sends the line, reads the reply until the run closes the connection, and:

1. on `OK` prints the body on stdout, without the `OK` line;
2. on `ERR <reason> <message>` prints `Error: <reason>: <message>` on stderr and exits 1, including for
   a reason word it does not know;
3. prints nothing on stdout unless the reply's envelope is whole: a first line that is `OK`, and a last
   byte that is a line feed. A reply that fails either test is never printed in part (exit 7).

The client judges the envelope and nothing inside the body, since the names are the run's. So it cannot
detect a reply cut exactly at a line boundary, and its exit 0 means "a well-formed `OK` reply", not "a
complete snapshot": a script that reads `status-json` parses the body as one JSON object and checks its
`protocol` field. A failure after the connection was made (exit 6 or 9) says nothing about the request: the
run acts on the line before it replies, so a setter may have been applied. Read it back with `-g` or
`status-json`.

Run it as the OS user that runs the job ([Security](#security)). It reads no stdin, loads no file, opens
no MySQL connection, and writes nothing but its output. Its exit codes are 0, 1 and 2 as for a run, and
5 to 9 ([Output and exit codes](#output-and-exit-codes)).

### The protocol contract

The socket speaks **protocol 1**. The version is an integer, reported as the first body line of `help`
(`protocol 1`) and in `status-json` as `"protocol": 1`. It covers everything in this section.

- Adding an action, a setting, an error reason or a `status-json` field keeps the version. A client must
  ignore fields and body lines it does not know.
- Renaming or removing any of them, or changing the grammar or the meaning of a reply, raises it.
- A change to the protocol is a behaviour change, and updates this section in the same change; a test
  compares the tables below with the server's.

**A request** is one line, `token`, `token=number` or `token=?`, ended by a line feed, on one connection;
the server reads up to the first line feed (at most 4096 bytes with it), acts on that line, writes its
reply and closes.

```text
line     = token | token "=" number | token "=?"
token    = lower *( lower | digit | "_" | "-" )
number   = 1*digit [ "." 1*digit ]
```

- A bare token is an action; `token=number` sets a setting; `token=?` reads it. A number has no sign, no
  exponent, no leading or trailing point and no space: `0.5` and `10` are numbers; `.5`, `5.`, `+5`, `-5`,
  `1e3`, `.nan` and `.inf` are not. Nothing is trimmed into validity: a space, a tab, a CR, an uppercase
  letter or an empty line breaks the form.
- **What follows the first line feed is not a request.** It is discarded, never run and never refused.
  No half-close is needed: a client may close its writing side after the line or leave it open, and the
  reply comes either way. A request with no line feed is not a request: `ERR malformed` if the stream
  ends first or 4096 bytes arrive without one, and a connection closed at the deadline with no reply if
  neither happens within it.
- A client sends one line and nothing more. One that sends more may find the connection reset instead of
  closed cleanly, because the server closed with input unread; it must not rely on a clean close.
- **The reply** is `OK` and a body, or one line `ERR <reason> <message>`. Every line ends with a line feed
  and the server closes after the reply, so the end of the stream is the end of the reply. A reply whose
  last byte is not a line feed was cut inside a line; one cut exactly at a line boundary cannot be told
  from a complete one. A script that reads `status-json` parses the body as one JSON object and checks its
  `protocol` field. Replies never contain credentials, DSNs, key values or the job's `where`.
- A setter's body is `key: <old> -> <new>, <when it takes effect>`, or `key: <value> (unchanged)`; a
  getter's body is `key=<value>`, a line that is itself a valid setter. An integer setting is written in
  digits, a sleep as the shortest plain decimal that reads back to the same value, never with an exponent.
- **A failed exchange says nothing about the request.** The server acts on the line before it replies. If
  the connection fails before you have read the reply, you do not know the outcome: the failure means
  neither a refusal nor that a change was undone. Read the value back with a getter or `status-json`.
- Each connection has a 5-second deadline on its reads and writes, at most 8 connections are handled at
  once, and the others wait.

**Actions.**

| Action | Effect |
|---|---|
| `help` | `protocol 1` on the first body line, then the actions and settings this run accepts, one line each |
| `status` | The state as text: a summary line, then labelled lines |
| `status-json` | The same as one JSON object on one line |
| `pause` | Pause before the next batch or recovery page; a batch in flight finishes first |
| `resume` | End a socket pause |
| `stop` | Stop gracefully, as the first `SIGTERM` does |

**Settings.** The names are the configuration keys.

| Setting | Takes | Job types |
|---|---|---|
| `batch_size` | a whole number | all |
| `batch_delete_size` | a whole number | archive and purge |
| `sleep_seconds` | a number of seconds | all |
| `delete_sleep_seconds` | a number of seconds | archive and purge |
| `seconds_behind_source_within` | a whole number of seconds | archive and purge, with `replication.enabled` |
| `check_interval` | a whole number of seconds | archive and purge, with `replication.enabled` |

`help` lists only the settings the run accepts, and a refused setting's getter is refused the same way.
`cache_ttl`, `group_applier_queue_within` and `group_certifier_queue_within` are not settings: over the socket they are unknown keys. Left out on purpose: anything that
takes SQL, a host, a URL or a path (the sentinel file included); `verification`, `--force-triggers`,
foreign-key checks, the preflight bypass; `replication.enabled`, the server list; an abort command.

**Errors.** `ERR` is followed by one reason word, then a message for a person. A script branches on the
word and never parses the message, which may be reworded without a protocol change. The words are a
closed list; a later addition keeps the version, so **a client must treat a well-formed reason it does not
know as a refusal**.

| Reason | Meaning |
|---|---|
| `denied` | The connection did not come from the run's user |
| `malformed` | The request does not have the form: no line feed before the end of input, over 4096 bytes, a character outside the grammar, a value that is not a number |
| `unknown` | The form is met; the action or setting is not one this protocol has |
| `inapplicable` | The setting exists and this run does not use it |
| `ending` | The run is ending; nothing is changed any more |
| `invalid` | The number is not a value this setting can take: a fraction for a whole-number setting, a number too large to hold, or one the configuration file would refuse |
| `ceiling` | A size above the run's starting value |
| `step` | A tolerance raise breaks the step rule; the message says the largest value accepted now, or that the replicas have not yet been checked under the current tolerance |
| `busy` | The run's log is not keeping up; nothing was changed. Try again |

A request that fails in more than one way gets the first word that applies, in the order of this table:
`denied`, `malformed`, `unknown`, `inapplicable`, `ending`, `invalid`, `ceiling` or `step`, `busy`. `status`
and the getters are never `ending`, `busy`, `ceiling` or `step`.

```text
→ batch_delete_size=100
← OK
← batch_delete_size: 500 -> 100, from the next DELETE

→ batch_size=5000
← ERR ceiling batch_size 5000 is above this run's configured 1000; the control socket never raises this setting above the run's configuration: change the file and run the job again
```

**`status-json`** is one object. Times are RFC 3339 in UTC and durations are seconds; what a run does not
have is `null`. `<name>` is a table's or a setting's name. The counters count committed work and never go
down within a run; a new run starts from zero, so a collector treats a changed `pid` or `started_at` as a
reset.

| Field | Content |
|---|---|
| `protocol` | `1` |
| `version` | The GoArchive version |
| `job` | The job's name |
| `job_type` | `archive`, `purge` or `copy-only` |
| `config_file` | The absolute path of the configuration file the run loaded |
| `pid` | The run's process id |
| `socket` | The socket's path |
| `started_at` | When the run began |
| `now` | When this snapshot was taken |
| `elapsed_seconds` | `now` minus `started_at` |
| `state` | One word: `ending`, `stopping`, `held`, `paused`, `recovering`, `running` or `starting`, by that precedence |
| `stop_requested` | A stop was requested, by a signal or over the socket |
| `activity.step` | `fetch`, `discover`, `copy`, `delete`, `log`, `sleep` or `wait`; `activity` is `null` when the run does nothing yet |
| `activity.table` | The table the step works on, where it has one |
| `pause.requested[]` | The inputs that ask for a pause: `socket`, `sentinel` |
| `pause.waiting_since` | When the loop began waiting in the pause gate; `null` when it is not waiting |
| `pause.sentinel_file` | The configured sentinel file, when there is one |
| `hold.since` | When the replication gate began holding the job; `hold` is `null` when it is not |
| `hold.servers[].server` | A server that holds the job, as `host:port` |
| `hold.servers[].healthy` | Its health at its last check |
| `hold.servers[].cause` | `unreachable`, `unreadable` or `unhealthy` |
| `hold.servers[].detail` | The reason as the hold's log line gives it |
| `hold.servers[].type` | `async` or `group` |
| `hold.servers[].lag_seconds` | The largest lag among the gated channels; `null` when a channel reports NULL or the read failed, and always `null` for a group server |
| `hold.servers[].applier_queue` | For a group server, the largest applier queue in its view at the last probe, in transactions; `null` when it was not probed or its read failed, and always `null` for an asynchronous server |
| `hold.servers[].certifier_queue` | The same for the certification queue |
| `hold.servers[].tolerance` | The tolerance it was judged by |
| `hold.servers[].checked_at` | When it was last probed |
| `counters.batches` | Batches completed |
| `counters.roots` | Roots in completed batches |
| `counters.roots_skipped` | Roots a copy-only resume left alone |
| `counters.roots_recovered` | Roots recovery replayed |
| `counters.rows_copied` | Rows committed to the destination |
| `counters.rows_deleted` | Rows deleted from the source and committed, recovery's included |
| `tables.<name>.copied` | Rows committed to the destination, per table |
| `tables.<name>.deleted` | Rows deleted from the source, per table |
| `progress.roots_estimate` | The optimizer's estimate of the roots in range; `progress` is `null` without `--progress` and before the estimate is read |
| `progress.percent` | The share done, capped at 100 |
| `progress.eta` | The time left in seconds; `null` while it cannot be told |
| `settings.<name>.value` | The value in force, for each setting the run accepts |
| `settings.<name>.configured` | The value the run started with |
| `replication.enabled` | `replication.enabled` in the configuration |
| `replication.used` | Whether this run uses the gate |
| `replication.cache_ttl` | The configured `cache_ttl`, which is not a setting |
| `replication.group_applier_queue_within` | The configured bound of a group server's applier queue, which is not a setting; `null` when no `type: group` server is listed |
| `replication.group_certifier_queue_within` | The same for the certification queue |
| `replication.last_pass_at` | When the gate last passed; `null` if it has not |
| `replication.servers[].server` | Each server's last verdict: `host:port` |
| `replication.servers[].healthy` | Healthy or not; `null` until it is first probed |
| `replication.servers[].cause` | `unreachable`, `unreadable` or `unhealthy`; empty when healthy |
| `replication.servers[].detail` | The rendered reason; empty when healthy |
| `replication.servers[].type` | `async` or `group` |
| `replication.servers[].lag_seconds` | The largest lag among the gated channels, or `null`; always `null` for a group server |
| `replication.servers[].applier_queue` | For a group server, the largest applier queue in its view at the last probe, or `null` when it was not probed or its read failed; always `null` for an asynchronous server |
| `replication.servers[].certifier_queue` | The same for the certification queue |
| `replication.servers[].tolerance` | The tolerance the verdict was judged by |
| `replication.servers[].checked_at` | When the server was probed; `null` until it is |
| `changes` | How many runtime changes this run has accepted |

`status` says the same as these fields, for a person: a summary line, then the content in labelled lines (the
build and its protocol, the times, whether a stop was requested, the pause inputs and the sentinel file, the
replication gate and its `cache_ttl` even when the run does not use it, the accepted changes, the roots
skipped and recovered, the estimate and the per-table counts). The JSON is the contract; the text's layout
may change, so a script reads `status-json`.

The gate is synchronous: outside a probe, `status` shows each server's last verdict and its age, not a
live value, and reading it never probes. `status` and `status-json` write no log entry and query nothing;
a collector may poll every second.

**Without the binary.** Two lines send a request with tools that speak to a Unix socket. They are the
lines the E2E test runs, against a run of the real binary, with the OpenBSD `nc` and `socat` 1.8: each
sends one line and returns with the reply, because the run closes the connection after it. Neither needs a
flag for the end of input.

```bash
printf 'status-json\n' | nc -U /home/archiver/.goarchive/run/orders_archive.sock
printf 'batch_delete_size=100\n' | socat - UNIX-CONNECT:/home/archiver/.goarchive/run/orders_archive.sock
```

A script reads the reply to the end and checks it as the client does: the first line is `OK` or one
`ERR <reason> <message>` line, and the last byte is a line feed. An `nc` of another family (a traditional
netcat) was not tried; one that keeps waiting for the end of input is not reading the run's close, and
`socat` or `goarchive control` is the tool to use there.

### When a change applies

| Setting | Read | A change applies |
|---|---|---|
| `batch_size` | Once at the top of each batch and of each recovery page | From the next batch or page. That one value is the fetch limit, the end test and the chunk size of every read, discovery and verification statement of the batch |
| `batch_delete_size` | Immediately before each DELETE is built, after its sleep and its gate decision | From the next DELETE to be issued, the one a hold is holding included |
| `sleep_seconds`, `delete_sleep_seconds` | At the start of each sleep, and again on each change | At once: a sleep in progress ends when the time already slept reaches the new value |
| `seconds_behind_source_within`, `check_interval` | Once per probe round | To every DELETE whose gate decision has not yet been made, one sleeping before its decision included |

Nothing pauses inside a batch: `pause` takes effect at the next boundary and the batch in flight finishes,
and `status` shows `running` with the pause listed as requested until the loop is actually waiting.

**The size ceilings.** `batch_size` and `batch_delete_size` may be lowered, and raised again up to the
value the run started with, never above it; a value above is `ERR ceiling`. The socket slows a run down
when the server is loaded or replication lags, and brings it back; the run's configuration is the ceiling.
The reasons: preflight proved the statement sizes only up to the configured values, so a larger size would
send statements that `MAX_ALLOWED_PACKET_CHECK` never judged; a typo that raises a size is refused, while
one that lowers it makes the run slow, visibly, and one command undoes it; and the starting value is what
`dry-run` showed and what memory was planned for. Growing a run beyond its configuration needs a stop and
a rerun with a changed file. Sleeps and `check_interval` have no ceiling or floor beyond the file's rules
(not negative, finite and small enough to be a duration; `check_interval` positive).

#### The tolerance's step rule and the warning

`seconds_behind_source_within` is lowered freely, and a raise back up to the
configured value is accepted in one command. Above the configured value a raise goes **one step at a time**:
the new value is at most the current value plus one step (the configured tolerance, or 1 when it is 0), and
only after the gate has probed the replicas under the current tolerance, so each step is judged before the
next is accepted. With a configured tolerance of 10 and a replica 42 seconds behind, you send 20, 30, 40 and
50, each after the gate has probed at the previous value; `50` sent first is refused:

```text
Error: step: seconds_behind_source_within 50 is more than one step above the current 10; the largest
value accepted now is 20 (the configured 10 is the step)
```

A raise above the configured value is reported loudly, as the run banners are: one `WARNING:` line on
stderr and a log entry, whatever the log level. The status shows `value` and `configured`, and the closing
output marks a final value above the configuration.

**What the log records.** Every accepted change is one log entry, past the level filter, with the setting,
the old and new values, the revision, the time, and the peer's user id and process id where the platform
gives them; `pause`, `resume` and `stop` are logged the same way. A refused request and a refused peer are
warnings, best effort when the log is not keeping up: an accepted change is never dropped, and when the log
cannot take another entry the next change is refused with `busy` instead. A run that accepted changes ends
with one more line in its closing entry and in the closing block, for each setting changed its configured
value, its final value and its number of changes, and the pauses with their time
([Output and exit codes](#output-and-exit-codes)).

### Security

The socket is protected by its directory and by a check of who connects.

- **The directory is private.** On Linux and macOS a socket directory that exists must be a real directory,
  not a symbolic link, owned by the user the run runs as, with no group or other permission bit. GoArchive
  creates the last element of `socket_dir` (or `.goarchive` and `run` under the home for the default) with
  mode `0700`, and never changes the mode or owner of a directory it did not create: `socket_dir: /tmp` is a
  refused mistake, not one silently repaired. The socket is created inside it with mode `0600`.
- **Same user only.** The run accepts a connection only from its own user: on Linux and macOS the kernel's
  record of the peer's user id must equal the run's, and every other user is refused with `ERR denied`,
  uid 0 included, because the superuser can open a `0600` socket in a `0700` directory. A connection whose
  credentials cannot be read is refused the same way, and the refusal is logged with the user id. A run
  started by uid 0 is no special case: only uid 0 controls it. There is no group mode and no list of
  further operators. Connect as the OS user that runs the job.
- **Use a dedicated, unprivileged OS user for jobs**, as the published image does (uid `65532`), so that
  pausing or tuning a run never needs a privileged account.
- **Windows** is best effort, as for log files. The socket is an AF_UNIX socket (Windows 10 version 1803
  and later); the default directory is `<profile>\.goarchive\run`, where `<profile>` is the profile
  directory Windows records for the account. Not enforced there: the directory's owner and mode, the
  socket's mode, and the same-user rule, since there are no peer credentials; a change is logged without a
  user id. The rules that keep another run's or another program's file from being removed are the same on
  every platform.
- **Do not put `socket_dir` in a directory that is cleaned by age**, or exclude it from the cleaner. The
  run checks once a minute that its directory, its lock and its socket are still the ones it created; if one
  was removed or replaced it logs it loudly, once, and repairs nothing: it does not create a new lock or
  bind a new socket, the run itself continues, and a signal still stops it. A live socket still refuses a
  second start even if its lock file is gone.

### Docker and systemd

**Docker.** The image's user (uid `65532`) has no home directory, so it has no default socket directory:
with control enabled and no `control.socket_dir`, a run in the image is refused, before it connects to
anything, with a message that names `control.socket_dir`. Point it at a directory under `/work`, the
image's working directory, which any user can write:

```yaml
control:
  enabled: true
  socket_dir: /work/control
```

The run creates `/work/control` with mode `0700` for the user the container runs as, and only that user can
use the socket, so use the client from inside the container, as the same user:

```bash
docker exec <container> goarchive control /work/control/orders_archive.sock status
```

The path is `<socket_dir>/<job>.sock`, the one `validate`, `dry-run` and the log show. The container's
log is the place to read it for a running one (`docker logs`, with the default JSON logging).

**systemd.** Let systemd make the socket directory, owned by the service's user and private, and clean it
up when the service stops:

```ini
[Service]
User=archiver
RuntimeDirectory=goarchive
RuntimeDirectoryMode=0700
ExecStart=/usr/local/bin/goarchive archive -c /etc/goarchive/archiver.yaml -j orders_archive
```

with `socket_dir: /run/goarchive` in the file. That directory satisfies the directory rules
([Security](#security)) and is not one a cleaner removes by age. Talk to the run as the service's user:
`sudo -u archiver goarchive control /run/goarchive/orders_archive.sock status`. `systemctl stop` sends
`SIGTERM`, a [graceful stop](#graceful-shutdown), like a `stop` over the socket.

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

A `stop` over the [control socket](#runtime-control) is the first phase, exactly as the first `SIGTERM` is:
the same log line and the same stop, so the same boundary stop and the same immediate end of a replication
hold. `OK` acknowledges the request, not an outcome: the exit code is decided as for a signal (3 if the run
stopped before the end of the `where` range without an error, 0 if the batch in flight was the last short
batch and the run completed, 1 if an error met the stop). A second `stop` says it is already stopping, and no
socket command cancels a run: a real signal after a `stop` is the second phase. `status` answers until the
run ends.

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
