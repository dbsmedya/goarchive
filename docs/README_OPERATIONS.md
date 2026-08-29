# Operations Reference

Commands, flags, tuning, pausing, and crash recovery — everything about running
GoArchive day to day.

**Related:** [Configuration](README_CONFIGURATION.md) ·
[Validation & Preflight](README_VALIDATION.md) ·
[Permissions](README_PERMISSIONS.md) · [Limitations](README_LIMITATIONS.md) ·
[← Back to README](../README.md)

---

## Contents

- [Commands](#commands)
- [Flags](#flags)
- [Recommended operator workflow](#recommended-operator-workflow)
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
| `archive` | Full workflow: discover → copy → verify → delete | **yes** |
| `copy-only` | Copy + verify, no deletion | no |
| `purge` | Delete-only, no copying | **yes** |
| `dry-run` | Simulate execution; print WHERE clause and filtered row estimates | no |
| `validate` | Configuration validation + full preflight for every job | no |
| `plan` | Display the dependency graph, copy order, and delete order | no |
| `list-jobs` | List all jobs defined in the configuration | no |
| `version` | Show version information | no |

`archive`, `purge`, and `copy-only` run preflight automatically at startup, before
any tracking state is written.

---

## Flags

### Persistent flags — available on every command

| Flag | Default | Description |
|------|---------|-------------|
| `-c`, `--config <path>` | `archiver.yaml` | Path to the configuration file |
| `--log-level <level>` | — | Override log level (`debug`, `info`, `warn`, `error`) |
| `--log-format <format>` | — | Override log format (`json`, `text`) |
| `--skip-verify` | `false` | Skip data verification after copy |

These four are the **only** global flags. Everything below is registered
per command.

### Command-specific flags

| Command | Flags |
|---------|-------|
| `archive` | `-j`, `--job` · `--force` · `--skip-validate-preflight` · `--force-triggers` · `--progress[=<interval>]` |
| `purge` | `-j`, `--job` · `--force` · `--skip-validate-preflight` · `--force-triggers` · `--progress[=<interval>]` |
| `copy-only` | `-j`, `--job` · `--force` · `--skip-validate-preflight` · `--progress[=<interval>]` |
| `validate` | `-j`, `--job` · `--force-triggers` |
| `dry-run` | `-j`, `--job` |
| `plan` | `-j`, `--job` |
| `list-jobs` | none |
| `version` | none |

| Flag | Meaning |
|------|---------|
| `-j`, `--job <name>` | Select the job to operate on |
| `--force` | Best-effort heartbeat takeover when a prior run's heartbeat is stale. **Not** hard exclusion — see [Concurrency](#concurrency-and-locking). `copy-only` also uses it to confirm bypassing duplicate preflight. |
| `--force-triggers` | Proceed despite source DELETE triggers. Triggers **will fire** during delete. Not offered by `copy-only`, which never deletes. |
| `--skip-validate-preflight` | **DANGEROUS.** Skip all preflight checks. Prints a full-width warning banner. Not offered by `dry-run` or `validate`. |
| `--progress[=<interval>]` | Print periodic progress to stdout for `archive`, `copy-only`, or `purge`. Bare `--progress` uses 30 seconds. |

### Progress display

`archive`, `copy-only`, and `purge` accept `--progress[=<interval>]`. A bare
`--progress` reports every 30 seconds. An attached Go duration such as
`--progress=10s` or `--progress=1m30s` changes the interval; a bare integer is
seconds. The minimum is one second. Because the flag has an optional value,
the value must use `=`: `--progress 10s` is rejected with a hint to use
`--progress=10s`.

Each report is one append-only stdout line:

```text
progress: roots 4000/~12000 (33.3%) remaining=8000 copied_rows=45210 deleted_rows=45210 elapsed=7m10s eta=14m20s
```

`roots`, percent, `remaining`, and ETA all use root rows, the unit advanced by
the batch loop. `copied_rows` and `deleted_rows` are absolute row totals and
include related child-table rows. `archive` prints both row counters;
`copy-only` prints only `copied_rows`; `purge` prints only `deleted_rows`.

The `~` marks the total as an initial estimate. Rows may enter or leave a
time-relative job condition after the initial count, so percent is capped at
100 and remaining is clamped at zero. A naturally completed run always ends
with `remaining=0`, `100.0%`, and `eta=0s`; a graceful stop or fatal error
retains the last completed-batch estimate. The final progress line is emitted
exactly once on every exit after reporting has activated.

On resume, recovery runs first and the estimate then counts from the fetcher's
current checkpoint. Recovered copied/deleted rows seed the displayed row totals
but are not added to the new main-loop root count. Startup, recovery, or count
failures that happen before reporter activation produce no progress line.

When the main-loop sentinel gate pauses a run, ticks show `eta=paused` and add
`[PAUSED: sentinel file "…" present]`; normal ETA rendering resumes after the
file is removed. Sentinel waits during crash recovery remain visible through
the configured logger and are not annotated in the progress line.

> **Processing settings have no CLI flags.** `batch_size`, `batch_delete_size`,
> `sleep_seconds`, `delete_sleep_seconds`, and `sentinel_file` are config-file
> only — one file holds many jobs, and a single CLI value cannot be correct for
> all of them. Set them in the global `processing:` block or a per-job override.

---

## Recommended operator workflow

```bash
# 1. Config + full preflight for every job
goarchive validate -c archiver.yaml

# 2. Preview: WHERE clause, filtered row counts, payload-limit validation
goarchive dry-run -c archiver.yaml -j archive_old_orders

# 3. The real run
goarchive archive -c archiver.yaml -j archive_old_orders
```

Step 2 matters more than it looks. It runs the non-destructive preflight profile,
shows the row counts the run would actually touch — filtered through the real
relation chain, not full-table counts — and validates `batch_size` against the
destination's limits inside a rolled-back transaction. See
[Dry-run payload validation](README_VALIDATION.md#dry-run-payload-validation).

Use `goarchive plan -j <job>` at any point to see the relation tree, copy order,
and delete order without touching either database.

---

## Tuning throughput

### `batch_size` — the universal copy chunk

`batch_size` is not just the root fetch size. GoArchive fetches `batch_size` root
primary keys per batch, discovers their full subgraph, and then copies **every**
table — root and children alike — `batch_size` rows at a time.

Two hard limits constrain it, both checked by `dry-run`:

- **Placeholder limit:** `batch_size × column_count` must stay under **65,535**.
- **`max_allowed_packet`:** a `batch_size`-sized chunk of the widest table must
  fit in the destination's packet limit.

It also drives memory — BFS discovery holds the batch's whole descendant set. On
deep, high-fanout schemas start at `100` and scale up while watching memory; see
[Deep or wide graphs can exhaust memory](README_LIMITATIONS.md#deep-or-wide-graphs-can-exhaust-memory)
for why the accumulator is unbounded.

### `batch_delete_size` — delete statement size

An independent throttle controlling how many rows are removed per `DELETE`
statement. Lower it to reduce replication lag on the destination replica. It does
not affect the copy phase.

### Two independent pacing knobs

They address different pressures. Using the wrong one wastes wall-clock without
fixing anything.

| Knob | Pauses | Use when |
|------|--------|----------|
| `sleep_seconds` | **between batches** — after each `batch_size` batch | General load on source/archive servers is the concern |
| `delete_sleep_seconds` | **between delete chunks** — after each `batch_delete_size` delete, except the last chunk of each table | Replication lag from binlog volume is the bottleneck |

`delete_sleep_seconds` defaults to `0`. Pair a small `batch_delete_size` with a
non-zero `delete_sleep_seconds` when replication lag — not source load — is what
limits you. The pause applies between chunks *within* a table, which is the
high-frequency case.

Both accept fractional seconds. Per-job `processing:` blocks use pointer
semantics: an explicitly set field wins **even when it is `0`**, so a job can
disable a global sleep with `sleep_seconds: 0`. See
[Per-job overrides](README_CONFIGURATION.md#per-job-overrides-and-precedence).

### If `batch_size` is too large

The real run fails fast on the first copy chunk. Already-processed root PKs stay
checkpointed; the interrupted batch's PKs are left in a resumable state and
replay automatically on the next run after you lower `batch_size`.

---

## Pausing a run: `sentinel_file`

An operator pause switch that does not require killing the process.

```yaml
processing:
  sentinel_file: /var/run/goarchive/pause.flag
```

Before each batch, GoArchive checks whether that file exists. **While it is
present, processing pauses**, re-checking once per second. **Remove the file to
resume.**

```bash
touch /var/run/goarchive/pause.flag   # pause — e.g. to relieve a struggling replica
rm /var/run/goarchive/pause.flag      # resume
```

Presence is the only signal; file contents are ignored. Empty (the default)
disables the switch.

Notes:

- Honoured by `archive`, `purge`, and `copy-only`, at the start of every batch —
  **including recovery batches**, checked before each recovery chunk.
- The wait is **interruptible**: `Ctrl-C` or shutdown aborts a paused run
  immediately, leaving the current batch unprocessed and recoverable.
- A very long pause leaves database connections idle. Keep MySQL `wait_timeout`
  comfortably above the expected pause duration, so the advisory lock connection
  and pooled connections are not dropped.

---

## Replication gating

When `replication.enabled: true`, GoArchive checks every configured replica
before each batch **and before each recovery chunk**, and **holds the job** while
any of them is unhealthy. The same invocation resumes once the whole fleet is
healthy again — a hold is a pause, not a failure.

> **`archive` and `purge` gate; `copy-only` does not.** `copy-only` never deletes
> from source, so it is not gated on replication. Throttle it with
> `processing.sleep_seconds` and `delete_sleep_seconds`, or pause it with
> [`sentinel_file`](#pausing-a-run-sentinel_file).
>
> `purge` gained replication gating in 2.1.0
> ([#19](https://github.com/dbsmedya/goarchive/issues/19)). In every earlier
> release it deleted without checking, even with monitoring configured and
> enabled.

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
```

A replica is unhealthy when it is unreachable, when replication is not configured
or not running, or when it is further behind than
`seconds_behind_source_within`. Each monitored account needs
`REPLICATION CLIENT`.

**Every channel counts.** By default the gate reads every replication channel a
server reports and holds if *any* one of them is unhealthy. Narrow that with
`channels` — see [Configuration](README_CONFIGURATION.md#replication). MySQL's
default channel is named `""`, and renders as `<default>` in logs.

### What you see while it holds

One line per unhealthy server per check, at `WARN`, naming the server and the
reason, with the accumulated hold duration:

```
replication hold: server replica1.internal:3306 unhealthy (channel <default>: lag=42s tolerance=10s); job held, retrying in 5s
```

Healthy servers stay silent. When a server recovers you get one `INFO` line for
that server, and once the last one recovers, one more announcing the job is
resuming and how long it was held in total. If the reason changes while a server
is still down — say it goes from lagging to unreachable — the hold duration keeps
accumulating rather than resetting, so the log shows how long the job has really
been waiting.

### `cache_ttl` and the bounded detection delay

A **passing** verdict is cached for `cache_ttl` seconds, so a fast batch loop does
not re-query the same healthy replicas on every batch. Failures are never cached.

The trade-off is explicit: for up to `cache_ttl` seconds after a passing check,
a replica that has just become unhealthy will not be noticed. Set `cache_ttl: 0`
to check every time and remove the delay entirely, at the cost of a status query
per batch.

---

## Crash recovery

GoArchive checkpoints progress so an interrupted run resumes rather than
restarting.

```sql
-- Tracking tables live in job_schema (default = destination database)
SELECT id, job_name, job_status, last_processed_root_pk_id
FROM archiver_job WHERE job_name = 'archive_old_orders';
```

Resume with the identical command — there is no separate resume subcommand:

```bash
goarchive archive -c archiver.yaml --job archive_old_orders
```

### Tracking tables

- **`archiver_job`** — one row per job. Integer `id` primary key, unique
  `job_name`. Holds checkpoint and heartbeat state.
- **`archiver_job_log_<id>`** — one table per job, named by that job's integer
  `id`. Per-root-PK status as a `TINYINT`: `0` pending, `1` copied, `2`
  completed, `3` failed (legacy only).

```sql
SELECT id, job_name FROM <job_schema>.archiver_job;
-- inspect: SELECT log_status, COUNT(*) FROM archiver_job_log_<id> GROUP BY log_status;
```

Full DDL, inspection queries, cleanup rules, and how to clear a crashed job:
[Job Tracking Schema — DBA Maintenance Guide](README_JOBS_SCHEMA.md).

### Checkpoint advancement

Checkpoints advance **only inside the atomic batch-completion transaction**, so a
checkpoint never claims progress that was not committed.

Recovery differs by command, because their source rows behave differently:

- **archive / purge** — recovered source rows are deleted, so the forward scan
  cannot re-fetch them. Recovery does **not** advance the checkpoint per chunk.
- **copy-only** — source rows persist, so the checkpoint must advance per
  ascending chunk or the forward scan would re-fetch recovered roots. It advances
  only for chunks whose maximum PK is **strictly above the job's startup
  checkpoint floor**, so requeuing a legacy row below the floor cannot regress
  the checkpoint.

Graceful shutdown (`SIGTERM`/`SIGINT`) stops at a **chunk boundary**: each started
chunk runs to completion, earlier chunks stay recovered, and the rest keep their
prior status. Re-running resumes safely.

---

## Resume semantics

Recovery is **status-aware** and behaves uniformly across `archive`, `copy-only`,
and `purge` — all three share one batch pipeline.

| Prior status | archive / purge | copy-only |
|--------------|-----------------|-----------|
| `0` pending | full replay — copy, verify, delete | full replay — copy + verify |
| `1` copied | **delete-only** replay (copy already verified) | promoted straight to completed (no re-copy) |
| `2` completed | skipped | skipped |
| `3` failed | **blocks resume** — legacy only | **blocks resume** — legacy only |

Current releases **never write `failed`**. An error aborts the run and leaves
rows in a recoverable status.

### Resume gates

Before replaying anything, four gates run in order. Each refuses with concrete
recovery SQL rather than risking data loss.

**Gate 1 — legacy `failed` rows (all commands).** Rows marked `log_status=3` by a
pre-1.8 release block resume. Such a row below the checkpoint would otherwise be
skipped forever. The error lists the PKs and gives per-PK options: re-queue
(`log_status=0`), or skip permanently by excluding the PK in the job's `where`
clause and clearing the marker (`log_status=2`).

> Editing the status alone does **not** skip a row — the forward scan re-fetches
> any source row above the checkpoint regardless of log status. Exclude it in
> `where` too.

**Gate 2 — count-mode archive.** `archive` with `verification.method: count`
**refuses resume outright** on *any* `copied` or `pending` row: pre-existing
destination rows cannot be proven equal to source by a count. Recover by
switching that job to `verification.method: sha256` (recommended), or by manually
inspecting and clearing the destination rows.

**Gate 3 — strict-INSERT pending rows** (`archive` and `copy-only`). When strict
`INSERT` is forced — by `verification.method: count`, `--skip-verify`, or a
destination secondary unique index — a `pending` row's destination copy may
already be committed, so re-copying would abort on duplicate and the job would
self-block on every resume. GoArchive therefore refuses.

`copied` rows are unaffected: they need no re-copy, so they still resume as
delete-only (archive) or promotion (copy-only).

Recovery options given in the error: delete the destination rows already written
for those pending PKs and re-run; or, if you have confirmed they match source,
mark them `log_status=1` so they resume as delete-only. For `copy-only`, dropping
`--skip-verify` in favour of `verification.method: sha256` restores idempotent
`INSERT IGNORE` replay.

> Because Gate 2 fires first, `archive` under `verification.method: count` never
> reaches Gate 3 — it refuses on any non-terminal row.

**Gate 4 — replay.** `copy-only` replays `copied` and `pending` as one merged,
globally ascending schedule, which keeps the per-chunk checkpoint monotonic.
`archive` and `purge` replay `copied` first (delete-only), then `pending`.

### Choosing a verification method for recoverability

`verification.method: sha256` is the recommended mode for anything you may need
to resume. The full comparison — INSERT strategy, dirty-destination behaviour,
and charset detection — is in
[`verification:`](README_CONFIGURATION.md#verification).

---

## Concurrency and locking

GoArchive runs [sequentially by design](README_LIMITATIONS.md#sequential-by-design).
Two independent mechanisms prevent overlap:

1. **MySQL advisory lock** (`GET_LOCK()`) serializes execution by job name across
   `archive`, `purge`, and `copy-only`.
2. **Heartbeat-aware same-root checks** in `archiver_job` prevent a different job
   name from operating on the same root table concurrently.

A third, transient lock serializes **startup itself**: each run briefly holds a
root-table advisory lock (`goarchive:root:<table>`) on the destination while it
initializes tracking state, and releases it before batch processing begins. A
startup that cannot acquire it within 10 seconds aborts with
`timed out acquiring root-table lock for "<table>" (another startup in progress)`
— retry once the concurrent startup has finished initializing. The lock is
released even when startup is cancelled or refused.

### The lock connection must stay alive

The advisory lock is held on a **dedicated connection**. Keepalive verifies
`IS_USED_LOCK()` against that connection id and **aborts the job if ownership is
lost** — GoArchive will not continue deleting without its lock.

Ensure MySQL `wait_timeout` exceeds the longest expected job duration. A low
timeout or flaky network can correctly fail a job rather than let it run
unlocked.

### `--force` is best-effort

`--force` proceeds past lock contention **only** when the prior holder's heartbeat
is stale, then refreshes the heartbeat so later startups are blocked.

Heartbeats are UTC wall-clock since 2.2, and a tracking schema written by an earlier
release is refused at startup until upgraded — see
[Job Tracking Schema](README_JOBS_SCHEMA.md#tracking-schema-version-marker).

> A stale heartbeat does not prove the old process is dead. It may still hold
> `GET_LOCK()` and still be deleting. **Verify the old process is actually dead
> before forcing.**

`--force` cannot bypass a live heartbeating job, the same-root concurrency check,
or preflight.
