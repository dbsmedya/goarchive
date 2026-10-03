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

`archive`, `purge`, and `copy-only` run preflight automatically at startup
([How preflight works](README_VALIDATION.md#how-preflight-works)).

---

## Flags

### Persistent flags — available on every command

| Flag | Default | Description |
|------|---------|-------------|
| `-c`, `--config <path>` | `archiver.yaml` | Path to the configuration file |
| `--log-level <level>` | — | Override log level (`debug`, `info`, `warn`, `error`) |
| `--log-format <format>` | — | Override log format (`json`, `text`) |
| `--skip-verify` | `false` | Skip copied-data comparison and accept reported conversion/truncation warnings (source originals may be deleted after conversion) |

These four are the **only** global flags. Everything below is registered
per command. Processing settings (`batch_size`, sleeps, `sentinel_file`) have no flags; they
are [config-file only](README_CONFIGURATION.md#how-configuration-is-loaded).

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
| `--force` | Lets `copy-only` proceed past a held lock whose heartbeat is stale, and confirm bypassing its duplicate check; `archive` and `purge` still refuse a held lock — see [`--force`](#--force-only-matters-for-copy-only) |
| `--force-triggers` | Proceed despite source DELETE triggers ([`DELETE_TRIGGER_CHECK`](README_VALIDATION.md#delete_trigger_check)). Triggers **will fire** during delete. |
| `--skip-validate-preflight` | **DANGEROUS.** Skips the preflight checks; connection-time guards still run ([Bypassing preflight](README_VALIDATION.md#bypassing-preflight)). |
| `--progress[=<interval>]` | Periodic progress to stdout — see [Progress display](#progress-display) |

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

Step 2 matters more than it looks: it shows the row counts the run would actually touch,
filtered through the real relation chain, and validates `batch_size` against the destination
([Dry-run payload validation](README_VALIDATION.md#dry-run-payload-validation)).

Use `goarchive plan -j <job>` at any point to see the relation tree, copy order,
and delete order without touching either database.

---

## Tuning throughput

### `batch_size` — the universal copy chunk

`batch_size` is not just the root fetch size. GoArchive fetches `batch_size` root
primary keys per batch, discovers their full subgraph, and then copies **every**
table — root and children alike — in chunks of up to `batch_size` rows.
INSERT subdivision can make individual statements smaller.

It is bounded by MySQL's placeholder limit and `max_allowed_packet`, both checked by
[`dry-run`](README_VALIDATION.md#dry-run-payload-validation). It also drives memory — BFS
discovery holds the batch's whole descendant set
([why it is unbounded](README_LIMITATIONS.md#deep-or-wide-graphs-can-exhaust-memory)). On deep,
high-fanout schemas start at `100` and scale up while watching memory.

### Memory

Go sets no memory limit from cgroups. `GOMEMLIMIT` is a soft limit on the memory the Go
runtime manages, not a hard RSS cap: when GoArchive runs under a container or systemd
`MemoryMax`, set `GOMEMLIMIT` below that limit. CPU quotas are already honoured by default.

### INSERT diagnostics and subdivision

Every successful application-data INSERT is followed immediately by
`SHOW COUNT(*) WARNINGS` on its transaction. Positive counts also require a
complete `SHOW WARNINGS` list. Unknown, forbidden or unproved diagnostics abort
that batch's copy transaction before its copied marker or source deletion.
Earlier completed batches remain completed; a later hash mismatch can leave a
committed destination copy while preserving its source rows.

Every physical connection initializes `sql_notes=1`, and each copy/sample operation reads
`sql_notes`, `sql_mode` and `max_error_count` before its INSERTs and refuses unproved session
facts. GoArchive never repairs these settings on pooled sessions and needs no extra privilege
or config field. DATE, DATETIME and TIMESTAMP payloads are read and copied as SQL text, never
normalized; legacy temporal payloads are copied when the destination accepts them and the
diagnostics and verification policy permit it.

Source and destination SQL modes need not match. To accept legacy payloads, a DBA can
configure the destination's server-level `sql_mode` for new sessions, or an applicable
`init_connect` policy, then reconnect or restart the job. These settings can affect other
applications, so assess the modes your data needs on the destination server. GoArchive has no
per-session SQL-mode option and makes no automatic global changes.

The normal `max_error_count=1024` retention capacity also caps each INSERT's row
count. A 5000-row candidate INSERT with 13 columns becomes
1024/1024/1024/1024/904 rows: five INSERT/count pairs in the same transaction.
Configured fetch chunks and placeholder limits can make it smaller already.
Session facts add one query per copy/sample operation. Capacity zero permits
clean INSERTs, but positive diagnostic counts fail as uninspectable. Configure
retention through the DBA; GoArchive does not increase it. Network RTT and subdivision affect
throughput; twice as many SQL statements does not imply twice the whole-job duration.

`--skip-verify` follows the narrow conversion policy in
[Configuration](README_CONFIGURATION.md#verification), with visible committed-copy
totals. Counts are diagnostics, not numbers of changed rows. Source originals may
be permanently deleted after accepted conversion.

For [eligible temporal keys](README_LIMITATIONS.md#temporal-values-and-identities),
count verification reads and checks raw identities on both sides.
Before any batch DELETE, every nonempty temporal-key table gets a native indexed
COUNT probe on the dedicated source connection used for deletion. All probes
must pass before any DELETE. Integer-only keys incur neither extra identity read.

### `batch_delete_size` — delete statement size

An independent throttle controlling how many rows are removed per `DELETE`
statement. Deletes run on the source, so lower it to reduce binlog volume and replication lag
on the source's replicas. It does not affect the copy phase.

### Two independent pacing knobs

They address different pressures. Using the wrong one wastes wall-clock without
fixing anything.

| Knob | Pauses | Use when |
|------|--------|----------|
| `sleep_seconds` | **between batches** — after each `batch_size` batch | General load on source/archive servers is the concern |
| `delete_sleep_seconds` | **between consecutive delete chunks of a batch, across tables** — every chunk except the batch's first | Replication lag on the source's replicas, from delete binlog volume, is the bottleneck |

`delete_sleep_seconds` defaults to `0`. Pair a small `batch_delete_size` with a
non-zero `delete_sleep_seconds` when replication lag — not source load — is what
limits you. The pause also separates the last chunk of one table from the first
chunk of the next, so a batch whose tables each fit in one chunk is paced too.
Both accept fractional seconds and can be overridden per job
([per-job overrides](README_CONFIGURATION.md#per-job-overrides-and-precedence)).

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
- The wait is **interruptible**: a first `Ctrl-C` or `SIGTERM` ends the pause and stops the
  run at that batch boundary, with nothing left pending; a second signal aborts immediately.

---

## Replication gating

When `replication.enabled: true` ([options](README_CONFIGURATION.md#replication)), GoArchive
checks every configured replica before each batch **and before each recovery chunk**, and
**holds the job** while any of them is unhealthy. The same invocation resumes once the whole
fleet is healthy again — a hold is a pause, not a failure.

> **`archive` and `purge` gate; `copy-only` does not.** `copy-only` never deletes
> from source, so it is not gated on replication. Throttle it with
> `processing.sleep_seconds`, or pause it with
> [`sentinel_file`](#pausing-a-run-sentinel_file).

A replica is unhealthy when it is unreachable, when replication is not configured
or not running, when it is further behind than `seconds_behind_source_within`, or when its
status cannot be read — a privilege error included, so an account lacking
[`REPLICATION CLIENT`](README_PERMISSIONS.md#monitored-replicas-optional) holds the job rather
than being skipped.

**Every channel counts.** By default the gate reads every replication channel a
server reports and holds if *any* one of them is unhealthy; `channels` narrows that
([channel selection](README_CONFIGURATION.md#channel-selection)).

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
restarting. The checkpoint and per-root status live in the
[tracking tables](README_JOBS_SCHEMA.md):

```sql
-- Tracking tables live in job_schema (default = destination database)
SELECT id, job_name, job_status, last_processed_root_pk_id
FROM archiver_job WHERE job_name = 'archive_old_orders';
```

Resume with the identical command — there is no separate resume subcommand:

```bash
goarchive archive -c archiver.yaml --job archive_old_orders
```

### Graceful shutdown

The first `SIGTERM` or `SIGINT` requests a stop: normal processing stops after the batch in
flight completes, and recovery stops after the recovery chunk in flight completes, leaving
nothing non-terminal behind. A second signal aborts in-flight work, which the next run
replays; a third terminates the process.

### Checkpoint advancement

Checkpoints advance **only inside the atomic batch-completion transaction**, so a
checkpoint never claims progress that was not committed.

Recovery differs by command, because their source rows behave differently:

- **archive / purge** — recovered source rows are deleted, so the forward scan
  cannot re-fetch them. Recovery does **not** advance the checkpoint per chunk.
- **copy-only** — source rows persist, so recovery replays `copied` and `pending` rows as one
  merged, globally ascending schedule and advances the checkpoint per chunk, or the forward
  scan would re-fetch recovered roots. It advances only for chunks whose maximum PK is
  **strictly above the job's startup checkpoint floor**, so requeuing a legacy row below the
  floor cannot regress the checkpoint.

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

Before any marker is read, the job row itself is checked: a job name is bound to
the `job_type` **and** the `root_table` that created it, and a mismatch refuses the
run outright — see [`root_table` is sticky](README_JOBS_SCHEMA.md#root_table-is-sticky).

Copied markers from older versions are not retroactively re-certified. Delete-only
replay does no new destination copy/verification or INSERT diagnostic collection;
freshly rediscovered temporal source identities still face key checks and the
pre-delete probes. Pending replay that reaches copying receives the new checks.
Copy-only promotion remains bookkeeping only. Before resuming a potentially
affected old job, inspect remaining source/destination values with raw SQL and
follow the recovery procedure; do not blindly reset or promote markers. These
checks cannot repair values whose originals were already deleted.

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
switching that job to `verification.method: sha256` (recommended for anything you may need to
resume; [comparison](README_CONFIGURATION.md#verification)), or by manually inspecting and
clearing the destination rows.

**Gate 3 — strict-INSERT pending rows** (`archive` and `copy-only`). When strict
`INSERT` is forced — by `verification.method: count`, `--skip-verify`, or a
destination secondary unique index — a `pending` row's destination copy may
already be committed, so re-copying would abort on duplicate and the job would
self-block on every resume. GoArchive therefore refuses. (`archive` under `count` never gets
here: Gate 2 refuses first.)

`copied` rows are unaffected: they need no re-copy, so they still resume as
delete-only (archive) or promotion (copy-only).

Recovery options given in the error: delete the destination rows already written
for those pending PKs and re-run; or, if you have confirmed they match source,
mark them `log_status=1` so they resume as delete-only. For `copy-only`, dropping
`--skip-verify` in favour of `verification.method: sha256` restores idempotent
`INSERT IGNORE` replay.

**Gate 4 — replay.** `copy-only` replays `copied` and `pending` as one merged,
ascending schedule ([above](#checkpoint-advancement)). `archive` and `purge` replay `copied`
first (delete-only), then `pending`.

---

## Concurrency and locking

GoArchive runs [sequentially by design](README_LIMITATIONS.md#sequential-by-design).
Serialization is per job name and per root table, not per destination:

1. **MySQL advisory lock** (`GET_LOCK()`) serializes execution by job name across
   `archive`, `purge`, and `copy-only`.
2. **Heartbeat-aware same-root checks** in `archiver_job` prevent a different job
   name from operating on the same root table concurrently.

Jobs with different names and different root tables can run at the same time against one
destination.

A third, transient lock serializes **startup itself**: each run briefly holds a
root-table advisory lock (`goarchive:root:<table>`) on the destination while it
initializes tracking state, and releases it before batch processing begins. A
startup that cannot acquire it within 10 seconds aborts with
`timed out acquiring root-table lock for "<table>" (another startup in progress)`
— retry once the concurrent startup has finished initializing. The lock is
released even when startup is cancelled or refused.

### Heartbeat and keepalive timing

| Constant | Value |
|----------|-------|
| Heartbeat write interval | 15 seconds |
| Staleness threshold | 60 seconds |
| Consecutive heartbeat failures before abort | 3 |
| Advisory lock keepalive interval | 30 seconds |

`last_heartbeat_at` is a UTC wall-clock; a tracking schema written with an older meaning is
refused at startup until upgraded
([Tracking-schema version marker](README_JOBS_SCHEMA.md#tracking-schema-version-marker)).

### The lock connection must stay alive

The advisory lock is held on a **dedicated connection**. Keepalive verifies
`IS_USED_LOCK()` against that connection id every 30 seconds and **aborts the job if
ownership is lost** — GoArchive will not continue deleting without its lock.

Keep MySQL `wait_timeout` well above 5 minutes, the connection pool's idle limit. The lock
connection is kept busy every 30 seconds on its own, so neither a long job nor a long pause
changes this. GoArchive enforces no minimum: a very low timeout or a flaky network can correctly
fail a job rather than let it run unlocked.

### `--force` only matters for `copy-only`

`archive` and `purge` refuse a held advisory lock with or without `--force`, stale heartbeat
or not: a held `GET_LOCK` cannot be safely taken over by a command that deletes. For
`copy-only`, `--force` proceeds past a held lock whose holder's heartbeat is stale, with a
warning banner.

> A stale heartbeat does not prove the old process is dead. It may still hold
> `GET_LOCK()` and still be deleting. **Verify the old process and its MySQL session are gone
> before retrying or forcing**; the lock releases when that session closes.

`--force` cannot bypass a live heartbeating job, the same-root concurrency check,
or preflight.
