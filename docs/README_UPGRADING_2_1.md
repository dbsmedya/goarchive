# Upgrading to GoArchive 2.1

GoArchive 2.1 replaces the replication lag monitor with a **replication gate** built on
[dbsgomysql](https://github.com/dbsmedya/dbsgomysql)'s `pkg/replication`. It monitors a
**fleet** of replicas instead of one, gates on **every replication channel** instead of one,
and says precisely why it is holding.

The configuration is a **clean break**: the `replica:` block and the two `safety:` lag keys are
**rejected**, not deprecated. A config carrying them fails validation with a message naming the
replacement. Nothing about archiving, copying, deleting, checkpointing, or resuming changes.

**Read this if** your 2.0 config has a `replica:` block or sets `safety.lag_threshold` /
`safety.check_interval`. If you never enabled lag monitoring, the only thing you may need to do
is delete a leftover stub — see [Even a disabled block is rejected](#even-a-disabled-block-is-rejected).

New users do not need this document. [Configuration](README_CONFIGURATION.md#replication) and
[Operations](README_OPERATIONS.md#replication-gating) describe the software as it is.

## The short version

| 2.0 | 2.1 |
|-----|-----|
| `replica:` (one server) | `replication.servers:` (a list — one entry or many) |
| `replica.host` / `port` / `user` / `password` | `replication.servers[].host` / `port` / `user` / `password` |
| `replica.replication_channel: "billing"` | `replication.servers[].channels: ["billing"]` |
| `replica.replication_channel: ""` | `replication.servers[].channels: [""]` — see the warning below |
| `safety.lag_threshold` | `replication.seconds_behind_source_within` |
| `safety.check_interval` | `replication.check_interval` |
| — | `replication.cache_ttl` (new) |
| — | `replication.servers[].tls` / `type` (new) |

**Before (2.0):**

```yaml
replica:
  enabled: true
  host: replica-db.internal
  port: 3306
  user: repl_user
  password: change_me
  replication_channel: ""

safety:
  lag_threshold: 10
  check_interval: 5
  disable_foreign_key_checks: false
```

**After (2.1):**

```yaml
replication:
  enabled: true
  seconds_behind_source_within: 10
  check_interval: 5
  cache_ttl: 15
  servers:
    - host: replica-db.internal
      port: 3306
      user: repl_user
      password: change_me
      channels: [""]        # keeps 2.0's behaviour — read this section

safety:
  disable_foreign_key_checks: false
```

Then run `goarchive validate`. It runs every check and changes nothing.

## The three rejected keys

Each produces a validation error naming its replacement. The run stops before touching any
data.

| Config still containing | Error |
|---|---|
| any `replica:` field | `the replica: block was removed in 2.1 — replication monitoring is now configured by the replication: block; see docs/README_UPGRADING_2_1.md` |
| `safety.lag_threshold` | `lag_threshold was removed in 2.1 — use replication.seconds_behind_source_within; see docs/README_UPGRADING_2_1.md` |
| `safety.check_interval` | `safety.check_interval was removed in 2.1 — use replication.check_interval; see docs/README_UPGRADING_2_1.md` |

### Even a disabled block is rejected

The trigger is **any** non-zero field, including `replica: {enabled: false, port: 3306}`. A
leftover stub fails validation rather than being silently ignored.

This is deliberate. The failure being prevented is the opposite one: a config with
`replica.enabled: true` that 2.1 no longer understands, quietly running with **no replication
monitoring at all** while the operator believes it is protected. Refusing every spelling is what
makes that impossible. Delete the block.

## One change that can widen what you gate on

**2.0 checked a single channel. 2.1 checks every channel by default.**

If your 2.0 config had `replication_channel: ""`, it monitored one channel. Translating it to
`channels: []` — or omitting `channels` — does **not** reproduce that: an empty or absent list
gates on **every channel the server reports**, so a second channel that was previously invisible
can now hold your job.

That is usually what you want, and it is why it is the default. But it is a change, so decide
deliberately:

| You want | Write |
|---|---|
| exactly what 2.0 did (default channel only) | `channels: [""]` |
| every channel on the server | `channels: []`, or omit it |
| specific channels | `channels: ["billing", "reporting"]` |

MySQL's default channel is **named `""` — an empty name, not an absence.** `[""]` is a
one-element list selecting that channel; `[]` is an empty list meaning "no filter". Every name
you list must exist on that server, or the check fails rather than silently matching nothing.

## New in 2.1

**A fleet.** `servers` takes as many replicas as you like. The job is held while *any* of them
is unhealthy and resumes when *all* of them are healthy. Two entries may not share a
`host:port`.

**Why it is holding.** Each hold names a cause: `unreachable` (no answer), `unreadable` (the
server answered with an error — a missing `REPLICATION CLIENT` grant lands here), or
`unhealthy` (replication read fine and fails policy). Unrecognised failures classify as
`unreadable` and hold — an unidentified failure is never treated as health.

**Hold and resume logging.** One `WARN` per unhealthy server per check, carrying the
accumulated hold duration; healthy servers stay silent. One `INFO` when a server recovers, and
one more when the last one does, reporting how long the job was held in total. If the reason
changes while a server is still down, the duration keeps accumulating instead of resetting.

**`cache_ttl` — and the delay it buys.** A *passing* verdict is cached for `cache_ttl` seconds
so a fast batch loop does not re-query healthy replicas every batch. Failures are never cached.
The cost is explicit: for up to `cache_ttl` seconds after a pass, a replica that has just gone
bad is not noticed. Set `cache_ttl: 0` to check every time.

**`tls` and `type`.** Each server takes `tls` (`disable`, `preferred`, `skip-verify`,
`required`; default `preferred`) and `type`, which accepts only `async` in 2.1 — anything else
is rejected rather than assumed.

## What did not change

- The check still runs **before each batch and before each recovery chunk**.
- An unreachable or erroring replica still **holds** the job. 2.0 did this too; 2.1 only says
  why.
- `REPLICATION CLIENT` is still the required grant — but it is needed **on every server in
  `replication.servers`**, granted to the account that entry names. See
  [Permissions](README_PERMISSIONS.md).
- Archiving, copying, deletion, verification, checkpointing, and resume are untouched.

## Two smaller rules worth knowing

**Server entries are validated even when `enabled: false`.** A typo in a disabled block fails
now, rather than the day someone turns it on.

**A host may not contain a newline or carriage return.** The `host:port` string is the server's
identity in logs and its duplicate-detection key, so an embedded newline could split a log line
in two or admit one server under two spellings. The error names the field without echoing the
value.
