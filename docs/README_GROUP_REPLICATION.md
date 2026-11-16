# Group Replication

**Related:** [Configuration](README_CONFIGURATION.md) · [Operations](README_OPERATIONS.md) ·
[Validation & Preflight](README_VALIDATION.md) · [Permissions](README_PERMISSIONS.md) ·
[MySQL compatibility notes](README_COMPAT.md) · [← Back to README](../README.md)

---

Every preflight, `validate` and `dry-run` included, starts by reading the Group Replication facts of
both servers. This document says what is read, in which order, what `validate`, `dry-run` and a run
print from it, which groups are supported, and the one grant it asks for. A pair of servers without Group
Replication reads the same facts, finds no group and prints nothing.

## What is supported

- **A member as the source or the destination.** Before preflight reads any table, it reads the group facts of
  both servers, and a block prints what they say. The reads are in [What is read](#what-is-read), and the block
  is in [What you see](#what-you-see).
- **Members in the gate.** A server listed with `type: group` is judged by what its group reports, not by a lag
  ([Group Replication members](#group-replication-members)).
- **Read-only servers.** A read-only source is refused for an archive or a purge, and a destination with
  `super_read_only` on is refused for every job type
  ([`SOURCE_READ_ONLY_CHECK`](README_VALIDATION.md#source_read_only_check),
  [`DEST_READ_ONLY_CHECK`](README_VALIDATION.md#dest_read_only_check)).
- **Single-primary groups.** A group in single-primary mode is supported, as the source or as the destination. A
  group in multi-primary mode is refused unless `--allow-multi-primary` accepts it, as described in
  [Single-primary groups are supported](#single-primary-groups-are-supported).
- **Named messages.** [A failover's backlog](#a-failovers-backlog) and
  [a transaction over the size limit](#a-transaction-over-the-size-limit) each have their own named messages.
- **The grant.** The group reads need one grant on the source, described in [Grants](#grants).

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
(the banners under [Command flags](README_OPERATIONS.md#command-flags)). It does not make the job safe: multi-primary mode has restrictions of its own,
cascading foreign keys among them, that GoArchive does not test. The mode is read once, at the start; a mode
switched during a run is not detected, and a peer that returns while a run is going changes nothing the run
reports.

## Point GoArchive at the members, not at the proxy

Both examples below connect GoArchive to the group's members directly, and the proxy fronts the application only.

- **A `type: group` server is the member itself.** The gate reads that server's own view and counters, and listing
  a member adds that member's own row. Through a proxy, the two reads of a probe may be answered by any member, and
  by two different ones.
- **The source and the destination are the server itself** in both examples: the group's primary. goarchive was
  not tested through ProxySQL or MySQL Router. It relies on state that belongs to one server connection: the
  advisory locks of its lock session ([Concurrency and locking](README_OPERATIONS.md#concurrency-and-locking)) and
  the [session settings](README_CONFIGURATION.md#session-settings) it pins on every connection. Whether a proxy that
  shares server connections between client sessions keeps that state for every statement goarchive sends was not
  measured, so this document does not offer that path.

## Configuring a job for a group

- **The source and the destination.** No key says that a server is a group member. Membership is read from the
  server, and the block under [What you see](#what-you-see) prints what was found.
- **The members in the gate.** Each member is listed with `type: group`, and the two bounds are set in the
  [`replication` block](README_CONFIGURATION.md#replication) and its [`servers[]` table](README_CONFIGURATION.md#servers).
  Each key's rule, default and load-time checks are in its row of those tables.

### Choosing each value

- **Which members to list.** List every member whose absence should hold the job, since a stopped member leaves its
  peers' views. List every member the proxy routes reads to, so that each is judged by its own current row and not
  by a copy up to a second old. One listed member already covers the present view from copies.
- **`group_applier_queue_within`.** Where a proxy has a threshold on the same counter, choose a value below it (the
  [ProxySQL example](#example-a-group-behind-proxysql)), and follow the threshold when it changes
  ([during a run](#following-a-proxys-threshold-during-a-run)). Otherwise choose it from how far behind the application's
  reads may be, in transactions. It counts transactions, not rows or bytes.
- **`group_certifier_queue_within`.** No proxy reads this queue. In measurements on 8.0, 8.4 and 9.7, the
  certification queue of a primary under GoArchive's own load stayed at 0, so a small value holds the job only when
  something else fills the queue.
- **Both bounds, an upper reference.** Where flow control is enabled (`group_replication_flow_control_mode` is
  `QUOTA`, the default; `DISABLED` turns it off), the group's flow control throttles writes once either of the same
  two queues is over its threshold, `group_replication_flow_control_applier_threshold` or
  `group_replication_flow_control_certifier_threshold`, which are 25000 transactions each by default
  ([the manual](https://dev.mysql.com/doc/refman/8.4/en/group-replication-flow-control.html)). A bound far below
  those thresholds is the conservative choice: it asks the job to hold at a much shorter queue than the one at which
  the group throttles. It promises no order: the queue can pass both values between two gate decisions, and other
  writers fill it too. With flow control disabled there is no such reference.
- **`check_interval`.** How soon a hold notices recovery.
- **`cache_ttl`.** How long a passing verdict is reused. After it expires, the next gate decision probes. It schedules
  no reading: the time between two gate decisions (a long statement, a batch's copy phase, a sleep) and the probe's
  own time come on top. Where a proxy acts on its own schedule, a shorter value is the conservative choice, and no
  more than that.
- **`seconds_behind_source_within`.** It judges asynchronous servers only, not a group member.
- **`batch_size` and `batch_delete_size`.** Each transaction must fit the size limit of its side
  ([A transaction over the size limit](#a-transaction-over-the-size-limit)). Their rules are in the
  [`processing` block](README_CONFIGURATION.md#processing).

## What is read
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

Before these, on each new connection, the connector reads the plugin's state and the session's own
`group_replication_consistency` and makes the session a fenced one where the plugin is `ACTIVE`
([`GROUP_CONSISTENCY_CHECK`](README_VALIDATION.md#group_consistency_check); a server without the plugin costs four small
reads there, and is not changed).

A read that fails stops preflight with the error, as any failed inspection does. The order matters:
the read-only refusals come before anything a failover's fence can hold ([a failover's backlog](#a-failovers-backlog)).
The reads of the plugin's state and of the view are of the server's own variables and views, which that fence does not
hold, so they also come before the first table read, and a source that was refused for being read-only can be asked why
(the refusal then names its cause). Only the third read needs a grant: `SELECT` on `performance_schema.replication_group_members`
([Permissions](README_PERMISSIONS.md#source)). Without it the job is refused with
[`SOURCE_GROUP_VIEW_PERMISSION_CHECK`](README_VALIDATION.md#source_group_view_permission_check),
exit 1, before anything is locked, copied or replayed.

## What you see
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

## Group Replication members
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
  after a failed read, with the two bounds it was judged by. The tolerance of a round does not apply to it.
- **A failed read** is classified as for a replica: `unreachable` for a connection that is gone, `unreadable` for
  anything else, a missing `SELECT` on either table included, in which case the line names the table and the grant.
  A server that is down when the run starts fails the connection phase, as every monitored server does; one that goes
  down during the run holds the job as `unreachable`.
- **The two bounds are configuration and runtime settings.** The file's values are where a run starts. Over the
  [control socket](README_RUNTIME_CONTROL.md#runtime-control) both can be changed while the run goes, and a change applies
  from the next gate decision, to every group server at once: a round judges all of them by one pair of bounds, a pass
  cached or reached under an older pair never stands, and a hold ends its wait and judges again at once. They share the
  tolerance's and the interval's gate round, so a change of any of the four moves it. A verdict names the bounds it was
  judged by, which between a change and the next probe are not the values in force. How to change them, and in which
  order, is [Following a proxy's threshold during a run](#following-a-proxys-threshold-during-a-run).

A hold line has the shape of every other, with the finding as its detail:

```
replication hold: server gr2.internal:3306 unhealthy (GROUP_REPLICATION_APPLIER_QUEUE_WITHIN: Group member "…" reports an applier queue of 4210, above the bound 1000); job held, retrying in 5s
```

`status` says the same in the server's line, with the queues in place of the lag and the tolerance, each queue with the
bound it was judged by:
`gr2.internal:3306 unhealthy (…), group, applier queue 4210 (bound 1000), certifier queue 0 (bound 1000), checked 3s ago (…)`.
`status-json` has them as `applier_queue_within` and `certifier_queue_within` on each server, and the bounds in force
under `replication` and `settings`.

## Following a proxy's threshold during a run

Where a proxy's threshold changes while a job runs, the bound is changed with it, over the
[control socket](README_RUNTIME_CONTROL.md#runtime-control), without stopping the job. `group_applier_queue_within` and
`group_certifier_queue_within` are settings of a run that uses the replication gate and lists a `type: group` server;
anywhere else the request is refused as `inapplicable`, with the reason in its message.

- **The order.** To loosen, raise the proxy's value first, then goarchive's bound. To tighten, lower goarchive's bound
  first, then the proxy's. So goarchive's bound is not above the proxy's value in between. It keeps the two numbers in
  that relation and says nothing about which of the two reacts first: the gate reads when the run asks it, as
  [the ProxySQL example](#example-a-group-behind-proxysql) says.
- **The steps.** A lowering is one command, to any value down to `0`, which tolerates no queue, and so is a raise back
  up to the configured value. Above the configured value a raise goes one step at a time: at most the current value
  plus the bound's own configured value, and only after the gate has completed a probe round under the current
  settings ([the step rule](README_RUNTIME_CONTROL.md#the-tolerances-step-rule-and-the-warning)). With a configured bound
  of 80 and a target of 400, you send `160`, `240`, `320` and `400`, each accepted after the gate's next round, each
  with a `WARNING:` line on stderr and in the log. `ERR step` means "not yet" or "too far", and its message says which;
  `400` sent first is too far:

  ```bash
  goarchive control /home/archiver/.goarchive/run/orders_archive.sock -s group_applier_queue_within=400
  # Error: step: group_applier_queue_within 400 is more than one step above the current 80; the largest value
  # accepted now is 160 (the configured 80 is the step)
  goarchive control /home/archiver/.goarchive/run/orders_archive.sock -s group_applier_queue_within=160
  ```
- **One round for the settings of the gate.** The tolerance, the interval and the two bounds share one round. After any
  accepted change of one of them, the next raise above a configured value waits for a probe round completed under the
  new settings, so raising both bounds waits one round between the two commands. During a hold that round runs at once;
  otherwise it is the next delete's gate decision, which can be a while away, and a script retries on `step`.
- **How a change applies.** From the next gate decision, to every delete whose decision has not yet been made. A bound
  below the queue a member has holds the job at that decision, and a raise during a hold is judged at once. The status
  shows the value in force, the configured value, and for each group server the bounds its last probe was judged by.
- **The file.** A change over the socket lasts for the run, and the next run starts from the file, so the file is
  changed too. A bound configured at `0` rises by 1 per round: configure the bound you mean, not `0`. A larger move
  than the steps allow is the file's: stop the run, change the bound, and run again; an archive or a purge goes on from
  its log.

## Example: a group behind ProxySQL

This example is written for ProxySQL 3.0.2 or later, where ProxySQL reads each backend's own counter on every server
version GoArchive supports. The source read covered ProxySQL 3.0.2 to 3.0.12 at each tag, and the tags 3.1.6, 3.1.12,
4.0.6 and 4.0.12. goarchive's tests run no ProxySQL. The numbers are illustrations.

For ProxySQL versions before 3.0.2, which backend reading applies depends on ProxySQL's version test on the
backend's server version:

| ProxySQL, at the tags read | Backend 8.0 or 8.4 | Backend 9.x |
|---|---|---|
| 2.4.0, 2.4.8 | the view | the view |
| 2.5.0, 2.5.5, 2.6.0, 2.6.6, 2.7.0, 2.7.3, 2.7.4, 3.0.1 | the counter | the view |

The tags of 2.5, 2.6 and 2.7 that are not named were not read. "The view" is `sys.gr_member_routing_candidate_status`,
which the operator installs. Its number is what that view computes, and it is not the counter this example's bounds
are compared with.

```yaml
source:
  host: gr1.internal               # the primary itself, not ProxySQL
  # ...
replication:
  enabled: true
  group_applier_queue_within: 80   # ProxySQL's max_transactions_behind is 100
  group_certifier_queue_within: 80 # ProxySQL reads nothing like it
  check_interval: 5
  cache_ttl: 5                     # a passing verdict is reused for at most 5 seconds
  servers:                         # every member ProxySQL routes to
    - {host: gr1.internal, port: 3306, user: monitor, password: "${MONITOR_PASSWORD}", type: group}
    - {host: gr2.internal, port: 3306, user: monitor, password: "${MONITOR_PASSWORD}", type: group}
    - {host: gr3.internal, port: 3306, user: monitor, password: "${MONITOR_PASSWORD}", type: group}
```

What the text beside it says:

1. On the counter reading, the applier bound and ProxySQL's `max_transactions_behind` count the same thing with the
   same comparison. A bound below ProxySQL's value is the conservative choice.
2. The two readers differ in three ways. Whose value a member is judged by: the gate judges a member that is not
   listed by a listed member's copy of its row, up to about a second late, while ProxySQL reads each backend's own
   value. When each reads: the gate reads only when the run asks, before each delete chunk; ProxySQL reads on its own
   schedule, every five seconds by default. How many readings each needs: the gate holds on one reading above the
   bound, and ProxySQL shuns after several lagging checks in a row, three by default. Nothing here says which of the
   two acts first.
3. `cache_ttl` reuses a passing verdict for less time when it is short. It does not make the gate read: the gate
   reads when the run next asks it. ProxySQL's three checks, five seconds apart by default, are how long it watches
   before it acts, and are given as that, not as a time GoArchive stays inside.
4. `group_certifier_queue_within` is set on its own terms, because ProxySQL reads nothing about certification.
5. By default ProxySQL shuns a backend for this in the reader hostgroup only. Where ProxySQL reads its view (the table
   above), it compares the view's number, which may differ, and the view has to be installed.
6. **When ProxySQL's `max_transactions_behind` is changed online, follow it during the run** with
   `goarchive control <socket> -s group_applier_queue_within=<n>`: raise ProxySQL's value first to loosen, lower
   goarchive's bound first to tighten, and climb above the configured 80 in steps of 80, each one accepted after the
   gate's next round. It promises no order of reaction
   ([Following a proxy's threshold during a run](#following-a-proxys-threshold-during-a-run)).

## Example: a group behind MySQL Router

The reading of MySQL Router is from its source in the `mysql/mysql-server` repository, on the branches 8.0, 8.4, 9.7
and trunk (26.10.0), and from the Router and MySQL Shell manuals. goarchive's tests run no Router. The numbers are
illustrations.

```yaml
source:
  host: gr1.internal                # the primary itself, not Router
  # ...
replication:
  enabled: true
  group_applier_queue_within: 100   # from how stale a read the application tolerates
  group_certifier_queue_within: 100
  check_interval: 5
  cache_ttl: 15                     # no proxy timing to stay below
  servers:                          # every member Router routes to
    - {host: gr1.internal, port: 3306, user: monitor, password: "${MONITOR_PASSWORD}", type: group}
    - {host: gr2.internal, port: 3306, user: monitor, password: "${MONITOR_PASSWORD}", type: group}
    - {host: gr3.internal, port: 3306, user: monitor, password: "${MONITOR_PASSWORD}", type: group}
```

What the text beside it says:

1. **Router has no lag threshold.** It routes by a member's state, never by its backlog: an `ONLINE` secondary
   receives reads however long its applier queue is.
2. **So goarchive's applier bound is the only automatic guard** on how far behind a member that serves reads may fall
   on this job's account. It guards this job only: it holds goarchive, it takes no member out of Router's rotation,
   and it does nothing about other writers.
3. **The bound is chosen from the application**: how many transactions behind a read on a secondary may be. The
   upper reference for both bounds is the flow-control bullet under [Choosing each value](#choosing-each-value).
4. **There is no Router value to follow**, so nothing is retuned when Router's configuration changes. The setters
   ([Following a proxy's threshold during a run](#following-a-proxys-threshold-during-a-run)) serve the operator's own
   judgement during a run: the same commands and steps, with no proxy value to move first.
5. **Member state.** Router routes to `ONLINE` members only, and treats `RECOVERING`, `UNREACHABLE`, `OFFLINE` and
   `ERROR` members as unavailable. goarchive's gate holds while a listed server's view shows a member that is not
   `ONLINE`, `RECOVERING` included, or while a listed server's own row is not `ONLINE`. The two act on the same
   states, each from its own reading at its own time, and this document does not say which of the two sees a change
   first. The Router manual describes how often Router refreshes its metadata: every `ttl`, 0.5 seconds by default,
   and, with the group's notifications in use, a bootstrap with `use_gr_notifications` sets `ttl` to 60
   ([Router configuration options](https://dev.mysql.com/doc/mysql-router/26.10/en/mysql-router-conf-options.html)).
   The MySQL Shell manual says that, with notifications on, a change to a tag can take up to 60 seconds for Router to
   detect ([Tagging metadata](https://dev.mysql.com/doc/mysql-shell/8.4/en/admin-api-tagging.html)).
6. **`wait_for_my_writes` is not a lag value.** On the read/write-split port of Router 8.2 and later, it makes a
   session's read wait, for at most its timeout, for that session's own writes. It never waits for this job's
   deletes, and it takes no member out of rotation.
7. **The `_hidden` tag** makes Router skip the member it is set on. The tag is set through
   [MySQL Shell](https://dev.mysql.com/doc/mysql-shell/8.4/en/admin-api-tagging.html).

## A failover's backlog
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
- **A copy-only run on that server is not refused**, because it only reads, and it is warned. When its
  source is the `PRIMARY` of its group and is read-only, the run prints one warning before its first table
  read: if the primary was newly elected, the server holds the run's reads until it has applied its backlog,
  for at most the session's `wait_timeout`, and GoArchive prints nothing meanwhile; and how a held run is stopped
  (below). The warning is not printed on a secondary or a writable primary. GoArchive makes its own sessions
  wait: on a server whose plugin is `ACTIVE`, a session at `EVENTUAL` (the 8.0 default) is set to
  `BEFORE_ON_PRIMARY_FAILOVER` when it connects, so the server holds the session's table reads on a newly elected
  primary until the backlog is applied, and the run goes on by itself when it is, with every row the backlog
  carried ("What the fence does", below).
- **A run the server is holding shows nothing new.** The log has `Run starting` and, with the control
  socket on, the `Control socket` line. `status` reads `starting, batch 0` with `activity: none` and
  `hold: none`, or its last state if the run was already going. A statement that takes much longer than
  usual after a failover is the server holding it, not GoArchive waiting for a replica: a
  [replication hold](README_OPERATIONS.md#replication-gating) is a different thing, and says so in the log and in `status`.

**What the fence does.** GoArchive runs every source and destination connection behind the server's fence, so that
none of its reads or writes of data or of the tracking tables is served by a newly elected primary that is still
applying its backlog. On each new connection to a server whose plugin is `ACTIVE`, before the pool hands it out and
so before any read can use it (a replacement opened in the middle of a run included), it asks for the session's own
`group_replication_consistency`: a session at `EVENTUAL` is set to `BEFORE_ON_PRIMARY_FAILOVER` and read back, and
the four stronger values (`BEFORE_ON_PRIMARY_FAILOVER`, `BEFORE`, `AFTER`, `BEFORE_AND_AFTER`) are kept as they are;
a value it does not know, or one it cannot establish, refuses the connection
([`GROUP_CONSISTENCY_CHECK`](README_VALIDATION.md#group_consistency_check)). The `SET` is one statement on the new
connection and needs no grant beyond the documented ones. A server whose plugin is not `ACTIVE` is left as it is.
What the fence does not do: a copy-only run that reads an ordinary lagging secondary is not made current by it. That
run reads what the secondary has applied, which is what reading a replica has always meant, because
`BEFORE_ON_PRIMARY_FAILOVER` adds no barrier on a secondary. An operator's own `BEFORE` or `BEFORE_AND_AFTER`, which
GoArchive keeps, makes a read wait on a secondary too, under that value's own rules. And a primary switched away
during a run is not covered by any rule: the run fails at its next write to the source with MySQL's read-only error.

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

Either way nothing the run had committed is undone, and the next run's [recovery](README_OPERATIONS.md#crash-recovery) finishes
what the log holds. Killing the run's connection on the server also ends it at once, with exit code 1.

Stopping a held run ends GoArchive, not the server's session: the server's thread stays in the process list
(`Executing hook on transaction begin.`) until the backlog is applied or the session's `wait_timeout` passes. `KILL`
it to end it sooner. Measured on MySQL 8.4.13, the thread keeps its statement: **a write outside a transaction that
GoArchive had cancelled, or whose client had gone, is applied when the hold ends**, and the thread keeps the session's
advisory locks until then. GoArchive's own work is all inside transactions, which are rolled back; the writes outside
one are the registry's, and the last of them is the job's final status. If a stop reaches the final status write while
it is held, the write is cancelled after the 5 seconds a cleanup statement gets, the run ends, its thread keeps the
job's lock, and **a run of the same job started within the lock session's 300-second `wait_timeout`, unless the
backlog is applied first, finds the lock held** and is refused as for any job that is running. The status the run
meant to write is written when the hold ends.

## A transaction over the size limit
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
have either lowered over the control socket ([Runtime control](README_RUNTIME_CONTROL.md#runtime-control)); after a failure the rerun
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

## Grants

- **The source**, when it is a member: the account reads the group's view. The recipe is in
  [Source](README_PERMISSIONS.md#source).
- **Each monitored `type: group` server**: the account reads the group's view and the members' counters, in place of
  `REPLICATION CLIENT`. The recipe is in [Monitored replicas (optional)](README_PERMISSIONS.md#monitored-replicas-optional).
