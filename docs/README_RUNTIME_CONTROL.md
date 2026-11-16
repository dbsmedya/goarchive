# Runtime control

**Related:** [Configuration](README_CONFIGURATION.md) · [Operations](README_OPERATIONS.md) ·
[Group Replication](README_GROUP_REPLICATION.md) · [Permissions](README_PERMISSIONS.md) ·
[← Back to README](../README.md)

---

A run can listen on a Unix socket. While it works you can look at it, pause it, stop it, and change how
fast it goes: slow it down when the source is loaded or a replica lags, and bring it back up when the
pressure is gone. Nothing listens unless the configuration says so. The protocol is a public contract
(below), so a script can talk to a run with nothing but a tool that speaks to a Unix socket.

A socket changes only what is run-scoped: a change lasts until the run ends, and the next run starts from
the file again ([how configuration is loaded](README_CONFIGURATION.md#how-configuration-is-loaded)).

## Enabling it and where the socket is

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

## The client: `goarchive control`

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
5 to 9 ([Output and exit codes](README_OPERATIONS.md#output-and-exit-codes)).

## The protocol contract

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
| `group_applier_queue_within` | a whole number of transactions | archive and purge, with `replication.enabled` and a `type: group` server |
| `group_certifier_queue_within` | a whole number of transactions | archive and purge, with `replication.enabled` and a `type: group` server |

`help` lists only the settings the run accepts, and a refused setting's getter is refused the same way:
in a run that does not use the gate the answer is `this run does not use the replication gate`, and in one
whose gate lists no server of `type: group`, `this run's gate has no server of type group`. The two bounds
follow `check_interval` in `help` and in the status.
`cache_ttl` is not a setting: over the socket it is an unknown key. Left out on purpose: anything that
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
| `step` | A raise of the tolerance or of a group bound breaks the step rule; the message says the largest value accepted now, or that the replicas (or the group) have not yet been checked under the current settings |
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
| `hold.servers[].applier_queue_within` | For a group server, the bound its last probe was judged by, which can differ from the setting in force; `null` for an asynchronous server and for a server not probed |
| `hold.servers[].certifier_queue_within` | The same for the certification queue |
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
| `settings.<name>.value` | The value in force, for each setting the run accepts; the two group bounds are among them where the gate lists a `type: group` server |
| `settings.<name>.configured` | The value the run started with |
| `replication.enabled` | `replication.enabled` in the configuration |
| `replication.used` | Whether this run uses the gate |
| `replication.cache_ttl` | The configured `cache_ttl`, which is not a setting |
| `replication.group_applier_queue_within` | The value in force of the bound on a group server's applier queue, read from the same snapshot as `settings.group_applier_queue_within.value`; where the run does not use the gate it is the configured value, which nothing can change there. `null` when no `type: group` server is listed |
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
| `replication.servers[].applier_queue_within` | For a group server, the bound its last probe was judged by, which can differ from the setting in force between a change and the next probe; `null` for an asynchronous server and for a server not probed |
| `replication.servers[].certifier_queue_within` | The same for the certification queue |
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

## When a change applies

| Setting | Read | A change applies |
|---|---|---|
| `batch_size` | Once at the top of each batch and of each recovery page | From the next batch or page. That one value is the fetch limit, the end test and the chunk size of every read, discovery and verification statement of the batch |
| `batch_delete_size` | Immediately before each DELETE is built, after its sleep and its gate decision | From the next DELETE to be issued, the one a hold is holding included |
| `sleep_seconds`, `delete_sleep_seconds` | At the start of each sleep, and again on each change | At once: a sleep in progress ends when the time already slept reaches the new value |
| `seconds_behind_source_within`, `check_interval`, `group_applier_queue_within`, `group_certifier_queue_within` | Once per probe round | To every DELETE whose gate decision has not yet been made, one sleeping before its decision included |

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
(not negative, finite and small enough to be a duration; `check_interval` positive). The two group bounds
have no ceiling either (no statement was proved against them): above the configured value they follow
[the step rule](#the-tolerances-step-rule-and-the-warning).

### The tolerance's step rule and the warning

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

**The two group bounds follow the same rule**, each with its own configured value as the step (1 when that
is 0): `group_applier_queue_within` and `group_certifier_queue_within` are lowered freely, down to 0, which
tolerates no queue, and a raise back up to the configured value is accepted in one command. Above it a raise
is at most the current value plus the bound's own configured value, from a value below the configuration at
most the higher of the configured value and that sum, and only after the gate has probed the group under the
current settings. With a configured applier bound of 1000 and a member's queue at 4210, you send 2000, 3000,
4000 and 5000; `5000` sent first is refused:

```text
Error: step: group_applier_queue_within 5000 is more than one step above the current 1000; the largest
value accepted now is 2000 (the configured 1000 is the step)
```

**The tolerance and the two bounds share one round.** Any accepted change of one of them, the interval
included, is a change of the gate's settings: it discards a pass cached or reached under the old ones, and the
next raise above a configured value, of any of the three, waits for a probe round completed under the new
settings. An operator who raises both bounds waits one round between the two commands: during a hold that
round runs at once after each accepted change; outside a hold it is the next `DELETE`'s gate decision, which
inside a batch's copy phase can be a while away, so a script retries on `step`. A raise that stays at or below
the configured value needs no round. A bound configured at 0 rises by 1 per round: to follow a threshold
upward, configure the bound you mean and not 0.

A raise above the configured value is reported loudly, as the run banners are: one `WARNING:` line on
stderr and a log entry, whatever the log level. The bounds' line names the group:

```text
WARNING: group_applier_queue_within raised to 2000, above the configured 1000, over the control socket (uid 1000): members of the group may fall further behind before deletes hold.
```

The status shows `value` and `configured`, and the closing output marks a final value above the
configuration. A lowering and a restore are logged as any other change and write no `WARNING:`.

**What the log records.** Every accepted change is one log entry, past the level filter, with the setting,
the old and new values, the revision, the time, and the peer's user id and process id where the platform
gives them; `pause`, `resume` and `stop` are logged the same way. A refused request and a refused peer are
warnings, best effort when the log is not keeping up: an accepted change is never dropped, and when the log
cannot take another entry the next change is refused with `busy` instead. A run that accepted changes ends
with one more line in its closing entry and in the closing block, for each setting changed its configured
value, its final value and its number of changes, and the pauses with their time
([Output and exit codes](README_OPERATIONS.md#output-and-exit-codes)).

## Security

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

## Docker and systemd

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
`SIGTERM`, a [graceful stop](README_OPERATIONS.md#graceful-shutdown), like a `stop` over the socket.
