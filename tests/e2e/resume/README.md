# resume — interrupt a live run, then finish it

These tests run the binary **twice** and prove the actual interruption and recovery
path through the CLI. The engine's integration tests kill a child process at named
points too (`internal/engine`, `TestMySQLKillRecovery`); these run the shipped
binary on Sakila, with a real signal and a fresh process.

| Test | Interrupt | Leaves behind | Resume path |
|---|---|---|---|
| 08 | one `SIGTERM` after batch 1 | batch 1's 100 roots `completed`, **zero** non-terminal roots, `job_status=0` | nothing to replay; run 2 continues the where range |
| 09 | `SIGKILL` inside batch 1's delete phase | its 100 roots at `copied`, `job_status=1` | startup recovery replays them **delete-only**, then the batch loop continues |

3.0 keeps no cursor. After an archive, the roots a run finished are gone from
the source, so the where range itself carries the progress; the log carries what a
crash left unfinished.

## Why two tests and not one

A single `SIGTERM` is **cooperative**. The run finishes the batch in flight and stops
at the next batch boundary, so a graceful stop leaves **nothing** for recovery to
replay. Recovery's replay is reachable only by killing the process outright.

So the two tests do not differ in degree; they exercise different code. Asking one
test for both sets of preconditions cannot pass.

## The hazard: this test is vacuous on *both* sides of its window

After a successful resume the database looks exactly like it does after an
uninterrupted run — all rows moved, no duplicates, source drained. The obvious
assertions therefore cannot tell *"resumed correctly"* from *"never actually
interrupted"*:

| Interrupt lands | Re-run does | End-state assertions |
|---|---|---|
| too early — 0 rows copied | the whole job from scratch | **all pass** |
| in the window | a genuine resume | pass — the real test |
| too late — already finished | nothing | **all pass** |

It is also the only test here whose vacuity can arrive **later, with nobody editing
it**: a faster machine, a changed `batch_size`, and the interrupt slides out of the
window while the test goes on passing.

Four things hold it shut, all in `../lib/interrupt.sh` and `../lib/engine.sh`:

**The interrupt point is asserted exactly**, not as a range. `interrupt_expect_dest`
must match the destination row count at the interrupt — `0` means nothing was copied,
more means the interrupt landed late and `min_duration` no longer describes run 2.
A failure there is an ERROR, not a pass.

**The log at the interrupt point.** After 08's stop, exactly 100 roots are
`completed` and none is `copied` or `deleting`, and run 1's report says it stopped
before the end of the where range. After 09's kill, exactly 100 roots are `copied`,
none is `deleting`, and the job is still `Running(1)`.

**The discriminator.** The engine's `"Recovery: replaying <n> copied roots of
payment"` line must be **absent** from 08's second run and **present** in 09's, with
`n` = 100, and 09's report must count them under `recovered:`. It is the only
assertion that catches 08 quietly becoming a crash test or 09 quietly becoming a
graceful one — because it asserts the path, which no end-state check can reach.
Run 2's `copied:` count must be the slice minus the 100 already copied: a re-copy of
the replayed roots would verify cleanly and change no total.

**`ensure_destination_schema` runs once, before run 1, and never between the runs.**
It drops and recreates the destination database, and `destination.job_schema`
defaults to that same database — so calling it between runs deletes the job, its
log and the copied rows. Run 2 would start from scratch and every end-state
assertion would still pass. This is the easiest possible way to make these tests
vacuous.

## How the interrupt is placed

| Test | Waits for | Why that point |
|---|---|---|
| 08 | the engine's `"Batch N:"` log line | logged at the end of batch N; the loop then sleeps `sleep_seconds` before its stop check, the signal lands in that sleep, and the run stops before fetching batch N+1 — exactly N batches |
| 09 | the log table holding N batches' roots, the last batch's at `copied` | the engine logs nothing between a batch's destination commit and its deletes, so the harness reads the log table itself, one `mysqlsh` call (~0.3 s) per poll, inside a window of 4 × 1 s |

08 polls the run's **own log**, not the database: the marker-to-signal latency is
milliseconds against a 0.2 s sleep. 09 cannot, and its wide delete throttle is what
makes the database poll safe.

`sentinel_file` is not used: creating a file faces the same deadline as sending a
signal, so it buys no determinism, and a leftover file pauses the next run.

## Two things that look like blockers and are not

**No wait is needed between the runs.** A killed process closes its MySQL sessions,
so its advisory locks are released at once and run 2 takes them.

**A stale `job_status=1` does not block the re-run.** The advisory lock, not the
status column, says whether a job is running.

## There is no "completed" job status

`archiver_job.job_status` is `Idle(0)` / `Running(1)` / `Failed(3)`. A finished job
returns to **Idle(0)** — the same value a job that never ran carries — so "the job
reached a terminal state" asserts almost nothing on its own.

STEP 6 asserts the **log table** instead: every root at `completed`, none at
`copied` or `deleting`. That is the assertion `job_status` cannot make. `job_status`
earns its place only as 09's crash signature, where `Running(1)` proves the final
status was never written.

## If one of these goes flaky

**Do not nudge the timing, widen a tolerance, or lower the floor.** Every one of
those moves the test toward a silent-pass region and produces something that never
interrupts and always passes. Mark it skipped with a stated reason and raise it — a
resume test that passes without resuming is worse than no resume test, because it
also removes the pressure to write a real one.

Both windows are **config values** rather than races (`sleep_seconds` for 08, the
delete throttle for 09), so flakiness here is a finding about the window.

See `../README.md` for the file format and the shared per-test contract.
