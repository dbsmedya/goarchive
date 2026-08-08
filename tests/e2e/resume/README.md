# resume — interrupt a live run, then finish it

The only tests in the suite that run the binary **twice**, and the only ones whose
subject is a **path** rather than an end state.

| Test | Interrupt | Leaves behind | Resume path |
|---|---|---|---|
| 08 | one `SIGTERM` at a batch boundary | checkpoint set, **zero** non-terminal rows | forward scan from `last_processed_root_pk_id` |
| 09 | `SIGKILL` inside the delete phase | checkpoint `NULL`, 100 rows at `copied`, `job_status=1` | `recover()` → `recoverChunks(copied, batchDeleteOnly)` |

## Why two tests and not one

A single `SIGTERM` is **cooperative**. It closes a stop channel the batch loops
check at boundaries, so the in-flight batch always runs to a terminal state and a
graceful stop leaves **nothing** for recovery to replay. `recover()`'s replay is
reachable only by killing the process outright.

So the two tests do not differ in degree; they exercise different code. Asking one
test for both sets of preconditions cannot pass — an earlier version of this plan
did, and could not have.

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

Three things hold it shut, and all three are in `../lib/interrupt.sh`:

**The interrupt point is asserted exactly**, not as a range. `interrupt_expect_dest`
must match the destination row count at the interrupt — `0` means nothing was copied,
more means the interrupt landed late and `min_duration` no longer describes run 2.
A failure there is an ERROR, not a pass.

**The discriminator.** `"Recovering non-terminal PKs from prior run"` must be
**absent** from 08's second run and **present** in 09's. One grep, and it is the only
assertion that catches 08 quietly becoming a crash test or 09 quietly becoming a
graceful one — because it asserts the path, which no end-state check can reach.

**`ensure_destination_schema` runs once, before run 1, and never between the runs.**
It drops and recreates the destination database, and `destination.job_schema`
defaults to that same database — so calling it between runs deletes the checkpoint,
the log table and the copied rows. Run 2 would start from scratch and every
end-state assertion would still pass. This is the easiest possible way to make these
tests vacuous.

## How the interrupt is placed

The harness waits on goarchive's **own log**, not on a row count: every
`get_row_count` spawns `mysqlsh` at ~0.3 s against a ~1 s deadline, while grepping a
file is milliseconds. Measured marker-to-signal latency is 0.004–0.008 s.

| Test | Marker | Why that line |
|---|---|---|
| 08 | `"Processing batch"` with `"batch":N` | logged *after* the loop's stop checks, so batch N has passed them; the batch then completes on its own and the next check breaks the loop |
| 09 | the Nth `"Starting delete phase"` | logged after `MarkBatchCopied` committed and before the first throttle sleep, so the rows are already `copied` |

**Exactly N batches, with no sentinel and no sleep.** Verified at N=1 (100 rows) and
N=3 (300 rows) — N=3 is the one that matters, since it rules out a mechanism that
always stops after the first batch and would have made N=1 a vacuous pass.

`sentinel_file` was tried and dropped: it faces the same deadline (create a file
rather than send a signal), so it bought no determinism, and a leftover file pauses
the next run indefinitely with no diagnostic.

## Two things that look like blockers and are not

**No wait is needed between the runs.** A killed process closes its MySQL session, so
`GET_LOCK` auto-releases; the 60 s heartbeat-staleness threshold is consulted only
when the lock was *not* acquired. Confirmed by probe: `IS_USED_LOCK` is `NULL`
immediately after the kill and run 2 acquires it with no `--force`.

**A stale `job_status=1` does not block the re-run.** `CheckSameRootConcurrency`
filters `job_name != ?`, so a crashed job's own Running row is excluded from its own
concurrency check.

## There is no "completed" job status

`archiver_job.job_status` is `Idle(0)` / `Running(1)` / `Paused(2)` / `Failed(3)`. A
finished job returns to **Idle(0)** — the same value a job that never ran carries —
so "the job reached a terminal state" asserts almost nothing on its own.

STEP 6 asserts the **log table** instead: every root PK at `completed`, none left
replayable. That is the assertion `job_status` cannot make. `job_status` earns its
place only as 09's crash signature, where `Running(1)` proves `cleanup()` never ran.

## If one of these goes flaky

**Do not nudge the timing, widen a tolerance, or lower the floor.** Every one of
those moves the test toward a silent-pass region and produces something that never
interrupts and always passes. Mark it skipped with a stated reason and raise it — a
resume test that passes without resuming is worse than no resume test, because it
also removes the pressure to write a real one.

Both windows are **config values** rather than races (`sleep_seconds` for 08, the
delete throttle for 09), so flakiness here is a finding about the window.

See `../README.md` for the file format and the shared per-test contract.
