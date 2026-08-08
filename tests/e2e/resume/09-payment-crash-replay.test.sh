# Test 09 — a crash mid-batch, then a status-aware replay.
#
# Run 1 is SIGKILLed inside the delete phase, so nothing cooperates: no cleanup
# hook, no log flush, no checkpoint. Run 2 has to find the abandoned work by reading
# log_status out of archiver_job_log_<id> and replaying it.
#
# The half of the resume story test 08 structurally cannot reach. A SIGTERM leaves
# every row terminal, so recover() has nothing to do; only a real crash leaves
# 'copied' rows behind. What run 2 exercises here is recoverChunks(copied,
# batchDeleteOnly) -- the delete-only replay, which skips the copy because it already
# succeeded and was verified.
#
# It also leaves archiver_job at job_status=1 (Running) with a fresh heartbeat,
# because cleanup() never ran. Nothing else in the suite produces that state, and
# run 2 has to proceed past it: GET_LOCK auto-releases when the killed session
# closes, and CheckSameRootConcurrency excludes the job's own name.

test_name="Test09_PaymentCrashReplay"
test_desc="Working archive: payment SIGKILLed mid-delete, then replayed from non-terminal log rows"
config_file="test09_payment_crash_replay.yaml"
tables="payment"
mode="working"
command="archive"

# Kill during batch 1's delete phase.
#
# The target interval is between MarkBatchCopied committing and CompleteBatch
# running, which is exactly the delete phase -- and the only part of it with any
# duration is the inter-chunk throttle. "Starting delete phase" is logged at the top
# of that phase, after the copy and verify are already durable, so the kill lands
# with the batch's rows at 'copied'.
#
# This is why the config sets delete_sleep_seconds: 1.0 where every other config uses
# 0.2: at 0.2 the whole window is 4 x 200ms and hitting it depends on the machine.
interrupt="crash"
interrupt_after_batches="1"

# 100 = one batch's copy. Asserted exactly, and asserted twice over: the destination
# holds 100 rows AND the log holds 100 at 'copied'. Both derive from batch_size, and
# a mismatch means the kill missed its window.
interrupt_expect_dest="100"

root_pk="payment_id"

# REQUIRED on this path, unlike test 08's. This run leaves non-terminal rows, and
# recover()'s count-mode gate refuses to resume a count-verified archive in that
# state: a pre-existing destination row cannot be proven equal to source by a row
# count. sha256 also selects INSERT IGNORE, which is what makes a replay idempotent.
verify_method="sha256"

# RUN 2's floor, with the recovery term DELIBERATELY EXCLUDED.
#
# Recovery runs before the batch loop and its cost is variable: the kill point
# decides how many of batch 1's 100 rows are still present to delete, and therefore
# how many chunk sleeps it spends -- 100 rows is 5 chunks and 4 sleeps, 80 rows is 4
# chunks and 3, 20 rows is 1 chunk and none. A probe measured 80 remaining. Zero is
# legal, so folding a recovery term into this floor would build a gate a correct run
# can fail, and the tempting fix would be to lower the floor.
#
# The forward scan then covers the remaining 399 rows in 4 batches (100/100/100/99,
# 5 chunks each):
#
#   4 x 0.2           =  0.8   batch sleep
#   4 x 4 x 1.0       = 16.0   delete throttle
#                       ----
#                        16.8
#
# That the recovery HAPPENED is proven by the discriminator instead -- run 2's log
# must contain "Recovering non-terminal PKs from prior run", which test 08's must
# not. Separate jobs; do not conflate them.
min_duration="16.8"

# The full slice, after BOTH runs. 499 rows, measured -- payment_id is not
# contiguous, so <= 500 is not 500. Smaller than test 08's slice because the wide
# delete throttle above costs ~4s per batch.
expected_rows="499"
