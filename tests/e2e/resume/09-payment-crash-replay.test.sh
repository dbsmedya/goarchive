# Test 09 — a crash mid-batch, then a replay from the log.
#
# Run 1 is SIGKILLed inside batch 1's delete phase, so nothing cooperates: no final
# status, no report. Run 2 has to find the abandoned work in archiver_job_log_<id>,
# where batch 1's roots are still at 'copied', and finish it.
#
# The half of the resume story test 08 structurally cannot reach. A SIGTERM leaves
# every root terminal, so recovery has nothing to do; only a real crash leaves
# 'copied' roots behind. Startup recovery replays them DELETE-ONLY (spec §6.4): their
# copy already committed and was verified in the same transaction that logged them.
#
# It also leaves archiver_job at job_status=1 (Running), because the final status
# was never written. Nothing else in the suite produces that state, and run 2 has to
# proceed past it: the advisory locks died with the killed sessions.

test_name="Test09_PaymentCrashReplay"
test_desc="Working archive: payment SIGKILLed mid-delete, then replayed from the log"
config_file="test09_payment_crash_replay.yaml"
tables="payment"
mode="working"
command="archive"

# Kill during batch 1's delete phase.
#
# The target interval is between batch 1's destination commit, which logs its
# roots at 'copied', and its completion -- exactly the delete phase, whose only
# duration is the throttle between delete statements. The engine logs nothing in
# between, so the harness polls the log until it holds the 100 roots at 'copied',
# then kills. This is why the config sets delete_sleep_seconds: 1.0 where every
# other config uses 0.2.
interrupt="crash"
interrupt_after_batches="1"

# 100 = one batch's copy. Asserted exactly, and asserted twice over: the destination
# holds 100 rows AND the log holds 100 at 'copied'. Both derive from batch_size, and
# a mismatch means the kill missed its window.
interrupt_expect_dest="100"

verify_method="sha256"

# RUN 2's floor, with the recovery term DELIBERATELY EXCLUDED.
#
# Recovery runs before the batch loop and its cost is variable: the kill point
# decides how many of batch 1's 100 rows are still present to delete, and therefore
# how many throttle sleeps it spends. Zero is legal, so folding a recovery term into
# this floor would build a gate a correct run can fail.
#
# The batch loop then covers the remaining 399 rows in 3 full batches and a last one
# of 99, 5 delete statements each:
#
#   3 x 0.2           =  0.6   batch sleep (the short last batch has none)
#   4 x 4 x 1.0       = 16.0   delete throttle
#                       ----
#                        16.6
#
# That the recovery HAPPENED is proven by the discriminator instead -- run 2's log
# must contain "Recovery: replaying 100 copied roots of payment", which test 08's
# must not. (2.x slept after the short last batch: 16.8.)
min_duration="16.6"

# The full slice, after BOTH runs. 499 rows, measured -- payment_id is not
# contiguous, so <= 500 is not 500. Smaller than test 08's slice because the wide
# delete throttle above costs 4s per batch.
expected_rows="499"
