# Test 08 — a graceful stop, then a checkpoint-driven resume.
#
# Run 1 is signalled with SIGTERM part-way through and stops COOPERATIVELY at a
# batch boundary; run 2 picks up from the checkpoint and finishes the slice.
#
# The pair with test 09 is not two samples of one thing. A single SIGTERM closes a
# stop channel the loops observe at batch boundaries, so the in-flight batch always
# runs to a terminal state and this test leaves ZERO non-terminal rows. Resume
# therefore happens entirely through last_processed_root_pk_id, and recover() finds
# nothing to replay. Test 09 covers the other path.
#
# Test 05 is this test's uninterrupted control: the config is identical, so a
# failure here but not in 05 isolates to the interrupt.

test_name="Test08_PaymentGracefulResume"
test_desc="Working archive: payment interrupted by SIGTERM at a batch boundary, then resumed from its checkpoint"
config_file="test08_payment_graceful_resume.yaml"
tables="payment"
mode="working"
command="archive"

# Interrupt at the start of batch 1 and let that batch finish on its own.
#
# "Processing batch" is logged AFTER the loop's stop checks, so seeing it proves
# batch 1 has passed them; the signal then lands mid-batch, the batch completes, and
# the next loop-top check breaks. Exactly one batch -- measured, and measured at
# batch 3 as well, so this is not a mechanism that merely always stops after the
# first one.
interrupt="graceful"
interrupt_after_batches="1"

# 100 = one batch at batch_size 100. Asserted EXACTLY, not as a range, and it is the
# assertion that keeps this test honest: 0 would mean nothing was copied and the
# re-run would silently do the whole job and pass, while more would mean the
# interrupt landed later than declared and min_duration below no longer describes
# run 2.
interrupt_expect_dest="100"

# For the checkpoint assertion: last_processed_root_pk_id must equal MAX(payment_id)
# in the destination. Presence alone is too weak -- a checkpoint BELOW what actually
# landed re-opens completed roots to the forward scan and is invisible to every
# end-state count, because the rows it re-reads have already been deleted.
root_pk="payment_id"

verify_method="sha256"

# RUN 2's floor, not the whole slice's. Run 1 completes batch 1, so run 2 processes
# the remaining 1899 rows in ceil(1899/100) = 19 batches, and every one of them --
# including the 99-row tail, ceil(99/20) = 5 chunks -- has 4 delete sleeps:
#
#   19 x 0.2          =  3.8   batch sleep
#   19 x 4 x 0.2      = 15.2   delete throttle
#                       ----
#                        19.0
#
# Exact, unlike test 09's, because a graceful stop's boundary is clean: no recovery
# runs and the batch count is fixed.
min_duration="19.0"

# The full slice, after BOTH runs. 1999 rows, measured -- payment_id is not
# contiguous, so <= 2000 is not 2000.
expected_rows="1999"
