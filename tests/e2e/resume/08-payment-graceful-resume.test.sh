# Test 08 — a graceful stop, then a resume that has nothing to replay.
#
# Run 1 is signalled with SIGTERM after batch 1 and stops COOPERATIVELY at the
# batch boundary; run 2 continues the where range and finishes the slice.
#
# The pair with test 09 is not two samples of one thing. A single SIGTERM is
# honoured at batch boundaries, so the batch in flight always reaches 'completed'
# and this test leaves ZERO non-terminal roots. Run 2's recovery finds nothing to
# replay; the roots run 1 archived are gone from the source, so the where range
# itself carries the progress (3.0 keeps no cursor, R10). Test 09 covers the
# replay path.
#
# Test 05 is this test's uninterrupted control: the config is identical, so a
# failure here but not in 05 isolates to the interrupt.

test_name="Test08_PaymentGracefulResume"
test_desc="Working archive: payment interrupted by SIGTERM at a batch boundary, then resumed"
config_file="test08_payment_graceful_resume.yaml"
tables="payment"
mode="working"
command="archive"

# Interrupt after batch 1.
#
# The engine logs "Batch 1: 100 roots of payment (archive)" at the end of batch 1
# and then sleeps sleep_seconds before its stop check; the signal lands in that
# sleep, and the run stops before fetching batch 2. Exactly one batch.
interrupt="graceful"
interrupt_after_batches="1"

# 100 = one batch at batch_size 100. Asserted EXACTLY, not as a range, and it is the
# assertion that keeps this test honest: 0 would mean nothing was copied and the
# re-run would silently do the whole job and pass, while more would mean the
# interrupt landed later than declared and min_duration below no longer describes
# run 2.
interrupt_expect_dest="100"

verify_method="sha256"

# RUN 2's floor, not the whole slice's. Run 1 completes batch 1, so run 2 processes
# the remaining 1899 rows in 18 full batches and a last one of 99, each with 5
# delete statements and so 4 gaps:
#
#   18 x 0.2          =  3.6   batch sleep (the short last batch has none)
#   19 x 4 x 0.2      = 15.2   delete throttle
#                       ----
#                        18.8
#
# Exact, unlike test 09's, because a graceful stop's boundary is clean: no recovery
# runs and the batch count is fixed. (2.x slept after the short last batch: 19.0.)
min_duration="18.8"

# The full slice, after BOTH runs. 1999 rows, measured -- payment_id is not
# contiguous, so <= 2000 is not 2000.
expected_rows="1999"
