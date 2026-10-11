# Test 03 — high-volume payment archive, multi-batch.
#
# The suite's throughput case: a single-column key archived across 20 batches, so
# the per-batch sleep and the throttle between delete statements both accumulate.
#
# Deliberately the same archive as test 05, differing only in the verification
# method, so a failure in 05 alone isolates to the method.

test_name="Test03_PaymentBatch"
test_desc="Working archive: high-volume payment (single-column key, multi-batch, count verification)"
config_file="test03_payment_batch.yaml"
tables="payment"
mode="working"
command="archive"

# The config declares verification: count (3.0's default is sha256).
verify_method="count"

# 1999 rows / batch_size 100 = 19 full batches and a last batch of 99.
#   batch sleep:  19 full batches x 0.2           =  3.8s
#   delete sleep: 20 batches x 4 gaps x 0.2       = 16.0s
# (100 or 99 rows / batch_delete_size 20 = 5 statements, so 4 gaps. The short
# last batch ends the run without a sleep -- 2.x slept after it, and its floor
# was 20.0.)
min_duration="19.8"

# payment_id <= 2000 selects 1999 rows, not 2000 -- payment_id is
# not contiguous. Measured, not computed.
expected_rows="1999"
