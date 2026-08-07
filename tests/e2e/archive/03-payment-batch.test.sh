# Test 03 — high-volume payment archive, multi-batch.
#
# The suite's throughput case: a single-column PK archived across 20 batches, so
# the per-batch sleep and the per-chunk delete sleep both accumulate.
#
# Deliberately the same archive as test 05, differing only in the verification
# method, so a failure in 05 alone isolates to the method.

test_name="Test03_PaymentBatch"
test_desc="Working archive: high-volume payment (single-column PK, multi-batch)"
config_file="test03_payment_batch.yaml"
tables="payment"
mode="working"
command="archive"

# No verification block in the config, so the documented default
# applies: method=count, skip_verification=false.
verify_method="count"

# 1999 rows / batch_size 100 = 20 batches.
#   batch sleep:  20 x 0.2                        = 4.0s
#   delete sleep: 20 batches x 4 chunk-gaps x 0.2 = 16.0s
# (100 rows / batch_delete_size 20 = 5 chunks, and the last chunk of
# each table is not followed by a sleep, so 4 gaps.)
min_duration="20.0"

# payment_id <= 2000 selects 1999 rows, not 2000 -- payment_id is
# not contiguous. Measured, not computed.
expected_rows="1999"
