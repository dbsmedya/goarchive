# Test 06 — purge half the payment table.
#
# The suite's only delete-without-copy case. Purge has no verification stage at
# all, and over-deletion is its unrecoverable failure mode, so the exact count is
# the assertion that matters most here.

test_name="Test06_PaymentPurge"
test_desc="Working purge: delete half the payment table without copying anything"
config_file="test06_payment_purge.yaml"
tables="payment"
mode="working"
command="purge"

# Purge is batchDeleteOnly: the verify block sits inside the
# batchFull||batchCopyVerify gate, so dataVerifier is never called.
# "none" asserts the verification line is ABSENT -- so a purge that
# silently gained a verify stage, or an archive misconfigured as a
# purge, fails instead of passing quietly.
verify_method="none"

# 8022 rows / batch_size 500 = 17 batches (16 full, one 22-row tail).
#   batch sleep:  17 x 0.2                        = 3.4s
#   delete sleep: 16 batches x 4 chunk-gaps x 0.2 = 12.8s
# (500 / batch_delete_size 100 = 5 chunks -> 4 gaps. The 22-row tail
# batch is a single chunk and contributes no gap.)
min_duration="16.2"

# HALF the table, exactly: payment_id <= 8024 selects 8022 of
# payment's 16044 rows. The boundary is 8024 rather than 8022
# because two ids below it are missing; it was queried against the
# fixture, not computed. Over-deletion is purge's unrecoverable
# failure, and this number is the only thing that catches it.
expected_rows="8022"
