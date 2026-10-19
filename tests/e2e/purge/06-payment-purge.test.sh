# Test 06 — purge half the payment table.
#
# The suite's ordinary delete-without-copy case. Purge copies nothing and so
# verifies nothing, and over-deletion is its unrecoverable failure mode, so the
# exact count is the assertion that matters most here.

test_name="Test06_PaymentPurge"
test_desc="Working purge: delete half the payment table without copying anything"
config_file="test06_payment_purge.yaml"
tables="payment"
mode="working"
command="purge"

# Purge never prints a verified line. "none" asserts the line is ABSENT --
# so a purge that silently gained a verify stage, or an archive misconfigured
# as a purge, fails instead of passing quietly.
verify_method="none"

# 8022 rows / batch_size 500 = 16 full batches and a 22-row last batch.
#   batch sleep:  16 full batches x 0.2           =  3.2s
#   delete sleep: 16 batches x 4 gaps x 0.2       = 12.8s
# (500 / batch_delete_size 100 = 5 statements -> 4 gaps. The 22-row last
# batch is one statement and has no gap, and ends the run without a sleep --
# 2.x slept after it, and its floor was 16.2.)
min_duration="16.0"

# HALF the table, exactly: payment_id <= 8024 selects 8022 of
# payment's 16044 rows. The boundary is 8024 rather than 8022
# because two ids below it are missing; it was queried against the
# fixture, not computed. Over-deletion is purge's unrecoverable
# failure, and this number is the only thing that catches it.
expected_rows="8022"
