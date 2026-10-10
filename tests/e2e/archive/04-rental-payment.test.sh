# Test 04 — rental -> payment, a two-level tree.
#
# The suite's graph case: a non-diamond, GDPR-shaped subgraph where the child
# must be copied after its parent.
#
# Shares its graph and its slice with copy-only test 07, differing in the
# command, so a failure in 07 alone isolates to copy-only -- with one deliberate
# exception, the pacing: 04 runs the slice in a single batch, 07 splits it into
# 20.

test_name="Test04_RentalPayment"
test_desc="Working archive: rental -> payment (2-level tree, non-diamond GDPR-shaped subgraph)"
config_file="test04_rental_payment.yaml"
tables="rental payment"
mode="working"
command="archive"
# The config declares verification: count (3.0's default is sha256).
verify_method="count"

# The config sets batch_size 200 and inherits sleep_seconds 1 and
# delete_sleep_seconds 0. 200 rentals / 200 = one FULL batch, which is
# followed by one more fetch that finds nothing, so the run sleeps once:
#   batch sleep:  1 x 1.0 = 1.0s
#   delete sleep: disabled by default
min_duration="1.0"

# One number per table, in the SAME ORDER as $tables above.
# rental_id <= 200 selects 200 rentals, which pull in 200 payments.
expected_rows="200 200"

# The counts above cannot see a payment copied without its rental:
# this config disables FK checks on the copy path (it must -- payment
# references the uncopied customer and staff), so the destination
# accepts an orphan silently.
orphan_checks="payment:rental_id:rental:rental_id"
