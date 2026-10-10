# Test 07 — copy rental -> payment, leaving the source untouched.
#
# The suite's only non-destructive command. The assertion that carries this test
# is a NEGATIVE one: the source must be unchanged. An archive misconfigured as a
# copy-only would satisfy every destination count and fail only here.
#
# Same graph and slice as archive test 04, differing in the command.

test_name="Test07_RentalPaymentCopyOnly"
test_desc="Working copy-only: copy rental -> payment, leaving the source untouched"
config_file="test07_rental_payment_copyonly.yaml"
tables="rental payment"
mode="working"
command="copy-only"

# copy-only verifies its copies like archive; the config declares count.
verify_method="count"

# Same 200-rental slice as test 04, but batch_size 10:
#   200 rentals / 10 = 20 full batches
#   batch sleep: 20 x 0.5 = 10.0s
# The 20th batch is full, so it sleeps too; the next fetch finds nothing.
# Batch term ONLY: copy-only deletes nothing, so delete_sleep_seconds is inert
# and the config does not set it.
min_duration="10.0"

# Same slice as test 04, so the same counts -- but for copy-only the
# assertion reads differently: the source must be UNCHANGED and the
# destination must hold exactly these. See assert_postcondition.
expected_rows="200 200"
orphan_checks="payment:rental_id:rental:rental_id"
