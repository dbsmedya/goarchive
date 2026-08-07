# Test 07 — copy rental -> payment, leaving the source untouched.
#
# The suite's only non-destructive command. The assertion that carries this test
# is a NEGATIVE one: the source must be byte-for-byte unchanged. An archive
# misconfigured as a copy-only would satisfy every destination count and fail
# only here.
#
# Same graph and slice as archive test 04, differing in the command.

test_name="Test07_RentalPaymentCopyOnly"
test_desc="Working copy-only: copy rental -> payment, leaving the source untouched"
config_file="test07_rental_payment_copyonly.yaml"
tables="rental payment"
mode="working"
command="copy-only"

# copy-only is batchCopyVerify, so the verify stage DOES run -- the
# copy+verify block is gated on batchFull||batchCopyVerify
# (batch_pipeline.go:122). Method is inherited, so "count".
verify_method="count"

# Same 200-rental slice as test 04, but NOT test 04's pacing. That
# config inherits batch_size 1000, making 200 rentals a single batch,
# and one batch exercises one sleep call -- no per-batch accumulation
# bug is reachable. This config sets batch_size 10:
#   200 rentals / 10 = 20 batches
#   batch sleep: 20 x 0.5 = 10.0s
# Batch term ONLY. copy-only never runs DeletePhase (gated on
# batchFull||batchDeleteOnly, batch_pipeline.go:146), so
# delete_sleep_seconds is inert for this command and the config does
# not set it. Adding a delete term here would build a gate that no
# correct run can pass.
min_duration="10.0"

# Same slice as test 04, so the same counts -- but for copy-only the
# assertion reads differently: the source must be UNCHANGED and the
# destination must hold exactly these. See assert_postcondition.
expected_rows="200 200"
orphan_checks="payment:rental_id:rental:rental_id"
