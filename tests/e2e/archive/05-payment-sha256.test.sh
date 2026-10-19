# Test 05 — payment archive verified by sha256.
#
# Same archive as test 03 with one variable changed: the verification method. A
# failure here but not in 03 isolates to the method.

test_name="Test05_PaymentVerifySHA256"
test_desc="Working archive: payment with sha256 verification"
config_file="test05_payment_verify_sha256.yaml"
tables="payment"
mode="working"
command="archive"

# The config declares verification: sha256, and asserting on "sha256" here is
# the entire subject of this test. If this ever reads "count", the test has
# silently become a copy of test 03.
verify_method="sha256"

# Same slice and same pacing as test 03, so the same floor.
min_duration="19.8"
expected_rows="1999"
