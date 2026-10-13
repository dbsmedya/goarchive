# Test 12 — source and destination are the same database.  [validation demo]
#
# A validation demo: this config is DESIGNED to fail. Success means the failure
# arrives with the documented error category, not that the run succeeded.
# Runs only under --sakila-examples / `make e2e-examples`.
#
# Both connection blocks name 127.0.0.1:3305/sakila. GoArchive identifies a
# database by the server_uuid its connection reports plus the selected schema,
# and refuses to start when source and destination match — at connection time,
# before preflight, out of reach of --skip-dangerously-validate-preflight-allow-experimental (issue #13).
#
# The demo runs `validate` only. The integration witness is
# TestConnectRefusesSameDatabase_Integration (internal/database).
#
# The root is `payment`, which carries a BEFORE INSERT trigger: were the
# identity guard to run after preflight, the destination's trigger would be
# refused first and the category would stop matching. So the demo also pins
# that the guard runs BEFORE preflight.

test_name="Test12_SameDatabaseRefused"
test_desc="Same-database refusal: source and destination are both 127.0.0.1:3305/sakila [validation demo]"
config_file="test12_same_database.yaml"
tables="payment"
mode="example"

# Bare category: SRC_DEST_IDENTITY_CHECK has exactly one cause (see README.md in
# this directory), so the wording may improve without breaking the test.
expected_error="SRC_DEST_IDENTITY_CHECK"
