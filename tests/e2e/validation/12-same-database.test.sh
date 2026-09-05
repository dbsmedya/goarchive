# Test 12 — source and destination are the same database.  [validation demo]
#
# A validation demo: this config is DESIGNED to fail. Success means the failure
# arrives with the documented error category, not that the run succeeded.
# Runs only under --sakila-examples / `make e2e-examples`.
#
# Both connection blocks name 127.0.0.1:3305/sakila. GoArchive identifies a
# database by the server_uuid its connection reports plus the selected schema,
# and refuses to start when source and destination match — at connection time,
# before preflight, out of reach of --skip-validate-preflight (issue #13).
#
# The demo runs `validate` only. The unchanged-source and no-tracking-write
# assertions live in the integration layer
# (internal/archiver/identity_guard_integration_test.go), which continues into
# Execute when the guard is absent so the row count witnesses the deletion.
#
# Why the root is `payment`, which carries a BEFORE INSERT trigger: with the
# identity guard absent, this config is refused by DEST_INSERT_TRIGGER_CHECK
# instead (observed 2026-09-05 with the guard commented out). So the demo pins
# not only the category but that the guard runs BEFORE preflight — move it
# after, and the trigger check wins and the category stops matching. What the
# demo does NOT show is an unguarded run destroying data; that witness is
# TestArchiveAgainstItselfIsRefused_Integration, on a trigger-free table.

test_name="Test12_SameDatabaseRefused"
test_desc="Same-database refusal: source and destination are both 127.0.0.1:3305/sakila [validation demo]"
config_file="test12_same_database.yaml"
tables="payment"
mode="example"

# Bare category: SRC_DEST_IDENTITY_CHECK has exactly one cause (see README.md in
# this directory), so the wording may improve without breaking the test.
expected_error="SRC_DEST_IDENTITY_CHECK"
