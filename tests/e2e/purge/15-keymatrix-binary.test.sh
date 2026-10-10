# Test 15 — key matrix: purge a BINARY(16) root whose bytes are not UTF-8.
#
# The fixture is tests/fixtures/keymatrix.sql, loaded into sakila after Sakila.
# 120 blobs (old = 1) with 2 references each.

test_name="Test15_KeyMatrixBinary"
test_desc="Working purge: BINARY(16) root with non-UTF-8 bytes, and its references"
config_file="test15_keymatrix_binary.yaml"
tables="km_blob km_blob_ref"
mode="working"
command="purge"
verify_method="none"

# 120 roots / batch_size 50 = 2 full batches and a last one of 20.
#   batch sleep:  2 full batches x 0.2        = 0.4s
#   delete sleep: 2 x 2 gaps x 0.1 + 1 x 0.1  = 0.5s
min_duration="0.9"

expected_rows="120 240"
# Purge copies nothing, so the destination holds no rows and this pair checks
# nothing there; it is declared because every multi-table test must name one.
orphan_checks="km_blob_ref:k:km_blob:k"
