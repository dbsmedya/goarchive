# Test 16 — key matrix: copy-only on a latin1 VARCHAR root with non-ASCII values.
#
# The fixture is tests/fixtures/keymatrix.sql, loaded into sakila after Sakila.
# 60 names (old = 1) with 2 tags each.

test_name="Test16_KeyMatrixLatin1"
test_desc="Working copy-only: latin1 VARCHAR root with non-ASCII values, and its tags"
config_file="test16_keymatrix_latin1.yaml"
tables="km_name km_name_tag"
mode="working"
command="copy-only"
verify_method="sha256"

# 60 roots / batch_size 20 = 3 full batches, each followed by a sleep.
#   batch sleep: 3 x 0.3 = 0.9s
min_duration="0.9"

expected_rows="60 120"
orphan_checks="km_name_tag:name:km_name:name"
