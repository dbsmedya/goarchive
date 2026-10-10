# Test 14 — key matrix: archive a composite root with a grandchild on a composite FK.
#
# The fixture is tests/fixtures/keymatrix.sql, loaded into sakila after Sakila.
# 180 lines (order_id <= 60) with 2 items each and a note per item.

test_name="Test14_KeyMatrixComposite"
test_desc="Working archive: composite root (order_id, line_no) -> items -> notes on a composite FK"
config_file="test14_keymatrix_composite.yaml"
tables="km_line km_item km_item_note"
mode="working"
command="archive"
verify_method="sha256"

# 180 roots / batch_size 50 = 3 full batches and a last one of 30.
#   batch sleep:  3 full batches x 0.2        = 0.6s
#   delete sleep: 3 x 7 gaps x 0.1 + 4 x 0.1  = 2.5s
min_duration="3.1"

expected_rows="180 360 360"
orphan_checks="km_item:order_id+line_no:km_line:order_id+line_no km_item_note:order_id+line_no+item_no:km_item:order_id+line_no+item_no"
