# Test 10 — a view named as the root table.  [validation demo]
#
# A validation demo: this config is DESIGNED to fail preflight. Success means the
# failure arrives with the documented error category, not that the run succeeded.
# Runs only under --sakila-examples / `make e2e-examples`.
#
# `customer_list` is a Sakila VIEW. goarchive archives base tables only, and
# preflight classifies object types early (deviation D5) so the operator is told the
# real problem.
#
# THE ONE TEST HERE THAT DEMONSTRATES AN IMPROVEMENT, not a preserved behaviour:
#
#   1.8.0   ->  PRIMARY_KEY_CHECK        ("fix the primary key" — on a view)
#   1.9.x   ->  TABLE_EXISTENCE_CHECK    ("only base tables can be archived")
#
# So this test FAILS against a 1.8 binary, and that is correct and recorded — an
# "expected failure, deviation D5" row in the baseline, not a regression. It is the
# only such row in the suite.

test_name="Test10_ViewRejectedAsNotBaseTable"
test_desc="Non-base-table rejection: root_table is the Sakila view 'customer_list' [validation demo]"
config_file="test10_view_not_base_table.yaml"
tables="customer_list"
mode="example"

# NOT the bare category, and that is deliberate.
#
# TABLE_EXISTENCE_CHECK is emitted for TWO distinct causes: a name that does not
# exist, and a name that exists but is not a base table. A bare category would
# therefore pass on a mere typo in the config — proving nothing about D5 and
# nothing about views. That is vacuous-by-construction, so the assertion carries
# enough of the message to separate the two causes.
#
# Proven necessary rather than assumed: mutating this to the bare category while
# typo-ing the root table leaves the test PASSING (mutation M3). The failure
# message also names `customer_list(VIEW)`; the harness greps a single substring,
# and the policy text is the stronger of the two choices.
expected_error="TABLE_EXISTENCE_CHECK: Only base tables"
