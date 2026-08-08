# Test 11 — an internal FK the relations config never declares.  [validation demo]
#
# A validation demo: this config is DESIGNED to fail preflight. Success means the
# failure arrives with the documented error category, not that the run succeeded.
# Runs only under --sakila-examples / `make e2e-examples`.
#
# Graph is customer -> {rental, payment}, both by customer_id. Every table is
# present and every declared relation is correct. What is missing is an edge
# BETWEEN two in-graph tables: payment.rental_id -> rental.rental_id
# (fk_payment_rental) is never declared, so goarchive would delete rental rows
# while in-graph payment rows still referenced them.
#
# The most realistic operator error in the demo set: nothing is absent, so the
# config looks complete and only the FK topology disagrees.
#
# Distinct from test 02 / FK_COVERAGE_CHECK, which is about tables OUTSIDE the graph
# referencing tables inside it. This one is entirely internal — hence the category.

test_name="Test11_UndeclaredInternalFK"
test_desc="Internal-FK rejection: payment.rental_id -> rental is undeclared though both tables are in the graph [validation demo]"
config_file="test11_internal_fk_undeclared.yaml"
tables="customer rental payment"
mode="example"

# The bare category is sufficient here, and that is not inconsistent with test 10:
# INTERNAL_FK_COVERAGE has exactly one cause, and this config has exactly one
# undeclared internal FK — goarchive's output lists a single entry,
# `payment.rental_id -> rental.rental_id (constraint: fk_payment_rental)
# [no graph edge]`. There is no second cause for the substring to be confused with.
expected_error="INTERNAL_FK_COVERAGE"
