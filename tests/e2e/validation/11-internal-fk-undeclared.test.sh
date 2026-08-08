# Test 11 — the customer -> rental -> payment GDPR diamond.  [validation demo]
#
# A validation demo: this config is DESIGNED to fail preflight. Success means the
# failure arrives with the documented error category, not that the run succeeded.
# Runs only under --sakila-examples / `make e2e-examples`.
#
# The shape operators ask for most often ("archive everything for this customer"),
# and GoArchive cannot represent it. config.Relation carries a single `foreign_key`,
# so the graph is a strict tree -- but `payment` has two in-graph parents here
# (rental and customer) and `rental.customer_id -> customer` closes the diamond.
# Whichever parent payment declares, the other edge is undeclared, and
# INTERNAL_FK_COVERAGE refuses it.
#
# NOT an operator typo and NOT fixable by rearranging -- re-nesting only moves which
# edge is uncovered, and dropping either parent trips FK_COVERAGE_CHECK instead. Both
# facts were measured during this phase, not assumed. The only passing shape is the
# narrower rental -> payment of test 04.
#
# Until this test, that limitation lived only as prose in tests/README.md, which
# explained why no such test existed. It now exists, as a gate: a future change that
# quietly accepted a diamond would over-delete, and nothing would have caught it.
#
# Distinct from test 02 / FK_COVERAGE_CHECK, which is about tables OUTSIDE the graph
# referencing tables inside it. This one is entirely internal -- hence the category.

test_name="Test11_GDPRDiamondRejected"
test_desc="Internal-FK rejection: the customer -> rental -> payment diamond leaves payment's second parent edge undeclared [validation demo]"
config_file="test11_internal_fk_undeclared.yaml"
tables="customer rental payment"
mode="example"

# The bare category is sufficient here, and that is not inconsistent with test 10's
# longer substring: INTERNAL_FK_COVERAGE has exactly one cause, so there is no second
# cause for it to be confused with. goarchive names the uncovered edge in the body --
# `payment.customer_id -> customer.customer_id (constraint: fk_payment_customer)
# [no graph edge]` -- which is the same output tests/README.md quotes.
expected_error="INTERNAL_FK_COVERAGE"
