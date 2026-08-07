# Test 02 — uncovered foreign-key rejection.  [validation demo]
#
# A validation demo: this config is DESIGNED to fail preflight. Success means the
# failure arrives with the documented error category, not that the run succeeded.
# Runs only under --sakila-examples / `make e2e-examples`.
#
# Archiving 'film' leaves out-of-graph tables (inventory, film_actor,
# film_category) still referencing it, so the delete would violate their FKs.

test_name="Test02_UncoveredFKCoverage"
test_desc="FK-coverage rejection: archiving 'film' leaves out-of-graph tables (inventory/film_actor/film_category) referencing it [validation demo]"
config_file="test02_one_to_many.yaml"
tables="language film"
mode="example"
expected_error="FK_COVERAGE_CHECK"
