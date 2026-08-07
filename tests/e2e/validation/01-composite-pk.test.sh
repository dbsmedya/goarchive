# Test 01 — composite-PK rejection.  [validation demo]
#
# A validation demo: this config is DESIGNED to fail preflight. Success means the
# failure arrives with the documented error category, not that the run succeeded.
# Runs only under --sakila-examples / `make e2e-examples`.
#
# The config includes film_actor and film_category, which have composite primary
# keys. goarchive supports single-column root PKs only.

test_name="Test01_CompositePKRejected"
test_desc="Composite-PK rejection: config includes film_actor/film_category (composite PKs) [validation demo]"
config_file="test01_one_to_one.yaml"
tables="film film_text film_actor film_category"
mode="example"
expected_error="COMPOSITE_PK_CHECK"
