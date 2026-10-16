# Test 18 -- Sakila's film tree as an account without TRIGGER.  [validation demo]
#
# A validation demo: this config is DESIGNED to fail preflight.
# Runs only under --sakila-examples / `make e2e-examples`.
#
# MySQL lists a table's triggers only to an account holding TRIGGER on it, so for
# this account film's del_film is invisible and an empty list would prove nothing.
# DELETE_TRIGGER_VISIBILITY_CHECK refuses the job before it can be taken for
# trigger-free (beta.1 audit A2). --force-triggers, which the demo arm passes by
# default, does not apply to it: the refusal must arrive anyway.
#
# The account holds exactly the documented source recipe (README_PERMISSIONS.md
# -> Source) less TRIGGER; every check before the trigger stages passes for it
# (probed 2026-10-16), so this is the refusal the operator meets.

test_name="Test18_FilmTriggerUnprovable"
test_desc="Trigger visibility: the film tree as an account without TRIGGER cannot prove there is no DELETE trigger [validation demo]"
config_file="test18_film_tree_recipe.yaml"
tables="film"
mode="example"
source_account="recipe"

# One cause, so the bare category; the detail pins the real table.
expected_error="DELETE_TRIGGER_VISIBILITY_CHECK"
expected_detail="film(unconfirmed)"
