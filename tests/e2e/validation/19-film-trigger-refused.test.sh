# Test 19 -- Sakila's film tree as an account holding TRIGGER.  [validation demo]
#
# A validation demo: this config is DESIGNED to fail preflight.
# Runs only under --sakila-examples / `make e2e-examples`.
#
# With TRIGGER held, the account sees film's DELETE trigger del_film, which would
# fire on every film the archive deletes. Without --force-triggers (the operator's
# explicit acceptance) DELETE_TRIGGER_CHECK refuses the job and names it. Every
# other E2E run passes --force-triggers, so this is the only place the refusal
# itself is shown on real data. The same account with --force-triggers passes
# (probed 2026-10-16): the trigger alone is what refuses it.
#
# DELETE_TRIGGER_CHECK is not a substring of DELETE_TRIGGER_VISIBILITY_CHECK, so
# this demo cannot pass on Test18's refusal.

test_name="Test19_FilmTriggerRefused"
test_desc="Trigger check: the film tree as an account holding TRIGGER is refused for del_film without --force-triggers [validation demo]"
config_file="test18_film_tree_recipe.yaml"
tables="film"
mode="example"
source_account="recipe-trigger"
force_triggers="no"

expected_error="DELETE_TRIGGER_CHECK"
expected_detail="film(del_film)"
