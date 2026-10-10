# Test 01 — the film tree, with composite-key children.
#
# Archives 50 films with their whole tree: film_text, the composite-key
# association tables film_actor (actor_id, film_id) and film_category
# (film_id, category_id), and inventory -> rental -> payment, three levels deep.
#
# 2.x refused this shape with COMPOSITE_PK_CHECK, and this file was a validation
# demo. 3.0 binds composite keys whole (spec §4), so it is a working archive
# (operator, 2026-10-10, R55). It is not a GDPR diamond: every table of the job
# has one parent in it -- compare test 11.

test_name="Test01_FilmTree"
test_desc="Working archive: film -> film_text, film_actor, film_category (composite keys), inventory -> rental -> payment"
config_file="test01_film_tree.yaml"
tables="film film_text film_actor film_category inventory rental payment"
mode="working"
command="archive"

# No verification in the config: 3.0's default, sha256.
verify_method="sha256"

# 50 films / batch_size 25 = 2 full batches, each followed by a sleep (the
# second by the fetch that finds nothing):
#   batch sleep:  2 x 0.5 = 1.0s
#   delete sleep: disabled by default
min_duration="1.0"

# One number per table, in the SAME ORDER as $tables above. Measured against
# the fixture: film_id <= 50 selects 50 films, whose trees hold 286 film_actor
# rows, 226 inventory items, 788 rentals and 788 payments.
expected_rows="50 50 286 50 226 788 788"

# Every parent -> child edge of the tree. FK checks are off on the copy, so the
# destination would accept a child whose parent never arrived.
orphan_checks="film_text:film_id:film:film_id film_actor:film_id:film:film_id film_category:film_id:film:film_id inventory:film_id:film:film_id rental:inventory_id:inventory:inventory_id payment:rental_id:rental:rental_id"
