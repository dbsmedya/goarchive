# archive — copy, verify, then delete from source

The full pipeline, and the command most of GoArchive exists for. An archive is
only safe because verification happens *before* the delete, so these tests assert
the verify stage ran by the declared method over every copied row.

| Test | Shape | Why it is here |
|---|---|---|
| 01 | `film` tree, three levels, composite-key children | the composite-key case on Sakila: `film_actor` and `film_category` are archived by their whole keys; 2.x refused this config |
| 03 | `payment`, 20 batches | the throughput case: the batch sleep and the throttle between delete statements both accumulate, so a lost throttle is visible |
| 04 | `rental` → `payment`, one batch | the graph case: a two-level tree where the child must be copied after its parent |
| 05 | `payment`, sha256 | the same archive as 03 with **one** variable changed |
| 14 | `km_line (order_id, line_no)` → items → notes | the key matrix's composite root, with a grandchild on a three-column foreign key |

**03 and 05 differ only in the verification method.** That is the point: a failure
in 05 alone isolates to sha256, and if 05's `verify_method` ever reads `count` it
has silently become a copy of 03.

**01, 04 and 14 are the source of the copy-order guarantee.** Their `orphan_checks`
catch a child copied without its parent — which no row count can see, because the
configs must disable FK checks and the destination therefore accepts an orphan
silently. 14's checks name composite columns (`order_id+line_no`).

Post-conditions for this command are the strictest in the suite: rows must have
left the source *and* arrived in the destination, both in exact numbers. See
`assert_postcondition` in `../lib/assert.sh`.
