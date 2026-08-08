# archive — copy, verify, then delete from source

The full pipeline, and the command most of GoArchive exists for. An archive is
only safe because verification happens *before* the delete, so these tests assert
the verify stage ran and name its method.

| Test | Shape | Why it is here |
|---|---|---|
| 03 | `payment`, 20 batches | the throughput case: batch sleep and delete-chunk sleep both accumulate, so a lost throttle is visible |
| 04 | `rental` → `payment`, one batch | the graph case: a two-level tree where the child must be copied after its parent |
| 05 | `payment`, sha256 | the same archive as 03 with **one** variable changed |

**03 and 05 differ only in the verification method.** That is the point: a failure
in 05 alone isolates to sha256, and if 05's `verify_method` ever reads `count` it
has silently become a third copy of 03.

**04 is also the source of the copy-order guarantee.** Its `orphan_checks` catch a
payment copied without its rental — which no row count can see, because the config
must disable FK checks and the destination therefore accepts an orphan silently.

Post-conditions for this command are the strictest in the suite: rows must have
left the source *and* arrived in the destination, both in exact numbers. See
`assert_postcondition` in `../lib/assert.sh`.
