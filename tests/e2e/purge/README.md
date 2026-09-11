# purge — delete from source, copy nothing

The destructive command with no safety net: there is no copy and **no
verification stage at all**, so nothing can be checked after the fact. Deleted is
deleted.

| Test | Shape | Why it is here |
|---|---|---|
| 06 | `payment`, exactly half | over-deletion is purge's unrecoverable failure, and the exact count is the only thing that catches it |
| 13 | `payment`, live replica enabled | an observed replication hold preserves rows; the same process resumes and completes after the applier restarts |

Two assertions carry this category, and both are about what must *not* happen:

**`verify_method="none"`** asserts the verification line is **absent**. Purge is
`batchDeleteOnly`, and the verify block sits inside the
`batchFull || batchCopyVerify` gate, so the verifier is never called. Asserting
absence catches a purge that silently gained a verify stage — and an archive
misconfigured as a purge.

**`expected_rows` is exact.** Direction alone was measured blind: with
conservation-only checks, purging the *entire* table passed. The pacing floor
cannot rescue it either, because the floor is one-sided — deleting more rows only
makes the run *longer*.

06 deletes half the table rather than a token slice precisely so the gap between a
correct run and a runaway one is unmistakable. Its boundary and expected count
were queried against the fixture, not computed, because `payment_id` is not
contiguous; both numbers and that reasoning live in `../../README.md`, and the
test file states the count it asserts.

Test13 adds a live hold/release witness to these exact outcome checks. Its helper
restores the applier and confirms the replica is caught up even when a test fails.
Test06 remains the ordinary purge control. The suite catalogue owns the selected
counts; `../README.md` owns the declaration and cleanup mechanics.

See `../README.md` for the file format.
