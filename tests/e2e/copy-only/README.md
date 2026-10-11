# copy-only — copy, verify, delete NOTHING

The non-destructive command. Everything the archive tests assert about the
destination applies here too; what makes this category distinct is a **negative**
assertion about the source.

| Test | Shape | Why it is here |
|---|---|---|
| 07 | `rental` → `payment`, 20 batches | the source must be untouched |
| 16 | `km_name` (`latin1` `VARCHAR`) → `km_name_tag` | the key matrix's `latin1` root: non-ASCII keys copied, verified by sha256 and logged unchanged |

**The negative assertion is the test.** An archive misconfigured as a copy-only
satisfies every destination count and every orphan check; it fails only because
the source shrank. `assert_postcondition`'s `copy-only` arm requires
`after_src == before` exactly.

**Same graph and slice as archive 04, deliberately** — so a failure in 07 alone
isolates to the command. There is one intended divergence, the pacing: 04 runs the
slice in a *single* batch, where one batch exercises one sleep call and no
per-batch accumulation bug is reachable, while 07 splits it into twenty. If you
ever make the two identical, say which one you meant to weaken. The batch sizes
themselves are in the configs and in `../../README.md`.

**No delete term in the floor.** copy-only deletes nothing, so
`delete_sleep_seconds` is inert for this command and the configs do not set it.
Adding a delete term to `min_duration` here would build a gate no correct run could
pass.

`copy-only` also does not accept `--force-triggers`; it never deletes, so the flag
does not exist and passing it aborts with `unknown flag`. `run_archive_job`
handles that per command, for `validate`, `dry-run` and the run alike.

Both tests run with `resume` at its default, `false`: the destination starts empty.
